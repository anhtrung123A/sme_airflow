from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pymysql
import requests
from airflow.decorators import dag, task
from requests import Response

LOGGER = logging.getLogger("airflow.task")

DAG_ID = "collect_facebook_comments"
DEFAULT_GRAPH_VERSION = "v25.0"
COMMENTS_FIELDS = "id,message,created_time,from{id,name},permalink_url"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log_json(event: str, payload: dict[str, Any]) -> None:
    log_data: dict[str, Any] = {
        "event": event,
        "timestamp": utc_now_iso(),
        **payload,
    }
    LOGGER.info(json.dumps(log_data, ensure_ascii=False))


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_mysql_connection() -> pymysql.connections.Connection:
    host = os.getenv("MYSQL_HOST", "db")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD") or os.getenv("MYSQL_ROOT_PASSWORD", "")
    database = os.getenv("MYSQL_DATABASE", "sme")

    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def _load_facebook_post_ids() -> list[str]:
    sql = """
        SELECT facebook_post_id
        FROM facebook_posts
        WHERE facebook_post_id IS NOT NULL
          AND facebook_post_id <> ''
    """

    connection = _get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
    finally:
        connection.close()

    post_ids: list[str] = []
    for row in rows:
        post_id = str(row.get("facebook_post_id", "")).strip()
        if post_id:
            post_ids.append(post_id)

    return post_ids


def _extract_graph_error(response: Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return f"HTTP {response.status_code}: {response.text}"

    if not isinstance(payload, dict):
        return f"HTTP {response.status_code}: Unexpected response format"

    error = payload.get("error", {})
    if not isinstance(error, dict):
        return f"HTTP {response.status_code}: {payload}"

    return (
        f"HTTP {response.status_code}; "
        f"type={error.get('type')}; "
        f"code={error.get('code')}; "
        f"subcode={error.get('error_subcode')}; "
        f"message={error.get('message')}; "
        f"fbtrace_id={error.get('fbtrace_id')}"
    )


def _fetch_comments_for_post(
    facebook_post_id: str,
    access_token: str,
    graph_version: str,
    timeout_seconds: int = 30,
) -> list[dict[str, Any]]:
    comments: list[dict[str, Any]] = []
    next_url = f"https://graph.facebook.com/{graph_version}/{facebook_post_id}/comments"
    params: dict[str, Any] | None = {
        "access_token": access_token,
        "fields": COMMENTS_FIELDS,
        "filter": "stream",
        "order": "chronological",
        "limit": 100,
    }

    while next_url:
        response = requests.get(next_url, params=params, timeout=timeout_seconds)
        if not response.ok:
            raise RuntimeError(_extract_graph_error(response))

        payload = response.json()
        batch = payload.get("data", []) if isinstance(payload, dict) else []
        if not isinstance(batch, list):
            break

        for item in batch:
            if isinstance(item, dict):
                comments.append(item)

        paging = payload.get("paging", {}) if isinstance(payload, dict) else {}
        next_url = paging.get("next") if isinstance(paging, dict) else None
        params = None

        if not batch:
            break

    return comments


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 5, 19),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["facebook", "comments", "sync"],
)
def collect_facebook_comments() -> None:
    @task(task_id="fetch_and_log_post_comments")
    def fetch_and_log_post_comments() -> None:
        access_token = _require_env("META_PAGE_ACCESS_TOKEN")
        graph_version = os.getenv("META_GRAPH_VERSION", DEFAULT_GRAPH_VERSION)

        post_ids = _load_facebook_post_ids()
        log_json(
            "facebook_posts_loaded",
            {"post_count": len(post_ids)},
        )

        for facebook_post_id in post_ids:
            log_json(
                "facebook_post_processing_started",
                {"facebook_post_id": facebook_post_id},
            )

            try:
                comments = _fetch_comments_for_post(
                    facebook_post_id=facebook_post_id,
                    access_token=access_token,
                    graph_version=graph_version,
                )

                log_json(
                    "facebook_comments_fetched",
                    {
                        "facebook_post_id": facebook_post_id,
                        "comment_count": len(comments),
                    },
                )

                for comment in comments:
                    author = comment.get("from") if isinstance(comment.get("from"), dict) else {}
                    author_name = author.get("name") if isinstance(author, dict) else None
                    author_id = author.get("id") if isinstance(author, dict) else None

                    log_json(
                        "facebook_comment",
                        {
                            "facebook_post_id": facebook_post_id,
                            "facebook_comment_id": comment.get("id"),
                            "from_id": author_id,
                            "author_name": author_name,
                            "message": comment.get("message") or "",
                            "created_time": comment.get("created_time"),
                            "permalink_url": comment.get("permalink_url"),
                        },
                    )
            except Exception as exc:
                # Continue with next post to avoid failing entire DAG for one bad post.
                log_json(
                    "facebook_comment_fetch_failed",
                    {
                        "facebook_post_id": facebook_post_id,
                        "error": str(exc),
                    },
                )

    fetch_and_log_post_comments()


dag = collect_facebook_comments()
