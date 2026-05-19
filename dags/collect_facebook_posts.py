from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pymysql
import requests
from requests import Response
from airflow.decorators import dag, task

LOGGER = logging.getLogger("airflow.task")

DAG_ID = "collect_facebook_posts"
DEFAULT_GRAPH_VERSION = "v25.0"
DEFAULT_MAX_POSTS_PER_RUN = 100


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_graph_base_url(graph_version: str) -> str:
    return f"https://graph.facebook.com/{graph_version}"


def _raise_graph_api_error(response: Response) -> None:
    try:
        payload = response.json()
    except ValueError:
        payload = {}

    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    message = error.get("message", response.text)
    code = error.get("code")
    error_type = error.get("type")
    subcode = error.get("error_subcode")
    fbtrace_id = error.get("fbtrace_id")

    raise RuntimeError(
        "Facebook Graph API request failed "
        f"(status={response.status_code}, type={error_type}, code={code}, subcode={subcode}, "
        f"message={message}, fbtrace_id={fbtrace_id})"
    )


def _fetch_page_posts(
    page_id: str,
    access_token: str,
    graph_version: str,
    max_posts: int,
    timeout_seconds: int = 30,
) -> list[dict[str, Any]]:
    fields = "id,message,created_time,permalink_url"
    posts: list[dict[str, Any]] = []

    next_url = f"{_get_graph_base_url(graph_version)}/{page_id}/posts"
    params: dict[str, Any] | None = {
        "access_token": access_token,
        "fields": fields,
        "limit": min(50, max_posts),
    }

    while next_url and len(posts) < max_posts:
        response = requests.get(next_url, params=params, timeout=timeout_seconds)
        if not response.ok:
            _raise_graph_api_error(response)
        payload = response.json()

        batch = payload.get("data", [])
        if not isinstance(batch, list):
            break

        for item in batch:
            if not isinstance(item, dict):
                continue
            posts.append(item)
            if len(posts) >= max_posts:
                break

        paging = payload.get("paging", {}) if isinstance(payload, dict) else {}
        next_url = paging.get("next") if isinstance(paging, dict) else None
        params = None

        if not batch:
            break

    return posts


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
        autocommit=False,
    )


def _upsert_posts(page_id: str, posts: list[dict[str, Any]]) -> tuple[int, int]:
    if not posts:
        return 0, 0

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    inserted = 0
    updated = 0

    upsert_sql = """
        INSERT INTO facebook_posts (
            facebook_post_id,
            page_id,
            message,
            permalink_url,
            created_time,
            raw_payload,
            last_synced_at,
            created_at,
            updated_at
        ) VALUES (
            %(facebook_post_id)s,
            %(page_id)s,
            %(message)s,
            %(permalink_url)s,
            %(created_time)s,
            %(raw_payload)s,
            %(last_synced_at)s,
            %(created_at)s,
            %(updated_at)s
        )
        ON DUPLICATE KEY UPDATE
            message = VALUES(message),
            permalink_url = VALUES(permalink_url),
            raw_payload = VALUES(raw_payload),
            last_synced_at = VALUES(last_synced_at),
            updated_at = VALUES(updated_at)
    """

    connection = _get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            for post in posts:
                facebook_post_id = str(post.get("id", "")).strip()
                if not facebook_post_id:
                    continue

                created_time_raw = post.get("created_time")
                created_time: datetime | None = None
                if isinstance(created_time_raw, str) and created_time_raw:
                    created_time = datetime.fromisoformat(
                        created_time_raw.replace("Z", "+00:00")
                    ).replace(tzinfo=None)

                payload = {
                    "facebook_post_id": facebook_post_id,
                    "page_id": page_id,
                    "message": post.get("message") or "",
                    "permalink_url": post.get("permalink_url"),
                    "created_time": created_time,
                    "raw_payload": json.dumps(post, ensure_ascii=False),
                    "last_synced_at": now_utc,
                    "created_at": now_utc,
                    "updated_at": now_utc,
                }

                affected_rows = cursor.execute(upsert_sql, payload)
                if affected_rows == 1:
                    inserted += 1
                elif affected_rows == 2:
                    updated += 1

        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return inserted, updated


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 5, 19),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["facebook", "sync", "posts"],
)
def collect_facebook_posts() -> None:
    @task(task_id="fetch_and_upsert_page_posts")
    def fetch_and_upsert_page_posts() -> None:
        page_id = _require_env("META_PAGE_ID")
        access_token = _require_env("META_PAGE_ACCESS_TOKEN")
        graph_version = os.getenv("META_GRAPH_VERSION", DEFAULT_GRAPH_VERSION)
        max_posts = int(os.getenv("META_MAX_POSTS_PER_RUN", str(DEFAULT_MAX_POSTS_PER_RUN)))

        posts = _fetch_page_posts(
            page_id=page_id,
            access_token=access_token,
            graph_version=graph_version,
            max_posts=min(max_posts, DEFAULT_MAX_POSTS_PER_RUN),
        )

        inserted, updated = _upsert_posts(page_id=page_id, posts=posts)

        LOGGER.info("Total posts fetched: %s", len(posts))
        LOGGER.info("Upsert result inserted=%s updated=%s", inserted, updated)

    fetch_and_upsert_page_posts()


dag = collect_facebook_posts()
