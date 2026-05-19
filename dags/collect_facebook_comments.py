from __future__ import annotations

"""
Ingestion-boundary DAG for Facebook comments.

This DAG only:
1) Collects raw Facebook comments from Graph API.
2) Stores immutable-ish raw interaction data in `raw_interactions`.
3) Publishes Kafka events only for newly inserted raw_interactions.

This DAG does NOT do user grouping, identity resolution, post-context enrichment,
NLP/intent detection, or lead creation. Those belong to downstream transform/NLP stages.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pymysql
import requests
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from confluent_kafka import Producer
from pymysql.err import IntegrityError
from requests import Response

LOGGER = logging.getLogger("airflow.task")

DAG_ID = "collect_facebook_comments"
DEFAULT_GRAPH_VERSION = "v25.0"
COMMENTS_FIELDS = "id,message,created_time,from{id,name},permalink_url"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def log_json(event: str, payload: dict[str, Any]) -> None:
    data = {
        "event": event,
        "timestamp": utc_now_iso(),
        **payload,
    }
    LOGGER.info(json.dumps(data, ensure_ascii=False))


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
        autocommit=False,
    )


def _get_kafka_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("Kafka__BootstrapServers", "kafka:9092")


def _get_kafka_topic() -> str:
    return os.getenv("KAFKA_RAW_INTERACTIONS_TOPIC") or os.getenv(
        "Kafka__RawInteractionsTopic", "sme.raw.interactions"
    )


def _load_facebook_post_ids(conn: pymysql.connections.Connection) -> list[str]:
    sql = """
        SELECT facebook_post_id
        FROM facebook_posts
        WHERE facebook_post_id IS NOT NULL
          AND facebook_post_id <> ''
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    return [str(row["facebook_post_id"]).strip() for row in rows if str(row.get("facebook_post_id", "")).strip()]


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


def parse_facebook_created_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        # Facebook usually returns "2026-05-19T13:59:00+0000"
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _get_raw_interactions_columns(conn: pymysql.connections.Connection) -> set[str]:
    with conn.cursor() as cursor:
        cursor.execute("SHOW COLUMNS FROM raw_interactions")
        rows = cursor.fetchall()
    return {str(row.get("Field", "")) for row in rows}


def normalize_facebook_comment(
    comment: dict[str, Any],
    post_id: str,
    available_columns: set[str],
) -> dict[str, Any] | None:
    facebook_comment_id = str(comment.get("id", "")).strip()
    if not facebook_comment_id:
        return None

    author = comment.get("from") if isinstance(comment.get("from"), dict) else {}
    author_name = author.get("name") if isinstance(author, dict) else None
    author_external_id = author.get("id") if isinstance(author, dict) else None

    created_at_utc = utc_now().replace(tzinfo=None)
    occurred_at = parse_facebook_created_time(comment.get("created_time"))

    metadata = {
        "facebook_post_id": post_id,
        "facebook_comment_id": facebook_comment_id,
        "permalink_url": comment.get("permalink_url"),
        "author_name": author_name,
        "author_external_id": author_external_id,
    }
    url_column = "source_url" if "source_url" in available_columns else "permalink_url"

    raw: dict[str, Any] = {
        "external_id": facebook_comment_id,
        "parent_external_id": post_id,
        "source_platform": "facebook",
        "source_type": "comment",
        "session_id": None,
        "visitor_id": None,
        "raw_text": comment.get("message") or "",
        url_column: comment.get("permalink_url"),
        "occurred_at": occurred_at,
        "raw_payload_json": json.dumps(comment, ensure_ascii=False),
        "metadata_json": json.dumps(metadata, ensure_ascii=False),
        "processing_status": 1,
        "phone": None,
        "collected_at": created_at_utc,
        "created_at": created_at_utc,
        "updated_at": created_at_utc,
    }

    if "author_name" in available_columns:
        raw["author_name"] = author_name
    if "author_external_id" in available_columns:
        raw["author_external_id"] = author_external_id

    return raw


def insert_or_update_raw_interaction(
    conn: pymysql.connections.Connection,
    raw: dict[str, Any],
    available_columns: set[str],
) -> tuple[str, int | None]:
    url_column = "source_url" if "source_url" in available_columns else "permalink_url"
    base_insert_columns = [
        "external_id",
        "parent_external_id",
        "source_platform",
        "source_type",
        "session_id",
        "visitor_id",
        "raw_text",
        url_column,
        "occurred_at",
        "raw_payload_json",
        "metadata_json",
        "processing_status",
        "phone",
        "collected_at",
        "created_at",
        "updated_at",
    ]

    if "author_name" in available_columns:
        base_insert_columns.append("author_name")
    if "author_external_id" in available_columns:
        base_insert_columns.append("author_external_id")

    placeholders = ", ".join(["%s"] * len(base_insert_columns))
    columns_sql = ", ".join(base_insert_columns)
    insert_values = [raw.get(col) for col in base_insert_columns]

    insert_sql = f"INSERT INTO raw_interactions ({columns_sql}) VALUES ({placeholders})"

    safe_update_columns = [
        "raw_text",
        url_column,
        "raw_payload_json",
        "metadata_json",
        "occurred_at",
        "updated_at",
    ]
    if "author_name" in available_columns:
        safe_update_columns.append("author_name")
    if "author_external_id" in available_columns:
        safe_update_columns.append("author_external_id")

    update_sql = f"""
        UPDATE raw_interactions
        SET {', '.join([f"{col} = %s" for col in safe_update_columns])}
        WHERE source_platform = %s
          AND source_type = %s
          AND external_id = %s
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, insert_values)
            raw_interaction_id = int(cursor.lastrowid) if cursor.lastrowid else None
        conn.commit()
        return "inserted", raw_interaction_id
    except IntegrityError as exc:
        if exc.args and int(exc.args[0]) == 1062:
            update_values = [raw.get(col) for col in safe_update_columns]
            update_values.extend([raw["source_platform"], raw["source_type"], raw["external_id"]])
            with conn.cursor() as cursor:
                cursor.execute(update_sql, update_values)
            conn.commit()
            return "updated", None
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise


def publish_raw_interaction_created(
    producer: Producer,
    topic: str,
    raw_interaction: dict[str, Any],
) -> None:
    payload = {
        "event": "raw_interaction_created",
        "raw_interaction_id": raw_interaction.get("id"),
        "source_platform": raw_interaction["source_platform"],
        "source_type": raw_interaction["source_type"],
        "external_id": raw_interaction["external_id"],
        "parent_external_id": raw_interaction["parent_external_id"],
        "occurred_at": (
            raw_interaction["occurred_at"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            if isinstance(raw_interaction.get("occurred_at"), datetime)
            else None
        ),
        "created_at": (
            raw_interaction["created_at"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            if isinstance(raw_interaction.get("created_at"), datetime)
            else utc_now_iso()
        ),
    }

    key = str(raw_interaction.get("id") or raw_interaction["external_id"])
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )
    producer.flush(10)


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 5, 19),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["facebook", "comments", "raw_interactions", "ingestion"],
)
def collect_facebook_comments() -> None:
    @task(task_id="fetch_normalize_upsert_and_publish")
    def fetch_normalize_upsert_and_publish() -> None:
        access_token = _require_env("META_PAGE_ACCESS_TOKEN")
        graph_version = os.getenv("META_GRAPH_VERSION", DEFAULT_GRAPH_VERSION)
        kafka_bootstrap_servers = _get_kafka_bootstrap_servers()
        kafka_topic = _get_kafka_topic()

        conn = _get_mysql_connection()
        producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})

        fetched_total = 0
        inserted_total = 0
        updated_total = 0
        published_total = 0
        skipped_total = 0
        failed_total = 0

        try:
            available_columns = _get_raw_interactions_columns(conn)
            post_ids = _load_facebook_post_ids(conn)
            log_json("facebook_posts_loaded", {"post_count": len(post_ids)})

            for facebook_post_id in post_ids:
                log_json("facebook_post_processing_started", {"facebook_post_id": facebook_post_id})
                try:
                    comments = _fetch_comments_for_post(
                        facebook_post_id=facebook_post_id,
                        access_token=access_token,
                        graph_version=graph_version,
                    )
                    fetched_total += len(comments)

                    for comment in comments:
                        raw = normalize_facebook_comment(comment=comment, post_id=facebook_post_id, available_columns=available_columns)
                        if not raw:
                            skipped_total += 1
                            log_json(
                                "raw_interaction_skipped",
                                {
                                    "facebook_post_id": facebook_post_id,
                                    "reason": "missing_facebook_comment_id",
                                },
                            )
                            continue

                        try:
                            action, raw_id = insert_or_update_raw_interaction(conn, raw, available_columns)
                            if action == "inserted":
                                inserted_total += 1
                                raw["id"] = raw_id
                                try:
                                    publish_raw_interaction_created(producer, kafka_topic, raw)
                                    published_total += 1
                                except Exception as kafka_exc:
                                    # TODO: adopt transactional outbox pattern for guaranteed DB-Kafka consistency.
                                    log_json(
                                        "raw_interaction_kafka_publish_failed",
                                        {
                                            "external_id": raw["external_id"],
                                            "raw_interaction_id": raw_id,
                                            "error": str(kafka_exc),
                                        },
                                    )
                            else:
                                updated_total += 1
                                log_json(
                                    "raw_interaction_duplicate_updated",
                                    {
                                        "external_id": raw["external_id"],
                                        "facebook_post_id": facebook_post_id,
                                    },
                                )
                        except Exception as record_exc:
                            failed_total += 1
                            log_json(
                                "raw_interaction_process_failed",
                                {
                                    "facebook_post_id": facebook_post_id,
                                    "facebook_comment_id": comment.get("id"),
                                    "error": str(record_exc),
                                },
                            )

                    log_json(
                        "facebook_comments_fetched",
                        {
                            "facebook_post_id": facebook_post_id,
                            "comment_count": len(comments),
                        },
                    )
                except Exception as post_exc:
                    failed_total += 1
                    log_json(
                        "facebook_comment_fetch_failed",
                        {
                            "facebook_post_id": facebook_post_id,
                            "error": str(post_exc),
                        },
                    )
                    continue
        finally:
            try:
                producer.flush(10)
            except Exception:
                pass
            conn.close()

        log_json(
            "collect_facebook_comments_summary",
            {
                "total_fetched_comments": fetched_total,
                "inserted_raw_interactions": inserted_total,
                "updated_duplicates": updated_total,
                "published_kafka_events": published_total,
                "skipped_records": skipped_total,
                "failed_records": failed_total,
            },
        )

    collect_task = fetch_normalize_upsert_and_publish()
    trigger_transform_raw_interactions = TriggerDagRunOperator(
        task_id="trigger_transform_raw_interactions",
        trigger_dag_id="transform_raw_interactions",
        wait_for_completion=False,
    )

    collect_task >> trigger_transform_raw_interactions


dag = collect_facebook_comments()
