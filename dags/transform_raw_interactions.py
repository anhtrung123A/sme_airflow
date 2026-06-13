from __future__ import annotations

"""
Transform-stage DAG for interaction intelligence.

This DAG consumes raw interaction events from Kafka, loads immutable raw interaction
records from MySQL, enriches Facebook comments with Facebook post context, and calls
OpenAI for structured extraction only. It stores enrichment output in the existing
`raw_interaction_analyses` table and updates `raw_interactions.processing_status`.

Lead creation, visitor grouping, and other downstream business actions belong to the
next stage and are intentionally not handled here.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pymysql
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from confluent_kafka import Consumer, KafkaError, Producer

LOGGER = logging.getLogger("airflow.task")

DAG_ID = "transform_raw_interactions"
KAFKA_TOPIC = "sme.raw.interactions"
KAFKA_ANALYZED_TOPIC = "sme.interactions.analyzed"
MODEL_PROVIDER = "openai"
DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
PROCESSING_STATUS_PENDING = 1
PROCESSING_STATUS_PROCESSED = 2
PROCESSING_STATUS_FAILED = 3


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_naive() -> datetime:
    return utc_now().replace(tzinfo=None)


def utc_now_iso() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def log_json(event: str, payload: dict[str, Any]) -> None:
    LOGGER.info(json.dumps({"event": event, "timestamp": utc_now_iso(), **payload}, ensure_ascii=False))


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _mysql_conn() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "db"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD") or os.getenv("MYSQL_ROOT_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "sme"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def _kafka_consumer() -> Consumer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("Kafka__BootstrapServers", "kafka:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "airflow-transform-raw-interactions")
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([os.getenv("KAFKA_RAW_INTERACTIONS_TOPIC", KAFKA_TOPIC)])
    return consumer


def _kafka_producer() -> Producer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or os.getenv("Kafka__BootstrapServers", "kafka:9092")
    return Producer({"bootstrap.servers": bootstrap})


def _get_analyzed_topic() -> str:
    return os.getenv("KAFKA_INTERACTIONS_ANALYZED_TOPIC", KAFKA_ANALYZED_TOPIC)


def _consume_batch(consumer: Consumer, max_messages: int, poll_timeout_ms: int, max_empty_polls: int) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    empty_polls = 0

    while len(messages) < max_messages:
        msg = consumer.poll(timeout=poll_timeout_ms / 1000.0)
        if msg is None:
            empty_polls += 1
            if empty_polls >= max_empty_polls:
                break
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise RuntimeError(f"Kafka consume error: {msg.error()}")

        empty_polls = 0
        raw_value = msg.value().decode("utf-8", errors="replace") if msg.value() else "{}"
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            payload = {"_raw_value": raw_value}
        messages.append(payload)

    return messages


def _extract_raw_interaction_ids(messages: list[dict[str, Any]]) -> list[int]:
    ids: list[int] = []
    for m in messages:
        raw_id = m.get("raw_interaction_id")
        if isinstance(raw_id, int):
            ids.append(raw_id)
        elif isinstance(raw_id, str) and raw_id.isdigit():
            ids.append(int(raw_id))
    return sorted(set(ids))


def _load_raw_interactions(conn: pymysql.connections.Connection, ids: list[int]) -> dict[int, dict[str, Any]]:
    if not ids:
        return {}
    placeholders = ", ".join(["%s"] * len(ids))
    sql = f"SELECT * FROM raw_interactions WHERE id IN ({placeholders})"
    with conn.cursor() as cur:
        cur.execute(sql, ids)
        rows = cur.fetchall()
    return {int(r["id"]): r for r in rows}


def _load_facebook_posts_context(conn: pymysql.connections.Connection, post_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not post_ids:
        return {}
    placeholders = ", ".join(["%s"] * len(post_ids))
    sql = f"SELECT * FROM facebook_posts WHERE facebook_post_id IN ({placeholders})"
    with conn.cursor() as cur:
        cur.execute(sql, post_ids)
        rows = cur.fetchall()
    return {str(r["facebook_post_id"]): r for r in rows}


def _build_prompt(raw: dict[str, Any], post: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "task": "Classify and extract lead signals from a Facebook comment.",
        "rules": [
            "Return strict JSON only.",
            "Short comments like 'ib', 'inbox', 'tu van', 'xin gia', 'hoc phi', 'con lop khong' can still be potential leads when post context is course/enrollment related.",
            "Do not hallucinate contact info.",
        ],
        "input": {
            "comment_text": raw.get("raw_text") or "",
            "comment_permalink_url": raw.get("source_url") or raw.get("permalink_url"),
            "facebook_post_id": raw.get("parent_external_id"),
            "post_text": (post or {}).get("message"),
            "post_permalink_url": (post or {}).get("permalink_url"),
            "source_platform": raw.get("source_platform"),
            "source_type": raw.get("source_type"),
        },
        "intent_values": [
            "CourseInquiry",
            "PriceInquiry",
            "TrialRequest",
            "RegistrationIntent",
            "ScheduleInquiry",
            "LocationInquiry",
            "SupportExisting",
            "Complaint",
            "Spam",
            "Irrelevant",
            "EngagementOnly",
            "Unknown",
        ],
        "output_schema": {
            "intent": "one of intent_values exactly, no other values allowed",
            "is_potential_lead": "boolean",
            "confidence": "number 0..1",
            "extracted_name": "string | null",
            "extracted_phone": "string | null",
            "extracted_email": "string | null",
            "course_interest": "string | null",
            "summary": "string | null",
            "next_action": "string | null",
        },
    }


def _call_openai(prompt_obj: dict[str, Any], model: str, api_key: str) -> dict[str, Any]:
    url = "https://api.openai.com/v1/chat/completions"
    body = {
        "model": model,
        "response_format": {"type": "json_object"},
        "temperature": 0,
        "messages": [
            {
                "role": "system",
                "content": "You are a CRM lead-intent extractor. Return strict JSON only.",
            },
            {
                "role": "user",
                "content": json.dumps(prompt_obj, ensure_ascii=False),
            },
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, json=body, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"OpenAI API error status={resp.status_code} body={resp.text}")

    payload = resp.json()
    content = payload.get("choices", [{}])[0].get("message", {}).get("content", "{}")
    try:
        extracted = json.loads(content)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"OpenAI returned non-JSON content: {content}") from exc

    return {"output": extracted, "raw_response": payload}


def _to_decimal_confidence(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _upsert_analysis(
    conn: pymysql.connections.Connection,
    raw_interaction_id: int,
    model_name: str,
    prompt_json: str,
    response_json: str,
    status: str,
    error_message: str | None,
    extracted: dict[str, Any] | None,
) -> int | None:
    now = utc_now_naive()
    extracted = extracted or {}

    sql = """
    INSERT INTO raw_interaction_analyses (
        raw_interaction_id, model_provider, model_name, intent, is_potential_lead, confidence,
        extracted_name, extracted_phone, extracted_email, course_interest,
        summary, next_action, prompt_json, response_json, status, error_message,
        created_at, updated_at
    ) VALUES (
        %(raw_interaction_id)s, %(model_provider)s, %(model_name)s, %(intent)s, %(is_potential_lead)s, %(confidence)s,
        %(extracted_name)s, %(extracted_phone)s, %(extracted_email)s, %(course_interest)s,
        %(summary)s, %(next_action)s, %(prompt_json)s, %(response_json)s, %(status)s, %(error_message)s,
        %(created_at)s, %(updated_at)s
    )
    ON DUPLICATE KEY UPDATE
        model_provider=VALUES(model_provider),
        model_name=VALUES(model_name),
        intent=VALUES(intent),
        is_potential_lead=VALUES(is_potential_lead),
        confidence=VALUES(confidence),
        extracted_name=VALUES(extracted_name),
        extracted_phone=VALUES(extracted_phone),
        extracted_email=VALUES(extracted_email),
        course_interest=VALUES(course_interest),
        summary=VALUES(summary),
        next_action=VALUES(next_action),
        prompt_json=VALUES(prompt_json),
        response_json=VALUES(response_json),
        status=VALUES(status),
        error_message=VALUES(error_message),
        updated_at=VALUES(updated_at)
    """

    params = {
        "raw_interaction_id": raw_interaction_id,
        "model_provider": MODEL_PROVIDER,
        "model_name": model_name,
        "intent": extracted.get("intent"),
        "is_potential_lead": bool(extracted.get("is_potential_lead", False)),
        "confidence": _to_decimal_confidence(extracted.get("confidence")),
        "extracted_name": extracted.get("extracted_name"),
        "extracted_phone": extracted.get("extracted_phone"),
        "extracted_email": extracted.get("extracted_email"),
        "course_interest": extracted.get("course_interest"),
        "summary": extracted.get("summary"),
        "next_action": extracted.get("next_action"),
        "prompt_json": prompt_json,
        "response_json": response_json,
        "status": status,
        "error_message": error_message,
        "created_at": now,
        "updated_at": now,
    }

    with conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.lastrowid:
            return int(cur.lastrowid)

        cur.execute(
            "SELECT id FROM raw_interaction_analyses WHERE raw_interaction_id=%s LIMIT 1",
            (raw_interaction_id,),
        )
        row = cur.fetchone()
        return int(row["id"]) if row and row.get("id") is not None else None


def _update_processing_status(conn: pymysql.connections.Connection, raw_interaction_id: int, status_value: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE raw_interactions SET processing_status=%s, updated_at=%s WHERE id=%s",
            (status_value, utc_now_naive(), raw_interaction_id),
        )


def _publish_analyzed_event(
    producer: Producer,
    topic: str,
    raw: dict[str, Any],
    raw_interaction_id: int,
    raw_interaction_analysis_id: int | None,
    extracted: dict[str, Any],
) -> None:
    payload = {
        "event": "raw_interaction_analyzed",
        "raw_interaction_id": raw_interaction_id,
        "raw_interaction_analysis_id": raw_interaction_analysis_id,
        "is_potential_lead": bool(extracted.get("is_potential_lead", False)),
        "confidence": extracted.get("confidence"),
        "source_platform": raw.get("source_platform"),
        "source_type": raw.get("source_type"),
        "created_at": utc_now_iso(),
    }
    producer.produce(
        topic=topic,
        key=str(raw_interaction_id).encode("utf-8"),
        value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
    )


def transform_batch() -> None:
    consumer = _kafka_consumer()
    producer = _kafka_producer()
    conn = _mysql_conn()

    max_messages = int(os.getenv("TRANSFORM_MAX_MESSAGES", "50"))
    poll_timeout_ms = int(os.getenv("TRANSFORM_POLL_TIMEOUT_MS", "5000"))
    max_empty_polls = int(os.getenv("TRANSFORM_MAX_EMPTY_POLLS", "3"))
    model_name = os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL)
    openai_api_key = _require_env("OPENAI_API_KEY")
    analyzed_topic = _get_analyzed_topic()

    try:
        messages = _consume_batch(consumer, max_messages, poll_timeout_ms, max_empty_polls)
        log_json("transform_kafka_consumed", {"consumed_message_count": len(messages)})
        if not messages:
            return

        ids = _extract_raw_interaction_ids(messages)
        raws = _load_raw_interactions(conn, ids)
        log_json("transform_raw_interactions_loaded", {"loaded_raw_interaction_count": len(raws)})

        facebook_post_ids = sorted(
            {
                str(r.get("parent_external_id"))
                for r in raws.values()
                if r.get("source_platform") == "facebook" and r.get("source_type") == "comment" and r.get("parent_external_id")
            }
        )
        posts_context = _load_facebook_posts_context(conn, facebook_post_ids)
        log_json("transform_post_context_loaded", {"loaded_post_context_count": len(posts_context)})

        openai_success = 0
        openai_failed = 0
        processed_ids: list[int] = []

        try:
            for rid in ids:
                raw = raws.get(rid)
                if not raw:
                    continue

                post_ctx = posts_context.get(str(raw.get("parent_external_id")))
                prompt_obj = _build_prompt(raw, post_ctx)
                prompt_json = json.dumps(prompt_obj, ensure_ascii=False)

                try:
                    ai_result = _call_openai(prompt_obj, model_name, openai_api_key)
                    extracted = ai_result["output"]
                    response_json = json.dumps(ai_result["raw_response"], ensure_ascii=False)

                    analysis_id = _upsert_analysis(
                        conn=conn,
                        raw_interaction_id=rid,
                        model_name=model_name,
                        prompt_json=prompt_json,
                        response_json=response_json,
                        status="succeeded",
                        error_message=None,
                        extracted=extracted,
                    )
                    _update_processing_status(conn, rid, PROCESSING_STATUS_PROCESSED)
                    _publish_analyzed_event(
                        producer=producer,
                        topic=analyzed_topic,
                        raw=raw,
                        raw_interaction_id=rid,
                        raw_interaction_analysis_id=analysis_id,
                        extracted=extracted,
                    )
                    openai_success += 1
                    processed_ids.append(rid)

                    log_json(
                        "transform_analysis_succeeded",
                        {
                            "raw_interaction_id": rid,
                            "intent": extracted.get("intent"),
                            "is_potential_lead": extracted.get("is_potential_lead"),
                            "confidence": extracted.get("confidence"),
                        },
                    )
                except Exception as exc:
                    openai_failed += 1
                    err_text = str(exc)
                    err_payload = {"error": err_text}

                    _upsert_analysis(
                        conn=conn,
                        raw_interaction_id=rid,
                        model_name=model_name,
                        prompt_json=prompt_json,
                        response_json=json.dumps(err_payload, ensure_ascii=False),
                        status="failed",
                        error_message=err_text,
                        extracted=None,
                    )
                    _update_processing_status(conn, rid, PROCESSING_STATUS_FAILED)
                    processed_ids.append(rid)

                    log_json(
                        "transform_analysis_failed",
                        {
                            "raw_interaction_id": rid,
                            "error": err_text,
                        },
                    )

            conn.commit()
            producer.flush(10)
            consumer.commit(asynchronous=False)
        except Exception:
            conn.rollback()
            raise

        log_json(
            "transform_batch_summary",
            {
                "consumed_message_count": len(messages),
                "loaded_raw_interaction_count": len(raws),
                "loaded_post_context_count": len(posts_context),
                "openai_success_count": openai_success,
                "openai_failed_count": openai_failed,
                "processed_raw_interaction_ids": processed_ids,
            },
        )
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            producer.flush(10)
        except Exception:
            pass
        conn.close()


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 5, 19),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["transform", "kafka", "openai", "raw_interactions"],
) as dag:
    transform_raw_interactions = PythonOperator(
        task_id="transform_raw_interactions",
        python_callable=transform_batch,
    )
    trigger_load_lead_candidates = TriggerDagRunOperator(
        task_id="trigger_load_lead_candidates",
        trigger_dag_id="load_lead_candidates",
        wait_for_completion=False,
    )

    transform_raw_interactions >> trigger_load_lead_candidates
