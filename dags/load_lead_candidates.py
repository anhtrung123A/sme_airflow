from __future__ import annotations

"""
Load-stage DAG for CRM candidate preparation.

This DAG consumes analyzed interaction events from Kafka, loads existing analysis + raw
interaction records, and converts them into sale-ready lead_candidates.

It does not create official CRM leads. Sales review and lead conversion belong to the
next CRM workflow stage.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pymysql
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaError

LOGGER = logging.getLogger("airflow.task")

DAG_ID = "load_lead_candidates"
DEFAULT_KAFKA_TOPIC = "sme.interactions.analyzed"

# Match backend InteractionIntent enum
INTENT_UNKNOWN = 0
INTENT_COURSE_INQUIRY = 1
INTENT_PRICE_INQUIRY = 2
INTENT_TRIAL_REQUEST = 3
INTENT_REGISTRATION_INTENT = 4
INTENT_SCHEDULE_INQUIRY = 5
INTENT_LOCATION_INQUIRY = 6
INTENT_SUPPORT_EXISTING = 20
INTENT_COMPLAINT = 21
INTENT_SPAM = 22
INTENT_IRRELEVANT = 23
INTENT_ENGAGEMENT_ONLY = 24

DECISION_AUTO_CREATE_LEAD = 1
DECISION_NEEDS_REVIEW = 2
DECISION_IGNORE = 3

STATUS_PENDING = 1


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_naive() -> datetime:
    return utc_now().replace(tzinfo=None)


def utc_now_iso() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def log_json(event: str, payload: dict[str, Any]) -> None:
    LOGGER.info(json.dumps({"event": event, "timestamp": utc_now_iso(), **payload}, ensure_ascii=False))


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
    group_id = os.getenv("KAFKA_LEAD_CANDIDATES_GROUP_ID", "airflow-load-lead-candidates")
    topic = os.getenv("KAFKA_INTERACTIONS_ANALYZED_TOPIC", DEFAULT_KAFKA_TOPIC)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def _consume_batch(consumer: Consumer, max_messages: int, poll_timeout_ms: int, max_empty_polls: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    empty_polls = 0

    while len(out) < max_messages:
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
        out.append(payload)

    return out


def _extract_analysis_ids(messages: list[dict[str, Any]]) -> list[int]:
    ids: list[int] = []
    for m in messages:
        analysis_id = m.get("raw_interaction_analysis_id")
        if isinstance(analysis_id, int):
            ids.append(analysis_id)
        elif isinstance(analysis_id, str) and analysis_id.isdigit():
            ids.append(int(analysis_id))
    return sorted(set(ids))


def _load_analyses(conn: pymysql.connections.Connection, analysis_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not analysis_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(analysis_ids))
    sql = f"SELECT * FROM raw_interaction_analyses WHERE id IN ({placeholders})"

    with conn.cursor() as cur:
        cur.execute(sql, analysis_ids)
        rows = cur.fetchall()

    return {int(r["id"]): r for r in rows}


def _load_raw_interactions(conn: pymysql.connections.Connection, raw_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not raw_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(raw_ids))
    sql = f"SELECT * FROM raw_interactions WHERE id IN ({placeholders})"

    with conn.cursor() as cur:
        cur.execute(sql, raw_ids)
        rows = cur.fetchall()

    return {int(r["id"]): r for r in rows}


def _intent_to_enum(intent: Any) -> int:
    if not isinstance(intent, str):
        return INTENT_UNKNOWN

    key = intent.strip().lower()
    mapping = {
        # Canonical enum values (PascalCase from LLM)
        "courseinquiry": INTENT_COURSE_INQUIRY,
        "priceinquiry": INTENT_PRICE_INQUIRY,
        "trialrequest": INTENT_TRIAL_REQUEST,
        "registrationintent": INTENT_REGISTRATION_INTENT,
        "scheduleinquiry": INTENT_SCHEDULE_INQUIRY,
        "locationinquiry": INTENT_LOCATION_INQUIRY,
        "supportexisting": INTENT_SUPPORT_EXISTING,
        "complaint": INTENT_COMPLAINT,
        "spam": INTENT_SPAM,
        "irrelevant": INTENT_IRRELEVANT,
        "engagementonly": INTENT_ENGAGEMENT_ONLY,
        "unknown": INTENT_UNKNOWN,
    }
    return mapping.get(key, INTENT_UNKNOWN)


def _has_value(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _calculate_score(analysis: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    confidence_raw = analysis.get("confidence")
    confidence = float(confidence_raw) if confidence_raw is not None else 0.0

    base = int(round(confidence * 100))
    has_phone_bonus = 20 if _has_value(analysis.get("extracted_phone")) else 0
    has_email_bonus = 10 if _has_value(analysis.get("extracted_email")) else 0
    has_course_interest_bonus = 10 if _has_value(analysis.get("course_interest")) else 0

    intent = str(analysis.get("intent") or "").strip().lower()
    # Intents that signal active purchase/enrollment interest
    intent_bonus_intents = {
        "courseinquiry",
        "priceinquiry",
        "trialrequest",
        "registrationintent",
        "scheduleinquiry",
        "locationinquiry",
    }
    intent_bonus = 10 if intent in intent_bonus_intents else 0

    # Non-lead intents that should suppress candidate creation
    non_lead_intents = {"", "unknown", "spam", "irrelevant", "engagementonly", "complaint", "supportexisting"}
    spam_or_unknown_penalty = 30 if intent in non_lead_intents else 0

    score = base + has_phone_bonus + has_email_bonus + has_course_interest_bonus + intent_bonus - spam_or_unknown_penalty
    score = max(0, min(100, score))

    breakdown = {
        "base_from_confidence": base,
        "has_phone_bonus": has_phone_bonus,
        "has_email_bonus": has_email_bonus,
        "has_course_interest_bonus": has_course_interest_bonus,
        "intent_bonus": intent_bonus,
        "spam_or_unknown_penalty": spam_or_unknown_penalty,
        "final_score": score,
    }
    return score, breakdown


def _score_to_decision_text(score: int) -> str:
    if score >= 80:
        return "qualified"
    if score >= 50:
        return "needs_review"
    return "rejected"


def _decision_to_enum(decision_text: str) -> int:
    if decision_text == "qualified":
        return DECISION_AUTO_CREATE_LEAD
    if decision_text == "needs_review":
        return DECISION_NEEDS_REVIEW
    return DECISION_IGNORE


def _upsert_lead_candidate(
    conn: pymysql.connections.Connection,
    raw: dict[str, Any],
    analysis: dict[str, Any],
    score: int,
    decision_text: str,
    breakdown: dict[str, Any],
) -> str:
    now = utc_now_naive()
    decision_reason = {
        "analysis_id": analysis.get("id"),
        "summary": analysis.get("summary"),
        "next_action": analysis.get("next_action"),
        "score_breakdown": breakdown,
        "source": {
            "raw_interaction_id": raw.get("id"),
            "source_platform": raw.get("source_platform"),
            "source_type": raw.get("source_type"),
            "external_id": raw.get("external_id"),
            "parent_external_id": raw.get("parent_external_id"),
        },
    }

    sql = """
    INSERT INTO lead_candidates (
        raw_interaction_id,
        source_platform,
        source_type,
        customer_name,
        phone,
        email,
        course_interest,
        normalized_text,
        detected_intent,
        intent_confidence,
        candidate_score,
        decision,
        decision_reason_json,
        status,
        reviewed_by_user_id,
        created_lead_id,
        created_at,
        updated_at
    ) VALUES (
        %(raw_interaction_id)s,
        %(source_platform)s,
        %(source_type)s,
        %(customer_name)s,
        %(phone)s,
        %(email)s,
        %(course_interest)s,
        %(normalized_text)s,
        %(detected_intent)s,
        %(intent_confidence)s,
        %(candidate_score)s,
        %(decision)s,
        %(decision_reason_json)s,
        %(status)s,
        %(reviewed_by_user_id)s,
        %(created_lead_id)s,
        %(created_at)s,
        %(updated_at)s
    )
    ON DUPLICATE KEY UPDATE
        customer_name=VALUES(customer_name),
        phone=VALUES(phone),
        email=VALUES(email),
        course_interest=VALUES(course_interest),
        normalized_text=VALUES(normalized_text),
        detected_intent=VALUES(detected_intent),
        intent_confidence=VALUES(intent_confidence),
        candidate_score=VALUES(candidate_score),
        decision=VALUES(decision),
        decision_reason_json=VALUES(decision_reason_json),
        status=IF(status=%(pending_status)s, VALUES(status), status),
        updated_at=VALUES(updated_at)
    """

    params = {
        "raw_interaction_id": int(raw["id"]),
        "source_platform": raw.get("source_platform"),
        "source_type": raw.get("source_type"),
        "customer_name": raw.get("author_name"),
        "phone": analysis.get("extracted_phone"),
        "email": analysis.get("extracted_email"),
        "course_interest": analysis.get("course_interest"),
        "normalized_text": raw.get("raw_text") or "",
        "detected_intent": _intent_to_enum(analysis.get("intent")),
        "intent_confidence": Decimal(str(analysis.get("confidence"))) if analysis.get("confidence") is not None else None,
        "candidate_score": score,
        "decision": _decision_to_enum(decision_text),
        "decision_reason_json": json.dumps(decision_reason, ensure_ascii=False),
        "status": STATUS_PENDING,
        "reviewed_by_user_id": None,
        "created_lead_id": None,
        "created_at": now,
        "updated_at": now,
        "pending_status": STATUS_PENDING,
    }

    with conn.cursor() as cur:
        affected = cur.execute(sql, params)

    return "created" if affected == 1 else "updated"


def process_load_batch() -> None:
    consumer = _kafka_consumer()
    conn = _mysql_conn()

    max_messages = int(os.getenv("LOAD_MAX_MESSAGES", "50"))
    poll_timeout_ms = int(os.getenv("LOAD_POLL_TIMEOUT_MS", "5000"))
    max_empty_polls = int(os.getenv("LOAD_MAX_EMPTY_POLLS", "3"))

    try:
        messages = _consume_batch(consumer, max_messages, poll_timeout_ms, max_empty_polls)
        log_json("load_kafka_consumed", {"consumed_message_count": len(messages)})
        if not messages:
            return

        analysis_ids = _extract_analysis_ids(messages)
        analyses = _load_analyses(conn, analysis_ids)
        log_json("load_analyses_loaded", {"loaded_analysis_count": len(analyses)})

        raw_ids = sorted({int(a["raw_interaction_id"]) for a in analyses.values() if a.get("raw_interaction_id") is not None})
        raws = _load_raw_interactions(conn, raw_ids)
        log_json("load_raw_interactions_loaded", {"loaded_raw_interaction_count": len(raws)})

        skipped = 0
        created = 0
        updated = 0
        rejected = 0
        processed_raw_ids: list[int] = []

        try:
            for analysis_id in analysis_ids:
                analysis = analyses.get(analysis_id)
                if not analysis:
                    skipped += 1
                    continue

                raw_id = int(analysis.get("raw_interaction_id"))
                raw = raws.get(raw_id)
                if not raw:
                    skipped += 1
                    continue

                status = str(analysis.get("status") or "").lower()
                is_potential_lead = bool(analysis.get("is_potential_lead"))
                confidence = float(analysis.get("confidence") or 0)

                if status != "succeeded" or (not is_potential_lead) or confidence < 0.7:
                    skipped += 1
                    continue

                score, breakdown = _calculate_score(analysis)
                decision_text = _score_to_decision_text(score)

                if decision_text == "rejected":
                    rejected += 1
                    continue

                result = _upsert_lead_candidate(conn, raw, analysis, score, decision_text, breakdown)
                if result == "created":
                    created += 1
                else:
                    updated += 1

                processed_raw_ids.append(raw_id)

            conn.commit()
            consumer.commit(asynchronous=False)
        except Exception:
            conn.rollback()
            raise

        log_json(
            "load_lead_candidates_summary",
            {
                "consumed_message_count": len(messages),
                "loaded_analysis_count": len(analyses),
                "loaded_raw_interaction_count": len(raws),
                "skipped_count": skipped,
                "created_count": created,
                "updated_count": updated,
                "rejected_count": rejected,
                "processed_raw_interaction_ids": processed_raw_ids,
            },
        )
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        conn.close()


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 5, 21),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["load", "lead_candidates", "kafka"],
) as dag:
    load_lead_candidates = PythonOperator(
        task_id="load_lead_candidates",
        python_callable=process_load_batch,
    )

    load_lead_candidates
