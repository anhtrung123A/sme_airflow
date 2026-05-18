from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_ID = "consume_sme_raw_interactions"
DEFAULT_TOPIC = "sme.raw.interactions"


def consume_messages() -> None:
    """Consume a small batch of Kafka messages and write them to Airflow logs."""
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'kafka-python'. Install it in Airflow image before running this DAG."
        ) from exc

    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "airflow-sme-raw-interactions")
    max_messages = int(os.getenv("KAFKA_MAX_MESSAGES", "100"))
    poll_timeout_ms = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "5000"))

    logger = logging.getLogger("airflow.task")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=poll_timeout_ms,
        value_deserializer=lambda value: value.decode("utf-8", errors="replace"),
        key_deserializer=lambda key: key.decode("utf-8", errors="replace") if key else None,
    )

    consumed = 0
    try:
        for message in consumer:
            raw_value = message.value
            parsed_value = raw_value
            if isinstance(raw_value, str):
                try:
                    parsed_value = json.loads(raw_value)
                except json.JSONDecodeError:
                    parsed_value = raw_value

            logger.info(
                "Kafka message topic=%s partition=%s offset=%s key=%s value=%s",
                message.topic,
                message.partition,
                message.offset,
                message.key,
                parsed_value,
            )

            consumed += 1
            if consumed >= max_messages:
                break

        if consumed > 0:
            consumer.commit()
            logger.info("Committed offsets for %s messages.", consumed)
        else:
            logger.info("No messages available in topic '%s'.", topic)
    finally:
        consumer.close()


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 5, 18),
    schedule=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "sme", "interactions"],
) as dag:
    consume_raw_interactions = PythonOperator(
        task_id="consume_raw_interactions",
        python_callable=consume_messages,
    )

    consume_raw_interactions
