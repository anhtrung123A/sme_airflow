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
    """Consume a batch of Kafka messages with retry polling and manual commits."""
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency 'confluent-kafka'. Install it in Airflow image before running this DAG."
        ) from exc

    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    group_id = os.getenv("KAFKA_GROUP_ID", "airflow-sme-raw-interactions")
    max_messages = int(os.getenv("KAFKA_MAX_MESSAGES", "100"))
    poll_timeout_ms = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "15000"))
    max_empty_polls = int(os.getenv("KAFKA_MAX_EMPTY_POLLS", "3"))

    logger = logging.getLogger("airflow.task")
    logger.info(
        "Starting Kafka consumer bootstrap_servers=%s topic=%s group_id=%s max_messages=%s",
        bootstrap_servers,
        topic,
        group_id,
        max_messages,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    logger.info("Subscribed to Kafka topic: %s", topic)

    consumed = 0
    empty_polls = 0
    try:
        while consumed < max_messages:
            message = consumer.poll(timeout=poll_timeout_ms / 1000.0)
            if message is None:
                empty_polls += 1
                logger.info(
                    "No message received. retry=%s/%s (timeout=%sms)",
                    empty_polls,
                    max_empty_polls,
                    poll_timeout_ms,
                )
                if empty_polls >= max_empty_polls:
                    logger.info(
                        "Reached max empty polls (%s). Stopping consume loop.",
                        max_empty_polls,
                    )
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(
                        "Reached partition EOF topic=%s partition=%s offset=%s",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                    )
                    continue
                raise RuntimeError(f"Kafka consume error: {message.error()}")

            empty_polls = 0
            raw_value_bytes = message.value()
            raw_value = (
                raw_value_bytes.decode("utf-8", errors="replace")
                if raw_value_bytes is not None
                else None
            )
            parsed_value = raw_value
            if isinstance(raw_value, str):
                try:
                    parsed_value = json.loads(raw_value)
                except json.JSONDecodeError:
                    parsed_value = raw_value

            raw_key_bytes = message.key()
            key = raw_key_bytes.decode("utf-8", errors="replace") if raw_key_bytes else None

            logger.info(
                "Kafka message topic=%s partition=%s offset=%s key=%s value=%s",
                message.topic(),
                message.partition(),
                message.offset(),
                key,
                parsed_value,
            )

            consumed += 1

        if consumed > 0:
            consumer.commit(asynchronous=False)
            logger.info("Committed offsets for %s messages.", consumed)
        else:
            logger.info("No messages available in topic '%s'.", topic)
        logger.info("Total consumed messages: %s", consumed)
    finally:
        try:
            consumer.close()
            logger.info("Kafka consumer closed safely.")
        except Exception:
            logger.exception("Failed to close Kafka consumer cleanly.")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 5, 18),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "sme", "interactions"],
) as dag:
    consume_raw_interactions = PythonOperator(
        task_id="consume_raw_interactions",
        python_callable=consume_messages,
    )

    consume_raw_interactions
