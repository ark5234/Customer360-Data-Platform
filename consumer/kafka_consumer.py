"""
Customer360 Data Platform
Kafka Consumer — base consumer that routes events to MinIO bronze layer

This consumer is supplemented by Spark Streaming for heavy processing.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from io import BytesIO

import click
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from loguru import logger
from minio import Minio
from minio.error import S3Error

load_dotenv()

BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "customer360-bronze")


def make_consumer(bootstrap_servers: str, group_id: str, topics: list[str]) -> Consumer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    }
    c = Consumer(conf)
    c.subscribe(topics)
    return c


def make_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "customer360"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "customer360secret"),
        secure=False,
    )


def upload_to_bronze(client: Minio, events: list[dict], topic: str) -> None:
    """Upload a batch of events to MinIO bronze layer as partitioned parquet."""
    if not events:
        return

    now = datetime.utcnow()
    partition = (
        f"topic={topic}/year={now.year}/month={now.month:02d}"
        f"/day={now.day:02d}/hour={now.hour:02d}"
    )
    filename = f"events_{now.strftime('%H%M%S_%f')}.parquet"
    object_name = f"{partition}/{filename}"

    df = pd.DataFrame(events)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    try:
        client.put_object(
            bucket_name=BRONZE_BUCKET,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        logger.success(
            f"Uploaded {len(events)} events -> {BRONZE_BUCKET}/{object_name}"
        )
    except S3Error as e:
        logger.error(f"MinIO upload failed: {e}")


@click.command()
@click.option(
    "--topics",
    default="customer-login,product-events,cart-events,purchase-events,payment-events,refund-events",
)
@click.option("--bootstrap-servers", default=None)
@click.option("--group-id", default="customer360-bronze-writer")
@click.option(
    "--batch-size", default=1000, help="Events to buffer before writing to MinIO"
)
def main(topics: str, bootstrap_servers: str, group_id: str, batch_size: int):
    """Consume Kafka events and write to MinIO bronze layer."""
    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    topic_list = [t.strip() for t in topics.split(",")]

    logger.info(f"Starting consumer | topics={topic_list} | group={group_id}")

    consumer = make_consumer(bootstrap_servers, group_id, topic_list)
    minio_client = make_minio_client()

    buffer: dict[str, list] = {t: [] for t in topic_list}
    msg_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            value = json.loads(msg.value().decode("utf-8"))
            buffer[topic].append(value)
            msg_count += 1

            if len(buffer[topic]) >= batch_size:
                upload_to_bronze(minio_client, buffer[topic], topic)
                buffer[topic] = []
                consumer.commit(asynchronous=False)

            if msg_count % 10000 == 0:
                logger.info(f"Processed {msg_count:,} messages")

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        # Flush remaining buffers
        for topic, events in buffer.items():
            if events:
                upload_to_bronze(minio_client, events, topic)
    finally:
        consumer.close()
        logger.success("Consumer closed cleanly.")


if __name__ == "__main__":
    main()
