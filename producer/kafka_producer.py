"""
Customer360 Data Platform
Kafka Producer — reads generated events and publishes to Kafka topics

Usage:
    python kafka_producer.py --source ../data/synthetic/ --rate 5000
    python kafka_producer.py --source ../data/synthetic/ --rate 0  # max speed
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import click
import pandas as pd
from confluent_kafka import KafkaError, KafkaException, Producer
from dotenv import load_dotenv
from loguru import logger
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from tqdm import tqdm

load_dotenv()

# ─────────────────────────────────────────────
# Prometheus Metrics
# ─────────────────────────────────────────────

EVENTS_PUBLISHED = Counter(
    "kafka_events_published_total",
    "Total events published to Kafka",
    ["topic", "event_type"],
)
EVENTS_FAILED = Counter(
    "kafka_events_failed_total", "Total events failed to publish", ["topic"]
)
PUBLISH_LATENCY = Histogram(
    "kafka_publish_latency_seconds",
    "Time to publish one event batch",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)
QUEUE_SIZE = Gauge("kafka_producer_queue_size", "Current producer queue depth")


# ─────────────────────────────────────────────
# Topic → Kafka mapping
# ─────────────────────────────────────────────

TOPIC_MAP = {
    "LOGIN": "customer-login",
    "LOGOUT": "customer-login",
    "PRODUCT_VIEW": "product-events",
    "SEARCH": "product-events",
    "ADD_TO_CART": "cart-events",
    "PURCHASE": "purchase-events",
    "REFUND": "refund-events",
    "SUBSCRIPTION": "payment-events",
    "PAYMENT_FAILURE": "payment-events",
}

ALL_TOPICS = list(set(TOPIC_MAP.values()))


# ─────────────────────────────────────────────
# Kafka Producer Setup
# ─────────────────────────────────────────────


def make_producer(bootstrap_servers: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "customer360-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 1000,
        "linger.ms": 10,
        "batch.size": 65536,
        "compression.type": "snappy",
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.kbytes": 1048576,
    }
    return Producer(conf)


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err} | topic={msg.topic()}")
        EVENTS_FAILED.labels(topic=msg.topic()).inc()
    else:
        EVENTS_PUBLISHED.labels(
            topic=msg.topic(), event_type=msg.key().decode() if msg.key() else "unknown"
        ).inc()


def create_topics(bootstrap_servers: str) -> None:
    """Ensure all topics exist (Kafka auto-creates, but we log them)."""
    logger.info("Topics configured:")
    for t in ALL_TOPICS:
        logger.info(f"  -> {t}")


# ─────────────────────────────────────────────
# Producer Logic
# ─────────────────────────────────────────────


def publish_dataframe(
    producer: Producer,
    df: pd.DataFrame,
    rate_per_second: int,
) -> int:
    """Publish all rows in a DataFrame to their respective Kafka topics."""
    published = 0
    batch_start = time.time()

    for _, row in df.iterrows():
        event = row.to_dict()
        event_type = event.get("event_type", "UNKNOWN")
        topic = TOPIC_MAP.get(event_type, "product-events")
        key = event.get("customer_id", "unknown").encode("utf-8")

        try:
            payload = json.dumps(event, default=str).encode("utf-8")
            producer.produce(
                topic=topic,
                key=key,
                value=payload,
                callback=delivery_report,
            )
            published += 1

            # Rate limiting
            if rate_per_second > 0 and published % 1000 == 0:
                elapsed = time.time() - batch_start
                expected = published / rate_per_second
                if elapsed < expected:
                    time.sleep(expected - elapsed)

            # Poll to handle delivery callbacks
            if published % 5000 == 0:
                producer.poll(0)
                QUEUE_SIZE.set(len(producer))

        except BufferError:
            logger.warning("Producer buffer full, flushing...")
            producer.flush(timeout=30)
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")
            EVENTS_FAILED.labels(topic=topic).inc()

    producer.flush(timeout=60)
    return published


# ─────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────


@click.command()
@click.option(
    "--source", default="../data/synthetic/", help="Directory of generated event files"
)
@click.option("--bootstrap-servers", default=None, help="Kafka bootstrap servers")
@click.option("--rate", default=5000, help="Events per second (0 = unlimited)")
@click.option("--metrics-port", default=8000, help="Prometheus metrics port")
@click.option("--loop", is_flag=True, help="Loop through files continuously")
def main(source: str, bootstrap_servers: str, rate: int, metrics_port: int, loop: bool):
    """Publish generated events to Kafka topics."""

    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )

    logger.info("=" * 60)
    logger.info("  Customer360 — Kafka Producer")
    logger.info("=" * 60)
    logger.info(f"  Source       : {source}")
    logger.info(f"  Kafka        : {bootstrap_servers}")
    logger.info(f"  Rate         : {rate if rate > 0 else 'unlimited'} events/sec")
    logger.info("=" * 60)

    # Start Prometheus metrics server
    start_http_server(metrics_port)
    logger.info(f"Prometheus metrics at http://localhost:{metrics_port}/metrics")

    producer = make_producer(bootstrap_servers)
    create_topics(bootstrap_servers)

    source_path = Path(source)
    files = sorted(source_path.glob("*.parquet")) + sorted(source_path.glob("*.jsonl"))

    if not files:
        logger.error(f"No event files found in {source}")
        logger.error("Run event_generator.py first!")
        sys.exit(1)

    logger.info(f"Found {len(files)} event files")

    run = True
    while run:
        total_published = 0

        for file_path in tqdm(files, desc="Files", unit="file"):
            logger.info(f"Publishing: {file_path.name}")

            if file_path.suffix == ".parquet":
                df = pd.read_parquet(file_path)
            else:
                df = pd.read_json(file_path, lines=True)

            with PUBLISH_LATENCY.time():
                published = publish_dataframe(producer, df, rate)

            total_published += published
            logger.success(f"  Published {published:,} events from {file_path.name}")

        logger.success(f"Round complete. Total published: {total_published:,}")

        if not loop:
            run = False
        else:
            logger.info("Looping — replaying events...")
            time.sleep(1)

    logger.success("Producer finished.")


if __name__ == "__main__":
    main()
