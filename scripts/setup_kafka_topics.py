"""
Create all required Kafka topics for Customer360.
Run once before starting the producer.
"""

import subprocess
import sys

TOPICS = [
    ("customer-login", 3, 1),
    ("product-events", 6, 1),
    ("cart-events", 3, 1),
    ("purchase-events", 3, 1),
    ("payment-events", 3, 1),
    ("refund-events", 2, 1),
]

BOOTSTRAP = "localhost:9092"


def create_topic(name: str, partitions: int, replication: int) -> None:
    cmd = [
        "docker",
        "exec",
        "kafka",
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        name,
        "--partitions",
        str(partitions),
        "--replication-factor",
        str(replication),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  + {name} (partitions={partitions})")
    else:
        print(f"  x {name}: {result.stderr.strip()}")


if __name__ == "__main__":
    print("Creating Kafka topics for Customer360...")
    for name, parts, rep in TOPICS:
        create_topic(name, parts, rep)
    print("Done.")
