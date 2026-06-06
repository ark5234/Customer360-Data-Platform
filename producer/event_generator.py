"""
Customer360 Data Platform
Synthetic Event Generator — generates 10M+ customer events

Usage:
    python event_generator.py --events 10000000 --output ../data/synthetic/
    python event_generator.py --events 100000 --output ../data/synthetic/ --preview
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import click
import numpy as np
import pandas as pd
from faker import Faker
from loguru import logger
from tqdm import tqdm

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

fake = Faker("en_IN")  # Indian locale for realistic data
Faker.seed(42)
random.seed(42)
np.random.seed(42)

NUM_CUSTOMERS = 500_000
NUM_PRODUCTS = 50_000
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2026, 6, 1)

REGIONS = [
    "Maharashtra",
    "Karnataka",
    "Delhi",
    "Tamil Nadu",
    "West Bengal",
    "Telangana",
    "Gujarat",
    "Rajasthan",
    "Uttar Pradesh",
    "Kerala",
    "Punjab",
    "Madhya Pradesh",
    "Andhra Pradesh",
    "Haryana",
    "Bihar",
]

COUNTRIES = (
    ["India"] * 70
    + ["USA"] * 10
    + ["UK"] * 5
    + ["UAE"] * 5
    + ["Singapore"] * 4
    + ["Canada"] * 3
    + ["Australia"] * 3
)

DEVICES = ["Mobile", "Desktop", "Tablet", "App"]
DEVICE_WEIGHTS = [0.55, 0.25, 0.10, 0.10]

CATEGORIES = {
    "Electronics": [
        "Smartphones",
        "Laptops",
        "Headphones",
        "Cameras",
        "Tablets",
        "Smart Watches",
        "Gaming",
        "Accessories",
    ],
    "Fashion": [
        "Men's Clothing",
        "Women's Clothing",
        "Footwear",
        "Accessories",
        "Kids",
    ],
    "Home & Kitchen": ["Furniture", "Appliances", "Cookware", "Bedding", "Decor"],
    "Books": ["Fiction", "Non-Fiction", "Textbooks", "Comics", "Self-Help"],
    "Sports": ["Fitness Equipment", "Outdoor", "Team Sports", "Yoga", "Cycling"],
    "Beauty": ["Skincare", "Haircare", "Makeup", "Fragrances", "Men's Grooming"],
    "Grocery": ["Fresh Produce", "Dairy", "Snacks", "Beverages", "Organic"],
    "Automotive": ["Car Accessories", "Bike Accessories", "Tools", "Care Products"],
}

LOGIN_METHODS = ["password", "google", "facebook", "otp", "biometric"]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking", "wallet", "cod"]
PAYMENT_WEIGHTS = [0.20, 0.20, 0.35, 0.10, 0.10, 0.05]

SEARCH_TERMS = [
    "iphone",
    "laptop",
    "wireless earphones",
    "running shoes",
    "saree",
    "kurta",
    "smartwatch",
    "refrigerator",
    "washing machine",
    "camera",
    "gaming chair",
    "office desk",
    "yoga mat",
    "protein powder",
    "moisturizer",
    "face wash",
    "air purifier",
    "water purifier",
    "mixer grinder",
    "pressure cooker",
]

REFUND_REASONS = [
    "defective_product",
    "wrong_item_delivered",
    "not_as_described",
    "quality_issue",
    "changed_mind",
    "found_better_price",
    "damaged_packaging",
]

FAILURE_REASONS = [
    "insufficient_funds",
    "card_declined",
    "payment_timeout",
    "bank_server_error",
    "invalid_cvv",
    "fraud_detected",
    "account_blocked",
]

# ─────────────────────────────────────────────
# Customer & Product Pools
# ─────────────────────────────────────────────


def build_customer_pool(n: int) -> list[dict]:
    """Pre-generate customer master data."""
    logger.info(f"Building customer pool: {n:,} customers...")
    customers = []
    for i in range(n):
        cid = f"C{i + 1:07d}"
        region = random.choice(REGIONS)
        country = random.choice(COUNTRIES)
        customers.append(
            {
                "customer_id": cid,
                "region": region,
                "country": country,
                "city": fake.city(),
                "device": random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
                "ip_address": fake.ipv4(),
                "user_agent": fake.user_agent(),
                # Behavioral segments
                "segment": random.choices(
                    ["high_value", "medium_value", "low_value", "churned"],
                    weights=[0.10, 0.30, 0.45, 0.15],
                )[0],
                "avg_order_value": round(random.lognormvariate(8.5, 1.2), 2),
                "order_frequency": max(1, int(random.lognormvariate(2.0, 1.0))),
            }
        )
    logger.success(f"Customer pool ready: {n:,} customers")
    return customers


def build_product_pool(n: int) -> list[dict]:
    """Pre-generate product catalog."""
    logger.info(f"Building product pool: {n:,} products...")
    products = []
    for i in range(n):
        category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        # Realistic price distribution per category
        base_prices = {
            "Electronics": (5000, 150000),
            "Fashion": (299, 15000),
            "Home & Kitchen": (499, 80000),
            "Books": (99, 2000),
            "Sports": (299, 50000),
            "Beauty": (99, 5000),
            "Grocery": (49, 2000),
            "Automotive": (199, 30000),
        }
        lo, hi = base_prices[category]
        price = round(random.uniform(lo, hi), 2)

        products.append(
            {
                "product_id": f"P{i + 1:07d}",
                "product_name": f"{fake.word().title()} {subcategory}",
                "category": category,
                "subcategory": subcategory,
                "price": price,
                "brand": fake.company(),
                "rating": round(random.uniform(2.5, 5.0), 1),
                "review_count": random.randint(0, 50000),
            }
        )
    logger.success(f"Product pool ready: {n:,} products")
    return products


# ─────────────────────────────────────────────
# Event Generators
# ─────────────────────────────────────────────


def random_timestamp() -> datetime:
    delta = END_DATE - START_DATE
    return START_DATE + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def make_base(customer: dict, event_type: str, topic: str) -> dict:
    ts = random_timestamp()
    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer["customer_id"],
        "event_type": event_type,
        "session_id": str(uuid.uuid4())[:8],
        "device": customer["device"],
        "region": customer["region"],
        "country": customer["country"],
        "city": customer["city"],
        "ip_address": customer["ip_address"],
        "user_agent": customer["user_agent"],
        "timestamp": ts.isoformat(),
        "kafka_topic": topic,
        "ingestion_time": datetime.utcnow().isoformat(),
        "schema_version": "1.0",
    }


def gen_login(customer: dict) -> dict:
    evt = make_base(customer, "LOGIN", "customer-login")
    method = random.choice(LOGIN_METHODS)
    success = random.random() > 0.05  # 95% success rate
    evt.update(
        {
            "login_method": method,
            "success": success,
            "failure_reason": None
            if success
            else random.choice(
                ["wrong_password", "account_locked", "otp_expired", "too_many_attempts"]
            ),
        }
    )
    return evt


def gen_logout(customer: dict) -> dict:
    evt = make_base(customer, "LOGOUT", "customer-login")
    evt["session_duration_seconds"] = random.randint(30, 7200)
    return evt


def gen_product_view(customer: dict, product: dict) -> dict:
    evt = make_base(customer, "PRODUCT_VIEW", "product-events")
    evt.update(
        {
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "subcategory": product["subcategory"],
            "price": product["price"],
            "brand": product["brand"],
            "rating": product["rating"],
            "view_duration_seconds": random.randint(5, 600),
            "referrer": random.choice(
                ["search", "home", "email", "social", "direct", None]
            ),
            "images_viewed": random.randint(1, 12),
        }
    )
    return evt


def gen_search(customer: dict) -> dict:
    evt = make_base(customer, "SEARCH", "product-events")
    query = random.choice(SEARCH_TERMS)
    results = random.randint(0, 5000)
    clicked = (
        random.randint(1, min(results, 20))
        if results > 0 and random.random() > 0.3
        else None
    )
    evt.update(
        {
            "search_query": query,
            "results_count": results,
            "clicked_position": clicked,
            "clicked_product_id": f"P{random.randint(1, NUM_PRODUCTS):07d}"
            if clicked
            else None,
            "filters_applied": random.choice(
                [None, "price", "rating", "brand", "category"]
            ),
        }
    )
    return evt


def gen_add_to_cart(customer: dict, product: dict) -> dict:
    evt = make_base(customer, "ADD_TO_CART", "cart-events")
    qty = random.randint(1, 5)
    evt.update(
        {
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "price": product["price"],
            "quantity": qty,
            "cart_total": round(product["price"] * qty * random.uniform(1.0, 3.0), 2),
            "is_wishlist_item": random.random() > 0.8,
        }
    )
    return evt


def gen_purchase(customer: dict, products: list[dict]) -> dict:
    evt = make_base(customer, "PURCHASE", "purchase-events")
    n_items = random.randint(1, 8)
    selected = random.sample(products, min(n_items, len(products)))
    subtotal = sum(p["price"] * random.randint(1, 3) for p in selected)
    discount = round(subtotal * random.uniform(0, 0.30), 2)
    tax = round((subtotal - discount) * 0.18, 2)
    total = round(subtotal - discount + tax, 2)
    order_id = f"ORD-{str(uuid.uuid4())[:8].upper()}"
    evt.update(
        {
            "order_id": order_id,
            "product_ids": [p["product_id"] for p in selected],
            "product_names": [p["product_name"] for p in selected],
            "categories": list({p["category"] for p in selected}),
            "subtotal_amount": round(subtotal, 2),
            "discount_amount": discount,
            "tax_amount": tax,
            "total_amount": total,
            "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[
                0
            ],
            "items_count": len(selected),
            "coupon_code": f"SAVE{random.randint(10, 50)}"
            if random.random() > 0.7
            else None,
            "delivery_type": random.choice(["standard", "express", "same_day"]),
            "estimated_delivery_days": random.randint(1, 7),
        }
    )
    return evt


def gen_refund(customer: dict) -> dict:
    evt = make_base(customer, "REFUND", "refund-events")
    evt.update(
        {
            "order_id": f"ORD-{str(uuid.uuid4())[:8].upper()}",
            "product_id": f"P{random.randint(1, NUM_PRODUCTS):07d}",
            "refund_amount": round(random.uniform(100, 50000), 2),
            "reason": random.choice(REFUND_REASONS),
            "initiated_by": random.choices(
                ["customer", "seller", "system"], weights=[0.7, 0.2, 0.1]
            )[0],
            "resolution_days": random.randint(1, 7),
        }
    )
    return evt


def gen_subscription(customer: dict) -> dict:
    evt = make_base(customer, "SUBSCRIPTION", "payment-events")
    plan = random.choice(["free", "basic", "pro", "enterprise"])
    prices = {"free": 0, "basic": 99, "pro": 499, "enterprise": 1999}
    evt.update(
        {
            "plan": plan,
            "action": random.choices(
                ["subscribe", "upgrade", "downgrade", "cancel"],
                weights=[0.50, 0.20, 0.15, 0.15],
            )[0],
            "amount": prices[plan],
            "billing_cycle": random.choice(["monthly", "annual"]),
            "trial_days_remaining": random.randint(0, 30) if plan == "free" else None,
        }
    )
    return evt


def gen_payment_failure(customer: dict) -> dict:
    evt = make_base(customer, "PAYMENT_FAILURE", "payment-events")
    evt.update(
        {
            "order_id": f"ORD-{str(uuid.uuid4())[:8].upper()}",
            "attempted_amount": round(random.uniform(100, 100000), 2),
            "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[
                0
            ],
            "failure_reason": random.choice(FAILURE_REASONS),
            "retry_count": random.randint(0, 3),
            "bank_error_code": f"ERR{random.randint(1000, 9999)}",
        }
    )
    return evt


# ─────────────────────────────────────────────
# Event Distribution
# ─────────────────────────────────────────────

EVENT_DISTRIBUTION = {
    "product_view": 0.30,
    "search": 0.20,
    "login": 0.15,
    "add_to_cart": 0.12,
    "logout": 0.10,
    "purchase": 0.07,
    "payment_failure": 0.03,
    "refund": 0.02,
    "subscription": 0.01,
}


def generate_batch(
    customers: list[dict],
    products: list[dict],
    batch_size: int,
) -> list[dict]:
    """Generate a batch of events following realistic distribution."""
    events = []
    choices = random.choices(
        list(EVENT_DISTRIBUTION.keys()),
        weights=list(EVENT_DISTRIBUTION.values()),
        k=batch_size,
    )
    for etype in choices:
        customer = random.choice(customers)
        product = random.choice(products)

        if etype == "login":
            events.append(gen_login(customer))
        elif etype == "logout":
            events.append(gen_logout(customer))
        elif etype == "product_view":
            events.append(gen_product_view(customer, product))
        elif etype == "search":
            events.append(gen_search(customer))
        elif etype == "add_to_cart":
            events.append(gen_add_to_cart(customer, product))
        elif etype == "purchase":
            events.append(
                gen_purchase(customer, random.sample(products, random.randint(1, 5)))
            )
        elif etype == "refund":
            events.append(gen_refund(customer))
        elif etype == "subscription":
            events.append(gen_subscription(customer))
        elif etype == "payment_failure":
            events.append(gen_payment_failure(customer))

    return events


# ─────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────


@click.command()
@click.option("--events", default=1_000_000, help="Total events to generate")
@click.option("--output", default="../data/synthetic/", help="Output directory")
@click.option("--batch-size", default=10_000, help="Events per batch / file")
@click.option(
    "--format", "fmt", default="parquet", type=click.Choice(["parquet", "json", "csv"])
)
@click.option("--preview", is_flag=True, help="Print 5 sample events and exit")
@click.option("--customers", default=NUM_CUSTOMERS, help="Customer pool size")
@click.option("--products", default=NUM_PRODUCTS, help="Product catalog size")
def main(
    events: int,
    output: str,
    batch_size: int,
    fmt: str,
    preview: bool,
    customers: int,
    products: int,
):
    """Generate synthetic customer events for Customer360 Data Platform."""

    logger.info("=" * 60)
    logger.info("  Customer360 — Synthetic Event Generator")
    logger.info("=" * 60)
    logger.info(f"  Events      : {events:,}")
    logger.info(f"  Output      : {output}")
    logger.info(f"  Format      : {fmt}")
    logger.info(f"  Batch size  : {batch_size:,}")
    logger.info("=" * 60)

    # Build data pools
    customer_pool = build_customer_pool(customers)
    product_pool = build_product_pool(products)

    if preview:
        sample = generate_batch(customer_pool[:100], product_pool[:100], 5)
        for evt in sample:
            print(json.dumps(evt, indent=2, default=str))
        return

    # Create output directory
    out_path = Path(output)
    out_path.mkdir(parents=True, exist_ok=True)

    # Generate events in batches
    total_generated = 0
    batch_num = 0
    start_time = time.time()

    with tqdm(
        total=events, desc="Generating events", unit="evt", colour="green"
    ) as pbar:
        while total_generated < events:
            current_batch = min(batch_size, events - total_generated)
            batch = generate_batch(customer_pool, product_pool, current_batch)

            # Partition by event_type for efficiency
            df = pd.DataFrame(batch)

            if fmt == "parquet":
                file_path = out_path / f"events_batch_{batch_num:05d}.parquet"
                df.to_parquet(file_path, index=False, engine="pyarrow")
            elif fmt == "json":
                file_path = out_path / f"events_batch_{batch_num:05d}.jsonl"
                df.to_json(file_path, orient="records", lines=True)
            else:
                file_path = out_path / f"events_batch_{batch_num:05d}.csv"
                df.to_csv(file_path, index=False)

            total_generated += len(batch)
            batch_num += 1
            pbar.update(len(batch))

    elapsed = time.time() - start_time
    rate = total_generated / elapsed

    logger.success("=" * 60)
    logger.success(f"  Events generated : {total_generated:,}")
    logger.success(f"  Batches written  : {batch_num}")
    logger.success(f"  Time elapsed     : {elapsed:.1f}s")
    logger.success(f"  Throughput       : {rate:,.0f} events/sec")
    logger.success(f"  Output directory : {out_path.absolute()}")
    logger.success("=" * 60)


if __name__ == "__main__":
    main()
