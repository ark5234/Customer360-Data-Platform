"""
Customer360 Data Platform
Event Schemas — Pydantic models for all customer event types
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, validator

# ─────────────────────────────────────────────
# Enumerations
# ─────────────────────────────────────────────


class EventType(str, Enum):
    LOGIN = "LOGIN"
    LOGOUT = "LOGOUT"
    PRODUCT_VIEW = "PRODUCT_VIEW"
    SEARCH = "SEARCH"
    ADD_TO_CART = "ADD_TO_CART"
    PURCHASE = "PURCHASE"
    REFUND = "REFUND"
    SUBSCRIPTION = "SUBSCRIPTION"
    PAYMENT_FAILURE = "PAYMENT_FAILURE"


class DeviceType(str, Enum):
    MOBILE = "Mobile"
    DESKTOP = "Desktop"
    TABLET = "Tablet"
    APP = "App"


class PaymentMethod(str, Enum):
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    UPI = "upi"
    NET_BANKING = "net_banking"
    WALLET = "wallet"
    COD = "cod"


class SubscriptionPlan(str, Enum):
    FREE = "free"
    BASIC = "basic"
    PRO = "pro"
    ENTERPRISE = "enterprise"


# ─────────────────────────────────────────────
# Base Event
# ─────────────────────────────────────────────


class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    event_type: EventType
    session_id: str
    device: DeviceType
    region: str
    country: str
    city: str
    ip_address: str
    user_agent: str
    timestamp: datetime
    kafka_topic: str

    class Config:
        use_enum_values = True


# ─────────────────────────────────────────────
# Specific Event Types
# ─────────────────────────────────────────────


class LoginEvent(BaseEvent):
    event_type: EventType = EventType.LOGIN
    kafka_topic: str = "customer-login"
    login_method: str  # password, google, facebook, otp
    success: bool
    failure_reason: Optional[str] = None


class LogoutEvent(BaseEvent):
    event_type: EventType = EventType.LOGOUT
    kafka_topic: str = "customer-login"
    session_duration_seconds: int


class ProductViewEvent(BaseEvent):
    event_type: EventType = EventType.PRODUCT_VIEW
    kafka_topic: str = "product-events"
    product_id: str
    product_name: str
    category: str
    subcategory: str
    price: float
    view_duration_seconds: int
    referrer: Optional[str] = None


class SearchEvent(BaseEvent):
    event_type: EventType = EventType.SEARCH
    kafka_topic: str = "product-events"
    search_query: str
    results_count: int
    clicked_position: Optional[int] = None
    clicked_product_id: Optional[str] = None


class AddToCartEvent(BaseEvent):
    event_type: EventType = EventType.ADD_TO_CART
    kafka_topic: str = "cart-events"
    product_id: str
    product_name: str
    category: str
    price: float
    quantity: int
    cart_total: float


class PurchaseEvent(BaseEvent):
    event_type: EventType = EventType.PURCHASE
    kafka_topic: str = "purchase-events"
    order_id: str = Field(
        default_factory=lambda: f"ORD-{str(uuid.uuid4())[:8].upper()}"
    )
    product_ids: list[str]
    total_amount: float
    discount_amount: float
    tax_amount: float
    payment_method: PaymentMethod
    items_count: int
    coupon_code: Optional[str] = None


class RefundEvent(BaseEvent):
    event_type: EventType = EventType.REFUND
    kafka_topic: str = "refund-events"
    order_id: str
    product_id: str
    refund_amount: float
    reason: str  # defective, wrong_item, not_needed, quality_issue
    initiated_by: str  # customer, seller, system


class SubscriptionEvent(BaseEvent):
    event_type: EventType = EventType.SUBSCRIPTION
    kafka_topic: str = "payment-events"
    plan: SubscriptionPlan
    action: str  # subscribe, upgrade, downgrade, cancel
    amount: float
    billing_cycle: str  # monthly, annual


class PaymentFailureEvent(BaseEvent):
    event_type: EventType = EventType.PAYMENT_FAILURE
    kafka_topic: str = "payment-events"
    order_id: str
    attempted_amount: float
    payment_method: PaymentMethod
    failure_reason: str  # insufficient_funds, card_declined, timeout, fraud_detected
    retry_count: int
