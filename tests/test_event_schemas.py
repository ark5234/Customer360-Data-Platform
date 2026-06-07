import pytest
from pydantic import ValidationError
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from producer.schemas import LoginEvent, PurchaseEvent

def test_valid_login_event():
    """Test valid login event schema."""
    event = LoginEvent(
        event_id="E123",
        customer_id="C456",
        timestamp="2026-06-07T12:00:00Z",
        device="Mobile",
        region="Mumbai",
        success=True
    )
    assert event.event_type == "LOGIN"
    assert event.device == "Mobile"

def test_valid_purchase_event():
    """Test valid purchase event schema."""
    event = PurchaseEvent(
        event_id="E789",
        customer_id="C456",
        timestamp="2026-06-07T12:05:00Z",
        device="Desktop",
        region="Delhi",
        order_id="O123",
        total_amount=100.50,
        items_count=2,
        payment_method="UPI"
    )
    assert event.event_type == "PURCHASE"
    assert event.total_amount == 100.50

def test_invalid_event():
    """Test invalid event schema raises error."""
    with pytest.raises(ValidationError):
        # Missing required fields
        LoginEvent(event_id="E123")
