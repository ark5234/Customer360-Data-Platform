"""
Synthetic Support Ticket Generator
Generates mock unstructured text data for customer support interactions.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
import os

ISSUES = [
    "I cannot login to my account. It says wrong password but I just reset it.",
    "My delivery is delayed by 3 days. Can I get a refund for shipping?",
    "The product P{product_id} arrived damaged. The screen is cracked.",
    "How do I apply coupon code SAVE20?",
    "I want to cancel my pro subscription.",
    "The color of the t-shirt is different from what was shown on the website.",
    "Why was my credit card charged twice for order ORD-{order_id}?",
    "I did not receive the OTP for login.",
    "Can you suggest a good laptop under $1000?",
    "I want to return the running shoes, they don't fit well."
]

def generate_tickets(num_tickets: int = 500, output_file: str = "data/tickets.json"):
    tickets = []
    for _ in range(num_tickets):
        issue_template = random.choice(ISSUES)
        product_id = f"{random.randint(1, 50000):07d}"
        order_id = str(uuid.uuid4())[:8].upper()
        
        issue_text = issue_template.replace("{product_id}", product_id).replace("{order_id}", order_id)
        
        ticket = {
            "ticket_id": f"TKT-{str(uuid.uuid4())[:8].upper()}",
            "customer_id": f"C{random.randint(1, 1000):07d}",
            "issue_text": issue_text,
            "status": random.choice(["open", "closed", "in_progress"]),
            "created_at": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat()
        }
        tickets.append(ticket)
        
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(tickets, f, indent=2)
    print(f"Generated {num_tickets} support tickets in {output_file}")

if __name__ == "__main__":
    generate_tickets()
