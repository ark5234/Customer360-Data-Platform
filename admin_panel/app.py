"""
Customer360 Data Platform - Admin Control Panel
Flask web application for monitoring and controlling the data platform
"""

import json
import os
import subprocess
from datetime import UTC, datetime

import psycopg2
import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

# Load environment variables from .env file in the project root
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
load_dotenv(env_path)

app = Flask(__name__)


# Database connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "customer360_warehouse"),
        user=os.getenv("POSTGRES_USER", "customer360"),
        password=os.getenv("POSTGRES_PASSWORD", "customer360secret"),
    )


# ============================================
# DASHBOARD ROUTES
# ============================================


@app.route("/")
def dashboard():
    """Main dashboard page"""
    return render_template("dashboard.html")


@app.route("/api/stats")
def get_stats():
    """Get quick stats for dashboard cards"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Total orders
        cursor.execute("SELECT COUNT(*) FROM fact_orders")
        row = cursor.fetchone()
        total_orders = row[0] if row else 0

        # Total revenue
        cursor.execute("SELECT COALESCE(SUM(total_amount), 0) FROM fact_orders")
        row = cursor.fetchone()
        total_revenue = float(row[0]) if row and row[0] else 0.0

        # Active customers (last 30 days)
        cursor.execute("""
            SELECT COUNT(DISTINCT customer_id) FROM fact_orders
            WHERE event_timestamp >= NOW() - INTERVAL '30 days'
        """)
        row = cursor.fetchone()
        active_customers = row[0] if row else 0

        # High-risk churn customers
        cursor.execute("""
            SELECT COUNT(*) FROM customer_churn_scores
            WHERE churn_segment = 'high_risk'
        """)
        row = cursor.fetchone()
        high_risk = row[0] if row and row[0] else 0

        cursor.close()
        conn.close()

        return jsonify(
            {
                "total_orders": total_orders,
                "total_revenue": round(total_revenue, 2),
                "active_customers": active_customers,
                "high_risk_churn": high_risk,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/kafka/metrics")
def kafka_metrics():
    """Get Kafka metrics"""
    try:
        # In a real setup, you'd query Kafka metrics from JMX or Prometheus
        # For now, return mock data
        return jsonify(
            {
                "topics": [
                    {"name": "customer-login", "messages": 125000, "lag": 450},
                    {"name": "product-events", "messages": 450000, "lag": 1200},
                    {"name": "cart-events", "messages": 180000, "lag": 800},
                    {"name": "purchase-events", "messages": 95000, "lag": 150},
                    {"name": "payment-events", "messages": 45000, "lag": 50},
                    {"name": "refund-events", "messages": 12000, "lag": 20},
                ],
                "total_messages": 907000,
                "total_lag": 2670,
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/warehouse/tables")
def warehouse_tables():
    """Get warehouse table row counts"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        tables = [
            "fact_orders",
            "fact_transactions",
            "dim_customer",
            "revenue_metrics",
            "customer_churn_scores",
            "feature_store",
        ]

        result = []
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row = cursor.fetchone()
                count = row[0] if row else 0
                result.append({"table": table, "rows": count})
            except Exception:
                result.append({"table": table, "rows": 0})

        cursor.close()
        conn.close()

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# CUSTOMER SEARCH
# ============================================


@app.route("/api/customer/<customer_id>")
def get_customer(customer_id):
    """Get customer 360 view"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Customer profile
        cursor.execute(
            """
            SELECT customer_id, region, total_events, total_purchases,
                   total_spend, avg_spend, last_seen_date
            FROM dim_customer WHERE customer_id = %s
        """,
            (customer_id,),
        )

        customer = cursor.fetchone()
        if not customer:
            return jsonify({"error": "Customer not found"}), 404

        # Recent orders
        cursor.execute(
            """
            SELECT order_id, event_timestamp, total_amount, payment_method
            FROM fact_orders WHERE customer_id = %s
            ORDER BY event_timestamp DESC LIMIT 10
        """,
            (customer_id,),
        )
        orders = cursor.fetchall()

        # Churn score
        cursor.execute(
            """
            SELECT churn_probability, churn_segment, scored_at
            FROM customer_churn_scores WHERE customer_id = %s
        """,
            (customer_id,),
        )
        churn = cursor.fetchone()

        cursor.close()
        conn.close()

        return jsonify(
            {
                "customer": {
                    "customer_id": customer[0],
                    "region": customer[1],
                    "total_events": customer[2],
                    "total_purchases": customer[3],
                    "total_spend": float(customer[4]) if customer[4] else 0,
                    "avg_spend": float(customer[5]) if customer[5] else 0,
                    "last_seen": customer[6].isoformat() if customer[6] else None,
                },
                "recent_orders": [
                    {
                        "order_id": o[0],
                        "timestamp": o[1].isoformat() if o[1] else None,
                        "amount": float(o[2]) if o[2] else 0,
                        "payment_method": o[3],
                    }
                    for o in orders
                ],
                "churn": {
                    "probability": float(churn[0]) if churn else 0,
                    "segment": churn[1] if churn else "unknown",
                    "scored_at": churn[2].isoformat() if churn and churn[2] else None,
                }
                if churn
                else None,
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# DATA QUALITY
# ============================================


@app.route("/api/data-quality")
def data_quality():
    """Get latest data quality check results"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT rule_name, table_name, total_records, failed_records,
                   failure_rate, passed, severity, checked_at
            FROM data_quality_results
            ORDER BY checked_at DESC LIMIT 20
        """)

        results = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify(
            [
                {
                    "rule_name": r[0],
                    "table_name": r[1],
                    "total_records": r[2],
                    "failed_records": r[3],
                    "failure_rate": float(r[4]) if r[4] else 0,
                    "passed": r[5],
                    "severity": r[6],
                    "checked_at": r[7].isoformat() if r[7] else None,
                }
                for r in results
            ]
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# ML MODEL MANAGEMENT
# ============================================


@app.route("/api/ml/model-info")
def model_info():
    """Get ML model information"""
    try:
        model_metrics_path = os.path.join(
            os.path.dirname(__file__), "../ml/models/churn_model_latest_metrics.json"
        )

        if os.path.exists(model_metrics_path):
            with open(model_metrics_path) as f:
                metrics = json.load(f)
            return jsonify(metrics)
        else:
            return jsonify({"error": "Model not trained yet"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ml/train", methods=["POST"])
def trigger_model_training():
    """Trigger model training"""
    try:
        # In production, this would trigger an Airflow DAG
        # For now, we'll just return a success message
        return jsonify(
            {
                "status": "success",
                "message": "Model training triggered. Check Airflow DAG: dag_model_retraining",
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ml/score/<customer_id>", methods=["POST"])
def score_customer(customer_id):
    """Score a single customer for churn"""
    try:
        # In a real implementation, this would call the ML model
        return jsonify(
            {
                "customer_id": customer_id,
                "churn_probability": 0.42,
                "churn_segment": "medium_risk",
                "scored_at": datetime.now(UTC).isoformat(),
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# AIRFLOW DAG TRIGGERS
# ============================================


@app.route("/api/airflow/dags")
def list_dags():
    """List all Airflow DAGs"""
    dags = [
        {
            "dag_id": "dag_kafka_to_bronze",
            "schedule": "*/5 * * * *",
            "status": "active",
        },
        {
            "dag_id": "dag_bronze_to_silver",
            "schedule": "*/30 * * * *",
            "status": "active",
        },
        {"dag_id": "dag_silver_to_gold", "schedule": "@hourly", "status": "active"},
        {
            "dag_id": "dag_gold_to_warehouse",
            "schedule": "0 */2 * * *",
            "status": "active",
        },
        {"dag_id": "dag_feature_engineering", "schedule": "@daily", "status": "active"},
        {"dag_id": "dag_model_retraining", "schedule": "0 2 * * 0", "status": "active"},
    ]
    return jsonify(dags)


@app.route("/api/airflow/trigger/<dag_id>", methods=["POST"])
def trigger_dag(dag_id):
    """Trigger an Airflow DAG"""
    try:
        response = requests.post(
            f"http://localhost:8081/api/v1/dags/{dag_id}/dagRuns",
            auth=("admin", "admin"),
            json={},
            timeout=5
        )
        if response.status_code == 200:
            return jsonify(
                {
                    "status": "success",
                    "message": f"DAG {dag_id} triggered successfully",
                    "dag_id": dag_id,
                    "triggered_at": datetime.now(UTC).isoformat(),
                }
            )
        else:
            return jsonify({"error": f"Failed to trigger DAG: {response.text}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# PIPELINE CONTROLS
# ============================================


@app.route("/api/producer/status")
def producer_status():
    """Get Kafka producer status"""
    # Check if producer process is running
    try:
        result = subprocess.run(
            ["pgrep", "-f", "kafka_producer.py"], capture_output=True, text=True
        )
        is_running = bool(result.stdout.strip())

        return jsonify(
            {
                "status": "running" if is_running else "stopped",
                "checked_at": datetime.now(UTC).isoformat(),
            }
        )
    except Exception:
        return jsonify({"status": "unknown"})


@app.route("/api/producer/control", methods=["POST"])
def control_producer():
    """Start/stop Kafka producer"""
    action = request.json.get("action")  # 'start' or 'stop'

    if action == "start":
        # In production, you'd start the producer process
        return jsonify({"status": "success", "message": "Producer started"})
    elif action == "stop":
        # In production, you'd stop the producer process
        return jsonify({"status": "success", "message": "Producer stopped"})
    else:
        return jsonify({"error": "Invalid action"}), 400


# ============================================
# LLM AI ASSISTANT (RAG & SQL)
# ============================================


@app.route("/api/chat", methods=["POST"])
def chat():
    """AI Assistant Chat Endpoint"""
    try:
        data = request.json
        query = data.get("query")
        if not query:
            return jsonify({"error": "Query is required"}), 400

        try:
            from agent.graph import get_agent
        except ImportError as e:
            try:
                from admin_panel.agent.graph import get_agent
            except ImportError as e2:
                return jsonify({"error": f"ImportError 1: {str(e)} | ImportError 2: {str(e2)}"}), 500

        from langchain_core.messages import HumanMessage

        agent = get_agent()
        if not agent:
            # Mock response if GOOGLE_API_KEY is not configured
            return jsonify(
                {
                    "response": "The AI assistant is currently in demo mode. "
                    "Please configure GOOGLE_API_KEY in the environment to enable LangChain/LangGraph capabilities."
                }
            )

        # Invoke the LangGraph agent
        final_state = agent.invoke({"messages": [HumanMessage(content=query)]})
        last_msg = final_state["messages"][-1]
        response_msg = last_msg.content

        if isinstance(response_msg, list):
            # Extract text from list of dicts if Gemini returns structured content
            texts = [item.get("text", "") for item in response_msg if isinstance(item, dict) and "text" in item]
            if texts:
                response_msg = " ".join(texts)
            else:
                response_msg = str(response_msg)

        if not response_msg:
            # Sometimes the LLM returns an empty string or just tool calls
            tool_calls = getattr(last_msg, "tool_calls", [])
            if tool_calls:
                response_msg = f"I am trying to use a tool to answer this, but ran into an issue. Tool calls: {tool_calls}"
            else:
                response_msg = "No text response was generated by the model."

        return jsonify({"response": str(response_msg)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
