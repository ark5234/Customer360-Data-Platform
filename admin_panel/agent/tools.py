import os
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.tools import tool

# Load .env from project root
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(_env_path, override=True)


@tool
def search_customer_tickets(query: str, k: int = 3) -> str:
    """Search for unstructured customer interactions, reviews, or support tickets matching the query."""
    from langchain_qdrant import QdrantVectorStore

    api_key = os.getenv("GOOGLE_API_KEY", "")
    api_key = "" # Force fallback
    if api_key:
        from langchain_google_genai import GoogleGenerativeAIEmbeddings

        embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
    else:
        from langchain_huggingface import HuggingFaceEmbeddings

        embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

    qdrant_url = os.getenv("QDRANT_URL", "http://127.0.0.1:6333").replace("localhost", "127.0.0.1")
    try:
        from qdrant_client import QdrantClient

        client = QdrantClient(url=qdrant_url)
        qdrant = QdrantVectorStore(
            client=client, collection_name="support_tickets", embedding=embeddings
        )
        docs = qdrant.similarity_search(query, k=k)
        if not docs:
            return "No matching support tickets found."

        results = []
        for d in docs:
            results.append(
                f"Ticket ID: {d.metadata.get('ticket_id')} | Status: {d.metadata.get('status')} | Issue: {d.page_content}"
            )
        return "\n".join(results)
    except Exception as e:
        return f"Error connecting to VectorDB: {str(e)}"


@tool
def query_warehouse(sql_query: str) -> str:
    """Execute a raw SQL query on the PostgreSQL data warehouse and return the result.
    Useful for getting metrics like LTV, churn, average order value.
    Available Tables:
    - fact_orders
    - fact_transactions
    - dim_customer
    - dim_product
    - revenue_metrics
    - customer_churn_scores (columns: customer_id, churn_probability, churn_segment, scored_at)
    - feature_store
    """
    try:
        user = os.getenv("POSTGRES_USER", "customer360")
        password = os.getenv("POSTGRES_PASSWORD", "customer360secret")
        host = os.getenv("POSTGRES_HOST", "postgres")
        # Ensure we can run it locally as well as in docker
        if host == "localhost" or not os.getenv("POSTGRES_HOST"):
            host = "127.0.0.1"
        db = os.getenv("POSTGRES_DB", "customer360_warehouse")
        uri = f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"
        import psycopg2
        conn = psycopg2.connect(host=host, port=5432, dbname=db, user=user, password=password)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return str(result)
    except Exception as e:
        return f"Error executing query: {str(e)}"


TOOLS = [search_customer_tickets, query_warehouse]
