import os

from langchain_community.utilities.sql_database import SQLDatabase
from langchain_core.tools import tool


@tool
def search_customer_tickets(query: str, k: int = 3) -> str:
    """Search for unstructured customer interactions, reviews, or support tickets matching the query."""
    from langchain_qdrant import QdrantVectorStore

    api_key = os.getenv("GOOGLE_API_KEY", "")
    if api_key:
        from langchain_google_genai import GoogleGenerativeAIEmbeddings

        embeddings = GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-2")
    else:
        from langchain_huggingface import HuggingFaceEmbeddings

        embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

    qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
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
    Fact tables: fact_orders
    Dimension tables: dim_customer, dim_product
    """
    try:
        user = os.getenv("POSTGRES_USER", "customer360")
        password = os.getenv("POSTGRES_PASSWORD", "customer360secret")
        host = os.getenv("POSTGRES_HOST", "postgres")
        # Ensure we can run it locally as well as in docker
        if host == "localhost" or not os.getenv("POSTGRES_HOST"):
            host = "localhost"
        db = os.getenv("POSTGRES_DB", "customer360_warehouse")
        uri = f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"
        db_engine = SQLDatabase.from_uri(uri)
        result = db_engine.run(sql_query)
        return result
    except Exception as e:
        return f"Error executing query: {str(e)}"


TOOLS = [search_customer_tickets, query_warehouse]
