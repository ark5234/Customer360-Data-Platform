"""
VectorDB Ingestion Script
Embeds and ingests support tickets into Qdrant for RAG.
"""

import json
import os
from langchain_community.vectorstores import Qdrant
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_core.documents import Document

def ingest_tickets(file_path: str = "data/tickets.json"):
    if not os.path.exists(file_path):
        print(f"File {file_path} not found. Run ticket_generator.py first.")
        return

    with open(file_path, "r") as f:
        tickets = json.load(f)

    documents = []
    for tkt in tickets:
        doc = Document(
            page_content=tkt["issue_text"],
            metadata={
                "ticket_id": tkt["ticket_id"],
                "customer_id": tkt["customer_id"],
                "status": tkt["status"],
                "created_at": tkt["created_at"]
            }
        )
        documents.append(doc)

    print(f"Loaded {len(documents)} documents.")

    # Initialize Embeddings (Requires GOOGLE_API_KEY)
    # Using local embeddings or dummy embeddings if key is not present?
    # For demonstration, we'll try GoogleGenerativeAIEmbeddings. If no key, we can fallback to HuggingFace
    api_key = os.getenv("GOOGLE_API_KEY", "")
    
    if api_key:
        embeddings = GoogleGenerativeAIEmbeddings(model="models/gemini-embedding-2")
    else:
        print("GOOGLE_API_KEY not found. Using HuggingFace embeddings as fallback.")
        from langchain_community.embeddings import HuggingFaceEmbeddings
        embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

    # Take a subset of documents to avoid hitting the free tier rate limits (100 per minute)
    documents = documents[:80]
    print(f"Subsampled to {len(documents)} documents to avoid Google Gemini free-tier rate limits.")
    
    qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
    print(f"Connecting to Qdrant at {qdrant_url}")
    
    qdrant = Qdrant.from_documents(
        documents,
        embeddings,
        url=qdrant_url,
        prefer_grpc=False,
        collection_name="support_tickets",
        force_recreate=True
    )
    
    print("Ingestion completed successfully.")

if __name__ == "__main__":
    ingest_tickets()
