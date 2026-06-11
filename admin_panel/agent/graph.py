import os
import warnings
from pathlib import Path

from dotenv import load_dotenv

from .tools import TOOLS

warnings.filterwarnings("ignore", category=DeprecationWarning)
from langchain.agents import create_agent  # noqa: E402

# Load .env from project root (2 levels up from this file: agent/ -> admin_panel/ -> project root)
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(_env_path, override=True)


def get_agent():
    """Returns a compiled LangGraph agent, or None if no API key is configured."""
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()

    if api_key and api_key not in ("", "your_google_api_key_here"):
        from langchain_google_genai import ChatGoogleGenerativeAI

        llm = ChatGoogleGenerativeAI(model="gemini-3.5-flash", temperature=0)
        # Create the ReAct agent graph with the LLM and the Tools
        agent = create_agent(llm, tools=TOOLS)
        return agent
    else:
        return None
