import os
import warnings
from pathlib import Path

from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=DeprecationWarning)
try:
    from langgraph.prebuilt import create_react_agent
except ImportError:
    from langchain.agents import create_agent as create_react_agent

from .tools import TOOLS

# Load .env from project root (2 levels up from this file: agent/ -> admin_panel/ -> project root)
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(_env_path, override=True)


def get_agent():
    """Returns a compiled LangGraph agent, or None if no API key is configured."""
    api_key = os.getenv("GOOGLE_API_KEY", "").strip()
    
    if api_key and api_key not in ("", "your_google_api_key_here"):
        from langchain_google_genai import ChatGoogleGenerativeAI

        primary_llm = ChatGoogleGenerativeAI(model="gemini-3.5-flash", temperature=0)
        fallback_llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", temperature=0)
        llm = primary_llm.with_fallbacks([fallback_llm])
        # Create the ReAct agent graph with the LLM and the Tools
        agent = create_react_agent(llm, tools=TOOLS)
        return agent
    else:
        return None

