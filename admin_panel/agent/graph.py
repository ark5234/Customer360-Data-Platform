import os

from langgraph.prebuilt import create_react_agent

from .tools import TOOLS


def get_agent():
    """Returns a compiled LangGraph agent, or None if no API key is configured."""
    api_key = os.getenv("GOOGLE_API_KEY", "")
    if api_key:
        from langchain_google_genai import ChatGoogleGenerativeAI
        llm = ChatGoogleGenerativeAI(model="gemini-3.5-flash", temperature=0)
        # Create the ReAct agent graph with the LLM and the Tools
        agent = create_react_agent(llm, tools=TOOLS)
        return agent
    else:
        return None
