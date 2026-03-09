
from agents.mcp import MCPServer, MCPServerManager
from typing import AsyncGenerator, List

import mlflow
from agents import Agent, Runner, set_default_openai_api, set_default_openai_client
from agents.tracing import set_trace_processors
from databricks_openai import AsyncDatabricksOpenAI
from databricks_openai.agents import McpServer
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from agent_server.utils import (
    build_mcp_url,
    get_user_workspace_client,
    process_agent_stream_events,
)

# NOTE: this will work for all databricks models OTHER than GPT-OSS, which uses a slightly different API
set_default_openai_client(AsyncDatabricksOpenAI())
set_default_openai_api("chat_completions")
set_trace_processors([])  # only use mlflow for trace processing
mlflow.openai.autolog()

# GENERATED

NAME = 'agent-redox'
SYSTEM_PROMPT = 'You are a helpful assistant.'
MODEL = 'databricks-claude-opus-4-6'
MCP_SERVERS = [
    ('mcp-redox', 'https://mcp-redox-7474657999482942.aws.databricksapps.com'),
]

# END GENERATED

def init_mcp_servers():
    return [McpServer(name=name, url=build_mcp_url(url)) for (name, url) in MCP_SERVERS]

def create_agent(mcp_servers: List[MCPServer]) -> Agent:
    return Agent(
        name=NAME,
        instructions=SYSTEM_PROMPT,
        model=MODEL,
        mcp_servers=mcp_servers,
    )


@invoke()
async def invoke(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    # Optionally use the user's workspace client for on-behalf-of authentication
    # user_workspace_client = get_user_workspace_client()
    mcp_servers = init_mcp_servers()
    async with MCPServerManager(servers = mcp_servers, connect_in_parallel=True) as manager:
        agent = create_agent(manager.active_servers)
        messages = [i.model_dump() for i in request.input]
        result = await Runner.run(agent, messages)
        return ResponsesAgentResponse(output=[item.to_input_item() for item in result.new_items])


@stream()
async def stream(request: dict) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    # Optionally use the user's workspace client for on-behalf-of authentication
    # user_workspace_client = get_user_workspace_client()
    mcp_servers = init_mcp_servers()
    async with MCPServerManager(servers = mcp_servers, connect_in_parallel=True) as manager:
        agent = create_agent(manager.active_servers)
        messages = [i.model_dump() for i in request.input]
        result = Runner.run_streamed(agent, input=messages)

        async for event in process_agent_stream_events(result.stream_events()):
            yield event
