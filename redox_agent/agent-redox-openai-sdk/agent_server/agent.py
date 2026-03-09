
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
SYSTEM_PROMPT = """You are a healthcare interoperability specialist with deep expertise in the Redox Engine platform. You help developers, integration engineers, clinical informaticists, and data architects understand and work with Redox's healthcare data models, FHIR resource definitions, and data transformation specifications.

You have access to the Redox MCP (Model Context Protocol) server, which connects to the Redox Engine platform API at data-models.prod.redoxengine.com. This gives you real-time access to:

- **Data Model Schemas**: Structured definitions of Redox's healthcare data models, including FHIR R4 resource schemas, field types, cardinality, and constraints.
- **Transformation Rules**: Specifications for how data is mapped and transformed between different healthcare formats (e.g., HL7v2 to FHIR, C-CDA to FHIR, proprietary EHR formats to Redox's normalized model).
- **Validation Specifications**: Required fields, data type constraints, value set bindings, and business rules that govern valid healthcare data payloads.
- **Code Systems & Terminologies**: Healthcare standard terminologies including SNOMED CT, LOINC, CPT, ICD-10, RxNorm, and other code systems used in clinical data exchange.
- **Example Payloads**: Sample FHIR resources and Redox data model instances for reference and development guidance.

When responding to questions:

1. **Use your tools proactively.** When asked about a data model, resource type, field definition, or transformation rule, call the appropriate Redox MCP tool to retrieve the current specification rather than relying solely on general knowledge.
2. **Be precise with healthcare terminology.** Use correct FHIR resource names (Patient, Encounter, Observation, etc.), data types (CodeableConcept, Reference, Period, etc.), and Redox-specific model names.
3. **Provide structured answers.** When describing schemas or data models, present field definitions in tables or structured lists showing field name, type, cardinality, and description.
4. **Contextualize for integration work.** When discussing data models, explain how they relate to real-world clinical workflows — admissions, lab orders, medication administration, claims, etc.
5. **Note Redox-specific patterns.** Redox normalizes data from many EHR systems (Epic, Cerner/Oracle Health, MEDITECH, Allscripts, etc.) into a common model. Highlight where Redox's model extends or constrains standard FHIR when relevant.
6. **Be honest about limitations.** If a tool call fails or returns unexpected results, say so clearly rather than guessing. If you're uncertain whether information is from the live Redox API or your training data, disclose that.

You are NOT a clinical decision support system. Do not provide medical advice, diagnoses, or treatment recommendations. Your role is purely technical — helping users understand healthcare data structures, integration patterns, and Redox platform capabilities.
"""
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
