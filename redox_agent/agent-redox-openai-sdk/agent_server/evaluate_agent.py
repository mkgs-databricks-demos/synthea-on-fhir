import asyncio
import logging

import mlflow
from dotenv import load_dotenv
from mlflow.genai.agent_server import get_invoke_function
from mlflow.genai.scorers import (
    Completeness,
    ConversationalSafety,
    ConversationCompleteness,
    Fluency,
    KnowledgeRetention,
    RelevanceToQuery,
    Safety,
    ToolCallCorrectness,
    UserFrustration,
)
from mlflow.genai.simulators import ConversationSimulator
from mlflow.types.responses import ResponsesAgentRequest

# Load environment variables from .env if it exists
load_dotenv(dotenv_path=".env", override=True)
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)

# need to import agent for our @invoke-registered function to be found
from agent_server import agent  # noqa: F401

# Create your evaluation dataset
# Refer to documentation for evaluations:
# Scorers: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/scorers
# Predefined LLM scorers: https://mlflow.org/docs/latest/genai/eval-monitor/scorers/llm-judge/predefined
# Defining custom scorers: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/custom-scorers

# ---------------------------------------------------------------------------
# Redox Engine Platform API – Operational Evaluation Scenarios
# Each test case targets a specific Redox platform workflow that the agent
# should be able to accomplish via its MCP tools.
# ---------------------------------------------------------------------------
test_cases = [
    # 1. List environments
    {
        "goal": "Discover what Redox environments are available in the organization",
        "persona": "An integration engineer onboarding onto a new Redox project. You need to see what environments exist before doing any configuration.",
        "simulation_guidelines": [
            "Ask the agent to list all available Redox environments.",
            "Ask what the difference is between the environments returned (e.g., development vs staging vs production).",
            "Prefer short, direct questions.",
        ],
    },
    # 2. Set the active environment
    {
        "goal": "Select a specific Redox environment and confirm it is active for subsequent operations",
        "persona": "An integration engineer who has just reviewed the environment list and wants to work in a development or sandbox environment.",
        "simulation_guidelines": [
            "Ask the agent to set the active environment to a development or sandbox environment.",
            "Confirm which environment is now active.",
            "Ask whether subsequent tool calls will use the selected environment.",
        ],
    },
    # 3. List destinations
    {
        "goal": "List the destinations configured in the current Redox environment",
        "persona": "A technical project manager reviewing existing integration endpoints before adding a new one.",
        "simulation_guidelines": [
            "Ask the agent to list all destinations in the current environment.",
            "Ask for details about one of the destinations returned (e.g., its name, ID, status).",
            "Ask what types of destinations Redox supports.",
        ],
    },
    # 4. Create a new destination
    {
        "goal": "Create a new destination in Redox for receiving clinical data",
        "persona": "A developer setting up a new integration endpoint for a partner health system.",
        "simulation_guidelines": [
            "Ask the agent to create a new destination with a descriptive name like 'Test Clinic Inbound'.",
            "Ask what parameters are required to create a destination.",
            "Confirm the destination was created and ask for its ID.",
        ],
    },
    # 5. List available test files
    {
        "goal": "Discover what test files or sample payloads are available to send to a destination",
        "persona": "A QA engineer preparing to test a newly created destination by sending sample data.",
        "simulation_guidelines": [
            "Ask the agent to list available test files that can be sent to a destination.",
            "Ask what data types or event types the test files cover (e.g., ADT, ORU, scheduling).",
            "Ask which test file would be appropriate for testing a patient admission workflow.",
        ],
    },
    # 6. Send test files to a destination
    {
        "goal": "Send one or more test files to a destination and confirm they were transmitted",
        "persona": "A QA engineer ready to execute an end-to-end test by sending sample data through Redox.",
        "simulation_guidelines": [
            "Ask the agent to send a test file to the destination.",
            "Ask for confirmation that the test file was sent successfully.",
            "Ask if there is a way to verify the destination received the data.",
        ],
    },
    # 7. Access transaction logs
    {
        "goal": "Access and retrieve transaction logs for recent activity, such as the test file send",
        "persona": "An integration engineer troubleshooting a data exchange and needing to review what was sent and received.",
        "simulation_guidelines": [
            "Ask the agent to pull up the transaction logs for recent activity.",
            "Ask to filter or find logs related to the test file that was just sent.",
            "Ask about the status of the transactions (success, failure, pending).",
        ],
    },
    # 8. Review and summarize transaction logs
    {
        "goal": "Get a human-readable summary of transaction log entries to understand what happened",
        "persona": "A project manager who needs a concise summary of recent transaction activity for a status report.",
        "simulation_guidelines": [
            "Ask the agent to summarize the recent transaction logs.",
            "Ask for a breakdown of how many transactions succeeded vs failed.",
            "Ask the agent to highlight any errors or issues that need attention.",
            "Prefer a summary format suitable for sharing with non-technical stakeholders.",
        ],
    },
]

simulator = ConversationSimulator(
    test_cases=test_cases,
    max_turns=6,
    user_model="databricks:/databricks-claude-sonnet-4-5",
)

# Get the invoke function that was registered via @invoke decorator in your agent
invoke_fn = get_invoke_function()
assert invoke_fn is not None, (
    "No function registered with the `@invoke` decorator found."
    "Ensure you have a function decorated with `@invoke()`."
)

# if invoke function is async, wrap it in a sync function.
# The simulator may already be running an event loop, so we use nest_asyncio
# to allow nested run_until_complete() calls without deadlocking.
if asyncio.iscoroutinefunction(invoke_fn):
    import nest_asyncio

    nest_asyncio.apply()

    def predict_fn(input: list[dict], **kwargs) -> dict:
        req = ResponsesAgentRequest(input=input)
        loop = asyncio.get_event_loop()
        response = loop.run_until_complete(invoke_fn(req))
        return response.model_dump()
else:

    def predict_fn(input: list[dict], **kwargs) -> dict:
        req = ResponsesAgentRequest(input=input)
        response = invoke_fn(req)
        return response.model_dump()


def evaluate():
    mlflow.genai.evaluate(
        data=simulator,
        predict_fn=predict_fn,
        scorers=[
            Completeness(),
            ConversationCompleteness(),
            ConversationalSafety(),
            KnowledgeRetention(),
            UserFrustration(),
            Fluency(),
            RelevanceToQuery(),
            Safety(),
            ToolCallCorrectness(),
        ],
    )
