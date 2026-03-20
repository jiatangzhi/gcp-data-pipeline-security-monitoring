"""
agent/agent.py
--------------
Conversational agent that lets users query BigQuery
using natural language. Powered by Claude (Anthropic API).

Architecture:
  User message → Claude (tool_use) → SQL tool → BigQuery → Result → Claude → Answer

Guardrails:
  - Blocks destructive SQL (DROP, DELETE, INSERT, UPDATE, TRUNCATE)
  - Validates inputs before execution
  - Limits query result size to avoid context overflow
"""

import os
import re
import logging
import pandas as pd
import anthropic
from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT", "my-gcp-project")
DATASET_ID = os.environ.get("BQ_DATASET", "security_monitoring")

# ─────────────────────────────────────────────
# GUARDRAILS
# ─────────────────────────────────────────────

# SQL keywords that signal a destructive or unsafe operation
DESTRUCTIVE_KEYWORDS = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|TRUNCATE|ALTER|CREATE|REPLACE|MERGE)\b",
    re.IGNORECASE,
)

MAX_RESULT_ROWS = 50  # Limit rows returned to avoid huge context


def is_safe_sql(sql: str) -> tuple[bool, str]:
    """
    Check whether a SQL query is safe to execute.
    Returns (is_safe, reason).
    """
    if DESTRUCTIVE_KEYWORDS.search(sql):
        found = DESTRUCTIVE_KEYWORDS.search(sql).group()
        return False, f"Query contains destructive keyword: {found}"
    if len(sql) > 2000:
        return False, "Query exceeds maximum length (2000 chars)"
    return True, "OK"


# ─────────────────────────────────────────────
# TOOL: EXECUTE SQL
# ─────────────────────────────────────────────

def execute_sql(sql: str) -> str:
    """
    Execute a read-only SQL query against BigQuery.
    Returns a formatted string result (or error message).
    """
    safe, reason = is_safe_sql(sql)
    if not safe:
        logger.warning(f"Blocked unsafe SQL: {reason}")
        return f"ERROR: Query blocked by safety guardrail. Reason: {reason}"

    try:
        client = bigquery.Client(project=PROJECT_ID)
        df = client.query(sql).result().to_dataframe()

        if df.empty:
            return "Query returned no results."

        # Truncate to avoid overwhelming the context window
        if len(df) > MAX_RESULT_ROWS:
            df = df.head(MAX_RESULT_ROWS)
            truncation_note = f"\n[Results truncated to {MAX_RESULT_ROWS} rows]"
        else:
            truncation_note = ""

        return df.to_string(index=False) + truncation_note

    except Exception as e:
        logger.error(f"BigQuery query error: {e}")
        return f"ERROR: {str(e)}"


def list_available_tables() -> str:
    """Return a list of tables available in the BigQuery dataset."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        tables = client.list_tables(f"{PROJECT_ID}.{DATASET_ID}")
        names = [t.table_id for t in tables]
        if not names:
            return "No tables found. Run the pipeline first."
        return "\n".join(names)
    except Exception as e:
        return f"ERROR: {str(e)}"


# ─────────────────────────────────────────────
# CLAUDE TOOL DEFINITIONS
# ─────────────────────────────────────────────

TOOLS = [
    {
        "name": "execute_sql",
        "description": (
            "Execute a read-only SQL query against BigQuery. "
            "Use fully qualified table names in the format: "
            "`project_id.dataset_id.table_name`. "
            "Use this to answer questions about users, events, logins, transactions, "
            "daily active users, suspicious users, sales, and login failure rates. "
            "Do NOT use destructive SQL (DROP, DELETE, INSERT, UPDATE, etc.)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A valid BigQuery SQL SELECT query with fully qualified table names.",
                }
            },
            "required": ["sql"],
        },
    },
    {
        "name": "list_tables",
        "description": "List all available tables in the BigQuery dataset.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]

SYSTEM_PROMPT = f"""You are a data analyst assistant for a cybersecurity and analytics pipeline.

You have access to a Google BigQuery dataset with these tables (use fully qualified names):
- `{PROJECT_ID}.{DATASET_ID}.raw_events`        — raw user activity events
- `{PROJECT_ID}.{DATASET_ID}.raw_logins`        — raw login attempts
- `{PROJECT_ID}.{DATASET_ID}.raw_transactions`  — raw transactions
- `{PROJECT_ID}.{DATASET_ID}.clean_events`      — cleaned events
- `{PROJECT_ID}.{DATASET_ID}.clean_logins`      — cleaned logins
- `{PROJECT_ID}.{DATASET_ID}.clean_transactions`— cleaned transactions
- `{PROJECT_ID}.{DATASET_ID}.agg_dau`           — daily active users per day
- `{PROJECT_ID}.{DATASET_ID}.agg_failed_login_rate` — failed login rate per day
- `{PROJECT_ID}.{DATASET_ID}.agg_sales_per_region`  — total sales by region
- `{PROJECT_ID}.{DATASET_ID}.agg_suspicious_users`  — users with repeated failed logins

Rules:
1. Always use fully qualified BigQuery table names: `project.dataset.table`
2. Use SELECT queries only — never modify data.
3. Be concise but explain your findings clearly.
4. If asked about suspicious users or security threats, prioritise clarity.
"""


# ─────────────────────────────────────────────
# AGENT LOOP
# ─────────────────────────────────────────────

def run_agent(user_question: str, api_key: str = None) -> str:
    """
    Run the conversational agent for a single user question.
    Uses Claude's tool_use to translate natural language → BigQuery SQL → answer.

    Args:
        user_question: The natural language question from the user.
        api_key: Anthropic API key (falls back to ANTHROPIC_API_KEY env var).

    Returns:
        The agent's final answer as a string.
    """
    if not user_question or not user_question.strip():
        return "Please provide a valid question."
    if len(user_question) > 1000:
        return "Question too long. Please keep it under 1000 characters."

    key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not key:
        return (
            "ANTHROPIC_API_KEY not set. "
            "Export it or pass it directly: export ANTHROPIC_API_KEY=sk-ant-..."
        )

    client = anthropic.Anthropic(api_key=key)
    messages = [{"role": "user", "content": user_question}]

    logger.info(f"Agent received question: {user_question}")

    # Agentic loop — runs until Claude stops requesting tools
    for iteration in range(10):  # safety limit
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
            thinking={"type": "adaptive"},
        )

        if response.stop_reason == "end_turn":
            for block in response.content:
                if block.type == "text":
                    logger.info("Agent finished")
                    return block.text
            return "Agent completed but produced no text output."

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    logger.info(f"Claude calling tool: {block.name}({block.input})")

                    if block.name == "execute_sql":
                        result = execute_sql(block.input.get("sql", ""))
                    elif block.name == "list_tables":
                        result = list_available_tables()
                    else:
                        result = f"Unknown tool: {block.name}"

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })

            messages.append({"role": "user", "content": tool_results})

        else:
            logger.warning(f"Unexpected stop reason: {response.stop_reason}")
            break

    return "Agent reached iteration limit without completing."


# ─────────────────────────────────────────────
# INTERACTIVE CLI
# ─────────────────────────────────────────────

def interactive_session():
    """Run an interactive Q&A session in the terminal."""
    print("\n" + "="*60)
    print("  Data Pipeline Agent  (type 'quit' to exit)")
    print("="*60)
    print("Ask anything about users, logins, transactions, or security.\n")

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if question.lower() in ("quit", "exit", "q"):
            print("Goodbye!")
            break

        if not question:
            continue

        print("Agent: thinking...\n")
        answer = run_agent(question)
        print(f"Agent: {answer}\n")


if __name__ == "__main__":
    interactive_session()
