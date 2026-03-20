"""
mcp/bigquery_server.py
----------------------
Independent MCP Server that exposes BigQuery as tools.

This server knows ONLY about BigQuery — it has no knowledge of the agent,
Google Sheets, or any other part of the pipeline.

Any agent (Claude, GPT, or future LLM) can connect to this server
via the MCP protocol and get BigQuery capabilities automatically.

Run standalone:
    python -m src.mcp.bigquery_server

The agent connects to it via subprocess (stdio transport).
"""

import os
import re
import asyncio
import logging
import pandas as pd
from google.cloud import bigquery
import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT", "my-gcp-project")
DATASET_ID = os.environ.get("BQ_DATASET", "security_monitoring")
MAX_ROWS    = 50

# Blocks destructive SQL before it reaches BigQuery
DESTRUCTIVE = re.compile(
    r"\b(DROP|DELETE|INSERT|UPDATE|TRUNCATE|ALTER|CREATE|REPLACE|MERGE)\b",
    re.IGNORECASE,
)

# ── Server instance ────────────────────────────────────────────────────────────
app = Server("bigquery-server")


# ── Tool registry ──────────────────────────────────────────────────────────────

@app.list_tools()
async def list_tools() -> list[types.Tool]:
    """
    Advertise available tools to any connecting MCP client.
    Claude discovers these automatically — no hardcoding in the agent.
    """
    return [
        types.Tool(
            name="execute_sql",
            description=(
                "Run a read-only SELECT query against BigQuery. "
                "Use fully qualified table names: `project.dataset.table`. "
                "Destructive SQL (DROP, DELETE, INSERT, UPDATE...) is blocked."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A BigQuery SQL SELECT statement.",
                    }
                },
                "required": ["sql"],
            },
        ),
        types.Tool(
            name="list_tables",
            description=(
                f"List all tables available in the "
                f"{PROJECT_ID}.{DATASET_ID} BigQuery dataset."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        types.Tool(
            name="get_table_schema",
            description="Return the column names and types for a given BigQuery table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Short table name (e.g. clean_events) — project and dataset are added automatically.",
                    }
                },
                "required": ["table_name"],
            },
        ),
    ]


# ── Tool execution ─────────────────────────────────────────────────────────────

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """Route tool calls to the appropriate BigQuery operation."""

    client = bigquery.Client(project=PROJECT_ID)

    if name == "execute_sql":
        return await _execute_sql(client, arguments.get("sql", ""))

    if name == "list_tables":
        return await _list_tables(client)

    if name == "get_table_schema":
        return await _get_table_schema(client, arguments.get("table_name", ""))

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def _execute_sql(client: bigquery.Client, sql: str) -> list[types.TextContent]:
    # Guardrail — same logic as before but now lives ONLY in this server
    if DESTRUCTIVE.search(sql):
        keyword = DESTRUCTIVE.search(sql).group()
        return [types.TextContent(
            type="text",
            text=f"BLOCKED: query contains destructive keyword '{keyword}'"
        )]

    if len(sql) > 2000:
        return [types.TextContent(type="text", text="BLOCKED: query too long (max 2000 chars)")]

    try:
        df: pd.DataFrame = client.query(sql).result().to_dataframe()

        if df.empty:
            return [types.TextContent(type="text", text="Query returned no results.")]

        note = ""
        if len(df) > MAX_ROWS:
            df = df.head(MAX_ROWS)
            note = f"\n[Truncated to {MAX_ROWS} rows]"

        return [types.TextContent(type="text", text=df.to_string(index=False) + note)]

    except Exception as e:
        logger.error(f"BigQuery error: {e}")
        return [types.TextContent(type="text", text=f"ERROR: {e}")]


async def _list_tables(client: bigquery.Client) -> list[types.TextContent]:
    try:
        tables = client.list_tables(f"{PROJECT_ID}.{DATASET_ID}")
        names = [t.table_id for t in tables]
        if not names:
            return [types.TextContent(type="text", text="No tables found. Run the pipeline first.")]
        return [types.TextContent(type="text", text="\n".join(names))]
    except Exception as e:
        return [types.TextContent(type="text", text=f"ERROR: {e}")]


async def _get_table_schema(client: bigquery.Client, table_name: str) -> list[types.TextContent]:
    try:
        table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{table_name}")
        lines = [f"{f.name}: {f.field_type}" for f in table.schema]
        return [types.TextContent(type="text", text="\n".join(lines))]
    except Exception as e:
        return [types.TextContent(type="text", text=f"ERROR: {e}")]


# ── Entry point ────────────────────────────────────────────────────────────────

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
