"""
mcp/sheets_server.py
--------------------
Independent MCP Server that exposes Google Sheets as tools.

This server knows ONLY about Google Sheets — it has no knowledge of the agent,
BigQuery, or any other part of the pipeline.

It replaces the GoogleSheetsClient in api/api_client.py with a proper
MCP-compatible interface that any agent can use.

Run standalone:
    python -m src.mcp.sheets_server

Auth: ADC (gcloud auth application-default login) or GOOGLE_APPLICATION_CREDENTIALS
"""

import os
import asyncio
import logging
from datetime import datetime
from typing import Any
import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

logger = logging.getLogger(__name__)

SPREADSHEET_ID = os.environ.get("SHEETS_SPREADSHEET_ID", "mock-sheet-id-001")

app = Server("sheets-server")

# ── In-memory store (replace with real gspread calls in production) ─────────────
# In production each function body would be:
#   gc = gspread.service_account()
#   sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(tab)
_store: dict[str, list[list[Any]]] = {
    "metrics": [["user_id", "metric", "value", "updated_at"]],
    "alerts":  [["user_id", "risk_level", "failed_attempts", "flagged_at"]],
}


# ── Tool registry ──────────────────────────────────────────────────────────────

@app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="read_sheet",
            description="Read all rows from a Google Sheets tab.",
            inputSchema={
                "type": "object",
                "properties": {
                    "tab": {
                        "type": "string",
                        "description": "Sheet tab name (e.g. 'metrics', 'alerts')",
                    }
                },
                "required": ["tab"],
            },
        ),
        types.Tool(
            name="write_metric",
            description=(
                "Append a metric row to the 'metrics' tab in Google Sheets. "
                "Use this to report pipeline results (DAU, failed login rate, etc.)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "user_id":  {"type": "string", "description": "User ID or 'all' for global metrics"},
                    "metric":   {"type": "string", "description": "Metric name (e.g. 'dau', 'failed_login_rate')"},
                    "value":    {"type": "number", "description": "Metric value"},
                },
                "required": ["user_id", "metric", "value"],
            },
        ),
        types.Tool(
            name="flag_suspicious_user",
            description="Append a suspicious user alert to the 'alerts' tab in Google Sheets.",
            inputSchema={
                "type": "object",
                "properties": {
                    "user_id":         {"type": "string"},
                    "risk_level":      {"type": "string", "enum": ["MEDIUM", "HIGH"]},
                    "failed_attempts": {"type": "integer"},
                },
                "required": ["user_id", "risk_level", "failed_attempts"],
            },
        ),
    ]


# ── Tool execution ─────────────────────────────────────────────────────────────

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:

    if name == "read_sheet":
        return _read_sheet(arguments.get("tab", "metrics"))

    if name == "write_metric":
        return _write_metric(
            arguments["user_id"],
            arguments["metric"],
            arguments["value"],
        )

    if name == "flag_suspicious_user":
        return _flag_user(
            arguments["user_id"],
            arguments["risk_level"],
            arguments["failed_attempts"],
        )

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


def _read_sheet(tab: str) -> list[types.TextContent]:
    """
    Read rows from a sheet tab.
    Production: sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(tab)
                return sheet.get_all_records()
    """
    rows = _store.get(tab, [])
    if not rows:
        return [types.TextContent(type="text", text=f"Tab '{tab}' is empty or not found.")]

    # Format as a readable table
    header = rows[0]
    data_rows = rows[1:]
    lines = ["\t".join(str(c) for c in header)]
    lines += ["\t".join(str(c) for c in row) for row in data_rows]
    return [types.TextContent(type="text", text="\n".join(lines))]


def _write_metric(user_id: str, metric: str, value: float) -> list[types.TextContent]:
    """
    Append a metric row.
    Production: sheet.append_row([user_id, metric, value, timestamp])
    """
    row = [user_id, metric, value, datetime.utcnow().isoformat()]
    _store.setdefault("metrics", []).append(row)
    logger.info(f"Wrote metric to Sheets: {row}")
    return [types.TextContent(type="text", text=f"Written: {row}")]


def _flag_user(user_id: str, risk_level: str, failed_attempts: int) -> list[types.TextContent]:
    """
    Append a suspicious user alert.
    Production: alerts_sheet.append_row([...])
    """
    row = [user_id, risk_level, failed_attempts, datetime.utcnow().isoformat()]
    _store.setdefault("alerts", []).append(row)
    logger.warning(f"Flagged suspicious user in Sheets: {row}")
    return [types.TextContent(type="text", text=f"Alert logged: {row}")]


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
