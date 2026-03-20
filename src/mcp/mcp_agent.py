"""
mcp/mcp_agent.py
----------------
Conversational agent that connects to independent MCP servers.

The agent has ZERO knowledge of BigQuery or Google Sheets internals.
It simply:
  1. Launches the MCP servers as subprocesses
  2. Asks Claude what tools are available (Claude discovers them via MCP)
  3. Routes tool calls to the correct server
  4. Returns Claude's final answer

Compare with agent/agent.py where BigQuery logic was hardcoded here.
Now adding a new data source = adding a new MCP server, not editing the agent.
"""

import os
import sys
import asyncio
import logging
import subprocess
import json
from typing import Any
import anthropic

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT", "my-gcp-project")
DATASET_ID = os.environ.get("BQ_DATASET", "security_monitoring")


# ── MCP Client ─────────────────────────────────────────────────────────────────

class MCPClient:
    """
    Minimal MCP client that communicates with a server over stdio (JSON-RPC).
    Launches the server as a subprocess and sends/receives JSON-RPC messages.
    """

    def __init__(self, name: str, command: list[str]):
        self.name = name
        self.command = command
        self.process: subprocess.Popen | None = None
        self._request_id = 0

    def start(self):
        self.process = subprocess.Popen(
            self.command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Send MCP initialize handshake
        self._send({
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "pipeline-agent", "version": "1.0"},
            },
        })
        self._recv()  # consume initialize response
        logger.info(f"MCP server '{self.name}' started")

    def stop(self):
        if self.process:
            self.process.terminate()
            logger.info(f"MCP server '{self.name}' stopped")

    def list_tools(self) -> list[dict]:
        """Ask the server what tools it provides."""
        self._send({
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "tools/list",
            "params": {},
        })
        response = self._recv()
        return response.get("result", {}).get("tools", [])

    def call_tool(self, tool_name: str, arguments: dict) -> str:
        """Call a specific tool on this server and return the text result."""
        self._send({
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        })
        response = self._recv()
        content = response.get("result", {}).get("content", [])
        return "\n".join(c.get("text", "") for c in content if c.get("type") == "text")

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _send(self, payload: dict):
        line = json.dumps(payload) + "\n"
        self.process.stdin.write(line)
        self.process.stdin.flush()

    def _recv(self) -> dict:
        line = self.process.stdout.readline()
        return json.loads(line) if line.strip() else {}


# ── Agent ──────────────────────────────────────────────────────────────────────

class MCPAgent:
    """
    Agent that discovers tools from multiple MCP servers and uses Claude
    to route questions to the right server automatically.

    Adding a new data source:
        servers["new_source"] = MCPClient("new_source", ["python", "src/mcp/new_server.py"])
    That's it — the agent picks up the new tools on the next run.
    """

    def __init__(self, api_key: str = None):
        self.client = anthropic.Anthropic(
            api_key=api_key or os.environ.get("ANTHROPIC_API_KEY")
        )

        # ── Register MCP servers here ──────────────────────────────────────────
        # Each entry is independent — BigQuery server knows nothing about Sheets
        self.servers: dict[str, MCPClient] = {
            "bigquery": MCPClient(
                name="bigquery",
                command=[sys.executable, "-m", "src.mcp.bigquery_server"],
            ),
            "sheets": MCPClient(
                name="sheets",
                command=[sys.executable, "-m", "src.mcp.sheets_server"],
            ),
        }

        # Maps tool_name → which server handles it (populated at startup)
        self._tool_server_map: dict[str, str] = {}
        # Claude-compatible tool definitions (populated at startup)
        self._claude_tools: list[dict] = []

    def start(self):
        """Launch all MCP servers and collect their tools."""
        for server_name, server in self.servers.items():
            server.start()
            tools = server.list_tools()

            for tool in tools:
                self._tool_server_map[tool["name"]] = server_name
                # Convert MCP tool format → Claude tool format
                self._claude_tools.append({
                    "name": tool["name"],
                    "description": tool.get("description", ""),
                    "input_schema": tool.get("inputSchema", {"type": "object", "properties": {}}),
                })

        logger.info(f"Agent ready with {len(self._claude_tools)} tools from {len(self.servers)} servers")
        logger.info(f"Tool → Server map: {self._tool_server_map}")

    def stop(self):
        for server in self.servers.values():
            server.stop()

    def ask(self, question: str) -> str:
        """
        Answer a natural language question using Claude + MCP tools.
        Claude decides which tools to call — the agent just routes the calls.
        """
        if not question.strip():
            return "Please provide a question."

        system = f"""You are a data analyst for a cybersecurity pipeline.
You have access to BigQuery ({PROJECT_ID}.{DATASET_ID}) and Google Sheets.
Use the available tools to answer questions. Always use fully qualified BigQuery table names."""

        messages = [{"role": "user", "content": question}]

        for _ in range(10):  # safety limit
            response = self.client.messages.create(
                model="claude-opus-4-6",
                max_tokens=2048,
                system=system,
                tools=self._claude_tools,   # discovered from MCP servers
                messages=messages,
                thinking={"type": "adaptive"},
            )

            if response.stop_reason == "end_turn":
                for block in response.content:
                    if block.type == "text":
                        return block.text
                return "No answer produced."

            if response.stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": response.content})

                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        # Route to the correct MCP server — agent doesn't know the implementation
                        server_name = self._tool_server_map.get(block.name)
                        if not server_name:
                            result = f"No server registered for tool: {block.name}"
                        else:
                            result = self.servers[server_name].call_tool(block.name, block.input)

                        logger.info(f"Tool '{block.name}' → server '{server_name}'")
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })

                messages.append({"role": "user", "content": tool_results})

        return "Agent reached iteration limit."

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


# ── CLI ────────────────────────────────────────────────────────────────────────

def interactive_session():
    print("\n" + "="*60)
    print("  MCP Agent  (BigQuery + Google Sheets)")
    print("  type 'quit' to exit")
    print("="*60 + "\n")

    with MCPAgent() as agent:
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
            print(f"Agent: {agent.ask(question)}\n")


if __name__ == "__main__":
    interactive_session()
