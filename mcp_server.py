"""
MCP Server for genpark-distributed-task-idempotency-deduplication-skill
Standard JSON-RPC 2.0 protocol over stdio.
"""

import sys
import json
from client import TaskIdempotencyDeduplicationClient

engine = TaskIdempotencyDeduplicationClient()

def handle_request(req):
    req_id = req.get("id")
    method = req.get("method")
    params = req.get("params", {})

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": [
                    {
                        "name": "check_or_lock_key",
                        "description": "Check if an idempotency key is already processed or acquire in-flight lock.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "key": {"type": "string"},
                                "ttl": {"type": "integer"}
                            },
                            "required": ["key"]
                        }
                    }
                ]
            }
        }
    elif method == "tools/call":
        tool_name = params.get("name")
        args = params.get("arguments", {})
        if tool_name == "check_or_lock_key":
            acquired, cached = engine.acquire_execution_lock(args["key"], args.get("ttl", 30))
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps({"acquired": acquired, "cached": cached})}]
                }
            }

    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": "Method not found"}}

def main():
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            req = json.loads(line)
            res = handle_request(req)
            sys.stdout.write(json.dumps(res) + "\n")
            sys.stdout.flush()
        except Exception as e:
            err_res = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": str(e)}}
            sys.stdout.write(json.dumps(err_res) + "\n")
            sys.stdout.flush()

if __name__ == "__main__":
    main()
