"""
Demonstration of genpark-distributed-task-idempotency-deduplication-skill
"""

from client import TaskIdempotencyDeduplicationClient

def create_invoice(invoice_id: str, amount: float):
    # Simulated non-idempotent operation
    return {"invoice_id": invoice_id, "amount": amount, "status": "ISSUED"}

def main():
    engine = TaskIdempotencyDeduplicationClient()
    key = engine.compute_key("create_invoice", {"invoice_id": "INV-2026-991", "amount": 850.0})

    # First call: Executes cleanly
    run1 = engine.execute_idempotent(key, create_invoice, "INV-2026-991", 850.0)
    print(f"Run 1 [{run1['source']}]: Invoice Created -> Status: {run1['data']['status']}")

    # Second call with same key: Replays cached result safely
    run2 = engine.execute_idempotent(key, create_invoice, "INV-2026-991", 850.0)
    print(f"Run 2 [{run2['source']}]: Replayed from Cache -> Status: {run2['data']['status']}")

    assert run1["data"] == run2["data"], "Idempotency invariant maintained!"
    print("Zero-duplicate execution successfully validated.")

if __name__ == "__main__":
    main()
