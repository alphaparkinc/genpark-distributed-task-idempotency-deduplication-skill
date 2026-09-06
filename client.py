"""
Distributed Task Idempotency Key and Deduplication Engine.
Zero external dependencies, standard library only.
"""

import time
import hashlib
from typing import Dict, Any, Optional, Tuple

class TaskIdempotencyDeduplicationClient:
    """
    Protects downstream systems against redundant LLM retries and agent replays:
    - Calculates deterministic idempotency keys from payload signatures
    - In-flight execution locking to prevent parallel duplicate runs
    - Replay response caching with configurable TTL
    """

    def __init__(self):
        # key -> {status: "IN_FLIGHT"|"COMPLETED", result: Any, expires_at: float}
        self.registry: Dict[str, Dict[str, Any]] = {}

    def compute_key(self, endpoint: str, payload: Dict[str, Any]) -> str:
        """Computes deterministic SHA-256 idempotency key."""
        sorted_repr = f"{endpoint}:{sorted(payload.items())}"
        return hashlib.sha256(sorted_repr.encode("utf-8")).hexdigest()

    def acquire_execution_lock(self, idempotency_key: str, ttl_sec: int = 30) -> Tuple[bool, Optional[Any]]:
        """
        Attempts to acquire execution lock for key.
        Returns: (is_new_execution, cached_result_if_any)
        """
        now = time.time()
        record = self.registry.get(idempotency_key)

        if record:
            if record["expires_at"] > now:
                if record["status"] == "COMPLETED":
                    return False, record["result"]
                elif record["status"] == "IN_FLIGHT":
                    # Parallel duplicate in progress
                    return False, {"warning": "OPERATION_IN_FLIGHT"}
            else:
                del self.registry[idempotency_key]

        # Acquire lock
        self.registry[idempotency_key] = {
            "status": "IN_FLIGHT",
            "result": None,
            "expires_at": now + ttl_sec
        }
        return True, None

    def commit_result(self, idempotency_key: str, result: Any, cache_ttl_sec: int = 3600):
        """Marks execution as completed and stores replayable response."""
        self.registry[idempotency_key] = {
            "status": "COMPLETED",
            "result": result,
            "expires_at": time.time() + cache_ttl_sec
        }

    def execute_idempotent(self, idempotency_key: str, task_fn, *args, **kwargs) -> Dict[str, Any]:
        """Safely executes task or returns cached response if previously executed."""
        acquired, cached = self.acquire_execution_lock(idempotency_key)
        if not acquired:
            return {"source": "CACHE_REPLAY", "data": cached}

        try:
            res = task_fn(*args, **kwargs)
            self.commit_result(idempotency_key, res)
            return {"source": "FRESH_EXECUTION", "data": res}
        except Exception as e:
            if idempotency_key in self.registry:
                del self.registry[idempotency_key]
            raise e
