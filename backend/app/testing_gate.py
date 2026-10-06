"""Concurrency gate for tests: forces overlapping requests to be in flight
together and controls who acquires the writer lock first (the winner's world
is the one left behind).

Enabled only when env PF_TEST_GATE=1; control endpoints are mounted then.
Production and single-threaded TestClient tests are unaffected."""
import os
import threading
import time

class Gate:
    def __init__(self):
        self._cond = threading.Condition()
        self._arrived: list[str] = []
        self._released = False
        self._block_holders: set[str] = set()
        self._scenario = ""
        self._parties: set[str] = set()
        self._block: set[str] = set()

    def enabled(self) -> bool:
        return os.environ.get("PF_TEST_GATE") == "1"

    def configure(self, scenario: str, parties: list[str], block: list[str]):
        with self._cond:
            self._scenario = scenario
            self._parties = set(parties)
            self._block = set(block)
            self._arrived = []
            self._released = False
            self._block_holders = set()
            self._cond.notify_all()

    def reset(self):
        with self._cond:
            self._scenario = ""
            self._parties = set()
            self._block = set()
            self._arrived = []
            self._released = False
            self._block_holders = set()
            self._cond.notify_all()

    def wait(self, kind: str, tag: str | None = None):
        if not self.enabled():
            return
        label = f"{kind}:{tag}" if tag else kind
        with self._cond:
            if label not in self._parties:
                return
            self._arrived.append(label)
            self._cond.notify_all()
            while not self._released:
                if not self._cond.wait(timeout=15):
                    raise TimeoutError(f"gate release timeout, arrived={self._arrived}")
            is_blocker = label in self._block
        # Outside the cond lock: blockers yield so the other party takes the
        # SQLite writer lock first and commits the world that stays.
        if is_blocker:
            with self._cond:
                self._block_holders.add(label)
                self._cond.notify_all()
            time.sleep(0.4)
            with self._cond:
                self._block_holders.discard(label)
                self._cond.notify_all()

    def arrived(self) -> list[str]:
        with self._cond:
            return list(self._arrived)

    def release(self):
        with self._cond:
            self._released = True
            self._cond.notify_all()

    def wait_until_arrived(self, parties: set[str], timeout: float = 15):
        deadline = time.time() + timeout
        with self._cond:
            while not parties.issubset(set(self._arrived)):
                remaining = deadline - time.time()
                if remaining <= 0 or not self._cond.wait(timeout=remaining):
                    raise TimeoutError(f"arrive timeout: {self._arrived}")

    def wait_until_blocked(self, block: set[str], timeout: float = 15):
        deadline = time.time() + timeout
        with self._cond:
            while not block.issubset(self._block_holders):
                remaining = deadline - time.time()
                if remaining <= 0 or not self._cond.wait(timeout=remaining):
                    raise TimeoutError(f"block timeout: {self._block_holders}")

    def register_routes(self, app):
        if not self.enabled():
            return
        from fastapi import Body
        from pydantic import BaseModel

        class GateConfig(BaseModel):
            scenario: str
            parties: list[str]
            block: list[str] = []

        @app.post("/__gate/config")
        def _cfg(body: GateConfig):
            self.configure(body.scenario, body.parties, body.block)
            return {"ok": True}

        @app.post("/__gate/release")
        def _release():
            self.release()
            return {"ok": True}

        @app.post("/__gate/reset")
        def _reset():
            self.reset()
            return {"ok": True}

        @app.get("/__gate/arrived")
        def _arrived():
            return {"arrived": self.arrived()}

gate = Gate()
