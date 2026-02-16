"""Benchmark measurement framework."""

import json
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import psutil


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    name: str
    wall_time_seconds: float = 0.0
    cpu_user_seconds: float = 0.0
    cpu_system_seconds: float = 0.0
    peak_memory_mb: float = 0.0
    disk_bytes: int = 0
    row_count: int = 0
    error: str | None = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def rows_per_second(self) -> float:
        if self.wall_time_seconds > 0 and self.row_count > 0:
            return self.row_count / self.wall_time_seconds
        return 0.0

    @property
    def disk_mb(self) -> float:
        return self.disk_bytes / (1024 * 1024)

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json())

    @classmethod
    def from_json(cls, text: str) -> "BenchmarkResult":
        data = json.loads(text)
        return cls(**data)

    @classmethod
    def load(cls, path: Path) -> "BenchmarkResult":
        return cls.from_json(path.read_text())


class _MemoryTracker:
    """Background thread that samples RSS at 100ms intervals."""

    def __init__(self, pid: int):
        self.process = psutil.Process(pid)
        self.peak_mb = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self):
        while not self._stop.is_set():
            try:
                rss = self.process.memory_info().rss / (1024 * 1024)
                self.peak_mb = max(self.peak_mb, rss)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            self._stop.wait(0.1)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=1.0)


def run_with_metrics(name: str, fn: Callable[[], dict[str, Any] | None]) -> BenchmarkResult:
    """Run a callable and measure wall time, CPU, and peak memory.

    The callable may return a dict with extra fields to merge into the result:
        - row_count: int
        - disk_bytes: int
        - metadata: dict
    """
    pid = os.getpid()
    proc = psutil.Process(pid)

    # Baseline CPU
    cpu_before = proc.cpu_times()

    # Start memory tracker
    mem_tracker = _MemoryTracker(pid)
    mem_tracker.start()

    error = None
    extra = {}
    t0 = time.perf_counter()
    try:
        result = fn()
        if isinstance(result, dict):
            extra = result
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
    finally:
        wall_time = time.perf_counter() - t0
        mem_tracker.stop()
        cpu_after = proc.cpu_times()

    return BenchmarkResult(
        name=name,
        wall_time_seconds=round(wall_time, 3),
        cpu_user_seconds=round(cpu_after.user - cpu_before.user, 3),
        cpu_system_seconds=round(cpu_after.system - cpu_before.system, 3),
        peak_memory_mb=round(mem_tracker.peak_mb, 1),
        disk_bytes=extra.get("disk_bytes", 0),
        row_count=extra.get("row_count", 0),
        error=error,
        metadata=extra.get("metadata", {}),
    )
