"""OpenTelemetry metrics (README: Observability), on the meter ``minifs``. No-ops without the ``otel`` extra
(``pip install 'minifs-pg[otel]'``, which installs ``opentelemetry-api``), and until the application configures a
``MeterProvider``: minifs only records, it never sets up an SDK or an exporter.

Names follow the OpenTelemetry conventions, so a Prometheus exporter turns ``minifs.tool.duration`` (unit ``s``)
into ``minifs_tool_duration_seconds``, next to ladderframe's ``ladderframe_turn_duration_seconds``. Tool metrics
carry ``gen_ai.tool.name``, the attribute of the tool spans pydantic-ai's instrumentation emits, so a metric joins
its traces; minifs emits no spans of its own.

Instruments, with their attributes:

- ``minifs.tool.duration`` (histogram, s): every tool call, by ``gen_ai.tool.name`` and ``minifs.outcome`` (ok,
  denied, retry, cancelled, error).
- ``minifs.run.tool_calls`` (histogram): minifs tool calls per agent run, from ``minifs.tools.RunMetrics``.
- ``minifs.search.duration`` (histogram, s): each content scan of a grep, by ``minifs.outcome`` (ok, timeout,
  error). ``minifs.search.bytes`` (histogram, By): the scan's scope in bytes, the most it reads.
- ``minifs.search.refusals`` (counter): greps refused, by ``minifs.reason`` (files, bytes: a scope cap; timeout).
- ``minifs.timeouts`` (counter): other operations out of time, by ``minifs.reason`` (statement, budget, pool).
- ``minifs.regex.rejections`` (counter): patterns grep rejects, by ``minifs.construct`` (look_around,
  backreference, syntax, newline, ...: ``RegexError.construct``).
- ``minifs.policy.denials`` (counter): ``Policy.check`` refusals, by ``minifs.actor`` and ``minifs.action``. Actors
  are policy roles (``agent``, ``ingestion``), a small set; don't make them per-user ids.
- ``minifs.cache.lookups``, ``minifs.cache.hits`` (observable counters): the directory cache's ``CacheStats``, for
  the hit rate, once ``observe_cache`` registers it (one cache per process, as ``open_vfs`` has).
- ``minifs.read.bytes`` (histogram, By): bytes per read through a ``VFSView``, by ``minifs.op`` (pread,
  pread_lines, pread_many).
- ``minifs.file.extents`` (histogram): extents per modified file, as the compaction scan counts them, once per
  modification in each process that runs the job.
  ``minifs.compaction.files`` and ``minifs.compaction.extents_removed`` (counters): what compaction did.
"""

from __future__ import annotations

import weakref
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from minifs.vfs.cache import DirEntryCache

type Attributes = Mapping[str, str]


class Counter(Protocol):
    def add(self, amount: int, attributes: Attributes | None = None) -> None: ...


class Histogram(Protocol):
    def record(self, amount: float, attributes: Attributes | None = None) -> None: ...


class _Noop:
    def add(self, amount: int, attributes: Attributes | None = None) -> None:
        pass

    def record(self, amount: float, attributes: Attributes | None = None) -> None:
        pass


_SECONDS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
_BYTES = tuple(float(1024 * 4**n) for n in range(10))  # 1 KB to 256 MB
_COUNTS = (1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10_000.0, 100_000.0)

try:
    from opentelemetry import metrics as _otel
except ImportError:  # pragma: no cover - exercised by test_metrics_without_otel
    _otel = None

AVAILABLE = _otel is not None
"""Whether ``opentelemetry-api`` is installed. Without it every instrument is a no-op."""

_caches: weakref.WeakSet[DirEntryCache] = weakref.WeakSet()

if _otel is not None:
    _meter = _otel.get_meter("minifs")

    def _histogram(name: str, unit: str, description: str, buckets: Sequence[float]) -> Histogram:
        return _meter.create_histogram(name, unit, description, explicit_bucket_boundaries_advisory=buckets)

    def _counter(name: str, unit: str, description: str) -> Counter:
        return _meter.create_counter(name, unit, description)

    def _cache_counter(field: str) -> Any:
        def observe(options: Any) -> Iterable[Any]:
            assert _otel is not None
            caches = list(_caches)  # a copy: the exporter calls this from its own thread
            yield _otel.Observation(sum(getattr(cache.stats(), field) for cache in caches))

        return observe

    _meter.create_observable_counter(
        "minifs.cache.lookups", [_cache_counter("lookups")], "{lookup}", "Directory-cache lookups"
    )
    _meter.create_observable_counter("minifs.cache.hits", [_cache_counter("hits")], "{lookup}", "Directory-cache hits")
else:  # pragma: no cover

    def _histogram(name: str, unit: str, description: str, buckets: Sequence[float]) -> Histogram:
        return _Noop()

    def _counter(name: str, unit: str, description: str) -> Counter:
        return _Noop()


TOOL_DURATION = _histogram("minifs.tool.duration", "s", "Tool call latency", _SECONDS)
RUN_TOOL_CALLS = _histogram("minifs.run.tool_calls", "{call}", "minifs tool calls per agent run", _COUNTS)
SEARCH_DURATION = _histogram("minifs.search.duration", "s", "Content scan duration of a grep", _SECONDS)
SEARCH_BYTES = _histogram("minifs.search.bytes", "By", "Bytes in a grep's scope, the most its scan reads", _BYTES)
SEARCH_REFUSALS = _counter("minifs.search.refusals", "{search}", "Greps refused over a scope cap or timed out")
TIMEOUTS = _counter("minifs.timeouts", "{operation}", "Operations other than grep that ran out of time")
REGEX_REJECTIONS = _counter("minifs.regex.rejections", "{pattern}", "Grep patterns rejected, by construct")
POLICY_DENIALS = _counter("minifs.policy.denials", "{operation}", "Operations the policy refused")
READ_BYTES = _histogram("minifs.read.bytes", "By", "Bytes returned per read", _BYTES)
FILE_EXTENTS = _histogram("minifs.file.extents", "{extent}", "Extents per file, as the compaction scan counts", _COUNTS)
COMPACTION_FILES = _counter("minifs.compaction.files", "{file}", "Files compacted")
COMPACTION_REMOVED = _counter("minifs.compaction.extents_removed", "{extent}", "Extents merged away by compaction")


def observe_cache(cache: DirEntryCache) -> None:
    """Reports ``cache``'s lookups and hits (``CacheStats``) as ``minifs.cache.lookups`` and ``minifs.cache.hits``
    while it lives, summed over every cache observed. ``open_vfs`` calls it for its cache."""
    _caches.add(cache)
