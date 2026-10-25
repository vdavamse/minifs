"""An in-memory OpenTelemetry reader for the metrics tests, installed as the global ``MeterProvider`` once per
test process: ``minifs.metrics`` creates its instruments on the API's proxy meter at import, and they record into
this provider from the moment it is set."""

from collections.abc import Mapping
from typing import Any

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

reader = InMemoryMetricReader()
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))


def total(name: str, attributes: Mapping[str, str] | None = None) -> float:
    """The cumulative value of counter ``name``, or the count of histogram ``name``, summed over the data points
    whose attributes include ``attributes``."""
    return sum(getattr(point, "count", None) or getattr(point, "value", 0) for point in _points(name, attributes))


def histogram_sum(name: str, attributes: Mapping[str, str] | None = None) -> float:
    """The sum of the values histogram ``name`` recorded, over the matching data points (as ``total``)."""
    return sum(getattr(point, "sum", 0) for point in _points(name, attributes))


def _points(name: str, attributes: Mapping[str, str] | None) -> list[Any]:
    data = reader.get_metrics_data()
    wanted = attributes or {}
    return [
        point
        for resource in (data.resource_metrics if data else ())
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == name
        for point in metric.data.data_points
        if all((point.attributes or {}).get(key) == value for key, value in wanted.items())
    ]
