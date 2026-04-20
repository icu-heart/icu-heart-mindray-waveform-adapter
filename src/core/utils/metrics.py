from __future__ import annotations
from typing import Any, Dict, Tuple, List, Union, Optional
import os
import time
from opentelemetry.metrics import Observation

from .status_updates import safe_print, Fore

Number = Union[int, float]
ValueSpec = Union[
    Number,
    Tuple[Number, Dict[str, Any]],
    List[Union[Number, Tuple[Number, Dict[str, Any]]]]
]

class MetricsManager:
    """
    Simple metrics helper.

    Usage:
        mm = MetricsManager(endpoint="atriumdb-otel:4317")  # or None to disable
        mm.record({
            "counters": {
                "hl7_messages_processed_total": 120,
                "hl7_messages_published_total": (118, {"route": "parsed"}),
                "hl7_unknown_bed_messages_total": [
                    (2, {"ward": "A", "bed": "01"}),
                    (1, {"ward": "B", "bed": "07"})
                ]
            },
            "gauges": {
                "hl7_queue_depth": 5432
            },
            "histograms": {
                "hl7_parse_batch_duration_ms": [5.2, 6.1, 4.9]
            }
        })
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        service_name: str = "app",
        export_interval_ms: int = 5000,
        enable: bool = True
    ):
        # Endpoint: env var if not provided
        self._endpoint = endpoint or os.getenv('OTEL_ENDPOINT')
        self.enabled = bool(enable and self._endpoint)
        self._service_name = service_name
        self._export_interval_ms = export_interval_ms
        self._provider = None
        self._meter = None

        self._counters: Dict[str, Any] = {}
        self._updown: Dict[str, Any] = {}
        self._histograms: Dict[str, Any] = {}
        self._latest_specs: Dict[str, Dict[Tuple, Number]] = {}

        # Avoid log spam if exporter endpoint is down
        self._warn_every_s = 10.0
        self._next_warn_at = 0.0

        if self.enabled:
            self._init_sdk()

    def _warn_exporter_unavailable(self, context: str, exc: Exception | None = None) -> None:
        now = time.time()
        if now < self._next_warn_at:
            return
        self._next_warn_at = now + self._warn_every_s
        safe_print(f"[Metrics] Exporter unavailable; metrics dropped ({context}): {exc}", Fore.YELLOW)

    def record(self, payload: Dict[str, Dict[str, ValueSpec]]) -> None:
        if not self.enabled or not self._meter:
            return
        try:
            self._apply_counters(payload.get("counters", {}))
            self._apply_gauges(payload.get("gauges", {}))
            self._apply_histograms(payload.get("histograms", {}))
        except Exception as e:
            # If exporter/otel internals are unhappy, do not crash caller.
            self._warn_exporter_unavailable("record()", e)

    def flush(self):
        if self._provider:
            try:
                self._provider.force_flush()
            except Exception as e:
                self._warn_exporter_unavailable("flush()", e)

    def shutdown(self):
        if self._provider:
            try:
                self._provider.shutdown()
            except Exception as e:
                self._warn_exporter_unavailable("shutdown()", e)

    def _init_sdk(self):
        try:
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.sdk.resources import Resource

            pid = os.getpid()
            resource = Resource.create({
                "service.name": self._service_name,
                "service.instance.id": str(pid)
            })

            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as _OTLPHTTPMetricExporter

            manager = self

            class _QuietOTLPHTTPMetricExporter(_OTLPHTTPMetricExporter):
                def export(self, metrics_data, timeout_millis: float = 10_000, **kwargs):
                    try:
                        return super().export(metrics_data, timeout_millis=timeout_millis, **kwargs)
                    except Exception as e:
                        # Never let exporter exceptions spam stderr or crash the reader thread
                        manager._warn_exporter_unavailable("export()", e)
                        # Signal failure to the reader without raising
                        try:
                            from opentelemetry.sdk.metrics.export import MetricExportResult
                            return MetricExportResult.FAILURE
                        except Exception:
                            return None

            exporter = _QuietOTLPHTTPMetricExporter(endpoint=self._endpoint)
            reader = PeriodicExportingMetricReader(exporter, export_interval_millis=self._export_interval_ms)
            self._provider = MeterProvider(metric_readers=[reader], resource=resource)

            from opentelemetry.metrics import set_meter_provider
            set_meter_provider(self._provider)
            self._meter = self._provider.get_meter(self._service_name)

        except Exception as e:
            self.enabled = False
            self._warn_exporter_unavailable("init_sdk()", e)

    def _iter_values(self, spec: ValueSpec):
        # Case 1: A single (value, attributes) tuple. This is the most common case for simple metrics.
        if isinstance(spec, tuple) and len(spec) == 2 and isinstance(spec[1], dict):
            yield spec
            return

        # Case 2: A list of items. This is for multi-attribute metrics.
        if isinstance(spec, list) or isinstance(spec, tuple):
            for item in spec:
                # Each item can be a raw value or a (value, attributes) tuple
                yield item if isinstance(item, tuple) else (item, None)
        
        # Case 3: A single raw value.
        else:
            yield (spec, None)

    def _apply_counters(self, data: Dict[str, ValueSpec]):
        """Use a standard synchronous counter. Call add() for each value."""
        for name, spec in data.items():
            inst = self._counters.get(name)
            if inst is None:
                inst = self._meter.create_counter(name)
                self._counters[name] = inst

            for value, attrs in self._iter_values(spec):
                if value is not None:
                    inst.add(value, attrs)

    def _apply_gauges(self, data: Dict[str, ValueSpec]):
        """Use an observable gauge, keeping state within the manager."""
        for name, spec in data.items():
            if name not in self._updown:
                # Create the observable gauge with a callback one time.
                self._meter.create_observable_gauge(name, [self._make_gauge_callback(name)])
                self._updown[name] = True  # Mark as created
                self._latest_specs[name] = {} # Prep state storage

            # Update the state with the latest values from this batch.
            for value, attrs in self._iter_values(spec):
                if value is not None:
                    attr_key = frozenset(attrs.items()) if attrs else None
                    self._latest_specs[name][attr_key] = value

    def _apply_histograms(self, data: Dict[str, ValueSpec]):
        for name, spec in data.items():
            inst = self._histograms.get(name)
            if inst is None:
                inst = self._meter.create_histogram(name)
                self._histograms[name] = inst
            for value, attrs in self._iter_values(spec):
                if value is None:
                    continue
                inst.record(value, attrs) if attrs else inst.record(value)

    def _make_gauge_callback(self, metric_name: str):
        """Creates a callback that reports the last known value for all attributes."""
        def callback(options) -> List[Observation]:
            observations: List[Observation] = []
            spec = self._latest_specs.get(metric_name, {}) or {}
            seen: Dict[Optional[Tuple[Tuple[str, Any], ...]], Tuple[Number, Optional[Dict[str, Any]]]] = {}
            for attr_key, value in spec.items():
                if value is None:
                    continue
                attrs = dict(attr_key) if attr_key else None
                key = tuple(sorted(attrs.items())) if attrs else None
                seen[key] = (value, attrs)
            for value, attrs in seen.values():
                observations.append(Observation(value, attrs))
            return observations
        return callback