import os
from opentelemetry import trace
from prometheus_client import Counter, Histogram

RUNS = Counter("baltic_agent_runs_total", "Completed agent runs", ["level", "outcome"])
LATENCY = Histogram(
    "baltic_job_queue_seconds",
    "Time from inbox arrival to run start",
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
DURATION = Histogram("baltic_agent_run_seconds", "Agent run duration", ["level"])
INGEST = Counter("baltic_observations_total", "Accepted input observations", ["source"])


def setup():
    if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        provider = TracerProvider(resource=Resource.create({"service.name": "baltic-guide"}))
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)
    return trace.get_tracer("baltic-guide")
