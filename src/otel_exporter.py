"""
Send OpenTelemetry log records to a Grafana Alloy (or any OTLP) endpoint.
"""
import time
from typing import Any, Optional

from opentelemetry._logs import get_logger, set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from .cdn_log_parser import ParsedCDNLog


def _parse_timestamp(time_local: str) -> Optional[int]:
    """
    Parse G-Core time_local like '[26/Apr/2019:09:47:40 +0000]' or '26/Apr/2019:09:47:40 +0000'
    to nanoseconds since epoch. Returns None if parsing fails.
    """
    if not time_local:
        return None
    s = time_local.strip("[]")
    try:
        # e.g. "26/Apr/2019:09:47:40 +0000"
        from datetime import datetime
        dt = datetime.strptime(s, "%d/%b/%Y:%H:%M:%S %z")
        return int(dt.timestamp() * 1_000_000_000)
    except Exception:
        return None


def create_logger_provider(
    service_name: str = "cdn-logs-collector",
    endpoint: Optional[str] = None,
    insecure: bool = True,
    resource_attributes: Optional[dict[str, str]] = None,
) -> tuple[LoggerProvider, Any]:
    """
    Create a LoggerProvider with OTLP gRPC log exporter and return (provider, logger).
    endpoint: e.g. "localhost:4317" or "alloy:4317". If None, uses OTEL_EXPORTER_OTLP_ENDPOINT.
    """
    resource = Resource.create(
        {"service.name": service_name, **(resource_attributes or {})}
    )
    provider = LoggerProvider(resource=resource)
    exporter = OTLPLogExporter(endpoint=endpoint, insecure=insecure)
    provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
    set_logger_provider(provider)
    logger = get_logger("cdn-logs")
    return provider, logger


def emit_parsed_log(
    logger: Any,
    parsed: ParsedCDNLog,
    s3_key: Optional[str] = None,
    observed_ts_ns: Optional[int] = None,
) -> None:
    """
    Emit a parsed CDN log line as an OpenTelemetry log record.
    Body is the raw line; attributes include all CDN fields plus optional s3_key.
    """
    ts_ns = _parse_timestamp(parsed.time_local) or (int(time.time() * 1_000_000_000))
    if observed_ts_ns is None:
        observed_ts_ns = int(time.time() * 1_000_000_000)
    attrs: dict[str, Any] = dict(parsed.attributes)
    if s3_key:
        attrs["cdn.s3_key"] = s3_key
    # Ensure string values for OTEL attributes
    attrs = {k: str(v) for k, v in attrs.items()}
    logger.emit(
        body=parsed.raw,
        timestamp=ts_ns,
        observed_timestamp=observed_ts_ns,
        severity_number=9,  # INFO
        severity_text="INFO",
        attributes=attrs,
    )
