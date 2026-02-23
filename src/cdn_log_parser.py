"""
Parse G-Core CDN access log lines (quoted-field format).
See: https://gcore.com/docs/cdn/logs/raw-logs-export-cdn-resource-logs-to-your-storage
CMCD (Common Media Client Data) is parsed from request query strings when present.
"""
from dataclasses import dataclass
from typing import Any, Optional

from .cmcd_parser import parse_cmcd_from_path, parse_cmcd_from_query_string


def _parse_quoted_fields(line: str) -> list[str]:
    """Parse a log line into fields; each field is double-quoted (may contain spaces)."""
    fields: list[str] = []
    i = 0
    n = len(line)
    while i < n:
        if line[i] == '"':
            i += 1
            start = i
            while i < n and line[i] != '"':
                if line[i] == "\\":
                    i += 1  # skip escaped char
                i += 1
            fields.append(line[start:i].replace('\\"', '"'))
            if i < n:
                i += 1  # consume closing quote
        else:
            i += 1  # skip whitespace between fields
    return fields


# G-Core log field order (from docs). New fields may be appended; we use indices for known ones.
FIELD_NAMES = [
    "remote_addr",
    "_",  # -
    "remote_user",
    "time_local",
    "request",
    "status",
    "body_bytes_sent",
    "http_referer",
    "http_user_agent",
    "bytes_sent",
    "edgename",
    "scheme",
    "host",
    "request_time",
    "upstream_response_time",
    "request_length",
    "http_range",
    "responding_node",
    "upstream_cache_status",
    "upstream_response_length",
    "upstream_addr",
    "gcdn_api_client_id",
    "gcdn_api_resource_id",
    "uid_got",
    "uid_set",
    "geoip_country_code",
    "geoip_city",
    "shield_type",
    "server_addr",
    "server_port",
    "upstream_status",
    "_2",
    "upstream_connect_time",
    "upstream_header_time",
    "shard_addr",
    "geoip2_data_asnumber",
    "connection",
    "connection_requests",
    "http_traceparent",
    "http_x_forwarded_proto",
    "gcdn_internal_status_code",
    "ssl_cipher",
    "ssl_session_id",
    "ssl_session_reused",
    "sent_http_content_type",
    "tcpinfo_rtt",
    "server_country_code",
    "gcdn_tcpinfo_snd_cwnd",
    "gcdn_tcpinfo_total_retrans",
    "gcdn_rule_id",
]


@dataclass
class ParsedCDNLog:
    """Parsed G-Core CDN access log record with common fields."""
    raw: str
    remote_addr: str = ""
    time_local: str = ""
    request: str = ""
    method: str = ""
    path: str = ""
    status: str = ""
    body_bytes_sent: str = ""
    http_referer: str = ""
    http_user_agent: str = ""
    bytes_sent: str = ""
    edgename: str = ""
    scheme: str = ""
    host: str = ""
    request_time: str = ""
    upstream_cache_status: str = ""
    geoip_country_code: str = ""
    sent_http_content_type: str = ""
    attributes: dict[str, Any] = None  # all fields as attributes for OTEL

    def __post_init__(self) -> None:
        if self.attributes is None:
            self.attributes = {}


def _strip_brackets(s: str) -> str:
    return s.strip("[]") if s else ""


def parse_request(request: str) -> tuple[str, str]:
    """Parse '$request' (e.g. 'GET /path HTTP/1.1') into method and path."""
    parts = request.split(None, 2)
    if len(parts) >= 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return parts[0], ""
    return "", ""


def _find_request_field(fields: list[str]) -> str:
    """Find the field that looks like 'GET /path HTTP/1.1'. G-Core exports can use different column orders."""
    for f in fields:
        s = (f or "").strip()
        if s.startswith("GET ") or s.startswith("POST ") or s.startswith("HEAD ") or s.startswith("PUT ") or s.startswith("OPTIONS "):
            if " " in s[4:].strip() and "/" in s:
                return s
    return ""


def _find_cmcd_raw_field(fields: list[str]) -> str:
    """Find a field that is the raw query string starting with 'CMCD='. Use as fallback when request field has no query."""
    for f in fields:
        s = (f or "").strip()
        if s.startswith("CMCD=") or s.startswith("cmcd="):
            return s
    return ""


def parse_gcore_log_line(line: str) -> Optional[ParsedCDNLog]:
    """
    Parse a single G-Core CDN access log line (quoted-field format).
    Returns ParsedCDNLog or None if the line is empty/invalid.
    """
    line = line.strip()
    if not line:
        return None
    fields = _parse_quoted_fields(line)
    if len(fields) < 10:
        return None

    def get(idx: int, default: str = "") -> str:
        if 0 <= idx < len(fields):
            return fields[idx].strip() or default
        return default

    attrs: dict[str, Any] = {}
    for idx, name in enumerate(FIELD_NAMES):
        if name.startswith("_"):
            continue
        if idx < len(fields):
            attrs[f"cdn.{name}"] = fields[idx].strip() or "-"

    time_local = _strip_brackets(get(3))
    # Request can be at different indices depending on G-Core export config; find it by content
    request = _find_request_field(fields) or get(4)
    method, path = parse_request(request)

    # CMCD (Common Media Client Data) from query string: from path (?CMCD=...) or raw "CMCD=..." field
    cmcd_attrs = parse_cmcd_from_path(path)
    if not cmcd_attrs:
        raw_cmcd = _find_cmcd_raw_field(fields)
        if raw_cmcd:
            cmcd_attrs = parse_cmcd_from_query_string(raw_cmcd)
    for k, v in cmcd_attrs.items():
        if v:
            attrs[k] = v

    return ParsedCDNLog(
        raw=line,
        remote_addr=get(0),
        time_local=time_local,
        request=request,
        method=method,
        path=path,
        status=get(5),
        body_bytes_sent=get(6),
        http_referer=get(7),
        http_user_agent=get(8),
        bytes_sent=get(9),
        edgename=_strip_brackets(get(10)),
        scheme=get(11),
        host=get(12),
        request_time=get(13),
        upstream_cache_status=get(18),
        geoip_country_code=get(25),
        sent_http_content_type=get(39),
        attributes=attrs,
    )
