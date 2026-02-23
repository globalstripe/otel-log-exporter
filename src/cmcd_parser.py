"""
Parse CMCD (Common Media Client Data) from request query strings.
CTA-5004 / DASH-IF: client media metrics sent as query params (e.g. cmcd=key=val,... or cmcd.key=val).
"""
from urllib.parse import parse_qs


# Known CMCD keys (v1 and common v2) for reference; we pass through any key=value.
CMCD_KEYS = frozenset({
    "br",   # bitrate (kbps)
    "bl",   # buffer length (ms)
    "d",    # duration (sec)
    "ot",   # object type (e.g. v, a, m, i)
    "tb",   # throughput (kbps)
    "mtp",  # measured throughput (kbps)
    "nor",  # next object request (relative path)
    "nrr",  # next range request (byte range)
    "cid",  # content ID
    "sid",  # session ID (UUID)
    "su",   # startup (boolean)
    "bs",   # buffer starvation (boolean)
    "rtp",  # requested maximum throughput (kbps)
    "pr",   # playback rate
    "sf",   # stream type (e.g. v, a)
    "st",   # stream type (v=video, a=audio, etc.)
    "v",    # version
    "dl",   # deadline (ms)
})


def parse_cmcd_from_query_string(query_string: str) -> dict[str, str]:
    """
    Extract CMCD key-value pairs from a URL query string.
    Supports:
      - Single parameter: cmcd=br=3200,bl=12500,d=400.2,ot=v,sid=...
      - Separate parameters: cmcd.br=3200&cmcd.bl=12500&cmcd.ot=v
    Returns a dict of cmcd.* attributes (e.g. cmcd.br, cmcd.sid). Values are strings.
    """
    if not query_string or not query_string.strip():
        return {}
    out: dict[str, str] = {}
    # Parse as form-style query string
    params = parse_qs(query_string, keep_blank_values=True)
    # 1) Single "cmcd" or "CMCD" parameter: comma-separated key=value (e.g. CMCD=bl=17900,br=5300,...)
    cmcd_param = next((params[k] for k in params if k.lower() == "cmcd"), None)
    def _clean(v: str) -> str:
        """Strip one level of surrounding double-quotes (CMCD string values)."""
        v = (v or "").strip()
        if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
            return v[1:-1]
        return v

    for raw in (cmcd_param or []):
        for pair in raw.split(","):
            pair = pair.strip()
            if "=" in pair:
                k, v = pair.split("=", 1)
                k, v = k.strip(), v.strip()
                if k:
                    out[f"cmcd.{k}"] = _clean(v)
    # 2) Prefixed parameters: cmcd.br=3200, cmcd.ot=v
    for key, values in params.items():
        if key.startswith("cmcd.") and values:
            cmcd_key = key[5:]  # after "cmcd."
            if cmcd_key:
                out[f"cmcd.{cmcd_key}"] = _clean(values[0] or "")
    return out


def parse_cmcd_from_path(path_with_query: str) -> dict[str, str]:
    """
    Extract CMCD from the path portion of a request (path may include ?query).
    Example: /vod/segment.m4s?cmcd=ot=v,br=3200 or /vod/segment.m4s?cmcd.br=3200
    """
    if not path_with_query:
        return {}
    if "?" in path_with_query:
        _path, _sep, qs = path_with_query.partition("?")
        return parse_cmcd_from_query_string(qs)
    return {}


if __name__ == "__main__":
    import sys
    print("src.cmcd_parser is a library, not a script. CMCD is parsed when you run the collector.", file=sys.stderr)
    print("To process S3 logs and send to Alloy (with CMCD in attributes and optional -v CMCD logging):", file=sys.stderr)
    print("  python -m src.collector --since-minutes 900 -v", file=sys.stderr)
    sys.exit(0)
