"""
Orchestrate: list S3 log objects, download, parse, and export as OTLP logs to Grafana Alloy.
"""
import argparse
import os
import sys
from datetime import datetime, timezone
from typing import List, Optional

from botocore.exceptions import TokenRetrievalError

from .cdn_log_parser import parse_gcore_log_line
from .otel_exporter import create_logger_provider, emit_parsed_log
from .s3_source import list_log_objects, list_raw_keys, stream_log_lines, verify_bucket_access


def _normalize_prefix(p: str) -> str:
    return p if p.endswith("/") else (p + "/")


def _iter_keys(
    bucket: str,
    single_key: Optional[str],
    prefixes: List[str],
    since: Optional[datetime],
    aws_profile: Optional[str],
    max_objects: Optional[int],
):
    """Yield (key, last_modified, size). Either one key or from list_log_objects over prefixes."""
    if single_key:
        yield (single_key, datetime.now(timezone.utc), 0)
        return
    count = 0
    for prefix in prefixes:
        for key, last_mod, size in list_log_objects(
            bucket, prefix=prefix, since=since, profile_name=aws_profile
        ):
            yield key, last_mod, size
            count += 1
            if max_objects and count >= max_objects:
                return


def run(
    bucket: str,
    prefixes: List[str],
    since_minutes: Optional[int] = None,
    endpoint: Optional[str] = None,
    insecure: bool = True,
    service_name: str = "cdn-logs-collector",
    aws_profile: Optional[str] = None,
    dry_run: bool = False,
    max_objects: Optional[int] = None,
    max_lines_per_file: Optional[int] = None,
    verbose: bool = False,
    inspect_cmcd: bool = False,
    key: Optional[str] = None,
) -> int:
    """
    List S3 log objects under each prefix (or fetch a single key), parse each line as G-Core CDN log, emit as OTLP logs.
    Returns number of log records emitted (or that would be emitted if dry_run).
    """
    since = None
    if since_minutes is not None:
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)

    prefixes = [_normalize_prefix(p) for p in prefixes]
    keys_iter = _iter_keys(bucket, key, prefixes, since, aws_profile, max_objects if not key else 1)

    if dry_run:
        count = 0
        for key_item, _last_mod, size in keys_iter:
            print(f"Would process: s3://{bucket}/{key_item} ({size} bytes)", file=sys.stderr)
            count += 1
        return 0

    if inspect_cmcd:
        # Only scan logs and print lines that have CMCD query strings (no OTLP export)
        cmcd_count = 0
        total_lines = 0
        objects_processed = 0
        if verbose and key:
            print(f"Fetching single key: s3://{bucket}/{key}", file=sys.stderr)
        for key_item, _last_mod, _size in keys_iter:
            if verbose and not key and objects_processed == 0:
                since_msg = f"since last {since_minutes} min" if since_minutes is not None else "all time"
                path = f"s3://{bucket}{prefixes[0]}" if prefixes and prefixes[0].startswith("/") else f"s3://{bucket}/{prefixes[0] if prefixes else ''}"
                print(f"Listing {path} ({since_msg})...", file=sys.stderr)
            line_count = 0
            for line in stream_log_lines(bucket, key_item, profile_name=aws_profile):
                if max_lines_per_file and line_count >= max_lines_per_file:
                    break
                parsed = parse_gcore_log_line(line)
                if parsed:
                    total_lines += 1
                    cmcd_attrs = {k: v for k, v in parsed.attributes.items() if k.startswith("cmcd.")}
                    if cmcd_attrs:
                        cmcd_count += 1
                        request = getattr(parsed, "request", "") or parsed.attributes.get("cdn.request", "")
                        print(f"request: {request}", file=sys.stderr)
                        print(f"CMCD:    {cmcd_attrs}", file=sys.stderr)
                        print("---", file=sys.stderr)
                line_count += 1
            objects_processed += 1
        print(
            f"Inspect summary: {cmcd_count} lines with CMCD out of {total_lines} total (from {objects_processed} objects)",
            file=sys.stderr,
        )
        return 0

    provider, logger = create_logger_provider(
        service_name=service_name,
        endpoint=endpoint or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"),
        insecure=insecure,
    )
    emitted = 0
    objects_processed = 0
    total_lines_read = 0
    try:
        for key_item, _last_mod, _size in keys_iter:
            if verbose:
                if key:
                    print(f"Processing: s3://{bucket}/{key_item}", file=sys.stderr)
                elif objects_processed == 0:
                    since_msg = f"since last {since_minutes} min" if since_minutes is not None else "all time"
                    path = f"s3://{bucket}{prefixes[0]}" if prefixes and prefixes[0].startswith("/") else f"s3://{bucket}/{prefixes[0] if prefixes else ''}"
                    print(f"Listing {path} ({since_msg})...", file=sys.stderr)
            line_count = 0
            parsed_count = 0
            for line in stream_log_lines(bucket, key_item, profile_name=aws_profile):
                if max_lines_per_file and line_count >= max_lines_per_file:
                    break
                parsed = parse_gcore_log_line(line)
                if parsed:
                    cmcd_attrs = {k: v for k, v in parsed.attributes.items() if k.startswith("cmcd.")}
                    if cmcd_attrs:
                        print(f"CMCD: {cmcd_attrs}", file=sys.stderr)
                    emit_parsed_log(logger, parsed, s3_key=key_item)
                    emitted += 1
                    parsed_count += 1
                line_count += 1
            total_lines_read += line_count
            if verbose:
                print(f"  {key_item}: {line_count} lines, {parsed_count} parsed", file=sys.stderr)
            objects_processed += 1
            if max_objects and objects_processed >= max_objects:
                break
    finally:
        provider.shutdown()
    if verbose:
        print(
            f"Summary: {objects_processed} objects, {total_lines_read} lines read, {emitted} log records emitted",
            file=sys.stderr,
        )
        if objects_processed == 0:
            print(
                "Debug: 0 objects matched. Listing raw keys (no suffix filter) to check prefix/bucket...",
                file=sys.stderr,
            )
            for p in prefixes:
                raw = list_raw_keys(bucket, prefix=p, max_keys=5, profile_name=aws_profile)
                if not raw and not p.startswith("/"):
                    # Some S3-compatible backends use keys with leading slash
                    raw = list_raw_keys(bucket, prefix="/" + p, max_keys=5, profile_name=aws_profile)
                if not raw:
                    print(f"  (no keys under prefix {p!r})", file=sys.stderr)
                    continue
                for key, last_mod, size in raw:
                    print(f"  key={key!r}  last_modified={last_mod!s}  size={size}", file=sys.stderr)
                if since is not None:
                    print(f"  Cutoff was: last_modified >= {since!s} (UTC)", file=sys.stderr)
    return emitted


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Transform CDN logs from S3 to OpenTelemetry and send to Grafana Alloy"
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("CDN_LOGS_BUCKET", "amzn-gcore-logs"),
        help="S3 bucket name (default: CDN_LOGS_BUCKET or amzn-gcore-logs)",
    )
    parser.add_argument(
        "--prefix",
        action="append",
        default=None,
        dest="prefixes",
        metavar="PREFIX",
        help="S3 prefix for log objects; can be repeated. Default when not set: gcore/logs/ and 5gemerge/logs/",
    )
    parser.add_argument(
        "--since-minutes",
        type=int,
        default=None,
        help="Only process objects modified in the last N minutes",
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
        help="OTLP gRPC endpoint (default: localhost:4317 for Alloy)",
    )
    parser.add_argument(
        "--no-insecure",
        action="store_true",
        help="Use TLS for OTLP (default is insecure=true)",
    )
    parser.add_argument(
        "--service-name",
        default=os.environ.get("OTEL_SERVICE_NAME", "cdn-logs-collector"),
        help="Service name for resource (default: cdn-logs-collector)",
    )
    parser.add_argument(
        "--aws-profile",
        default=os.environ.get("AWS_PROFILE"),
        help="AWS profile for S3 (default: AWS_PROFILE)",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", "eu-west-1"),
        help="AWS region for the bucket (default: eu-west-1)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify read access to the bucket and exit (no log processing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list S3 objects that would be processed",
    )
    parser.add_argument(
        "--max-objects",
        type=int,
        default=None,
        help="Max S3 objects to process (for testing)",
    )
    parser.add_argument(
        "--max-lines-per-file",
        type=int,
        default=None,
        help="Max lines to process per file (for testing)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print progress: objects processed, lines read, parse counts",
    )
    parser.add_argument(
        "--inspect-cmcd",
        action="store_true",
        help="Only scan logs and print lines that have CMCD query strings (no OTLP export). Use to verify CMCD in requests.",
    )
    parser.add_argument(
        "--key",
        metavar="S3_KEY",
        default=None,
        help="Fetch and process only this S3 object key (e.g. /5gemerge/logs/2026/02/23/23/08/12/file_access.log.gz). Use with --inspect-cmcd to inspect one file.",
    )
    args = parser.parse_args()

    default_prefixes = ["/gcore/logs/", "/5gemerge/logs/"]
    if os.environ.get("CDN_LOGS_PREFIX"):
        default_prefixes = [os.environ.get("CDN_LOGS_PREFIX", "").rstrip("/") + "/"]
    prefixes = args.prefixes if args.prefixes is not None else default_prefixes

    try:
        if args.verify:
            ok = verify_bucket_access(
                bucket=args.bucket,
                region=args.region,
                profile_name=args.aws_profile,
                prefix=prefixes[0] if prefixes else "/5gemerge/logs/",
            )
            return 0 if ok else 1

        emitted = run(
            bucket=args.bucket,
            prefixes=prefixes,
            since_minutes=args.since_minutes,
            endpoint=args.endpoint,
            insecure=not args.no_insecure,
            service_name=args.service_name,
            aws_profile=args.aws_profile,
            dry_run=args.dry_run,
            max_objects=args.max_objects,
            max_lines_per_file=args.max_lines_per_file,
            verbose=args.verbose,
            inspect_cmcd=args.inspect_cmcd,
            key=args.key,
        )
        if not args.dry_run and not args.inspect_cmcd:
            print(f"Emitted {emitted} log records to {args.endpoint}", file=sys.stderr)
        return 0
    except TokenRetrievalError:
        print("AWS SSO token has expired. Log in again with:", file=sys.stderr)
        print("  aws sso login", file=sys.stderr)
        if args.aws_profile:
            print(f"  (or: aws sso login --profile {args.aws_profile})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
