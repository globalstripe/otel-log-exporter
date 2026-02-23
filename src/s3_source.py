"""
List and stream G-Core CDN access logs from S3.
Paths follow: prefix/logs/YYYY/MM/DD/HH/mm/ss/{edgename}_{cname}_access.log.gz
"""
import gzip
import sys
from datetime import datetime, timezone
from typing import Iterator, List, Optional

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError


def verify_bucket_access(
    bucket: str,
    region: str = "eu-west-1",
    profile_name: Optional[str] = None,
    prefix: str = "/5gemerge/logs/",
) -> bool:
    """
    Verify read access to the bucket in the given region.
    Performs head_bucket and a minimal list (1 key) under prefix.
    Returns True if access OK, False otherwise. Prints result to stderr.
    """
    try:
        session = boto3.Session(profile_name=profile_name)
        client = session.client("s3", region_name=region)
        client.head_bucket(Bucket=bucket)
        # Optional: confirm we can list under the logs prefix
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        count = resp.get("KeyCount", 0)
        print(
            f"OK: Can access bucket s3://{bucket} (region={region}). "
            f"Prefix '{prefix}' has {'≥1 object' if count else 'no objects yet'}.",
            file=sys.stderr,
        )
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        msg = e.response.get("Error", {}).get("Message", str(e))
        print(
            f"ERROR: Cannot access bucket s3://{bucket} (region={region}): {code} - {msg}",
            file=sys.stderr,
        )
        return False
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return False


def list_raw_keys(
    bucket: str,
    prefix: str,
    max_keys: int = 10,
    profile_name: Optional[str] = None,
) -> List[tuple[str, datetime, int]]:
    """List up to max_keys objects under prefix (any suffix). For debugging."""
    session = boto3.Session(profile_name=profile_name)
    client = session.client("s3", region_name="eu-west-1")
    paginator = client.get_paginator("list_objects_v2")
    out: List[tuple[str, datetime, int]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            out.append((obj["Key"], obj["LastModified"], obj.get("Size", 0)))
            if len(out) >= max_keys:
                return out
    return out


def list_log_objects(
    bucket: str,
    prefix: str = "/5gemerge/logs/",
    since: Optional[datetime] = None,
    suffix: str = "_access.log.gz",
    profile_name: Optional[str] = None,
    use_unsigned: bool = False,
) -> Iterator[tuple[str, datetime, int]]:
    """
    List S3 objects under bucket/prefix that end with suffix.
    Yields (key, last_modified, size) for each object.
    If since is set, only yield objects last_modified >= since.
    """
    config = Config(signature_version=UNSIGNED) if use_unsigned else None
    session = boto3.Session(profile_name=profile_name)
    client = session.client("s3", config=config, region_name="eu-west-1")
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if not key.endswith(suffix):
                continue
            last_modified = obj["LastModified"]
            # Normalize to comparable: S3 returns aware datetime; since is UTC-aware
            if since:
                lm_utc = last_modified if last_modified.tzinfo else last_modified.replace(tzinfo=timezone.utc)
                if lm_utc < since:
                    continue
            size = obj.get("Size", 0)
            yield key, last_modified, size


def download_and_decompress(
    bucket: str,
    key: str,
    profile_name: Optional[str] = None,
    use_unsigned: bool = False,
) -> bytes:
    """Download an object from S3 and, if gzipped, decompress. Return raw bytes (utf-8 decodable)."""
    config = Config(signature_version=UNSIGNED) if use_unsigned else None
    session = boto3.Session(profile_name=profile_name)
    client = session.client("s3", config=config, region_name="eu-west-1")
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    # Some objects are named .gz but are plain text (e.g. under 5gemerge); only decompress real gzip
    if key.endswith(".gz") and len(body) >= 2 and body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return body


def stream_log_lines(
    bucket: str,
    key: str,
    profile_name: Optional[str] = None,
    use_unsigned: bool = False,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> Iterator[str]:
    """Download a log object from S3, decompress if needed, and yield lines."""
    raw = download_and_decompress(bucket, key, profile_name=profile_name, use_unsigned=use_unsigned)
    text = raw.decode(encoding, errors=errors)
    for line in text.splitlines():
        yield line
