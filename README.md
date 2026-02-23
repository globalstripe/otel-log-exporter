# CDN Logs → OpenTelemetry (Grafana Alloy)

Utility to read **G-Core CDN access logs** from an S3 bucket, parse them, and send them as **OpenTelemetry logs** to a **Grafana Alloy** (or any OTLP) endpoint.

## Prerequisites

- Python 3.10+
- AWS credentials configured (e.g. `AWS_PROFILE` or default credentials) for S3 access
- Grafana Alloy (or another OTLP receiver) listening for logs on gRPC (e.g. port 4317)

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
# or editable install
pip install -e .
```

## Running Alloy and Grafana with Docker

To run Grafana Alloy, Loki, and Grafana locally (e.g. in Docker Desktop):

```bash
./docker-up.sh
```

This starts:

- **Alloy** – receives OTLP logs on port 4317 (gRPC) and 4318 (HTTP), UI at http://localhost:14251
- **Loki** – stores logs
- **Grafana** – http://localhost:3000 (login: `admin` / `admin`)

After starting, run the collector with `--endpoint localhost:4317`. In Grafana go to **Explore** → choose **Loki** → query e.g. `{service_name="cdn-logs-collector"}`.

To stop: `docker compose down`.

## Alloy configuration (reference)

The repo includes `alloy/config.alloy` used by the Docker setup. It receives OTLP, batches, and exports to Loki’s OTLP endpoint.

## Usage

**List S3 objects only (dry run):**

```bash
python -m src.collector --dry-run
```

**Process logs from the last 60 minutes and send to Alloy on localhost:**

```bash
python -m src.collector --since-minutes 60 --endpoint localhost:4317
```

**Using environment variables:**

```bash
export CDN_LOGS_BUCKET=amzn-gcore-logs
export CDN_LOGS_PREFIX=gcore/logs/
export OTEL_EXPORTER_OTLP_ENDPOINT=alloy:4317
export AWS_PROFILE=myprofile
python -m src.collector --since-minutes 120
```

**Limit scope (for testing):**

```bash
python -m src.collector --since-minutes 5 --max-objects 2 --max-lines-per-file 100
```

**Inspect logs for CMCD query strings (no export):**

```bash
python -m src.collector --inspect-cmcd --since-minutes 900 -v
```

Prints each log line that contains CMCD in the request URL: the `request` field and parsed `CMCD` key-value pairs. Use `--max-objects` / `--max-lines-per-file` to limit scope.

### Options

| Option | Env | Default | Description |
|--------|-----|---------|-------------|
| `--bucket` | `CDN_LOGS_BUCKET` | `amzn-gcore-logs` | S3 bucket name |
| `--prefix` | (see below) | `/gcore/logs/` and `/5gemerge/logs/` | S3 key prefix; can be repeated. This bucket uses keys with a leading slash (e.g. `/5gemerge/logs/...`). If set, `CDN_LOGS_PREFIX` is used as the only default. |
| `--since-minutes` | — | (all) | Only process objects modified in the last N minutes |
| `--endpoint` | `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTLP gRPC endpoint (host:port) |
| `--no-insecure` | — | false | Use TLS for OTLP |
| `--service-name` | `OTEL_SERVICE_NAME` | `cdn-logs-collector` | Service name in resource |
| `--aws-profile` | `AWS_PROFILE` | — | AWS profile for S3 |
| `--dry-run` | — | false | Only list S3 keys, do not send logs |
| `--max-objects` | — | — | Max S3 objects to process |
| `--max-lines-per-file` | — | — | Max lines per file (for testing) |

## Log format

- **Input:** G-Core CDN raw access logs in S3, path pattern  
  `{prefix}/YYYY/MM/DD/HH/mm/ss/{edgename}_{cname}_access.log.gz`  
  (see [G-Core Raw logs](https://gcore.com/docs/cdn/logs/raw-logs-export-cdn-resource-logs-to-your-storage).)
- **Output:** OTLP log records with body = raw log line and attributes:
  - **cdn.*** — CDN log fields (e.g. `cdn.remote_addr`, `cdn.status`, `cdn.host`, `cdn.request`, `cdn.upstream_cache_status`, `cdn.s3_key`).
  - **cmcd.*** — [Common Media Client Data](https://dashif.org/DASH-IF-IOP/cmcd/) parsed from the request query string when present. Supports:
    - Single parameter: `?cmcd=br=3200,bl=12500,d=400.2,ot=v,sid=...`
    - Prefixed parameters: `?cmcd.br=3200&cmcd.ot=v&cmcd.sid=...`  
    Attributes include e.g. `cmcd.br` (bitrate), `cmcd.bl` (buffer length), `cmcd.ot` (object type), `cmcd.sid` (session ID), `cmcd.cid` (content ID), and other CMCD keys sent by the player.

## Running as a cron or loop

To run periodically (e.g. every 5 minutes) for recent logs:

```bash
# cron (every 5 min), process last 10 minutes of objects
*/5 * * * * cd /path/to/otel-logs-collector && python -m src.collector --since-minutes 10 >> /var/log/cdn-otel.log 2>&1
```

Or run once with a time window that matches your CDN delivery delay (e.g. `--since-minutes 120`).
# otel-log-exporter
