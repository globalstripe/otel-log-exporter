# CDN Logs Collector (TypeScript)

TypeScript/Node.js port of the Python CDN logs collector. Reads G-Core CDN access logs from S3, parses them (including CMCD from query strings), and exports as OpenTelemetry logs to Grafana Alloy (OTLP gRPC).

## Prerequisites

- Node.js 18+
- AWS credentials configured (e.g. `AWS_PROFILE` or default) for S3 access

## Install

```bash
cd ts
npm install
npm run build
```

## Usage

Same CLI options as the Python version:

```bash
# Verify bucket access
node dist/collector.js --verify

# Dry run (list objects only)
node dist/collector.js --dry-run

# Process last 60 minutes, send to Alloy
node dist/collector.js --since-minutes 60 --endpoint localhost:4317

# Inspect CMCD only (no export)
node dist/collector.js --since-minutes 60 --inspect-cmcd -v

# Single S3 key
node dist/collector.js --key "/5gemerge/logs/2026/02/23/23/08/12/file_access.log.gz" --inspect-cmcd -v
```

Or after `npm run build`:

```bash
npm start -- --since-minutes 60 --endpoint localhost:4317
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--bucket` | `CDN_LOGS_BUCKET` or `amzn-gcore-logs` | S3 bucket |
| `--prefix` | `/gcore/logs/`, `/5gemerge/logs/` | S3 prefix (repeatable) |
| `--since-minutes` | (all) | Only objects modified in last N minutes |
| `--endpoint` | `localhost:4317` | OTLP gRPC endpoint |
| `--key` | — | Process only this S3 object key |
| `--verify` | — | Verify bucket access and exit |
| `--dry-run` | — | List objects only |
| `--inspect-cmcd` | — | Print CMCD lines, no OTLP export |
| `-v`, `--verbose` | — | Progress output |
| `--max-objects` | — | Max S3 objects to process |
| `--max-lines-per-file` | — | Max lines per file |

## Environment

- `CDN_LOGS_BUCKET` – S3 bucket name
- `CDN_LOGS_PREFIX` – Single S3 prefix (overrides default prefixes)
- `OTEL_EXPORTER_OTLP_ENDPOINT` – OTLP gRPC endpoint
- `OTEL_SERVICE_NAME` – Service name (default: `cdn-logs-collector`)
- `AWS_PROFILE` – AWS profile for S3
- `AWS_REGION` – AWS region (default: `eu-west-1`)
