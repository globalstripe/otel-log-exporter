#!/usr/bin/env bash
# Bring up Grafana Alloy, Loki, and Grafana in Docker Desktop.
# OTLP logs from the collector (localhost:4317) -> Alloy -> Loki -> Grafana.

set -e
cd "$(dirname "$0")"

echo "Starting Alloy, Loki, and Grafana..."
docker compose up -d

echo ""
echo "Containers:"
docker compose ps

echo ""
echo "URLs (wait ~10s for services to be ready):"
echo "  Grafana:    http://localhost:3000   (admin / admin)"
echo "  Alloy UI:   http://localhost:14251"
echo "  OTLP gRPC:  localhost:4317  (CDN collector sends here)"
echo ""
echo "To view CDN logs: Grafana -> Explore -> select Loki -> query e.g. {service_name=\"cdn-logs-collector\"}"
echo ""
echo "To stop: docker compose down"

# Down and Up
# docker compose down && docker compose up -d
