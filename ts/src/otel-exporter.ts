/**
 * Send OpenTelemetry log records to Grafana Alloy (or any OTLP gRPC) endpoint.
 */

import { SeverityNumber } from '@opentelemetry/api-logs';
import {
  LoggerProvider,
  BatchLogRecordProcessor,
} from '@opentelemetry/sdk-logs';
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-grpc';
import { Resource } from '@opentelemetry/resources';
import type { Logger } from '@opentelemetry/api-logs';
import type { ParsedCDNLog } from './cdn-log-parser.js';

const G_CORE_TIME_RE = /^\[?(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2}\s+[+-]\d{4})\]?$/;

function parseTimestamp(timeLocal: string): bigint | null {
  if (!timeLocal?.trim()) return null;
  const m = timeLocal.trim().replace(/^\[|\]$/g, '').match(G_CORE_TIME_RE);
  if (!m) return null;
  try {
    // e.g. "26/Apr/2019:09:47:40 +0000"
    const str = m[1];
    const day = parseInt(str.slice(0, 2), 10);
    const months: Record<string, number> = {
      Jan: 0, Feb: 1, Mar: 2, Apr: 3, May: 4, Jun: 5,
      Jul: 6, Aug: 7, Sep: 8, Oct: 9, Nov: 10, Dec: 11,
    };
    const mon = months[str.slice(3, 6)];
    const year = parseInt(str.slice(7, 11), 10);
    const hour = parseInt(str.slice(12, 14), 10);
    const min = parseInt(str.slice(15, 17), 10);
    const sec = parseInt(str.slice(18, 20), 10);
    const tz = str.slice(21); // e.g. +0000
    const tzSign = tz.startsWith('+') ? 1 : -1;
    const tzH = parseInt(tz.slice(1, 3), 10);
    const tzM = parseInt(tz.slice(3, 5), 10);
    const offsetMs = tzSign * (tzH * 60 + tzM) * 60 * 1000;
    const date = new Date(Date.UTC(year, mon, day, hour, min, sec) - offsetMs);
    return BigInt(date.getTime()) * 1_000_000n;
  } catch {
    return null;
  }
}

export function createLoggerProvider(
  serviceName: string = 'cdn-logs-collector',
  endpoint?: string,
  _insecure: boolean = true,
  resourceAttributes?: Record<string, string>
): { provider: LoggerProvider; logger: Logger } {
  const resource = new Resource({
    'service.name': serviceName,
    ...resourceAttributes,
  });
  const provider = new LoggerProvider({ resource });
  const exporterOpts = endpoint
    ? { url: endpoint.includes('://') ? endpoint : `http://${endpoint}` }
    : {};
  const logExporter = new OTLPLogExporter(exporterOpts as { url?: string });
  provider.addLogRecordProcessor(new BatchLogRecordProcessor(logExporter));
  const logger = provider.getLogger('cdn-logs');
  return { provider, logger };
}

export function emitParsedLog(
  logger: Logger,
  parsed: ParsedCDNLog,
  s3Key?: string,
  observedTsNs?: bigint
): void {
  const tsNs = parseTimestamp(parsed.time_local) ?? BigInt(Date.now()) * 1_000_000n;
  const observed = observedTsNs ?? BigInt(Date.now()) * 1_000_000n;
  const attrs: Record<string, string> = { ...parsed.attributes };
  if (s3Key) attrs['cdn.s3_key'] = s3Key;
  // TimeInput: number (ms) or HrTime [s, ns]. Use ms for compatibility.
  const timestampMs = Number(tsNs / 1_000_000n);
  const observedMs = Number(observed / 1_000_000n);
  logger.emit({
    body: parsed.raw,
    timestamp: timestampMs,
    observedTimestamp: observedMs,
    severityNumber: SeverityNumber.INFO,
    severityText: 'INFO',
    attributes: attrs,
  });
}
