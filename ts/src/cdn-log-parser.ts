/**
 * Parse G-Core CDN access log lines (quoted-field format).
 * CMCD is parsed from request query strings when present.
 */

import { parseCmcdFromPath, parseCmcdFromQueryString } from './cmcd-parser.js';

const FIELD_NAMES = [
  'remote_addr', '_', 'remote_user', 'time_local', 'request', 'status',
  'body_bytes_sent', 'http_referer', 'http_user_agent', 'bytes_sent',
  'edgename', 'scheme', 'host', 'request_time', 'upstream_response_time',
  'request_length', 'http_range', 'responding_node', 'upstream_cache_status',
  'upstream_response_length', 'upstream_addr', 'gcdn_api_client_id',
  'gcdn_api_resource_id', 'uid_got', 'uid_set', 'geoip_country_code',
  'geoip_city', 'shield_type', 'server_addr', 'server_port', 'upstream_status',
  '_2', 'upstream_connect_time', 'upstream_header_time', 'shard_addr',
  'geoip2_data_asnumber', 'connection', 'connection_requests', 'http_traceparent',
  'http_x_forwarded_proto', 'gcdn_internal_status_code', 'ssl_cipher',
  'ssl_session_id', 'ssl_session_reused', 'sent_http_content_type', 'tcpinfo_rtt',
  'server_country_code', 'gcdn_tcpinfo_snd_cwnd', 'gcdn_tcpinfo_total_retrans',
  'gcdn_rule_id',
];

export interface ParsedCDNLog {
  raw: string;
  remote_addr: string;
  time_local: string;
  request: string;
  method: string;
  path: string;
  status: string;
  body_bytes_sent: string;
  http_referer: string;
  http_user_agent: string;
  bytes_sent: string;
  edgename: string;
  scheme: string;
  host: string;
  request_time: string;
  upstream_cache_status: string;
  geoip_country_code: string;
  sent_http_content_type: string;
  attributes: Record<string, string>;
}

function parseQuotedFields(line: string): string[] {
  const fields: string[] = [];
  let i = 0;
  const n = line.length;
  while (i < n) {
    if (line[i] === '"') {
      i += 1;
      const start = i;
      while (i < n && line[i] !== '"') {
        if (line[i] === '\\') i += 1;
        i += 1;
      }
      fields.push(line.slice(start, i).replace(/\\"/g, '"'));
      if (i < n) i += 1;
    } else {
      i += 1;
    }
  }
  return fields;
}

function stripBrackets(s: string): string {
  return s ? s.replace(/^\[|\]$/g, '') : '';
}

function parseRequest(request: string): [string, string] {
  const parts = request.trim().split(/\s+/, 3);
  if (parts.length >= 2) return [parts[0], parts[1]];
  if (parts.length === 1) return [parts[0], ''];
  return ['', ''];
}

function findRequestField(fields: string[]): string {
  const methods = ['GET ', 'POST ', 'HEAD ', 'PUT ', 'OPTIONS '];
  for (const f of fields) {
    const s = (f ?? '').trim();
    for (const m of methods) {
      if (s.startsWith(m) && s.includes('/') && s.slice(m.length).trim().includes(' ')) {
        return s;
      }
    }
  }
  return '';
}

function findCmcdRawField(fields: string[]): string {
  for (const f of fields) {
    const s = (f ?? '').trim();
    if (s.startsWith('CMCD=') || s.startsWith('cmcd=')) return s;
  }
  return '';
}

export function parseGcoreLogLine(line: string): ParsedCDNLog | null {
  const trimmed = line.trim();
  if (!trimmed) return null;
  const fields = parseQuotedFields(trimmed);
  if (fields.length < 10) return null;

  const get = (idx: number, def = ''): string =>
    (idx >= 0 && idx < fields.length ? (fields[idx]?.trim() || def) : def);

  const attrs: Record<string, string> = {};
  for (let idx = 0; idx < FIELD_NAMES.length && idx < fields.length; idx++) {
    const name = FIELD_NAMES[idx];
    if (name.startsWith('_')) continue;
    attrs[`cdn.${name}`] = fields[idx]?.trim() || '-';
  }

  const time_local = stripBrackets(get(3));
  const request = findRequestField(fields) || get(4);
  const [method, path] = parseRequest(request);

  let cmcdAttrs = parseCmcdFromPath(path);
  if (Object.keys(cmcdAttrs).length === 0) {
    const rawCmcd = findCmcdRawField(fields);
    if (rawCmcd) cmcdAttrs = parseCmcdFromQueryString(rawCmcd);
  }
  for (const [k, v] of Object.entries(cmcdAttrs)) {
    if (v) attrs[k] = v;
  }

  return {
    raw: trimmed,
    remote_addr: get(0),
    time_local,
    request,
    method,
    path,
    status: get(5),
    body_bytes_sent: get(6),
    http_referer: get(7),
    http_user_agent: get(8),
    bytes_sent: get(9),
    edgename: stripBrackets(get(10)),
    scheme: get(11),
    host: get(12),
    request_time: get(13),
    upstream_cache_status: get(18),
    geoip_country_code: get(25),
    sent_http_content_type: get(39),
    attributes: attrs,
  };
}
