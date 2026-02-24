/**
 * Parse CMCD (Common Media Client Data) from request query strings.
 * CTA-5004 / DASH-IF: client media metrics as query params (e.g. cmcd=key=val,... or cmcd.key=val).
 */

function parseQueryString(queryString: string): Record<string, string[]> {
  const params: Record<string, string[]> = {};
  const searchParams = new URLSearchParams(queryString);
  for (const [key, value] of searchParams.entries()) {
    if (!params[key]) params[key] = [];
    params[key].push(value);
  }
  return params;
}

function cleanValue(v: string): string {
  const s = (v ?? '').trim();
  if (s.length >= 2 && s.startsWith('"') && s.endsWith('"')) {
    return s.slice(1, -1);
  }
  return s;
}

/**
 * Extract CMCD key-value pairs from a URL query string.
 * Supports single param CMCD=bl=17900,br=5300,... or prefixed cmcd.br=3200.
 */
export function parseCmcdFromQueryString(queryString: string): Record<string, string> {
  if (!queryString?.trim()) return {};
  const out: Record<string, string> = {};
  const params = parseQueryString(queryString);

  // Single "cmcd" or "CMCD" parameter: comma-separated key=value
  const cmcdKey = Object.keys(params).find((k) => k.toLowerCase() === 'cmcd');
  const cmcdParam = cmcdKey ? params[cmcdKey] : null;
  if (cmcdParam) {
    for (const raw of cmcdParam) {
      for (const pair of raw.split(',')) {
        const trimmed = pair.trim();
        const eq = trimmed.indexOf('=');
        if (eq !== -1) {
          const k = trimmed.slice(0, eq).trim();
          const v = trimmed.slice(eq + 1).trim();
          if (k) out[`cmcd.${k}`] = cleanValue(v);
        }
      }
    }
  }

  // Prefixed parameters: cmcd.br=3200, cmcd.ot=v
  for (const [key, values] of Object.entries(params)) {
    if (key.startsWith('cmcd.') && values?.length) {
      const cmcdKeyName = key.slice(5);
      if (cmcdKeyName) out[`cmcd.${cmcdKeyName}`] = cleanValue(values[0] ?? '');
    }
  }
  return out;
}

/**
 * Extract CMCD from path that may include ?query.
 */
export function parseCmcdFromPath(pathWithQuery: string): Record<string, string> {
  if (!pathWithQuery) return {};
  const q = pathWithQuery.indexOf('?');
  if (q !== -1) {
    return parseCmcdFromQueryString(pathWithQuery.slice(q + 1));
  }
  return {};
}
