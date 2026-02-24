#!/usr/bin/env node
/**
 * Orchestrate: list S3 log objects, download, parse, and export as OTLP logs to Grafana Alloy.
 */

import { parseGcoreLogLine } from './cdn-log-parser.js';
import { createLoggerProvider, emitParsedLog } from './otel-exporter.js';
import {
  listLogObjects,
  listRawKeys,
  streamLogLines,
  verifyBucketAccess,
  type S3ObjectInfo,
} from './s3-source.js';

const DEFAULT_BUCKET = process.env.CDN_LOGS_BUCKET ?? 'amzn-gcore-logs';
const DEFAULT_PREFIXES = ['/gcore/logs/', '/5gemerge/logs/'];
const DEFAULT_ENDPOINT = process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? 'localhost:4317';

function normalizePrefix(p: string): string {
  return p.endsWith('/') ? p : p + '/';
}

async function* iterKeys(
  bucket: string,
  singleKey: string | null,
  prefixes: string[],
  since: Date | null,
  profile: string | undefined,
  maxObjects: number | null
): AsyncGenerator<S3ObjectInfo> {
  if (singleKey) {
    yield { key: singleKey, lastModified: new Date(), size: 0 };
    return;
  }
  let count = 0;
  for (const prefix of prefixes) {
    for await (const obj of listLogObjects(bucket, prefix, since ?? undefined, undefined, profile)) {
      yield obj;
      count += 1;
      if (maxObjects != null && count >= maxObjects) return;
    }
  }
}

export interface RunOptions {
  bucket: string;
  prefixes: string[];
  sinceMinutes: number | null;
  endpoint: string | null;
  insecure: boolean;
  serviceName: string;
  awsProfile: string | undefined;
  dryRun: boolean;
  maxObjects: number | null;
  maxLinesPerFile: number | null;
  verbose: boolean;
  inspectCmcd: boolean;
  key: string | null;
}

export interface CliArgs extends RunOptions {
  verify: boolean;
  region: string;
}

export async function run(options: RunOptions): Promise<number> {
  const {
    bucket,
    prefixes,
    sinceMinutes,
    endpoint,
    insecure,
    serviceName,
    awsProfile,
    dryRun,
    maxObjects,
    maxLinesPerFile,
    verbose,
    inspectCmcd,
    key,
  } = options;

  const since =
    sinceMinutes != null
      ? new Date(Date.now() - sinceMinutes * 60 * 1000)
      : null;
  const normalizedPrefixes = prefixes.map(normalizePrefix);
  const keysGen = iterKeys(
    bucket,
    key,
    normalizedPrefixes,
    since,
    awsProfile,
    key ? 1 : maxObjects
  );

  if (dryRun) {
    for await (const obj of keysGen) {
      console.error(`Would process: s3://${bucket}/${obj.key} (${obj.size} bytes)`);
    }
    return 0;
  }

  if (inspectCmcd) {
    let cmcdCount = 0;
    let totalLines = 0;
    let objectsProcessed = 0;
    if (verbose && key) {
      console.error(`Fetching single key: s3://${bucket}/${key}`);
    }
    for await (const obj of keysGen) {
      if (verbose && !key && objectsProcessed === 0) {
        const sinceMsg = sinceMinutes != null ? `since last ${sinceMinutes} min` : 'all time';
        const path = normalizedPrefixes[0]?.startsWith('/')
          ? `s3://${bucket}${normalizedPrefixes[0]}`
          : `s3://${bucket}/${normalizedPrefixes[0] ?? ''}`;
        console.error(`Listing ${path} (${sinceMsg})...`);
      }
      let lineCount = 0;
      for await (const line of streamLogLines(bucket, obj.key, awsProfile)) {
        if (maxLinesPerFile != null && lineCount >= maxLinesPerFile) break;
        const parsed = parseGcoreLogLine(line);
        if (parsed) {
          totalLines += 1;
          const cmcdAttrs = Object.fromEntries(
            Object.entries(parsed.attributes).filter(([k]) => k.startsWith('cmcd.'))
          );
          if (Object.keys(cmcdAttrs).length > 0) {
            cmcdCount += 1;
            const request = parsed.request || parsed.attributes['cdn.request'] || '';
            console.error(`request: ${request}`);
            console.error(`CMCD:    ${JSON.stringify(cmcdAttrs)}`);
            console.error('---');
          }
        }
        lineCount += 1;
      }
      objectsProcessed += 1;
    }
    console.error(
      `Inspect summary: ${cmcdCount} lines with CMCD out of ${totalLines} total (from ${objectsProcessed} objects)`
    );
    return 0;
  }

  const { provider, logger } = createLoggerProvider(
    serviceName,
    endpoint ?? undefined,
    insecure
  );
  let emitted = 0;
  let objectsProcessed = 0;
  let totalLinesRead = 0;

  try {
    for await (const obj of keysGen) {
      if (verbose) {
        if (key) {
          console.error(`Processing: s3://${bucket}/${obj.key}`);
        } else if (objectsProcessed === 0) {
          const sinceMsg = sinceMinutes != null ? `since last ${sinceMinutes} min` : 'all time';
          const path = normalizedPrefixes[0]?.startsWith('/')
            ? `s3://${bucket}${normalizedPrefixes[0]}`
            : `s3://${bucket}/${normalizedPrefixes[0] ?? ''}`;
          console.error(`Listing ${path} (${sinceMsg})...`);
        }
      }
      let lineCount = 0;
      let parsedCount = 0;
      for await (const line of streamLogLines(bucket, obj.key, awsProfile)) {
        if (maxLinesPerFile != null && lineCount >= maxLinesPerFile) break;
        const parsed = parseGcoreLogLine(line);
        if (parsed) {
          const cmcdAttrs = Object.fromEntries(
            Object.entries(parsed.attributes).filter(([k]) => k.startsWith('cmcd.'))
          );
          if (Object.keys(cmcdAttrs).length > 0 && verbose) {
            console.error(`CMCD: ${JSON.stringify(cmcdAttrs)}`);
          }
          emitParsedLog(logger, parsed, obj.key);
          emitted += 1;
          parsedCount += 1;
        }
        lineCount += 1;
      }
      totalLinesRead += lineCount;
      if (verbose) {
        console.error(`  ${obj.key}: ${lineCount} lines, ${parsedCount} parsed`);
      }
      objectsProcessed += 1;
      if (maxObjects != null && objectsProcessed >= maxObjects) break;
    }
  } finally {
    await provider.shutdown();
  }

  if (verbose) {
    console.error(
      `Summary: ${objectsProcessed} objects, ${totalLinesRead} lines read, ${emitted} log records emitted`
    );
    if (objectsProcessed === 0) {
      console.error('Debug: 0 objects matched. Listing raw keys (no suffix filter)...');
      for (const p of normalizedPrefixes) {
        const raw = await listRawKeys(bucket, p, 5, awsProfile);
        const alt = !p.startsWith('/') ? await listRawKeys(bucket, '/' + p, 5, awsProfile) : [];
        const list = raw.length ? raw : alt;
        if (!list.length) {
          console.error(`  (no keys under prefix ${JSON.stringify(p)})`);
          continue;
        }
        for (const o of list) {
          console.error(`  key=${JSON.stringify(o.key)}  last_modified=${o.lastModified.toISOString()}  size=${o.size}`);
        }
        if (since) console.error(`  Cutoff was: last_modified >= ${since.toISOString()} (UTC)`);
      }
    }
  }
  return emitted;
}

function parseArgs(): CliArgs {
  const argv = process.argv.slice(2);
  let verify = false;
  let dryRun = false;
  let noInsecure = false;
  let verbose = false;
  let inspectCmcd = false;
  let bucket = DEFAULT_BUCKET;
  let prefixes: string[] | null = null;
  let sinceMinutes: number | null = null;
  let endpoint: string | null = DEFAULT_ENDPOINT;
  let serviceName = process.env.OTEL_SERVICE_NAME ?? 'cdn-logs-collector';
  let awsProfile = process.env.AWS_PROFILE;
  let region = process.env.AWS_REGION ?? 'eu-west-1';
  let maxObjects: number | null = null;
  let maxLinesPerFile: number | null = null;
  let key: string | null = null;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--verify') verify = true;
    else if (arg === '--dry-run') dryRun = true;
    else if (arg === '--no-insecure') noInsecure = true;
    else if (arg === '-v' || arg === '--verbose') verbose = true;
    else if (arg === '--inspect-cmcd') inspectCmcd = true;
    else if (arg === '--bucket' && argv[i + 1]) {
      bucket = argv[++i];
    } else if ((arg === '--prefix' || arg === '-p') && argv[i + 1]) {
      if (!prefixes) prefixes = [];
      prefixes.push(argv[++i]);
    } else if (arg === '--since-minutes' && argv[i + 1]) {
      sinceMinutes = parseInt(argv[++i], 10);
    } else if (arg === '--endpoint' && argv[i + 1]) {
      endpoint = argv[++i];
    } else if (arg === '--service-name' && argv[i + 1]) {
      serviceName = argv[++i];
    } else if (arg === '--aws-profile' && argv[i + 1]) {
      awsProfile = argv[++i];
    } else if (arg === '--region' && argv[i + 1]) {
      region = argv[++i];
    } else if (arg === '--max-objects' && argv[i + 1]) {
      maxObjects = parseInt(argv[++i], 10);
    } else if (arg === '--max-lines-per-file' && argv[i + 1]) {
      maxLinesPerFile = parseInt(argv[++i], 10);
    } else if (arg === '--key' && argv[i + 1]) {
      key = argv[++i];
    }
  }

  if (process.env.CDN_LOGS_PREFIX) {
    prefixes = [process.env.CDN_LOGS_PREFIX.replace(/\/+$/, '') + '/'];
  } else if (!prefixes) {
    prefixes = [...DEFAULT_PREFIXES];
  }

  return {
    verify,
    bucket,
    prefixes,
    sinceMinutes,
    endpoint,
    insecure: !noInsecure,
    serviceName,
    awsProfile,
    region,
    dryRun,
    maxObjects,
    maxLinesPerFile,
    verbose,
    inspectCmcd,
    key,
  } as CliArgs;
}

async function main(): Promise<number> {
  const args = parseArgs();

  if (args.verify) {
    const ok = await verifyBucketAccess(
      args.bucket,
      args.region,
      args.awsProfile,
      args.prefixes[0] ?? '/5gemerge/logs/'
    );
    return ok ? 0 : 1;
  }

  try {
    const emitted = await run({
      bucket: args.bucket,
      prefixes: args.prefixes,
      sinceMinutes: args.sinceMinutes,
      endpoint: args.endpoint,
      insecure: args.insecure,
      serviceName: args.serviceName,
      awsProfile: args.awsProfile,
      dryRun: args.dryRun,
      maxObjects: args.maxObjects,
      maxLinesPerFile: args.maxLinesPerFile,
      verbose: args.verbose,
      inspectCmcd: args.inspectCmcd,
      key: args.key,
    });
    if (!args.dryRun && !args.inspectCmcd) {
      console.error(`Emitted ${emitted} log records to ${args.endpoint}`);
    }
    return 0;
  } catch (e: unknown) {
    const err = e as { name?: string; message?: string };
    if (err.name === 'TokenRetrievalError' || (err.message && err.message.includes('Token') && err.message.includes('expired'))) {
      console.error('AWS SSO token has expired. Log in again with:');
      console.error('  aws sso login');
      if (args.awsProfile) console.error(`  (or: aws sso login --profile ${args.awsProfile})`);
      return 1;
    }
    throw e;
  }
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
