/**
 * List and stream G-Core CDN access logs from S3.
 */

import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  HeadBucketCommand,
} from '@aws-sdk/client-s3';
import { fromIni } from '@aws-sdk/credential-providers';
import { Readable } from 'stream';
import * as zlib from 'zlib';

const REGION = 'eu-west-1';
const GZIP_MAGIC = new Uint8Array([0x1f, 0x8b]);

export interface S3ObjectInfo {
  key: string;
  lastModified: Date;
  size: number;
}

export async function verifyBucketAccess(
  bucket: string,
  region: string = REGION,
  profile?: string,
  prefix: string = '/5gemerge/logs/'
): Promise<boolean> {
  try {
    const client = createS3Client(region, profile);
    await client.send(new HeadBucketCommand({ Bucket: bucket }));
    const list = await client.send(
      new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, MaxKeys: 1 })
    );
    const count = list.KeyCount ?? 0;
    console.error(
      `OK: Can access bucket s3://${bucket} (region=${region}). Prefix '${prefix}' has ${count ? '≥1 object' : 'no objects yet'}.`
    );
    return true;
  } catch (e: unknown) {
    const err = e as { name?: string; message?: string; $metadata?: { httpStatusCode?: number } };
    const code = err.name ?? '';
    const msg = err.message ?? String(e);
    console.error(`ERROR: Cannot access bucket s3://${bucket} (region=${region}): ${code} - ${msg}`);
    return false;
  }
}

function createS3Client(region: string, profile?: string): S3Client {
  const opts: { region: string; credentials?: ReturnType<typeof fromIni> } = { region };
  if (profile) {
    opts.credentials = fromIni({ profile });
  }
  return new S3Client(opts);
}

export async function listRawKeys(
  bucket: string,
  prefix: string,
  maxKeys: number = 10,
  profile?: string
): Promise<S3ObjectInfo[]> {
  const client = createS3Client(REGION, profile);
  const out: S3ObjectInfo[] = [];
  let continuationToken: string | undefined;
  do {
    const cmd = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      MaxKeys: maxKeys - out.length,
      ContinuationToken: continuationToken,
    });
    const resp = await client.send(cmd);
    for (const obj of resp.Contents ?? []) {
      if (obj.Key != null) {
        out.push({
          key: obj.Key,
          lastModified: obj.LastModified ?? new Date(0),
          size: obj.Size ?? 0,
        });
        if (out.length >= maxKeys) return out;
      }
    }
    continuationToken = resp.NextContinuationToken;
  } while (continuationToken);
  return out;
}

export async function* listLogObjects(
  bucket: string,
  prefix: string = '/5gemerge/logs/',
  since?: Date,
  suffix: string = '_access.log.gz',
  profile?: string
): AsyncGenerator<S3ObjectInfo> {
  const client = createS3Client(REGION, profile);
  let continuationToken: string | undefined;
  do {
    const cmd = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    });
    const resp = await client.send(cmd);
    for (const obj of resp.Contents ?? []) {
      const key = obj.Key;
      if (key == null || !key.endsWith(suffix)) continue;
      const lastModified = obj.LastModified ?? new Date(0);
      if (since && lastModified < since) continue;
      yield {
        key,
        lastModified,
        size: obj.Size ?? 0,
      };
    }
    continuationToken = resp.NextContinuationToken;
  } while (continuationToken);
}

export async function downloadAndDecompress(
  bucket: string,
  key: string,
  profile?: string
): Promise<Buffer> {
  const client = createS3Client(REGION, profile);
  const resp = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const body = resp.Body as Readable;
  if (!body) throw new Error('Empty S3 response body');
  const chunks: Buffer[] = [];
  for await (const chunk of body) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  let buf = Buffer.concat(chunks);
  if (key.endsWith('.gz') && buf.length >= 2 && buf[0] === GZIP_MAGIC[0] && buf[1] === GZIP_MAGIC[1]) {
    buf = zlib.gunzipSync(buf);
  }
  return buf;
}

export async function* streamLogLines(
  bucket: string,
  key: string,
  profile?: string,
  encoding: BufferEncoding = 'utf-8'
): AsyncGenerator<string> {
  const raw = await downloadAndDecompress(bucket, key, profile);
  const text = raw.toString(encoding);
  for (const line of text.split(/\r?\n/)) {
    yield line;
  }
}
