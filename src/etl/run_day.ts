import { basename, relative } from "path";
import { listFilesRecursive, makeTempDir, awsS3CpRecursive, decompressLz4Recursive, awsS3HasAny, awsS3List, awsS3CpObject } from "./helpers";
import { ingestLocalFile } from "./ingest_file";
import { withClient } from "../db/client";

function normalizeDateArg(arg?: string): { yyyymmdd: string; isoDay: string } {
  const d = arg ? new Date(arg) : new Date(Date.now() - 24 * 3600 * 1000);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return { yyyymmdd: `${yyyy}${mm}${dd}`, isoDay: `${yyyy}-${mm}-${dd}` };
}

async function run(): Promise<void> {
  const { yyyymmdd, isoDay } = normalizeDateArg(process.argv[2]);
  const prefix = process.env["HL_S3_FILLS_PREFIX"] ?? "s3://hl-mainnet-node-data/node_fills_by_block";
  const payer = process.env["AWS_REQUEST_PAYER"] ?? "requester";

  const tmp = await makeTempDir();
  // Try both date directory formats: yyyymmdd and yyyy-mm-dd
  const cand1 = `${prefix}/${yyyymmdd}/`;
  const cand2 = `${prefix}/${isoDay}/`;
  let s3Prefix = cand1;
  if (!(await awsS3HasAny(cand1, payer))) {
    if (await awsS3HasAny(cand2, payer)) s3Prefix = cand2;
  }
  // New: list objects and fetch per object with limits
  const objects = await awsS3List(s3Prefix, payer);
  const maxBlocks = Number(process.env["MAX_BLOCKS"] ?? 0);
  const selected = maxBlocks > 0 ? objects.slice(0, maxBlocks) : objects;
  console.log(`Found ${selected.length} files to ingest for ${isoDay}`);

  let totalInserted = 0;
  const concurrency = Number(process.env["INGEST_CONCURRENCY"] ?? 4);
  let idx = 0;
  async function worker() {
    while (idx < selected.length) {
      const i = idx++;
      const name = selected[i];
      const s3Key = `${s3Prefix}${name}`;
      const keyGuess = `hourly/${yyyymmdd}/${name}`;
      const already = await withClient(async (c) => {
        const { rows } = await c.query("select 1 from ingested_files where s3_key = $1", [keyGuess]);
        return rows.length > 0;
      });
      if (already) continue;
      try {
        const localLz4 = `${tmp}/${name}`;
        await awsS3CpObject(s3Key, localLz4, payer);
        await decompressLz4Recursive(tmp);
        const localRaw = localLz4.replace(/\.lz4$/, "");
        const { inserted } = await ingestLocalFile(localRaw, keyGuess);
        totalInserted += inserted;
        await withClient(async (c) => {
          await c.query("insert into ingested_files (s3_key, etag) values ($1, $2) on conflict do nothing", [keyGuess, null]);
        });
      } catch (e) {
        console.error(`Failed ingest for ${s3Key}:`, e);
      }
    }
  }
  const workers = Array.from({ length: Math.max(1, concurrency) }, () => worker());
  await Promise.all(workers);
  console.log(`Inserted ${totalInserted} fills for ${isoDay}`);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

