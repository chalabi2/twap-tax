import { basename, relative } from "path";
import { listFilesRecursive, makeTempDir, awsS3CpRecursive, decompressLz4Recursive, awsS3HasAny } from "./helpers";
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
  console.log(`Syncing ${s3Prefix} -> ${tmp}`);
  await awsS3CpRecursive(s3Prefix, tmp, payer);

  console.log("Decompressing .lz4 files if any...");
  await decompressLz4Recursive(tmp);

  const files = await listFilesRecursive(tmp);
  console.log(`Found ${files.length} files to ingest for ${isoDay}`);

  // Insert watermark for each file after ingest completes
  let totalInserted = 0;
  for (const f of files) {
    const keyGuess = `${yyyymmdd}/${basename(f)}`;
    const already = await withClient(async (c) => {
      const { rows } = await c.query("select 1 from ingested_files where s3_key = $1", [keyGuess]);
      return rows.length > 0;
    });
    if (already) {
      continue;
    }
    const { inserted } = await ingestLocalFile(f, keyGuess);
    totalInserted += inserted;
    await withClient(async (c) => {
      await c.query("insert into ingested_files (s3_key, etag) values ($1, $2) on conflict do nothing", [keyGuess, null]);
    });
  }

  console.log(`Inserted ${totalInserted} fills for ${isoDay}`);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});

