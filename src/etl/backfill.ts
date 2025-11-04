import { basename } from "path";
import { listFilesRecursive, makeTempDir, awsS3CpRecursive, decompressLz4Recursive, awsS3HasAny, awsS3List, awsS3CpObject } from "./helpers";
import { ingestLocalFile } from "./ingest_file";
import { withClient } from "../db/client";

function normalizeDateArg(d: Date): { yyyymmdd: string; isoDay: string } {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return { yyyymmdd: `${yyyy}${mm}${dd}`, isoDay: `${yyyy}-${mm}-${dd}` };
}

async function processDay(date: Date): Promise<{ day: string; inserted: number; skipped: number; error?: string }> {
  const { yyyymmdd, isoDay } = normalizeDateArg(date);
  const prefix = process.env["HL_S3_FILLS_PREFIX"] ?? "s3://hl-mainnet-node-data/node_fills_by_block";
  const payer = process.env["AWS_REQUEST_PAYER"] ?? "requester";

  try {
    const tmp = await makeTempDir();
    const cand1 = `${prefix}/${yyyymmdd}/`;
    const cand2 = `${prefix}/${isoDay}/`;
    let s3Prefix = cand1;
    
    if (!(await awsS3HasAny(cand1, payer))) {
      if (await awsS3HasAny(cand2, payer)) {
        s3Prefix = cand2;
      } else {
        return { day: isoDay, inserted: 0, skipped: 0, error: "No data found" };
      }
    }

    const objects = await awsS3List(s3Prefix, payer);
    const maxBlocks = Number(process.env["MAX_BLOCKS"] ?? 0);
    const selected = maxBlocks > 0 ? objects.slice(0, maxBlocks) : objects;

    if (selected.length === 0) {
      return { day: isoDay, inserted: 0, skipped: 0, error: "No files found" };
    }

    let totalInserted = 0;
    let skipped = 0;
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
        
        if (already) {
          skipped++;
          continue;
        }

        try {
          const localLz4 = `${tmp}/${name}`;
          await awsS3CpObject(s3Key, localLz4, payer);
          await decompressLz4Recursive(tmp);
          const localRaw = localLz4.replace(/\.lz4$/, "");
          const { inserted } = await ingestLocalFile(localRaw, keyGuess);
          totalInserted += inserted;
          
          await withClient(async (c) => {
            await c.query(
              "insert into ingested_files (s3_key, etag) values ($1, $2) on conflict do nothing",
              [keyGuess, null],
            );
          });
        } catch (e) {
          console.error(`  Failed file ${s3Key}:`, e instanceof Error ? e.message : String(e));
        }
      }
    }

    const workers = Array.from({ length: Math.max(1, concurrency) }, () => worker());
    await Promise.all(workers);

    return { day: isoDay, inserted: totalInserted, skipped };
  } catch (error) {
    return {
      day: isoDay,
      inserted: 0,
      skipped: 0,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

async function run(): Promise<void> {
  const daysToBackfill = Number(process.env["BACKFILL_DAYS"] ?? 180);
  const startDate = new Date();
  startDate.setUTCHours(0, 0, 0, 0);

  console.log(`Starting backfill for last ${daysToBackfill} days...\n`);

  const results: Array<{ day: string; inserted: number; skipped: number; error?: string }> = [];
  let totalInserted = 0;
  let totalSkipped = 0;
  let errorCount = 0;

  for (let i = 1; i <= daysToBackfill; i++) {
    const date = new Date(startDate);
    date.setUTCDate(date.getUTCDate() - i);

    const result = await processDay(date);
    results.push(result);
    totalInserted += result.inserted;
    totalSkipped += result.skipped;
    
    if (result.error) {
      errorCount++;
      console.log(`[${i}/${daysToBackfill}] ${result.day}: ERROR - ${result.error}`);
    } else {
      console.log(
        `[${i}/${daysToBackfill}] ${result.day}: Inserted ${result.inserted} fills, Skipped ${result.skipped} files`,
      );
    }
  }

  console.log("\n=== Backfill Summary ===");
  console.log(`Days processed: ${daysToBackfill}`);
  console.log(`Total fills inserted: ${totalInserted}`);
  console.log(`Total files skipped (already ingested): ${totalSkipped}`);
  console.log(`Days with errors: ${errorCount}`);
  
  if (errorCount > 0) {
    console.log("\nDays with errors:");
    results.filter((r) => r.error).forEach((r) => {
      console.log(`  ${r.day}: ${r.error}`);
    });
  }
}

run().catch((err) => {
  console.error("Backfill failed:", err);
  process.exit(1);
});

