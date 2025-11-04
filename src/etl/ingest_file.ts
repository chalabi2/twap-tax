import { createReadStream } from "fs";
import { stat } from "fs/promises";
import readline from "readline";
import { withClient } from "../db/client";
import { parseFillRecord } from "./parse_fill";

function isLikelyJsonLine(s: string): boolean {
  const t = s.trim();
  return t.startsWith("{") || t.startsWith("[");
}

export async function ingestLocalFile(fullPath: string, s3KeyGuess: string): Promise<{ lines: number; inserted: number }> {
  const st = await stat(fullPath);
  if (!st.isFile()) return { lines: 0, inserted: 0 };

  const rl = readline.createInterface({ input: createReadStream(fullPath), crlfDelay: Infinity });
  let lineNo = 0;
  let inserted = 0;
  const rows: any[][] = [];
  for await (const line of rl) {
    const trimmed = line.trim();
    if (trimmed === "") continue;
    lineNo += 1;
    if (!isLikelyJsonLine(trimmed)) continue;
    try {
      const raw = JSON.parse(trimmed);
      const candidates: unknown[] = [];
      if (Array.isArray(raw)) {
        for (const item of raw) candidates.push(item);
      } else if (raw && typeof raw === "object") {
        const obj = raw as Record<string, unknown>;
        if (Array.isArray(obj["fills"])) {
          for (const item of obj["fills"] as unknown[]) candidates.push(item);
        } else if (Array.isArray(obj["node_fills"])) {
          for (const item of obj["node_fills"] as unknown[]) candidates.push(item);
        } else if (Array.isArray(obj["events"])) {
          // HL node_fills_by_block: events is an array of [wallet, {fill}]
          for (const ev of obj["events"] as unknown[]) {
            if (Array.isArray(ev) && ev.length >= 2 && typeof ev[1] === "object") {
              const wallet = ev[0] as unknown;
              const payload = ev[1] as Record<string, unknown>;
              const merged = { wallet, block_number: obj["block_number"], block_time: obj["block_time"], ...payload } as unknown;
              candidates.push(merged);
            }
          }
        } else {
          candidates.push(raw);
        }
      } else {
        candidates.push(raw);
      }

      let idx = 0;
      for (const item of candidates) {
        const parsed = parseFillRecord(item);
        // Require at least timestamp and price/size or wallet to avoid writing non-fill wrappers
        const hasSignal = !!(parsed.wallet || parsed.price || parsed.size || parsed.asset || parsed.ts);
        if (!hasSignal) continue;
        rows.push([
          s3KeyGuess,
          lineNo,
          idx,
          parsed.blockNum,
          parsed.txHash,
          parsed.ts ? parsed.ts.toISOString() : null,
          parsed.wallet,
          parsed.asset,
          parsed.side,
          parsed.price,
          parsed.size,
          parsed.twapId,
          parsed.closedPnl,
          parsed.fee,
          parsed.feeToken,
          parsed.dir,
          parsed.builder,
          parsed.orderId,
          parsed.tid,
          parsed.cloid,
          parsed.startPosition,
          parsed.crossed,
          JSON.stringify(item),
        ]);
        idx += 1;
      }
    } catch (e) {
      // Skip malformed lines
      continue;
    }
  }
  if (rows.length > 0) {
    const chunkSize = Number(process.env["BULK_INSERT_CHUNK_SIZE"] ?? 1000);
    await withClient(async (c) => {
      for (let i = 0; i < rows.length; i += chunkSize) {
        const batch = rows.slice(i, i + chunkSize);
        const cols = [
          "s3_key","source_line","item_idx","block_num","tx_hash","ts","wallet","asset","side","price","size","twap_id",
          "closed_pnl","fee","fee_token","dir","builder","oid","tid","cloid","start_position","crossed","raw"
        ];
        const valuesSql = batch.map((_, bi) => `(${cols.map((__, cj) => `$${bi * cols.length + cj + 1}`).join(",")})`).join(",");
        const flatParams = batch.flat();
        const sql = `insert into fills (${cols.join(",")}) values ${valuesSql} on conflict (s3_key, source_line, item_idx) do nothing`;
        await c.query(sql, flatParams as any);
        inserted += batch.length;
      }
    });
  }
  return { lines: lineNo, inserted };
}

