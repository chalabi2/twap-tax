export type ParsedFill = {
  blockNum: number | null;
  txHash: string | null;
  ts: Date | null;
  wallet: string | null;
  asset: string | null;
  side: string | null;
  price: number | null;
  size: number | null;
  twapId: string | null;
  closedPnl: number | null;
  fee: number | null;
  feeToken: string | null;
  dir: string | null;
  builder: string | null;
  orderId: number | null;
  tid: number | null;
  cloid: string | null;
  startPosition: number | null;
  crossed: boolean | null;
};

function toNumber(val: unknown): number | null {
  if (typeof val === "number") return Number.isFinite(val) ? val : null;
  if (typeof val === "string") {
    const n = Number(val);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function toString(val: unknown): string | null {
  if (typeof val === "string" && val.length > 0) return val;
  if (typeof val === "number") return String(val);
  return null;
}

function toDate(val: unknown): Date | null {
  const n = toNumber(val);
  if (n !== null) {
    // Heuristic: nanoseconds > 1e15, milliseconds > 1e12, else seconds
    const ms = n > 1e15 ? Math.floor(n / 1e6) : n > 1e12 ? n : n * 1000;
    const d = new Date(ms);
    return isNaN(d.getTime()) ? null : d;
  }
  if (typeof val === "string") {
    const d = new Date(val);
    return isNaN(d.getTime()) ? null : d;
  }
  return null;
}

export function parseFillRecord(raw: unknown): ParsedFill {
  const r: Record<string, unknown> = (raw ?? {}) as any;

  // Common key guesses across HL datasets
  const ts =
    toDate(r["time"]) ||
    toDate(r["ts"]) ||
    toDate(r["timestamp"]) ||
    toDate(r["blockTime"]) ||
    toDate(r["block_time"]) ||
    toDate(r["t"]) ||
    toDate(r["timeMs"]) ||
    toDate(r["timeNs"]) ||
    toDate((r as any)?.fill?.time);

  const wallet =
    toString(r["trader"]) ||
    toString(r["user"]) ||
    toString(r["wallet"]) ||
    toString(r["address"]) ||
    toString(r["addr"]) ||
    toString((r as any)?.fill?.trader);

  const asset =
    toString(r["coin"]) ||
    toString(r["asset"]) ||
    toString(r["sym"]) ||
    toString((r as any)?.fill?.coin);

  const side =
    toString(r["side"]) ||
    toString(r["dir"]) ||
    toString((r as any)?.fill?.side);

  const price =
    toNumber(r["px"]) ||
    toNumber(r["price"]) ||
    toNumber((r as any)?.fill?.px) ||
    toNumber((r as any)?.fill?.price);

  const size =
    toNumber(r["sz"]) ||
    toNumber(r["size"]) ||
    toNumber((r as any)?.fill?.sz) ||
    toNumber((r as any)?.fill?.size);

  const twapId =
    toString(r["twapId"]) ||
    toString(r["parentOrderId"]) ||
    toString(r["oidParent"]) ||
    toString((r as any)?.order?.twapId) ||
    toString((r as any)?.parent?.twapId);

  const blockNum =
    (toNumber(r["block"]) ?? toNumber(r["blockNumber"]) ?? toNumber(r["block_number"]) ?? toNumber((r as any)?.blk)) ?? null;

  const txHash =
    toString(r["txHash"]) ||
    toString(r["tx_hash"]) ||
    toString(r["hash"]) ||
    toString((r as any)?.tx?.hash) ||
    null;

  const closedPnl = toNumber(r["closedPnl"]) ?? null;
  const fee = toNumber(r["fee"]) ?? null;
  const feeToken = toString(r["feeToken"]) ?? null;
  const dir = toString(r["dir"]) ?? null;
  const builder = toString(r["builder"]) ?? null;
  const orderId = toNumber(r["oid"]) ?? null;
  const tid = toNumber(r["tid"]) ?? null;
  const cloid = toString(r["cloid"]) ?? null;
  const startPosition = toNumber(r["startPosition"]) ?? null;
  const crossed = typeof r["crossed"] === "boolean" ? (r["crossed"] as boolean) : null;

  return { blockNum, txHash, ts, wallet, asset, side, price, size, twapId, closedPnl, fee, feeToken, dir, builder, orderId, tid, cloid, startPosition, crossed };
}

