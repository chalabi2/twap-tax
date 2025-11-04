import { dbPool, withClient } from "./db/client";

type Query = Record<string, string | undefined>;

function parseQuery(url: URL): Query {
  const q: Query = {};
  url.searchParams.forEach((v, k) => (q[k] = v));
  return q;
}

function json(res: ResponseInit & { body: unknown; status?: number }): Response {
  const { body, status = 200, headers, ...rest } = res;
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...(headers ?? {}) },
    ...rest,
  });
}

function badRequest(message: string): Response {
  return json({ status: 400, body: { error: message } });
}

async function handleHealth(): Promise<Response> {
  try {
    await withClient(async (c) => c.query("select 1"));
    return json({ body: { ok: true } });
  } catch (e) {
    return json({ status: 500, body: { ok: false } });
  }
}

async function handleTwaps(url: URL): Promise<Response> {
  const q = parseQuery(url);
  const wallet = q.wallet;
  const asset = q.asset;
  const includeUngrouped = q.includeUngrouped === "1";
  const start = q.start ? new Date(q.start) : null;
  const end = q.end ? new Date(q.end) : null;
  if ((q.start && !start) || (q.end && !end)) return badRequest("Invalid start/end");

  const params: any[] = [];
  const where: string[] = [];
  if (wallet) {
    params.push(wallet);
    where.push(`wallet = $${params.length}`);
  }
  if (asset) {
    params.push(asset);
    where.push(`asset = $${params.length}`);
  }
  if (start) {
    params.push(start.toISOString());
    where.push(`ts >= $${params.length}`);
  }
  if (end) {
    params.push(end.toISOString());
    where.push(`ts <= $${params.length}`);
  }
  if (!includeUngrouped) {
    where.push("twap_id is not null");
  }
  const whereSql = where.length ? `where ${where.join(" and ")}` : "";

  const rows = await withClient(async (c) => {
    const { rows } = await c.query(
      `select twap_id, wallet, asset, ts, side, price, size, tx_hash
       from fills
       ${whereSql}
       order by twap_id nulls last, ts asc`,
      params,
    );
    return rows as any[];
  });

  type FillRow = {
    twap_id: string | null;
    wallet: string | null;
    asset: string | null;
    ts: string | null;
    side: string | null;
    price: string | number | null;
    size: string | number | null;
    tx_hash: string | null;
  };

  const groups = new Map<string, any>();
  for (const r of rows as FillRow[]) {
    const gid = r.twap_id ?? "__ungrouped";
    const g = groups.get(gid) ?? {
      twapId: r.twap_id,
      wallet: r.wallet,
      asset: r.asset,
      fills: [] as any[],
    };
    const price = r.price != null ? Number(r.price) : null;
    const size = r.size != null ? Number(r.size) : null;
    g.fills.push({ ts: r.ts, side: r.side, price, size, txHash: r.tx_hash });
    groups.set(gid, g);
  }

  const out = Array.from(groups.values()).map((g) => {
    const prices = g.fills.map((f: any) => f.price).filter((n: number | null) => n != null) as number[];
    const sizes = g.fills.map((f: any) => f.size).filter((n: number | null) => n != null) as number[];
    const tses = g.fills.map((f: any) => (f.ts ? new Date(f.ts) : null)).filter(Boolean) as Date[];
    const totalSize = sizes.reduce((a: number, b: number) => a + b, 0);
    const avgPrice = prices.length ? prices.reduce((a: number, b: number) => a + b, 0) / prices.length : null;
    const minPrice = prices.length ? Math.min(...prices) : null;
    const maxPrice = prices.length ? Math.max(...prices) : null;
    const startTs = tses.length ? new Date(Math.min(...tses.map((d) => d.getTime()))).toISOString() : null;
    const endTs = tses.length ? new Date(Math.max(...tses.map((d) => d.getTime()))).toISOString() : null;
    return {
      twapId: g.twapId,
      wallet: g.wallet,
      asset: g.asset,
      fillsCount: g.fills.length,
      totalSize,
      avgPrice,
      minPrice,
      maxPrice,
      startTs,
      endTs,
      fills: g.fills,
    };
  });

  return json({ body: out });
}

async function handleTwapById(id: string): Promise<Response> {
  const rows = await withClient(async (c) => {
    const { rows } = await c.query(
      `select twap_id, wallet, asset, ts, side, price, size, tx_hash
       from fills where twap_id = $1 order by ts asc`,
      [id],
    );
    return rows as any[];
  });

  if (rows.length === 0) return json({ body: null, status: 404 });
  const fills = rows.map((r: any) => ({ ts: r.ts, side: r.side, price: r.price ? Number(r.price) : null, size: r.size ? Number(r.size) : null, txHash: r.tx_hash }));
  const prices = fills.map((f: any) => f.price).filter((n: number | null) => n != null) as number[];
  const sizes = fills.map((f: any) => f.size).filter((n: number | null) => n != null) as number[];
  const tses = fills.map((f: any) => (f.ts ? new Date(f.ts) : null)).filter(Boolean) as Date[];
  const totalSize = sizes.reduce((a: number, b: number) => a + b, 0);
  const avgPrice = prices.length ? prices.reduce((a: number, b: number) => a + b, 0) / prices.length : null;
  const minPrice = prices.length ? Math.min(...prices) : null;
  const maxPrice = prices.length ? Math.max(...prices) : null;
  const startTs = tses.length ? new Date(Math.min(...tses.map((d) => d.getTime()))).toISOString() : null;
  const endTs = tses.length ? new Date(Math.max(...tses.map((d) => d.getTime()))).toISOString() : null;
  return json({
    body: {
      twapId: rows[0].twap_id,
      wallet: rows[0].wallet,
      asset: rows[0].asset,
      fillsCount: fills.length,
      totalSize,
      avgPrice,
      minPrice,
      maxPrice,
      startTs,
      endTs,
      fills,
    },
  });
}

const server = Bun.serve({
  port: Number(process.env["PORT"] ?? 3000),
  fetch: async (req) => {
    const url = new URL(req.url);
    if (url.pathname === "/healthz") return handleHealth();
    if (url.pathname === "/twaps") return handleTwaps(url);
    if (url.pathname.startsWith("/twaps/")) {
      const id = url.pathname.split("/")[2] ?? "";
      if (!id) return badRequest("Missing twapId");
      return handleTwapById(id);
    }
    if (url.pathname === "/") {
      return new Response("OK", { status: 200 });
    }
    return new Response("Not found", { status: 404 });
  },
});

console.log(`API listening on http://localhost:${server.port}`);

process.on("SIGINT", async () => {
  await dbPool.end();
  process.exit(0);
});

