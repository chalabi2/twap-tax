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

function forbidden(): Response {
  return new Response(JSON.stringify({ error: "Forbidden" }), {
    status: 403,
    headers: { "content-type": "application/json" },
  });
}

function requireApiKey(req: Request): Response | null {
  const required = process.env["API_KEY"];
  if (!required) return null;
  const key = req.headers.get("X-API-Key");
  if (!key || key !== required) return forbidden();
  return null;
}

function parseDate(val?: string | null): Date | null {
  if (!val) return null;
  const d = new Date(val);
  return isNaN(d.getTime()) ? null : d;
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
  const wallet = q["wallet"];
  const asset = q["asset"];
  const start = q["start"] ? new Date(q["start"]) : null;
  const end = q["end"] ? new Date(q["end"]) : null;
  let limit = q["limit"] ? Number(q["limit"]) : 100;
  if (!Number.isFinite(limit) || limit < 1) limit = 1;
  if (limit > 100) limit = 100;
  let offset = q["offset"] ? Number(q["offset"]) : 0;
  if (!Number.isFinite(offset) || offset < 0) offset = 0;
  if ((q["start"] && !start) || (q["end"] && !end)) return badRequest("Invalid start/end");

  const params: any[] = [];
  const where: string[] = ["twap_id is not null"];
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
  const whereSql = `where ${where.join(" and ")}`;
  params.push(limit + 1, offset);

  const twapIds = await withClient(async (c) => {
    const { rows } = await c.query(
      `select distinct twap_id from fills ${whereSql} order by twap_id asc limit $${params.length - 1} offset $${params.length}`,
      params,
    );
    return rows as any[];
  });
  const hasMore = twapIds.length > limit;
  if (hasMore) twapIds.pop();

  if (twapIds.length === 0) {
    return json({
      body: {
        data: [],
        pagination: { limit, offset, has_more: false },
      },
    });
  }

  const ids = twapIds.map((r: any) => r.twap_id);
  const fillRows = await withClient(async (c) => {
    const { rows } = await c.query(
      `select twap_id, wallet, asset, ts, side, price, size, tx_hash
       from fills
       where twap_id = any($1)
       order by twap_id, ts asc`,
      [ids],
    );
    return rows as any[];
  });

  type FillRow = {
    twap_id: string;
    wallet: string | null;
    asset: string | null;
    ts: string | null;
    side: string | null;
    price: string | number | null;
    size: string | number | null;
    tx_hash: string | null;
  };

  const groups = new Map<string, any>();
  for (const r of fillRows as FillRow[]) {
    const g = groups.get(r.twap_id) ?? {
      twapId: r.twap_id,
      wallet: r.wallet,
      asset: r.asset,
      fills: [] as any[],
    };
    const price = r.price != null ? Number(r.price) : null;
    const size = r.size != null ? Number(r.size) : null;
    g.fills.push({ ts: r.ts, side: r.side, price, size, txHash: r.tx_hash });
    groups.set(r.twap_id, g);
  }

  const out = ids.map((id) => {
    const g = groups.get(id);
    if (!g) return null;
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
  }).filter(Boolean);

  const includeTotalParam = q["include_total"];
  const includeTotal = includeTotalParam === "1" || includeTotalParam === "true";
  let total: number | undefined;
  if (includeTotal) {
    total = await withClient(async (c) => {
      const countParams = params.slice(0, -2);
      const { rows } = await c.query(
        `select count(distinct twap_id) as cnt from fills ${whereSql}`,
        countParams,
      );
      return Number(rows[0].cnt);
    });
  }

  const currentPage = Math.floor(offset / limit) + 1;
  const nextOffset = hasMore ? offset + limit : null;
  const prevOffset = offset > 0 ? Math.max(0, offset - limit) : null;

  return json({
    body: {
      data: out,
      pagination: {
        limit,
        offset,
        current_page: currentPage,
        returned: out.length,
        has_more: hasMore,
        next_offset: nextOffset,
        prev_offset: prevOffset,
        ...(total !== undefined && {
          total,
          total_pages: Math.ceil(total / limit),
        }),
      },
    },
  });
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
  hostname: process.env["HOST"] ?? "0.0.0.0",
  development: false,
  fetch: async (req) => {
    const url = new URL(req.url);
    if (url.pathname === "/trades") {
      const authErr = requireApiKey(req);
      if (authErr) return authErr;
      const q = parseQuery(url);
      const walletsParam = q["wallet_addresses"];
      const asset = q["asset"];
      const twapId = q["twap_id"];
      const start = parseDate(q["start_date"]);
      const end = parseDate(q["end_date"]);
      let limit = q["limit"] ? Number(q["limit"]) : 100;
      if (!Number.isFinite(limit) || limit < 1) limit = 1;
      if (limit > 100) limit = 100;
      let offset = q["offset"] ? Number(q["offset"]) : 0;
      if (!Number.isFinite(offset) || offset < 0) offset = 0;

      const where: string[] = [];
      const params: any[] = [];
      if (walletsParam) {
        const ws = walletsParam.split(",").map((w) => w.trim()).filter(Boolean);
        if (ws.length) {
          params.push(ws);
          where.push(`wallet = any($${params.length})`);
        }
      }
      if (asset) {
        params.push(asset);
        where.push(`asset = $${params.length}`);
      }
      if (twapId) {
        params.push(twapId);
        where.push(`twap_id = $${params.length}`);
      }
      if (start) {
        params.push(start.toISOString());
        where.push(`ts >= $${params.length}`);
      }
      if (end) {
        params.push(end.toISOString());
        where.push(`ts <= $${params.length}`);
      }
      const whereSql = where.length ? `where ${where.join(" and ")}` : "";
      params.push(limit + 1, offset);
      const rows = await withClient(async (c) => {
        const { rows } = await c.query(
          `select id, twap_id, wallet, ts, asset, size, price, side, fee
           from fills
           ${whereSql}
           order by ts asc
           limit $${params.length - 1} offset $${params.length}`,
          params,
        );
        return rows as any[];
      });
      const hasMore = rows.length > limit;
      if (hasMore) rows.pop();
      const out = rows.map((r: any) => ({
        id: Number(r.id),
        twap_id: r.twap_id,
        wallet_address: r.wallet,
        timestamp: r.ts,
        asset: r.asset,
        quantity: r.size != null ? Number(r.size) : null,
        price: r.price != null ? Number(r.price) : null,
        side: r.side === "B" ? "buy" : r.side === "A" ? "sell" : r.side,
        fee: r.fee != null ? Number(r.fee) : null,
        exchange: "hyperliquid",
      }));
      const includeTotalParam = q["include_total"];
      const includeTotal = includeTotalParam === "1" || includeTotalParam === "true";
      let total: number | undefined;
      if (includeTotal) {
        total = await withClient(async (c) => {
          const countParams = params.slice(0, -2);
          const { rows } = await c.query(
            `select count(*) as cnt from fills ${whereSql}`,
            countParams,
          );
          return Number(rows[0].cnt);
        });
      }

      const currentPage = Math.floor(offset / limit) + 1;
      const nextOffset = hasMore ? offset + limit : null;
      const prevOffset = offset > 0 ? Math.max(0, offset - limit) : null;

      return json({
        body: {
          data: out,
          pagination: {
            limit,
            offset,
            current_page: currentPage,
            returned: out.length,
            has_more: hasMore,
            next_offset: nextOffset,
            prev_offset: prevOffset,
            ...(total !== undefined && {
              total,
              total_pages: Math.ceil(total / limit),
            }),
          },
        },
      });
    }
    if (url.pathname.startsWith("/twap/")) {
      const authErr = requireApiKey(req);
      if (authErr) return authErr;
      const id = url.pathname.split("/")[2] ?? "";
      if (!id) return badRequest("Missing twap_id");
      const q = parseQuery(url);
      let limit = q["limit"] ? Number(q["limit"]) : 100;
      if (!Number.isFinite(limit) || limit < 1) limit = 1;
      if (limit > 100) limit = 100;
      let offset = q["offset"] ? Number(q["offset"]) : 0;
      if (!Number.isFinite(offset) || offset < 0) offset = 0;

      const rows = await withClient(async (c) => {
        const { rows } = await c.query(
          `select id, twap_id, wallet, ts, asset, size, price, side, fee from fills where twap_id = $1 order by ts asc limit $2 offset $3`,
          [id, limit + 1, offset],
        );
        return rows as any[];
      });
      if (rows.length === 0) return json({ status: 404, body: { error: "Not found" } });
      const hasMore = rows.length > limit;
      if (hasMore) rows.pop();
      const sizes = rows.map((r: any) => (r.size != null ? Number(r.size) : 0));
      const prices = rows.map((r: any) => (r.price != null ? Number(r.price) : 0));
      const vol = sizes.reduce((a, b) => a + b, 0);
      const vwSum = rows.reduce((a: number, r: any, i: number) => a + (sizes[i] || 0) * (prices[i] || 0), 0);
      const avgPrice = vol > 0 ? vwSum / vol : null;
      const trades = rows.map((r: any) => ({
        id: Number(r.id),
        twap_id: r.twap_id,
        wallet_address: r.wallet,
        timestamp: r.ts,
        asset: r.asset,
        quantity: r.size != null ? Number(r.size) : null,
        price: r.price != null ? Number(r.price) : null,
        side: r.side === "B" ? "buy" : r.side === "A" ? "sell" : r.side,
        fee: r.fee != null ? Number(r.fee) : null,
        exchange: "hyperliquid",
      }));
      return json({
        body: {
          twap_id: id,
          total_volume: vol,
          avg_price: avgPrice,
          trades,
          pagination: {
            limit,
            offset,
            has_more: hasMore,
          },
        },
      });
    }
    if (url.pathname === "/status") {
      const authErr = requireApiKey(req);
      if (authErr) return authErr;
      const status = await withClient(async (c) => {
        const lastIngest = await c.query(`select max(ingested_at) as ts from ingested_files`);
        const total = await c.query(`select count(*)::bigint as cnt from fills`);
        return {
          last_ingestion: lastIngest.rows[0]?.ts || null,
          total_records: Number(total.rows[0]?.cnt || 0),
          status: Number(total.rows[0]?.cnt || 0) > 0 ? "success" : "no_data",
          last_error: null,
        };
      });
      return json({ body: status });
    }
    if (url.pathname === "/coverage") {
      const authErr = requireApiKey(req);
      if (authErr) return authErr;
      const coverage = await withClient(async (c) => {
        const result = await c.query(`
          select 
            min(date(ts)) as first_date,
            max(date(ts)) as last_date,
            count(distinct date(ts)) as days_with_data,
            count(*)::bigint as total_fills
          from fills
          where ts is not null
        `);
        const row = result.rows[0];
        return {
          first_date: row?.first_date || null,
          last_date: row?.last_date || null,
          days_with_data: Number(row?.days_with_data || 0),
          total_fills: Number(row?.total_fills || 0),
        };
      });
      return json({ body: coverage });
    }
    if (url.pathname === "/healthz") return handleHealth();
    if (url.pathname === "/twaps") return handleTwaps(url);
    if (url.pathname === "/") {
      return new Response("OK", { status: 200 });
    }
    return new Response("Not found", { status: 404 });
  },
});

console.log(`API listening on http://${server.hostname}:${server.port}`);

process.on("SIGINT", async () => {
  await dbPool.end();
  process.exit(0);
});

