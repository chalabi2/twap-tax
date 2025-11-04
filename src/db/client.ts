import { Pool } from "pg";

export type DatabaseConfig = {
  connectionString: string;
  max?: number;
};

const connectionString = process.env["DATABASE_URL"] ?? "postgres://localhost:5432/twap_tax";

export const dbPool = new Pool({
  connectionString,
  max: Number(process.env["PGPOOL_MAX"] ?? 10),
  ssl: process.env["PGSSL"] === "1" ? { rejectUnauthorized: false } : undefined,
});

export async function withClient<T>(fn: (client: import("pg").PoolClient) => Promise<T>): Promise<T> {
  const client = await dbPool.connect();
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}

export async function ensureConnected(): Promise<void> {
  await withClient(async (c) => {
    await c.query("select 1");
  });
}

