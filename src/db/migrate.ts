import { readFile } from "fs/promises";
import { resolve } from "path";
import { withClient } from "./client";

async function run(): Promise<void> {
  const schemaPath = resolve(process.cwd(), "sql/schema.sql");
  const sql = await readFile(schemaPath, "utf8");
  await withClient(async (client) => {
    await client.query(sql);
  });
  console.log("Migrations applied.");
}

run().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});

