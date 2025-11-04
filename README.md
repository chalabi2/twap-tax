# twap-tax

ETL + API for Hyperliquid TWAP-related fills.

- Pulls historical fills from S3 requester-pays buckets to a local Postgres database
- Decompresses `.lz4` assets using `unlz4`
- Exposes a lightweight HTTP API via Bun to query TWAPs by wallet/timeframe, grouped by `twap_id`

## Prerequisites

- Bun (`brew install bun`)
- Postgres running locally (default: `postgres://localhost:5432/twap_tax`)
- AWS CLI (`brew install awscli`) configured with credentials
- LZ4 CLI (`brew install lz4`) for `unlz4`

## Configuration

Environment variables:

- `DATABASE_URL` (default: `postgres://localhost:5432/twap_tax`)
- `PORT` (default: `3000`)
- `AWS_REQUEST_PAYER` (default: `requester`)
- `HL_S3_FILLS_PREFIX` (default: `s3://hl-mainnet-node-data/node_fills_by_block`)
- `DATA_TMP_DIR` optional temp directory for downloads

fish shell example:

```fish
set -x DATABASE_URL postgres://localhost:5432/twap_tax
set -x AWS_REQUEST_PAYER requester
set -x HL_S3_FILLS_PREFIX s3://hl-mainnet-node-data/node_fills_by_block
```

## Install deps

```fish
bun install
```

## Database setup

Create the database if missing and run migrations:

```fish
createdb twap_tax ^/dev/null; and echo ok
bun run src/db/migrate.ts
```

## Run ETL (daily)

Run for a specific day (defaults to yesterday if no arg). Date can be `YYYY-MM-DD` or `YYYYMMDD`.

```fish
# Yesterday
bun run src/etl/run_daily.ts

# Specific day
bun run src/etl/run_day.ts 2025-11-01
```

Notes:

- This uses `aws s3 cp --recursive --request-payer requester` under the hood
- It will automatically `unlz4 --rm` any downloaded `.lz4` files
- An ingestion watermark table prevents duplicate inserts per file

## API

Start server:

```fish
bun run src/server.ts
```

Endpoints:

- `GET /healthz` – health check
- `GET /twaps?wallet=<WALLET>&start=<ISO>&end=<ISO>&asset=<COIN>&includeUngrouped=1` – grouped by `twap_id`
- `GET /twaps/<twapId>` – details for a single TWAP id

Examples:

```fish
curl 'http://localhost:3000/twaps?wallet=0xabc...&start=2025-11-01T00:00:00Z&end=2025-11-02T00:00:00Z'
curl 'http://localhost:3000/twaps/some-twap-id'
```

Response shape (example):

```json
[
  {
    "twapId": "abcd-1234",
    "wallet": "0xabc...",
    "asset": "ETH",
    "fillsCount": 12,
    "totalSize": 42.5,
    "avgPrice": 3450.12,
    "minPrice": 3420.0,
    "maxPrice": 3475.5,
    "startTs": "2025-11-01T00:05:00.000Z",
    "endTs": "2025-11-01T01:35:00.000Z",
    "fills": [
      {
        "ts": "2025-11-01T00:05:00Z",
        "side": "buy",
        "price": 3430.5,
        "size": 2.0,
        "txHash": "0x..."
      }
    ]
  }
]
```

## Production (no containers)

Minimal steps on a server:

```fish
# 1) Clone and enter repo
git clone <your-repo-url> twap-tax; and cd twap-tax

# 2) Create .env
cp .env.example .env; and edit .env

# 3) Install & migrate
bun install
bun run migrate

# 4) Build and run
bun run build
bun run start
```

Daily ETL via cron (uses built artifact and loads .env):

```bash
0 3 * * * bash -lc 'cd /Users/chalabi/Code/twap-tax && bun run etl:daily:built >> /Users/chalabi/Code/twap-tax/etl.log 2>&1'
```

Notes:

- `bun run start` executes `dist/server.js` with `--env-file .env`.
- `bun run etl:daily:built` executes `dist/etl/run_daily.js` with `--env-file .env`.

## Cron (daily)

Example crontab entry (runs at 03:00 UTC daily):

```bash
# Edit crontab
crontab -e

# Add line (adjust env as needed)
0 3 * * * bash -c 'export DATABASE_URL=postgres://localhost:5432/twap_tax && export AWS_REQUEST_PAYER=requester && export HL_S3_FILLS_PREFIX=s3://hl-mainnet-node-data/node_fills_by_block && cd /Users/chalabi/Code/twap-tax && bun run src/etl/run_daily.ts >> /Users/chalabi/Code/twap-tax/etl.log 2>&1'
```

## Notes on data sources

- Hyperliquid historical market data (e.g., L2 books) lives at `s3://hyperliquid-archive/market_data/...` and is `.lz4` compressed
- Trade fills streamed by block: `s3://hl-mainnet-node-data/node_fills_by_block`
- Older formats: `s3://hl-mainnet-node-data/node_fills` and `node_trades`
- Explorer/tx data: `s3://hl-mainnet-node-data/explorer_blocks` and `replica_cmds`

This ETL focuses on `node_fills_by_block`. The parser attempts to derive `twap_id` from common fields (`twapId`, `parentOrderId`, etc.). If the actual structure differs, adjust the field mapping in `src/etl/parse_fill.ts`.
