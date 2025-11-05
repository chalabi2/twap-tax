# twap-tax

API and ETL for Hyperliquid trade fills, grouped by TWAP order IDs. You can test the API @ [https://twap-backend.jchalabi.xyz/](https://twap-backend.jchalabi.xyz/). No authentication is required. An example query is:

```bash
curl 'https://twap-backend.jchalabi.xyz/twaps?wallet=0x5b5d51203a0f9079f8aeb098a6523a13f298c060&limit=10'
```

## Setup

**Prerequisites:**

- Bun
- PostgreSQL
- AWS CLI (configured)
- lz4 (`apt install liblz4-tool` or `brew install lz4`)

Ensure you have your AWS credentials for your IAM user with S3 permissions. This is required to pull the fills from S3 bucket provided by Hyperliquid.

**Install:**

```bash
bun install
createdb twap_tax
bun run migrate
```

**Environment (.env):**

```bash
DATABASE_URL="postgresql://postgres@localhost:5432/twap_tax"
PORT=3000
API_KEY=your-secret-key-here  # Optional: omit to disable auth
AWS_REQUEST_PAYER=requester
HL_S3_FILLS_PREFIX=s3://hl-mainnet-node-data/node_fills_by_block/hourly
AWS_CLI_PATH=/opt/homebrew/bin/aws
UNLZ4_PATH=/opt/homebrew/bin/unlz4
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-1
BULK_INSERT_CHUNK_SIZE=1000
INGEST_CONCURRENCY=6
MAX_BLOCKS=0 # 0 = all blocks
BACKFILL_DAYS=180
```

**Note:** If `API_KEY` is not set, all endpoints are public. If set, all endpoints except `/healthz` and `/` require the `X-API-Key` header.

## Usage

**Start API:**

```bash
bun run dev
```

**Load data:**

```bash
# Last 180 days (one-time backfill)
bun run etl:backfill

# Specific day
bun run etl:day 2025-11-01

# Yesterday (for daily cron)
bun run etl:daily
```

## API Endpoints

**Authentication:** Set `API_KEY` in `.env` to require `X-API-Key` header. If not set, all endpoints are public.

### Pagination

All list endpoints support pagination with the following query parameters:

- `limit` - Results per page (default: 100, max: 100)
- `offset` - Number of results to skip (default: 0)
- `include_total` - Set to `1` or `true` to include total count (optional, slower)

**Pagination response fields:**

- `limit` - Requested page size
- `offset` - Current offset
- `current_page` - Current page number (1-indexed)
- `returned` - Number of items in current response
- `has_more` - Whether more results exist
- `next_offset` - Offset for next page (null if no more results)
- `prev_offset` - Offset for previous page (null if on first page)
- `total` - Total count (only if `include_total=1`)
- `total_pages` - Total pages (only if `include_total=1`)

**Navigation examples:**

```bash
# First page
curl '.../trades?limit=50'

# Next page
curl '.../trades?limit=50&offset=50'

# With total count (slower)
curl '.../trades?limit=50&include_total=1'
```

### GET /trades

Get individual trade fills with filters.

```bash
# With API key
curl -H "X-API-Key: xxx" \
  'http://localhost:3000/trades?wallet_addresses=0xabc,0xdef&asset=ETH&limit=50&offset=0'

# Without auth (if API_KEY not configured)
curl 'https://twap-backend.jchalabi.xyz/trades?wallet_addresses=0xabc,0xdef&asset=ETH&limit=50'
```

**Query params:** `wallet_addresses` (comma-separated), `asset`, `twap_id`, `start_date`, `end_date`, `limit`, `offset`

**Response:**

```json
{
  "data": [
    {
      "id": 123,
      "twap_id": "abc-123",
      "wallet_address": "0xabc...",
      "timestamp": "2025-11-01T12:00:00Z",
      "asset": "ETH",
      "quantity": 1.5,
      "price": 3500.0,
      "side": "buy",
      "fee": 0.05,
      "exchange": "hyperliquid"
    }
  ],
  "pagination": {
    "limit": 50,
    "offset": 0,
    "current_page": 1,
    "returned": 50,
    "has_more": true,
    "next_offset": 50,
    "prev_offset": null
  }
}
```

### GET /twaps

Get TWAPs with aggregated stats (paginated at TWAP level, not fill level).

```bash
curl -H "X-API-Key: xxx" \
  'http://localhost:3000/twaps?wallet=0x5b5d51203a0f9079f8aeb098a6523a13f298c060&limit=10'
```

**Query params:**

- `wallet` - Filter by wallet address
- `asset` - Filter by asset symbol
- `start` - Start date (ISO 8601)
- `end` - End date (ISO 8601)
- `limit` - TWAPs per page (max 100, default 100)
- `offset` - Page offset

**Response:**

```json
{
  "data": [
    {
      "twapId": "abc-123",
      "wallet": "0x5b5d...",
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
          "side": "B",
          "price": 3430.5,
          "size": 2.0,
          "txHash": "0x..."
        }
      ]
    }
  ],
  "pagination": {
    "limit": 10,
    "offset": 0,
    "current_page": 1,
    "returned": 10,
    "has_more": true,
    "next_offset": 10,
    "prev_offset": null
  }
}
```

### GET /twap/{twap_id}

Get all trades for a specific TWAP order with aggregated metrics.

```bash
curl -H "X-API-Key: xxx" \
  'http://localhost:3000/twap/abc-123?limit=100&offset=0'
```

**Query params:** `limit`, `offset`

**Response:**

```json
{
  "twap_id": "abc-123",
  "total_volume": 67.5,
  "avg_price": 3485.2,
  "trades": [...],
  "pagination": { "limit": 100, "offset": 0, "has_more": false }
}
```

### GET /status

System status and ingestion info.

```bash
curl -H "X-API-Key: xxx" http://localhost:3000/status
```

**Response:**

```json
{
  "last_ingestion": "2025-11-03T14:23:45Z",
  "total_records": 7655360,
  "status": "success",
  "last_error": null
}
```

### GET /coverage

Data coverage information (first/last date, total days).

```bash
curl -H "X-API-Key: xxx" http://localhost:3000/coverage
```

**Response:**

```json
{
  "first_date": "2025-05-06",
  "last_date": "2025-11-03",
  "days_with_data": 180,
  "total_fills": 7655360
}
```

### GET /healthz

Health check (no auth required).

```bash
curl http://localhost:3000/healthz
```

**Response:**

```json
{ "ok": true }
```

### GET /

Simple root endpoint. Returns "OK" with status 200 (no auth required).

## Production Deployment

See `deploy/README.md` for systemd service setup.

**Quick start:**

```bash
bun run build
sudo cp deploy/*.service deploy/*.timer /etc/systemd/system/
sudo systemctl enable --now twap-tax-api
sudo systemctl enable --now twap-tax-etl.timer
```

The timer runs daily ETL at 2 AM automatically.
