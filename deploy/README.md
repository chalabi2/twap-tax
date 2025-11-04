# Production Deployment

## Prerequisites

- Bun installed on production server
- PostgreSQL running and accessible
- AWS CLI configured with credentials
- lz4 decompression tool installed (`sudo apt-get install liblz4-tool`)

## Initial Setup

### 1. Build the project

```bash
bun run build
```

### 2. Run database migrations

```bash
bun run migrate
```

### 3. Run initial backfill (one-time)

```bash
bun run etl:backfill
```

## Service Installation

### 1. Edit service files

Update these values in the `.service` files:

- `YOUR_USER` → your actual username
- `/path/to/twap-tax` → actual project path

### 2. Copy service files

```bash
sudo cp deploy/twap-tax-api.service /etc/systemd/system/
sudo cp deploy/twap-tax-etl.service /etc/systemd/system/
sudo cp deploy/twap-tax-etl.timer /etc/systemd/system/
```

### 3. Reload systemd

```bash
sudo systemctl daemon-reload
```

### 4. Enable and start the API

```bash
sudo systemctl enable twap-tax-api
sudo systemctl start twap-tax-api
sudo systemctl status twap-tax-api
```

### 5. Enable and start the daily ETL timer

```bash
sudo systemctl enable twap-tax-etl.timer
sudo systemctl start twap-tax-etl.timer
sudo systemctl list-timers twap-tax-etl.timer
```

## Management Commands

### API Server

```bash
# View logs
sudo journalctl -u twap-tax-api -f

# Restart
sudo systemctl restart twap-tax-api

# Stop
sudo systemctl stop twap-tax-api
```

### ETL Jobs

```bash
# View ETL logs
sudo journalctl -u twap-tax-etl -f

# Run ETL manually (doesn't affect timer)
sudo systemctl start twap-tax-etl

# Check when next ETL will run
sudo systemctl list-timers twap-tax-etl.timer
```

## Environment Variables

Ensure your `.env` file includes:

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/twap_tax
PORT=3000
API_KEY=your-secret-key
AWS_REQUEST_PAYER=requester
HL_S3_FILLS_PREFIX=s3://hl-mainnet-node-data/node_fills_by_block
INGEST_CONCURRENCY=4
```

## Alternative: Docker Deployment

If you prefer Docker, create a `Dockerfile`:

```dockerfile
FROM oven/bun:1
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
RUN bun run build
CMD ["bun", "run", "dist/server.js"]
```

Then use `docker-compose.yml` for orchestration.

## Monitoring

Consider adding:

- Health check endpoint monitoring (GET /healthz)
- Log aggregation (e.g., Loki, ELK)
- Alerts for ETL job failures
- Database backup automation
