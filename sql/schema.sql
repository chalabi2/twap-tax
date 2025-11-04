create table if not exists ingested_files (
  s3_key text primary key,
  etag text,
  ingested_at timestamptz default now()
);

create table if not exists fills (
  id bigserial primary key,
  s3_key text not null,
  source_line integer not null,
  item_idx integer not null default 0,
  block_num bigint,
  tx_hash text,
  ts timestamptz,
  wallet text,
  asset text,
  side text,
  price numeric,
  size numeric,
  closed_pnl numeric,
  fee numeric,
  fee_token text,
  dir text,
  builder text,
  oid bigint,
  tid bigint,
  cloid text,
  start_position numeric,
  crossed boolean,
  twap_id text,
  raw jsonb not null,
  unique (s3_key, source_line, item_idx)
);

create index if not exists idx_fills_wallet_ts on fills (wallet, ts);
create index if not exists idx_fills_twap on fills (twap_id);
create index if not exists idx_fills_asset_ts on fills (asset, ts);

-- Backfill-safe alters for existing deployments
alter table if exists fills add column if not exists item_idx integer not null default 0;
alter table if exists fills add column if not exists closed_pnl numeric;
alter table if exists fills add column if not exists fee numeric;
alter table if exists fills add column if not exists fee_token text;
alter table if exists fills add column if not exists dir text;
alter table if exists fills add column if not exists builder text;
alter table if exists fills add column if not exists oid bigint;
alter table if exists fills add column if not exists tid bigint;
alter table if exists fills add column if not exists cloid text;
alter table if exists fills add column if not exists start_position numeric;
alter table if exists fills add column if not exists crossed boolean;

