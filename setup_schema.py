import os, pathlib, psycopg2

# Load secrets from the shared env file (parse directly; never `source` it so a
# `$`/backtick in a password isn't shell-expanded). Falls back to process env.
def load_env():
    env = {}
    p = pathlib.Path(os.path.expanduser("~/.config/orebot.env"))
    if p.exists():
        for line in p.read_text().splitlines():
            s = line.strip()
            if s and not s.startswith("#") and "=" in s:
                k, v = s.split("=", 1)
                env[k.strip()] = v.strip()
    for k in ("SUPABASE_DB_PASSWORD", "UEX_API_TOKEN"):
        env.setdefault(k, os.environ.get(k, ""))
    return env

SCHEMA = """
-- Reference table: minerals we track, pulled from /commodities
CREATE TABLE IF NOT EXISTS commodity (
    id_commodity   INTEGER PRIMARY KEY,   -- UEX commodities.id
    id_parent      INTEGER,               -- links raw ore -> refined form
    id_item        INTEGER,               -- matches listings.id_item
    name           TEXT NOT NULL,
    code           TEXT,
    kind           TEXT,
    is_mineral     BOOLEAN,
    is_raw         BOOLEAN,
    is_refined     BOOLEAN,
    is_refinable   BOOLEAN,
    is_harvestable BOOLEAN,
    last_synced    TIMESTAMPTZ DEFAULT now()
);

-- One row per listing, written ONLY when its in_stock/price/is_sold_out changed
-- from the previous observation (change-only writes). First sighting always writes.
CREATE TABLE IF NOT EXISTS listing_snapshot (
    snapshot_id        BIGSERIAL PRIMARY KEY,
    observed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    listing_id         BIGINT  NOT NULL,        -- listings.id (stable)
    id_item            INTEGER,
    operation          TEXT,                    -- buy / sell
    title              TEXT,
    quality            INTEGER,                 -- raw 0-1000, may be null
    price              NUMERIC,
    currency           TEXT,
    in_stock           INTEGER,                 -- decrements = fills
    is_sold_out        BOOLEAN,
    total_negotiations INTEGER,
    total_views        INTEGER,
    date_added         TIMESTAMPTZ,
    date_expiration    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_snap_listing ON listing_snapshot(listing_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_snap_item    ON listing_snapshot(id_item, observed_at);
CREATE INDEX IF NOT EXISTS idx_snap_time    ON listing_snapshot(observed_at);

-- Derived: inferred sales, from diffing snapshots
CREATE TABLE IF NOT EXISTS fill_event (
    fill_id      BIGSERIAL PRIMARY KEY,
    listing_id   BIGINT NOT NULL,
    id_item      INTEGER,
    quality      INTEGER,
    fill_price   NUMERIC,
    fill_qty     INTEGER,
    observed_at  TIMESTAMPTZ NOT NULL,
    signal_type  TEXT,        -- 'sold_out' | 'qty_drop' | 'expired'
    confidence   TEXT,        -- 'high' | 'medium' | 'low'
    UNIQUE(listing_id, observed_at, signal_type)
);
CREATE INDEX IF NOT EXISTS idx_fill_item ON fill_event(id_item, observed_at);

-- One row per poll cycle: heartbeat + run stats. Survives change-only writes so
-- the dashboard can count true poll cadence and detect a down poller.
CREATE TABLE IF NOT EXISTS poll_run (
    run_id            BIGSERIAL PRIMARY KEY,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    listings_seen     INTEGER,
    snapshots_written INTEGER,
    fills_detected    INTEGER,
    ok                BOOLEAN DEFAULT TRUE,
    note              TEXT
);

-- Current live state of each listing (one row per listing). Two jobs:
--   1) source for change-only diffing (no full snapshot scan needed)
--   2) the "seen last run" set, so listings that vanish can be flagged 'expired'
CREATE TABLE IF NOT EXISTS listing_state (
    listing_id        BIGINT PRIMARY KEY,
    id_item           INTEGER,
    operation         TEXT,
    title             TEXT,
    quality           INTEGER,
    last_price        NUMERIC,
    last_in_stock     INTEGER,
    last_is_sold_out  BOOLEAN,
    last_negotiations INTEGER,
    last_views        INTEGER,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at      TIMESTAMPTZ NOT NULL,
    last_seen_run     BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_state_run  ON listing_state(last_seen_run);
CREATE INDEX IF NOT EXISTS idx_state_seen ON listing_state(last_seen_at);

-- Daily rollups of raw snapshots. The retention job aggregates raw rows older
-- than the retention window into here, then deletes them. fill_event is NEVER
-- deleted; its daily counts are copied here for convenient long-term queries.
CREATE TABLE IF NOT EXISTS snapshot_daily (
    day            DATE    NOT NULL,
    id_item        INTEGER NOT NULL,
    operation      TEXT    NOT NULL,
    quality_band   TEXT    NOT NULL,   -- '0-99'..'900-1000' (100-wide) or 'no_quality'
    listing_count  INTEGER,            -- distinct listings seen that day in band
    snapshot_count INTEGER,
    median_price   NUMERIC,
    p25_price      NUMERIC,
    p75_price      NUMERIC,
    total_stock    INTEGER,
    fills          INTEGER,
    PRIMARY KEY (day, id_item, operation, quality_band)
);
"""

if __name__ == "__main__":
    env = load_env()
    conn = psycopg2.connect(
        host="aws-1-us-west-2.pooler.supabase.com",
        port=5432, dbname="postgres",
        user="postgres.ibwimbhflnzuodrtxbrl", password=env["SUPABASE_DB_PASSWORD"],
        connect_timeout=15)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(SCHEMA)
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_name IN
        ('commodity','listing_snapshot','fill_event','poll_run','listing_state','snapshot_daily')
        ORDER BY table_name;
    """)
    print("Tables present:", [r[0] for r in cur.fetchall()])
    conn.close()
