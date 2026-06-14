"""
UEX marketplace poller for Star Citizen minerals -> Supabase Postgres.

Every cycle it:
  - syncs the mineral list from /commodities (excluding the 3 junk id_item=0 rows,
    which UEX treats as "no filter" and would dump the whole marketplace),
  - fetches sell/buy listings for each mineral,
  - writes a listing_snapshot row ONLY when in_stock/price/is_sold_out changed vs
    the previous observation (change-only writes -> far less storage),
  - upserts current state into listing_state (the diff source + "seen this run" set),
  - derives fill_event rows: sold_out / qty_drop (high confidence), and 'expired'
    for listings that were live last run but vanished this run (low confidence),
  - records a poll_run heartbeat row so cadence survives change-only writes.

Run:  python3 poller.py            # loop forever (systemd service)
      python3 poller.py --once     # single cycle (testing / cron)
"""
import os, sys, time, pathlib, datetime as dt
import urllib.request, urllib.parse, json
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values

INTERVAL_MIN = 25
BASE = "https://api.uexcorp.space/2.0"


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


ENV = load_env()
DB = dict(host="aws-1-us-west-2.pooler.supabase.com", port=5432,
          dbname="postgres", user="postgres.ibwimbhflnzuodrtxbrl",
          password=ENV["SUPABASE_DB_PASSWORD"], connect_timeout=20)
HEADERS = {"Authorization": f"Bearer {ENV['UEX_API_TOKEN']}",
           # A browser User-Agent is REQUIRED or Cloudflare returns 403.
           "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
           "Accept": "application/json"}


def api(endpoint, retries=3, **params):
    url = f"{BASE}/{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=40) as r:
                return json.load(r).get("data", [])
        except Exception as e:
            last = e
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))  # 2s, 4s backoff
    raise last


def parse_ts(v):
    if not v:
        return None
    try:
        return dt.datetime.fromtimestamp(int(v), dt.timezone.utc)
    except (ValueError, TypeError):
        return None


def numeq(a, b):
    """Numeric equality that survives Decimal(DB) vs float/int(API) and Nones."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return Decimal(str(a)) == Decimal(str(b))
    except Exception:
        return a == b


def sync_commodities(cur):
    rows = api("commodities")
    minerals = [c for c in rows if c.get("is_mineral") or c.get("is_raw") or c.get("is_refinable")]
    execute_values(cur, """
        INSERT INTO commodity (id_commodity,id_parent,id_item,name,code,kind,
            is_mineral,is_raw,is_refined,is_refinable,is_harvestable,last_synced)
        VALUES %s
        ON CONFLICT (id_commodity) DO UPDATE SET
            id_parent=EXCLUDED.id_parent, id_item=EXCLUDED.id_item,
            name=EXCLUDED.name, code=EXCLUDED.code, kind=EXCLUDED.kind,
            is_mineral=EXCLUDED.is_mineral, is_raw=EXCLUDED.is_raw,
            is_refined=EXCLUDED.is_refined, is_refinable=EXCLUDED.is_refinable,
            is_harvestable=EXCLUDED.is_harvestable, last_synced=now()
    """, [(c["id"], c.get("id_parent"), c.get("id_item"), c.get("name"),
           c.get("code"), c.get("kind"), bool(c.get("is_mineral")),
           bool(c.get("is_raw")), bool(c.get("is_refined")),
           bool(c.get("is_refinable")), bool(c.get("is_harvestable")),
           dt.datetime.now(dt.timezone.utc)) for c in minerals])
    # id_item=0 is a sentinel UEX reads as "no filter" -> dumps the whole market. Skip it.
    cur.execute("SELECT DISTINCT id_item FROM commodity WHERE id_item IS NOT NULL AND id_item > 0")
    return sorted({r[0] for r in cur.fetchall()})


def fetch_mineral_listings(item_ids):
    out = []
    for iid in item_ids:
        try:
            out.extend(api("marketplace_listings", id_item=iid))
        except Exception as e:
            print(f"  item {iid} failed: {e}", flush=True)
        time.sleep(0.5)
    return out


def load_prev_state(cur):
    """Current state per listing, used for change-only diffing and fill detection."""
    cur.execute("""SELECT listing_id, last_in_stock, last_is_sold_out, last_price, last_seen_run
                   FROM listing_state""")
    return {r[0]: dict(stock=r[1], sold=r[2], price=r[3], last_run=r[4]) for r in cur.fetchall()}


def run_cycle(conn):
    cur = conn.cursor()

    # Open a heartbeat row and capture the immediately-prior run id.
    cur.execute("SELECT max(run_id) FROM poll_run")
    prev_run_id = cur.fetchone()[0]
    cur.execute("INSERT INTO poll_run (started_at) VALUES (now()) RETURNING run_id")
    run_id = cur.fetchone()[0]
    conn.commit()

    item_ids = sync_commodities(cur); conn.commit()
    mineral_set = set(item_ids)
    listings = fetch_mineral_listings(item_ids)
    now = dt.datetime.now(dt.timezone.utc)
    prev = load_prev_state(cur)

    snap_rows, fill_rows, state_rows = [], [], []
    seen = set()
    for l in listings:
        item = l.get("id_item")
        # Defensive: only our minerals, never the id_item=0 sentinel.
        if item not in mineral_set or not item or item <= 0:
            continue
        lid = l["id"]; seen.add(lid)
        stock = l.get("in_stock"); sold = bool(l.get("is_sold_out")); price = l.get("price")
        qual = l.get("quality"); neg = l.get("total_negotiations"); views = l.get("total_views")

        p = prev.get(lid)
        changed = (p is None or p["stock"] != stock or p["sold"] != sold
                   or not numeq(p["price"], price))
        if changed:
            snap_rows.append((now, lid, item, l.get("operation"), l.get("title"),
                qual, price, l.get("currency"), stock, sold, neg, views,
                parse_ts(l.get("date_added")), parse_ts(l.get("date_expiration"))))

        # Fill detection only against a *continuous* observation (live last run).
        if p is not None and p["last_run"] == prev_run_id:
            if sold and not p["sold"]:
                fill_rows.append((lid, item, qual, price, p["stock"], now, "sold_out", "high"))
            elif p["stock"] is not None and stock is not None and stock < p["stock"]:
                fill_rows.append((lid, item, qual, price, p["stock"] - stock, now, "qty_drop", "high"))

        state_rows.append((lid, item, l.get("operation"), l.get("title"), qual,
                           price, stock, sold, neg, views, now, run_id))

    # Disappearance -> 'expired': live in the prior run, absent now, and not already
    # sold out (those were captured as sold_out). Ambiguous sale-vs-expiry => low conf.
    expired = 0
    if prev_run_id is not None:
        cur.execute("""SELECT listing_id, id_item, quality, last_price, last_in_stock
                       FROM listing_state
                       WHERE last_seen_run = %s AND last_is_sold_out = FALSE
                         AND COALESCE(last_in_stock,0) > 0""", (prev_run_id,))
        for lid, item, qual, lprice, lstock in cur.fetchall():
            if lid not in seen:
                fill_rows.append((lid, item, qual, lprice, lstock, now, "expired", "low"))
                expired += 1

    if snap_rows:
        execute_values(cur, """INSERT INTO listing_snapshot (observed_at,listing_id,
            id_item,operation,title,quality,price,currency,in_stock,is_sold_out,
            total_negotiations,total_views,date_added,date_expiration) VALUES %s""", snap_rows)
    if fill_rows:
        execute_values(cur, """INSERT INTO fill_event (listing_id,id_item,quality,
            fill_price,fill_qty,observed_at,signal_type,confidence) VALUES %s
            ON CONFLICT (listing_id,observed_at,signal_type) DO NOTHING""", fill_rows)
    if state_rows:
        execute_values(cur, """INSERT INTO listing_state (listing_id,id_item,operation,
            title,quality,last_price,last_in_stock,last_is_sold_out,last_negotiations,
            last_views,last_seen_at,last_seen_run) VALUES %s
            ON CONFLICT (listing_id) DO UPDATE SET
              id_item=EXCLUDED.id_item, operation=EXCLUDED.operation, title=EXCLUDED.title,
              quality=EXCLUDED.quality, last_price=EXCLUDED.last_price,
              last_in_stock=EXCLUDED.last_in_stock, last_is_sold_out=EXCLUDED.last_is_sold_out,
              last_negotiations=EXCLUDED.last_negotiations, last_views=EXCLUDED.last_views,
              last_seen_at=EXCLUDED.last_seen_at, last_seen_run=EXCLUDED.last_seen_run""", state_rows)

    cur.execute("""UPDATE poll_run SET finished_at=now(), listings_seen=%s,
                   snapshots_written=%s, fills_detected=%s WHERE run_id=%s""",
                (len(seen), len(snap_rows), len(fill_rows), run_id))
    conn.commit()
    print(f"[{now:%Y-%m-%d %H:%M}] run {run_id} | minerals {len(item_ids)} | "
          f"live {len(seen)} | snapshots {len(snap_rows)} | fills {len(fill_rows)} "
          f"(expired {expired})", flush=True)


def main():
    once = "--once" in sys.argv
    print(f"Poller started ({'single cycle' if once else 'loop'}).", flush=True)
    failed = False
    while True:
        try:
            conn = psycopg2.connect(**DB)
            run_cycle(conn)
            conn.close()
            failed = False
        except Exception as e:
            print("ERROR:", repr(e), flush=True)
            failed = True
        if once:
            break
        time.sleep(INTERVAL_MIN * 60)
    # In --once mode (CI/cron) surface failure as a non-zero exit so the run goes
    # red; the long-running loop deliberately keeps going on transient errors.
    if once and failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
