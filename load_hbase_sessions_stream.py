import json
import ijson
import happybase
import argparse
from pathlib import Path
from decimal import Decimal

def json_safe(x):
    # Convert Decimal -> float for JSON
    if isinstance(x, Decimal):
        return float(x)
    # If anything else weird appears, fail loudly (better for debugging)
    raise TypeError(f"Type not serializable: {type(x)}")

def b(x):
    if x is None:
        return b""
    return str(x).encode("utf-8")

def clean_part(x: str) -> str:
    # Prevent rowkey issues (rare but safe)
    return str(x).replace("\n", " ").replace("\r", " ").strip()

def load_one_file(table, file_path: Path, batch_size: int, flush_every: int = 2000, limit: int = 0):
    inserted = 0
    skipped = 0
    batch = table.batch(batch_size=batch_size)

    print(f"Reading: {file_path.name}")

    with open(file_path, "rb") as f:
        for s in ijson.items(f, "item"):
            try:
                user_id = clean_part(s.get("user_id", ""))
                start_time = clean_part(s.get("start_time", ""))
                session_id = clean_part(s.get("session_id", ""))

                rowkey = f"{user_id}|{start_time}|{session_id}".encode("utf-8")

                geo = s.get("geo_data", {}) or {}
                device = s.get("device_profile", {}) or {}
                page_views = s.get("page_views", []) or []
                cart = s.get("cart_contents", {}) or {}
                viewed_products = s.get("viewed_products", []) or []

                data = {
                    b"meta:session_id": b(session_id),
                    b"meta:user_id": b(user_id),
                    b"meta:start_time": b(start_time),
                    b"meta:end_time": b(s.get("end_time")),
                    b"meta:conversion_status": b(s.get("conversion_status")),
                    b"meta:referrer": b(s.get("referrer")),

                    b"geo:city": b((geo or {}).get("city")),
                    b"geo:state": b((geo or {}).get("state")),
                    b"geo:country": b((geo or {}).get("country")),
                    b"geo:ip_address": b((geo or {}).get("ip_address")),

                    b"device:type": b((device or {}).get("type")),
                    b"device:os": b((device or {}).get("os")),
                    b"device:browser": b((device or {}).get("browser")),

                    b"stats:duration_seconds": b(s.get("duration_seconds")),
                    b"stats:page_views_count": b(len(page_views)),
                    b"stats:viewed_products_count": b(len(viewed_products)),
                    b"stats:cart_distinct_items": b(len(cart)),

                    # ✅ FIX: use default=json_safe so Decimal works
                    b"events:page_views_json": json.dumps(page_views, ensure_ascii=False, default=json_safe).encode("utf-8"),
                    b"events:cart_json": json.dumps(cart, ensure_ascii=False, default=json_safe).encode("utf-8"),
                }

                batch.put(rowkey, data)
                inserted += 1

                # flush periodically (keeps memory stable)
                if inserted % flush_every == 0:
                    batch.send()
                    batch = table.batch(batch_size=batch_size)
                    print(f"  inserted {inserted:,} rows from {file_path.name}...")

                if limit and inserted >= limit:
                    break

            except Exception as e:
                skipped += 1
                # continue instead of crashing whole job
                if skipped <= 5:
                    print(f"  ⚠️ skipped a row due to error: {e}")
                elif skipped == 6:
                    print("  ⚠️ too many row errors; continuing silently...")

    # final flush
    batch.send()
    print(f"Finished {file_path.name}: inserted={inserted:,}, skipped={skipped:,}")
    return inserted, skipped

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="Folder containing sessions_0.json ... sessions_9.json")
    ap.add_argument("--table", default="user_sessions")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--batch-size", type=int, default=700, help="500-1000 is safe on 8GB")
    ap.add_argument("--flush-every", type=int, default=2000, help="send batch every N rows (memory safe)")
    args = ap.parse_args()

    data_dir = Path(args.dir)
    if not data_dir.exists():
        raise FileNotFoundError(f"Folder not found: {data_dir}")

    files = [data_dir / f"sessions_{i}.json" for i in range(10)]
    missing = [str(p) for p in files if not p.exists()]
    if missing:
        raise FileNotFoundError("Missing files:\n" + "\n".join(missing))

    conn = happybase.Connection(host=args.host, port=args.port)
    conn.open()
    table = conn.table(args.table)

    grand_total = 0
    grand_skipped = 0

    for p in files:
        print(f"\n=== Loading {p.name} ===")
        inserted, skipped = load_one_file(
            table,
            p,
            batch_size=args.batch_size,
            flush_every=args.flush_every
        )
        grand_total += inserted
        grand_skipped += skipped
        print(f"TOTAL so far: inserted={grand_total:,}, skipped={grand_skipped:,}")

    conn.close()
    print(f"\n✅ ALL DONE. Inserted TOTAL rows into '{args.table}': {grand_total:,} (skipped {grand_skipped:,})")

if __name__ == "__main__":
    main()
