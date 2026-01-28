# integrated_query_engagement_vs_spend.py
# Integrated Analytical Query:
# "Do highly engaged users (more sessions / longer sessions) also become high spenders?"
#
# Inputs:
#   - MongoDB: ecommerce_db.transactions (spend + order count)
#   - HBase:   user_sessions table (session count + total duration)
#
# Outputs (in --out-dir):
#   1) engagement_metrics.csv
#   2) spend_metrics.csv
#   3) integrated_metrics.csv
#   4) integrated_summary.txt


import argparse
import os
from collections import defaultdict
from math import sqrt

import pandas as pd
from pymongo import MongoClient
import happybase


def safe_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, bytes):
            x = x.decode("utf-8", errors="ignore")
        return int(float(str(x)))
    except Exception:
        return default


def hbase_user_engagement(hbase_host: str, hbase_port: int, table_name: str, limit_rows: int = 0):
    """
    Streams through HBase sessions and computes:
      - sessions_count per user
      - total_duration_seconds per user
    Returns dict: user_id -> (sessions_count, total_duration_seconds)
    """
    conn = happybase.Connection(host=hbase_host, port=hbase_port, timeout=60000)
    conn.open()
    table = conn.table(table_name)

    COL_USER = b"meta:user_id"
    COL_DUR = b"stats:duration_seconds"

    sessions_count = defaultdict(int)
    total_duration = defaultdict(int)

    scanned = 0
    print(f"[HBase] Scanning table '{table_name}' on {hbase_host}:{hbase_port} ...")

    for _, data in table.scan(columns=[COL_USER, COL_DUR], batch_size=2000):
        user_id_b = data.get(COL_USER, b"")
        user_id = user_id_b.decode("utf-8", errors="ignore").strip()

        if not user_id:
            continue

        dur = safe_int(data.get(COL_DUR, b"0"), default=0)

        sessions_count[user_id] += 1
        total_duration[user_id] += dur

        scanned += 1
        if scanned % 20000 == 0:
            print(f"[HBase] scanned {scanned:,} rows ...")

        if limit_rows and scanned >= limit_rows:
            print(f"[HBase] limit reached: {limit_rows:,} rows")
            break

    conn.close()

    print(f"[HBase] DONE. scanned={scanned:,} rows, users={len(sessions_count):,}")
    return sessions_count, total_duration, scanned


def mongo_user_spend(mongo_uri: str, db_name: str, tx_collection: str):
    """
    Uses MongoDB aggregation to compute per-user:
      - total_spent (sum of total)
      - num_orders  (count)
    Returns dict: user_id -> (total_spent, num_orders)
    """
    client = MongoClient(mongo_uri)
    db = client[db_name]
    col = db[tx_collection]

    print(f"[MongoDB] Aggregating spend from {db_name}.{tx_collection} ...")

    pipeline = [
        {"$group": {
            "_id": "$user_id",
            "total_spent": {"$sum": "$total"},
            "num_orders": {"$sum": 1}
        }},
        {"$project": {
            "_id": 0,
            "user_id": "$_id",
            "total_spent": {"$ifNull": ["$total_spent", 0]},
            "num_orders": {"$ifNull": ["$num_orders", 0]}
        }}
    ]

    spend = {}
    for doc in col.aggregate(pipeline, allowDiskUse=True):
        user_id = doc.get("user_id", "")
        if user_id:
            spend[user_id] = (float(doc.get("total_spent", 0.0)), int(doc.get("num_orders", 0)))

    client.close()
    print(f"[MongoDB] DONE. users with purchases={len(spend):,}")
    return spend


def pearson_corr(xs, ys):
    """Pearson correlation without numpy (safe + lightweight)."""
    n = len(xs)
    if n < 2:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return 0.0
    return num / (den_x * den_y)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
    ap.add_argument("--mongo-db", default="ecommerce_db", help="MongoDB database name")
    ap.add_argument("--mongo-tx", default="transactions", help="MongoDB transactions collection name")

    ap.add_argument("--hbase-host", default="localhost", help="HBase Thrift host")
    ap.add_argument("--hbase-port", type=int, default=9090, help="HBase Thrift port")
    ap.add_argument("--hbase-table", default="user_sessions", help="HBase table name")

    ap.add_argument("--out-dir", default="integrated_out", help="Output folder")
    ap.add_argument("--limit-hbase-rows", type=int, default=0,
                    help="For quick test: scan only first N HBase rows (0 = scan all)")

    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    # 1) HBase engagement metrics
    sessions_count, total_duration, scanned_rows = hbase_user_engagement(
        args.hbase_host, args.hbase_port, args.hbase_table, limit_rows=args.limit_hbase_rows
    )

    # Build engagement dataframe
    engagement_rows = []
    for user_id in sessions_count.keys():
        sc = sessions_count[user_id]
        td = total_duration[user_id]
        engagement_rows.append({
            "user_id": user_id,
            "sessions_count": sc,
            "total_duration_seconds": td,
            "avg_duration_seconds": (td / sc) if sc else 0.0
        })

    df_eng = pd.DataFrame(engagement_rows)
    df_eng.to_csv(os.path.join(args.out_dir, "engagement_metrics.csv"), index=False)
    print(f"[OUT] saved engagement_metrics.csv ({len(df_eng):,} rows)")

    # 2) MongoDB spend metrics
    spend = mongo_user_spend(args.mongo_uri, args.mongo_db, args.mongo_tx)

    spend_rows = []
    for user_id, (total_spent, num_orders) in spend.items():
        spend_rows.append({
            "user_id": user_id,
            "total_spent": round(total_spent, 2),
            "num_orders": num_orders
        })

    df_spend = pd.DataFrame(spend_rows)
    df_spend.to_csv(os.path.join(args.out_dir, "spend_metrics.csv"), index=False)
    print(f"[OUT] saved spend_metrics.csv ({len(df_spend):,} rows)")

    # 3) Join (integrated metrics)
    df = df_eng.merge(df_spend, on="user_id", how="left")
    df["total_spent"] = df["total_spent"].fillna(0.0)
    df["num_orders"] = df["num_orders"].fillna(0).astype(int)

    # sessions_count + normalized duration effect
    df["engagement_score"] = df["sessions_count"] * (1.0 + (df["avg_duration_seconds"] / 600.0))
    df["spend_per_order"] = df.apply(
        lambda r: (r["total_spent"] / r["num_orders"]) if r["num_orders"] > 0 else 0.0, axis=1
    )

    # Segments
    df["is_buyer"] = df["num_orders"] > 0
    # thresholds based on quantiles (robust)
    ses_q = df["sessions_count"].quantile(0.75)
    spend_q = df["total_spent"].quantile(0.75)

    def segment(row):
        high_eng = row["sessions_count"] >= ses_q
        high_spend = row["total_spent"] >= spend_q
        if high_eng and high_spend:
            return "HighEngagement-HighSpend"
        if high_eng and not high_spend:
            return "HighEngagement-LowSpend"
        if (not high_eng) and high_spend:
            return "LowEngagement-HighSpend"
        return "LowEngagement-LowSpend"

    df["segment"] = df.apply(segment, axis=1)

    out_csv = os.path.join(args.out_dir, "integrated_metrics.csv")
    df.sort_values(["total_spent", "sessions_count"], ascending=False).to_csv(out_csv, index=False)
    print(f"[OUT] saved integrated_metrics.csv ({len(df):,} rows)")

    # 4) Summary + correlations
    # For correlations, we only consider buyers (spend > 0) to avoid a “flat” spend distribution
    buyers = df[df["total_spent"] > 0].copy()

    corr_spend_sessions = pearson_corr(buyers["total_spent"].tolist(), buyers["sessions_count"].tolist()) if len(buyers) else 0.0
    corr_spend_duration = pearson_corr(buyers["total_spent"].tolist(), buyers["total_duration_seconds"].tolist()) if len(buyers) else 0.0

    seg_counts = df["segment"].value_counts().to_dict()

    summary_path = os.path.join(args.out_dir, "integrated_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("INTEGRATED ANALYTICAL QUERY SUMMARY\n")
        f.write("Query: Do highly engaged users also become high spenders?\n\n")

        f.write("Data Sources:\n")
        f.write(f"- HBase table '{args.hbase_table}' (sessions): scanned_rows={scanned_rows}\n")
        f.write(f"- MongoDB '{args.mongo_db}.{args.mongo_tx}' (transactions)\n\n")

        f.write("Key Metrics:\n")
        f.write("- Engagement: sessions_count, total_duration_seconds, avg_duration_seconds\n")
        f.write("- Spend: total_spent, num_orders\n\n")

        f.write("Correlations (buyers only, total_spent > 0):\n")
        f.write(f"- corr(total_spent, sessions_count) = {corr_spend_sessions:.4f}\n")
        f.write(f"- corr(total_spent, total_duration_seconds) = {corr_spend_duration:.4f}\n\n")

        f.write("Segments (based on 75th percentiles):\n")
        for k, v in seg_counts.items():
            f.write(f"- {k}: {v}\n")

        f.write("\nVERY IMPORTANT (for your report):\n")
        f.write("This integrated query demonstrates polyglot persistence: MongoDB stores transactional value, "
                "HBase stores high-volume clickstream sessions, and the analytics layer joins these signals "
                "by user_id to evaluate the relationship between engagement and spending.\n")

    print(f"[OUT] saved integrated_summary.txt")
    print("\nDONE. Integrated query executed successfully.")


if __name__ == "__main__":
    main()
