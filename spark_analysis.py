import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, sum as _sum, count as _count, round as _round
)

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def main(in_dir: str, out_dir: str):
    # -----------------------------
    # Spark Session (8GB-safe)
    # -----------------------------
    spark = (
        SparkSession.builder
        .appName("Ecommerce Multi-Model Analytics (Local)")
        .master("local[2]")  # ✅ safe on 8GB RAM
        .config("spark.sql.shuffle.partitions", "8")  # ✅ reduce memory pressure
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------
    # Input files
    # -----------------------------
    transactions_path = os.path.join(in_dir, "transactions.json")
    products_path = os.path.join(in_dir, "products.json")

    if not os.path.exists(transactions_path):
        raise FileNotFoundError(f"Missing file: {transactions_path}")
    if not os.path.exists(products_path):
        raise FileNotFoundError(f"Missing file: {products_path}")

    ensure_dir(out_dir)

    # -----------------------------
    # Load JSON (Spark reads efficiently)
    # -----------------------------
    print("Loading products.json ...")
    products_df = spark.read.option("multiline", "true").json(products_path)
    products_df = products_df.select("product_id", "category_id", "base_price")

    print("Loading transactions.json ...")
    tx_df = spark.read.option("multiline", "true").json(transactions_path)

    # tx: explode items
    tx_items = (
        tx_df
        .select(
            col("transaction_id"),
            col("user_id"),
            explode(col("items")).alias("item")
        )
        .select(
            col("transaction_id"),
            col("user_id"),
            col("item.product_id").alias("product_id"),
            col("item.quantity").cast("int").alias("quantity"),
            col("item.subtotal").cast("double").alias("subtotal")
        )
    )

    # -----------------------------
    # 1) Revenue by Category
    # -----------------------------
    print("Computing revenue_by_category ...")

    revenue_by_category = (
        tx_items
        .join(products_df, on="product_id", how="left")
        .groupBy("category_id")
        .agg(
            _round(_sum("subtotal"), 2).alias("revenue"),
            _sum("quantity").alias("units_sold"),
            _count("transaction_id").alias("orders")
        )
        .orderBy(col("revenue").desc())
    )

    out_rev = os.path.join(out_dir, "revenue_by_category")
    revenue_by_category.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_rev)

    # -----------------------------
    # 2) Top Spenders
    # -----------------------------
    print("Computing top_spenders ...")

    top_spenders = (
        tx_df
        .select(
            col("user_id"),
            col("total").cast("double").alias("total"),
            col("transaction_id")
        )
        .groupBy("user_id")
        .agg(
            _round(_sum("total"), 2).alias("total_spent"),
            _count("transaction_id").alias("num_orders")
        )
        .orderBy(col("total_spent").desc())
    )

    out_spenders = os.path.join(out_dir, "top_spenders")
    top_spenders.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_spenders)

    # -----------------------------
    # 3) Also Bought (Top 50 Pairs)
    # -----------------------------
    print("Computing also_bought_top50 ...")

    # items per transaction
    tx_products = (
        tx_items
        .select("transaction_id", "product_id")
        .dropna()
        .dropDuplicates()
    )

    # self-join to create product pairs per transaction
    a = tx_products.alias("a")
    b = tx_products.alias("b")

    pairs = (
        a.join(b, on="transaction_id")
        .where(col("a.product_id") < col("b.product_id"))  # ✅ avoid duplicates + self-pairs
        .select(
            col("a.product_id").alias("product_x"),
            col("b.product_id").alias("product_y")
        )
    )

    also_bought = (
        pairs
        .groupBy("product_x", "product_y")
        .agg(_count("*").alias("co_purchase_count"))
        .orderBy(col("co_purchase_count").desc())
        .limit(50)
    )

    out_pairs = os.path.join(out_dir, "also_bought_top50")
    also_bought.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_pairs)

    print("DONE Spark outputs saved to:", out_dir)

    spark.stop()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True, help="Input folder (raw_data) containing JSON files")
    ap.add_argument("--out-dir", required=True, help="Output folder (spark_out)")
    args = ap.parse_args()

    main(args.in_dir, args.out_dir)
