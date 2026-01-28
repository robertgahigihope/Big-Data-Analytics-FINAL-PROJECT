import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

SPARK_OUT = "spark_out"
PLOTS_OUT = "plots"


def first_csv(folder):
    files = glob.glob(os.path.join(folder, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV found in: {folder}")
    return files[0]


def shorten_label(x, max_len=18):
    """Shorten long labels to avoid messy x-axis."""
    x = str(x)
    return x if len(x) <= max_len else x[:max_len] + "..."


def add_bar_labels(ax, bars, fmt="{:,.0f}", fontsize=9):
    """Add values on top of each bar."""
    for bar in bars:
        h = bar.get_height()
        if pd.isna(h):
            continue
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            h,
            fmt.format(h),
            ha="center",
            va="bottom",
            fontsize=fontsize,
            rotation=0
        )


def save_bar(df, x, y, title, out_png, label_format="{:,.0f}", rotate=45, max_label_len=18):
    df = df.copy()

    df[y] = pd.to_numeric(df[y], errors="coerce")
    df = df.dropna(subset=[y])

    # sort descending (largest first)
    df = df.sort_values(y, ascending=False)

    # shorten x labels if needed
    df[x] = df[x].apply(lambda v: shorten_label(v, max_len=max_label_len))

    plt.figure(figsize=(11, 6))
    ax = plt.gca()

    bars = ax.bar(df[x].astype(str), df[y])

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel(x, fontsize=11)
    ax.set_ylabel(y, fontsize=11)

    # grid for professionalism
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    # rotate x labels
    plt.xticks(rotation=rotate, ha="right")

    # add values on bars
    add_bar_labels(ax, bars, fmt=label_format, fontsize=9)

    plt.tight_layout()
    plt.savefig(out_png, dpi=180)
    plt.close()


def main():
    os.makedirs(PLOTS_OUT, exist_ok=True)

    # 1) revenue_by_category (Top 10)
    rev_dir = os.path.join(SPARK_OUT, "revenue_by_category")
    rev_csv = first_csv(rev_dir)
    rev = pd.read_csv(rev_csv).head(10)

    save_bar(
        rev,
        x="category_id",
        y="revenue",
        title="Top 10 Categories by Revenue",
        out_png=os.path.join(PLOTS_OUT, "01_revenue_by_category_top10.png"),
        label_format="{:,.0f}",
        rotate=45,
        max_label_len=20
    )

    # 2) top_spenders (Top 10)
    spend_dir = os.path.join(SPARK_OUT, "top_spenders")
    spend_csv = first_csv(spend_dir)
    spend = pd.read_csv(spend_csv).head(10)

    save_bar(
        spend,
        x="user_id",
        y="total_spent",
        title="Top 10 Users by Total Spent",
        out_png=os.path.join(PLOTS_OUT, "02_top_spenders_top10.png"),
        label_format="{:,.0f}",
        rotate=45,
        max_label_len=18
    )

    # 3) also_bought_top50 (Top 10 pairs)
    ab_dir = os.path.join(SPARK_OUT, "also_bought_top50")
    ab_csv = first_csv(ab_dir)
    ab = pd.read_csv(ab_csv).head(10)
    ab["pair"] = ab["product_x"].astype(str) + " + " + ab["product_y"].astype(str)

    save_bar(
        ab,
        x="pair",
        y="co_purchase_count",
        title="Top 10 Products Bought Together (Pairs)",
        out_png=os.path.join(PLOTS_OUT, "03_also_bought_pairs_top10.png"),
        label_format="{:,.0f}",
        rotate=60,          # pairs are longer -> rotate more
        max_label_len=22
    )

    print("DONE ✅ Professional vertical bar charts saved to:", os.path.abspath(PLOTS_OUT))


if __name__ == "__main__":
    main()
