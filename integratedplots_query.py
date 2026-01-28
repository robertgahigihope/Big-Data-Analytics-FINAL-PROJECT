# integratedplots_query.py
# Visualization for Integrated Analytical Query
# Engagement vs Spending

import os
import pandas as pd
import matplotlib.pyplot as plt

DATA_PATH = "integrated_out/integrated_metrics.csv"
OUT_DIR = "integrated_plots"

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # Load integrated metrics
    df = pd.read_csv(DATA_PATH)

    # Keep only users who actually spent money
    df = df[df["total_spent"] > 0]

    # Scatter plot: Engagement vs Spending
    plt.figure(figsize=(9, 6))
    plt.scatter(
        df["sessions_count"],
        df["total_spent"],
        alpha=0.5
    )

    plt.xlabel("Number of Sessions (Engagement)")
    plt.ylabel("Total Spent")
    plt.title("User Engagement vs Spending")

    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()

    out_png = os.path.join(OUT_DIR, "engagement_vs_spend.png")
    plt.savefig(out_png, dpi=160)
    plt.close()

    print("DONE ✅ Integrated plot saved to:", out_png)

if __name__ == "__main__":
    main()
