#!/usr/bin/env python3
"""
Generate benchmark chart — Serial run (JIT off, no parallelism)

Usage:
    python3 chart.py                          # uses results_serial.csv
    python3 chart.py path/to/results.csv      # custom CSV

Outputs:
    benchmark.png
    benchmark.svg
"""
import csv, pathlib, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

HERE = pathlib.Path(__file__).parent
CSV  = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "results_serial.csv"
PNG  = HERE / "benchmark_serial.png"
SVG  = HERE / "benchmark_serial.svg"

rows = {}
with open(CSV) as f:
    for r in csv.DictReader(f):
        rows.setdefault(r["AM"], {})[r["query"]] = float(r["median_ms"])

AM_ORDER  = ["heap", "colcompress", "rowcompress", "citus_columnar"]
AM_LABELS = {
    "heap":           "PostgreSQL heap",
    "colcompress":    "colcompress\n(storage_engine)",
    "rowcompress":    "rowcompress\n(storage_engine)",
    "citus_columnar": "columnar\n(citus_columnar)",
}
AM_COLORS = {
    "heap":           "#6c8ebf",
    "colcompress":    "#2c7a4b",
    "rowcompress":    "#82b366",
    "citus_columnar": "#e07b39",
}

all_ams = [a for a in AM_ORDER if a in rows]
all_qs  = list(next(iter(rows.values())).keys())
n_q, n_am = len(all_qs), len(all_ams)

fig, axes = plt.subplots(1, n_q, figsize=(max(14, n_q * 1.7), 5.5), sharey=False)
if n_q == 1:
    axes = [axes]
fig.patch.set_facecolor("#fafafa")

for ax, q in zip(axes, all_qs):
    vals = [rows[am].get(q, 0) for am in all_ams]
    cols = [AM_COLORS[am] for am in all_ams]
    xs   = np.arange(n_am)
    bars = ax.bar(xs, vals, color=cols, width=0.6, edgecolor="white", linewidth=0.8, zorder=3)
    ax.set_title(q, fontsize=7.5, pad=4, fontweight="bold")
    ax.set_xticks(xs)
    ax.set_xticklabels([AM_LABELS[am] for am in all_ams], fontsize=6.5)
    ax.tick_params(axis="y", labelsize=7)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda v, _: f"{v/1000:.1f}s" if v >= 1000 else f"{v:.0f}ms"))
    ax.set_facecolor("#f5f5f5")
    ax.grid(axis="y", color="white", linewidth=0.8, zorder=0)
    ax.spines[:].set_visible(False)
    for bar, val in zip(bars, vals):
        label = f"{val/1000:.2f}s" if val >= 1000 else f"{val:.0f}ms"
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(vals) * 0.02,
                label, ha="center", va="bottom", fontsize=6.5, fontweight="bold")
    if "heap" in all_ams and "colcompress" in all_ams:
        h, c = rows["heap"].get(q, 0), rows["colcompress"].get(q, 0)
        if h > 0 and c > 0 and c < h:
            xi = all_ams.index("colcompress")
            ax.annotate(f"×{h/c:.1f}",
                        xy=(xs[xi], vals[xi]), xytext=(0, 8),
                        textcoords="offset points",
                        ha="center", fontsize=6.5, color="#2c7a4b", fontweight="bold")

fig.legend(
    handles=[plt.Rectangle((0,0),1,1, color=AM_COLORS[am],
             label=AM_LABELS[am].replace("\n"," ")) for am in all_ams],
    loc="upper center", ncol=n_am, fontsize=8, frameon=False, bbox_to_anchor=(0.5, 1.02))
fig.suptitle(
    "PostgreSQL Table AM Benchmark — 1M rows (heap vs storage_engine vs citus_columnar)\n"
    "Median of 3 runs, no parallelism, JIT off",
    fontsize=9.5, y=1.08)
fig.text(0.5, -0.03,
         "Lower is better  ·  Green annotation = speedup of colcompress vs heap",
         ha="center", fontsize=7.5, color="#555")

plt.tight_layout(rect=[0, 0, 1, 1])
fig.savefig(PNG, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
fig.savefig(SVG, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"Chart saved → {PNG}")
print(f"Chart saved → {SVG}")
