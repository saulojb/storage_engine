#!/usr/bin/env python3
"""
Compression codec comparison chart — colcompress on aarch64
Platform: ARM Neoverse-N1 / Graviton2 · PostgreSQL 18.1

Shows ZSTD vs LZ4 vs Deflate side-by-side for each benchmark query.

Usage:
    python3 chart_compression.py                       # serial (compression_serial.csv)
    python3 chart_compression.py compression_parallel.csv

Outputs:
    compression_comparison_serial.png / .svg   (or _parallel)
"""
import csv, pathlib, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

HERE = pathlib.Path(__file__).parent
CSV  = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "compression_serial.csv"

is_parallel = "parallel" in CSV.stem
mode_label  = "JIT on · 4 parallel workers" if is_parallel else "JIT off · no parallelism"
PNG = HERE / f"compression_comparison_{'parallel' if is_parallel else 'serial'}.png"
SVG = HERE / f"compression_comparison_{'parallel' if is_parallel else 'serial'}.svg"

CODEC_ORDER  = ["zstd", "lz4", "deflate"]
CODEC_LABELS = {"zstd": "ZSTD", "lz4": "LZ4", "deflate": "Deflate"}
CODEC_COLORS = {"zstd": "#2c5f8a", "lz4": "#e07b39", "deflate": "#2c7a4b"}
DISK_SIZE    = {"zstd": "66 MB", "lz4": "118 MB", "deflate": "83 MB"}

rows = {}
with open(CSV) as f:
    lines = (ln for ln in f if not ln.startswith("#"))
    for r in csv.DictReader(lines):
        rows.setdefault(r["compression"], {})[r["query"]] = float(r["median_ms"])

all_codecs = [c for c in CODEC_ORDER if c in rows]
all_qs     = list(next(iter(rows.values())).keys())
n_q        = len(all_qs)
n_c        = len(all_codecs)

fig, axes = plt.subplots(1, n_q, figsize=(max(14, n_q * 1.9), 5.5), sharey=False)
if n_q == 1:
    axes = [axes]
fig.patch.set_facecolor("#fafafa")

bar_w  = 0.22
offset = np.array([-bar_w, 0, bar_w])

for ax, q in zip(axes, all_qs):
    vals = [rows[c].get(q, 0) for c in all_codecs]
    max_v = max(vals)
    xs    = np.arange(1)

    for i, (codec, val) in enumerate(zip(all_codecs, vals)):
        bar = ax.bar(xs + offset[i], val,
                     width=bar_w - 0.02,
                     color=CODEC_COLORS[codec],
                     edgecolor="white", linewidth=0.8, zorder=3,
                     label=CODEC_LABELS[codec])
        label = f"{val/1000:.2f}s" if val >= 1000 else f"{val:.0f}ms"
        ax.text(xs[0] + offset[i],
                val + max_v * 0.02,
                label, ha="center", va="bottom", fontsize=7, fontweight="bold")

    # annotate LZ4 speedup vs ZSTD
    if "zstd" in rows and "lz4" in rows:
        z, l = rows["zstd"].get(q, 0), rows["lz4"].get(q, 0)
        if z > 0 and l > 0 and l < z:
            lx = all_codecs.index("lz4")
            ax.annotate(f"×{z/l:.2f}",
                        xy=(xs[0] + offset[lx], rows["lz4"][q]),
                        xytext=(0, 8), textcoords="offset points",
                        ha="center", fontsize=6.5, color="#e07b39", fontweight="bold")

    ax.set_title(q, fontsize=7.5, pad=4, fontweight="bold")
    ax.set_xticks([])
    ax.tick_params(axis="y", labelsize=7)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda v, _: f"{v/1000:.1f}s" if v >= 1000 else f"{v:.0f}ms"))
    ax.set_facecolor("#f5f5f5")
    ax.grid(axis="y", color="white", linewidth=0.8, zorder=0)
    ax.spines[:].set_visible(False)

disk_info = "  ·  ".join(
    f"{CODEC_LABELS[c]} {DISK_SIZE[c]}" for c in all_codecs if c in DISK_SIZE
)
legend_labels = [
    f"{CODEC_LABELS[c]} ({DISK_SIZE[c]})" for c in all_codecs
]
fig.legend(
    handles=[plt.Rectangle((0,0),1,1, color=CODEC_COLORS[c]) for c in all_codecs],
    labels=legend_labels,
    loc="upper center", ncol=n_c, fontsize=9, frameon=False, bbox_to_anchor=(0.5, 1.02))
fig.suptitle(
    "colcompress Compression Codec Comparison — aarch64 (Neoverse-N1 / Graviton2)\n"
    f"1M rows · Median of 3 runs · {mode_label}",
    fontsize=9.5, y=1.08)
fig.text(0.5, -0.03,
         "Lower is better  ·  Orange annotation = LZ4 speedup vs ZSTD",
         ha="center", fontsize=7.5, color="#555")

plt.tight_layout(rect=[0, 0, 1, 1])
fig.savefig(PNG, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
fig.savefig(SVG, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"Chart saved → {PNG}")
print(f"Chart saved → {SVG}")
