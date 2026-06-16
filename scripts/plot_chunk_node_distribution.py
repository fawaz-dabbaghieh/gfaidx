#!/usr/bin/env python3
"""Plot the number of nodes per chunk from a .ndx node-hash index file.

Usage:
    python3 scripts/plot_chunk_node_distribution.py graph.gfa.gz.ndx

The script reads the binary .ndx file, counts how many nodes map to each
community id, and writes a bar plot with one bar per chunk.
"""

from __future__ import annotations

import argparse
import mmap
import statistics
import struct
from collections import Counter
from pathlib import Path
from typing import Iterable, List, Tuple

import matplotlib

# Force a non-interactive backend so the script works in headless shells.
matplotlib.use("Agg")

import matplotlib.pyplot as plt


# The on-disk .ndx entry matches NodeHashEntry in src/indexer/node_hash_index.h.
NODE_HASH_ENTRY_STRUCT = struct.Struct("<QII")


def default_output_path(ndx_path: Path) -> Path:
    """Build a default PNG path beside the input .ndx file."""
    if ndx_path.suffix == ".ndx":
        return ndx_path.with_suffix("").with_name(ndx_path.stem + ".chunk_node_distribution.png")
    return ndx_path.with_name(ndx_path.name + ".chunk_node_distribution.png")


def load_chunk_sizes(ndx_path: Path) -> Counter[int]:
    """Count how many nodes belong to each community id in the .ndx file."""
    chunk_sizes: Counter[int] = Counter()
    file_size = ndx_path.stat().st_size

    if file_size == 0:
        raise RuntimeError("The .ndx file is empty")

    if file_size % NODE_HASH_ENTRY_STRUCT.size != 0:
        raise RuntimeError(
            "The .ndx file size is not a multiple of the NodeHashEntry size "
            f"({file_size} bytes vs {NODE_HASH_ENTRY_STRUCT.size}-byte entries); "
            "the file may be truncated or from an incompatible format"
        )

    with ndx_path.open("rb") as handle:
        # Iterate over the file through an mmap so regular-file short reads cannot
        # masquerade as a truncated trailing entry.
        with mmap.mmap(handle.fileno(), length=0, access=mmap.ACCESS_READ) as mapped:
            for offset in range(0, file_size, NODE_HASH_ENTRY_STRUCT.size):
                # The first two fields are hashes; only the community id is needed here.
                _, _, community_id = NODE_HASH_ENTRY_STRUCT.unpack_from(mapped, offset)
                chunk_sizes[community_id] += 1

    if not chunk_sizes:
        raise RuntimeError("The .ndx file does not contain any node entries")

    return chunk_sizes


def sort_chunk_sizes(chunk_sizes: Counter[int], sort_mode: str) -> List[Tuple[int, int]]:
    """Return the chunk sizes in the requested plotting order."""
    items = list(chunk_sizes.items())

    if sort_mode == "community":
        # Plotting by community id preserves the original chunk numbering.
        items.sort(key=lambda item: item[0])
    else:
        # Plotting by descending size makes the heavy tail obvious at a glance.
        items.sort(key=lambda item: (-item[1], item[0]))

    return items


def limit_chunk_sizes(items: List[Tuple[int, int]], top: int) -> List[Tuple[int, int]]:
    """Optionally keep only the first N chunks after sorting."""
    if top <= 0 or top >= len(items):
        return items
    return items[:top]


def figure_size(num_bars: int) -> Tuple[float, float]:
    """Scale the figure width mildly with the number of plotted bars."""
    width = min(32.0, max(12.0, num_bars * 0.08))
    return width, 7.0


def print_summary(items: Iterable[Tuple[int, int]]) -> None:
    """Print a compact text summary alongside the saved plot."""
    sizes = [size for _, size in items]
    print(f"communities: {len(sizes)}")
    print(f"min_nodes: {min(sizes)}")
    print(f"median_nodes: {statistics.median(sizes):.1f}")
    print(f"mean_nodes: {statistics.mean(sizes):.2f}")
    print(f"max_nodes: {max(sizes)}")


def plot_chunk_sizes(items: List[Tuple[int, int]],
                     output_path: Path,
                     title: str,
                     sort_mode: str) -> None:
    """Render and save the bar plot."""
    community_ids = [community_id for community_id, _ in items]
    node_counts = [size for _, size in items]

    fig, ax = plt.subplots(figsize=figure_size(len(items)))

    # Use plotting rank on the x-axis when sorted by size so the ordering is stable and readable.
    x_values = list(range(len(items)))
    ax.bar(x_values, node_counts, width=0.9, color="#2d6a4f", edgecolor="#1b4332", linewidth=0.2)

    ax.set_title(title)
    ax.set_ylabel("Nodes per chunk")
    if sort_mode == "community":
        ax.set_xlabel("Community id")
    else:
        ax.set_xlabel("Chunk rank by size")

    if sort_mode == "community" and len(items) <= 50:
        # Show individual community ids only when there are few enough bars to remain legible.
        ax.set_xticks(x_values)
        ax.set_xticklabels([str(community_id) for community_id in community_ids], rotation=90)
    else:
        # Hide dense x tick labels so the bar heights remain the focus.
        ax.set_xticks([])

    # Mark the largest chunk so the worst-case size is visible immediately.
    max_index = max(range(len(node_counts)), key=node_counts.__getitem__)
    ax.scatter([max_index], [node_counts[max_index]], color="#d00000", s=18, zorder=3)
    ax.annotate(f"max={node_counts[max_index]}",
                xy=(max_index, node_counts[max_index]),
                xytext=(8, 8),
                textcoords="offset points",
                color="#d00000",
                fontsize=9)

    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot the number of nodes per chunk from a .ndx file")
    parser.add_argument("ndx", help="input .ndx file")
    parser.add_argument("--output", help="output PNG path (default: beside the input .ndx)")
    parser.add_argument("--sort", choices=["size", "community"], default="size",
                        help="plot chunks sorted by descending size or by community id")
    parser.add_argument("--top", type=int, default=0,
                        help="plot only the first N chunks after sorting; 0 means plot all")
    parser.add_argument("--title", default="Chunk Node Count Distribution",
                        help="plot title")
    args = parser.parse_args()

    ndx_path = Path(args.ndx)
    if not ndx_path.exists():
        raise FileNotFoundError(f"Input .ndx file does not exist: {ndx_path}")

    chunk_sizes = load_chunk_sizes(ndx_path)
    sorted_items = sort_chunk_sizes(chunk_sizes, args.sort)
    plotted_items = limit_chunk_sizes(sorted_items, args.top)

    if not plotted_items:
        raise RuntimeError("No chunks were selected for plotting")

    output_path = Path(args.output) if args.output else default_output_path(ndx_path)

    # Print a quick summary so the terminal still gives useful numbers without opening the PNG.
    print_summary(sorted_items)
    print(f"plotting_chunks: {len(plotted_items)}")
    print(f"output_png: {output_path}")

    plot_chunk_sizes(plotted_items, output_path, args.title, args.sort)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
