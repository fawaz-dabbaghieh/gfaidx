#!/usr/bin/env python3
"""Plot the distribution of nodes per chunk from a .ndx node-hash index file.

Usage:
    python3 scripts/plot_chunk_node_distribution.py graph.gfa.gz.ndx

The script reads the binary .ndx file, counts how many nodes map to each
community id, and writes a binned bar plot where:
  - x-axis: number of nodes in a community
  - y-axis: number of communities that fall in that size bin
"""

from __future__ import annotations

import argparse
import mmap
import math
import statistics
import struct
from collections import Counter
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

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


def figure_size(num_bins: int) -> Tuple[float, float]:
    """Scale the figure width mildly with the number of plotted bins."""
    width = min(20.0, max(10.0, num_bins * 0.35))
    return width, 7.0


def print_summary(items: Iterable[Tuple[int, int]]) -> None:
    """Print a compact text summary alongside the saved plot."""
    sizes = [size for _, size in items]
    print(f"communities: {len(sizes)}")
    print(f"min_nodes: {min(sizes)}")
    print(f"median_nodes: {statistics.median(sizes):.1f}")
    print(f"mean_nodes: {statistics.mean(sizes):.2f}")
    print(f"max_nodes: {max(sizes)}")


def build_bin_edges(node_counts: Sequence[int],
                    num_bins: int,
                    bin_size: int | None) -> List[float]:
    """Build histogram bin edges from either a fixed width or a bin count."""
    min_nodes = min(node_counts)
    max_nodes = max(node_counts)

    if bin_size is not None:
        if bin_size <= 0:
            raise ValueError("--bin-size must be positive")
        start = (min_nodes // bin_size) * bin_size
        stop = ((max_nodes + bin_size - 1) // bin_size) * bin_size
        # Include the final right edge so the largest community lands inside the last bin.
        return [float(value) for value in range(start, stop + bin_size, bin_size)]

    if num_bins <= 0:
        raise ValueError("--bins must be positive")

    if min_nodes == max_nodes:
        # Expand the degenerate case into one visible bin centered on the shared size.
        return [float(min_nodes) - 0.5, float(max_nodes) + 0.5]

    width = (max_nodes - min_nodes) / num_bins
    # Use ceil so every value fits inside the constructed equal-width bins.
    step = max(1, math.ceil(width))
    start = min_nodes
    stop = max_nodes
    return [float(value) for value in range(start, stop + step, step)]


def plot_chunk_size_distribution(items: List[Tuple[int, int]],
                                 output_path: Path,
                                 title: str,
                                 num_bins: int,
                                 bin_size: int | None) -> Tuple[int, int]:
    """Render and save the binned chunk-size distribution plot."""
    node_counts = [size for _, size in items]
    bin_edges = build_bin_edges(node_counts, num_bins, bin_size)
    actual_bin_count = max(1, len(bin_edges) - 1)

    fig, ax = plt.subplots(figsize=figure_size(actual_bin_count))

    counts, _, _ = ax.hist(node_counts,
                           bins=bin_edges,
                           color="#2d6a4f",
                           edgecolor="#1b4332",
                           linewidth=0.6)

    ax.set_title(title)
    ax.set_xlabel("Nodes per community")
    ax.set_ylabel("Number of communities")

    # Highlight the bin that contains the largest communities so the tail is easy to spot.
    max_bin_index = max(range(len(counts)), key=counts.__getitem__)
    max_bin_left = bin_edges[max_bin_index]
    max_bin_right = bin_edges[max_bin_index + 1]
    ax.annotate(f"largest communities <= {max(node_counts)} nodes",
                xy=((max_bin_left + max_bin_right) / 2.0, counts[max_bin_index]),
                xytext=(8, 8),
                textcoords="offset points",
                color="#d00000",
                fontsize=9)

    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200)
    plt.close(fig)
    return actual_bin_count, int(max(counts))


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot the distribution of nodes per chunk from a .ndx file")
    parser.add_argument("ndx", help="input .ndx file")
    parser.add_argument("--output", help="output PNG path (default: beside the input .ndx)")
    parser.add_argument("--bins", type=int, default=40,
                        help="number of equal-width bins to use when --bin-size is not provided")
    parser.add_argument("--bin-size", type=int,
                        help="fixed bin width in nodes; overrides --bins")
    parser.add_argument("--title", default="Chunk Node Count Distribution",
                        help="plot title")
    args = parser.parse_args()

    ndx_path = Path(args.ndx)
    if not ndx_path.exists():
        raise FileNotFoundError(f"Input .ndx file does not exist: {ndx_path}")

    chunk_sizes = load_chunk_sizes(ndx_path)
    all_items = sorted(chunk_sizes.items(), key=lambda item: item[0])

    output_path = Path(args.output) if args.output else default_output_path(ndx_path)

    # Print a quick summary so the terminal still gives useful numbers without opening the PNG.
    print_summary(all_items)
    actual_bin_count, max_bin_height = plot_chunk_size_distribution(all_items,
                                                                    output_path,
                                                                    args.title,
                                                                    args.bins,
                                                                    args.bin_size)
    print(f"plotting_chunks: {len(all_items)}")
    print(f"bins: {actual_bin_count}")
    print(f"max_bin_height: {max_bin_height}")
    print(f"output_png: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
