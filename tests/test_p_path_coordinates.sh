#!/usr/bin/env bash
set -euo pipefail

gfaidx=$1
input_gfa=$2
coordinate_paths=$3
bad_span_gfa=$4
bad_span_paths=$5
overlapping_gfa=$6
overlapping_paths=$7
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-p-coordinates.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

indexed_gfa="$work_dir/graph.gfa.gz"
"$gfaidx" index_gfa \
    "$input_gfa" \
    "$indexed_gfa" \
    --progress_every 0 >/dev/null

# Before a CDX exists, the PDX/LNX fallback must find a suffixed P fragment by
# its suffix-free coordinate namespace and preserve the encoded coordinate base.
"$gfaidx" get_region \
    "$indexed_gfa" \
    ref#0#chr1:101-105 \
    "$work_dir/fallback.gfa" \
    --cdx "$work_dir/missing.cdx" \
    --all_haplotypes \
    --with_coords >/dev/null
grep -F $'P\tref#0#chr1:100-106\tA+,B+,C+\t*' \
    "$work_dir/fallback.gfa" >/dev/null
if grep -F 'ref#0#chr1:100-106:' "$work_dir/fallback.gfa" >/dev/null; then
    echo "Coordinate-bearing P output contains a duplicated interval suffix" >&2
    exit 1
fi

# Index both adjacent P fragments. Their raw suffixed names select exact .pdx
# records, while CDX queries and listings expose one shared base namespace.
"$gfaidx" index_coordinates \
    "$input_gfa" \
    "$indexed_gfa.cdx" \
    --ndx "$indexed_gfa.ndx" \
    --pdx "$indexed_gfa.pdx" \
    --path_names_file "$coordinate_paths" \
    --progress_every 0 >/dev/null
"$gfaidx" get_region "$indexed_gfa" --list_coordinates \
    >"$work_dir/coordinates.tsv"
grep -F $'P\t\t0\tref#0#chr1\t100\t106\t3\tcdx' \
    "$work_dir/coordinates.tsv" >/dev/null
grep -F $'P\t\t0\tref#0#chr1\t106\t112\t3\tcdx' \
    "$work_dir/coordinates.tsv" >/dev/null
grep -F $'P\t\t0\tlocal\t0\t*\t6\ton_the_fly' \
    "$work_dir/coordinates.tsv" >/dev/null

# A query crossing the fragment boundary must retain one exact interval from
# each original P record, with coordinates clipped to segment boundaries.
"$gfaidx" get_region \
    "$indexed_gfa" \
    ref#0#chr1:105-107 \
    "$work_dir/from_cdx.gfa" \
    --all_haplotypes \
    --with_coords >/dev/null
grep -F $'P\tref#0#chr1:104-106\tC+\t*' \
    "$work_dir/from_cdx.gfa" >/dev/null
grep -F $'P\tref#0#chr1:106-108\tD+\t*' \
    "$work_dir/from_cdx.gfa" >/dev/null

# Unsuffixed paths keep their established path-local coordinate system.
"$gfaidx" get_region \
    "$indexed_gfa" \
    local:1-3 \
    "$work_dir/local.gfa" \
    --cdx "$work_dir/missing.cdx" \
    --all_haplotypes \
    --with_coords >/dev/null
grep -F $'P\tlocal:0-4\tA+,B+\t*' "$work_dir/local.gfa" >/dev/null

# A terminal interval is authoritative. Coordinate indexing must reject a path
# whose segment-length sum does not equal the encoded half-open span.
bad_indexed_gfa="$work_dir/bad.gfa.gz"
"$gfaidx" index_gfa \
    "$bad_span_gfa" \
    "$bad_indexed_gfa" \
    --progress_every 0 >/dev/null
if "$gfaidx" index_coordinates \
    "$bad_span_gfa" \
    "$bad_indexed_gfa.cdx" \
    --ndx "$bad_indexed_gfa.ndx" \
    --pdx "$bad_indexed_gfa.pdx" \
    --path_names_file "$bad_span_paths" \
    --progress_every 0 >"$work_dir/bad.stdout" 2>"$work_dir/bad.stderr"; then
    echo "Coordinate indexing unexpectedly accepted an inconsistent P span" >&2
    exit 1
fi
grep -F "inconsistent with segment lengths" "$work_dir/bad.stderr" >/dev/null

# Separate records may share a suffix-free namespace only when their intervals
# do not overlap. Reject ambiguity in both accelerated and on-demand indexing.
overlapping_indexed_gfa="$work_dir/overlapping.gfa.gz"
"$gfaidx" index_gfa \
    "$overlapping_gfa" \
    "$overlapping_indexed_gfa" \
    --progress_every 0 >/dev/null
if "$gfaidx" index_coordinates \
    "$overlapping_gfa" \
    "$overlapping_indexed_gfa.cdx" \
    --ndx "$overlapping_indexed_gfa.ndx" \
    --pdx "$overlapping_indexed_gfa.pdx" \
    --path_names_file "$overlapping_paths" \
    --progress_every 0 \
    >"$work_dir/overlap.stdout" 2>"$work_dir/overlap.stderr"; then
    echo "Coordinate indexing unexpectedly accepted overlapping P fragments" >&2
    exit 1
fi
grep -F "Overlapping reference-coordinate track fragments" \
    "$work_dir/overlap.stderr" >/dev/null

if "$gfaidx" get_region \
    "$overlapping_indexed_gfa" \
    ref#0#chr1:105-106 \
    "$work_dir/overlap.gfa" \
    --cdx "$work_dir/missing.cdx" \
    --all_haplotypes \
    >"$work_dir/overlap-query.stdout" \
    2>"$work_dir/overlap-query.stderr"; then
    echo "On-demand query unexpectedly accepted overlapping P fragments" >&2
    exit 1
fi
grep -F "Overlapping P/W coordinate fragments" \
    "$work_dir/overlap-query.stderr" >/dev/null
