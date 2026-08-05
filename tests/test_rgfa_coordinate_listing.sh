#!/usr/bin/env bash
set -euo pipefail

gfaidx=$1
input_gfa=$2
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-rgfa-coordinates.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

indexed_gfa="$work_dir/graph.gfa.gz"
"$gfaidx" index_gfa "$input_gfa" "$indexed_gfa" \
    --progress_every 0 >/dev/null

# An rGFA with no P/W records needs an actionable explanation when the
# standalone coordinate index has not been built yet.
if "$gfaidx" get_region \
    "$indexed_gfa" chrR:0-4 "$work_dir/region.gfa" \
    >"$work_dir/missing_cdx.stdout" \
    2>"$work_dir/missing_cdx.stderr"; then
    echo "get_region unexpectedly succeeded without a .cdx" >&2
    exit 1
fi
grep -F "No coordinate index was found" "$work_dir/missing_cdx.stderr"
grep -F "gfaidx index_coordinates" "$work_dir/missing_cdx.stderr"

"$gfaidx" index_coordinates \
    "$indexed_gfa" \
    "$indexed_gfa.cdx" \
    --progress_every 0 >/dev/null

# The clearer alias must retain the exact output of the original flag.
"$gfaidx" get_region "$indexed_gfa" --list_coordinates \
    >"$work_dir/list_coordinates.tsv"
"$gfaidx" get_region "$indexed_gfa" --print_path_names \
    >"$work_dir/print_path_names.tsv"
diff -u "$work_dir/print_path_names.tsv" "$work_dir/list_coordinates.tsv"

awk -F '\t' \
    'NR > 1 && $1 == "S" && $4 == "chrR" && $5 == 0 && $6 == 8 &&
     $7 == 2 && $8 == "cdx" {found = 1} END {exit !found}' \
    "$work_dir/list_coordinates.tsv"

# Listing SR-derived coordinate tracks must also work for an index that was
# intentionally built without any path sidecars.
cdx_only_gfa="$work_dir/graph.no_paths.gfa.gz"
"$gfaidx" index_gfa "$input_gfa" "$cdx_only_gfa" \
    --no_paths \
    --progress_every 0 >/dev/null
"$gfaidx" index_coordinates \
    "$cdx_only_gfa" \
    "$cdx_only_gfa.cdx" \
    --progress_every 0 >/dev/null
"$gfaidx" get_region "$cdx_only_gfa" --list_coordinates \
    >"$work_dir/cdx_only.tsv"
diff -u "$work_dir/list_coordinates.tsv" "$work_dir/cdx_only.tsv"
