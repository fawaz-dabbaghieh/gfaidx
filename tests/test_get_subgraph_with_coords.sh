#!/usr/bin/env bash
set -euo pipefail

gfaidx=$1
input_gfa=$2
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-subgraph-coords-test.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

indexed_gfa="$work_dir/graph.gfa.gz"
"$gfaidx" index_gfa "$input_gfa" "$indexed_gfa" \
    --progress_every 0 >/dev/null

# Selecting all three nodes should emit a coordinate-named P path and a W walk
# in the original chr1 coordinate namespace. Companion .lnx/.pcx paths are
# deliberately omitted here to exercise automatic sidecar discovery.
"$gfaidx" get_subgraph \
    "$indexed_gfa" \
    1 \
    "$work_dir/with_coords.gfa" \
    --max_nodes 3 \
    --with_coords >/dev/null

awk -F '\t' '$1 == "P" {print $2 "\t" $3}' \
    "$work_dir/with_coords.gfa" >"$work_dir/actual_p.tsv"
cat >"$work_dir/expected_p.tsv" <<'EOF'
reference_path:0-6	1+,2+,3+
EOF
diff -u "$work_dir/expected_p.tsv" "$work_dir/actual_p.tsv"

awk -F '\t' '$1 == "W" {print $2 "\t" $3 "\t" $4 "\t" $5 "\t" $6 "\t" $7}' \
    "$work_dir/with_coords.gfa" >"$work_dir/actual_w.tsv"
cat >"$work_dir/expected_w.tsv" <<'EOF'
reference	0	chr1	100	106	>1>2>3
EOF
diff -u "$work_dir/expected_w.tsv" "$work_dir/actual_w.tsv"

# Coordinates describe path output, so accepting them together with --no_paths
# would silently discard the requested result.
if "$gfaidx" get_subgraph \
    "$indexed_gfa" \
    1 \
    "$work_dir/invalid.gfa" \
    --with_coords \
    --no_paths >"$work_dir/invalid.stdout" 2>"$work_dir/invalid.stderr"; then
    echo "get_subgraph unexpectedly accepted --with_coords with --no_paths" >&2
    exit 1
fi
grep -F -- "--with_coords requires path output" "$work_dir/invalid.stderr" >/dev/null
