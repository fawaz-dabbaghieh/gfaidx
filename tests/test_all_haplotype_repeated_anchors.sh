#!/usr/bin/env bash
set -euo pipefail

gfaidx=$1
input_gfa=$2
coordinate_paths=$3
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-repeat-test.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

indexed_gfa="$work_dir/graph.gfa.gz"
"$gfaidx" index_gfa "$input_gfa" "$indexed_gfa" --progress_every 0 >/dev/null
"$gfaidx" index_coordinates \
    "$input_gfa" \
    "$indexed_gfa.cdx" \
    --ndx "$indexed_gfa.ndx" \
    --pdx "$indexed_gfa.pdx" \
    --path_names_file "$coordinate_paths" \
    --progress_every 0 >/dev/null

check_output() {
    local output_gfa=$1
    local actual_paths="$work_dir/actual_paths.tsv"
    local actual_nodes="$work_dir/actual_nodes.txt"

    awk -F '\t' '$1 == "P" {print $2 "\t" $3}' "$output_gfa" >"$actual_paths"
    cat >"$work_dir/expected_paths.tsv" <<'EOF'
ref:1-4	B+,C+,D+
insertion:1-5	B+,X+,C+,D+
reverse:3-7	D+,Y+,C+,B+
repeatnoise:4-8	B+,X+,C+,D+
EOF
    diff -u "$work_dir/expected_paths.tsv" "$actual_paths"

    awk -F '\t' '$1 == "S" {print $2}' "$output_gfa" | sort >"$actual_nodes"
    cat >"$work_dir/expected_nodes.txt" <<'EOF'
B
C
D
X
Y
EOF
    diff -u "$work_dir/expected_nodes.txt" "$actual_nodes"
}

# The CDX query must preserve the exact reference occurrence while chaining
# forward and reverse anchors only on paths whose anchor copies are ambiguous.
"$gfaidx" get_region \
    "$indexed_gfa" \
    ref:1-4 \
    "$work_dir/from_cdx.gfa" \
    --all_haplotypes \
    --with_coords >/dev/null
check_output "$work_dir/from_cdx.gfa"

# The slower PDX/LNX fallback computes the same exact source run and must have
# identical repeated-anchor behavior when no coordinate sidecar is available.
"$gfaidx" get_region \
    "$indexed_gfa" \
    ref:1-4 \
    "$work_dir/from_fallback.gfa" \
    --cdx "$work_dir/not_present.cdx" \
    --all_haplotypes \
    --with_coords >/dev/null
check_output "$work_dir/from_fallback.gfa"
