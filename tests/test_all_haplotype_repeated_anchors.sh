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

# A two-step stride deliberately puts extracted start/end positions both on and
# between checkpoints, providing compact boundary coverage for the fused path.
mv "$indexed_gfa.pcx" "$work_dir/default_checkpoint_index.pcx"
"$gfaidx" index_path_checkpoints \
    "$indexed_gfa" \
    "$indexed_gfa.pcx" \
    --checkpoint_steps 2 \
    --progress_every_paths 0 >/dev/null

# Name listing is driven by the complete .pdx path table. The optional .cdx
# only marks the selected reference path as accelerated.
"$gfaidx" get_region "$indexed_gfa" --print_path_names \
    >"$work_dir/path_names_with_cdx.tsv"
awk -F '\t' 'NR > 1 && $1 == "P" {print $4 "\t" $8}' \
    "$work_dir/path_names_with_cdx.tsv" \
    | sort >"$work_dir/path_access_with_cdx.tsv"
cat >"$work_dir/expected_path_access_with_cdx.tsv" <<'EOF'
insertion	on_the_fly
ref	cdx
repeatnoise	on_the_fly
reverse	on_the_fly
EOF
diff -u "$work_dir/expected_path_access_with_cdx.tsv" \
    "$work_dir/path_access_with_cdx.tsv"

# A missing .cdx is not an error: all coordinate-capable P paths remain
# available through the slower .pdx/.lnx fallback.
"$gfaidx" get_region "$indexed_gfa" --print_path_names \
    --cdx "$work_dir/not_present.cdx" \
    >"$work_dir/path_names_without_cdx.tsv"
awk -F '\t' 'NR > 1 && $1 == "P" {print $4 "\t" $8}' \
    "$work_dir/path_names_without_cdx.tsv" \
    | sort >"$work_dir/path_access_without_cdx.tsv"
cat >"$work_dir/expected_path_access_without_cdx.tsv" <<'EOF'
insertion	on_the_fly
ref	on_the_fly
repeatnoise	on_the_fly
reverse	on_the_fly
EOF
diff -u "$work_dir/expected_path_access_without_cdx.tsv" \
    "$work_dir/path_access_without_cdx.tsv"

check_output() {
    local output_gfa=$1
    local actual_paths="$work_dir/actual_paths.tsv"
    local actual_nodes="$work_dir/actual_nodes.txt"

    awk -F '\t' '$1 == "P" {print $2 "\t" $3}' "$output_gfa" >"$actual_paths"
    cat >"$work_dir/expected_paths.tsv" <<'EOF'
ref:1-4	B+,C+,D+
insertion:1-5	B+,X+,C+,D+
reverse:1-7	B+,R+,D+,Y+,C+,B+
repeatnoise:1-8	B+,Q+,R+,B+,X+,C+,D+
EOF
    diff -u "$work_dir/expected_paths.tsv" "$actual_paths"

    awk -F '\t' '$1 == "S" {print $2}' "$output_gfa" | sort >"$actual_nodes"
    cat >"$work_dir/expected_nodes.txt" <<'EOF'
B
C
D
Q
R
X
Y
EOF
    diff -u "$work_dir/expected_nodes.txt" "$actual_nodes"
}

# The CDX query preserves the exact reference occurrence, while every other
# path conservatively keeps all sequence between its outermost anchor hits.
"$gfaidx" get_region \
    "$indexed_gfa" \
    ref:1-4 \
    "$work_dir/from_cdx.gfa" \
    --all_haplotypes \
    --with_coords >/dev/null
check_output "$work_dir/from_cdx.gfa"

# Interval extraction uses the same ordered worker pool. Four workers must emit
# exactly the same S/L/P byte stream as the default one-worker path.
"$gfaidx" get_region \
    "$indexed_gfa" \
    ref:1-4 \
    "$work_dir/from_cdx_parallel.gfa" \
    --all_haplotypes \
    --with_coords \
    --threads 4 >/dev/null
cmp "$work_dir/from_cdx.gfa" "$work_dir/from_cdx_parallel.gfa"
check_output "$work_dir/from_cdx_parallel.gfa"

# The slower PDX/LNX fallback computes the same exact source run and must have
# identical min/max haplotype behavior when no coordinate sidecar is available.
# Remove .pcx for this final query so the legacy CoordinateSlice fallback is
# tested independently of the checkpoint-backed direct formatter above.
mv "$indexed_gfa.pcx" "$work_dir/checkpoint_stride_2.pcx"
"$gfaidx" get_region \
    "$indexed_gfa" \
    ref:1-4 \
    "$work_dir/from_fallback.gfa" \
    --cdx "$work_dir/not_present.cdx" \
    --all_haplotypes \
    --with_coords >/dev/null
check_output "$work_dir/from_fallback.gfa"
