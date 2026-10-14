#!/usr/bin/env bash
# Base-pair --context selection for get_region, which mirrors gbz-base's
# subgraph query: a search over node sides where crossing a node costs its
# length minus one and following a link costs one.
#
# Fixture (tests/data/context_paths.gfa), reference coordinates on chr1:
#   1[0,10) 2[10,11) 4[11,21) 10[21,31) 11[31,41)
# Node 3 is a SNP alternative to 2, and hapA carries a 100 bp insertion
# 5-6-7-8-9 (20 bp each) between 4 and 10. The query chr1:15-25 starts 4 bp
# into node 4 and ends 6 bp before the end of node 10, so node 4's left side
# starts at distance 4 and node 10's right side at distance 5 (gbz-base
# subtracts one on the exit side).
set -euo pipefail

gfaidx=$1
input_gfa=$2
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-context-test.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

indexed_gfa="$work_dir/graph.gfa.gz"
"$gfaidx" index_gfa "$input_gfa" "$indexed_gfa" --progress_every 0 >/dev/null
"$gfaidx" index_coordinates \
    "$input_gfa" \
    "$indexed_gfa.cdx" \
    --ndx "$indexed_gfa.ndx" \
    --pdx "$indexed_gfa.pdx" \
    --progress_every 0 >/dev/null

region=chr1:15-25

nodes() {
    awk -F '\t' '$1 == "S" {print $2}' "$1" | sort -n | paste -sd, -
}

walks() {
    awk -F '\t' '$1 == "W" {print $7}' "$1" | sort | paste -sd' ' -
}

walk_headers() {
    awk -F '\t' '$1 == "W" {print $2, $3, $4, $5, $6, $7}' "$1" | sort
}

expect() {
    local label=$1 expected=$2 actual=$3
    if [[ "$expected" != "$actual" ]]; then
        echo "$label: expected '$expected', got '$actual'" >&2
        exit 1
    fi
}

# Reference mode: the node set grows with the context, and the single
# reference walk extends through every reference step inside the subgraph.
check_reference() {
    local context=$1 expected_nodes=$2 expected_walk=$3
    local out="$work_dir/reference_$context.gfa"
    "$gfaidx" get_region "$indexed_gfa" "$region" "$out" \
        --reference ref --mode reference --context "$context" \
        --with_coords >/dev/null
    expect "reference --context $context nodes" "$expected_nodes" "$(nodes "$out")"
    expect "reference --context $context walk" "$expected_walk" "$(walk_headers "$out")"
}

# Context 0 keeps only the interval nodes. The 4 bp lead into node 4 keeps the
# SNP at distance 5, so context 3 reaches only the insertion's outer nodes,
# context 5 reaches the SNP, and context 6 crosses the 1 bp SNP nodes into
# node 1 and reaches node 11.
check_reference 0 "4,10" "ref 0 chr1 11 31 >4>10"
check_reference 3 "4,5,9,10" "ref 0 chr1 11 31 >4>10"
check_reference 5 "2,3,4,5,9,10" "ref 0 chr1 10 31 >2>4>10"
check_reference 6 "1,2,3,4,5,9,10,11" "ref 0 chr1 0 41 >1>2>4>10>11"
# Node 7 sits 41 bases away through either end of the insertion, so the
# middle of the long variant stays out until the context reaches it.
check_reference 40 "1,2,3,4,5,6,8,9,10,11" "ref 0 chr1 0 41 >1>2>4>10>11"
check_reference 41 "1,2,3,4,5,6,7,8,9,10,11" "ref 0 chr1 0 41 >1>2>4>10>11"
check_reference 41bp "1,2,3,4,5,6,7,8,9,10,11" "ref 0 chr1 0 41 >1>2>4>10>11"

# Omitting --context uses gbz-base's default of 100 bp.
"$gfaidx" get_region "$indexed_gfa" "$region" "$work_dir/reference_default.gfa" \
    --reference ref --mode reference >/dev/null
expect "reference default nodes" "1,2,3,4,5,6,7,8,9,10,11" \
    "$(nodes "$work_dir/reference_default.gfa")"

# The .pdx/.lnx fallback must report the same partial-node bounds as the .cdx.
"$gfaidx" get_region "$indexed_gfa" "$region" "$work_dir/reference_fallback.gfa" \
    --reference ref --mode reference --context 3 \
    --cdx "$work_dir/not_present.cdx" >/dev/null 2>&1
expect "reference fallback nodes" "4,5,9,10" "$(nodes "$work_dir/reference_fallback.gfa")"

# bfs with a context selects the same nodes and emits every haplotype visit
# inside them, including single-step fragments, as gbz-base does.
check_bfs() {
    local context=$1 expected_nodes=$2 expected_walks=$3
    local out="$work_dir/bfs_$context.gfa"
    "$gfaidx" get_region "$indexed_gfa" "$region" "$out" \
        --reference ref --mode bfs --context "$context" >/dev/null
    expect "bfs --context $context nodes" "$expected_nodes" "$(nodes "$out")"
    expect "bfs --context $context walks" "$expected_walks" "$(walks "$out")"
}

check_bfs 0 "4,10" ">10 >4 >4>10 >4>10"
check_bfs 25 "1,2,3,4,5,6,8,9,10,11" \
    ">1>2>4>10>11 >1>2>4>10>11 >1>3>4>5>6 >8>9>10>11"
check_bfs 41 "1,2,3,4,5,6,7,8,9,10,11" \
    ">1>2>4>10>11 >1>2>4>10>11 >1>3>4>5>6>7>8>9>10>11"

# Coordinates of the split insertion haplotype follow its own walk.
"$gfaidx" get_region "$indexed_gfa" "$region" "$work_dir/bfs_coords.gfa" \
    --reference ref --mode bfs --context 25 --with_coords >/dev/null
expect "bfs coordinates" \
    "$(printf 'hapA 1 chr1 0 61 >1>3>4>5>6\nhapA 1 chr1 81 141 >8>9>10>11\nhapB 1 chr1 0 41 >1>2>4>10>11\nref 0 chr1 0 41 >1>2>4>10>11')" \
    "$(walk_headers "$work_dir/bfs_coords.gfa")"

expect_failure() {
    local label=$1 message=$2
    shift 2
    if "$gfaidx" get_region "$indexed_gfa" "$region" "$work_dir/failure.gfa" \
        --reference ref "$@" >"$work_dir/failure.stdout" 2>"$work_dir/failure.stderr"; then
        echo "get_region unexpectedly accepted $label" >&2
        exit 1
    fi
    grep -F -- "$message" "$work_dir/failure.stderr" >/dev/null
}

expect_failure "--context under --mode all" \
    "--context requires --mode reference or --mode bfs" --mode all --context 10
expect_failure "--context with --max_nodes" \
    "--context and --max_nodes are mutually exclusive" \
    --mode bfs --context 10 --max_nodes 5
expect_failure "an invalid --context" "--context" --mode reference --context ten
