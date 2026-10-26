#!/usr/bin/env bash
set -euo pipefail

gfaidx=$1
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-index-input-errors.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

# Indexing must fail cleanly: a non-zero exit, the expected message, and no
# final outputs or temporary directories left beside the input.
expect_index_failure() {
    local name=$1 expected=$2
    local input="$work_dir/$name.gfa"
    if "$gfaidx" index_gfa "$input" "$work_dir/$name.gfa.gz" \
        --progress_every 0 >"$work_dir/$name.stdout" 2>"$work_dir/$name.stderr"; then
        echo "index_gfa unexpectedly succeeded on $name" >&2
        exit 1
    fi
    grep -F "$expected" "$work_dir/$name.stderr"
    if compgen -G "$work_dir/$name.gfa.gz*" >/dev/null ||
       compgen -G "$work_dir/gfaidx_tmp_*" >/dev/null; then
        echo "index_gfa left outputs behind for $name" >&2
        ls -la "$work_dir" >&2
        exit 1
    fi
}

# A graph without L lines used to crash inside Louvain community detection.
printf 'H\tVN:Z:1.0\nS\t1\tACGTA\nS\t2\tCCC\n' >"$work_dir/no_links.gfa"
expect_index_failure no_links "has no L lines (edges)"

# GFA 2.0 announced by its header tag.
printf 'H\tVN:Z:2.0\nS\t1\t5\tACGTA\nS\t2\t3\tCCC\nE\te1\t1+\t2+\t5$\t5$\t0\t0\t0M\n' \
    >"$work_dir/gfa2_header.gfa"
expect_index_failure gfa2_header "a VN:Z:2.0 header tag on line 1"

# GFA 2.0 without a header, recognized by its first E record.
printf 'S\t1\t5\tACGTA\nS\t2\t3\tCCC\nE\te1\t1+\t2+\t5$\t5$\t0\t0\t0M\n' \
    >"$work_dir/gfa2_edge.gfa"
expect_index_failure gfa2_edge "a GFA 2.0 E record on line 3"

# A single link is enough for community detection, so the smallest valid
# GFA 1 graph must still index.
printf 'H\tVN:Z:1.0\nS\t1\tACGTA\nS\t2\tCCC\nL\t1\t+\t2\t+\t0M\n' >"$work_dir/one_link.gfa"
"$gfaidx" index_gfa "$work_dir/one_link.gfa" "$work_dir/one_link.gfa.gz" \
    --progress_every 0 >/dev/null
test -s "$work_dir/one_link.gfa.gz"
test -s "$work_dir/one_link.gfa.gz.ndx"
