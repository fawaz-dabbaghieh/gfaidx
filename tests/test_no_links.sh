#!/usr/bin/env bash
# index_gfa on a GFA with S and P lines but no L lines: Louvain is skipped,
# every node goes into the singleton community, and the graph and its paths
# stay queryable.
set -euo pipefail

gfaidx=$1
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-no-links-test.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

input_gfa="$work_dir/no_links.gfa"
printf 'H\tVN:Z:1.0\nS\t1\tACGT\nS\t2\tGGCC\nS\t3\tTTAA\nP\tp1\t1+,2-\t*\nP\tp2\t3+,1-\t*\n' > "$input_gfa"

indexed_gfa="$work_dir/graph.gfa.gz"
"$gfaidx" index_gfa "$input_gfa" "$indexed_gfa" --progress_every 0 >/dev/null

for node in 1 2 3; do
    "$gfaidx" get_subgraph "$indexed_gfa" "$node" "$work_dir/sub.gfa" >/dev/null 2>&1
    expected=$(awk -F '\t' -v n="$node" '$1 == "S" && $2 == n' "$input_gfa")
    actual=$(awk -F '\t' '$1 == "S"' "$work_dir/sub.gfa")
    if [[ "$actual" != "$expected" ]]; then
        echo "get_subgraph $node returned '$actual', expected '$expected'" >&2
        exit 1
    fi
done

for path in p1 p2; do
    expected=$(awk -F '\t' -v p="$path" '$1 == "P" && $2 == p' "$input_gfa")
    actual=$("$gfaidx" get_path "$indexed_gfa" --path_id "$path" 2>/dev/null | grep '^P')
    if [[ "$actual" != "$expected" ]]; then
        echo "get_path $path returned '$actual', expected '$expected'" >&2
        exit 1
    fi
done

actual=$("$gfaidx" get_path "$indexed_gfa" --nodes 1,3 2>/dev/null | grep '^P')
if [[ "$actual" != $'P\tp2#subpath_0_1\t3+,1-\t*' ]]; then
    echo "get_path --nodes 1,3 returned '$actual'" >&2
    exit 1
fi

echo "no-links index ok"
