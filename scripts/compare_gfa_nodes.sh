#!/usr/bin/env bash
set -euo pipefail

# Compare the unique S-line node IDs in two plain-text GFA files.
# Usage: scripts/compare_gfa_nodes.sh graph1.gfa graph2.gfa

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <graph1.gfa> <graph2.gfa>" >&2
    exit 1
fi

graph1=$1
graph2=$2

for graph in "$graph1" "$graph2"; do
    if [[ ! -f "$graph" || ! -r "$graph" ]]; then
        echo "Input GFA is not a readable file: $graph" >&2
        exit 1
    fi
done

# sort may spill to this temporary directory, so the complete node sets do not
# need to fit in memory. LC_ALL=C ensures sort and comm use the same ordering.
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-compare-nodes.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

extract_sorted_node_ids() {
    local graph=$1
    local output=$2

    awk -F $'\t' '
        $1 == "S" {
            if (NF < 2 || $2 == "") {
                print "Malformed S line at input line " NR > "/dev/stderr"
                exit 1
            }
            print $2
        }
    ' "$graph" |
        LC_ALL=C TMPDIR="$work_dir" sort -u >"$output"
}

ids1="$work_dir/graph1.ids"
ids2="$work_dir/graph2.ids"
extract_sorted_node_ids "$graph1" "$ids1"
extract_sorted_node_ids "$graph2" "$ids2"

# The sorted files contain one ID per node. comm -12 streams only IDs shared by
# both files, and awk counts them without retaining the intersection.
graph1_nodes=$(awk 'END {print NR}' "$ids1")
graph2_nodes=$(awk 'END {print NR}' "$ids2")
intersection_nodes=$(
    LC_ALL=C comm -12 "$ids1" "$ids2" |
        awk 'END {print NR}'
)

printf 'graph1_nodes\t%s\n' "$graph1_nodes"
printf 'graph2_nodes\t%s\n' "$graph2_nodes"
printf 'intersection_nodes\t%s\n' "$intersection_nodes"
