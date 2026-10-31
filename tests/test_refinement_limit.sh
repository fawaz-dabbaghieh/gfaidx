#!/usr/bin/env bash
# Refinement stops before a Louvain level that would create a piece above
# --max_chunk_nodes. Louvain levels only merge pieces, so the previous level
# always fits; only the first level is taken unconditionally.
set -euo pipefail

gfaidx=$1
repo=$2
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-refinement-limit-test.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

python3 "$repo/scripts/sim_pangenome.py" 3000 12 7 > "$work_dir/sim.gfa"
"$gfaidx" index_gfa "$work_dir/sim.gfa" "$work_dir/sim.gz" \
    --max_chunk_nodes 12 --min_chunk_nodes 2 --no_paths --progress_every 0 > "$work_dir/build.log" 2>&1

# On this graph, converged Louvain would leave refined pieces of up to 19 nodes.
if ! grep -q 'Keeping Louvain level' "$work_dir/build.log"; then
    echo "refinement never stopped at an earlier Louvain level" >&2
    exit 1
fi
refined=$(grep -c 'largest refined piece has' "$work_dir/build.log")
above=$(grep -o 'largest refined piece has [0-9]*' "$work_dir/build.log" | awk '$5 > 12' | wc -l)
if [[ "$refined" -eq 0 || "$above" -ne 0 ]]; then
    echo "expected refined pieces of at most 12 nodes; $above of $refined refinements are larger" >&2
    exit 1
fi

echo "refined pieces stay within --max_chunk_nodes"
