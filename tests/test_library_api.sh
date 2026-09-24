#!/usr/bin/env bash
set -euo pipefail

gfaidx_cli=$1
api_test=$2
input_gfa=$3

workdir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-library-api.XXXXXX")
trap 'rm -rf -- "$workdir"' EXIT

"$gfaidx_cli" index_gfa "$input_gfa" "$workdir/graph.gfa.gz" \
    --tmp_dir "$workdir" --max_chunk_nodes 0 --min_chunk_nodes 0
"$api_test" "$workdir/graph.gfa.gz"

