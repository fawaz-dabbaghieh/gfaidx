#!/usr/bin/env bash
set -euo pipefail

converter=$1
resolver=$2
seed_parser=$3
collector=$4
input_gfa=$5
vg_paths=$6
odgi_paths=$7
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-benchmark-locus.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

# Produce the exact W-to-P mapping consumed by the benchmark resolver.
python3 "$converter" "$input_gfa" "$work_dir/converted.gfa" \
    --mapping-out "$work_dir/w_to_p.tsv"

# Coordinate 12 lies in W interval 10-16, so every VG/ODGI coordinate must be
# path-local position 2 while gfaidx and GBZ retain the absolute coordinates.
python3 "$resolver" \
    --mapping "$work_dir/w_to_p.tsv" \
    --vg-paths "$vg_paths" \
    --odgi-paths "$odgi_paths" \
    --graph demo --query-id chr1_12 \
    --sample GRCh38 --haplotype 0 --seq-id chr1 \
    --node-position 12 --region-start 11 --region-end 15 \
    --notes "test locus" \
    --out "$work_dir/locus.json"

python3 - "$work_dir/locus.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    locus = json.load(handle)

expected = {
    "vg_path_name": "GRCh38#0#chr1[10-16]",
    "odgi_path_name": "GRCh38#0#chr1:10-16",
    "node_local_position": 2,
    "vg_node_region": "GRCh38#0#chr1[10-16]:2",
    "odgi_node_position": "GRCh38#0#chr1:10-16,2",
    "gfaidx_region": "chr1:11-15",
    "vg_region": "GRCh38#0#chr1[10-16]:1-5",
    "odgi_region": "GRCh38#0#chr1:10-16:1-5",
    "gbz_interval": "11..15",
}
for key, value in expected.items():
    if locus.get(key) != value:
        raise SystemExit(f"{key}: expected {value!r}, observed {locus.get(key)!r}")
PY

# The original-node parser rejects ambiguity and writes a unique S-line ID.
printf 'H\tVN:Z:1.0\nS\t42\tAC\n' >"$work_dir/seed.gfa"
python3 "$seed_parser" --gfa "$work_dir/seed.gfa" --out "$work_dir/original.node_id"
test "$(cat "$work_dir/original.node_id")" = "42"

# Verify that the final audit table joins both ID spaces to the resolution.
mkdir -p "$work_dir/results/maps/loci/demo" \
    "$work_dir/results/maps/nodes/original/demo" \
    "$work_dir/results/maps/odgi/demo"
cp "$work_dir/locus.json" "$work_dir/results/maps/loci/demo/chr1_12.json"
printf '42\n' >"$work_dir/results/maps/nodes/original/demo/chr1_12.node_id"
printf '7\n' >"$work_dir/results/maps/odgi/demo/chr1_12.node_id"
python3 "$collector" --results-dir "$work_dir/results" --graphs demo \
    --out "$work_dir/resolved_loci.tsv"
awk -F '\t' 'NR == 2 && $10 == 42 && $11 == 7 { found = 1 } END { exit !found }' \
    "$work_dir/resolved_loci.tsv"

# A coordinate outside the W fragment must fail rather than choosing a nearby
# or similarly named path.
if python3 "$resolver" \
    --mapping "$work_dir/w_to_p.tsv" --vg-paths "$vg_paths" \
    --odgi-paths "$odgi_paths" --graph demo --query-id bad \
    --sample GRCh38 --haplotype 0 --seq-id chr1 --node-position 100 \
    --out "$work_dir/bad.json" 2>/dev/null; then
    echo "Out-of-range benchmark locus unexpectedly resolved" >&2
    exit 1
fi
