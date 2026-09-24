#!/usr/bin/env bash
set -euo pipefail

resolver=$1
selector=$2
path_names=$3
gbz_metadata=$4
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-benchmark-p-locus.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

# The selector consumes get_path-style rows and emits only exact requested P
# names in the format accepted by gfaidx index_coordinates.
{
    printf 'P\tCHM13#0#chr1\n'
    printf 'P\tHG00099#1#JBHDWO010000005.1\n'
    printf 'W\tignored\t0\tchr1\t0\t10\n'
} | python3 "$selector" --out "$work_dir/selected.tsv" \
    --requested CHM13#0#chr1 HG00099#1#JBHDWO010000005.1
printf 'P\tCHM13#0#chr1\nP\tHG00099#1#JBHDWO010000005.1\n' \
    > "$work_dir/expected.tsv"
diff -u "$work_dir/expected.tsv" "$work_dir/selected.tsv"

# Resolve an unsuffixed P path. VG receives an inclusive interval end, while
# the manifest and all other tools retain the half-open 11-15 interval.
python3 "$resolver" \
    --vg-paths "$path_names" --odgi-paths "$path_names" \
    --gbz-metadata "$gbz_metadata" \
    --graph demo --query-id chr1_12 \
    --sample CHM13 --haplotype 0 --seq-id chr1 \
    --node-position 12 --region-start 11 --region-end 15 \
    --notes "P test locus" --out "$work_dir/locus.json"

python3 - "$work_dir/locus.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    locus = json.load(handle)

expected = {
    "vg_path_name": "CHM13#0#chr1",
    "odgi_path_name": "CHM13#0#chr1",
    "node_local_position": 12,
    "vg_node_region": "CHM13#0#chr1:12",
    "odgi_node_position": "CHM13#0#chr1,12",
    "gfaidx_region": "CHM13#0#chr1:11-15",
    "vg_region": "CHM13#0#chr1:11-14",
    "odgi_region": "CHM13#0#chr1:11-15",
    "gbz_interval": "11..15",
}
for key, value in expected.items():
    if locus.get(key) != value:
        raise SystemExit(f"{key}: expected {value!r}, observed {locus.get(key)!r}")
PY

# A path absent from VG/ODGI must fail during setup rather than during a timed
# query after expensive indexes have already been built.
if python3 "$resolver" \
    --vg-paths "$path_names" --odgi-paths "$path_names" \
    --gbz-metadata "$gbz_metadata" --graph demo --query-id missing \
    --sample missing --haplotype 0 --seq-id chr1 --node-position 1 \
    --out "$work_dir/missing.json" 2>/dev/null; then
    echo "Missing P path unexpectedly resolved" >&2
    exit 1
fi
