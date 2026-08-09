#!/usr/bin/env bash
set -euo pipefail

converter=$1
input_gfa=$2
expected_gfa=$3
expected_mapping=$4
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-w-to-p-python.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

# The faster Python converter must remain byte-identical to the established AWK
# output for headers, coordinates, orientations, tags, and unpositioned walks.
python3 "$converter" "$input_gfa" "$work_dir/converted.gfa" \
    --mapping-out "$work_dir/w_to_p.tsv"
diff -u "$expected_gfa" "$work_dir/converted.gfa"
diff -u "$expected_mapping" "$work_dir/w_to_p.tsv"

# Standard input and output remain available for use in shell pipelines.
python3 "$converter" - --mapping-out "$work_dir/streamed_mapping.tsv" \
    <"$input_gfa" >"$work_dir/streamed.gfa"
diff -u "$expected_gfa" "$work_dir/streamed.gfa"
diff -u "$expected_mapping" "$work_dir/streamed_mapping.tsv"

# Malformed walks and duplicate converted path names must fail clearly.
if printf 'W\tsample\t0\tchr1\t0\t1\t1>\n' |
    python3 "$converter" - "$work_dir/malformed.gfa" 2>/dev/null; then
    echo "Malformed W walk unexpectedly converted successfully" >&2
    exit 1
fi

if printf 'P\tsample#0#chr1:0-1\t1+\t*\nW\tsample\t0\tchr1\t0\t1\t>1\n' |
    python3 "$converter" - "$work_dir/duplicate.gfa" 2>/dev/null; then
    echo "Duplicate converted path name unexpectedly succeeded" >&2
    exit 1
fi
