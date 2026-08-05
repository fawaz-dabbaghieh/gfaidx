#!/usr/bin/env bash
set -euo pipefail

converter=$1
input_gfa=$2
expected_gfa=$3
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-w-to-p.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

# Verify GFA version conversion, PanSN-style coordinate names, orientation,
# optional tag preservation, and pass-through of existing graph records.
awk -f "$converter" "$input_gfa" >"$work_dir/converted.gfa"
diff -u "$expected_gfa" "$work_dir/converted.gfa"

# Malformed walks must fail rather than silently emitting an incomplete P path.
if printf 'W\tsample\t0\tchr1\t0\t1\t1>\n' |
    awk -f "$converter" >"$work_dir/malformed.gfa" 2>/dev/null; then
    echo "Malformed W walk unexpectedly converted successfully" >&2
    exit 1
fi
