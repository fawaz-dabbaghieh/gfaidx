#!/usr/bin/env bash
# Building with --threads > 1 must write the same index files as one thread.
# The GFAIDX_TEST_* variables force the parallel paths on a small graph: every
# P/W line is parsed in parallel, postings spill into many tiny runs, and the
# runs are merged in several passes of several groups at once.
set -euo pipefail

gfaidx=$1
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-parallel-test.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

input_gfa="$work_dir/graph.gfa"
awk -v n=2000 'BEGIN {
    OFS = "\t"
    print "H", "VN:Z:1.0"
    for (i = 1; i <= n; i++) print "S", i, "ACGT"
    for (i = 1; i < n; i++) print "L", i, "+", i + 1, "+", "0M"
    for (i = 1; i + 2 <= n; i += 7) print "L", i, "+", i + 2, "+", "0M"
    p = ""; for (i = 1; i <= n; i++) p = p (i > 1 ? "," : "") i "+"
    print "P", "forward", p, "*"
    p = ""; for (i = 1; i <= n; i++) if (i % 7 != 2) p = p (p != "" ? "," : "") i "+"
    print "P", "skipping", p, "*"
    p = ""; for (i = n; i >= 1; i--) p = p (i < n ? "," : "") i "-"
    print "P", "reverse", p, "*"
    w = ""; for (i = 1; i <= n; i++) w = w (i % 5 ? ">" : "<") i
    print "W", "sample", 1, "chr1", 0, 4 * n, w
    w = ""; for (r = 0; r < 3; r++) for (i = 100; i <= 400; i++) w = w ">" i
    print "W", "sample", 2, "chr1", 0, 3612, w
}' > "$input_gfa"

build() {
    local out=$1
    shift
    "$gfaidx" index_gfa "$input_gfa" "$out" --progress_every 0 "$@" > "$out.log" 2>&1
}

build "$work_dir/serial.gz"
GFAIDX_TEST_PARALLEL_PARSE_MIN_BYTES=1 \
GFAIDX_TEST_POSTING_CHUNK_RECORDS=500 \
GFAIDX_TEST_POSTING_MERGE_FAN_IN=3 \
    build "$work_dir/parallel.gz" --threads 4
GFAIDX_TEST_POSTING_CHUNK_RECORDS=700 \
GFAIDX_TEST_POSTING_MERGE_FAN_IN=4 \
    build "$work_dir/serial_runs.gz"

if ! grep -q 'groups at a time' "$work_dir/parallel.gz.log"; then
    echo "parallel build did not merge posting runs in parallel" >&2
    exit 1
fi

for variant in parallel serial_runs; do
    for ext in "" .idx .ndx .lnx .pdx .pcx; do
        if ! cmp -s "$work_dir/serial.gz$ext" "$work_dir/$variant.gz$ext"; then
            echo "$variant build differs from the serial build in graph.gz$ext" >&2
            exit 1
        fi
    done
done

# index_paths takes --threads too and must match the .pdx index_gfa wrote.
GFAIDX_TEST_PARALLEL_PARSE_MIN_BYTES=1 GFAIDX_TEST_POSTING_CHUNK_RECORDS=300 \
    "$gfaidx" index_paths "$input_gfa" "$work_dir/paths.pdx" \
        --ndx "$work_dir/serial.gz.ndx" --threads 3 > "$work_dir/paths.log" 2>&1
cmp "$work_dir/serial.gz.pdx" "$work_dir/paths.pdx"

# A parallel parse reports the error a serial parse would hit first: the
# malformed token comes before the unknown node.
awk 'BEGIN { OFS = "\t" } $1 == "P" && $2 == "forward" {
    $3 = $3 ",,12+,99999+"
} { print }' "$input_gfa" > "$work_dir/bad.gfa"
for threads in 1 4; do
    if GFAIDX_TEST_PARALLEL_PARSE_MIN_BYTES=1 "$gfaidx" index_gfa "$work_dir/bad.gfa" \
            "$work_dir/bad_$threads.gz" --threads "$threads" > "$work_dir/bad_$threads.log" 2>&1; then
        echo "index_gfa accepted a malformed path with --threads $threads" >&2
        exit 1
    fi
    if ! grep -q 'Malformed path step token' "$work_dir/bad_$threads.log"; then
        echo "--threads $threads reported a different error:" >&2
        tail -n 3 "$work_dir/bad_$threads.log" >&2
        exit 1
    fi
done

if "$gfaidx" index_gfa "$input_gfa" "$work_dir/zero.gz" --threads 0 > "$work_dir/zero.log" 2>&1; then
    echo "index_gfa accepted --threads 0" >&2
    exit 1
fi

echo "parallel build matches the serial build"
