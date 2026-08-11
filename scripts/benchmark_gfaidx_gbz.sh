#!/usr/bin/env bash
# Benchmark gfaidx and gbz-base interval and node extraction on an indexed graph.
#
# How it works:
#   1. Edit the executable and index paths in the configuration block below.
#   2. Add interval queries as:
#        query_id|reference_sample|contig|start|end
#      Coordinates are zero-based half-open intervals. gfaidx receives
#      contig:start-end, while gbz-base receives sample, contig, and start..end.
#   3. Add node queries as:
#        query_id|node_id|context_bp
#      gbz-base runs first. The script counts S lines in its output GFA and uses
#      that count as gfaidx --max_nodes, so both runs return the same node scale.
#   4. Every command is measured with GNU `time -v`. Raw outputs and logs are
#      kept under OUT_DIR, and two compact result tables are written:
#        interval_metrics.tsv
#        node_metrics.tsv
#
# The node benchmark assumes the gbz-base database and gfaidx graph use the
# same input node IDs. This is true only when GBZ construction did not chop or
# otherwise renumber the queried nodes.

set -euo pipefail
export LC_ALL=C

# ---------------------------------------------------------------------------
# Configuration: edit these paths on the benchmark server.
# ---------------------------------------------------------------------------

GFAIDX_BIN="/home/fawaz/projects/gfaidx/build/gfaidx"
GBZ_BASE_BIN="/home/fawaz/tools/gbz-base/target/release/gbz-base"
TIME_BIN="/usr/bin/time"

GFAIDX_GRAPH="/mnt/scratch/fawaz/hprc_mc_v2_indexed/hprc-v2.0-mc-chm13.indexed.gfa.gz"
GBZ_DB="/mnt/scratch/fawaz/hprc_mc_v2_indexed/gbz_graph/hprc-v2.1-mc-chm13.gbz.db"

# A relative OUT_DIR is created under the directory where this script is run.
OUT_DIR="gfaidx_gbz_benchmark"

# Nested example intervals. Edit, remove, or add rows as needed.
INTERVAL_QUERIES=(
    "chr1_148m_1kb|CHM13|chr1|148000000|148001000"
    "chr1_148m_10kb|CHM13|chr1|148000000|148010000"
    "chr1_148m_100kb|CHM13|chr1|148000000|148100000"
    "chr1_148m_1mb|CHM13|chr1|148000000|149000000"
    "chr1_148m_5mb|CHM13|chr1|148000000|153000000"
    "chr1_148m_10mb|CHM13|chr1|148000000|158000000"
)

# Node queries from the example commands in the project notes.
NODE_QUERIES=(
    "node_629060_1mb|629060|1000000"
    "node_629060_10mb|629060|10000000"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

die() {
    printf 'benchmark_gfaidx_gbz.sh: %s\n' "$*" >&2
    exit 1
}

require_executable() {
    local path=$1
    [[ -x "$path" ]] || die "executable not found or not executable: $path"
}

require_file() {
    local path=$1
    [[ -f "$path" ]] || die "input file not found: $path"
}

run_timed() {
    local time_log=$1
    local stdout_file=$2
    local stderr_file=$3
    shift 3

    # GNU time writes its statistics directly to time_log. Program stdout and
    # stderr remain separate, so a GFA emitted on stdout is not mixed with logs.
    if ! "$TIME_BIN" -v -o "$time_log" "$@" \
        >"$stdout_file" 2>"$stderr_file"; then
        printf 'Command failed; see %s and %s\n' "$time_log" "$stderr_file" >&2
        return 1
    fi
}

elapsed_seconds() {
    local time_log=$1
    local elapsed
    elapsed=$(sed -n \
        's/^[[:space:]]*Elapsed (wall clock) time (h:mm:ss or m:ss):[[:space:]]*//p' \
        "$time_log" | tail -n 1)
    [[ -n "$elapsed" ]] || die "could not parse elapsed time from $time_log"

    # GNU time uses either m:ss or h:mm:ss. Convert both forms to seconds.
    awk -F ':' -v value="$elapsed" 'BEGIN {
        count = split(value, fields, ":")
        if (count == 3) {
            seconds = fields[1] * 3600 + fields[2] * 60 + fields[3]
        } else if (count == 2) {
            seconds = fields[1] * 60 + fields[2]
        } else {
            seconds = fields[1]
        }
        printf "%.6f", seconds
    }'
}

maximum_rss_kb() {
    local time_log=$1
    local rss
    rss=$(awk -F ':' '/Maximum resident set size \(kbytes\)/ {
        gsub(/[[:space:]]/, "", $2)
        print $2
    }' "$time_log" | tail -n 1)
    [[ -n "$rss" ]] || die "could not parse maximum RSS from $time_log"
    printf '%s' "$rss"
}

count_gfa_nodes() {
    local gfa=$1
    awk -F '\t' '$1 == "S" { nodes++ } END { print nodes + 0 }' "$gfa"
}

validate_query_id() {
    local query_id=$1
    [[ "$query_id" =~ ^[A-Za-z0-9._-]+$ ]] || \
        die "query ID must contain only letters, digits, dots, underscores, or dashes: $query_id"
}

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

require_executable "$GFAIDX_BIN"
require_executable "$GBZ_BASE_BIN"
require_executable "$TIME_BIN"
require_file "$GFAIDX_GRAPH"
require_file "$GBZ_DB"

mkdir -p "$OUT_DIR/intervals" "$OUT_DIR/nodes" "$OUT_DIR/logs"

INTERVAL_TABLE="$OUT_DIR/interval_metrics.tsv"
NODE_TABLE="$OUT_DIR/node_metrics.tsv"

printf 'query_id\treference\tcontig\tstart\tend\tinterval_bp\ttool\telapsed_seconds\tmax_rss_kb\toutput_nodes\toutput_gfa\ttime_log\n' \
    >"$INTERVAL_TABLE"
printf 'query_id\tnode_id\tcontext_bp\tmatched_nodes\ttool\telapsed_seconds\tmax_rss_kb\toutput_nodes\toutput_gfa\ttime_log\n' \
    >"$NODE_TABLE"

# ---------------------------------------------------------------------------
# Interval extraction
# ---------------------------------------------------------------------------

for query in "${INTERVAL_QUERIES[@]}"; do
    IFS='|' read -r query_id reference contig start end <<<"$query"
    validate_query_id "$query_id"
    [[ "$start" =~ ^[0-9]+$ && "$end" =~ ^[0-9]+$ && "$end" -gt "$start" ]] || \
        die "invalid interval query: $query"

    interval_bp=$((end - start))
    printf 'Running interval query %s (%s:%s-%s)\n' \
        "$query_id" "$contig" "$start" "$end"

    gbz_gfa="$OUT_DIR/intervals/${query_id}.gbz-base.gfa"
    gbz_time="$OUT_DIR/logs/${query_id}.gbz-base.interval.time.log"
    gbz_stderr="$OUT_DIR/logs/${query_id}.gbz-base.interval.stderr.log"
    run_timed "$gbz_time" "$gbz_gfa" "$gbz_stderr" \
        "$GBZ_BASE_BIN" query \
        --sample "$reference" --contig "$contig" \
        --interval "${start}..${end}" "$GBZ_DB"
    gbz_nodes=$(count_gfa_nodes "$gbz_gfa")
    printf '%s\t%s\t%s\t%s\t%s\t%s\tgbz-base\t%s\t%s\t%s\t%s\t%s\n' \
        "$query_id" "$reference" "$contig" "$start" "$end" "$interval_bp" \
        "$(elapsed_seconds "$gbz_time")" "$(maximum_rss_kb "$gbz_time")" \
        "$gbz_nodes" "$gbz_gfa" "$gbz_time" >>"$INTERVAL_TABLE"

    gfaidx_gfa="$OUT_DIR/intervals/${query_id}.gfaidx.gfa"
    gfaidx_time="$OUT_DIR/logs/${query_id}.gfaidx.interval.time.log"
    gfaidx_stdout="$OUT_DIR/logs/${query_id}.gfaidx.interval.stdout.log"
    gfaidx_stderr="$OUT_DIR/logs/${query_id}.gfaidx.interval.stderr.log"
    run_timed "$gfaidx_time" "$gfaidx_stdout" "$gfaidx_stderr" \
        "$GFAIDX_BIN" get_region "$GFAIDX_GRAPH" \
        "${contig}:${start}-${end}" "$gfaidx_gfa" \
        --reference "$reference" --with_coords --all_haplotypes
    gfaidx_nodes=$(count_gfa_nodes "$gfaidx_gfa")
    printf '%s\t%s\t%s\t%s\t%s\t%s\tgfaidx\t%s\t%s\t%s\t%s\t%s\n' \
        "$query_id" "$reference" "$contig" "$start" "$end" "$interval_bp" \
        "$(elapsed_seconds "$gfaidx_time")" "$(maximum_rss_kb "$gfaidx_time")" \
        "$gfaidx_nodes" "$gfaidx_gfa" "$gfaidx_time" >>"$INTERVAL_TABLE"
done

# ---------------------------------------------------------------------------
# Node extraction
# ---------------------------------------------------------------------------

for query in "${NODE_QUERIES[@]}"; do
    IFS='|' read -r query_id node_id context_bp <<<"$query"
    validate_query_id "$query_id"
    [[ "$node_id" =~ ^[0-9]+$ && "$context_bp" =~ ^[0-9]+$ && "$context_bp" -gt 0 ]] || \
        die "invalid node query: $query"

    printf 'Running node query %s (node %s, context %s bp)\n' \
        "$query_id" "$node_id" "$context_bp"

    gbz_gfa="$OUT_DIR/nodes/${query_id}.gbz-base.gfa"
    gbz_time="$OUT_DIR/logs/${query_id}.gbz-base.node.time.log"
    gbz_stderr="$OUT_DIR/logs/${query_id}.gbz-base.node.stderr.log"
    run_timed "$gbz_time" "$gbz_gfa" "$gbz_stderr" \
        "$GBZ_BASE_BIN" query --node "$node_id" --context "$context_bp" "$GBZ_DB"
    matched_nodes=$(count_gfa_nodes "$gbz_gfa")
    [[ "$matched_nodes" -gt 0 ]] || \
        die "gbz-base returned no S lines for node query $query_id"
    printf '%s\t%s\t%s\t%s\tgbz-base\t%s\t%s\t%s\t%s\t%s\n' \
        "$query_id" "$node_id" "$context_bp" "$matched_nodes" \
        "$(elapsed_seconds "$gbz_time")" "$(maximum_rss_kb "$gbz_time")" \
        "$matched_nodes" "$gbz_gfa" "$gbz_time" >>"$NODE_TABLE"

    gfaidx_gfa="$OUT_DIR/nodes/${query_id}.gfaidx.gfa"
    gfaidx_time="$OUT_DIR/logs/${query_id}.gfaidx.node.time.log"
    gfaidx_stdout="$OUT_DIR/logs/${query_id}.gfaidx.node.stdout.log"
    gfaidx_stderr="$OUT_DIR/logs/${query_id}.gfaidx.node.stderr.log"
    run_timed "$gfaidx_time" "$gfaidx_stdout" "$gfaidx_stderr" \
        "$GFAIDX_BIN" get_subgraph "$GFAIDX_GRAPH" "$node_id" "$gfaidx_gfa" \
        --with_coords --max_nodes "$matched_nodes"
    gfaidx_nodes=$(count_gfa_nodes "$gfaidx_gfa")
    printf '%s\t%s\t%s\t%s\tgfaidx\t%s\t%s\t%s\t%s\t%s\n' \
        "$query_id" "$node_id" "$context_bp" "$matched_nodes" \
        "$(elapsed_seconds "$gfaidx_time")" "$(maximum_rss_kb "$gfaidx_time")" \
        "$gfaidx_nodes" "$gfaidx_gfa" "$gfaidx_time" >>"$NODE_TABLE"
done

printf 'Finished. Results:\n  %s\n  %s\n' "$INTERVAL_TABLE" "$NODE_TABLE"
