#!/usr/bin/env bash
# Repeated interval benchmark for gfaidx thread variants and gbz-base.
#
# The input TSV describes genomic locations. Each location is executed several
# times with every configured gfaidx thread count and with gbz-base so the raw
# table can distinguish the first execution from later executions that may
# benefit from the operating-system page cache. Tool order rotates within each
# repetition, and query order is shuffled with a recorded seed to reduce
# systematic order effects.
#
# Required TSV columns:
#   query_id  locus_id  reference  contig  start  end
#
# Coordinates are zero-based and half-open. `query_id` identifies one exact
# interval and must be unique. `locus_id` should be shared by intervals centered
# on the same genomic location when several interval sizes are tested.
#
# GNU time -v records wall time, peak RSS, page faults, and filesystem I/O. Raw
# outputs are deleted after their GFA counts and byte sizes are recorded unless
# --keep-outputs is supplied. Logs and one row per execution are always kept.

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUMMARY_SCRIPT="$SCRIPT_DIR/summarize_gfaidx_gbz_intervals.py"

GFAIDX_BIN=""
GBZ_BASE_BIN=""
GFAIDX_GRAPH=""
GBZ_DB=""
QUERY_TSV=""
OUT_DIR="gfaidx_gbz_interval_benchmark"
TIME_BIN="/usr/bin/time"
PYTHON_BIN="python3"
REPEATS=5
GFAIDX_THREADS="1,8"
SHUFFLE_SEED=17
BOOTSTRAP_SAMPLES=10000
PLOT_FORMATS="png,svg"
KEEP_OUTPUTS=0
MAKE_PLOTS=1

usage() {
    cat <<'USAGE'
Usage:
  benchmark_gfaidx_gbz_intervals.sh \
    --gfaidx-bin PATH \
    --gbz-base-bin PATH \
    --gfaidx-graph PATH \
    --gbz-db PATH \
    --queries intervals.tsv \
    [options]

Required:
  --gfaidx-bin PATH       gfaidx executable
  --gbz-base-bin PATH     gbz-base executable
  --gfaidx-graph PATH     indexed GFA used by gfaidx
  --gbz-db PATH           gbz-base database for the same source graph
  --queries PATH          TSV containing query_id, locus_id, reference,
                          contig, start, and end

Options:
  --repeats N             Executions per tool and interval (default: 5)
  --gfaidx-threads LIST   Comma-separated gfaidx thread counts (default: 1,8)
  --out-dir PATH          Output directory (default: gfaidx_gbz_interval_benchmark)
  --time-bin PATH         GNU time executable (default: /usr/bin/time)
  --python PATH           Python executable used for summaries (default: python3)
  --shuffle-seed N        Seed used to shuffle query order (default: 17)
  --bootstrap-samples N   Resamples used for 95% confidence intervals (default: 10000)
  --plot-formats LIST     Comma-separated Matplotlib formats (default: png,svg)
  --keep-outputs          Retain every extracted GFA
  --no-plots              Produce only raw_runs.tsv; skip summaries and plots
  -h, --help              Show this help

The output directory must not already contain raw_runs.tsv. This prevents an
accidental rerun from mixing measurements from different experiments.
USAGE
}

die() {
    printf 'benchmark_gfaidx_gbz_intervals.sh: %s\n' "$*" >&2
    exit 1
}

require_executable() {
    local path=$1
    if [[ "$path" == */* ]]; then
        [[ -x "$path" ]] || die "executable not found or not executable: $path"
    else
        command -v "$path" >/dev/null 2>&1 || die "executable not found on PATH: $path"
    fi
}

require_file() {
    local path=$1
    [[ -f "$path" ]] || die "input file not found: $path"
}

require_positive_integer() {
    local name=$1
    local value=$2
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "$name must be a positive integer: $value"
}

validate_identifier() {
    local name=$1
    local value=$2
    [[ "$value" =~ ^[A-Za-z0-9._-]+$ ]] || \
        die "$name must contain only letters, digits, dots, underscores, or dashes: $value"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --gfaidx-bin)
            GFAIDX_BIN=${2:-}
            shift 2
            ;;
        --gbz-base-bin)
            GBZ_BASE_BIN=${2:-}
            shift 2
            ;;
        --gfaidx-graph)
            GFAIDX_GRAPH=${2:-}
            shift 2
            ;;
        --gbz-db)
            GBZ_DB=${2:-}
            shift 2
            ;;
        --queries)
            QUERY_TSV=${2:-}
            shift 2
            ;;
        --repeats)
            REPEATS=${2:-}
            shift 2
            ;;
        --gfaidx-threads)
            GFAIDX_THREADS=${2:-}
            shift 2
            ;;
        --out-dir)
            OUT_DIR=${2:-}
            shift 2
            ;;
        --time-bin)
            TIME_BIN=${2:-}
            shift 2
            ;;
        --python)
            PYTHON_BIN=${2:-}
            shift 2
            ;;
        --shuffle-seed)
            SHUFFLE_SEED=${2:-}
            shift 2
            ;;
        --bootstrap-samples)
            BOOTSTRAP_SAMPLES=${2:-}
            shift 2
            ;;
        --plot-formats)
            PLOT_FORMATS=${2:-}
            shift 2
            ;;
        --keep-outputs)
            KEEP_OUTPUTS=1
            shift
            ;;
        --no-plots)
            MAKE_PLOTS=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $1"
            ;;
    esac
done

[[ -n "$GFAIDX_BIN" ]] || die "--gfaidx-bin is required"
[[ -n "$GBZ_BASE_BIN" ]] || die "--gbz-base-bin is required"
[[ -n "$GFAIDX_GRAPH" ]] || die "--gfaidx-graph is required"
[[ -n "$GBZ_DB" ]] || die "--gbz-db is required"
[[ -n "$QUERY_TSV" ]] || die "--queries is required"
require_executable "$GFAIDX_BIN"
require_executable "$GBZ_BASE_BIN"
require_executable "$TIME_BIN"
require_file "$GFAIDX_GRAPH"
require_file "$GBZ_DB"
require_file "$QUERY_TSV"
require_positive_integer "--repeats" "$REPEATS"
require_positive_integer "--bootstrap-samples" "$BOOTSTRAP_SAMPLES"
[[ "$SHUFFLE_SEED" =~ ^[0-9]+$ ]] || die "--shuffle-seed must be a non-negative integer"

# Parse the comma-separated thread sweep once. Each count becomes a separate
# tool label in the raw table, which lets the same summarizer compare any number
# of gfaidx configurations without needing separate benchmark runs or tables.
[[ "$GFAIDX_THREADS" =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]] || \
    die "--gfaidx-threads must be a comma-separated list of positive integers"
declare -a GFAIDX_THREAD_VALUES=()
IFS=',' read -r -a GFAIDX_THREAD_VALUES <<< "$GFAIDX_THREADS"
SEEN_THREAD_VALUES=$'\n'
for threads in "${GFAIDX_THREAD_VALUES[@]}"; do
    [[ "$SEEN_THREAD_VALUES" != *$'\n'"$threads"$'\n'* ]] || \
        die "--gfaidx-threads contains a duplicate value: $threads"
    SEEN_THREAD_VALUES+="$threads"$'\n'
done

# Parallel arrays keep the display label and actual gfaidx thread count paired.
# gbz-base has no thread option for these queries, so its count is recorded as NA.
declare -a TOOL_VARIANTS=()
declare -a TOOL_THREAD_COUNTS=()
for threads in "${GFAIDX_THREAD_VALUES[@]}"; do
    TOOL_VARIANTS+=("gfaidx-${threads}t")
    TOOL_THREAD_COUNTS+=("$threads")
done
TOOL_VARIANTS+=("gbz-base")
TOOL_THREAD_COUNTS+=("NA")

# The parser below depends on the stable labels printed by GNU time -v.
if ! "$TIME_BIN" --version 2>&1 | grep -qi 'GNU time'; then
    die "$TIME_BIN is not GNU time; install GNU time and pass its path with --time-bin"
fi

if (( MAKE_PLOTS )); then
    require_executable "$PYTHON_BIN"
    require_file "$SUMMARY_SCRIPT"
fi

RAW_TABLE="$OUT_DIR/raw_runs.tsv"
[[ ! -e "$RAW_TABLE" ]] || \
    die "$RAW_TABLE already exists; select a new --out-dir to keep experiments separate"
mkdir -p "$OUT_DIR/logs" "$OUT_DIR/outputs"

# Store query columns in parallel arrays so the shuffled execution order only
# moves small integer indexes and never reparses path or reference names.
declare -a QUERY_IDS=()
declare -a LOCUS_IDS=()
declare -a REFERENCES=()
declare -a CONTIGS=()
declare -a STARTS=()
declare -a ENDS=()
SEEN_QUERY_IDS=$'\n'

line_number=0
while IFS=$'\t' read -r query_id locus_id reference contig start end extra || \
      [[ -n "${query_id:-}" ]]; do
    line_number=$((line_number + 1))
    end=${end%$'\r'}

    [[ -n "${query_id:-}" ]] || continue
    [[ "${query_id:0:1}" != "#" ]] || continue
    if [[ "$query_id" == "query_id" ]]; then
        [[ "$locus_id" == "locus_id" && "$reference" == "reference" && \
           "$contig" == "contig" && "$start" == "start" && "$end" == "end" ]] || \
            die "unexpected TSV header on line $line_number"
        continue
    fi

    [[ -z "${extra:-}" ]] || die "too many TSV columns on line $line_number"
    [[ -n "$locus_id" && -n "$reference" && -n "$contig" ]] || \
        die "empty locus, reference, or contig on line $line_number"
    validate_identifier "query_id on line $line_number" "$query_id"
    validate_identifier "locus_id on line $line_number" "$locus_id"
    [[ "$start" =~ ^[0-9]+$ && "$end" =~ ^[0-9]+$ ]] || \
        die "start and end must be non-negative integers on line $line_number"
    (( end > start )) || die "end must be greater than start on line $line_number"
    # Newline delimiters are unambiguous because validated identifiers cannot
    # contain whitespace. A scalar also avoids empty-array behavior differences
    # between the Bash 3.2 shipped by macOS and current Linux Bash releases.
    [[ "$SEEN_QUERY_IDS" != *$'\n'"$query_id"$'\n'* ]] || \
        die "duplicate query_id on line $line_number: $query_id"

    SEEN_QUERY_IDS+="$query_id"$'\n'
    QUERY_IDS+=("$query_id")
    LOCUS_IDS+=("$locus_id")
    REFERENCES+=("$reference")
    CONTIGS+=("$contig")
    STARTS+=("$start")
    ENDS+=("$end")
done < "$QUERY_TSV"

(( ${#QUERY_IDS[@]} > 0 )) || die "no interval rows were found in $QUERY_TSV"

# Fisher-Yates gives a deterministic but non-size-sorted query order. Exact
# order is also written into every raw row for auditing.
declare -a QUERY_ORDER=()
for ((i = 0; i < ${#QUERY_IDS[@]}; ++i)); do
    QUERY_ORDER+=("$i")
done
RANDOM=$((SHUFFLE_SEED % 32768))
for ((i = ${#QUERY_ORDER[@]} - 1; i > 0; --i)); do
    j=$((RANDOM % (i + 1)))
    tmp=${QUERY_ORDER[$i]}
    QUERY_ORDER[$i]=${QUERY_ORDER[$j]}
    QUERY_ORDER[$j]=$tmp
done

printf '%s\n' \
    $'timestamp\tquery_order\tquery_id\tlocus_id\treference\tcontig\tstart\tend\tinterval_bp\ttool\trun_index\torder_in_block\tgfaidx_threads\telapsed_seconds\tmax_rss_kb\tuser_seconds\tsystem_seconds\tcpu_percent\tmajor_page_faults\tminor_page_faults\tfilesystem_inputs\tfilesystem_outputs\toutput_nodes\toutput_edges\toutput_paths\toutput_bytes\texit_status\toutput_retained\toutput_gfa\ttime_log\tstderr_log\tcommand' \
    > "$RAW_TABLE"

# Record enough environment information to reproduce or diagnose the run while
# avoiding checksums over very large graph indexes.
{
    printf 'started_utc\t%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'host\t%s\n' "$(hostname)"
    printf 'platform\t%s-%s\n' "$(uname -s)" "$(uname -m)"
    printf 'gfaidx_binary\t%s\n' "$GFAIDX_BIN"
    printf 'gbz_base_binary\t%s\n' "$GBZ_BASE_BIN"
    printf 'gfaidx_graph\t%s\n' "$GFAIDX_GRAPH"
    printf 'gbz_database\t%s\n' "$GBZ_DB"
    printf 'query_tsv\t%s\n' "$QUERY_TSV"
    printf 'repeats\t%s\n' "$REPEATS"
    printf 'gfaidx_threads\t%s\n' "$GFAIDX_THREADS"
    printf 'shuffle_seed\t%s\n' "$SHUFFLE_SEED"
    printf 'gfaidx_version\t%s\n' "$("$GFAIDX_BIN" --version 2>&1 | tr '\n' ' ')"
    printf 'gbz_base_version\t%s\n' "$("$GBZ_BASE_BIN" --version 2>&1 | tr '\n' ' ' || true)"
} > "$OUT_DIR/metadata.tsv"

time_value() {
    local time_log=$1
    local label=$2
    local line
    line=$(grep -F "$label" "$time_log" | tail -n 1 || true)
    if [[ -z "$line" ]]; then
        printf 'NA'
        return
    fi
    line=${line#*: }
    printf '%s' "$line"
}

elapsed_seconds() {
    local elapsed=$1
    if [[ "$elapsed" == "NA" ]]; then
        printf 'NA'
        return
    fi
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

file_size_bytes() {
    local path=$1
    if [[ ! -f "$path" ]]; then
        printf '0'
    elif stat -c '%s' "$path" >/dev/null 2>&1; then
        stat -c '%s' "$path"
    else
        wc -c < "$path" | tr -d '[:space:]'
    fi
}

gfa_counts() {
    local path=$1
    if [[ ! -f "$path" ]]; then
        printf '0\t0\t0'
        return
    fi
    awk -F '\t' '
        $1 == "S" { nodes++ }
        $1 == "L" { edges++ }
        $1 == "P" || $1 == "W" { paths++ }
        END { printf "%d\t%d\t%d", nodes + 0, edges + 0, paths + 0 }
    ' "$path"
}

quote_command() {
    local rendered=""
    local argument quoted
    for argument in "$@"; do
        printf -v quoted '%q' "$argument"
        rendered+="${rendered:+ }${quoted}"
    done
    printf '%s' "$rendered"
}

run_one_tool() {
    local tool=$1
    local gfaidx_threads=$2
    local query_index=$3
    local query_order=$4
    local run_index=$5
    local order_in_block=$6

    local query_id=${QUERY_IDS[$query_index]}
    local locus_id=${LOCUS_IDS[$query_index]}
    local reference=${REFERENCES[$query_index]}
    local contig=${CONTIGS[$query_index]}
    local start=${STARTS[$query_index]}
    local end=${ENDS[$query_index]}
    local interval_bp=$((end - start))
    local run_label
    printf -v run_label '%02d' "$run_index"

    local query_dir="$OUT_DIR/outputs/$query_id"
    local log_dir="$OUT_DIR/logs/$query_id"
    mkdir -p "$query_dir" "$log_dir"

    local output_gfa="$query_dir/${tool}.run_${run_label}.gfa"
    local stdout_file="$log_dir/${tool}.run_${run_label}.stdout.log"
    local stderr_file="$log_dir/${tool}.run_${run_label}.stderr.log"
    local time_log="$log_dir/${tool}.run_${run_label}.time.log"
    local -a command

    if [[ "$gfaidx_threads" != "NA" ]]; then
        command=(
            "$GFAIDX_BIN" get_region "$GFAIDX_GRAPH"
            "${contig}:${start}-${end}" "$output_gfa"
            --reference "$reference"
            --all_haplotypes
            --with_coords
            --threads "$gfaidx_threads"
        )
    else
        command=(
            "$GBZ_BASE_BIN" query
            --sample "$reference"
            --contig "$contig"
            --interval "${start}..${end}"
            --context 0
            "$GBZ_DB"
        )
    fi

    printf 'Query %d/%d, run %d/%d, order %d: %s %s (%s:%s-%s)\n' \
        "$query_order" "${#QUERY_IDS[@]}" "$run_index" "$REPEATS" \
        "$order_in_block" "$tool" "$query_id" "$contig" "$start" "$end"

    local exit_status=0
    if [[ "$gfaidx_threads" != "NA" ]]; then
        "$TIME_BIN" -v -o "$time_log" "${command[@]}" \
            > "$stdout_file" 2> "$stderr_file" || exit_status=$?
    else
        "$TIME_BIN" -v -o "$time_log" "${command[@]}" \
            > "$output_gfa" 2> "$stderr_file" || exit_status=$?
        # gbz-base writes the GFA to stdout, so no separate stdout log exists.
        stdout_file="NA"
    fi

    local elapsed_text elapsed max_rss user_seconds system_seconds cpu_percent
    local major_faults minor_faults filesystem_inputs filesystem_outputs
    elapsed_text=$(time_value "$time_log" "Elapsed (wall clock) time")
    elapsed=$(elapsed_seconds "$elapsed_text")
    max_rss=$(time_value "$time_log" "Maximum resident set size (kbytes)")
    user_seconds=$(time_value "$time_log" "User time (seconds)")
    system_seconds=$(time_value "$time_log" "System time (seconds)")
    cpu_percent=$(time_value "$time_log" "Percent of CPU this job got")
    cpu_percent=${cpu_percent%\%}
    major_faults=$(time_value "$time_log" "Major (requiring I/O) page faults")
    minor_faults=$(time_value "$time_log" "Minor (reclaiming a frame) page faults")
    filesystem_inputs=$(time_value "$time_log" "File system inputs")
    filesystem_outputs=$(time_value "$time_log" "File system outputs")

    local output_nodes output_edges output_paths
    IFS=$'\t' read -r output_nodes output_edges output_paths \
        <<< "$(gfa_counts "$output_gfa")"
    local output_bytes
    output_bytes=$(file_size_bytes "$output_gfa")
    local command_text
    command_text=$(quote_command "${command[@]}")
    local retained=0
    (( KEEP_OUTPUTS == 0 )) || retained=1
    local -a fields=(
        "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        "$query_order" "$query_id" "$locus_id" "$reference" "$contig"
        "$start" "$end" "$interval_bp" "$tool" "$run_index"
        "$order_in_block" "$gfaidx_threads" "$elapsed" "$max_rss" "$user_seconds"
        "$system_seconds" "$cpu_percent" "$major_faults" "$minor_faults"
        "$filesystem_inputs" "$filesystem_outputs" "$output_nodes"
        "$output_edges" "$output_paths" "$output_bytes" "$exit_status"
        "$retained" "$output_gfa" "$time_log" "$stderr_file" "$command_text"
    )
    (IFS=$'\t'; printf '%s\n' "${fields[*]}") >> "$RAW_TABLE"

    if (( KEEP_OUTPUTS == 0 )); then
        rm -f "$output_gfa"
    fi
    (( exit_status == 0 )) || \
        die "$tool failed for $query_id run $run_index; see $stderr_file"
}

execution_position=0
for query_index in "${QUERY_ORDER[@]}"; do
    execution_position=$((execution_position + 1))
    for ((run_index = 1; run_index <= REPEATS; ++run_index)); do
        # Rotate which variant runs first. This balances execution-order effects
        # across gfaidx thread counts and gbz-base while keeping repetitions of
        # the same interval close enough to expose page-cache behavior.
        rotation=$(((execution_position + run_index - 2) % ${#TOOL_VARIANTS[@]}))
        for ((offset = 0; offset < ${#TOOL_VARIANTS[@]}; ++offset)); do
            variant_index=$(((rotation + offset) % ${#TOOL_VARIANTS[@]}))
            run_one_tool \
                "${TOOL_VARIANTS[$variant_index]}" \
                "${TOOL_THREAD_COUNTS[$variant_index]}" \
                "$query_index" "$execution_position" "$run_index" "$((offset + 1))"
        done
    done
done

if (( MAKE_PLOTS )); then
    "$PYTHON_BIN" "$SUMMARY_SCRIPT" \
        --raw "$RAW_TABLE" \
        --output-dir "$OUT_DIR" \
        --bootstrap-samples "$BOOTSTRAP_SAMPLES" \
        --seed "$SHUFFLE_SEED" \
        --formats "$PLOT_FORMATS"
fi

printf 'Finished repeated interval benchmark.\nRaw measurements: %s\n' "$RAW_TABLE"
if (( MAKE_PLOTS )); then
    printf 'Summaries: %s/tables\nPlots: %s/plots\n' "$OUT_DIR" "$OUT_DIR"
fi
