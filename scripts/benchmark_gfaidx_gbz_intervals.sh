#!/usr/bin/env bash
# Repeated interval benchmark comparing gfaidx and gbz-base context queries.
#
# The input TSV describes genomic locations. Every location is queried with each
# configured context length, and every (location, context) unit is executed
# several times with every timed variant so the raw table can distinguish the
# first execution from later executions that may benefit from the
# operating-system page cache. Variant order rotates within each repetition,
# and unit order is shuffled with a recorded seed to reduce systematic order
# effects.
#
# The timed variants form three comparisons:
#
#   reference_only   gbz-base --context N --reference-only
#                    gfaidx --mode reference --context N --with_coords
#                    Same subgraph and one reference walk with coordinates in
#                    both tools: the like-for-like comparison.
#   all_haplotypes   gbz-base --context N
#                    gfaidx --mode bfs --context N            (matched work)
#                    gfaidx --mode bfs --context N --with_coords (full output)
#                    gbz-base writes non-reference haplotypes as anonymous
#                    `unknown` walks without coordinates; gfaidx without
#                    --with_coords does matching work, while --with_coords
#                    adds sample names and coordinates for every subwalk.
#   haplotype_span   optional gfaidx --mode all --with_coords, gfaidx's own
#                    haplotype-span selection; it has no gbz-base counterpart
#                    and runs once per location without a context.
#
# Required TSV columns:
#   query_id  locus_id  reference  contig  start  end
#
# Coordinates are zero-based and half-open. `query_id` identifies one exact
# interval and must be unique. `locus_id` should be shared by intervals centered
# on the same genomic location when several interval sizes are tested.
#
# GNU time -v records wall time, peak RSS, page faults, and filesystem I/O. Raw
# outputs are deleted after their GFA counts, byte sizes, and parity hashes are
# recorded unless --keep-outputs is supplied. Logs and one row per execution are
# always kept.

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUMMARY_SCRIPT="$SCRIPT_DIR/summarize_gfaidx_gbz_intervals.py"
PARITY_SCRIPT="$SCRIPT_DIR/gfa_output_parity.py"

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
CONTEXTS="100"
INCLUDE_ALL_MODE=0
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
  --gbz-db PATH           gbz-base database (or GBZ) for the same source graph
  --queries PATH          TSV containing query_id, locus_id, reference,
                          contig, start, and end

Options:
  --repeats N             Executions per variant, interval, and context (default: 5)
  --contexts LIST         Comma-separated context lengths in bases passed to
                          --context of both tools (default: 100)
  --gfaidx-threads LIST   Comma-separated gfaidx thread counts (default: 1,8)
  --include-all-mode      Also time gfaidx --mode all --with_coords once per
                          interval; gfaidx-only, not paired with gbz-base
  --out-dir PATH          Output directory (default: gfaidx_gbz_interval_benchmark)
  --time-bin PATH         GNU time executable (default: /usr/bin/time)
  --python PATH           Python executable used for summaries (default: python3)
  --shuffle-seed N        Seed used to shuffle query/context order (default: 17)
  --bootstrap-samples N   Resamples used for 95% confidence intervals (default: 10000)
  --plot-formats LIST     Comma-separated Matplotlib formats (default: png,svg)
  --keep-outputs          Retain every extracted GFA
  --no-plots              Produce only raw_runs.tsv; skip summaries and plots
  -h, --help              Show this help

Each context runs two paired comparisons: reference_only (gbz-base
--reference-only vs gfaidx --mode reference --with_coords) and
all_haplotypes (gbz-base vs gfaidx --mode bfs, timed without --with_coords
for matched work and with it for full output). Node-set and reference-walk
hashes are recorded so the summaries can check that paired tools extracted
the same subgraph.

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
        --contexts)
            CONTEXTS=${2:-}
            shift 2
            ;;
        --gfaidx-threads)
            GFAIDX_THREADS=${2:-}
            shift 2
            ;;
        --include-all-mode)
            INCLUDE_ALL_MODE=1
            shift
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
# variant label in the raw table, which lets the same summarizer compare any
# number of gfaidx configurations without needing separate benchmark runs.
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

# Contexts are passed verbatim to both tools. Plain base counts avoid the two
# tools' different suffix spellings (gbz-base `k`, gfaidx `kb`), and zero is
# valid: it restricts both tools to the reference interval nodes.
[[ "$CONTEXTS" =~ ^(0|[1-9][0-9]*)(,(0|[1-9][0-9]*))*$ ]] || \
    die "--contexts must be a comma-separated list of non-negative integers"
declare -a CONTEXT_VALUES=()
IFS=',' read -r -a CONTEXT_VALUES <<< "$CONTEXTS"
SEEN_CONTEXT_VALUES=$'\n'
for context in "${CONTEXT_VALUES[@]}"; do
    [[ "$SEEN_CONTEXT_VALUES" != *$'\n'"$context"$'\n'* ]] || \
        die "--contexts contains a duplicate value: $context"
    SEEN_CONTEXT_VALUES+="$context"$'\n'
done

# Every timed configuration is one variant. Parallel arrays keep the label, the
# comparison it belongs to, and the flags that build its command line together.
# gbz-base has neither a thread option nor a coordinate switch for these
# queries, so both are recorded as NA.
declare -a VARIANT_LABELS=()
declare -a VARIANT_TOOLS=()
declare -a VARIANT_COMPARISONS=()
declare -a VARIANT_MODES=()
declare -a VARIANT_COORDS=()
declare -a VARIANT_THREADS=()

add_variant() {
    VARIANT_LABELS+=("$1")
    VARIANT_TOOLS+=("$2")
    VARIANT_COMPARISONS+=("$3")
    VARIANT_MODES+=("$4")
    VARIANT_COORDS+=("$5")
    VARIANT_THREADS+=("$6")
}

add_variant gbz-base-ref gbz-base reference_only reference-only NA NA
for threads in "${GFAIDX_THREAD_VALUES[@]}"; do
    add_variant "gfaidx-ref-coords-${threads}t" gfaidx reference_only reference 1 "$threads"
done
add_variant gbz-base-all gbz-base all_haplotypes all NA NA
for threads in "${GFAIDX_THREAD_VALUES[@]}"; do
    add_variant "gfaidx-bfs-${threads}t" gfaidx all_haplotypes bfs 0 "$threads"
    add_variant "gfaidx-bfs-coords-${threads}t" gfaidx all_haplotypes bfs 1 "$threads"
done
if (( INCLUDE_ALL_MODE )); then
    for threads in "${GFAIDX_THREAD_VALUES[@]}"; do
        add_variant "gfaidx-span-coords-${threads}t" gfaidx haplotype_span all 1 "$threads"
    done
fi

# The parser below depends on the stable labels printed by GNU time -v.
if ! "$TIME_BIN" --version 2>&1 | grep -qi 'GNU time'; then
    die "$TIME_BIN is not GNU time; install GNU time and pass its path with --time-bin"
fi

# Output counts and parity hashes come from one Python pass per output.
require_executable "$PYTHON_BIN"
require_file "$PARITY_SCRIPT"
if (( MAKE_PLOTS )); then
    require_file "$SUMMARY_SCRIPT"
fi

RAW_TABLE="$OUT_DIR/raw_runs.tsv"
[[ ! -e "$RAW_TABLE" ]] || \
    die "$RAW_TABLE already exists; select a new --out-dir to keep experiments separate"
mkdir -p "$OUT_DIR/logs" "$OUT_DIR/outputs"
SUMMARY_SCRATCH=$(mktemp -d "$OUT_DIR/.output_summaries.XXXXXX")
trap 'rm -rf "$SUMMARY_SCRATCH"' EXIT

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

# One execution unit is one query at one context. All repetitions of a unit stay
# together so later repetitions can measure reuse of cached pages. The optional
# haplotype-span variant has no context and gets its own unit per query.
declare -a UNIT_QUERIES=()
declare -a UNIT_CONTEXTS=()
for ((i = 0; i < ${#QUERY_IDS[@]}; ++i)); do
    for context in "${CONTEXT_VALUES[@]}"; do
        UNIT_QUERIES+=("$i")
        UNIT_CONTEXTS+=("$context")
    done
    if (( INCLUDE_ALL_MODE )); then
        UNIT_QUERIES+=("$i")
        UNIT_CONTEXTS+=("NA")
    fi
done

# Fisher-Yates gives a deterministic but non-size-sorted unit order. Exact
# order is also written into every raw row for auditing.
declare -a UNIT_ORDER=()
for ((i = 0; i < ${#UNIT_QUERIES[@]}; ++i)); do
    UNIT_ORDER+=("$i")
done
RANDOM=$((SHUFFLE_SEED % 32768))
for ((i = ${#UNIT_ORDER[@]} - 1; i > 0; --i)); do
    j=$((RANDOM % (i + 1)))
    tmp=${UNIT_ORDER[$i]}
    UNIT_ORDER[$i]=${UNIT_ORDER[$j]}
    UNIT_ORDER[$j]=$tmp
done

printf '%s\n' \
    $'timestamp\tquery_order\tquery_id\tlocus_id\treference\tcontig\tstart\tend\tinterval_bp\tcomparison\ttool\tvariant\tcontext_bp\twith_coords\trun_index\torder_in_block\tgfaidx_threads\telapsed_seconds\tmax_rss_kb\tuser_seconds\tsystem_seconds\tcpu_percent\tmajor_page_faults\tminor_page_faults\tfilesystem_inputs\tfilesystem_outputs\toutput_nodes\toutput_edges\toutput_paths\toutput_bytes\tnode_set_sha1\treference_walks\treference_walk_steps\treference_walk_sha1\texit_status\toutput_retained\toutput_gfa\ttime_log\tstderr_log\tcommand' \
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
    printf 'contexts\t%s\n' "$CONTEXTS"
    printf 'gfaidx_threads\t%s\n' "$GFAIDX_THREADS"
    printf 'include_all_mode\t%s\n' "$INCLUDE_ALL_MODE"
    printf 'variants\t%s\n' "$(IFS=','; printf '%s' "${VARIANT_LABELS[*]}")"
    printf 'parity_hash\t%s\n' "sha1 ($PARITY_SCRIPT)"
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

# Counts and parity hashes for one output, from a single streaming pass. GFA
# outputs are deterministic, so each query/context/variant is summarized on its
# first repetition and later repetitions reuse that line; only the byte size is
# re-measured. The scratch file name encodes the unit, which keeps this working
# under bash 3.2 without associative arrays.
output_summary() {
    local path=$1 cache=$2 reference=$3 contig=$4 start=$5 end=$6
    if [[ ! -s "$cache" ]]; then
        "$PYTHON_BIN" "$PARITY_SCRIPT" "$path" --reference "$reference" \
            --contig "$contig" --start "$start" --end "$end" > "$cache"
    fi
    cat "$cache"
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

run_one_variant() {
    local variant_index=$1
    local query_index=$2
    local context=$3
    local query_order=$4
    local run_index=$5
    local order_in_block=$6

    local variant=${VARIANT_LABELS[$variant_index]}
    local tool=${VARIANT_TOOLS[$variant_index]}
    local comparison=${VARIANT_COMPARISONS[$variant_index]}
    local mode=${VARIANT_MODES[$variant_index]}
    local with_coords=${VARIANT_COORDS[$variant_index]}
    local gfaidx_threads=${VARIANT_THREADS[$variant_index]}

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

    local stem="$variant"
    [[ "$context" == "NA" ]] || stem+=".ctx${context}"
    stem+=".run_${run_label}"
    local output_gfa="$query_dir/${stem}.gfa"
    local stdout_file="$log_dir/${stem}.stdout.log"
    local stderr_file="$log_dir/${stem}.stderr.log"
    local time_log="$log_dir/${stem}.time.log"
    local -a command

    if [[ "$tool" == "gfaidx" ]]; then
        command=(
            "$GFAIDX_BIN" get_region "$GFAIDX_GRAPH"
            "${contig}:${start}-${end}" "$output_gfa"
            --reference "$reference"
            --mode "$mode"
        )
        [[ "$context" == "NA" ]] || command+=(--context "$context")
        [[ "$with_coords" != "1" ]] || command+=(--with_coords)
        command+=(--threads "$gfaidx_threads")
    else
        command=(
            "$GBZ_BASE_BIN" query
            --sample "$reference"
            --contig "$contig"
            --interval "${start}..${end}"
            --context "$context"
        )
        [[ "$mode" != "reference-only" ]] || command+=(--reference-only)
        command+=("$GBZ_DB")
    fi

    printf 'Unit %d/%d, run %d/%d, order %d: %s context=%s %s (%s:%s-%s)\n' \
        "$query_order" "${#UNIT_QUERIES[@]}" "$run_index" "$REPEATS" \
        "$order_in_block" "$variant" "$context" "$query_id" "$contig" "$start" "$end"

    local exit_status=0
    if [[ "$tool" == "gfaidx" ]]; then
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

    local output_bytes
    output_bytes=$(file_size_bytes "$output_gfa")
    # Counts and parity values are computed after GNU time has finished, so
    # they never contribute to the measured wall time or memory.
    local output_nodes output_edges output_paths node_hash
    local reference_walks reference_walk_steps reference_walk_hash
    local summary_cache="$SUMMARY_SCRATCH/${query_id}.${variant}.ctx${context}.tsv"
    (( exit_status != 0 )) || IFS=$'\t' read -r output_nodes output_edges output_paths \
        node_hash reference_walks reference_walk_steps reference_walk_hash \
        <<< "$(output_summary "$output_gfa" "$summary_cache" "$reference" \
            "$contig" "$start" "$end")"
    local command_text
    command_text=$(quote_command "${command[@]}")
    local retained=0
    (( KEEP_OUTPUTS == 0 )) || retained=1
    local -a fields=(
        "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        "$query_order" "$query_id" "$locus_id" "$reference" "$contig"
        "$start" "$end" "$interval_bp" "$comparison" "$tool" "$variant"
        "$context" "$with_coords" "$run_index"
        "$order_in_block" "$gfaidx_threads" "$elapsed" "$max_rss" "$user_seconds"
        "$system_seconds" "$cpu_percent" "$major_faults" "$minor_faults"
        "$filesystem_inputs" "$filesystem_outputs" "$output_nodes"
        "$output_edges" "$output_paths" "$output_bytes" "$node_hash"
        "$reference_walks" "$reference_walk_steps" "$reference_walk_hash"
        "$exit_status" "$retained" "$output_gfa" "$time_log" "$stderr_file"
        "$command_text"
    )
    (IFS=$'\t'; printf '%s\n' "${fields[*]}") >> "$RAW_TABLE"

    if (( KEEP_OUTPUTS == 0 )); then
        rm -f "$output_gfa"
    fi
    (( exit_status == 0 )) || \
        die "$variant failed for $query_id (context $context) run $run_index; see $stderr_file"
}

execution_position=0
for unit_index in "${UNIT_ORDER[@]}"; do
    execution_position=$((execution_position + 1))
    query_index=${UNIT_QUERIES[$unit_index]}
    context=${UNIT_CONTEXTS[$unit_index]}

    # A context unit runs both gbz-base comparisons and their gfaidx partners;
    # a context-free unit runs only the optional gfaidx haplotype-span variant.
    declare -a block=()
    for ((variant_index = 0; variant_index < ${#VARIANT_LABELS[@]}; ++variant_index)); do
        if [[ "$context" == "NA" ]]; then
            [[ "${VARIANT_COMPARISONS[$variant_index]}" == "haplotype_span" ]] || continue
        else
            [[ "${VARIANT_COMPARISONS[$variant_index]}" != "haplotype_span" ]] || continue
        fi
        block+=("$variant_index")
    done

    for ((run_index = 1; run_index <= REPEATS; ++run_index)); do
        # Rotate which variant runs first. This balances execution-order effects
        # across tools, modes, and thread counts while keeping repetitions of the
        # same unit close enough to expose page-cache behavior.
        rotation=$(((execution_position + run_index - 2) % ${#block[@]}))
        for ((offset = 0; offset < ${#block[@]}; ++offset)); do
            run_one_variant \
                "${block[$(((rotation + offset) % ${#block[@]}))]}" \
                "$query_index" "$context" "$execution_position" \
                "$run_index" "$((offset + 1))"
        done
    done
    unset block
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
