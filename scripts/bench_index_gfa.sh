#!/usr/bin/env bash
set -euo pipefail

# Benchmark runner for `gfaidx index_gfa`.
#
# The script reads benchmark definitions from benchmarks/datasets.tsv, runs one
# or more datasets through the local binary, captures a detailed log for each
# run, measures wall time and peak RSS for the full process tree, and appends a
# single TSV row to benchmarks/results.tsv.
#
# Generated outputs are deleted by default because the medium and stress graphs
# are large. Use --keep-output when you want to inspect the produced files.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATASETS_TSV="$REPO_ROOT/benchmarks/datasets.tsv"
RESULTS_TSV="$REPO_ROOT/benchmarks/results.tsv"
LOG_DIR="$REPO_ROOT/benchmarks/logs"
WORK_DIR="$REPO_ROOT/benchmarks/work"
MEASURE_SCRIPT="$REPO_ROOT/scripts/measure_tree_rss.py"

DATASET_SELECTOR="all"
RUNS=1
BINARY="$REPO_ROOT/build/gfaidx"
KEEP_OUTPUT=0
SAMPLE_INTERVAL="0.10"
TMP_BASE="$WORK_DIR"

usage() {
    cat <<USAGE
Usage:
  $(basename "$0") [dataset_name|all] [options]

Options:
  --runs N             Number of repetitions per dataset (default: 1)
  --binary PATH        Path to the gfaidx binary (default: build/gfaidx)
  --keep-output        Keep the generated .gz/.idx/.ndx files and temp dir
  --sample-interval S  RSS sampling interval in seconds (default: 0.10)
  --tmp-base PATH      Parent directory for per-run work directories
  -h, --help           Show this help message

Examples:
  $(basename "$0") small
  $(basename "$0") medium --runs 3
  $(basename "$0") all --binary "$REPO_ROOT/cmake-build-debug/gfaidx"
USAGE
}

# Return the size of a file in bytes on either macOS or Linux.
# `stat` uses different flags on the two systems, so we probe both forms.
file_size_bytes() {
    local path="$1"
    if [[ ! -e "$path" ]]; then
        echo 0
        return
    fi
    if stat -f%z "$path" >/dev/null 2>&1; then
        stat -f%z "$path"
    else
        stat -c%s "$path"
    fi
}

# Write the header the first time results.tsv is created so later appends stay
# simple and the file remains easy to inspect with `column -t -s $'\t'`.
ensure_results_header() {
    if [[ -f "$RESULTS_TSV" ]]; then
        return
    fi
    mkdir -p "$(dirname "$RESULTS_TSV")"
    cat > "$RESULTS_TSV" <<'HEADER'
timestamp	git_commit	hostname	platform	dataset	class	input_gfa	input_bytes	run_index	wall_seconds	peak_rss_kb	exit_code	measurement_mode	output_gz_bytes	output_idx_bytes	output_ndx_bytes	command	log_path
HEADER
}

# Read a key=value metrics file produced by measure_tree_rss.py and export the
# keys as shell variables for the current scope.
load_metrics() {
    local metrics_file="$1"
    if [[ ! -f "$metrics_file" ]]; then
        return
    fi
    while IFS='=' read -r key value; do
        [[ -z "$key" ]] && continue
        printf -v "$key" '%s' "$value"
    done < "$metrics_file"
}

# Look up one dataset row by name from benchmarks/datasets.tsv.
# The file is tab-separated and may contain comment lines beginning with '#'.
lookup_dataset() {
    local wanted="$1"
    awk -F '\t' -v wanted="$wanted" '
        BEGIN { found = 0 }
        /^#/ { next }
        NR == 1 { next }
        $1 == wanted {
            print $1 "\t" $2 "\t" $3 "\t" $4
            found = 1
            exit
        }
        END {
            if (!found) {
                exit 1
            }
        }
    ' "$DATASETS_TSV"
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            all)
                DATASET_SELECTOR="all"
                shift
                ;;
            small|medium|stress)
                DATASET_SELECTOR="$1"
                shift
                ;;
            --runs)
                RUNS="$2"
                shift 2
                ;;
            --binary)
                BINARY="$2"
                shift 2
                ;;
            --keep-output)
                KEEP_OUTPUT=1
                shift
                ;;
            --sample-interval)
                SAMPLE_INTERVAL="$2"
                shift 2
                ;;
            --tmp-base)
                TMP_BASE="$2"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                echo "Unknown argument: $1" >&2
                usage >&2
                exit 1
                ;;
        esac
    done
}

append_result_row() {
    local timestamp="$1"
    local git_commit="$2"
    local hostname="$3"
    local platform="$4"
    local dataset_name="$5"
    local dataset_class="$6"
    local input_gfa="$7"
    local input_bytes="$8"
    local run_index="$9"
    local wall_seconds="${10}"
    local peak_rss_kb="${11}"
    local exit_code="${12}"
    local measurement_mode="${13}"
    local output_gz_bytes="${14}"
    local output_idx_bytes="${15}"
    local output_ndx_bytes="${16}"
    local command="${17}"
    local log_path="${18}"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$timestamp" "$git_commit" "$hostname" "$platform" "$dataset_name" \
        "$dataset_class" "$input_gfa" "$input_bytes" "$run_index" "$wall_seconds" \
        "$peak_rss_kb" "$exit_code" "$measurement_mode" "$output_gz_bytes" "$output_idx_bytes" \
        "$output_ndx_bytes" "$command" "$log_path" >> "$RESULTS_TSV"
}

run_one_dataset() {
    local dataset_name="$1"
    local row
    row="$(lookup_dataset "$dataset_name")"
    IFS=$'\t' read -r dataset_name dataset_class input_gfa progress_every <<< "$row"

    local input_path="$REPO_ROOT/$input_gfa"
    if [[ ! -f "$input_path" ]]; then
        echo "Input dataset not found: $input_path" >&2
        exit 1
    fi

    local input_bytes
    input_bytes="$(file_size_bytes "$input_path")"

    for ((run_index = 1; run_index <= RUNS; run_index++)); do
        local timestamp
        timestamp="$(date '+%Y-%m-%dT%H:%M:%S%z')"
        local safe_timestamp
        safe_timestamp="$(date '+%Y%m%d_%H%M%S')"
        local run_id="${safe_timestamp}_${dataset_name}_run${run_index}"

        local run_work_dir="$TMP_BASE/$run_id"
        local output_dir="$run_work_dir/out"
        local tmp_dir="$run_work_dir/tmp"
        local output_gz="$output_dir/${dataset_name}.indexed.gfa.gz"
        local log_path="$LOG_DIR/${run_id}.log"
        local metrics_path="$LOG_DIR/${run_id}.metrics"

        mkdir -p "$LOG_DIR" "$output_dir" "$tmp_dir"

        local -a cmd
        cmd=("$BINARY" index_gfa "$input_path" "$output_gz" --tmp_dir "$tmp_dir")
        if [[ "$progress_every" != "0" && -n "$progress_every" ]]; then
            cmd+=(--progress_every "$progress_every")
        fi

        local command_string
        command_string="${cmd[*]}"

        echo "Running dataset=$dataset_name run=$run_index"
        echo "Command: $command_string"

        local measure_status=0
        if "$MEASURE_SCRIPT" \
            --log "$log_path" \
            --metrics "$metrics_path" \
            --sample-interval "$SAMPLE_INTERVAL" \
            -- "${cmd[@]}"; then
            measure_status=0
        else
            measure_status=$?
        fi

        local wall_seconds=""
        local peak_rss_kb=""
        local exit_code=""
        local measurement_mode=""
        load_metrics "$metrics_path"
        if [[ -z "$exit_code" ]]; then
            exit_code="$measure_status"
        fi

        local output_gz_bytes output_idx_bytes output_ndx_bytes
        output_gz_bytes="$(file_size_bytes "$output_gz")"
        output_idx_bytes="$(file_size_bytes "$output_gz.idx")"
        output_ndx_bytes="$(file_size_bytes "$output_gz.ndx")"

        append_result_row \
            "$timestamp" \
            "$(git -C "$REPO_ROOT" rev-parse --short HEAD)" \
            "$(hostname)" \
            "$(uname -s)-$(uname -m)" \
            "$dataset_name" \
            "$dataset_class" \
            "$input_gfa" \
            "$input_bytes" \
            "$run_index" \
            "$wall_seconds" \
            "$peak_rss_kb" \
            "$exit_code" \
            "$measurement_mode" \
            "$output_gz_bytes" \
            "$output_idx_bytes" \
            "$output_ndx_bytes" \
            "$command_string" \
            "$log_path"

        echo "Result: wall=${wall_seconds}s peak_rss=${peak_rss_kb}KB exit=${exit_code}"
        echo "Log: $log_path"

        if [[ "$exit_code" != "0" ]]; then
            echo "Benchmark failed; kept work directory in $run_work_dir"
        elif [[ "$KEEP_OUTPUT" -eq 0 ]]; then
            rm -rf "$run_work_dir"
        else
            echo "Kept outputs in $run_work_dir"
        fi
    done
}

main() {
    parse_args "$@"

    if [[ ! -f "$DATASETS_TSV" ]]; then
        echo "Missing dataset manifest: $DATASETS_TSV" >&2
        exit 1
    fi
    if [[ ! -x "$MEASURE_SCRIPT" ]]; then
        echo "Measurement helper is not executable: $MEASURE_SCRIPT" >&2
        exit 1
    fi
    if [[ ! -x "$BINARY" ]]; then
        echo "gfaidx binary is not executable: $BINARY" >&2
        exit 1
    fi

    ensure_results_header
    mkdir -p "$LOG_DIR" "$WORK_DIR"

    if [[ "$DATASET_SELECTOR" == "all" ]]; then
        while IFS=$'\t' read -r name _class _input _progress; do
            [[ -z "$name" || "$name" == "name" || "$name" == \#* ]] && continue
            run_one_dataset "$name"
        done < "$DATASETS_TSV"
    else
        run_one_dataset "$DATASET_SELECTOR"
    fi
}

main "$@"
