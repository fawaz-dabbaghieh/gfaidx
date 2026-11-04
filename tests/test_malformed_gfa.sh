#!/usr/bin/env bash
# index_gfa and the query commands on malformed or unusual GFA input: each
# command must succeed or stop with exit status 1 and a message, never crash.
# Inputs that are valid GFA 1 must index, and inputs that cannot be indexed
# must be rejected, with the same result for --threads 2 and legacy_hash.
set -euo pipefail

gfaidx=$1
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-malformed-test.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

H=$'H\tVN:Z:1.0\n'
S=$'S\t1\tACGT\nS\t2\tGG\nS\t3\tTTT\n'
L=$'L\t1\t+\t2\t+\t0M\nL\t2\t+\t3\t+\t0M\n'
P=$'P\tp1\t1+,2+,3+\t*\n'
W=$'W\ts1\t1\tchr1\t0\t9\t>1>2>3\n'

failures=0
fail() {
    echo "FAIL $*" >&2
    failures=$((failures + 1))
}

# Run one command and set status. Exit statuses other than 0 and 1 (signals
# appear as 128 + N), sanitizer reports, and silent errors are failures.
run() {
    local label=$1
    shift
    status=0
    "$@" >"$work_dir/stdout" 2>"$work_dir/stderr" || status=$?
    if (( status != 0 && status != 1 )); then
        fail "$label: exit status $status"
        tail -n 3 "$work_dir/stderr" >&2
    elif grep -q -e "AddressSanitizer" -e "runtime error:" "$work_dir/stderr"; then
        fail "$label: sanitizer report"
        grep -m 1 -e "AddressSanitizer" -e "runtime error:" "$work_dir/stderr" >&2
    elif (( status == 1 )) && ! grep -v -q "^gfaidx version" "$work_dir/stderr"; then
        fail "$label: exit status 1 without an error message"
    fi
}

# check_case <name> <ok|fail|any> <GFA text>
check_case() {
    local name=$1 expect=$2 text=$3
    local dir="$work_dir/$name"
    mkdir "$dir"
    printf '%s' "$text" >"$dir/in.gfa"
    local out="$dir/g.gz"

    run "$name index_gfa" "$gfaidx" index_gfa "$dir/in.gfa" "$out" --progress_every 0 --tmp_dir "$dir"
    local indexed=$status
    if [[ $expect == ok && $indexed != 0 ]]; then
        fail "$name: index_gfa rejected a valid input: $(tail -n 1 "$work_dir/stderr")"
    elif [[ $expect == fail && $indexed == 0 ]]; then
        fail "$name: index_gfa accepted an input it cannot index"
    fi
    run "$name index_gfa --threads 2" "$gfaidx" index_gfa "$dir/in.gfa" "$dir/t2.gz" \
        --progress_every 0 --tmp_dir "$dir" --threads 2
    [[ $status == "$indexed" ]] || fail "$name: --threads 2 exit status $status, default $indexed"
    run "$name index_gfa legacy_hash" "$gfaidx" index_gfa "$dir/in.gfa" "$dir/legacy.gz" \
        --progress_every 0 --tmp_dir "$dir" --node_order legacy_hash
    [[ $status == "$indexed" ]] || fail "$name: legacy_hash exit status $status, default $indexed"
    if [[ $indexed != 0 ]]; then
        # A rejected input leaves no partial index behind.
        if ls "$out"* >/dev/null 2>&1; then
            fail "$name: rejected input left $(ls "$out"*)"
        fi
        return
    fi

    run "$name get_chunk" "$gfaidx" get_chunk "$out" --community_id 0
    run "$name get_chunk --node_id" "$gfaidx" get_chunk "$out" --node_id 1
    run "$name get_subgraph" "$gfaidx" get_subgraph "$out" 1 "$dir/sub.gfa" --with_coords
    run "$name get_path --print_path_names" "$gfaidx" get_path "$out" --print_path_names
    run "$name get_path --path_id" "$gfaidx" get_path "$out" --path_id p1
    run "$name get_path --nodes" "$gfaidx" get_path "$out" --nodes 1,2 --with_walk_coords
    run "$name index_coordinates" "$gfaidx" index_coordinates "$out" "$out.cdx" --progress_every 0
    run "$name index_path_checkpoints" "$gfaidx" index_path_checkpoints "$out" "$dir/c.pcx" \
        --checkpoint_steps 1
    run "$name get_region" "$gfaidx" get_region "$out" chr1:2-7 "$dir/r1.gfa" --reference s1 --with_coords
    run "$name get_region gap" "$gfaidx" get_region "$out" chr1:2-7 "$dir/r2.gfa" --reference s1 \
        --with_coords --haplotype_gap 0 --threads 2
    run "$name get_region bfs" "$gfaidx" get_region "$out" chr1:2-7 "$dir/r3.gfa" --reference s1 \
        --mode bfs --context 2
    run "$name get_region P" "$gfaidx" get_region "$out" p1:0-5 "$dir/r4.gfa" --mode reference
}

long_sequence=$(head -c 200000 /dev/zero | tr '\0' 'A')
long_name=$(head -c 100000 /dev/zero | tr '\0' 'n')

# Valid GFA 1 that differs from the usual layout.
check_case valid ok "$H$S$L$P$W"
check_case no_links ok "$H$S$P$W"
check_case no_paths ok "$H$S$L"
check_case one_segment ok $'H\tVN:Z:1.0\nS\t1\tACGT\n'
check_case no_header ok "$S$L$P$W"
check_case records_out_of_order ok "$H$W$P$L$S"
check_case gfa_1_2 ok $'H\tVN:Z:1.2\n'"$S$L$P$W"
check_case comments_and_blank_lines ok "$H"$'# comment\n\n'"$S"$'\n'"$L$P$W"
check_case no_final_newline ok "$H$S$L$P${W%$'\n'}"
check_case star_sequences_with_LN ok "$H"$'S\t1\t*\tLN:i:4\nS\t2\t*\tLN:i:2\nS\t3\t*\tLN:i:3\n'"$L$P$W"
check_case segment_tags ok "$H"$'S\t1\tACGT\tLN:i:4\tSN:Z:chr1\tSO:i:0\tSR:i:0\n'$'S\t2\tGG\nS\t3\tTTT\n'"$L$P$W"
check_case unknown_record_types ok "$H$S"$'X\tfoo\nC\t1\t+\t2\t+\t0\t0M\nJ\t1\t+\t3\t+\t*\n'"$L$P$W"
check_case self_loops_and_reverse_links ok "$H$S$L"$'L\t1\t+\t1\t+\t0M\nL\t2\t+\t2\t-\t0M\nL\t3\t-\t2\t-\t0M\n'"$P$W"
check_case duplicate_links ok "$H$S$L$L$P$W"
check_case non_numeric_names ok "$H"$'S\tnodeA\tACGT\nS\tnode-B.2\tGG\nL\tnodeA\t+\tnode-B.2\t+\t0M\nP\tp1\tnodeA+,node-B.2+\t*\n'
check_case long_sequence_line ok "$H"$'S\t1\t'"$long_sequence"$'\nS\t2\tGG\nS\t3\tTTT\n'"$L$P"
check_case long_name ok "$H"$'S\t'"$long_name"$'\tACGT\n'"$S$L$P"
check_case single_step_path ok "$H$S$L"$'P\tp1\t2+\t*\n'
check_case repeated_node_path ok "$H$S$L"$'L\t3\t+\t1\t+\t0M\nP\tp1\t1+,2+,3+,1+,2+,3+\t*\n'
check_case path_without_links ok "$H$S$L"$'P\tp1\t3+,1+\t*\n'
check_case path_overlaps ok "$H$S$L"$'P\tp1\t1+,2+,3+\t0M,0M\n'"$W"
check_case walk_star_coordinates ok "$H$S$L$P"$'W\ts1\t1\tchr1\t*\t*\t>1>2>3\n'
check_case walk_length_mismatch ok "$H$S$L$P"$'W\ts1\t1\tchr1\t0\t100\t>1>2>3\n'
check_case walk_overlapping_fragments ok "$H$S$L$P$W"$'W\ts1\t1\tchr1\t4\t9\t>2>3\n'

# Inputs that cannot be indexed.
check_case gfa2 fail $'H\tVN:Z:2.0\nS\t1\t4\tACGT\nS\t2\t2\tGG\nE\t*\t1+\t2+\t4$\t4$\t0\t0\t*\n'
check_case no_segments fail "$H$L$P$W"
check_case paths_only fail "$H$P$W"
check_case links_only fail "$H$L"
check_case spaces_for_tabs fail "${H// /}"$'S 1 ACGT\nS 2 GG\nL 1 + 2 + 0M\n'
check_case segment_without_sequence fail "$H"$'S\t1\nS\t2\tGG\nS\t3\tTTT\n'"$L$P$W"
check_case segment_letter_only fail "$H"$'S\n'"$S$L$P$W"
check_case segment_empty_name fail "$H"$'S\t\tACGT\n'"$S$L$P$W"
check_case star_sequence_without_LN fail "$H"$'S\t1\t*\nS\t2\tGG\nS\t3\tTTT\n'"$L$P$W"
check_case duplicate_segment fail "$H$S"$'S\t2\tGGGG\n'"$L$P$W"
check_case link_letter_only fail "$H$S"$'L\n'"$L$P$W"
check_case link_two_fields fail "$H$S"$'L\t1\t+\n'"$L$P$W"
check_case link_unknown_node fail "$H$S$L"$'L\t3\t+\t99\t+\t0M\n'"$P$W"
check_case path_without_steps fail "$H$S$L"$'P\tp1\n'"$W"
check_case path_empty_step fail "$H$S$L"$'P\tp1\t1+,,3+\t*\n'"$W"
check_case path_step_without_orientation fail "$H$S$L"$'P\tp1\t1,2,3\t*\n'"$W"
check_case path_bad_orientation fail "$H$S$L"$'P\tp1\t1x,2+\t*\n'"$W"
check_case path_unknown_node fail "$H$S$L"$'P\tp1\t1+,99+,3+\t*\n'"$W"
check_case walk_without_steps fail "$H$S$L$P"$'W\ts1\t1\tchr1\t0\t9\n'
check_case walk_too_few_fields fail "$H$S$L$P"$'W\ts1\t1\tchr1\n'
check_case walk_text_haplotype fail "$H$S$L$P"$'W\ts1\tx\tchr1\t0\t9\t>1>2>3\n'
check_case walk_text_coordinates fail "$H$S$L$P"$'W\ts1\t1\tchr1\tabc\tdef\t>1>2>3\n'
check_case walk_coordinate_overflow fail "$H$S$L$P"$'W\ts1\t1\tchr1\t0\t99999999999999999999\t>1>2>3\n'
check_case walk_step_without_orientation fail "$H$S$L$P"$'W\ts1\t1\tchr1\t0\t9\t1>2>3\n'
check_case walk_empty_step fail "$H$S$L$P"$'W\ts1\t1\tchr1\t0\t9\t>1>>3\n'
check_case walk_unknown_node fail "$H$S$L$P"$'W\ts1\t1\tchr1\t0\t9\t>1>99>3\n'

# Accepted for now; these only have to fail cleanly downstream.
check_case empty_file any ""
check_case header_only any "$H"
check_case windows_line_endings any "${H//$'\n'/$'\r\n'}${S//$'\n'/$'\r\n'}${L//$'\n'/$'\r\n'}${P//$'\n'/$'\r\n'}"
check_case empty_sequence any "$H"$'S\t1\t\nS\t2\tGG\nS\t3\tTTT\n'"$L$P$W"
check_case link_without_overlap any "$H$S"$'L\t1\t+\t2\t+\nL\t2\t+\t3\t+\n'"$P$W"
check_case link_bad_orientation any "$H$S"$'L\t1\tx\t2\t?\t0M\n'"$L$P$W"
check_case path_without_overlap_field any "$H$S$L"$'P\tp1\t1+,2+,3+\n'"$W"
check_case path_trailing_comma any "$H$S$L"$'P\tp1\t1+,2+,\t*\n'"$W"
check_case duplicate_path_name any "$H$S$L$P"$'P\tp1\t3-,2-\t*\n'"$W"
check_case duplicate_walk any "$H$S$L$P$W$W"
check_case walk_start_after_end any "$H$S$L$P"$'W\ts1\t1\tchr1\t9\t0\t>1>2>3\n'
check_case walk_negative_start any "$H$S$L$P"$'W\ts1\t1\tchr1\t-5\t4\t>1>2>3\n'

# A gzipped GFA indexes like the plain file.
mkdir "$work_dir/gzip"
printf '%s' "$H$S$L$P$W" | gzip -c >"$work_dir/gzip/in.gfa.gz"
run "gzip index_gfa" "$gfaidx" index_gfa "$work_dir/gzip/in.gfa.gz" "$work_dir/gzip/g.gz" --progress_every 0
[[ $status == 0 ]] || fail "gzipped GFA input was rejected"

# GFA 2 is rejected by every command that reads a GFA, with a message saying so.
gfa2="$work_dir/gfa2/in.gfa"
for command in "index_paths $work_dir/gfa2.pdx --ndx $work_dir/valid/g.gz.ndx" \
               "index_coordinates $work_dir/gfa2.cdx --ndx $work_dir/valid/g.gz.ndx --progress_every 0" \
               "index_gfa $work_dir/gfa2.gz --progress_every 0"; do
    set -- $command
    name=$1
    shift
    run "gfa2 $name" "$gfaidx" "$name" "$gfa2" "$@"
    if [[ $status != 1 ]] || ! grep -q "GFA version 2 is not supported" "$work_dir/stderr"; then
        fail "$name did not reject GFA 2: $(tail -n 1 "$work_dir/stderr")"
    fi
done

if (( failures > 0 )); then
    echo "$failures malformed-input check(s) failed" >&2
    exit 1
fi
echo "malformed GFA inputs ok"
