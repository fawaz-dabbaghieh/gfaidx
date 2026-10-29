#!/usr/bin/env bash
# Locality-ordered node ids (.ndx v2 / .pdx v5) must be invisible to queries:
# every query returns byte-identical output whether the graph was indexed
# with --node_order legacy_hash (hash-rank ids, v4 .pdx) or the default auto
# numbering with delta steps and slot-coded postings. Mixed index sets built
# against different numberings must be rejected.
set -euo pipefail

gfaidx=$1
repo=$2
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gfaidx-node-order-test.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

# Inputs: a simulated MC-like graph (W walks, loops, inversions, contig
# fragments), its P-line and non-numeric renamings, and two small fixtures.
python3 "$repo/scripts/sim_pangenome.py" 3000 12 7 > "$work_dir/sim.gfa"
python3 "$repo/scripts/w_to_p.py" "$work_dir/sim.gfa" "$work_dir/sim_p.gfa"
python3 "$repo/scripts/rename_gfa_nodes.py" "$work_dir/sim.gfa" "$work_dir/sim_nonnum.gfa" --mode non_numeric
python3 "$repo/scripts/rename_gfa_nodes.py" "$work_dir/sim.gfa" "$work_dir/sim_shuf.gfa" --mode shuffled_int
cp "$repo/tests/data/repeated_anchor_paths.gfa" "$work_dir/anchors.gfa"
cp "$repo/tests/data/context_paths.gfa" "$work_dir/context.gfa"

run_queries() {
    local graph=$1 input=$2 out=$3
    mkdir -p "$out"
    "$gfaidx" get_path "$graph" --print_path_names > "$out/names.txt" 2>/dev/null
    local i=0
    while IFS=$'\t' read -r kind f1 f2 f3 f4 f5; do
        local id=$f1
        [[ "$kind" == "W" ]] && id="$f1|$f2|$f3|$f4|$f5"
        "$gfaidx" get_path "$graph" --path_id "$id" > "$out/path_$i.gfa" 2>/dev/null
        i=$((i + 1))
        [[ $i -lt 6 ]] || break
    done < "$out/names.txt"

    # Node sets: a prefix of the S lines, every third node, and single nodes.
    awk -F'\t' '$1 == "S" {print $2}' "$input" > "$out/nodes.txt"
    head -n 60 "$out/nodes.txt" | paste -sd, > "$out/set_a.txt"
    awk 'NR % 3 == 0 && ++n <= 400' "$out/nodes.txt" | paste -sd, > "$out/set_b.txt"
    for set in set_a set_b; do
        "$gfaidx" get_path "$graph" --nodes "$(cat "$out/$set.txt")" > "$out/$set.gfa" 2>/dev/null || true
        "$gfaidx" get_path "$graph" --nodes "$(cat "$out/$set.txt")" --with_walk_coords \
            > "$out/${set}_coords.gfa" 2>/dev/null || true
    done
    for seed in $(awk 'NR % 97 == 1 && ++n <= 5' "$out/nodes.txt"); do
        "$gfaidx" get_subgraph "$graph" "$seed" "$out/sub_$seed.gfa" --max_nodes 200 >/dev/null 2>&1
        "$gfaidx" get_subgraph "$graph" "$seed" "$out/subc_$seed.gfa" --max_nodes 200 \
            --with_coords >/dev/null 2>&1
    done
    rm -f "$out/nodes.txt"
}

run_regions() {
    local graph=$1 region=$2 reference=$3 out=$4
    local -a ref_args=()
    [[ -z "$reference" ]] || ref_args=(--reference "$reference")
    for mode in all bfs reference; do
        local -a mode_args=(--mode "$mode")
        [[ "$mode" == "all" ]] || mode_args+=(--context 300)
        "$gfaidx" get_region "$graph" "$region" "$out/region_$mode.gfa" "${ref_args[@]}" \
            "${mode_args[@]}" >/dev/null 2>&1
        "$gfaidx" get_region "$graph" "$region" "$out/region_${mode}_coords.gfa" "${ref_args[@]}" \
            "${mode_args[@]}" --with_coords >/dev/null 2>&1
    done
}

compare_dirs() {
    local label=$1 a=$2 b=$3
    if ! diff -r -q "$a" "$b" >/dev/null; then
        echo "Outputs differ between legacy and auto node order for $label" >&2
        diff -r -q "$a" "$b" >&2 || true
        exit 1
    fi
}

for name in sim sim_p sim_nonnum sim_shuf anchors context; do
    input="$work_dir/$name.gfa"
    for variant in legacy auto small_blocks; do
        mkdir -p "$work_dir/$variant"
        args=(--progress_every 0)
        [[ "$variant" != "legacy" ]] || args+=(--node_order legacy_hash)
        [[ "$variant" != "small_blocks" ]] || args+=(--posting_block_nodes 3)
        "$gfaidx" index_gfa "$input" "$work_dir/$variant/$name.gfa.gz" "${args[@]}" >/dev/null 2>&1
        run_queries "$work_dir/$variant/$name.gfa.gz" "$input" "$work_dir/out_$variant/$name"
    done
    compare_dirs "$name" "$work_dir/out_legacy/$name" "$work_dir/out_auto/$name"
    compare_dirs "$name (3-node posting blocks)" "$work_dir/out_legacy/$name" "$work_dir/out_small_blocks/$name"
done

# Region queries through a .cdx and through on-the-fly path coordinates.
for variant in legacy auto; do
    graph="$work_dir/$variant/sim.gfa.gz"
    out="$work_dir/region_$variant"
    mkdir -p "$out/fly" "$out/cdx"
    run_regions "$graph" chr1:2000-2600 REF "$out/fly"
    "$gfaidx" get_path "$graph" --print_path_names 2>/dev/null | awk -F'\t' '$2 == "REF"' > "$work_dir/ref_names_$variant.txt"
    "$gfaidx" index_coordinates "$graph" "$graph.cdx" \
        --path_names_file "$work_dir/ref_names_$variant.txt" --progress_every 0 >/dev/null 2>&1
    run_regions "$graph" chr1:2000-2600 REF "$out/cdx"
done
compare_dirs "sim regions" "$work_dir/region_legacy" "$work_dir/region_auto"
compare_dirs "sim regions (.cdx vs on the fly)" "$work_dir/region_auto/fly" "$work_dir/region_auto/cdx"

# The auto index is smaller than the legacy one.
legacy_size=$(stat -c %s "$work_dir/legacy/sim.gfa.gz.pdx" 2>/dev/null || stat -f %z "$work_dir/legacy/sim.gfa.gz.pdx")
auto_size=$(stat -c %s "$work_dir/auto/sim.gfa.gz.pdx" 2>/dev/null || stat -f %z "$work_dir/auto/sim.gfa.gz.pdx")
if [[ $auto_size -ge $legacy_size ]]; then
    echo "Expected the auto .pdx ($auto_size bytes) to be smaller than legacy ($legacy_size bytes)" >&2
    exit 1
fi

# Mixed numberings must be rejected rather than silently remapped.
mixed="$work_dir/mixed"
mkdir -p "$mixed"
cp "$work_dir/legacy/sim.gfa.gz" "$work_dir/legacy/sim.gfa.gz.idx" "$work_dir/legacy/sim.gfa.gz.ndx" "$mixed/"
cp "$work_dir/auto/sim.gfa.gz.pdx" "$mixed/sim.gfa.gz.pdx"
if "$gfaidx" get_path "$mixed/sim.gfa.gz" --nodes "$(cut -d, -f1-3 "$work_dir/out_legacy/sim/set_a.txt")" \
    >/dev/null 2>"$mixed/err.txt"; then
    echo "A .pdx built against a different .ndx numbering was accepted" >&2
    exit 1
fi
grep -q "different .ndx node numbering" "$mixed/err.txt"
cp "$work_dir/auto/sim.gfa.gz.cdx" "$mixed/sim.gfa.gz.cdx"
cp "$work_dir/legacy/sim.gfa.gz.pdx" "$mixed/sim.gfa.gz.pdx"
if "$gfaidx" get_region "$mixed/sim.gfa.gz" chr1:2000-2600 "$mixed/r.gfa" --reference REF \
    >/dev/null 2>"$mixed/err.txt"; then
    echo "A .cdx built against a different .ndx numbering was accepted" >&2
    exit 1
fi
grep -q "different .ndx node numbering" "$mixed/err.txt"

echo "node order equivalence tests passed"
