# libgbwt query prototype

Standalone correctness prototype — not integrated into gfaidx's build.
See `docs/gbwt-path-index-exploration.md` for full context and validation
results.

## Building

Requires `libgbwt` (github.com/jltsiren/gbwt) built against the
**`vgteam/sdsl-lite`** fork specifically (not upstream `simongog/sdsl-lite`
— it's missing `sdsl/simple_sds.hpp`, which GBWT's v5+ format needs):

```
git clone https://github.com/vgteam/sdsl-lite.git
cd sdsl-lite && ./install.sh $(pwd)

git clone https://github.com/jltsiren/gbwt.git
cd gbwt && make SDSL_DIR=../sdsl-lite -j8
```

Then:

```
g++ -std=c++17 -O2 -DNDEBUG \
  -I<gbwt>/include -I<sdsl-lite>/include \
  query_subgraph.cpp <gbwt>/lib/libgbwt.a \
  -L<sdsl-lite>/lib -lsdsl -lzstd -fopenmp -pthread \
  -o query_subgraph
```

## Usage

```
./query_subgraph <gbwt-file> <node-id-list-file>
```

`node-id-list-file`: one raw GFA segment id per line. Output: one line per
contiguous run found, tab-separated
`sample  haplotype  contig  path_seq=N  step_range=[a,b)  first_segment=X  last_segment=Y  run_length=N`.

**Node identity caveat:** this assumes numeric segment IDs, using the
shortcut `gbwt_node = 2 * segment_id` for the forward strand and
`2 * segment_id + 1` for the reverse strand (bidirectional GBWT
convention). Confirmed valid for `hprc_v2.0_chr22` by checking the GBWT
header's `offset`/`alphabet_size` fields against the graph's known
segment-id range. Does not generalize to non-numeric segment IDs — see
"Node identity" section in the design doc. **Both orientations must be
inserted into the target-node set** — an earlier version only inserted the
forward id, which silently dropped any sequence touching a target segment
exclusively via its reverse strand; see "A real bug the N=500 test didn't
catch" in the design doc for how this was found (a benchmark at larger N)
and confirmed (diffing actual mismatched rows, not assumed).

**Known gap vs. gfaidx's own `find_subpaths_for_node_ids`:** this prototype
does not suppress single-step (length-1) runs; gfaidx does. Filter
`run_length=1` entries when diffing against gfaidx output.

## Validated result

Exact match against gfaidx's own ground truth at every neighborhood size
benchmarked so far: N = 500, 1,000, 10,000, 50,000, and 100,000 nodes (all
BFS neighborhoods from seed node 112000000 on `hprc_v2.0_chr22`), 0 diff
lines at each size after excluding single-step runs (the known gap above).
The original N=500 single-point validation (74 keys, 111 records) is still
accurate but was, in hindsight, too narrow a test on its own — the
orientation bug above only showed up once larger, more sprawling
neighborhoods were tried. See the design doc for the full methodology and
the bug-finding story.

## Disk-seeking variant (`query_subgraph_diskseek.cpp`)

A second prototype, same query semantics and output format, but never
loads the BWT's flat `data` blob into RAM — it `pread()`s exactly the
bytes of each record it touches, on demand, decoding them with GBWT's own
`CompressedRecord`. Build the same way, substituting the source file:

```
g++ -std=c++17 -O2 -DNDEBUG \
  -I<gbwt>/include -I<sdsl-lite>/include \
  query_subgraph_diskseek.cpp <gbwt>/lib/libgbwt.a \
  -L<sdsl-lite>/lib -lsdsl -lzstd -fopenmp -pthread \
  -o query_subgraph_diskseek
```

Usage is identical: `./query_subgraph_diskseek <gbwt-file> <node-id-list-file>`.
Requires a **non-zstd** GBWT file (format version ≤5, `vg gbwt`'s default —
see "Problem B" in the design doc for why zstd compression, version ≥6,
destroys seekability); it throws a clear error if given a zstd file.

**Validated result:** byte-identical output to `query_subgraph` on the same
500-node chr22 query (123/123 rows), at 24.6 MB RSS vs. the eager loader's
113 MB (file is 107 MB). Full writeup, including a real finding about
`extract()`'s per-query record-fetch cost and its design implication, is in
the design doc's "Disk-resident reader — validated" section — read that
before reusing this prototype for a bigger query, since unconditional
`extract()` costs one record fetch per step of the *entire* haplotype, not
just the local neighborhood.

## Bounded-extraction variant (`query_subgraph_bounded.cpp`)

A third prototype, testing whether replacing unconditional `extract()`
(walk to the sequence's true end) with a local/bounded walk (stop at the
target set's boundary) closes the speed gap to gfaidx. Build the same way:

```
g++ -std=c++17 -O2 -DNDEBUG \
  -I<gbwt>/include -I<sdsl-lite>/include \
  query_subgraph_bounded.cpp <gbwt>/lib/libgbwt.a \
  -L<sdsl-lite>/lib -lsdsl -lzstd -fopenmp -pthread \
  -o query_subgraph_bounded
```

Usage identical: `./query_subgraph_bounded <gbwt-file> <node-id-list-file>`.
Output format drops `step_range` (not available without walking from the
sequence's true start, which is exactly the cost this design avoids):
`sample  haplotype  contig  path_seq=N  first_segment=X  last_segment=Y  run_length=N`.

**Result: correct at every size tested (0 diff lines, N=500 through
100,000), but not faster where it matters.** Wins 2–4× over unbounded
extraction at small neighborhoods (N≤10,000), but *loses* — 2× slower at
N=50,000, 6.5× slower at N=100,000 — once the neighborhood is large
enough to span genuinely conserved stretches shared by many haplotypes,
where "local" runs stop being short. Full writeup, including a worked
example of why GBWT's compression doesn't translate into cheap
per-haplotype reconstruction, is in the design doc's "Attempt 4:
bounded/local extraction" section — this was the deciding experiment
behind the final decision not to adopt GBWT-style indexing.

## Full reproduction

Exact commands used, in order. `docs/gbwt-path-index-exploration.md` has
the same sequence with rationale for each step.

```bash
# 0. Build libgbwt (see "Building" above) and vg (for --max-node 0 GBWT
#    construction; via bioconda: `mamba create -n gbwt-test -c bioconda vg`)

# 1. Reconstruct a complete GFA with real P/W lines (gfaidx's own .gz only
#    has H/S/L -- see reconstruct_full_gfa_wlines.sh / _plines.sh; edit the
#    GRAPH/OUT variables at the top of the script for your graph first)
./reconstruct_full_gfa_wlines.sh   # chr22 example, W-lines, ~19 min

# 2. Build the GBWT with node chopping disabled, so vg's internal node ids
#    stay 1:1 with gfaidx's own segment ids (see "Node identity" in the
#    design doc)
conda activate gbwt-test   # or however vg is on PATH
vg gbwt -G /tmp/gbwt_test/chr22_full.gfa -o chr22_nochop.gbwt \
  --max-node 0 --translation chr22_translation.txt

# 3. Compile the prototype (see "Building" above)
g++ -std=c++17 -O2 -DNDEBUG \
  -I<gbwt>/include -I<sdsl-lite>/include \
  query_subgraph.cpp <gbwt>/lib/libgbwt.a \
  -L<sdsl-lite>/lib -lsdsl -lzstd -fopenmp -pthread \
  -o query_subgraph

# 4. Generate ground truth from gfaidx's real get_subgraph, and extract its
#    500-node set from the output GFA's S-lines
cd /home/user3/tools/gfaidx
./build/gfaidx get_subgraph \
  /home/user3/graphs/hprc_v2.0_chr22/hprc-v2.0-mc-chm13_chr22.indexed.gfa.gz \
  112000000 /tmp/gbwt_test/gfaidx_subgraph.gfa --max_nodes 500
awk '$1=="S"{print $2}' /tmp/gbwt_test/gfaidx_subgraph.gfa | sort -n \
  > /tmp/gbwt_test/gfaidx_subgraph_nodes.txt

# 5. Run the prototype on the same node set
./query_subgraph chr22_nochop.gbwt /tmp/gbwt_test/gfaidx_subgraph_nodes.txt \
  > /tmp/gbwt_test/prototype_out2.tsv 2> /tmp/gbwt_test/prototype_stderr2.log

# 6. Compare: (sample, haplotype, contig, first_node, last_node, run_length)
#    from each side, gfaidx's own suppression of single-step runs excluded
#    on the prototype side (gfaidx already excludes them by construction)
cd /tmp/gbwt_test
awk -F'\t' '$1=="W"{
  n=split($7,segs,">"); first=segs[2]; last=segs[n]; run=n-1;
  print $2"\t"$3"\t"$4"\t"first"\t"last"\t"run
}' gfaidx_subgraph.gfa | sed 's/#subpath_[0-9]*_[0-9]*//' | sort > gt_compare.tsv

awk -F'\t' '{
  split($6,b,"="); first=b[2]; split($7,c,"="); last=c[2];
  split($8,d,"="); run=d[2];
  if (run > 1) print $1"\t"$2"\t"$3"\t"first"\t"last"\t"run
}' prototype_out2.tsv | sort > proto_compare.tsv

diff gt_compare.tsv proto_compare.tsv && echo "EXACT MATCH"

# 7. Disk-seeking variant: same query, compiled from query_subgraph_diskseek.cpp
#    (see "Disk-seeking variant" above), should reproduce the same 123 raw
#    rows byte-for-byte
/usr/bin/time -v ./query_subgraph_diskseek chr22_nochop.gbwt \
  /tmp/gbwt_test/gfaidx_subgraph_nodes.txt > diskseek_out.tsv 2> diskseek_stderr.log
diff <(sort prototype_out2.tsv) <(sort diskseek_out.tsv) && echo "EAGER == DISKSEEK, BYTE-IDENTICAL"
grep -E "Maximum resident|Elapsed" diskseek_stderr.log
```

Re-verified this comparison against the files still on disk while writing
this section: `gt_compare.tsv` and `proto_compare.tsv` are both 111 lines,
`diff` reports zero differences, and `cut -f1-3 gt_compare.tsv | sort -u`
gives 74 distinct keys — matching the numbers reported above and in the
design doc exactly. Step 7 (added when validating the disk-seeking
prototype) reproduced 24.6 MB RSS and byte-identical output to the eager
loader.
