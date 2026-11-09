# Helper scripts

Small tools used alongside gfaidx: converting GFA files, inspecting index
files, and generating test data. None of them is needed to index or query a
graph. Run any Python script with `--help` for its options where it has them.

The benchmarks against other tools live in [`benchmark/`](../benchmark):
the Snakemake workflow is described in
[`benchmark/README.md`](../benchmark/README.md), and the repeated
gfaidx/gbz-base interval benchmark in
[`docs/gfaidx_gbz_interval_benchmark.md`](../docs/gfaidx_gbz_interval_benchmark.md).

## Converting and preparing GFA files

| Script | What it does |
|---|---|
| `w_to_p.py`, `w_to_p.awk` | Convert GFA 1.1 `W` lines to GFA 1.0 `P` lines, for tools such as ODGI that do not read `W` lines (see below). |
| `filter_orphan_links.sh` | Drop `L` lines whose ends have no `S` line. Usage: `filter_orphan_links.sh in.gfa > out.gfa`. |
| `compare_gfa_nodes.sh` | Compare the `S`-line node names of two GFA files using disk-backed `sort`. Usage: `compare_gfa_nodes.sh a.gfa b.gfa`. |
| `gfa_to_metis.py` | Convert a GFA to METIS `.graph` format (undirected, no self-loops), with disk-backed node numbering. |

### `w_to_p.py` and `w_to_p.awk`

The converted path name keeps the `W` line's sample, haplotype, sequence and
interval:

```text
W  GRCh38  0  chr1  1830045  1840123  ...
P  GRCh38#0#chr1:1830045-1840123  ...
```

They also set the header to `VN:Z:1.0`, turn `>`/`<` orientations into
`+`/`-`, keep optional `W` tags, and copy all other lines unchanged. A `W`
line whose coordinates are both `*` becomes `sample#haplotype#sequence`
without an interval.

```bash
# Recommended for large chromosome-scale walks
python3 scripts/w_to_p.py input.gfa output.gfa

# Also write every W identity and the P name it became
python3 scripts/w_to_p.py input.gfa output.gfa --mapping-out w_to_p.tsv

# Portable AWK version for smaller files
awk -f scripts/w_to_p.awk input.gfa > output.gfa
```

`-` reads stdin or writes stdout in the Python version. gfaidx reads the
`:start-end` suffix of a `P` name as that path's coordinates (see
[coordinates.md](../docs/coordinates.md)). ODGI does not: it numbers every
imported path from 0.

## Inspecting and repairing indexes

| Script | What it does |
|---|---|
| `build_lnx.py` | Build the `.lnx` node-length file for an index made by an early gfaidx version without one. Usage: `build_lnx.py graph.gfa.gz`. |
| `plot_chunk_node_distribution.py` | Plot how many nodes each community holds, from a `.ndx`. |
| `pdx_size_breakdown.py` | Print the size of each section of a `.pdx` file, per step and per posting (`--tsv` for a table). |
| `check_pdx_path_loops.py` | Report whether one indexed path visits any node more than once. Usage: `check_pdx_path_loops.py graph.gfa.gz.pdx CHM13#0#chr1`. |

## Benchmarking and testing

| Script | What it does |
|---|---|
| `measure_tree_rss.py` | Run a command and record its wall time and peak memory, including child processes such as `sort`. |
| `pdx_ab_benchmark.py` | Run the same queries against two gfaidx binaries and indexes, check that the outputs are identical, and report warm and cold-cache latency. |
| `sim_pangenome.py` | Simulate a Minigraph-Cactus-like GFA with bubbles, inversions and copy-number loops. Used by the tests. Usage: `sim_pangenome.py <sites> <haplotypes> [seed] > graph.gfa`. |
| `rename_gfa_nodes.py` | Rename every segment with shuffled integers or non-numeric names, to test node ordering. Used by the tests. |
| `pdx_step_delta_estimate.py` | Estimate the `.pdx` step-table size under different node orders. Needs numpy and a `.pdx` built with `--step_encoding packed32`. |
| `pdx_step_bitpack_estimate.py` | Estimate the step-table size with fixed-width bit packing. Reads `.pdx` files in the older 32-bit-per-step layout (v4 or `packed32`). |
| `create_random_paths.py` | Write 200 random `P` lines of 100,000 steps over a GFA's nodes, as path-indexing stress data. Usage: `create_random_paths.py graph.gfa paths.txt`. |
| `generate_random_nodes.py` | Write random node names with community IDs plus a query list, from early tests of the node index. |
