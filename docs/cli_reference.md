# gfaidx command reference

This page lists every `gfaidx` command and option. For a short introduction
with examples, see the [README](../README.md). For how the index files are
built and stored, see [how_it_works.md](how_it_works.md). For coordinate
systems in rGFA, `P`-line and `W`-line graphs, see
[coordinates.md](coordinates.md).

Every query command finds its index files from the indexed graph's name, for
example `graph.gfa.gz.ndx` beside `graph.gfa.gz`. Options such as `--ndx` or
`--pdx` are only needed when a file was renamed or moved.

## Contents

- [`gfaidx index_gfa`](#gfaidx-index_gfa)
- [`gfaidx index_coordinates`](#gfaidx-index_coordinates)
- [`gfaidx get_region`](#gfaidx-get_region)
- [`gfaidx get_subgraph`](#gfaidx-get_subgraph)
- [`gfaidx get_path`](#gfaidx-get_path)
- [`gfaidx get_chunk`](#gfaidx-get_chunk)
- [`gfaidx index_paths`](#gfaidx-index_paths)
- [`gfaidx index_path_checkpoints`](#gfaidx-index_path_checkpoints)
- [Build `.lnx` for old indexes](#build-lnx-for-old-indexes)

## `gfaidx index_gfa`

Build the chunked gzip graph plus `.idx`, `.ndx`, `.lnx`, `.params`, and by
default `.pdx` and `.pcx`.

```bash
gfaidx index_gfa <in_gfa> <out_gfa.gz> [options]
```

Arguments:

- `in_gfa`
  input GFA 1 graph (GFA 1.0, 1.1, or 1.2) with at least one `L` line, plain
  or gzipped; GFA 2 input and graphs without links are rejected with an error
- `out_gfa.gz`
  output multi-member gzip graph

Options:

- `--keep_tmp`
  keep temporary files instead of deleting them at the end
- `--tmp_dir <dir>`
  base directory for the run's `gfaidx_tmp_*` directory (default: the input
  GFA's directory). All intermediate files, and the outputs until they are
  complete, are written there; the outputs are then moved to their final
  paths (copied if `--tmp_dir` is on a different filesystem)
- `--progress_every <N>`
  progress logging interval while reading the GFA
- `--gzip_level <1..9>`
  gzip compression level for the final chunked output
- `--gzip_mem_level <1..9>`
  zlib memory level for gzip compression
- `--checkpoint_steps <N>`
  store one cumulative path-coordinate checkpoint every `N` path steps in
  `.pcx`; defaults to `4096`
- `--max_chunk_nodes <N>`
  re-run Louvain inside communities containing at least `N` nodes and prevent
  small-community merges from producing a chunk larger than `N`; `0` disables
  refinement and leaves merging without an upper bound. Louvain levels only
  merge communities, so refinement stops before a level that would create a
  piece larger than `N` and keeps the previous level
- `--min_chunk_nodes <N>`
  merge communities smaller than `N` nodes into the neighboring community with
  the most connecting edges; `0` disables small-community merging
- `--no_paths`
  skip building `<out_gfa.gz>.pdx` and `.pcx`; still write `.gz`, `.idx`,
  `.ndx`, and `.lnx`
The next four options change how the `.ndx` and `.pdx` are encoded. The
defaults give the smallest and fastest index; the other values exist for
comparisons and for indexes that gfaidx 2.1 and older must read.

- `--node_order <auto|legacy_hash>`
  `auto` (default) numbers nodes by community, then by integer segment name
  (or one shared prefix plus digits, such as `s123`) or by first appearance in
  `L` lines, whichever gives smaller estimated path-step deltas on the edge
  list. `legacy_hash` keeps the earlier hash-rank IDs and writes the previous
  `.ndx` and v4 `.pdx` byte for byte, which older gfaidx releases can read
- `--step_encoding <delta|packed32>`
  `.pdx` step table layout; defaults to `delta` (`legacy_hash` forces `packed32`)
- `--posting_encoding <slots|groups>`
  `.pdx` posting layout; defaults to `slots` (`legacy_hash` forces `groups`)
- `--posting_block_nodes <N>`
  node IDs per independently decodable posting block for `slots`; defaults
  to `32`. Larger blocks compress better but make an isolated single-node
  lookup decode more of its block
- `--threads <N>`
  threads for building the path index; defaults to `1`. Long `P`/`W` lines
  have their node names resolved in parallel, posting runs are sorted in the
  background (up to 4 runs of 64 MB in flight), and posting-run merge groups
  run in parallel as far as the soft open-file limit allows. Every thread
  count writes the same files.

  More threads need more temporary disk space. Path indexing writes 12 bytes
  per path step to `--tmp_dir` as sorted 64 MB runs (about 12 GB per billion
  steps). Above 128 runs (about 716 million steps), groups of 128 runs are
  merged into one file of about 8.6 GB each. A group's input runs are deleted
  only once its merged file is complete, so every group being merged needs
  about 8.6 GB on top of the runs. With `--threads N`, up to N groups are
  merged at once, which needs up to (N - 1) x 8.6 GB more than one thread:
  about 26 GB more with 4 threads and 60 GB more with 8, however large the
  graph. For example, on a PGGB chr1 graph with 4.5 billion path steps, the
  runs took 54 GB. With 8 threads, path indexing needed about 45 GB more
  temporary space and took 7.4 minutes instead of 27

Outputs:

- `<out_gfa.gz>`
- `<out_gfa.gz>.idx`
- `<out_gfa.gz>.ndx`
- `<out_gfa.gz>.lnx`
- `<out_gfa.gz>.pdx` unless `--no_paths` is used
- `<out_gfa.gz>.pcx` unless `--no_paths` is used
- `<out_gfa.gz>.params`, a JSON summary of the options and the graph;
  `path_steps` is `null` with `--no_paths`

Example:

```bash
gfaidx index_gfa graph.gfa graph.indexed.gfa.gz --tmp_dir /scratch/tmp
```

## `gfaidx index_coordinates`

Build a standalone `.cdx` aligned to an existing `.ndx`. Reference W records
are selected from the header `RS:Z` sample list. If the supplied GFA is an
indexed gzip that no longer contains W records, `index_coordinates` can read the
reference W metadata and steps from a companion `.pdx`. If no eligible reference
W record exists, `SR:i:0` segments with `SN` and `SO` tags are indexed instead.
Alternatively, provide a filtered `get_path --print_path_names` output file to
index explicit P paths and W walks from the `.pdx`.

```bash
gfaidx index_coordinates <in_gfa> <out.cdx> [options]
```

Options:

- `--ndx <path>`
  node hash index; defaults to `<in_gfa>.ndx` when present
- `--pdx <path>`
  optional path index used as the source of reference W records when they are
  absent from the supplied GFA; defaults to `<in_gfa>.pdx` when present
- `--reference <sample>`
  index only this sample from the header `RS:Z` list; by default all listed
  reference samples are indexed
- `--path_names_file <path>`
  tab-separated file in the same format emitted by
  `gfaidx get_path graph.gfa.gz --print_path_names`; selected `W` rows keep their
  W coordinates. A selected `P` name ending in `:start-end` uses that 0-based,
  half-open interval; other P paths start at 0. P paths with non-`*` overlaps
  are rejected because their coordinate lengths would be ambiguous.
- `--progress_every <N>`
  report input progress every `N` lines; `0` disables progress logging

Example:

```bash
gfaidx index_coordinates chr22.gfa chr22.gfa.gz.cdx \
  --ndx chr22.gfa.gz.ndx --reference CHM13

gfaidx index_coordinates chr22.indexed.gfa.gz chr22.gfa.gz.cdx --reference CHM13

# print_path_names prints all available paths in the indexed graph
gfaidx get_path graph.indexed.gfa.gz --print_path_names > path_names.tsv
# edit or filter path_names.tsv, then index exactly those P/W records
gfaidx index_coordinates graph.indexed.gfa.gz graph.gfa.gz.cdx --path_names_file path_names.tsv
```

## `gfaidx get_region`

Resolve a 0-based, half-open reference interval through `.cdx`, translate its
node ranks through `.pdx`, and either expand a graph neighborhood around the
interval (`--mode bfs` or `--mode reference`) or select the P/W path spans
supported by its nodes (`--mode all`). If `.cdx` is absent
or does not contain the requested coordinate track, `get_region` can fall back
to the resolved `.pdx` and `.lnx` to compute coordinates on the fly for any
indexed `P` path or concrete-coordinate `W` walk.

```bash
gfaidx get_region <in_gz> <sequence:start-end> <out_gfa> [options]
```

Options:

- `--reference <sample>`
  select the coordinate namespace when multiple reference samples contain the
  requested sequence
- `--cdx`, `--idx`, `--ndx`, `--pdx`, `--lnx`, `--pcx`
  override companion indexes; each defaults to `<in_gz>.<suffix>`
- `--max_nodes <N>`
  cap the total seed plus BFS node count; it must be at least the seed count.
  Defaults to `10000`. Only used with `--mode bfs` without `--context`
- `--threads <N>`
  number of workers that read path postings and format `P/W` records; the
  output order is unchanged. Defaults to `1`
- `--context <LIMIT>`
  base-pair context around the interval, with the same rules as gbz-base's
  `query --context`: starting from both sides of every node on the reference
  interval, crossing a node costs its length minus one and following a link
  costs one, and every node with a side reached within `LIMIT` bases joins the
  subgraph. A partially covered first or last node starts its outer side at
  the bases outside the interval. The node set depends only on graph topology,
  so a long variant split across many nodes keeps only the nodes within
  `LIMIT` of either end; its haplotypes are then emitted as separate
  fragments. Used by `--mode reference` (default `100`, gbz-base's default)
  and optionally by `--mode bfs`, where it replaces `--max_nodes`; rejected by
  `--mode all`. `LIMIT` accepts the same suffixes as `--haplotype_gap`.
  `--context 0` keeps only the interval's own nodes
- `--mode {bfs,all,reference}`
  select the extraction strategy. Default is `all`
  - `bfs`: explore a graph neighborhood around the interval, either by BFS up
    to `--max_nodes` or, with `--context`, by base-pair distance. Every
    indexed P/W run inside the resulting node set is emitted; haplotype spans
    are not resolved exactly. With `--context`, single-step runs are kept so
    the walks match gbz-base's subgraph output one for one
  - `all`: avoid BFS and use the `.pdx` posting table to find every indexed
    P/W record containing a reference interval node. For a coordinate-indexed
    P/W source, the exact path-step range returned by the coordinate binary
    search is kept; repeated occurrences of one of its node ids elsewhere on
    that same path do not widen the reference interval. Every other path uses
    the minimum and maximum step containing any reference anchor. All steps
    between those endpoints are retained, including insertions, duplications,
    and inverted sequence. The exact node union and edges whose endpoints are
    both in the union are then materialized. This conservative behavior can
    produce a broad interval when one anchor occurs at distant positions on
    the same haplotype; use `--mode bfs` when path support is ambiguous. This
    mode assumes the graph nodes of interest are covered by indexed P/W
    records; graph-only nodes are not discovered
  - `reference`: extract the `--context` subgraph and emit only the queried
    reference path, matching gbz-base's `query --reference-only`. The node and
    link set is the same one `--mode bfs --context` returns; only the other
    haplotypes' P/W records are skipped, along with their formatting and
    coordinate work. The reference record covers the exact interval extended
    through every adjacent reference step that stays inside the subgraph, as
    gbz-base reports it. Not compatible with `--haplotype_gap`, which only
    affects non-reference paths
- `--all_haplotypes`
  deprecated alias for `--mode all`, kept for compatibility with existing
  scripts. Prints a deprecation warning to stderr; rejected if combined with
  an explicit `--mode` that names a different mode
- `--haplotype_gap <LIMIT>`
  requires `--mode all`. Optionally replace the non-reference minimum/maximum
  span with ODGI-style local anchor clustering. Consecutive anchor occurrences
  stay in one path run while the intervening non-anchor sequence is at most
  `LIMIT` bases; the next anchor starts a new run after the limit is exceeded.
  This can prevent distant repeat hits from pulling most of a PGGB haplotype
  into a small query. The coordinate-selected reference run always remains
  exact. Omitting the option preserves the established conservative behavior
  and its lower overhead. `LIMIT` accepts a bare base count or a
  case-insensitive `bp`, `kb`, `mb`, or `gb` suffix using decimal units; for
  example, `10kb` is 10,000 bases and `0`
  joins only directly adjacent anchors. The option requires
  `--mode all` and a matching `.lnx` node-length index
- `--no_paths`
  omit P/W output; `.pdx` remains required for rank-to-node-name conversion
- `--with_coords`
  emit returned `W` subwalks with concrete `SeqStart`/`SeqEnd` coordinates and
  returned no-overlap `P` subpaths with coordinate names such as
  `CHM13#0#chr1:1830045-1840123`. An existing terminal interval supplies the
  source coordinate base and is replaced by the extracted interval; otherwise
  P coordinates are path-local from 0. The command uses the resolved `.pdx` for
  path metadata, `.lnx` for node lengths, and `.pcx` to start near the requested
  path step instead of scanning from step zero. If `.pcx` is absent, it uses the
  previous bounded path-prefix scan. If `.lnx` is absent, it falls back to
  scanning indexed GFA `S` lines; if validation fails, it falls back to ordinary
  subpath output and logs a warning.
- On-the-fly `.pdx` coordinate lookup
  recognizes a terminal `:start-end` interval on a P name and otherwise starts
  that P path at 0. It handles `W` walks from their stored `SeqStart`. This
  fallback requires `.lnx`, because the path steps in `.pdx` need rank-aligned
  node lengths to derive cumulative coordinates.
- `--list_coordinates`, `--print_path_names`
  print every P/W record available through `.pdx`, plus any `.cdx`-only rGFA
  tracks, then exit. `--print_path_names` is retained as a compatibility alias;
  `--list_coordinates` is clearer when the index contains rGFA segment tracks.
  The output is TSV with columns `source`, `reference`, `haplotype`, `sequence`,
  `start`, `end`, `entries`, and `coordinate_access`. The final column is `cdx`
  for accelerated tracks, `on_the_fly` for paths calculated from `.pdx` and
  `.lnx`, or `unavailable` when the required lengths or W coordinates are
  absent. A missing `.cdx` is not an error when usable P/W records remain.
- `--no_header`
  omit the TSV header when listing coordinate tracks

Example:

```bash
gfaidx get_region chr22.gfa.gz chr22:1500000-2000000 region.gfa \
  --reference CHM13 --mode bfs --max_nodes 100000

gfaidx get_region chr22.gfa.gz chr22:1500000-2000000 haplotypes.gfa \
  --reference CHM13 --mode all

gfaidx get_region chr22.gfa.gz chr22:1500000-2000000 local_haplotypes.gfa \
  --reference CHM13 --mode all --haplotype_gap 10kb

gfaidx get_region chr22.gfa.gz chr22:1500000-2000000 reference_only.gfa \
  --reference CHM13 --mode reference --context 100 --with_coords

gfaidx get_region chr22.gfa.gz chr22:1500000-2000000 context.gfa \
  --reference CHM13 --mode bfs --context 1kb

gfaidx get_region chr22.gfa.gz --list_coordinates
```

Note that `--mode all` is the default, so the second example above also works
with just `--reference CHM13` and no `--mode` at all.

### How `--mode all` preserves paths

Node ids alone do not identify where a node occurs on a path. The coordinate
query therefore keeps the exact source-path steps in addition to the node ids.
For other haplotypes, consider this small example where the requested reference
interval contains `B,C`:

```text
reference:  A B C D E F G H
haplotype:    B G H C
```

The reference contributes the interval `B,C`. The anchor nodes `B,C` select
the complete haplotype interval `B,G,H,C`, so the materialized graph contains
the node union `B,C,G,H`.

Searching that union against the complete reference again would incorrectly
find two reference runs, `B,C` and `G,H`. The second run is only present because
`G,H` were selected through the haplotype. `--mode all` therefore keeps
the path-specific intervals found during anchor matching and emits:

```text
reference:  B C
haplotype:  B G H C
```

The node union is still used to write the graph's `S` and `L` records. The
preserved path intervals are used only for `P` and `W` output. By default, each
matched P/W record produces one output interval, including paths whose selected
interval is reversed relative to the reference. With `--haplotype_gap`, one
matched P/W record can instead produce several coordinate-labelled local runs.

If `B` also occurs later on the reference path, the `.cdx` step range identifies
which occurrence overlaps the requested coordinates. On another haplotype,
all sequence between the outermost matching anchors is retained. For example,
`B Q B X C D E` remains complete when `B` and `E` anchor the interval; gfaidx
does not discard `Q B X` by choosing the shorter of several possible matches.
If `--haplotype_gap 1kb` is supplied and more than 1,000 non-anchor bases lie
between the first and second `B`, the two anchor clusters are emitted as
separate runs. This rule depends only on distance along that haplotype; it does
not attempt to infer a unique collinear alignment through a repeat.

## `gfaidx get_subgraph`

Extract a BFS neighborhood subgraph from the indexed graph, optionally followed
by the matching `P/W` subpaths if a companion `.pdx` is available.

```bash
gfaidx get_subgraph <in_indexed_gz> <start_node> <out_gfa> [options]
```

Arguments:

- `in_gz`
  input indexed gzip graph produced by `index_gfa`
- `start_node`
  node ID to start the BFS neighborhood from
- `out_gfa`
  output file for the extracted GFA subgraph

Index options:

- `--idx <path>`
  override the companion `.idx`; defaults to `<in_gz>.idx`
- `--ndx <path>`
  override the companion `.ndx`; defaults to `<in_gz>.ndx`
- `--pdx <path>`
  override the companion `.pdx`; defaults to `<in_gz>.pdx`
- `--lnx <path>`
  override the optional node-length index used by `--with_coords`; defaults to
  `<in_gz>.lnx`
- `--pcx <path>`
  override the optional path-coordinate checkpoints used by `--with_coords`;
  defaults to `<in_gz>.pcx`

Other options:

- `--max_nodes <N>`
  BFS node cap; defaults to `100`
- `--threads <N>`
  number of workers that format `P/W` records; the output order is unchanged.
  Defaults to `1`
- `--with_coords`
  emit coordinate-bearing `W` subwalks and coordinate-named `P` subpaths
- `--no_paths`
  omit all `P/W` subpaths; this cannot be combined with `--with_coords`

Behavior:

- explicit `--idx`, `--ndx`, and `--pdx` take priority over inferred companion files
- if `.pdx` is present, `get_subgraph` appends the matching `P/W` subpaths for the extracted node set
- if `.pdx` is missing and was not explicitly requested, `get_subgraph` warns and continues with `S/L` output only
- `--with_coords` requires `.pdx`; `.lnx` and `.pcx` accelerate coordinate
  calculation but remain optional for compatibility with older indexes
- coordinate output preserves the original coordinate namespace and emits one
  record for each contiguous selected run along a path
- BFS discovery uses a simple node-level adjacency, but final output re-streams the original chunk lines so emitted `S/L` records preserve the original GFA edge orientations

Examples:

```bash
gfaidx get_subgraph graph.indexed.gfa.gz s12345 neighborhood.gfa
gfaidx get_subgraph graph.indexed.gfa.gz s12345 neighborhood.gfa --max_nodes 2000
gfaidx get_subgraph graph.indexed.gfa.gz s12345 neighborhood.gfa --max_nodes 2000 --with_coords
```

## `gfaidx get_path`

Query an indexed graph's companion `.pdx` index in one of four modes:

1. list indexed `P` path names and `W` walk coordinate identifiers
2. full path lookup by canonical path ID
3. full `W` lookup by structured fields
4. node-set or subgraph lookup returning subpaths/subwalks

```bash
gfaidx get_path <in_gfa> [query mode options]
```

Arguments:

- `in_gfa`
  input indexed GFA graph; `get_path` looks for `<in_gfa>.pdx`

General options:

- `--ndx <path>`
  optional override for the node hash index; for node-set queries, `get_path`
  first tries `<in_gfa>.ndx`
- `--pdx <path>`
  optional override for the path index if the companion `.pdx` was renamed or
  stored elsewhere
- `--lnx <path>`
  optional override for the node-length index used by `--with_walk_coords`;
  defaults to `<in_gfa>.lnx` when present

### Mode 1: print path names

```bash
gfaidx get_path graph.indexed.gfa.gz --print_path_names
```

Output is tab-separated:

- `P <path_name>` for original `P` records
- `W <sample> <hap_index> <seq_id> <seq_start|*> <seq_end|*>` for original
  `W` records

This output can be filtered and supplied to
`gfaidx index_coordinates --path_names_file` to coordinate-index selected paths
or walks.

### Mode 2: exact path ID lookup

```bash
gfaidx get_path graph.indexed.gfa.gz --path_id <id>
```

Options:

- `--path_id <id>`
  canonical path ID

Notes:

- for `P` lines, this is the path name from the GFA
- for `W` lines, the canonical key is `sample|hap|seq_id|start|end`

Example:

```bash
gfaidx get_path graph.indexed.gfa.gz --path_id loopbackP
```

### Mode 3: structured `W` lookup

```bash
gfaidx get_path graph.indexed.gfa.gz --sample <sample> --hap_index <hap> --seq_id <seq> [--seq_start <n|*>] [--seq_end <n|*>]
```

Options:

- `--sample <sample>`
  `W` sample ID
- `--hap_index <hap>`
  `W` haplotype index
- `--seq_id <seq>`
  `W` sequence ID
- `--seq_start <n|*>`
  optional exact start coordinate filter
- `--seq_end <n|*>`
  optional exact end coordinate filter

If multiple `W` lines match and start/end are omitted, the lookup is rejected as ambiguous.

Example:

```bash
gfaidx get_path graph.indexed.gfa.gz --sample HG002 --hap_index 1 --seq_id chr22
```

### Mode 4: node-set or subgraph lookup

Return all `P`/`W` runs that remain contiguous inside the requested node set.

```bash
gfaidx get_path graph.indexed.gfa.gz --nodes 1,2,3
gfaidx get_path graph.indexed.gfa.gz --nodes_file nodes.txt
gfaidx get_path graph.indexed.gfa.gz --subgraph_gfa subgraph.gfa
```

Node query options:

- `--nodes <csv>`
  comma-separated node IDs
- `--nodes_file <path>`
  file with node IDs, one per line or comma-separated per line
- `--subgraph_gfa <path>`
  GFA whose `S` lines define the node set

Coordinate options for path output:

- `--with_walk_coords`
  try to emit exact `SeqStart`/`SeqEnd` for returned `W` subwalks and
  coordinate-bearing names for no-overlap `P` subpaths
- `--lnx <path>`
  node-length sidecar used to avoid scanning all `S` lines; defaults to
  `<in_gfa>.lnx` when present
- `--pcx <path>`
  optional path-coordinate checkpoints used to avoid scanning P/W paths from
  step zero; defaults to `<in_gfa>.pcx` when present
- `--source_gfa <path>`
  original source GFA used as a fallback length source when `.lnx` is absent

Coordinate rules:

- only applies to node-set queries
- affects `W` output coordinates and `P` output names
- `.lnx` stores node-ID-aligned `uint32` segment lengths; fallback scanning takes
  length from the sequence field first, then `LN:i:`
- if anything is missing or inconsistent, `get_path` falls back to ordinary
  subpath output and logs a warning

Example:

```bash
gfaidx get_path graph.indexed.gfa.gz \
  --subgraph_gfa extracted_subgraph.gfa \
  --with_walk_coords \
  --source_gfa graph.gfa
```

If the companion `.ndx` file was renamed or moved, provide it explicitly with `--ndx`.

## `gfaidx get_chunk`

Stream one community member from the indexed gzip graph.

```bash
gfaidx get_chunk <in_gz> [--community_id <id> | --node_id <node>] [options]
```

Arguments:

- `in_gz`
  input indexed gzip graph produced by `index_gfa`

Options:

- `--idx <path>`
  path to the `.idx` file; defaults to `<in_gz>.idx`
- `--ndx <path>`
  path to the `.ndx` file; defaults to `<in_gz>.ndx`
- `--community_id <id>`
  stream this community directly
- `--node_id <node>`
  resolve the node through `.ndx` and stream its community

Notes:

- if `--node_id` is provided, it takes precedence over `--community_id`
- the legacy `--index` and `--node_index` spellings are still accepted
- shared edges are stored in a final extra member so streamed communities remain self-contained

Example:

```bash
gfaidx get_chunk graph.gfa.gz --node_id s12345
```

## `gfaidx index_paths`

Build or rebuild a standalone `.pdx` index for `P` and `W` lines.

```bash
gfaidx index_paths <in_gfa> <out_index.pdx> [options]
```

Arguments:

- `in_gfa`
  input GFA graph
- `out_index.pdx`
  output binary path index

Options:

- `--ndx <path>`
  node hash index produced by `index_gfa`; defaults to `<in_gfa>.ndx` when
  present. Provide this explicitly when the node index was renamed or stored
  elsewhere.
- `--tmp_dir <dir>`
  base directory for temporary files used by the external posting sort; defaults to the output directory
- `--progress_every <N>`
  progress logging interval while reading
- `--step_encoding <delta|packed32>`, `--posting_encoding <slots|groups>`,
  `--posting_block_nodes <N>`, `--threads <N>`
  as for `index_gfa`. With a legacy `.ndx`, `packed32` plus `groups` writes a
  v4 `.pdx`; any other combination writes v5

Notes:

- `index_gfa` already builds `<out_gfa.gz>.pdx` by default
- `index_paths` is mainly useful when you want to rebuild only the path index after path-index code changes
- the `.pdx` must be rebuilt whenever its `.ndx` is rebuilt; a `.pdx` built
  against a different node numbering is rejected at query time

What gets indexed:

- `P` lines
- `W` lines
- node names from `S` lines

Current behavior:

- `P` and `W` are both stored as ordered walks
- steps are stored as zigzag deltas of consecutive node IDs in 128-step blocks
- per-node postings are predicted from the neighboring node IDs of the same
  posting block where possible (see below)
- large posting tables are built through disk-backed sorted runs to reduce peak RAM

Example:

```bash
gfaidx index_paths graph.indexed.gfa.gz graph.indexed.gfa.gz.pdx
```

## `gfaidx index_path_checkpoints`

Build the small `.pcx` acceleration sidecar for an existing `.pdx` and `.lnx`
without rebuilding the graph or path index.

```bash
gfaidx index_path_checkpoints <indexed_gfa> [out_index.pcx] [options]
```

The command infers `<indexed_gfa>.pdx`, `<indexed_gfa>.lnx`, and by default
writes `<indexed_gfa>.pcx`. Use `--pdx`, `--lnx`, or an explicit output path
when the files were renamed.

`--checkpoint_steps <N>` controls the checkpoint interval and defaults to
4096. Each checkpoint is one `uint64` cumulative path length, so the default
sidecar is normally much smaller than `.pdx`. Queries scan at most 4095 path
steps before the requested subpath instead of starting at path step zero.

Checkpoint values are streamed into a visible
`gfaidx_path_checkpoints_tmp_*` directory beside the requested `.pcx`.
`latest_path_checkpoints` points to the active directory. After a successful
atomic rename, or after a handled error, the command removes both. If the
process is interrupted, the remaining directory and symlink make the partial
file easy to find and remove. Progress reports include completed paths, path
steps, checkpoint counts, and elapsed time every 10 paths by default. Use
`--progress_every_paths <N>` to change the interval or `0` to disable periodic
progress.

Example:

```bash
gfaidx index_path_checkpoints graph.indexed.gfa.gz
```

## Build `.lnx` for old indexes

`index_gfa` writes a `.lnx` node-length file. Indexes made by early gfaidx
versions may lack one, and do not need to be rebuilt to get it. Build it from
the indexed graph and its matching `.ndx`:

```bash
python3 scripts/build_lnx.py graph.indexed.gfa.gz
```

Defaults:

- input graph: `graph.gfa.gz`
- node index: `graph.gfa.gz.ndx`
- output: `graph.gfa.gz.lnx`

Use `--ndx`, `--out`, or `--force` if the files were renamed or the output
should be replaced.
