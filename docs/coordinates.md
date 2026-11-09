# Coordinates in gfaidx

`gfaidx get_region` takes a genomic interval, `sequence:start-end`, and returns
the part of the graph that covers it. Coordinates are 0-based and half-open,
as in GFA `W` lines and BED files: `chr1:100-200` is the 100 bases from 100 to
199.

How gfaidx finds the nodes of an interval depends on how the GFA stores
genomic positions:

| Graph | Where coordinates come from | `.cdx` needed? |
|---|---|---|
| `W` lines (for example Minigraph-Cactus) | `SeqStart`/`SeqEnd` of each walk | no, but faster with one |
| `P` lines (for example PGGB) | the path name's `:start-end` suffix, or 0 | no, but faster with one |
| rGFA (minigraph) | `SN`, `SO` and `SR` tags on `S` lines | yes |

Without a `.cdx`, `get_region` works out positions on a `P` or `W` path from
the `.pdx` and `.lnx` while it runs. A `.cdx`, built by `gfaidx
index_coordinates`, stores the positions of selected reference paths so they
do not have to be recomputed for each query. Build one for the paths you query
often.

To see every coordinate track an index can answer, and whether it uses the
`.cdx` or is computed on the fly:

```bash
gfaidx get_region graph.indexed.gfa.gz --list_coordinates
```

The sections below show the steps for each kind of graph. All examples start
from an index built by:

```bash
gfaidx index_gfa graph.gfa graph.indexed.gfa.gz
```

and write the coordinate index as `graph.indexed.gfa.gz.cdx`, where
`get_region` finds it automatically.

## rGFA with `SN`, `SO`, and `SR` tags

In an rGFA, reference coordinates are stored on `S` lines:

```text
S	node1	ACGT	SN:Z:chr1	SO:i:100	SR:i:0
```

`index_coordinates` automatically selects `SR:i:0` segments and groups them by
their `SN` value. Do not provide `--reference` for this mode:

```bash
gfaidx index_coordinates \
  graph.indexed.gfa.gz \
  graph.indexed.gfa.gz.cdx
```

List the available `SN` coordinate tracks and their indexed bounds:

```bash
gfaidx get_region graph.indexed.gfa.gz --list_coordinates
```

Rows with `source` equal to `S` came from rGFA segment tags. Query using the
exact value in the `sequence` column:

```bash
gfaidx get_region \
  graph.indexed.gfa.gz \
  chr1:1000000-1100000 \
  chr1.region.gfa \
  --mode bfs \
  --max_nodes 100000
```

A pure rGFA does not record complete sample paths. Its reference nodes can seed
a `--mode bfs` extraction, but `--mode all` cannot reconstruct
haplotypes that are not represented by `P` or `W` records. Do not use
`index_gfa --no_paths` for this workflow: `get_region` currently uses the
otherwise empty `.pdx` node table to convert coordinate ranks back to node
names.

## GFA with `P` lines

`P` lines do not have dedicated genomic start and end fields. By default,
gfaidx gives a P path a 0-based path-local coordinate system by summing its
segment lengths. When the path name ends in `:start-end`, such as
`GRCh38#0#chr1:1830045-1840123`, gfaidx instead treats that suffix as the
path's 0-based, half-open interval and queries it through the suffix-free
namespace `GRCh38#0#chr1`. The encoded span must equal the summed segment
lengths. This requires `*` overlaps; paths with overlap CIGARs are rejected
because node lengths alone do not define their coordinates.

Multiple P records may provide adjacent or disjoint fragments of one namespace:

```text
P  GRCh38#0#chr1:100-200  ...
P  GRCh38#0#chr1:200-300  ...
```

A query such as `GRCh38#0#chr1:150-250` uses both fragments. Overlapping
fragments with the same base namespace are rejected as ambiguous.

First list the indexed paths:

```bash
gfaidx get_path graph.indexed.gfa.gz --print_path_names > paths.tsv
```

Keep the required `P` rows in a selection file. For example:

```bash
awk -F '\t' \
  '$1 == "P" && ($2 == "CHM13#0#chr1" || $2 == "GRCh38#0#chr1")' \
  paths.tsv > coordinate_paths.tsv
```

Build `.cdx` from that selection:

```bash
gfaidx index_coordinates \
  graph.indexed.gfa.gz \
  graph.indexed.gfa.gz.cdx \
  --path_names_file coordinate_paths.tsv
```

Inspect the indexed ranges and query a selected path by its name:

```bash
gfaidx get_region graph.indexed.gfa.gz --list_coordinates
gfaidx get_region \
  graph.indexed.gfa.gz \
  CHM13#0#chr1:1000000-1100000 \
  chr1.region.gfa
```

Unselected P paths remain queryable on the fly when matching `.pdx` and `.lnx`
files are available, but selecting frequently queried paths for `.cdx` avoids
recomputing their coordinate positions.

## GFA with `W` lines

`W` records already contain a sample, haplotype, sequence name, and concrete
`SeqStart`/`SeqEnd` coordinates. If the GFA header identifies reference samples
with `RS:Z`, coordinate indexing without a selection file indexes those
reference walks. Use `--reference` to select one reference sample:

```bash
gfaidx index_coordinates \
  graph.indexed.gfa.gz \
  graph.indexed.gfa.gz.cdx \
  --reference CHM13
```

To select individual walks instead, print all P/W records and retain the desired
`W` rows:

```bash
gfaidx get_path graph.indexed.gfa.gz --print_path_names > paths.tsv
awk -F '\t' \
  '$1 == "W" && $2 == "CHM13" && $4 == "chr1"' \
  paths.tsv > coordinate_walks.tsv
gfaidx index_coordinates \
  graph.indexed.gfa.gz \
  graph.indexed.gfa.gz.cdx \
  --path_names_file coordinate_walks.tsv
```

List and query the resulting coordinate namespace:

```bash
gfaidx get_region graph.indexed.gfa.gz --list_coordinates
gfaidx get_region \
  graph.indexed.gfa.gz \
  chr1:1000000-1100000 \
  chr1.region.gfa \
  --reference CHM13
```

When only one reference namespace contains the requested sequence,
`--reference` may be omitted from the query.

