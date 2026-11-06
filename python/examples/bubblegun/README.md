# Bubble detection on an indexed graph

[`bubbles_gfaidx.py`](bubbles_gfaidx.py) finds bubbles (simple bubbles,
insertions and superbubbles) with the algorithm from
[BubbleGun](https://github.com/fawaz-dabbaghieh/bubble_gun), using one of
two graph back ends:

| Mode | Graph access |
| --- | --- |
| `inmemory` | BubbleGun's original approach: read the whole GFA into memory, then try every node as a bubble source. |
| `indexed` | Open a gfaidx index and process it community by community, keeping only a few communities in memory. |

Both modes call the same, unmodified BubbleGun function (`find_sb_alg`) and
write the same TSV, so their outputs can be compared line by line. Only
individual bubbles are reported; BubbleGun's bubble chains are not built.

## Requirements

- BubbleGun (`pip install BubbleGun`, or an editable install of its checkout)
- pygfaidx 2.3.1 or newer, with `IndexedGraph.cross_community_edges()` and
  `get_community(..., include_sequences=False)`

## Usage

```bash
# The original approach. Reads .gfa or .gfa.gz, skipping sequences as
# BubbleGun's --save_memory does.
python bubbles_gfaidx.py inmemory graph.gfa bubbles_inmemory.tsv

# The indexed approach. Takes the .gfa.gz written by gfaidx index_gfa.
python bubbles_gfaidx.py indexed graph.indexed.gfa.gz bubbles_indexed.tsv \
    --cache-size 64 --progress 500

# The two outputs agree once sorted.
diff <(tail -n +2 bubbles_inmemory.tsv | sort) <(tail -n +2 bubbles_indexed.tsv | sort)
```

Only the `.idx` and `.ndx` sidecars are needed, so an index built with
`--no_paths` works too.

The output has one bubble per line:

| Column | Content |
| --- | --- |
| `source`, `sink` | The bubble ends, in BubbleGun's key order (larger id is `sink`) |
| `type` | `simple`, `insertion` or `super` |
| `n_inside` | Number of nodes between the ends |
| `inside` | Those nodes, sorted and comma-separated |

## How the indexed mode works

- **Node lookups.** `find_sb_alg` reads the graph only through
  `graph.nodes[node_id]`. In indexed mode `graph.nodes` is a `dict` holding
  the nodes of the cached communities, so a cached node costs an ordinary
  dict lookup. A missing node calls `__missing__`, which finds the node's
  community with `community_id()` (an mmap hash lookup) and builds that
  community's BubbleGun `Node` objects.
- **Topology only.** Communities are read with
  `get_community(id, include_sequences=False)`. Some PGGB communities hold
  hundreds of megabases, which bubble detection never needs.
- **Cross-community links.** `get_community()` returns only the links inside
  one community. The links between communities are read once at startup with
  `cross_community_edges()`; MC chr1 has 6,654 of them and PGGB chr1 66,981.
- **Cache.** After each search the cache shrinks back to `--cache-size`
  communities, dropping the least recently used. Evictions wait until the
  search ends, because one search can span more communities than the cache
  holds and visits them in no particular order. No search loads a community
  twice.
- **No visited set.** Like BubbleGun itself, every node is tried as a source
  in both directions. Nodes inside a bubble cannot be skipped, because nested
  bubbles start from them. Each bubble is found from both of its ends, and it
  is written only from the end with the smaller id. That makes a global set of
  visited nodes or found bubbles unnecessary.

Memory therefore depends on the cache size and on the largest single search.
Some searches in complex regions cover a few hundred thousand nodes. Time is
mostly spent loading communities, about 10 to 20 ms each, so it depends on
how far the searches from one community reach. On MC they mostly stay close.
On PGGB, searches from the first 1,000 communities touch 2,504 of the 3,366
communities, so a larger cache trades memory for fewer reloads.

## Results: HPRC release 2, chromosome 1

Both graphs were indexed with `--min_chunk_nodes 1000 --max_chunk_nodes 10000`.
Both modes read the same `.indexed.gfa.gz`, one run at a time, on a 20-core
Linux aarch64 machine with Python 3.13. Wall time and peak memory come from
`/usr/bin/time -v`. The indexed mode used the default cache of 64 communities.

| | MC `inmemory` | MC `indexed` | PGGB `inmemory` | PGGB `indexed` |
| --- | --- | --- | --- | --- |
| Nodes | 11,763,396 | | 11,728,931 | |
| Links | 16,216,402 | | 16,557,149 | |
| Communities / cross-community links | | 3,356 / 6,654 | | 3,366 / 66,981 |
| Bubbles | 3,680,188 | identical | 3,558,275 | identical |
| Load graph / open index | 35.0 s | 0.02 s | 49.3 s | 0.18 s |
| Bubble detection | 56.6 s | 144.3 s | 63.1 s | 683.6 s |
| Wall time | 99.6 s | 144.4 s | 122.8 s | 683.9 s |
| Peak memory | 12.11 GiB | 1.09 GiB | 12.15 GiB | 0.52 GiB |
| Community loads | | 5,566 | | 20,193 |

Bubble types are simple / insertion / superbubble: 3,066,604 / 306,972 /
306,612 for MC and 3,030,501 / 264,877 / 262,897 for PGGB.

The cache size trades memory for time. These runs used six processes at once,
so their times are approximate:

| Cache | MC time | MC memory | PGGB time | PGGB memory |
| --- | --- | --- | --- | --- |
| 16 | 155 s | 0.94 GiB | 1,896 s | 0.39 GiB |
| 64 | 154 s | 1.09 GiB | 667 s | 0.52 GiB |
| 256 | 141 s | 1.62 GiB | 500 s | 1.14 GiB |
