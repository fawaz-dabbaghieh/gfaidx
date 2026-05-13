# Genomic Coordinate Query Ideas For `gfaidx`

This file is a design note for future work on genomic coordinate queries in
`gfaidx`. It is meant to help a new Codex session quickly recover the current
state of the codebase, what has already been implemented, and what the next
design and implementation steps should be.

The main goal of the new feature is:

- allow a user to query a genomic interval such as `chr22:10000-100000`
- get the set of nodes overlapping that interval
- map those nodes to communities
- retrieve the induced subgraph around those nodes from the chunked graph index
- retrieve the `P`/`W` subpaths that pass through that resulting subgraph
- make the result easy to visualize in an external graph viewer

This should work for:

- rGFA graphs that encode stable coordinates on segments using `SN`/`SO`/`SR`
- GFA graphs with `W` lines that encode haplotype or walk coordinates using
  `SampleId`, `HapIndex`, `SeqId`, `SeqStart`, `SeqEnd`


## Current State Of The Repository

At the time of writing, `gfaidx` has 4 main CLI subcommands:

- `index_gfa`
- `get_chunk`
- `index_paths`
- `get_path`

### Current chunking/indexing pipeline

`index_gfa`:

- reads a GFA
- builds an edge list
- performs community detection
- writes a multi-member gzip file where each member is one community
- writes:
  - `.idx` for `community_id -> gzip offset/size`
  - `.ndx` for `node string id -> community_id`

`get_chunk`:

- given a `community_id` or `node_id`
- resolves to a community
- streams the corresponding community chunk from the multi-member gzip

### Current path indexing pipeline

`index_paths`:

- indexes `P` and `W` lines into a `.pdx`
- requires `--ndx`
- uses the sorted rank of the matched `.ndx` entry as the canonical `node_id`
  inside `.pdx`

This matters because node-based path queries no longer need to build a giant
global `node string -> int id` map in RAM. Instead:

- input node names are resolved through `.ndx`
- the `.ndx` rank becomes the `.pdx` node id

`get_path` currently supports:

- exact path retrieval by path id
- exact `W` retrieval by structured fields
- node-set or subgraph-based subpath retrieval
- optional exact `W` subwalk coordinates using `--with_walk_coords` and
  `--source_gfa`

### Current `.pdx` design

The current `.pdx` stores:

- path metadata
- node metadata
- packed step table
- compressed per-node postings
- string blob

Important implementation notes:

- step records are packed to 4 bytes
- node postings are compressed per node using delta encoding plus varints
- path metadata is loaded eagerly
- node metadata and node names are loaded lazily
- node-based lookup uses `.ndx`, not an eagerly built node-name hash map

This existing work is the key reason a coordinate query feature is now realistic:
most of the heavy graph/path primitives are already present.


## What The New Feature Should Do

The desired user workflow is not just "return a walk slice".

Instead, the user wants:

1. give an interval such as `chr22:10000-100000`
2. identify the nodes overlapping that interval
3. find which communities those nodes belong to
4. retrieve the graph context around that interval by loading those communities
5. retrieve all the `P`/`W` subpaths that go through the resulting subgraph
6. inspect or visualize that local graph neighborhood

This is closer to "region query over a pangenome graph plus haplotype context"
than to plain path slicing.

The important distinction is:

- the exact interval hit gives a seed node set
- the user-visible graph is often the induced subgraph after expanding to the
  communities containing those seed nodes

These are related but not identical.


## Two Coordinate Models Need To Be Supported

There are two different but complementary coordinate systems.

### 1. `W`-based haplotype/walk coordinates

`W` lines already carry:

- `SampleId`
- `HapIndex`
- `SeqId`
- `SeqStart`
- `SeqEnd`
- the walk itself

This is ideal for queries like:

- `HG002 hap 1 chr22:10000-100000`

What it means:

- find the relevant `W` interval(s)
- find the steps in the requested sub-interval
- return the nodes for that exact walk slice

This is haplotype-aware and should be the primary mechanism for
sample/haplotype-specific interval search.

### 2. rGFA stable coordinates on segments

rGFA segments can encode stable/reference coordinates using:

- `SN`
- `SO`
- `SR`

This is ideal for queries like:

- `chr22:10000-100000`

when the intent is:

- "find graph segments that overlap this stable reference interval"

This is not exactly the same as a haplotype walk query:

- it can identify segments overlapping a reference interval
- but it does not by itself define one single haplotype walk

So both query types should be supported, but they should probably remain
conceptually distinct.


## Recommended High-Level Design

Do not try to force coordinate queries into the existing `.pdx` only.

Instead:

- keep `.pdx` focused on path-first and node-first path indexing
- add a separate coordinate side-index, suggested extension: `.rdx`

The purpose of `.rdx` would be:

- map genomic intervals to `node_id`s or to `(path_id, step interval)` seeds

The rest of the query pipeline can then reuse:

- `.ndx` for community lookup by canonical node id
- `.idx` and the gzip graph for community streaming
- `.pdx` for subpaths/subwalks through the final node set


## Why `.rdx` Should Be Separate

The current `.pdx` is optimized for:

- exact full-path retrieval
- node-set to subpath lookup

Coordinate queries are different:

- they are interval queries
- they are walk-first or stable-coordinate-first
- they need efficient overlap search, not just exact lookup

Trying to store interval acceleration directly in `.pdx` would make the path
index more complicated and less focused.

A side-index is cleaner:

- `.pdx` remains the path index
- `.rdx` becomes the coordinate/region index


## Proposed `.rdx` Contents

The best design is probably one file with two logical indexes.

### A. Stable segment interval index for rGFA

One record per `S` line that has usable stable-coordinate tags.

Suggested record contents:

- `stable_seq_id` or an interned integer key for `SN`
- `start` = `SO`
- `end` = `SO + segment_length`
- `node_id`
- optional `stable_rank` = `SR`

Sorted by:

- `(stable_seq_id, start, end, node_id)`

Purpose:

- resolve a stable coordinate query like `chr22:10000-100000`
- return all `node_id`s whose stable intervals overlap the query

This is the seed node set for reference-coordinate search.

### B. Walk interval index for `W` lines

One record per `W` line:

- `sample_id`
- `hap_index`
- `seq_id`
- `seq_start`
- `seq_end`
- `path_id`

Sorted by:

- `(sample_id, hap_index, seq_id, seq_start)`

Purpose:

- quickly find which `W` records overlap a queried interval

This alone is not enough to identify exact steps inside the walk interval, so a
second structure is needed.

### C. Sparse walk checkpoints

For each indexed `W`, store sparse checkpoints along the walk, for example every
16 kb, 32 kb, or 64 kb.

A checkpoint record could contain:

- `path_id`
- `coord_offset_from_seq_start`
- `step_rank`

Purpose:

- jump near the requested coordinate range inside a long walk
- then linearly scan a short suffix of the walk to find exact boundaries

This avoids full O(length of chromosome walk) scans for every interval query.


## Query Types To Support

There should probably be a new subcommand instead of overloading `get_path`.

Suggested new subcommands:

- `index_regions`
- `get_region`

### `index_regions`

Inputs:

- GFA
- `.ndx`
- maybe `.pdx`, depending on implementation choice

Outputs:

- `.rdx`

Possible CLI:

```bash
gfaidx index_regions graph.gfa graph.regions.rdx --ndx graph.gfa.gz.ndx --pdx graph.paths.pdx
```

### `get_region`

Should support at least these modes.

#### Mode 1: stable-coordinate query

Examples:

- `chr22:10000-100000`
- `--stable chr22:10000-100000`

Meaning:

- use the rGFA stable-segment index
- return seed nodes overlapping that stable interval

#### Mode 2: walk/haplotype interval query

Examples:

- `--sample HG002 --hap 1 chr22:10000-100000`
- `HG002/1/chr22:10000-100000`

Meaning:

- use the `W` interval index
- return seed nodes along that exact walk interval


## Expected Output Of A Region Query

The region-query result should conceptually include multiple layers:

### 1. Exact interval hit

This is the most precise answer:

- exact seed nodes overlapping the requested interval
- exact walk slice(s), if the query was `W`-based

### 2. Expanded graph context

This is what the user likely wants to visualize:

- all communities containing those seed nodes
- the induced subgraph obtained by streaming those communities

### 3. Paths/walks through the expanded graph

Once the final node set is known:

- use `.pdx` to return all `P`/`W` subpaths that pass through it

This lets the user see:

- the query locus
- other haplotypes and paths that run near it

This is one of the main use cases motivating the feature.


## Recommended Query Pipeline

### Pipeline for stable-coordinate queries

1. Parse the interval, e.g. `chr22:10000-100000`
2. Search the stable segment interval index in `.rdx`
3. Collect overlapping `node_id`s
4. These nodes become the seed node set
5. Map each `node_id` to a `community_id`
6. Deduplicate communities
7. Stream those communities from the chunked graph
8. Build the final node set of the induced subgraph
9. Use `.pdx` to retrieve all subpaths/subwalks through that final node set
10. Emit:
   - seed nodes
   - context communities
   - subgraph
   - subpaths/subwalks

### Pipeline for `W`-based haplotype queries

1. Parse `(sample, hap, seq_id, start, end)`
2. Search the walk interval table in `.rdx`
3. Identify overlapping `W` records
4. Use sparse checkpoints to jump near the query start
5. Read the relevant walk steps from `.pdx`
6. Compute exact step boundaries using node lengths
7. Collect the nodes in that exact walk slice
8. These nodes become the seed node set
9. Map them to communities
10. Stream the communities
11. Build the induced subgraph
12. Use `.pdx` to get the subpaths/subwalks through that final subgraph


## What Existing Indexes Can Be Reused

### Reuse `.ndx`

This is very important.

Because `.pdx` node IDs are now aligned to `.ndx` entry rank:

- `node_id` in `.pdx` is the same as the rank in `.ndx`

This means `.ndx` can be reused for:

- `node string -> canonical node_id` via binary search on hash
- `canonical node_id -> community_id` via direct access by rank

This is one of the most useful consequences of the recent `.pdx` refactor.

Suggested improvement:

- expose a direct API on `NodeHashIndex` for rank-based community lookup
- something like `community_for_rank(node_id)` or `entry_at_rank(node_id)`

That will make the region query path cheaper than re-hashing node names.

### Reuse `.pdx`

Use `.pdx` for:

- exact `W` step slicing after an interval search
- subpaths/subwalks through the final node set after subgraph expansion

### Reuse `.idx` plus chunked graph file

Use the existing chunk index to:

- map `community_id -> gzip member offset/size`
- stream the communities making up the local context graph


## Data Model Recommendation

The query system should distinguish between:

- seed nodes
- expanded graph context

This distinction should be explicit in the design and, if possible, the output.

Why:

- seed nodes are the exact locus hit by the coordinate query
- expanded context may be much larger

If these are conflated, the query can feel imprecise or hard to debug.

Suggested conceptual output fields:

- queried interval
- matched mode (`stable` or `walk`)
- seed node count
- seed communities
- expanded node count
- expanded community count
- optional exact walk slice(s)
- final subgraph
- final subpaths/subwalks


## Coordinate Computation For `W` Queries

There is already existing work in `get_path` that recomputes exact subwalk
coordinates using node lengths from the source GFA.

That logic should be reused conceptually:

- segment length is taken from sequence length first
- then from `LN:i:`
- if anything is missing or inconsistent, coordinates are abandoned and a
  warning is produced

For a future region query index:

- node lengths do not necessarily have to live inside `.rdx`
- they can be recomputed from the source GFA during indexing
- or a compact side array of node lengths could be stored once if repeated
  coordinate queries justify it

For repeated region queries, storing lengths once may be worth it.


## One Important Caveat About `P` Lines

Coordinate indexing should primarily target:

- `W` lines
- rGFA stable segment tags on `S` lines

It should not try to infer genomic interval semantics for generic `P` lines by
default, because:

- `P` lines may have overlaps
- `P` lines do not necessarily correspond to stable genomic coordinates
- the coordinate interpretation may be ambiguous or not meaningful

So:

- use `W` for haplotype interval queries
- use rGFA `SN`/`SO`/`SR` for stable/reference interval queries


## Proposed `.rdx` File Layout

This section is still provisional, but the file could look like this.

### Header

- magic
- version
- counts
- offsets to each section

### Interned string tables

For:

- stable sequence names
- sample IDs
- `seq_id`s

### Stable interval records

One per rGFA-annotated segment:

- `stable_seq_key`
- `start`
- `end`
- `node_id`
- optional `stable_rank`

### Walk interval records

One per `W` line:

- `sample_key`
- `hap_index`
- `seq_key`
- `seq_start`
- `seq_end`
- `path_id`
- `checkpoint_begin`
- `checkpoint_count`

### Checkpoint records

One sparse record every fixed span:

- `coord_offset`
- `step_rank`

Potentially stored per path in a contiguous block.


## Query Performance Expectations

### Stable-coordinate query

Likely performance shape:

- binary search to the first overlapping stable interval
- short linear scan across the overlap window
- fast seed node extraction

This should be quite good if the stable interval table is sorted and compact.

### `W`-coordinate query

Without checkpoints:

- O(length of walk) worst case per query

With checkpoints:

- O(log checkpoints + local scan)

This should be good enough even for chromosome-length `W` walks.


## Suggested Incremental Implementation Plan

This should be built in phases.

### Phase 1: region-query prototype without a new `.rdx`

Goal:

- prove the query semantics and expected outputs first

Prototype behavior:

- for `W` queries, scan matching walks from `.pdx`
- compute prefix lengths on the fly
- derive exact step slices
- collect seed nodes
- map seed nodes to communities
- stream those communities
- get final subpaths through the resulting node set

For rGFA stable-coordinate queries:

- scan `S` lines from the original GFA
- parse `SN`/`SO`/`SR`
- find overlapping nodes

This phase is likely slower, but it validates the end-to-end UX.

### Phase 2: build `.rdx`

Add:

- stable segment interval table
- `W` interval table
- sparse checkpoints

Then switch the prototype query path over to `.rdx`.

### Phase 3: optimize node to community expansion

Add direct `.ndx` rank-based community access so the region query path can do:

- seed `node_id`
- direct `community_id = ndx[node_id].community_id`

without string lookups.

### Phase 4: user-facing output cleanup

Decide how `get_region` should emit:

- node list
- graph GFA
- exact query path slice
- final context subpaths


## Suggested CLI Design

Possible future commands:

### Build region index

```bash
gfaidx index_regions graph.gfa graph.regions.rdx --ndx graph.gfa.gz.ndx --pdx graph.paths.pdx
```

### Query by stable coordinate

```bash
gfaidx get_region graph.gfa.gz graph.paths.pdx graph.regions.rdx \
  --ndx graph.gfa.gz.ndx \
  --region chr22:10000-100000
```

### Query by `W` interval

```bash
gfaidx get_region graph.gfa.gz graph.paths.pdx graph.regions.rdx \
  --ndx graph.gfa.gz.ndx \
  --sample HG002 \
  --hap 1 \
  --region chr22:10000-100000
```

Potential output options:

- `--seed_nodes_out`
- `--subgraph_out`
- `--subpaths_out`
- `--communities_out`


## Open Questions

These are the main unresolved design points.

### 1. Should `get_region` expand by full communities or only induce on the seed node set?

Current thinking:

- full communities are useful for context
- but may be much larger than expected

Possible solution:

- support both:
  - exact seed-induced mode
  - community-expanded context mode

### 2. Should node lengths be stored explicitly in `.rdx`?

Pros:

- avoids rescanning source GFA for lengths
- faster repeated `W` coordinate slicing

Cons:

- larger index

This may depend on query frequency.

### 3. How should rGFA stable-coordinate overlaps be interpreted?

Likely:

- include any segment overlapping the interval

But it may be worth defining whether touching endpoints counts and whether
output should be clipped conceptually or segment-based only.

### 4. How should the result be serialized?

Possibilities:

- plain GFA for the context subgraph
- a TSV/JSON side report with:
  - query interval
  - seed nodes
  - seed communities
  - exact walk slice

This may be helpful for downstream tooling.

### 5. Collision handling

Currently `.ndx` uses:

- 64-bit FNV-1a
- 32-bit FNV-1a

That is likely sufficient for current scale, but it is not collision-proof.

Eventually, a collision-proof side table of original node strings may be needed
if graphs grow much larger or if the project wants stronger guarantees.


## Recommended Next Practical Step

The best immediate next step is probably not to build the full `.rdx` right
away.

Instead:

1. implement a prototype `get_region` for `W` interval queries without a new
   coordinate index
2. make it:
   - compute the exact walk slice
   - collect seed nodes
   - expand to communities
   - output subgraph plus subpaths
3. after the semantics feel right, build `.rdx` to accelerate it

Reason:

- it validates the actual user-facing workflow first
- it reduces the risk of optimizing the wrong query semantics


## Short Summary For A Future Session

If a future Codex session needs a fast recap:

- `.pdx` already supports exact path retrieval and node-set subpath retrieval
- `.pdx` node IDs are aligned to `.ndx` ranks
- `.ndx` can be reused as the canonical node identity bridge
- the missing feature is a coordinate side-index
- the best design is a separate `.rdx` with:
  - rGFA stable segment interval records
  - `W` interval records
  - sparse checkpoints per `W`
- region query flow should be:
  - coordinates -> seed nodes
  - seed nodes -> communities
  - communities -> local subgraph
  - local subgraph nodes -> subpaths/subwalks through `.pdx`
- the user wants both:
  - exact interval-hit nodes
  - expanded graph context for visualization

