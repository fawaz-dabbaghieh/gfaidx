# Worked example: indexing and querying a GFA graph

This document gives a complete small example that can be adapted into the
first panel of a gfaidx paper figure. It follows the current path-index format,
`.pdx` version 4. The byte counts and query outputs were verified with gfaidx
1.8.3.

Coordinates in this document are 0-based and half-open. For example,
`chr1:106-114` includes bases 106 through 113.

## Panel-level summary

The example can be drawn as four connected blocks:

```text
1. Input GFA                 2. On-disk index

graph topology               graph.indexed.gfa.gz
reference W walk      --->   .idx  community byte ranges
other W/P haplotypes         .ndx  node -> rank/community
                              .lnx  rank -> node length
                              .pdx  paths, steps, postings
                              .pcx  path coordinate checkpoints
                              .cdx  reference coordinates

3. Query                     4. Output

node A, max_nodes=2   --->   nodes A,B plus edge A->B
                              matching P/W subpaths

chr1:106-114          --->   reference nodes B,C,D
--all_haplotypes              B-C-D, B-F-D,
                              B-G-C(-)-C(+)-D
```

The indexed GFA remains a valid multi-member gzip file. Graph records are
stored in independently compressed community members. `P` and `W` records are
stored in `.pdx` instead of being copied into the indexed gzip.

## 1. Example input graph

Save the following as `paper_example.gfa`:

```gfa
H    VN:Z:1.1    RS:Z:REF
S    A    AAAA
S    B    CCC
S    C    GGGGG
S    D    TT
S    E    AAAA
S    F    CCCCC
S    G    GG
L    A    +    B    +    0M
L    B    +    C    +    0M
L    C    +    D    +    0M
L    D    +    E    +    0M
L    B    +    F    +    0M
L    F    +    D    +    0M
L    B    +    G    +    0M
L    G    +    C    -    0M
L    C    -    C    +    0M
L    C    -    D    +    0M
W    REF    0    chr1    100    118    >A>B>C>D>E
W    HAP1    0    chr1    100    118    >A>B>F>D>E
P    HAP2    A+,B+,G+,C-,C+,D+,E+    *
```

The node lengths are:

| Node | Sequence | Length |
| ---- | -------- | ------:|
| A    | `AAAA`   | 4      |
| B    | `CCC`    | 3      |
| C    | `GGGGG`  | 5      |
| D    | `TT`     | 2      |
| E    | `AAAA`   | 4      |
| F    | `CCCCC`  | 5      |
| G    | `GG`     | 2      |

The three indexed paths are:

| Source record           | Steps             | Interpretation                                                  |
| ----------------------- | ----------------- | --------------------------------------------------------------- |
| `W REF 0 chr1 100 118`  | `A B C D E`       | Reference walk                                                  |
| `W HAP1 0 chr1 100 118` | `A B F D E`       | Replaces `C` with `F`                                           |
| `P HAP2`                | `A B G C- C+ D E` | Inserts `G`, then traverses an inverted and forward copy of `C` |

All three paths begin with `A,B` and end with `D,E`. The middle of each path
differs:

```text
REF:   A -> B -> C -------------> D -> E
HAP1:  A -> B -------> F -------> D -> E
HAP2:  A -> B -> G -> C(-) -> C(+) -> D -> E
```

The two `W` walks span 18 bases:

```text
4 + 3 + 5 + 2 + 4 = 18
```

Therefore, their declared interval is `[100,118)`. `HAP2` is a `P` path, so
its path-local coordinates begin at zero. Its total length is 25 bases.

## 2. Build the indexes

For this small example, a checkpoint every two path steps makes the `.pcx`
contents easy to show:

```bash
gfaidx index_gfa \
    paper_example.gfa \
    paper_example.indexed.gfa.gz \
    --checkpoint_steps 2

gfaidx index_coordinates \
    paper_example.indexed.gfa.gz \
    paper_example.indexed.gfa.gz.cdx \
    --reference REF
```

The first command writes:

```text
paper_example.indexed.gfa.gz
paper_example.indexed.gfa.gz.idx
paper_example.indexed.gfa.gz.ndx
paper_example.indexed.gfa.gz.lnx
paper_example.indexed.gfa.gz.pdx
paper_example.indexed.gfa.gz.pcx
```

The second command adds:

```text
paper_example.indexed.gfa.gz.cdx
```

## 3. Graph communities and gzip members

For this example, Louvain partitioning produced:

```text
community 0: D, E, F
community 1: A, B, C, G
shared-link member 2: links whose endpoints are in different communities
```

The uncompressed logical content of the members is:

```gfa
# Member 0
H    VN:Z:1.1    RS:Z:REF
S    D    TT
S    E    AAAA
S    F    CCCCC
L    D    +    E    +    0M
L    F    +    D    +    0M

# Member 1
S    A    AAAA
S    B    CCC
S    C    GGGGG
S    G    GG
L    A    +    B    +    0M
L    B    +    C    +    0M
L    B    +    G    +    0M
L    G    +    C    -    0M
L    C    -    C    +    0M

# Member 2: shared links
L    C    +    D    +    0M
L    B    +    F    +    0M
L    C    -    D    +    0M
```

The `.idx` file maps each member to its compressed byte range:

```text
#community_id    gz_offset    gz_size
0    0    75
1    75    74
2    149    44
```

These exact compressed sizes can change with compression settings. The
important fields are the member offset and compressed size. They let gfaidx
seek to and inflate one member without inflating every earlier member.

The `P` and `W` records are not present in these members. They are represented
by `.pdx`.

## 4. Shared rank space: `.ndx` and `.lnx`

### `.ndx` contains one fixed 16-byte record per node:

```text
uint64 FNV-1a hash
uint32 second FNV-1a hash
uint32 community_id
```

Records are sorted by the two hashes. The position of a record in this sorted
array is its node rank. `.pdx`, `.lnx`, and `.cdx` use the same ranks.

For this example:

| Rank | Node | Community |
| ----:| ---- | ---------:|
| 0    | C    | 1         |
| 1    | B    | 1         |
| 2    | A    | 1         |
| 3    | G    | 1         |
| 4    | F    | 0         |
| 5    | E    | 0         |
| 6    | D    | 0         |

The ranks do not follow the order of the `S` lines. They follow the sorted
hash order in `.ndx`.



`.lnx` starts with a 24-byte header and then stores one rank-aligned `uint32`
length per node. The example length array is:

```text
rank:    0  1  2  3  4  5  6
node:    C  B  A  G  F  E  D
length:  5  3  4  2  5  4  2
```

Thus, `length[1] = 3` gives the length of node `B`. `.lnx` is memory-mapped;
gfaidx does not parse the complete array into a separate in-memory structure.

## 5. Complete `.pdx` layout for the example

The current `.pdx` is one binary file with six sections:

| Section       | Offset | Size  | Representation                                |
| ------------- | ------:| -----:| --------------------------------------------- |
| Header        | 0      | 96 B  | Fixed width, not compressed                   |
| Path table    | 96     | 384 B | 3 fixed 128-byte records, not compressed      |
| Node table    | 480    | 224 B | 7 fixed 32-byte records, not compressed       |
| Step table    | 704    | 68 B  | **Bit-packed**, 4 bytes per step              |
| Posting table | 772    | 49 B  | **Delta encoded and variable-length encoded** |
| String blob   | 821    | 64 B  | Raw concatenated bytes, not compressed        |
| End of file   | 885    |       |                                               |

The `.pdx` file itself is not gzip-compressed. Compression is applied only to
the posting table, while the step table uses fixed-width bit packing.

### 5.1 Header

The 96-byte header contains:

```text
magic                 GFPATH1\0
version               4
reserved              0
path_count            3
node_count            7
step_count            17
posting_count         17
path_table_offset     96
node_table_offset     480
step_table_offset     704
posting_table_offset  772
strings_offset        821
strings_size          64
```

There is one posting for every path-step occurrence, so this example has 17
steps and 17 postings.

### 5.2 Path table

Each path record is fixed at 128 bytes. It stores its type, string offsets,
step range, and `P`- or `W`-specific metadata.

| Path ID | Type | Internal lookup name      | `step_begin` | `step_count` | Other metadata                                       |
| -------:| ---- | ------------------------- | ------------:| ------------:| ---------------------------------------------------- |
| 0       | W    | `REF\|0\|chr1\|100\|118`  | 0            | 5            | sample=`REF`, hap=0, seq=`chr1`, start=100, end=118  |
| 1       | W    | `HAP1\|0\|chr1\|100\|118` | 5            | 5            | sample=`HAP1`, hap=0, seq=`chr1`, start=100, end=118 |
| 2       | P    | `HAP2`                    | 10           | 7            | overlap=`*`, W fields unused                         |

`W` records do not contain a single path-name field in GFA. gfaidx constructs
the internal key:

```text
sample|haplotype|sequence|start|end
```

The original fields remain separately available, so reconstructing the `W`
line does not depend on parsing this key.

### 5.3 String blob

Names, overlap fields, tags, sample names, and sequence names share one raw
string blob:

```text
ABCDEFGREF|0|chr1|100|118REFchr1HAP1|0|chr1|100|118HAP1chr1HAP2*
```

Selected offsets are:

| Value                     | Offset | Length |
| ------------------------- | ------:| ------:|
| node `A`                  | 0      | 1      |
| node `B`                  | 1      | 1      |
| node `C`                  | 2      | 1      |
| `REF\|0\|chr1\|100\|118`  | 7      | 18     |
| sample `REF`              | 25     | 3      |
| sequence `chr1`           | 28     | 4      |
| `HAP1\|0\|chr1\|100\|118` | 32     | 19     |
| sample `HAP1`             | 51     | 4      |
| second `chr1`             | 55     | 4      |
| path `HAP2`               | 59     | 4      |
| overlap `*`               | 63     | 1      |

The blob avoids storing fixed-size character arrays, but it is not otherwise
compressed or deduplicated. For example, `chr1` occurs twice.

### 5.4 Packed step table

Every path step is one `uint32`:

```text
bits 0..30: node rank
bit 31:     reverse orientation
```

This is fixed-width bit packing, not varint or delta encoding. It reduces the
step record from an earlier two-field, 8-byte representation to 4 bytes while
keeping direct random access by step rank.

| Path ID | GFA steps         | Node ranks      | Packed values                                                    |
| -------:| ----------------- | --------------- | ---------------------------------------------------------------- |
| 0       | `A B C D E`       | `2,1,0,6,5`     | `00000002 00000001 00000000 00000006 00000005`                   |
| 1       | `A B F D E`       | `2,1,4,6,5`     | `00000002 00000001 00000004 00000006 00000005`                   |
| 2       | `A B G C- C+ D E` | `2,1,3,0,0,6,5` | `00000002 00000001 00000003 80000000 00000000 00000006 00000005` |

The reverse traversal of rank 0 is `0x80000000`; the forward traversal of the
same node is `0x00000000`.

### 5.5 Node table

The node table points from each node rank to its name and compressed posting
block:

```text
uint64 name_offset
uint64 name_length
uint64 posting_begin
uint64 posting_count
```

`posting_begin` is a byte offset relative to the start of the compressed
posting table. `posting_count` is the decoded number of `(path_id, step_rank)`
occurrences.

| Rank | Node | Name offset | Posting byte range | Decoded postings |
| ----:| ---- | -----------:| ------------------ | ----------------:|
| 0    | C    | 2           | `[0,7)`            | 3                |
| 1    | B    | 1           | `[7,16)`           | 3                |
| 2    | A    | 0           | `[16,25)`          | 3                |
| 3    | G    | 6           | `[25,28)`          | 1                |
| 4    | F    | 5           | `[28,31)`          | 1                |
| 5    | E    | 4           | `[31,40)`          | 3                |
| 6    | D    | 3           | `[40,49)`          | 3                |

This table gives node-level random access to postings. Looking up node `C`
requires reading only bytes 0 through 6 of the posting table.

### 5.6 Compressed posting table

A posting is the occurrence:

```text
(node_rank, path_id, step_rank)
```

Before compression, postings are sorted by:

```text
node_rank, then path_id, then step_rank
```

Each node gets one independent compressed block. Within a node block, postings
are grouped by path. A path group is encoded as unsigned varints:

```text
delta(path_id from previous group)
number of occurrences in this path
first absolute step_rank
delta(step_rank) for every later occurrence in the same path
```

The first path ID in a node block is stored as its absolute ID. All later path
IDs are deltas.

The complete posting table for this example is:

| Node | Decoded `(path,step)` postings | Varint integers before byte encoding | Hex bytes                    |
| ---- | ------------------------------ | ------------------------------------ | ---------------------------- |
| C    | `(0,2), (2,3), (2,4)`          | `0,1,2 \| 2,2,3,1`                   | `00 01 02 02 02 03 01`       |
| B    | `(0,1), (1,1), (2,1)`          | `0,1,1 \| 1,1,1 \| 1,1,1`            | `00 01 01 01 01 01 01 01 01` |
| A    | `(0,0), (1,0), (2,0)`          | `0,1,0 \| 1,1,0 \| 1,1,0`            | `00 01 00 01 01 00 01 01 00` |
| G    | `(2,2)`                        | `2,1,2`                              | `02 01 02`                   |
| F    | `(1,2)`                        | `1,1,2`                              | `01 01 02`                   |
| E    | `(0,4), (1,4), (2,6)`          | `0,1,4 \| 1,1,4 \| 1,1,6`            | `00 01 04 01 01 04 01 01 06` |
| D    | `(0,3), (1,3), (2,5)`          | `0,1,3 \| 1,1,3 \| 1,1,5`            | `00 01 03 01 01 03 01 01 05` |

Node `C` shows both delta types:

1. Path 0 contributes one occurrence at step 2: `0,1,2`.
2. The next path is path 2, so its path delta is `2 - 0 = 2`.
3. Path 2 contains `C` twice, at steps 3 and 4.
4. This group is encoded as `2,2,3,1`: path delta 2, count 2, first step
   3, then step delta `4 - 3 = 1`.

All integers in this example are below 128, so each occupies one byte. Larger
values use continuation bytes. For example, unsigned value 300 is encoded as:

```text
AC 02
```

The 17 postings would occupy 136 bytes as fixed pairs of two `uint32` values.
They occupy 49 bytes in this small example. This ratio is illustrative; the
ratio on a real graph depends on path IDs, step distances, and repeated-node
patterns.

## 6. Which path-index construction stages use disk?

The path index is built in two GFA scans plus an external sort.

### Pass 1: node names and ranks

gfaidx scans all `S` lines and resolves each node through `.ndx`.

Kept in memory:

```text
node name -> rank lookup map
node metadata records
node names in the string blob
seen-node bit vector
```

The much larger path step and posting collections are not built in memory in
this pass.

### Pass 2: path steps and unsorted postings

gfaidx scans the `P` and `W` lines.

For every path occurrence it produces:

```text
packed step:     uint32(node_rank | reverse_bit)
temporary post: (uint32 node_rank, uint32 path_id, uint32 step_rank)
```

The packed steps are streamed directly, in path order, to:

```text
tmp_steps.bin
```

They are not sorted because the path table already records the starting step
and number of steps for each path.

Temporary postings are accumulated in a bounded 64 MiB vector. A temporary
posting is 12 bytes, so one chunk holds about 5.59 million records. When the
buffer is full:

1. The chunk is sorted in memory by node, path, and step.
2. The sorted chunk is written to `posting_run_N.bin`.
3. The in-memory buffer is cleared and reused.

This is the external-sort spill stage.

### Bounded k-way merge

If there are more than 128 posting runs, gfaidx merges them in groups of at
most 128:

```text
posting_run_0.bin  \
posting_run_1.bin   \
...                  > min-heap k-way merge -> posting_merge_pass_0_0.bin
posting_run_127.bin /
```

Only the current posting from each input run is held in the merge heap. After
an intermediate merged run is complete, its input run files are deleted.
Merge passes continue until no more than 128 final runs remain.

The final k-way merge streams postings in global node/path/step order. gfaidx
holds the postings for one node, encodes that node's delta-varint block, and
writes it directly to:

```text
tmp_posting_blob.bin
```

At this point the node table receives that block's byte offset and decoded
posting count.

The k-way sort and merge applies only to the posting table. The path-first step
table is already in its required order and does not go through this sort.

### Final assembly

The final `.pdx` is assembled in this order:

```text
header
path records
node records
copy tmp_steps.bin
copy tmp_posting_blob.bin
string blob
```

It is first written to a staged sibling file. The completed file is atomically
renamed to the requested `.pdx` path. Temporary runs and blobs are removed
unless temporary-file retention was explicitly requested.

For a large graph, the major disk-backed construction data are therefore:

```text
packed step file
sorted posting run files
intermediate k-way merge files, when needed
compressed posting blob
staged final .pdx
```

Path metadata, node metadata, and the string blob are still assembled in
memory. The node-name-to-rank map is also kept in memory during the `P/W` scan.

## 7. Coordinate sidecars used with `.pdx`

### Reference coordinate index: `.cdx`

Only the `REF` walk is selected because the header contains `RS:Z:REF`.
The `.cdx` track is:

```text
source type:      W
reference:        REF
sequence:         chr1
haplotype:        0
sequence range:   [100,118)
entry range:      5 entries
```

The rank-aligned entries are:

| Step | Start | End derived from next start | Node | Rank |
| ----:| -----:| ---------------------------:| ---- | ----:|
| 0    | 100   | 104                         | A    | 2    |
| 1    | 104   | 107                         | B    | 1    |
| 2    | 107   | 112                         | C    | 0    |
| 3    | 112   | 114                         | D    | 6    |
| 4    | 114   | 118, from track end         | E    | 5    |

The end of an entry is not stored directly. It is the next entry's start, or
the track's `sequence_end` for the last entry.

`.cdx` uses binary search over these sorted starts to isolate entries
overlapping a region. It is a fixed-width coordinate index, not part of `.pdx`.

### Node lengths: `.lnx`

`.lnx` supplies the length of every rank. It lets gfaidx calculate path
coordinates without scanning all `S` lines in the indexed GFA.

### Path coordinate checkpoints: `.pcx`

`.pcx` stores cumulative path length every N steps. With
`--checkpoint_steps 2`, the example checkpoints are:

| Path | Checkpointed cumulative lengths |
| ---- | ------------------------------- |
| REF  | `0, 7, 14`                      |
| HAP1 | `0, 7, 14`                      |
| HAP2 | `0, 7, 14, 21`                  |

For example, the `HAP2` value 14 is the length before step 4:

```text
A(4) + B(3) + G(2) + C-(5) = 14
```

At query time gfaidx begins at the nearest checkpoint and scans fewer than N
remaining prefix steps. The default stride is 4096; the stride of 2 is used
here only to make the example visible.

## 8. Node-centered subgraph query

Run:

```bash
gfaidx get_subgraph \
    paper_example.indexed.gfa.gz \
    A \
    node_A.gfa \
    --max_nodes 2 \
    --with_coords
```

### 8.1 Find and load the graph chunk

1. `.ndx` maps node `A` to rank 2 and community 1.
2. `.idx` gives the compressed byte range for community member 1.
3. gfaidx inflates member 1 and reads its `S/L` records.
4. The shared-link member is consulted for cross-community links.
5. BFS starts at `A`, discovers `B`, and reaches the two-node cap.

The selected graph is:

```gfa
S    A    AAAA
S    B    CCC
L    A    +    B    +    0M
```

### 8.2 Find matching path runs

The selected node ranks are:

```text
A -> 2
B -> 1
```

gfaidx reads only the posting blocks for ranks 2 and 1:

```text
A: (path 0, step 0), (path 1, step 0), (path 2, step 0)
B: (path 0, step 1), (path 1, step 1), (path 2, step 1)
```

Within each path, steps 0 and 1 are consecutive, so each becomes one subpath
run:

```text
(path 0, start_step 0, step_count 2)
(path 1, start_step 0, step_count 2)
(path 2, start_step 0, step_count 2)
```

For a general node set, gaps split a path into separate runs. One-node runs
are suppressed.

### 8.3 Calculate coordinates

For the two `W` runs:

```text
start = W SeqStart + prefix before step 0
      = 100 + 0
      = 100

end   = start + length(A) + length(B)
      = 100 + 4 + 3
      = 107
```

For the `P` run, path-local coordinates begin at zero:

```text
start = 0
end   = 4 + 3 = 7
```

Coordinate-bearing `P` output assumes the original path has no overlaps. The
example uses `*`, so the assumption holds.

The complete output is:

```gfa
H    VN:Z:1.1    RS:Z:REF
S    A    AAAA
S    B    CCC
L    A    +    B    +    0M
W    REF    0    chr1    100    107    >A>B
W    HAP1    0    chr1    100    107    >A>B
P    HAP2:0-7    A+,B+    *
```

For `W`, the original `SeqId` is retained and the concrete `SeqStart/SeqEnd`
identify the subwalk. For `P`, the path-local interval is appended to the path
name.

## 9. Coordinate query with all path-supported haplotypes

Run:

```bash
gfaidx get_region \
    paper_example.indexed.gfa.gz \
    chr1:106-114 \
    region.gfa \
    --reference REF \
    --all_haplotypes \
    --with_coords
```

### 9.1 Reference lookup in `.cdx`

The query `[106,114)` overlaps:

```text
B [104,107)
C [107,112)
D [112,114)
```

It does not overlap `A [100,104)` or `E [114,118)`. `.cdx` therefore returns:

```text
nodes: B, C, D
ranks: 1, 0, 6
exact REF run: path 0, steps 1 through 3
```

The exact reference occurrence comes from the coordinate track. This matters
when a reference path visits the same node more than once.

### 9.2 Use `.pdx` postings as an inverted index

gfaidx reads the three node posting blocks:

```text
B: (0,1), (1,1), (2,1)
C: (0,2), (2,3), (2,4)
D: (0,3), (1,3), (2,5)
```

This is 9 decoded postings across 3 paths.

For the coordinate-source path, gfaidx preserves the exact `.cdx` interval:

```text
REF: path 0, steps 1..3 -> B,C,D
```

For every other path, it keeps the sequence between the minimum and maximum
anchor step:

```text
HAP1:
  B occurs at step 1
  D occurs at step 3
  selected steps 1..3 -> B,F,D

HAP2:
  B occurs at step 1
  C occurs at steps 3 and 4
  D occurs at step 5
  selected steps 1..5 -> B,G,C-,C+,D
```

This min/max rule retains inserted, inverted, or duplicated sequence between
the outer anchors. It can widen an interval when a non-reference path repeats
an anchor far away. The reference path is protected from that widening because
its exact coordinate-selected step range is kept.

The union contains five nodes:

```text
B, C, D, F, G
```

With `--all_haplotypes`, this is an exact path-supported selection; BFS is not
run. Without `--all_haplotypes`, `B,C,D` become multi-source BFS seeds and
`--max_nodes` controls graph expansion instead.

### 9.3 Materialize graph records

The selected ranks map through `.ndx` to both graph communities:

```text
community 0: D,F
community 1: B,C,G
```

gfaidx inflates these community members and the shared-link member, then emits
only `S` records in the selected set and `L` records whose two endpoints are
selected.

### 9.4 Calculate the three path intervals

Reference:

```text
prefix before B = length(A) = 4
start = 100 + 4 = 104
run length = B(3) + C(5) + D(2) = 10
end = 104 + 10 = 114
```

HAP1:

```text
prefix before B = length(A) = 4
start = 100 + 4 = 104
run length = B(3) + F(5) + D(2) = 10
end = 114
```

HAP2:

```text
prefix before B = length(A) = 4
start = 4
run length = B(3) + G(2) + C-(5) + C+(5) + D(2) = 17
end = 4 + 17 = 21
```

The emitted path records are:

```gfa
W    REF    0    chr1    104    114    >B>C>D
W    HAP1    0    chr1    104    114    >B>F>D
P    HAP2:4-21    B+,G+,C-,C+,D+    *
```

This example shows the different roles of the path-index sections:

```text
path table       identifies the three P/W records
posting table    finds their step occurrences from B,C,D
step table       reads the selected path intervals
string blob      restores node and path names
.lnx             converts node ranks to lengths
.pcx             avoids scanning each path prefix from step zero
.cdx             finds the exact reference steps for chr1:106-114
.ndx + .idx      locate and materialize the needed graph communities
```

## 10. Query-time disk access and memory

The complete `.pdx` is not loaded into RAM for a query:

- The small path metadata table is loaded because path names and ranges are
  used repeatedly.
- Node metadata records are read lazily and cached only when their ranks are
  queried.
- A selected node's compressed posting byte block is read and decoded on
  demand.
- Requested step ranges are read directly from the packed step table.
- `.ndx`, `.lnx`, and `.pcx` are memory-mapped and paged by the operating
  system.
- The small text `.idx` span table is loaded to resolve community byte ranges.
- Selected gzip community members are inflated for graph materialization.
  Member 0 may also be touched to recover the `H` header when it is not one of
  the selected communities.
- The current implementation scans the shared-link member during BFS adjacency
  loading and again filters it during graph materialization.

This is why gfaidx can query an indexed graph without constructing the full
graph or the full path index as in-memory C++ objects.

## 11. Short figure-caption version

> gfaidx partitions GFA segments into Louvain communities and writes each
> community as an independently compressed member of a standard multi-member
> gzip file. The `.idx` and `.ndx` sidecars map nodes to compressed graph
> ranges, while `.pdx` stores packed path steps and a compressed node-to-path
> posting index. In this example, a coordinate lookup maps
> `REF:chr1:106-114` to reference nodes `B,C,D`. Their `.pdx` postings recover
> the reference interval `B-C-D` and the path-supported alternatives `B-F-D`
> and `B-G-C(-)-C(+)-D`. The selected graph records are read from the needed
> community and shared-link gzip members, while `.lnx/.pcx` provide the node
> lengths and cumulative path coordinates needed to emit coordinate-bearing
> subpaths.
