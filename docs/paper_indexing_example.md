# Worked example: indexing and querying a GFA graph

This document gives a complete small example that can be adapted into the
first panel of a gfaidx paper figure. It follows the current path-index format,
`.pdx` version 4. The byte counts and query outputs were verified with gfaidx
2.0.0.

Coordinates in this document are 0-based and half-open. For example,
`chr1:106-114` includes bases 106 through 113.

The example distinguishes three kinds of quantities:

- **Stable format values**, such as the 96-byte `.pdx` header, the 4-byte
  packed step width, and the formula used to locate a checkpoint.
- **Values calculated from this input**, such as node ranks, path step ranges,
  cumulative lengths, and posting blocks.
- **Partition- and compression-dependent values**, such as numeric Louvain
  community labels and compressed gzip member sizes. The values shown below
  are from the verified 2.0.0 run. A different gfaidx version, gzip setting, or
  equivalent relabeling of the Louvain communities can change these values
  without changing the query algorithm.

In formulas, a **path-local step rank** is the zero-based position inside one
P/W record. A **global step index** is the position in the flat `.pdx` step
table. A **node rank** is the position of a node's 16-byte record in the sorted
`.ndx` array. These are three different integer spaces.

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

Save the following as `paper_example.gfa`. The fields below are separated by
literal tab characters, as required by GFA:

```gfa
H	VN:Z:1.1	RS:Z:REF
S	A	AAAA
S	B	CCC
S	C	GGGGG
S	D	TT
S	E	AAAA
S	F	CCCCC
S	G	GG
L	A	+	B	+	0M
L	B	+	C	+	0M
L	C	+	D	+	0M
L	D	+	E	+	0M
L	B	+	F	+	0M
L	F	+	D	+	0M
L	B	+	G	+	0M
L	G	+	C	-	0M
L	C	-	C	+	0M
L	C	-	D	+	0M
W	REF	0	chr1	100	118	>A>B>C>D>E
W	HAP1	0	chr1	100	118	>A>B>F>D>E
P	HAP2	A+,B+,G+,C-,C+,D+,E+	*
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

### 2.1 What `index_gfa` does, in order

The graph and sidecars share ranks and community assignments, so they are not
built as unrelated files. The top-level construction order is:

1. **Scan links and assign temporary integer node IDs.** gfaidx reads every
   `L` record, assigns an integer ID to each endpoint name on first sight,
   and writes an undirected integer edge pair. Each pair is normalized as
   `(min_id,max_id)`; GFA endpoint orientations are irrelevant to community
   detection but remain in the original `L` record for later output.
2. **Sort and deduplicate the edge list.** The external `sort` command orders
   the normalized pairs numerically and removes duplicate topology pairs. The
   sorted text is converted to the binary graph consumed by Louvain.
3. **Partition the topology.** Louvain local-moving and graph-contraction
   levels run until no further improvement is found or the implementation's
   iteration bound is reached. Nodes absent from every `L` record are found
   in a later `S`-line scan and placed in a singleton community. Optional
   `--max_chunk_nodes` refinement and `--min_chunk_nodes` coarsening occur
   before any final index uses the partition.
4. **Split graph records.** gfaidx scans the input again. An `S` record goes
   to its node's community. An `L` record whose endpoints share a community
   goes to that community; a cross-community `L` goes to the final shared
   member. `H` records go to community 0. `P` and `W` records are not
   copied into these graph members; the separate path-index scan handles them.
5. **Compress and index members.** Every nonempty temporary community file is
   deflated as one complete gzip member and appended to the output stream.
   Before and after each append, `tellp()` gives the compressed start and end,
   so `gz_size = end - gz_offset`. These values form `.idx`.
6. **Build aligned sidecars.** The final node-to-community mapping creates
   `.ndx`; an `S`-line scan creates rank-aligned `.lnx`; two further GFA
   scans plus an external posting sort create `.pdx`; finally `.pdx` and
   `.lnx` are scanned to create `.pcx`.
7. **Publish complete files.** gfaidx builds the graph and sidecars under
   staged sibling names. It renames the sidecars first and the indexed
   `.gfa.gz` last, so the visible graph path is the signal that the complete
   bundle was successfully produced.

`index_coordinates` is separate because users may choose only particular
reference samples or paths. For this command, it reads the selected REF walk
from the already built `.pdx`, reads segment lengths from the indexed GFA,
resolves each segment through the same `.ndx` ranks, and writes `.cdx`.

## 3. Graph communities and gzip members

For this example, Louvain partitioning produced:

```text
community 0: A, B, C, G
community 1: D, E, F
shared-link member 2: links whose endpoints are in different communities
```

The uncompressed logical content of the members is:

```gfa
# Member 0
H    VN:Z:1.1    RS:Z:REF
S    A    AAAA
S    B    CCC
S    C    GGGGG
S    G    GG
L    A    +    B    +    0M
L    B    +    C    +    0M
L    B    +    G    +    0M
L    G    +    C    -    0M
L    C    -    C    +    0M

# Member 1
S    D    TT
S    E    AAAA
S    F    CCCCC
L    D    +    E    +    0M
L    F    +    D    +    0M

# Member 2: shared links
L    C    +    D    +    0M
L    B    +    F    +    0M
L    C    -    D    +    0M
```

The `.idx` file maps each member to its compressed byte range:

```text
#community_id    gz_offset    gz_size
0    0    92
1    92    57
2    149    44
```

These exact compressed sizes can change with compression settings. The
important fields are the member offset and compressed size. They let gfaidx
seek to and inflate one member without inflating every earlier member.

For example, member 1 is the half-open compressed byte interval
`[92,149)`, because `92 + 57 = 149`. To read it, gfaidx:

1. opens the multi-member gzip as a binary file;
2. seeks directly to byte 92;
3. reads at most 57 compressed bytes, in 64 KiB input blocks;
4. initializes zlib with `inflateInit2(15 + 16)`, which expects a gzip
   wrapper;
5. inflates into 64 KiB output blocks and yields complete newline-delimited
   GFA records to the query;
6. stops when the requested compressed range is exhausted.

The range reader can also handle more than one gzip member in a supplied
range: after `Z_STREAM_END`, it reinitializes zlib and continues with any
unconsumed input bytes. Normal `.idx` records describe one member each.

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
| 0    | C    | 0         |
| 1    | B    | 0         |
| 2    | A    | 0         |
| 3    | G    | 0         |
| 4    | F    | 1         |
| 5    | E    | 1         |
| 6    | D    | 1         |

The ranks do not follow the order of the `S` lines. They follow the sorted
hash order in `.ndx`.

To look up a name such as `B`, gfaidx does not scan strings:

1. It calculates both the 64-bit and 32-bit FNV-1a hashes of the bytes in
   `"B"`.
2. It binary-searches the memory-mapped 16-byte records by the 64-bit hash.
3. If that hash is present, it walks left to the first equal 64-bit hash and
   scans the equal-hash run for the 32-bit hash.
4. The matching record's array position is rank 1, and its final `uint32`
   gives community 0.

The second hash makes an accidental collision much less likely, but `.ndx`
does not retain the original node string; a collision in both hashes cannot be
distinguished at lookup time. During index construction, duplicate resolved
ranks are rejected when `.lnx` and `.pdx` are aligned.

`.ndx` has no header. Its file size must be a multiple of 16, and the number
of records is therefore:

```text
112 bytes / 16 bytes per node = 7 nodes
```

`.lnx` starts with a 24-byte header containing magic `GFALNX01`, version 1,
value width 4, and node count 7. It then stores one rank-aligned `uint32`
length per node. The example length array is:

```text
rank:    0  1  2  3  4  5  6
node:    C  B  A  G  F  E  D
length:  5  3  4  2  5  4  2
```

Thus, `length[1] = 3` gives the length of node `B`. Its byte address in the
file is:

```text
24-byte header + rank(1) * 4 bytes = byte offset 28
```

`.lnx` is memory-mapped; gfaidx does not parse the complete array into a
separate in-memory structure. Accessing `length[rank]` is one bounds check and
one rank-addressed mapped load. The operating system brings in the containing
memory page on demand.

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

The section offsets are calculated, not searched for:

```text
path_table_offset    = sizeof(header)
                     = 96

node_table_offset    = 96 + path_count(3) * path_record_size(128)
                     = 480

step_table_offset    = 480 + node_count(7) * node_record_size(32)
                     = 704

posting_table_offset = 704 + step_count(17) * step_record_size(4)
                     = 772

strings_offset       = 772 + compressed_posting_bytes(49)
                     = 821

file_size            = 821 + strings_size(64)
                     = 885
```

When a `PathIndexReader` opens this file, it reads and validates the header,
then eagerly reads the three path records and their referenced metadata
strings. It builds an in-memory path-name-to-ID map because path names are
queried repeatedly. In contrast, node records, node names, step slices, and
posting blocks remain on disk and are read on demand. Queried node metadata
and names are cached by rank.

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

The header also makes cross-section validation possible before a query uses an
offset. For example, version 4 must use 4-byte steps, node ranks must fit in
the low 31 bits of a packed step, and the posting byte extent is
`strings_offset - posting_table_offset = 49`.

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

Path-local step `s` of path `p` is stored at global step index:

```text
global_step = path[p].step_begin + s
```

For example, local step 1 of HAP2 is `B`. HAP2 begins at global step 10, so:

```text
global_step = 10 + 1 = 11
step byte    = step_table_offset + global_step * 4
             = 704 + 11 * 4
             = 748
```

Reading HAP2 local steps 1 through 5 is therefore one 20-byte read from
`[748,768)`. The path table supplies both numbers needed for this direct
address; gfaidx does not scan the preceding REF and HAP1 steps.

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

A node name lookup first reads its 32-byte node record, then uses:

```text
name byte range =
    [strings_offset + name_offset,
     strings_offset + name_offset + name_length)
```

For rank 1, node `B`, this is
`[821 + 1, 821 + 1 + 1) = [822,823)`. Path metadata strings use the same
formula with offsets from the 128-byte path record.

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

On read, gfaidx applies:

```text
node_rank = packed & 0x7fffffff
reverse   = (packed & 0x80000000) != 0
```

`read_steps()` performs one allocation and one contiguous read for a
requested slice. The streaming `for_each_step()` variant uses the same byte
formula but reads at most (2^{20}) packed records per chunk, so memory does
not grow with a chromosome-scale path.

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

More precisely, querying rank `r` performs these accesses:

1. Read the rank's node record at
   `node_table_offset + r * 32`.
2. Read the next rank's node record to obtain the exclusive end of the byte
   block. For the final rank, use the total posting-table byte size instead.
3. Read exactly
   `[posting_table_offset + posting_begin,`
   `posting_table_offset + next_posting_begin)`.
4. Decode until exactly `posting_count` occurrences have been emitted, then
   require the byte cursor to equal the end of the block.

For node `B`, rank 1:

```text
B node record offset       = 480 + 1 * 32 = 512
next node record offset    = 480 + 2 * 32 = 544
B relative posting range  = [7,16)
B absolute .pdx byte range = [772 + 7, 772 + 16)
                           = [779,788), 9 bytes
decoded occurrences        = 3
```

The two 32-byte metadata reads are cached after the first query. Only the
9-byte compressed B block is decoded for B; blocks for unrelated nodes are not
touched.

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

The codec is unsigned little-endian base-128. Each byte contributes seven
payload bits; bit 7 says that another byte follows:

```text
300 decimal = 0b1_0010_1100
low 7 bits  = 0b0101100 = 44; set continuation bit -> 0xAC
remaining   = 2                                  -> 0x02
```

For B's block `00 01 01 01 01 01 01 01 01`, decoding proceeds as:

```text
current_path initially 0

00: path delta 0  -> path 0
01: group count 1
01: first step 1  -> emit (0,1)

01: path delta 1  -> path 1
01: group count 1
01: first step 1  -> emit (1,1)

01: path delta 1  -> path 2
01: group count 1
01: first step 1  -> emit (2,1)
```

The decoder rejects an empty group, a zero step delta, arithmetic overflow,
too many decoded postings, truncated varints, or trailing bytes. Thus the
fixed `posting_count` in the node record acts as a decoded-length check even
though the block itself is variable width.

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

The complete example `.cdx` is 239 bytes:

| Section | Offset | Size | Contents |
| ------- | -----: | ---: | -------- |
| Header | 0 | 72 B | magic/version, 7 nodes, 1 track, 5 entries, section offsets |
| Track table | 72 | 80 B | REF/chr1 metadata and entry range |
| Entry table | 152 | 80 B | 5 fixed 16-byte `(start,node_rank)` records |
| String blob | 232 | 7 B | `REFchr1` |

The reader eagerly loads the small track table and name blob, but not the entry
table. During binary search, one entry at a time is read with:

```text
entry byte = entry_table_offset + entry_index * 16
```

After finding the lower and upper step bounds, it performs one contiguous read
for the matching entry range. Section 9.1 works through the comparisons and
byte range for `[106,114)`.

### Node lengths: `.lnx`

`.lnx` supplies the length of every rank. It lets gfaidx calculate path
coordinates without scanning all `S` lines in the indexed GFA. Orientation
does not change length: both `C-` and `C+` read `length[rank(C)] = 5`.

### Path coordinate checkpoints: `.pcx`

`.pcx` is a memory-mapped, fixed-width acceleration index. It stores a
cumulative path length at every N-step boundary. It does not store a node name,
node rank, file seek position, or pointer into the gzip graph. The associated
`.pdx` path ID and a path-local step boundary are enough to address it.

#### 7.1 Building the checkpoints

The builder first opens the completed `.pdx` and rank-aligned `.lnx`, checks
that their node counts agree, and scans each path's packed steps in order. For
one path:

```text
cumulative = 0
write checkpoint for boundary step 0: value 0

for each path-local step s:
    read packed step s from .pdx
    unpack its node rank
    read length[node_rank] from .lnx
    cumulative += length[node_rank]
    if (s + 1) is divisible by stride:
        write cumulative as the checkpoint for boundary s + 1
```

A checkpoint value is therefore the number of bases **before** the
checkpointed step. A terminal checkpoint may also represent the exclusive
boundary after the last step when the path length is an exact multiple of the
stride.

With `--checkpoint_steps 2`, the example checkpoints are:

| Path | Step boundaries | Cumulative lengths |
| ---- | --------------- | ------------------ |
| REF  | `0, 2, 4`       | `0, 7, 14`        |
| HAP1 | `0, 2, 4`       | `0, 7, 14`        |
| HAP2 | `0, 2, 4, 6`    | `0, 7, 14, 21`    |

For example, the `HAP2` value 14 is the length before step 4:

```text
A(4) + B(3) + G(2) + C-(5) = 14
```

The number of stored values for a path is:

```text
checkpoint_count = floor(step_count / stride) + 1
```

Thus the paths with 5, 5, and 7 steps store 3, 3, and 4 values, respectively.

#### 7.2 Complete `.pcx` layout for this example

The 232-byte file has three fixed-width sections:

| Section | Offset | Size | Contents |
| ------- | -----: | ---: | -------- |
| Header | 0 | 80 B | magic `GFAPCX01`, version, counts, stride, compatibility hash, offsets |
| Per-path table | 80 | 72 B | 3 records of 24 bytes |
| Checkpoint table | 152 | 80 B | 10 contiguous `uint64` values |

Each per-path record is:

```text
uint64 step_count
uint64 checkpoint_begin
uint64 checkpoint_count
```

For the example:

| Path ID | Steps | `checkpoint_begin` | Count | Flat checkpoint indices |
| ------: | ----: | -------------------: | ----: | ----------------------- |
| 0 REF | 5 | 0 | 3 | 0, 1, 2 |
| 1 HAP1 | 5 | 3 | 3 | 3, 4, 5 |
| 2 HAP2 | 7 | 6 | 4 | 6, 7, 8, 9 |

The flat value table is:

```text
index:  0  1   2   3  4   5   6  7   8   9
value:  0  7  14   0  7  14   0  7  14  21
path:   REF-------  HAP1------  HAP2----------
```

The header also records `.pdx` path count, node count, total step count, and
a 64-bit hash of path layout metadata. On open, gfaidx verifies those values,
the `.lnx` node count, every path's step count, and every checkpoint slice.
The layout hash includes path type, step count, W coordinates, and path/sample/
sequence names. It deliberately does not hash the full step table, because
doing so would require the expensive full-path read that checkpoints avoid.

If `.pcx` is absent or fails validation, coordinate output remains correct:
gfaidx warns and falls back to scanning path prefixes from step zero using
`.pdx` and `.lnx`.

#### 7.3 How gfaidx finds one checkpoint

For path ID `p`, requested path-local boundary `b`, and stride `N`:

```text
checkpoint_index       = floor(b / N)
checkpoint_step        = checkpoint_index * N
absolute_checkpoint    = path[p].checkpoint_begin + checkpoint_index
prefix_at_checkpoint   = checkpoint_values[absolute_checkpoint]
```

No binary search is needed because the stride is regular and every path has a
contiguous checkpoint slice.

For the start of the HAP2 regional run, `p = 2`, `b = 1`, and `N = 2`:

```text
checkpoint_index     = floor(1 / 2) = 0
checkpoint_step      = 0 * 2 = 0
absolute_checkpoint  = checkpoint_begin(HAP2) 6 + 0 = 6
prefix_at_checkpoint = checkpoint_values[6] = 0
```

Boundary 1 is one step after that checkpoint. gfaidx asks `.pdx` for HAP2
local steps `[0,1)`. Using HAP2's `step_begin = 10`, that is global step 10
at:

```text
.pdx byte = step_table_offset 704 + global_step 10 * 4
          = 744
```

The packed value is rank 2, node A. gfaidx then reads `.lnx[2]` at
`24 + 2 * 4 = 32`, obtains length 4, and adds it:

```text
prefix before HAP2 step 1 = checkpoint value 0 + length(A) 4 = 4
```

For the exclusive end boundary of that run, `b = 6`:

```text
checkpoint_index     = floor(6 / 2) = 3
checkpoint_step      = 3 * 2 = 6
absolute_checkpoint  = 6 + 3 = 9
prefix_at_checkpoint = checkpoint_values[9] = 21
```

The checkpoint lies exactly on boundary 6, so no remainder steps or lengths
are read. HAP2 has no external W offset, giving the final P interval
`[4,21)`.

In general, the remainder is `b mod N`, so recovering one boundary reads at
most `N - 1` packed `.pdx` steps and the same number of rank-addressed
`.lnx` values.

#### 7.4 Short-run and large-run coordinate paths

The formatter has two checkpoint-backed strategies:

- For an output run shorter than two checkpoint strides, it finds the nearest
  checkpoint at or before `start_step`, then makes one forward step-table
  scan from that checkpoint through the run's exclusive end. Prefix steps
  contribute only lengths; selected steps also go into the output step vector.
  This has low call overhead for short runs.
- For a run at least two strides long, it recovers the start and end boundaries
  independently with the formula above. Each endpoint needs fewer than N
  remainder steps. It then streams the selected `.pdx` steps directly into
  the final P/W record; the selected interval does not need one `.lnx` access
  per step or an intermediate step vector.

With stride 2, the five-step HAP2 regional run uses the second strategy because
`floor(5 / 2) >= 2`. The three-step REF and HAP1 runs use the first strategy.
With the default stride 4096, the crossover is an output run of at least 8192
steps. The stride of 2 is used here only to make all calculations visible.

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

Opening the indexed graph first loads the small text `.idx` span table,
memory-maps `.ndx`, and opens `.pdx`. Because the optional files exist, it
also opens `.cdx` metadata; `.lnx` and `.pcx` are deferred until
coordinate-bearing paths actually need them.

The current BFS then does the following:

1. Sort and deduplicate the seed names. The queue initially contains `A`.
2. Inflate shared-link member 2, compressed range `[149,193)`, once for this
   query. Every `L` record is inserted into a query-local, orientation-agnostic
   adjacency map in both directions. This makes cross-community neighbors
   discoverable before their local community has been loaded.
3. Pop `A`, add it to the selected set, and hash-lookup `A` in the
   memory-mapped `.ndx`. The sorted record position is rank 2 and the record
   says community 0.
4. Use `.idx[0] = [0,92)` to seek to and inflate community member 0. For BFS,
   only its `L` records are parsed. Each local link is added in both
   directions; the original `+`/`-` orientations remain in the gzip member
   for output but do not affect neighborhood reachability.
5. Read `adjacency[A]`, discover `B`, and append it to the FIFO queue.
6. Pop and select `B`. Its community is already loaded. The selection now
   contains two nodes, so the next loop test reaches `max_nodes = 2` and BFS
   ends.

The final names are resolved to ranks and distinct community IDs for output:

```text
A -> rank 2, community 0
B -> rank 1, community 0
```

Graph materialization is a second, filtering pass over original records:

1. Read the beginning of member 0 and stop after finding the `H` header.
2. Re-read member 0, emitting only selected `S` records and `L` records
   whose two endpoint names are selected.
3. Re-read shared member 2 and apply the same two-endpoint filter. None of its
   links has both endpoints in `{A,B}`, so it emits nothing.

The extra pass is intentional: BFS stores only query-local adjacency strings,
whereas materialization replays the original GFA lines and therefore preserves
sequences, orientations, overlaps, and tags exactly.

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

The corresponding `.pdx` accesses are:

```text
B rank 1 node record: byte 480 + 1*32 = 512
B posting block:      [779,788), 9 bytes

A rank 2 node record: byte 480 + 2*32 = 544
A posting block:      [788,797), 9 bytes
```

Each decoded posting is appended to a vector keyed by path ID. gfaidx then
sorts path IDs, sorts the step ranks within each path, and scans for adjacent
integers.

Within each path, steps 0 and 1 are consecutive, so each becomes one subpath
run:

```text
(path 0, start_step 0, step_count 2)
(path 1, start_step 0, step_count 2)
(path 2, start_step 0, step_count 2)
```

For a general node set, gaps split a path into separate runs. One-node runs
are suppressed.

For example, selected step ranks `0,1,4,5` would become two runs
`[0,2)` and `[4,6)`; a lone rank 8 would be discarded. Repeated occurrences
are retained by the posting index, so the grouping is based on occurrences,
not merely on unique node names.

### 8.3 Calculate coordinates

All three two-step runs use the short-run checkpoint strategy described in
Section 7.4. For REF:

```text
path ID                 = 0
start_step              = 0
exclusive end_step      = 0 + 2 = 2
nearest start checkpoint = boundary 0, prefix 0

.pdx step read          = global steps [0,2)
                        = bytes [704,712)
unpacked ranks          = 2,1
.lnx lengths            = length[2]=4, length[1]=3

local start             = 0
local end               = 0 + 4 + 3 = 7
```

HAP1 begins at global step 5, so its selected two steps are read from
`[704 + 5*4, 704 + 7*4) = [724,732)`. HAP2 begins at global step 10, so its
slice is `[744,752)`. Both unpack to ranks 2 and 1 and therefore accumulate
the same local interval `[0,7)`.

For the two `W` runs, add the original W coordinate base:

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

Finally, the selected-name lookup converts ranks 2 and 1 back to the already
owned strings `A` and `B`, and the orientation bit from each packed step
produces `>A>B` for W or `A+,B+` for P. Path metadata supplies the W sample,
haplotype, sequence name, and tags.

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
