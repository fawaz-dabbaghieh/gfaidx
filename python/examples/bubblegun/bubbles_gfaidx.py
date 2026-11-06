#!/usr/bin/env python3
"""
Detect individual bubbles (simple, insertion, super) with BubbleGun's
superbubble algorithm, using one of two graph back ends:

  inmemory  BubbleGun's original approach: read the whole GFA into Node
            objects, then try every node as a bubble source.
  indexed   Stream a gfaidx-indexed graph community by community. Node
            adjacency is materialized on demand through a small LRU cache of
            communities, so memory depends on the cache size and the largest
            single traversal, not on the size of the graph.

Both modes call the same, unmodified BubbleGun function (find_sb_alg) and
write the same TSV, so their outputs can be compared directly.
"""

import argparse
import builtins
import gzip
import resource
import sys
import time
from collections import OrderedDict

from BubbleGun.Node import Node
from BubbleGun.find_bubbles import find_sb_alg

TSV_HEADER = "source\tsink\ttype\tn_inside\tinside\n"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def peak_rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def bubble_type(bubble):
    if bubble.is_simple():
        return "simple"
    if bubble.is_insertion():
        return "insertion"
    return "super"


def bubble_row(bubble):
    # Report the endpoints in BubbleGun's key order so both modes agree.
    sink, source = bubble.key
    inside = sorted(n.id for n in bubble.inside)
    return f"{source}\t{sink}\t{bubble_type(bubble)}\t{len(inside)}\t{','.join(inside)}\n"


def parse_overlap(overlap):
    # Same rule as BubbleGun's read_gfa: "*" means no overlap, else "<n>M".
    return 0 if overlap == "*" else int(overlap[:-1])


def edge_halves(x, x_rev, y, y_rev, overlap):
    """
    Translate one GFA link into BubbleGun's two adjacency entries.

    BubbleGun keeps, per node, the neighbours attached to its start and end
    as (neighbour_id, neighbour_side, overlap) with side 0=start, 1=end.
    This reproduces the four cases in BubbleGun.graph_io.read_gfa.
    """
    return (
        (x, "start" if x_rev else "end", (y, int(y_rev), overlap)),
        (y, "end" if y_rev else "start", (x, int(not x_rev), overlap)),
    )


################################ in-memory mode ################################

def run_inmemory(args, out):
    import BubbleGun.graph_io as graph_io
    from BubbleGun.Graph import Graph
    from BubbleGun.find_bubbles import find_bubbles

    # BubbleGun's reader only opens plain text; let it read .gz transparently
    # without changing its parsing code.
    graph_io.open = lambda p, mode="r": (
        gzip.open(p, "rt") if p.endswith(".gz") else builtins.open(p, mode))

    t0 = time.time()
    log(f"reading {args.graph} into memory")
    graph = Graph(args.graph, low_memory=not args.keep_sequences)
    t_load = time.time() - t0
    log(f"loaded {len(graph)} nodes in {t_load:.1f}s, peak RSS {peak_rss_mb():.0f} MB")

    t1 = time.time()
    find_bubbles(graph)  # BubbleGun's own loop: every node, both directions
    t_find = time.time() - t1
    log(f"found {len(graph.bubbles)} bubbles in {t_find:.1f}s")

    for bubble in graph.bubbles.values():
        out.write(bubble_row(bubble))
    return {"bubbles": len(graph.bubbles), "load_s": t_load, "find_s": t_find}


################################ indexed mode ##################################

class IndexedNodes(dict):
    """
    A dict node_id -> BubbleGun Node that loads nodes from a gfaidx index.

    find_sb_alg only ever does graph.nodes[node_id], so this dict can stand in
    for BubbleGun's in-memory one. It holds the nodes of the communities
    loaded so far, so looking up a loaded node is an ordinary dict lookup. A
    missing node calls __missing__, which finds the node's community through
    the .ndx (an mmap hash lookup), materializes it with get_community() plus
    the cross-community links in self.cross, and adds its nodes.

    A single search can span more communities than the cache holds, and it
    visits nodes in no particular order, so evictions wait for release(),
    called after each search. The cache then shrinks back to `cache_size`
    communities, least recently used first, and no search loads a community
    twice.
    """

    def __init__(self, index, cache_size):
        super().__init__()
        self.index = index
        self.cache_size = cache_size
        self.loaded = OrderedDict()  # community id -> its node ids, oldest first
        self.community_loads = 0
        # get_community() returns only links inside one community; the links
        # between communities are few, so keep them all for the whole run:
        # node_id -> [(attribute, adjacency entry)]
        self.cross = {}
        for e in index.cross_community_edges():
            for node_id, attr, entry in edge_halves(
                    e.from_endpoint.node_id, e.from_endpoint.reverse,
                    e.to_endpoint.node_id, e.to_endpoint.reverse,
                    parse_overlap(e.overlap)):
                self.cross.setdefault(node_id, []).append((attr, entry))

    def _materialize(self, community_id):
        # Bubble detection needs topology only. Without sequences, a community
        # of long nodes costs no more memory than any other.
        sub = self.index.get_community(community_id, include_sequences=False)
        nodes = {n.id: Node(n.id) for n in sub.nodes}
        overlaps = {}
        # get_community() returns only links with both ends in the community.
        # Same translation as edge_halves(), inlined because this loop runs
        # for every link of every loaded community.
        for e in sub.edges:
            a, b = e.from_endpoint, e.to_endpoint
            x, x_rev, y, y_rev = a.node_id, a.reverse, b.node_id, b.reverse
            overlap = overlaps.get(e.overlap)
            if overlap is None:
                overlap = overlaps[e.overlap] = parse_overlap(e.overlap)
            nx, ny = nodes[x], nodes[y]
            (nx.start if x_rev else nx.end).add((y, int(y_rev), overlap))
            (ny.end if y_rev else ny.start).add((x, int(not x_rev), overlap))
        for node_id, node in nodes.items():
            for attr, entry in self.cross.get(node_id, ()):
                getattr(node, attr).add(entry)
            # read_gfa deduplicates with sets and then hands out lists
            node.start = list(node.start)
            node.end = list(node.end)
        self.community_loads += 1
        return nodes

    def community(self, community_id):
        """Load a community unless cached, mark it most recently used, and
        return its node ids."""
        ids = self.loaded.get(community_id)
        if ids is None:
            nodes = self._materialize(community_id)
            self.update(nodes)
            ids = self.loaded[community_id] = list(nodes)
        else:
            self.loaded.move_to_end(community_id)
        return ids

    def release(self):
        """After a search, shrink the cache back to cache_size communities."""
        while len(self.loaded) > self.cache_size:
            _, ids = self.loaded.popitem(last=False)
            for node_id in ids:
                del self[node_id]

    def __missing__(self, node_id):
        self.community(self.index.community_id(node_id))
        node = self.get(node_id)  # dict.get does not call __missing__ again
        if node is None:
            raise KeyError(node_id)
        return node


class IndexedGraphView:
    """The only graph attribute find_sb_alg needs is .nodes."""

    def __init__(self, nodes):
        self.nodes = nodes


def run_indexed(args, out):
    import pygfaidx

    t0 = time.time()
    index = pygfaidx.IndexedGraph(args.graph)
    nodes = IndexedNodes(index, cache_size=args.cache_size)
    graph = IndexedGraphView(nodes)
    t_load = time.time() - t0
    log(f"opened index: {index.node_count} nodes, {index.community_count} "
        f"communities, {sum(len(v) for v in nodes.cross.values()) // 2} "
        f"cross-community links, in {t_load:.2f}s")

    t1 = time.time()
    n_bubbles = 0
    for community_id in range(index.community_count):
        # Snapshot the member ids: searches below may evict the community.
        for node_id in list(nodes.community(community_id)):
            for direction in (0, 1):
                # Keep the community being processed the most recently used.
                nodes.community(community_id)
                bubble = find_sb_alg(graph, nodes[node_id], direction)
                nodes.release()
                # Each bubble is found from both of its ends. Emit it only
                # from the end with the smaller id, so no global set of found
                # bubbles (or of visited nodes) has to be kept.
                if bubble is not None and bubble.source.id < bubble.sink.id:
                    out.write(bubble_row(bubble))
                    n_bubbles += 1
        if args.progress and (community_id + 1) % args.progress == 0:
            log(f"{community_id + 1}/{index.community_count} communities, "
                f"{n_bubbles} bubbles, {nodes.community_loads} community "
                f"loads, peak RSS {peak_rss_mb():.0f} MB")
    t_find = time.time() - t1
    log(f"found {n_bubbles} bubbles in {t_find:.1f}s with "
        f"{nodes.community_loads} community loads "
        f"({index.community_count} communities)")
    return {"bubbles": n_bubbles, "load_s": t_load, "find_s": t_find,
            "community_loads": nodes.community_loads}


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=["inmemory", "indexed"])
    parser.add_argument("graph", help="GFA (.gfa or .gfa.gz) for inmemory; "
                                      "gfaidx .indexed.gfa.gz for indexed")
    parser.add_argument("output", help="output TSV")
    parser.add_argument("--cache-size", type=int, default=64,
                        help="indexed mode: communities kept in memory between "
                             "searches (default 64)")
    parser.add_argument("--keep-sequences", action="store_true",
                        help="inmemory mode: also load sequences (BubbleGun default "
                             "without --save_memory)")
    parser.add_argument("--progress", type=int, default=0,
                        help="indexed mode: log every N communities")
    args = parser.parse_args()

    t0 = time.time()
    with open(args.output, "w") as out:
        out.write(TSV_HEADER)
        stats = (run_inmemory if args.mode == "inmemory" else run_indexed)(args, out)
    stats["total_s"] = time.time() - t0
    stats["peak_rss_mb"] = peak_rss_mb()
    log("summary " + " ".join(f"{k}={v:.1f}" if isinstance(v, float) else f"{k}={v}"
                              for k, v in stats.items()))


if __name__ == "__main__":
    main()
