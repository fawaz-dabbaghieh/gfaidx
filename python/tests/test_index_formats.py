"""pygfaidx reads both node numberings: the default locality node ids with the
compressed .pdx, and --node_order legacy_hash. Queries must match the GFA and
give the same results on either bundle; mixing the two is rejected."""
import io
from pathlib import Path

import pytest

import pygfaidx

FORMATS = ("default", "legacy_hash")


def open_index(bundles, name):
    return pygfaidx.IndexedGraph(str(bundles[name]))


def truth_key(descriptor):
    if descriptor.record_type == "P":
        return descriptor.name
    return (descriptor.sample, descriptor.haplotype, descriptor.sequence_name)


def steps_of(path_steps):
    return [(step.node_id, step.reverse) for step in path_steps]


def stream(call):
    out = io.StringIO()
    call(out)
    return out.getvalue()


def edge_key(edge):
    # The same canonical form as GraphTruth.edges: an L line and its reverse
    # complement are one edge.
    a, b = edge.from_endpoint, edge.to_endpoint
    forward = (a.node_id, "-" if a.reverse else "+", b.node_id, "-" if b.reverse else "+")
    backward = (b.node_id, "+" if b.reverse else "-", a.node_id, "+" if a.reverse else "-")
    return min(forward, backward)


def test_bundles_use_different_formats(bubble_graph):
    _, bundles = bubble_graph
    default_ndx = Path(f"{bundles['default']}.ndx").read_bytes()
    legacy_ndx = Path(f"{bundles['legacy_hash']}.ndx").read_bytes()
    assert default_ndx[:8] == b"GFANDX02"
    assert legacy_ndx[:8] != b"GFANDX02"
    assert (Path(f"{bundles['default']}.pdx").read_bytes()
            != Path(f"{bundles['legacy_hash']}.pdx").read_bytes())


@pytest.mark.parametrize("fmt", FORMATS)
def test_nodes_match_gfa(bubble_graph, fmt):
    truth, bundles = bubble_graph
    index = open_index(bundles, fmt)
    assert index.node_count == len(truth.sequences)
    for node, sequence in truth.sequences.items():
        assert index.get_node(node).sequence == sequence
        assert set(index.neighbors(node)) == truth.neighbors(node)
    assert not index.node_exists("not_a_node")


@pytest.mark.parametrize("fmt", FORMATS)
def test_communities_and_cross_edges_cover_gfa(bubble_graph, fmt):
    truth, bundles = bubble_graph
    index = open_index(bundles, fmt)
    internal = []
    for community in range(index.community_count):
        for edge in index.get_community(community).edges:
            assert index.community_id(edge.from_endpoint.node_id) == community
            assert index.community_id(edge.to_endpoint.node_id) == community
            internal.append(edge_key(edge))
    cross = index.cross_community_edges()
    assert cross, "the bubble graph spans several communities"
    for edge in cross:
        assert (index.community_id(edge.from_endpoint.node_id)
                != index.community_id(edge.to_endpoint.node_id))
    # Every link is reported exactly once: in its community or as a cross link.
    assert sorted(internal + [edge_key(e) for e in cross]) == sorted(truth.edges)


@pytest.mark.parametrize("fmt", FORMATS)
def test_paths_match_gfa(bubble_graph, fmt):
    truth, bundles = bubble_graph
    index = open_index(bundles, fmt)
    descriptors = index.paths()
    assert {repr(truth_key(d)) for d in descriptors} == {repr(key) for key in truth.paths}
    for descriptor in descriptors:
        record_type, steps = truth.paths[truth_key(descriptor)]
        assert descriptor.record_type == record_type
        assert descriptor.step_count == len(steps)
        assert steps_of(index.path_steps(descriptor.key)) == steps
        # A window that starts and ends inside 128-step blocks.
        assert steps_of(index.path_steps(descriptor.key, 100, 300)) == steps[100:400]


def query_results(index, scratch):
    results = {"community_count": index.community_count}
    nodes = [str(n) for n in range(1, index.node_count + 1)]
    results["community_id"] = [index.community_id(n) for n in nodes]
    results["communities"] = [sorted(node.id for node in index.get_community(c).nodes)
                              for c in range(index.community_count)]
    results["cross_community_edges"] = sorted(edge_key(e) for e in index.cross_community_edges())

    seeds = ["1", "401", "1200"]
    for max_nodes in (50, 5000):
        for threads in (1, 2):
            results[f"subgraph max_nodes={max_nodes} threads={threads}"] = stream(
                lambda out: index.stream_subgraph(seeds, out, max_nodes=max_nodes,
                                                  include_coordinates=True, threads=threads))

    path_key = next(d.key for d in index.paths() if d.name == "contig_a")
    path_graph = scratch / "path_subgraph.gfa"
    index.get_path_subgraph(path_key, 250, 200, include_coordinates=True).write_gfa(str(path_graph))
    results["path_subgraph"] = path_graph.read_text()

    tracks = index.coordinate_tracks()
    results["tracks"] = [(t.reference, t.sequence, t.begin, t.end, t.entry_count) for t in tracks]
    track = tracks[0]
    modes = (
        ("bfs", pygfaidx.RegionMode.BFS, {}),
        ("bfs context", pygfaidx.RegionMode.BFS, {"context": 150}),
        ("all_haplotypes", pygfaidx.RegionMode.ALL_HAPLOTYPES, {}),
        ("reference", pygfaidx.RegionMode.REFERENCE, {}),
    )
    for begin in (track.begin, (track.begin + track.end) // 2, track.end - 300):
        for label, mode, extra in modes:
            results[f"region {begin} {label}"] = stream(
                lambda out: index.stream_region(track.reference, track.sequence, begin, begin + 300,
                                                out, mode=mode, include_coordinates=True, **extra))
    return results


def test_queries_match_across_formats(bubble_graph, tmp_path):
    _, bundles = bubble_graph
    default = query_results(open_index(bundles, "default"), tmp_path)
    legacy = query_results(open_index(bundles, "legacy_hash"), tmp_path)
    assert default.keys() == legacy.keys()
    for key in default:
        assert default[key] == legacy[key], key
    # Guard against comparing empty results.
    assert default["community_count"] > 1
    assert default["subgraph max_nodes=5000 threads=1"].count("\nW\t") == 5
    assert all(default[key].count("\nS\t") > 0 for key in default if key.startswith("region"))


@pytest.mark.parametrize("sidecar", ("pdx", "cdx"))
def test_mixed_bundles_are_rejected(bubble_graph, sidecar):
    _, bundles = bubble_graph
    for graph_fmt, sidecar_fmt in (("default", "legacy_hash"), ("legacy_hash", "default")):
        overrides = pygfaidx.IndexPaths()
        setattr(overrides, sidecar, f"{bundles[sidecar_fmt]}.{sidecar}")
        with pytest.raises(RuntimeError, match="different .ndx node numbering"):
            pygfaidx.IndexedGraph(str(bundles[graph_fmt]), overrides)
