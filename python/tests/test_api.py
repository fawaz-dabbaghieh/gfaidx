import io

import pygfaidx


def test_indexed_and_mutable_graph(indexed_graph_path):
    index = pygfaidx.IndexedGraph(str(indexed_graph_path))
    assert index.node_count == 3
    assert index.node_exists("1")
    assert index.get_node("1").sequence == "AA"
    assert index.neighbors("1") == ["2"]

    graph = index.get_subgraph(["1"], max_nodes=3, include_coordinates=True)
    assert graph.node_count == 3
    assert graph.edge_count == 2
    assert graph.path_count == 2

    # Snapshot mutation is committed explicitly through update_node().
    node = graph.get_node("1")
    node.sequence = "TT"
    graph.update_node(node)
    assert graph.get_node("1").sequence == "TT"

    output = io.StringIO()
    index.stream_subgraph(["1"], output, max_nodes=3)
    assert output.getvalue().startswith("H\t")
    assert "\nS\t1\tAA\n" in output.getvalue()


def test_owned_graph_edits():
    graph = pygfaidx.Graph()
    graph.add_node("a", "AC")
    graph.add_node("b", "G")
    edge = pygfaidx.Edge()
    edge.from_endpoint = pygfaidx.Endpoint("a")
    edge.to_endpoint = pygfaidx.Endpoint("b", reverse=True)
    graph.add_edge(edge)
    assert graph.neighbors("a") == ["b"]
    graph.remove_node("b")
    assert graph.edge_count == 0
    assert graph.validate() == []
