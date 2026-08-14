"""Python bindings for the gfaidx indexed graph query library.

The module intentionally exposes the same immutable IndexedGraph and mutable
Graph split as the C++ API.  The previous pure-Python prototype is not part of
this interface.
"""

from ._pygfaidx import (
    Capabilities,
    CoordinateTrack,
    Edge,
    Endpoint,
    Graph,
    IndexedGraph,
    IndexPaths,
    Node,
    Path,
    PathDescriptor,
    PathStep,
    RegionMode,
    ValidationIssue,
)

__all__ = [
    "Capabilities",
    "CoordinateTrack",
    "Edge",
    "Endpoint",
    "Graph",
    "IndexedGraph",
    "IndexPaths",
    "Node",
    "Path",
    "PathDescriptor",
    "PathStep",
    "RegionMode",
    "ValidationIssue",
]
