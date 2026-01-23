import argparse
import os
import sys

from .chgraph import ChGraph


def main():
    parser = argparse.ArgumentParser(description="Output BFS neighborhood as GFA to stdout.")
    parser.add_argument("graph", type=str, help="input .gfa.gz (indexed)")
    parser.add_argument("node_id", type=str, help="start node id")
    parser.add_argument("size", nargs="?", type=int, default=100, help="neighborhood size")
    parser.add_argument("outgfa", nargs="?", type=str, default="", help="output gfa file")
    parser.add_argument("--no-shared-cache", action="store_true",
                        help="disable shared-edge cache and scan the shared chunk each time")
    args = parser.parse_args()

    if not os.path.exists(args.graph):
        print(f"The graph {args.graph} does not exist", file=sys.stderr)
        return 1

    g = ChGraph(args.graph, use_shared_edges_cache=not args.no_shared_cache)
    neighborhood = g.bfs(args.node_id, args.size)
    if args.outgfa:
        outputfile = args.outgfa
    else:
        outputfile = "/dev/stdout"
    g.write_gfa(set_of_nodes=neighborhood, output_file=outputfile, append=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
