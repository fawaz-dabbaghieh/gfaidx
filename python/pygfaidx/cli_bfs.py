import argparse
import os
import sys

from .chgraph import ChGraph


def main():
    parser = argparse.ArgumentParser(description="Output BFS neighborhood as GFA to stdout.")
    parser.add_argument("graph", help="input .gfa.gz (indexed)")
    parser.add_argument("node_id", help="start node id")
    parser.add_argument("size", type=int, help="neighborhood size")
    parser.add_argument("outgfa", help="output gfa file")
    args = parser.parse_args()

    if not os.path.exists(args.graph):
        print(f"The graph {args.graph} does not exist", file=sys.stderr)
        return 1


    g = ChGraph(args.graph)
    neighborhood = g.bfs(args.node_id, args.size)
    if args.outgfa:
        outputfile = args.outgfa
    else:
        outputfile = "/dev/stdout"
    g.write_gfa(set_of_nodes=neighborhood, output_file=outputfile, append=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
