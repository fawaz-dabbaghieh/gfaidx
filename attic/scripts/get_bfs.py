#!/usr/bin/env python3
import sys
import os
import argparse
from gfaidx.chgraph import ChGraph

def main():
    parser = argparse.ArgumentParser(description="Output BFS neighborhood as GFA to stdout.")
    parser.add_argument("graph", help="input .gfa.gz (indexed)")
    parser.add_argument("node_id", help="start node id")
    parser.add_argument("size", type=int, help="neighborhood size")
    args = parser.parse_args()

    if not os.path.exists(args.graph):
      print(f"The graph {args.graph} does not exist")
      sys.exit(1)

    g = ChGraph(args.graph)
    neighborhood = g.bfs(args.node_id, args.size)

    # Write GFA to stdout
    g.write_gfa(set_of_nodes=neighborhood, output_file="/dev/stdout", append=False)

if __name__ == "__main__":
    main()
