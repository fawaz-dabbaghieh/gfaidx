#!/usr/bin/env python3
"""Rename every segment of a GFA with a random permutation, for node-order tests.

Usage:
    python3 scripts/rename_gfa_nodes.py in.gfa out.gfa --mode shuffled_int [--seed 1]
    python3 scripts/rename_gfa_nodes.py in.gfa out.gfa --mode non_numeric

Modes:
  shuffled_int  segment names become a random permutation of 1..N, so integer
                names carry no path locality at all.
  non_numeric   segment names become "n<k>_<hex>" with k drawn from a random
                permutation, so no name parses as an integer and the indexer
                must fall back to its non-numeric node order.

S, L, P and W lines are rewritten; every other line is copied unchanged. The
mapping is built from S lines in a first pass, so the input must be a file.
"""

from __future__ import annotations

import argparse
import random
import re
import sys

WALK_TOKEN = re.compile(r"([<>])([^<>]+)")


def build_mapping(path: str, mode: str, seed: int) -> dict[str, str]:
    names: list[str] = []
    with open(path, "r", encoding="ascii") as handle:
        for line in handle:
            if line.startswith("S\t"):
                names.append(line.split("\t", 2)[1])
    perm = list(range(1, len(names) + 1))
    random.Random(seed).shuffle(perm)
    if mode == "shuffled_int":
        return {name: str(k) for name, k in zip(names, perm)}
    return {name: f"n{k}_{(k * 2654435761) & 0xffff:x}" for name, k in zip(names, perm)}


def rename_p_steps(field: str, mapping: dict[str, str]) -> str:
    out = []
    for token in field.split(","):
        out.append(mapping[token[:-1]] + token[-1])
    return ",".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("in_gfa")
    ap.add_argument("out_gfa")
    ap.add_argument("--mode", choices=["shuffled_int", "non_numeric"], required=True)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()

    mapping = build_mapping(args.in_gfa, args.mode, args.seed)
    with open(args.in_gfa, "r", encoding="ascii") as src, \
            open(args.out_gfa, "w", encoding="ascii") as dst:
        for line in src:
            kind = line[:2]
            if kind == "S\t":
                fields = line.split("\t")
                fields[1] = mapping[fields[1]]
                line = "\t".join(fields)
            elif kind == "L\t":
                fields = line.split("\t")
                fields[1] = mapping[fields[1]]
                fields[3] = mapping[fields[3]]
                line = "\t".join(fields)
            elif kind == "P\t":
                fields = line.rstrip("\n").split("\t")
                fields[2] = rename_p_steps(fields[2], mapping)
                line = "\t".join(fields) + "\n"
            elif kind == "W\t":
                fields = line.rstrip("\n").split("\t")
                fields[6] = WALK_TOKEN.sub(lambda m: m.group(1) + mapping[m.group(2)], fields[6])
                line = "\t".join(fields) + "\n"
            dst.write(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
