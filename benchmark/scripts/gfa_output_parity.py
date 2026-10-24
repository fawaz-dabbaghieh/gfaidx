#!/usr/bin/env python3
"""Count records and hash one extracted GFA for benchmark parity checks.

Prints one tab-separated line:

    nodes  edges  paths  node_set_sha1  reference_walks  reference_walk_steps  reference_walk_sha1

The node-set hash covers the sorted S identifiers, so it does not depend on
output order. The reference walk is the W record whose sample is the queried
reference, whose sequence (ignoring a #subpath suffix) is the queried contig,
and which overlaps the query when it carries coordinates. gbz-base names
non-reference walks `unknown`, while gfaidx names every walk, so these filters
pick the same record in both tools. A lone P/W record is used when nothing
matched, which covers reference-only output on P-path graphs. Its hash is the
smaller of the forward and reversed walk hashes, so the same walk written in
either direction matches. Walk hashes are NA unless exactly one record
qualifies.

One streaming pass replaces the earlier awk pipeline, which was quadratic in
walk length under BSD awk and dominated benchmark wall time on large outputs.
"""

import argparse
import hashlib
import sys


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("gfa")
    parser.add_argument("--reference", required=True)
    parser.add_argument("--contig", required=True)
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    return parser.parse_args()


def walk_steps(record_type, steps):
    """Return a W walk or P path as a list of (is_reverse, node) pairs."""
    if record_type == b"W":
        result = []
        start = 0
        for index in range(1, len(steps) + 1):
            if index == len(steps) or steps[index] in b"<>":
                result.append((steps[start:start + 1] == b"<", steps[start + 1:index]))
                start = index
        return result
    return [(step[-1:] == b"-", step[:-1]) for step in steps.split(b",") if step]


def walk_sha1(steps):
    forward = hashlib.sha1()
    for is_reverse, node in steps:
        forward.update((b"<" if is_reverse else b">") + node)
    reverse = hashlib.sha1()
    for is_reverse, node in reversed(steps):
        reverse.update((b">" if is_reverse else b"<") + node)
    return min(forward.hexdigest(), reverse.hexdigest())


def overlaps(seq_start, seq_end, start, end):
    # Coordinate-free subpaths (`*`) cannot be filtered by position.
    if not (seq_start.isdigit() and seq_end.isdigit()):
        return True
    return int(seq_start) < end and int(seq_end) > start


def main():
    args = parse_args()
    reference = args.reference.encode()
    contig = args.contig.encode()

    nodes = []
    edges = 0
    paths = 0
    candidates = []
    last_path = None
    try:
        handle = open(args.gfa, "rb")
    except FileNotFoundError:
        print("0\t0\t0\tNA\t0\tNA\tNA")
        return 0
    with handle:
        for line in handle:
            record_type = line[:1]
            if record_type == b"S":
                nodes.append(line.split(b"\t", 2)[1].rstrip(b"\n"))
            elif record_type == b"L":
                edges += 1
            elif record_type in (b"W", b"P"):
                paths += 1
                fields = line.rstrip(b"\n").split(b"\t")
                steps = fields[6] if record_type == b"W" else fields[2]
                last_path = (record_type, steps)
                if (record_type == b"W" and fields[1] == reference and
                        fields[3].split(b"#", 1)[0] == contig and
                        overlaps(fields[4], fields[5], args.start, args.end)):
                    candidates.append((record_type, steps))

    nodes.sort()
    node_hash = hashlib.sha1(b"".join(node + b"\n" for node in nodes)).hexdigest()
    if not candidates and paths == 1:
        candidates = [last_path]
    walk_count = len(candidates)
    if walk_count == 1:
        steps = walk_steps(*candidates[0])
        walk_fields = f"{walk_count}\t{len(steps)}\t{walk_sha1(steps)}"
    else:
        walk_fields = f"{walk_count}\tNA\tNA"
    print(f"{len(nodes)}\t{edges}\t{paths}\t{node_hash}\t{walk_fields}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
