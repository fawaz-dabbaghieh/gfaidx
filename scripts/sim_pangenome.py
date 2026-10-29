#!/usr/bin/env python3
"""Simulate a Minigraph-Cactus-like GFA: topologically numbered nodes, bubbles,
inversions, CNV loops, haplotypes split into contig W lines.

Usage: python3 scripts/sim_pangenome.py <sites> <haplotypes> [seed] > graph.gfa

Used for .pdx compression experiments and the node-order equivalence test,
e.g. `sim_pangenome.py 400000 40` gives ~624k nodes and ~17M steps."""
import random
import sys

random.seed(int(sys.argv[3]) if len(sys.argv) > 3 else 1)
n_sites = int(sys.argv[1])
n_haps = int(sys.argv[2])

seq_of = {}
next_id = 1


def new_node():
    global next_id
    nid = next_id
    next_id += 1
    seq_of[nid] = "".join(random.choice("ACGT") for _ in range(random.choice([1, 1, 1, 2, 5, 12, 40])))
    return nid


sites = []  # each site: list of alleles; allele = list of node ids
for _ in range(n_sites):
    r = random.random()
    if r < 0.70:
        sites.append([[new_node()]])
    elif r < 0.95:
        k = random.choice([2, 2, 2, 3])
        alleles = [[new_node()] for _ in range(k)]
        if random.random() < 0.3:
            alleles.append([])  # deletion allele
        sites.append(alleles)
    else:
        ref = [new_node()]
        ins = [new_node() for _ in range(random.randint(2, 8))]
        sites.append([ref, ins])

freqs = [[0.8] + [0.2 / (len(s) - 1)] * (len(s) - 1) if len(s) > 1 else [1.0] for s in sites]

walks = []
for h in range(n_haps):
    path = []
    i = 0
    while i < n_sites:
        if h > 0 and random.random() < 0.0005:  # inversion of a run of sites
            j = min(n_sites, i + random.randint(5, 60))
            seg = []
            for s in range(i, j):
                seg.extend(sites[s][0])
            path.extend((nid, "<") for nid in reversed(seg))
            i = j
            continue
        if h > 0 and random.random() < 0.0003 and i > 300:  # CNV: repeat previous region
            back = random.randint(20, 300)
            for s in range(i - back, i):
                path.extend((nid, ">") for nid in sites[s][0])
        if h == 0:
            allele = sites[i][0]
        else:
            allele = random.choices(sites[i], weights=freqs[i])[0]
        path.extend((nid, ">") for nid in allele)
        i += 1
    walks.append(path)

out = sys.stdout
out.write("H\tVN:Z:1.1\tRS:Z:REF\n")
for nid in range(1, next_id):
    out.write(f"S\t{nid}\t{seq_of[nid]}\n")
links = set()
for path in walks:
    for (a, oa), (b, ob) in zip(path, path[1:]):
        links.add((a, "+" if oa == ">" else "-", b, "+" if ob == ">" else "-"))
for a, oa, b, ob in sorted(links):
    out.write(f"L\t{a}\t{oa}\t{b}\t{ob}\t0M\n")
for h, path in enumerate(walks):
    sample = "REF" if h == 0 else f"S{h:03d}"
    pos = 0
    cuts = [0] if h == 0 else sorted({0, *random.sample(range(1, len(path)), k=min(len(path) - 1, random.randint(5, 40)))})
    cuts.append(len(path))
    for c, (s, e) in enumerate(zip(cuts, cuts[1:])):
        contig = path[s:e]
        length = sum(len(seq_of[n]) for n, _ in contig)
        steps = "".join(f"{o}{n}" for n, o in contig)
        out.write(f"W\t{sample}\t0\t{'chr1' if h == 0 else f'ctg{c}'}\t{pos}\t{pos + length}\t{steps}\n")
        pos = 0 if h > 0 else pos + length
