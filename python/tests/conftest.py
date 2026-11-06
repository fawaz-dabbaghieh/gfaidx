import os
import random
import shutil
import subprocess

import pytest


@pytest.fixture(scope="session")
def gfaidx_bin():
    """The gfaidx CLI used to build test indexes: $GFAIDX_BIN, else gfaidx on PATH."""
    value = os.environ.get("GFAIDX_BIN")
    if value:
        if not os.access(value, os.X_OK):
            pytest.fail(f"GFAIDX_BIN is not an executable: {value}")
        return value
    found = shutil.which("gfaidx")
    if not found:
        pytest.skip("set GFAIDX_BIN to the gfaidx CLI (or put gfaidx on PATH)")
    return found


def run_gfaidx(gfaidx_bin, *args):
    result = subprocess.run([gfaidx_bin, *map(str, args)], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"gfaidx {' '.join(map(str, args))} failed:\n{result.stdout}{result.stderr}")


@pytest.fixture(scope="session")
def indexed_graph_path(gfaidx_bin, tmp_path_factory):
    work = tmp_path_factory.mktemp("tiny")
    gfa = work / "tiny.gfa"
    gfa.write_text(
        "H\tVN:Z:1.0\n"
        "S\t1\tAA\nS\t2\tC\nS\t3\tGT\n"
        "L\t1\t+\t2\t+\t0M\nL\t2\t+\t3\t+\t0M\n"
        "P\tp1\t1+,2+,3+\t*\nP\tp2\t3-,2-\t*\n"
    )
    out = work / "tiny.gz"
    run_gfaidx(gfaidx_bin, "index_gfa", gfa, out, "--progress_every", 0)
    return out


def flip(orientation):
    return "-" if orientation == "+" else "+"


def canonical_edge(a, oa, b, ob):
    # An L line and its reverse complement (b, flip(ob), a, flip(oa)) are one edge.
    return min((a, oa, b, ob), (b, flip(ob), a, flip(oa)))


class GraphTruth:
    """What the test GFA contains, for checking query results against it."""

    def __init__(self):
        self.sequences = {}
        self.edges = set()
        self.paths = {}  # key -> (record_type, [(node, reverse)])

    def neighbors(self, node):
        result = set()
        for a, _, b, _ in self.edges:
            if a == node:
                result.add(b)
            if b == node:
                result.add(a)
        return result


def write_bubble_graph(path):
    """A chain of 800 two-way bubbles walked by P and W records.

    Paths pick a branch at every site, so consecutive nodes share most paths
    but not all of them. Some records are reversed, start mid-graph, skip a
    site, or repeat a stretch, and a reference W with RS:Z gives coordinates.
    Long records span many 128-step blocks and 32-node posting blocks.
    """
    rng = random.Random(11)
    sites = 800
    truth = GraphTruth()
    for site in range(sites):
        for node in (2 * site + 1, 2 * site + 2):
            truth.sequences[str(node)] = "".join(rng.choice("ACGT") for _ in range(rng.randint(1, 12)))
    for site in range(sites - 1):
        for a in (2 * site + 1, 2 * site + 2):
            for b in (2 * site + 3, 2 * site + 4):
                truth.edges.add(canonical_edge(str(a), "+", str(b), "+"))

    def haplotype(first, last, skip_every=0, repeat=None):
        steps = []
        site = first
        while site < last:
            steps.append((str(2 * site + 1 + rng.randint(0, 1)), False))
            site += 2 if skip_every and site % skip_every == 0 else 1
        if repeat:
            begin, end = repeat
            steps = steps[:end] + steps[begin:end] + steps[end:]
        return steps

    def reverse(steps):
        return [(node, not rev) for node, rev in reversed(steps)]

    records = [
        ("W", ("REF", 0, "chr1"), [(str(2 * s + 1), False) for s in range(sites)]),
        ("W", ("HG1", 1, "chr1"), haplotype(0, sites)),
        ("W", ("HG1", 2, "chr1"), haplotype(0, sites, skip_every=37)),
        ("W", ("HG2", 1, "chr1"), haplotype(150, 620)),
        ("W", ("HG2", 2, "chr1"), reverse(haplotype(0, sites))),
        ("P", "contig_a", haplotype(0, sites, repeat=(300, 340))),
        ("P", "contig_b", haplotype(400, sites)),
        ("P", "contig_c", reverse(haplotype(10, 700, skip_every=53))),
    ]

    lines = ["H\tVN:Z:1.1\tRS:Z:REF"]
    lines += [f"S\t{node}\t{seq}" for node, seq in truth.sequences.items()]
    for record_type, meta, steps in records:
        for (a, ra), (b, rb) in zip(steps, steps[1:]):
            truth.edges.add(canonical_edge(a, "-" if ra else "+", b, "-" if rb else "+"))
    lines += [f"L\t{a}\t{oa}\t{b}\t{ob}\t0M" for a, oa, b, ob in sorted(truth.edges)]
    for record_type, meta, steps in records:
        if record_type == "P":
            lines.append(f"P\t{meta}\t" + ",".join(f"{n}{'-' if r else '+'}" for n, r in steps) + "\t*")
            truth.paths[meta] = ("P", steps)
        else:
            sample, hap, seq = meta
            length = sum(len(truth.sequences[n]) for n, _ in steps)
            walk = "".join(("<" if r else ">") + n for n, r in steps)
            lines.append(f"W\t{sample}\t{hap}\t{seq}\t0\t{length}\t{walk}")
            truth.paths[(sample, hap, seq)] = ("W", steps)
    path.write_text("\n".join(lines) + "\n")
    return truth


@pytest.fixture(scope="session")
def bubble_graph(gfaidx_bin, tmp_path_factory):
    """The bubble graph indexed twice: with the default locality node ids and
    compressed .pdx, and with --node_order legacy_hash. Both get a .cdx."""
    work = tmp_path_factory.mktemp("bubble")
    gfa = work / "bubble.gfa"
    truth = write_bubble_graph(gfa)
    bundles = {}
    for name, extra in (("default", []), ("legacy_hash", ["--node_order", "legacy_hash"])):
        out = work / name / "bubble.gz"
        out.parent.mkdir()
        run_gfaidx(gfaidx_bin, "index_gfa", gfa, out, "--progress_every", 0, *extra)
        run_gfaidx(gfaidx_bin, "index_coordinates", out, f"{out}.cdx", "--reference", "REF")
        bundles[name] = out
    return truth, bundles
