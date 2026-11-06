"""All-haplotype regions, with and without haplotype_gap, checked against the
GFA. The reference nodes of the region are its anchors. Every other record
keeps the steps between its first and last anchor, and with a gap limit it is
split wherever more than that many bases lie between two anchors."""
import io

import pytest

import pygfaidx

REF = ("REF", 0, "chr1")


def record_line(truth, key, record_type, steps, first, last):
    lengths = [len(truth.sequences[node]) for node, _ in steps]
    start = sum(lengths[:first])
    end = start + sum(lengths[first:last + 1])
    run = steps[first:last + 1]
    if record_type == "P":
        names = ",".join(node + ("-" if reverse else "+") for node, reverse in run)
        return f"P\t{key}:{start}-{end}\t{names}\t*"
    sample, haplotype, sequence = key
    walk = "".join(("<" if reverse else ">") + node for node, reverse in run)
    return f"W\t{sample}\t{haplotype}\t{sequence}\t{start}\t{end}\t{walk}"


def expected_region(truth, begin, end, gap):
    """The sorted P/W lines and the node set of an all-haplotype region, and
    how many records were split into more than one run."""
    ref_steps = truth.paths[REF][1]
    position = 0
    ref_run = []
    for i, (node, _) in enumerate(ref_steps):
        length = len(truth.sequences[node])
        if position < end and position + length > begin:
            ref_run.append(i)
        position += length
    anchors = {ref_steps[i][0] for i in ref_run}

    lines = [record_line(truth, REF, "W", ref_steps, ref_run[0], ref_run[-1])]
    nodes = set(anchors)
    split_records = 0
    for key, (record_type, steps) in truth.paths.items():
        if key == REF:
            continue
        hits = [i for i, (node, _) in enumerate(steps) if node in anchors]
        if not hits:
            continue
        runs = []
        first = last = hits[0]
        for i in hits[1:]:
            between = sum(len(truth.sequences[node]) for node, _ in steps[last + 1:i])
            if gap is not None and between > gap:
                runs.append((first, last))
                first = i
            last = i
        runs.append((first, last))
        split_records += len(runs) > 1
        for first, last in runs:
            lines.append(record_line(truth, key, record_type, steps, first, last))
            nodes.update(node for node, _ in steps[first:last + 1])
    return sorted(lines), nodes, split_records


@pytest.mark.parametrize("fmt", ("default", "legacy_hash"))
def test_all_haplotypes_match_gfa(bubble_graph, fmt):
    truth, bundles = bubble_graph
    index = pygfaidx.IndexedGraph(str(bundles[fmt]))
    ref_steps = truth.paths[REF][1]
    site_offsets = [0]
    for node, _ in ref_steps:
        site_offsets.append(site_offsets[-1] + len(truth.sequences[node]))

    split_records = 0
    # Site 330 sits in contig_a's repeated stretch (sites 300-339 are walked
    # twice), and contig_b starts at site 400.
    for site in (0, 148, 330, 395, 790):
        begin = site_offsets[site]
        end = min(begin + 120, site_offsets[-1])
        for gap in (None, 0, 5, 40):
            lines, nodes, split = expected_region(truth, begin, end, gap)
            split_records += split
            for threads in (1, 4):
                out = io.StringIO()
                index.stream_region("REF", "chr1", begin, end, out,
                                    mode=pygfaidx.RegionMode.ALL_HAPLOTYPES,
                                    haplotype_gap=gap, include_coordinates=True,
                                    threads=threads)
                result = out.getvalue().splitlines()
                label = f"site={site} gap={gap} threads={threads}"
                assert sorted(line for line in result if line[0] in "PW") == lines, label
                assert {line.split("\t")[1] for line in result if line.startswith("S\t")} == nodes, label
    # Guard against regions where no record is split.
    assert split_records > 0
