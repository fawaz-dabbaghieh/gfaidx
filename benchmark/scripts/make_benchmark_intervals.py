#!/usr/bin/env python3
"""Generate a stratified interval manifest for the gfaidx/gbz-base benchmark.

Each locus is one center position, and every requested interval size is placed
around that same center, so interval sizes are compared at fixed locations as
the benchmark runner expects. Locus identifiers are `<stratum>-<nn>`, so a
result row can be assigned to its stratum by the text before the dash.

Loci are placed in this order:

1. `--locus NAME CONTIG CENTER` places a fixed, named locus in the `named`
   stratum, for example a gene known for complex structural variation.
2. `--stratum NAME COUNT INSIDE_BP SOURCE` draws COUNT centers uniformly by
   base pair. SOURCE is either `genome` or a comma-separated list of BED files
   (plain or gzipped).

   - A BED stratum excludes every earlier BED stratum, so each locus belongs to
     exactly one stratum. Its center keeps INSIDE_BP / 2 bases from the region
     edges, so a window of INSIDE_BP lies entirely inside the annotation. Set
     INSIDE_BP to the smallest size to require only the smallest window inside,
     or to the largest size to require all of them inside. A COUNT of 0 draws
     nothing and only excludes its regions from later strata.
   - A `genome` stratum draws from the whole of the selected chromosomes,
     ignoring every annotation. It is the unbiased baseline, and it neither
     excludes nor is excluded by other strata.

Every center is at least --min-spacing from every other center. Every window
lies inside its chromosome: a window that would cross a chromosome end is
shifted inward, so it is no longer centered but still contains every smaller
window of its locus. Contigs shorter than the largest size are skipped. A
second table (--loci-out) records each
locus's stratum and annotation: the BED name of the region a center was drawn
from (or the merged region when the BED has no names), or for genome loci,
the annotated strata the center happens to fall in.
"""

import argparse
import bisect
import gzip
import random
import re
import sys
from collections import defaultdict


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n\n".join(__doc__.split("\n\n")[1:]))
    parser.add_argument("--chrom-sizes", required=True,
                        help="two-column contig and length table, such as a .fai or chrom.sizes")
    parser.add_argument("--reference", required=True,
                        help="reference sample written to the manifest, such as CHM13")
    parser.add_argument("--chroms", default=r"^chr([0-9]+|X)$",
                        help="regular expression selecting contigs (default: chr1-22 and chrX)")
    parser.add_argument("--sizes", default="1000,10000,100000,1000000",
                        help="comma-separated interval lengths in bp")
    parser.add_argument("--min-spacing", type=int, default=None,
                        help="minimum distance between centers (default: the largest size)")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--stratum", nargs=4, action="append", default=[],
                        metavar=("NAME", "COUNT", "INSIDE_BP", "SOURCE"))
    parser.add_argument("--locus", nargs=3, action="append", default=[],
                        metavar=("NAME", "CONTIG", "CENTER"))
    parser.add_argument("--output", required=True, help="manifest TSV for the benchmark runner")
    parser.add_argument("--loci-out", help="optional TSV describing every locus")
    return parser.parse_args()


def size_label(size):
    for unit, factor in (("Mb", 1_000_000), ("kb", 1000)):
        if size % factor == 0:
            return f"{size // factor}{unit}"
    return f"{size}bp"


def open_text(path):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path)


def merge(intervals):
    merged = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [tuple(interval) for interval in merged]


def subtract(intervals, removed):
    """Remove the merged `removed` intervals from the merged `intervals`."""
    result = []
    index = 0
    for start, end in intervals:
        while index < len(removed) and removed[index][1] <= start:
            index += 1
        cursor = start
        scan = index
        while scan < len(removed) and removed[scan][0] < end:
            if removed[scan][0] > cursor:
                result.append((cursor, removed[scan][0]))
            cursor = max(cursor, removed[scan][1])
            scan += 1
        if cursor < end:
            result.append((cursor, end))
    return result


def contains(intervals, position):
    index = bisect.bisect_right(intervals, (position, float("inf"))) - 1
    return index >= 0 and intervals[index][0] <= position < intervals[index][1]


class BedStratum:
    def __init__(self, name, paths, sizes):
        self.name = name
        self.records = defaultdict(list)  # contig -> sorted (start, end, name)
        raw = defaultdict(list)
        for path in paths:
            with open_text(path) as handle:
                for line in handle:
                    if not line.strip() or line.startswith(("#", "track", "browser")):
                        continue
                    fields = line.rstrip("\n").split("\t")
                    contig = fields[0]
                    if contig not in sizes:
                        continue
                    start, end = int(fields[1]), int(fields[2])
                    label = fields[3] if len(fields) > 3 else ""
                    raw[contig].append((start, end))
                    self.records[contig].append((start, end, label))
        self.regions = {contig: merge(intervals) for contig, intervals in raw.items()}
        for records in self.records.values():
            records.sort()

    def label_at(self, contig, position):
        labels = [label for start, end, label in self.records.get(contig, [])
                  if start <= position < end and label]
        if labels:
            return ",".join(dict.fromkeys(labels))
        # Unnamed BED records are described by the merged region instead.
        regions = self.regions.get(contig, [])
        index = bisect.bisect_right(regions, (position, float("inf"))) - 1
        start, end = regions[index]
        return f"{contig}:{start}-{end}"


def eligible_centers(regions, inside, chrom_lo, chrom_hi):
    """Half-open ranges of centers whose INSIDE window stays in a region."""
    half = inside // 2
    ranges = []
    for start, end in regions:
        lo = max(start + half, chrom_lo)
        hi = min(end - (inside - half) + 1, chrom_hi)
        if lo < hi:
            ranges.append((lo, hi))
    return ranges


def draw(rng, ranges, count, spacing, placed, stratum):
    """Draw COUNT centers uniformly by base pair, keeping the minimum spacing."""
    if count == 0:
        return []
    flat = [(contig, lo, hi) for contig, contig_ranges in ranges.items()
            for lo, hi in contig_ranges]
    weights = [hi - lo for _, lo, hi in flat]
    total = sum(weights)
    if total == 0:
        raise SystemExit(f"stratum {stratum}: no eligible centers")
    cumulative = []
    running = 0
    for weight in weights:
        running += weight
        cumulative.append(running)
    chosen = []
    attempts = 0
    while len(chosen) < count:
        attempts += 1
        if attempts > 100_000:
            raise SystemExit(f"stratum {stratum}: found only {len(chosen)} of {count} "
                             f"centers {spacing} bp apart")
        offset = rng.randrange(total)
        index = bisect.bisect_right(cumulative, offset)
        contig, lo, _ = flat[index]
        center = lo + offset - (cumulative[index] - weights[index])
        if all(other_contig != contig or abs(other - center) >= spacing
               for other_contig, other in placed):
            placed.append((contig, center))
            chosen.append((contig, center))
    return sorted(chosen, key=lambda item: (natural_key(item[0]), item[1]))


def natural_key(contig):
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", contig)]


def main():
    args = parse_args()
    sizes = sorted({int(value) for value in args.sizes.split(",")})
    if not sizes or sizes[0] <= 0:
        raise SystemExit("--sizes must contain positive lengths")
    largest = sizes[-1]
    spacing = largest if args.min_spacing is None else args.min_spacing
    selector = re.compile(args.chroms)

    chrom_sizes = {}
    with open_text(args.chrom_sizes) as handle:
        for line in handle:
            fields = line.split()
            if len(fields) >= 2 and selector.search(fields[0]):
                chrom_sizes[fields[0]] = int(fields[1])
    if not chrom_sizes:
        raise SystemExit("no contigs matched --chroms")
    short = sorted((contig for contig, length in chrom_sizes.items() if length < largest),
                   key=natural_key)
    if short:
        print(f"skipping contigs shorter than {largest} bp: {', '.join(short)}", file=sys.stderr)
    chrom_sizes = {contig: length for contig, length in chrom_sizes.items() if length >= largest}
    # The smallest window must fit around a center; larger windows are shifted
    # inward at chromosome ends when they are written.
    half = sizes[0] // 2
    chrom_bounds = {contig: (half, length - (sizes[0] - half) + 1)
                    for contig, length in chrom_sizes.items()}

    rng = random.Random(args.seed)
    placed = []
    loci = []  # (locus_id, stratum, contig, center, annotation)

    for index, (name, contig, center) in enumerate(args.locus, start=1):
        center = int(center)
        lo, hi = chrom_bounds.get(contig, (1, 0))
        if not lo <= center < hi:
            raise SystemExit(f"--locus {name}: {contig}:{center} is outside the selected "
                             "chromosomes or too close to a chromosome end")
        placed.append((contig, center))
        loci.append((f"named-{index:02d}", "named", contig, center, name))

    bed_strata = []
    for name, count, inside, source in args.stratum:
        if not re.fullmatch(r"[A-Za-z0-9_]+", name):
            raise SystemExit(f"stratum name {name!r} may contain only letters, digits, and _")
        count, inside = int(count), int(inside)
        if source == "genome":
            ranges = {contig: [bounds] for contig, bounds in chrom_bounds.items()}
            stratum = None
        else:
            stratum = BedStratum(name, source.split(","), chrom_sizes)
            ranges = {}
            for contig, regions in stratum.regions.items():
                for earlier in bed_strata:
                    regions = subtract(regions, earlier.regions.get(contig, []))
                ranges[contig] = eligible_centers(regions, inside, *chrom_bounds[contig])
        for index, (contig, center) in enumerate(
                draw(rng, ranges, count, spacing, placed, name), start=1):
            # Genome loci are annotated once every BED stratum has been read.
            annotation = None if stratum is None else stratum.label_at(contig, center)
            loci.append((f"{name}-{index:02d}", name, contig, center, annotation))
        if stratum is not None:
            bed_strata.append(stratum)

    loci = [row if row[4] is not None else row[:4] + (
                ",".join(other.name for other in bed_strata
                         if contains(other.regions.get(row[2], []), row[3])) or "none",)
            for row in loci]

    with open(args.output, "w") as out:
        out.write("query_id\tlocus_id\treference\tcontig\tstart\tend\n")
        for locus_id, _, contig, center, _ in loci:
            for size in sizes:
                start = min(max(center - size // 2, 0), chrom_sizes[contig] - size)
                out.write(f"{locus_id}_{size_label(size)}\t{locus_id}\t{args.reference}\t"
                          f"{contig}\t{start}\t{start + size}\n")
    if args.loci_out:
        with open(args.loci_out, "w") as out:
            out.write("locus_id\tstratum\tcontig\tcenter\tannotation\n")
            for row in loci:
                out.write("\t".join(map(str, row)) + "\n")
    counts = defaultdict(int)
    for _, stratum, *_ in loci:
        counts[stratum] += 1
    print(f"{len(loci)} loci x {len(sizes)} sizes = {len(loci) * len(sizes)} intervals: " +
          ", ".join(f"{name} {count}" for name, count in counts.items()), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
