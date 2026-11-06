#!/usr/bin/env python3
"""A/B query benchmark: same queries against two gfaidx binary + index pairs.

Usage:
    python3 scripts/pdx_ab_benchmark.py \\
        --side base:/path/gfaidx_old:/idx/old/graph.gz \\
        --side new:/path/gfaidx_new:/idx/new/graph.gz \\
        --queries queries.tsv --repeats 5 --cold --out results.tsv

queries.tsv has three tab-separated columns: query_id, query_type, and the
gfaidx arguments. "{GZ}" is replaced by each side's indexed graph and "{OUT}"
by a per-run output file; without "{OUT}" the query's stdout is the output.

For every query and side the script runs one cold-cache run (with --cold:
the side's graph and sidecar files are evicted from the page cache with
posix_fadvise(DONTNEED) first, which needs no root), one untimed warm-up and
--repeats timed warm runs, alternating sides. The output of every run is
hashed; any difference from the first side is reported as a mismatch, so the
same run proves byte-identical results and measures latency.

The raw per-run table goes to --out; a per-query-type summary (p50/p95 over
queries of each query's median warm time, cold time, and max RSS) is printed.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shlex
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass

SIDECARS = ["", ".idx", ".ndx", ".pdx", ".lnx", ".pcx", ".cdx"]


@dataclass
class Side:
    label: str
    binary: str
    graph: str


def parse_side(text: str) -> Side:
    parts = text.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError("--side must be label:binary:indexed_graph.gz")
    return Side(*parts)


def evict(side: Side) -> None:
    for suffix in SIDECARS:
        path = side.graph + suffix
        if not os.path.exists(path):
            continue
        fd = os.open(path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)


def run_once(side: Side, args: str, workdir: str) -> dict:
    out_path = os.path.join(workdir, f"{side.label}.out")
    if os.path.exists(out_path):
        os.remove(out_path)
    argv = [side.binary] + shlex.split(args.replace("{GZ}", side.graph).replace("{OUT}", out_path))
    stdout_path = os.path.join(workdir, f"{side.label}.stdout")
    with open(stdout_path, "wb") as stdout:
        start = time.perf_counter()
        proc = subprocess.Popen(argv, stdout=stdout, stderr=subprocess.DEVNULL)
        _, status, usage = os.wait4(proc.pid, 0)
        elapsed = time.perf_counter() - start
        proc.returncode = os.waitstatus_to_exitcode(status)
    result_path = out_path if "{OUT}" in args else stdout_path
    digest = hashlib.sha1()
    size = 0
    if os.path.exists(result_path):
        with open(result_path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
                size += len(chunk)
    return {"elapsed": elapsed, "maxrss_kb": usage.ru_maxrss, "status": proc.returncode,
            "sha1": digest.hexdigest(), "bytes": size}


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    index = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
    return ordered[index]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--side", type=parse_side, action="append", required=True)
    ap.add_argument("--queries", required=True)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--cold", action="store_true", help="add one cold-cache run per query and side")
    ap.add_argument("--out", required=True, help="raw per-run TSV")
    args = ap.parse_args()
    if len(args.side) < 2:
        ap.error("give at least two --side values")

    queries = []
    with open(args.queries) as handle:
        for line in handle:
            if not line.strip() or line.startswith("#") or line.startswith("query_id\t"):
                continue
            qid, qtype, qargs = line.rstrip("\n").split("\t", 2)
            queries.append((qid, qtype, qargs))

    rows = []
    mismatches = []
    failures = []
    with tempfile.TemporaryDirectory(prefix="pdx_ab_") as workdir, open(args.out, "w") as out:
        out.write("query_id\tquery_type\tside\tmode\trun\telapsed_s\tmaxrss_kb\tstatus\tbytes\tsha1\n")

        def record(qid, qtype, side, mode, run, result):
            rows.append((qid, qtype, side.label, mode, result))
            out.write(f"{qid}\t{qtype}\t{side.label}\t{mode}\t{run}\t{result['elapsed']:.6f}\t"
                      f"{result['maxrss_kb']}\t{result['status']}\t{result['bytes']}\t{result['sha1']}\n")
            if result["status"] != 0:
                failures.append((qid, side.label))

        for number, (qid, qtype, qargs) in enumerate(queries, 1):
            digests = {}
            if args.cold:
                for side in args.side:
                    evict(side)
                    result = run_once(side, qargs, workdir)
                    record(qid, qtype, side, "cold", 0, result)
                    digests[side.label] = result["sha1"]
            for side in args.side:
                result = run_once(side, qargs, workdir)  # warm-up, not timed
                digests.setdefault(side.label, result["sha1"])
            for run in range(1, args.repeats + 1):
                order = args.side if run % 2 else list(reversed(args.side))
                for side in order:
                    result = run_once(side, qargs, workdir)
                    record(qid, qtype, side, "warm", run, result)
                    if result["sha1"] != digests[side.label]:
                        mismatches.append((qid, side.label, "nondeterministic output"))
            reference = digests[args.side[0].label]
            for side in args.side[1:]:
                if digests[side.label] != reference:
                    mismatches.append((qid, side.label, f"differs from {args.side[0].label}"))
            out.flush()
            print(f"[{number}/{len(queries)}] {qid}", file=sys.stderr)

    print(f"queries {len(queries)}  output mismatches {len(mismatches)}  failed runs {len(failures)}")
    for qid, label, why in mismatches[:20]:
        print(f"  MISMATCH {qid} {label}: {why}")
    for qid, label in failures[:20]:
        print(f"  FAILED {qid} {label}")

    labels = [side.label for side in args.side]
    types = sorted({qtype for _, qtype, _ in queries})
    header = ["query_type", "n"]
    for label in labels:
        header += [f"{label}_warm_p50", f"{label}_warm_p95"]
        if args.cold:
            header += [f"{label}_cold_p50", f"{label}_cold_p95"]
        header += [f"{label}_rss_p50_mb"]
    for label in labels[1:]:
        header += [f"{label}/{labels[0]}_warm_p50"]
        if args.cold:
            header += [f"{label}/{labels[0]}_cold_p50"]
    print("\t".join(header))
    for qtype in types:
        cells = [qtype]
        per_label = {}
        for label in labels:
            warm = {}
            cold = []
            rss = []
            for qid, rtype, rlabel, mode, result in rows:
                if rtype != qtype or rlabel != label:
                    continue
                if mode == "warm":
                    warm.setdefault(qid, []).append(result["elapsed"])
                    rss.append(result["maxrss_kb"] / 1024.0)
                else:
                    cold.append(result["elapsed"])
            medians = [statistics.median(v) for v in warm.values()]
            per_label[label] = (medians, cold, rss)
        cells.append(str(len(per_label[labels[0]][0])))
        for label in labels:
            medians, cold, rss = per_label[label]
            cells += [f"{pct(medians, 0.5):.4f}", f"{pct(medians, 0.95):.4f}"]
            if args.cold:
                cells += [f"{pct(cold, 0.5):.4f}", f"{pct(cold, 0.95):.4f}"]
            cells += [f"{pct(rss, 0.5):.1f}"]
        base_medians, base_cold, _ = per_label[labels[0]]
        for label in labels[1:]:
            medians, cold, _ = per_label[label]
            cells.append(f"{pct(medians, 0.5) / pct(base_medians, 0.5):.3f}")
            if args.cold:
                cells.append(f"{pct(cold, 0.5) / pct(base_cold, 0.5):.3f}")
        print("\t".join(cells))
    return 1 if mismatches or failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
