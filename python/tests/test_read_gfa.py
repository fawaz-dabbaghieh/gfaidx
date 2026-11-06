"""Graph.read_gfa reads GFA 1 and rejects GFA 2 and malformed records with a
Python exception."""
import pytest

import pygfaidx

VALID = ("H\tVN:Z:1.2\nS\t1\tACGT\nS\t2\tGG\nL\t1\t+\t2\t+\t0M\n"
         "P\tp1\t1+,2+\t*\nW\ts1\t1\tchr1\t0\t6\t>1>2\n")


def read(tmp_path, text):
    path = tmp_path / "graph.gfa"
    path.write_text(text, newline="")
    return pygfaidx.Graph.read_gfa(str(path))


def test_reads_gfa_1(tmp_path):
    graph = read(tmp_path, VALID)
    assert graph.node_count == 2


@pytest.mark.parametrize("text, error, message", [
    ("H\tVN:Z:2.0\nS\t1\t4\tACGT\n", RuntimeError, "GFA version 2 is not supported"),
    ("S\t1\n", RuntimeError, "Malformed S record"),
    ("S\t1\tA\nS\t2\tC\nL\t1\tx\t2\t+\t0M\n", RuntimeError, "Malformed L record"),
    ("S\t1\tA\nL\t1\t+\t9\t+\t0M\n", ValueError, "endpoints must exist"),
    ("S\t1\tA\nP\tp1\t1\t*\n", RuntimeError, "Malformed P step"),
    ("S\t1\tA\nP\tp1\t9+\t*\n", ValueError, "missing node: 9"),
    ("S\t1\tA\nW\ts\tx\tc\t0\t1\t>1\n", RuntimeError, "Invalid W haplotype"),
    ("S\t1\tA\nW\ts\t1\tc\t5\t1\t>1\n", RuntimeError, "W end is before W start"),
    ("S\t1\tA\nS\t1\tC\n", ValueError, "Duplicate node"),
    ("S\t1\tA\nP\tp1\t1+\t*\nP\tp1\t1+\t*\n", ValueError, "Duplicate path"),
])
def test_rejects_malformed_records(tmp_path, text, error, message):
    with pytest.raises(error, match=message):
        read(tmp_path, text)
