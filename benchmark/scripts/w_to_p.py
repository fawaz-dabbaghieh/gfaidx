#!/usr/bin/env python3

"""Convert GFA W records to coordinate-named P records."""

import argparse
import os
import re
import sys
from typing import BinaryIO, List, Optional, Set


# The bytes regular expression scans node-name contents in compiled code. Python
# therefore handles one graph step per iteration instead of one walk character.
STEP_PATTERN = re.compile(rb"[<>]([^<>]+)")
STEPS_PER_OUTPUT_CHUNK = 65536


class ConversionError(RuntimeError):
    """Report malformed GFA input without a Python traceback."""


def write_normalized_line(output: BinaryIO, line: bytes, content_end: int) -> None:
    """Write one record with the LF line ending produced by the AWK converter."""
    output.write(memoryview(line)[:content_end])
    output.write(b"\n")


def convert_walk(output: BinaryIO, walk: memoryview, line_number: int) -> None:
    """Convert one >/< walk into comma-separated +/- P steps."""
    if len(walk) == 0:
        raise ConversionError("empty W walk on input line {}".format(line_number))

    converted_steps: List[bytes] = []
    position = 0
    wrote_steps = False

    for match in STEP_PATTERN.finditer(walk):
        # A gap means the walk did not begin with an orientation or contained
        # characters that were not part of a complete oriented step.
        if match.start() != position:
            raise ConversionError(
                "malformed W walk on input line {}".format(line_number)
            )

        orientation = walk[match.start()]
        node_name = match.group(1)
        converted_steps.append(node_name + (b"+" if orientation == ord(">") else b"-"))
        position = match.end()

        # Bound per-step Python object overhead while still issuing large writes.
        if len(converted_steps) == STEPS_PER_OUTPUT_CHUNK:
            if wrote_steps:
                output.write(b",")
            output.write(b",".join(converted_steps))
            converted_steps.clear()
            wrote_steps = True

    if position != len(walk) or (not wrote_steps and not converted_steps):
        raise ConversionError(
            "malformed W walk on input line {}".format(line_number)
        )

    if converted_steps:
        if wrote_steps:
            output.write(b",")
        output.write(b",".join(converted_steps))


def convert_gfa(
    input_handle: BinaryIO,
    output: BinaryIO,
    mapping_output: Optional[BinaryIO] = None,
) -> None:
    """Stream GFA records and optionally record every W-to-P name mapping."""
    seen_path_names: Set[bytes] = set()

    if mapping_output is not None:
        # This small side table lets downstream tools resolve a W coordinate to
        # the exact P name emitted here without duplicating the naming rules.
        mapping_output.write(
            b"sample\thaplotype\tseq_id\tseq_start\tseq_end\twalk_name\tp_path_name\n"
        )

    for line_number, line in enumerate(input_handle, 1):
        # Locate the logical line end without copying a chromosome-scale W line.
        content_end = len(line)
        if content_end > 0 and line[content_end - 1] == ord("\n"):
            content_end -= 1
        if content_end > 0 and line[content_end - 1] == ord("\r"):
            content_end -= 1

        first_tab = line.find(b"\t", 0, content_end)
        record_type_end = content_end if first_tab < 0 else first_tab
        record_type = line[:record_type_end]

        if record_type == b"H":
            # Header records are short, so rebuilding their field list is cheap.
            fields = line[:content_end].split(b"\t")
            for index in range(1, len(fields)):
                if fields[index].startswith(b"VN:Z:"):
                    fields[index] = b"VN:Z:1.0"
            output.write(b"\t".join(fields))
            output.write(b"\n")
            continue

        if record_type == b"P":
            # Track existing P names without splitting or copying their long
            # segment field, then pass the original record through.
            if first_tab >= 0:
                name_end = line.find(b"\t", first_tab + 1, content_end)
                if name_end < 0:
                    name_end = content_end
                path_name = line[first_tab + 1:name_end]
                if path_name in seen_path_names:
                    raise ConversionError(
                        "duplicate P path name {!r} on input line {}".format(
                            path_name.decode("utf-8", "replace"), line_number
                        )
                    )
                seen_path_names.add(path_name)
            write_normalized_line(output, line, content_end)
            continue

        if record_type != b"W":
            write_normalized_line(output, line, content_end)
            continue

        # A W record has six mandatory tabs before its Walk field. Locate those
        # delimiters without splitting and duplicating the complete walk string.
        tabs = []
        search_start = 0
        for _ in range(6):
            tab = line.find(b"\t", search_start, content_end)
            if tab < 0:
                raise ConversionError(
                    "W record has fewer than 7 fields on input line {}".format(
                        line_number
                    )
                )
            tabs.append(tab)
            search_start = tab + 1

        sample = line[tabs[0] + 1:tabs[1]]
        haplotype = line[tabs[1] + 1:tabs[2]]
        sequence = line[tabs[2] + 1:tabs[3]]
        start = line[tabs[3] + 1:tabs[4]]
        end = line[tabs[4] + 1:tabs[5]]
        if not sample or not haplotype or not sequence:
            raise ConversionError(
                "W sample, haplotype, and sequence fields must be non-empty "
                "on input line {}".format(line_number)
            )

        start_missing = start == b"*"
        end_missing = end == b"*"
        if start_missing != end_missing:
            raise ConversionError(
                "W SeqStart and SeqEnd must either both be integers or both be "
                "* on input line {}".format(line_number)
            )

        if start_missing:
            path_name = b"#".join((sample, haplotype, sequence))
        else:
            if not start.isdigit() or not end.isdigit() or int(end) <= int(start):
                raise ConversionError(
                    "W SeqStart/SeqEnd must be increasing integers on input "
                    "line {}".format(line_number)
                )
            path_name = b"#".join((sample, haplotype, sequence)) + b":" + start + b"-" + end

        if path_name in seen_path_names:
            raise ConversionError(
                "duplicate converted path name {!r} on input line {}".format(
                    path_name.decode("utf-8", "replace"), line_number
                )
            )
        seen_path_names.add(path_name)

        if mapping_output is not None:
            walk_name = b"#".join((sample, haplotype, sequence))
            mapping_output.write(
                b"\t".join(
                    (sample, haplotype, sequence, start, end, walk_name, path_name)
                )
            )
            mapping_output.write(b"\n")

        walk_start = tabs[5] + 1
        tags_start = line.find(b"\t", walk_start, content_end)
        walk_end = content_end if tags_start < 0 else tags_start

        output.write(b"P\t")
        output.write(path_name)
        output.write(b"\t")
        convert_walk(output, memoryview(line)[walk_start:walk_end], line_number)
        output.write(b"\t*")
        if tags_start >= 0:
            output.write(memoryview(line)[tags_start:content_end])
        output.write(b"\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert GFA 1.1 W records to coordinate-named GFA 1.0 P records"
    )
    parser.add_argument("input_gfa", help="plain-text input GFA, or - for stdin")
    parser.add_argument(
        "output_gfa",
        nargs="?",
        default="-",
        help="plain-text output GFA; defaults to stdout",
    )
    parser.add_argument(
        "--mapping-out",
        default="",
        help="optional TSV receiving the exact W-record to P-path name mapping",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if (
        args.input_gfa != "-"
        and args.output_gfa != "-"
        and os.path.abspath(args.input_gfa) == os.path.abspath(args.output_gfa)
    ):
        raise ConversionError("input and output GFA paths must differ")
    if args.mapping_out and args.input_gfa != "-":
        if os.path.abspath(args.input_gfa) == os.path.abspath(args.mapping_out):
            raise ConversionError("input GFA and mapping output paths must differ")
    if args.mapping_out and args.output_gfa != "-":
        if os.path.abspath(args.output_gfa) == os.path.abspath(args.mapping_out):
            raise ConversionError("GFA and mapping output paths must differ")

    input_handle = sys.stdin.buffer if args.input_gfa == "-" else open(args.input_gfa, "rb")
    output_handle = sys.stdout.buffer if args.output_gfa == "-" else open(args.output_gfa, "wb")
    mapping_handle = open(args.mapping_out, "wb") if args.mapping_out else None
    try:
        convert_gfa(input_handle, output_handle, mapping_handle)
    finally:
        if input_handle is not sys.stdin.buffer:
            input_handle.close()
        if output_handle is not sys.stdout.buffer:
            output_handle.close()
        if mapping_handle is not None:
            mapping_handle.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BrokenPipeError:
        # Match ordinary Unix filter behavior when a downstream reader exits.
        sys.exit(0)
    except (ConversionError, OSError) as error:
        print("w_to_p.py: {}".format(error), file=sys.stderr)
        sys.exit(1)
