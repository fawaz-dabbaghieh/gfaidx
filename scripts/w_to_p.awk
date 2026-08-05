#!/usr/bin/awk -f

# Convert GFA 1.1 W-lines into GFA 1.0 P-lines for tools such as ODGI.
# Usage:
#   awk -f scripts/w_to_p.awk input.gfa > output.gfa
#
# Behavior:
# - W <sample> <hap> <seq_id> <start> <end> <walk> [tags...]
#   becomes
# - P <sample>#<hap>#<seq_id>:<start>-<end> <segments> * [tags...]
# - H-line VN:Z tags are changed to VN:Z:1.0.
# - All non-W lines are printed unchanged.
#
# Important: the :start-end suffix preserves the source W interval in the path
# name, but GFA P paths and ODGI still number positions locally from zero.

BEGIN {
    FS = OFS = "\t"
}

function fail(message) {
    print "w_to_p.awk: " message > "/dev/stderr"
    exit 1
}

function walk_to_p_segments(walk,    i, c, orient, node_name, out, first) {
    orient = ""
    node_name = ""
    out = ""
    first = 1

    if (walk == "") {
        fail("empty W walk on input line " NR)
    }

    for (i = 1; i <= length(walk); ++i) {
        c = substr(walk, i, 1)
        if (c == ">" || c == "<") {
            if (orient != "") {
                if (node_name == "") {
                    fail("empty node name in W walk on input line " NR)
                }
                out = out (first ? "" : ",") node_name (orient == ">" ? "+" : "-")
                first = 0
            }
            orient = c
            node_name = ""
        } else {
            if (orient == "") {
                fail("W walk must begin with > or < on input line " NR)
            }
            node_name = node_name c
        }
    }

    if (orient == "" || node_name == "") {
        fail("malformed W walk on input line " NR)
    }
    out = out (first ? "" : ",") node_name (orient == ">" ? "+" : "-")

    return out
}

$1 == "H" {
    # ODGI imports the strict GFA 1.0 subset, whose paths are P records.
    for (i = 2; i <= NF; ++i) {
        if ($i ~ /^VN:Z:/) {
            $i = "VN:Z:1.0"
        }
    }
    print
    next
}

$1 == "W" {
    if (NF < 7) {
        fail("W record has fewer than 7 fields on input line " NR)
    }
    if ($2 == "" || $3 == "" || $4 == "") {
        fail("W sample, haplotype, and sequence fields must be non-empty on input line " NR)
    }
    if (($5 == "*") != ($6 == "*")) {
        fail("W SeqStart and SeqEnd must either both be integers or both be * on input line " NR)
    }

    if ($5 == "*") {
        # An unpositioned W can still become a local P path, but it has no
        # source interval to preserve in the name.
        path_name = $2 "#" $3 "#" $4
    } else {
        if ($5 !~ /^[0-9]+$/ || $6 !~ /^[0-9]+$/ || $6 + 0 <= $5 + 0) {
            fail("W SeqStart/SeqEnd must be increasing integers on input line " NR)
        }
        path_name = $2 "#" $3 "#" $4 ":" $5 "-" $6
    }
    if (path_name in seen_path_names) {
        fail("duplicate converted path name '" path_name "' on input line " NR)
    }
    seen_path_names[path_name] = 1
    segments = walk_to_p_segments($7)

    printf "P%s%s%s*", OFS, path_name, OFS segments OFS
    if (NF > 7) {
        for (i = 8; i <= NF; ++i) {
            printf "%s%s", OFS, $i
        }
    }
    printf "\n"
    next
}

$1 == "P" {
    if (NF >= 2) {
        if ($2 in seen_path_names) {
            fail("duplicate P path name '" $2 "' on input line " NR)
        }
        seen_path_names[$2] = 1
    }
    print
    next
}

{
    print
}
