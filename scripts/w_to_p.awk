#!/usr/bin/awk -f

# Convert GFA W-lines into lossy P-lines for visualization/testing.
# Usage:
#   awk -f scripts/w_to_p.awk input.gfa > output.gfa
#
# Behavior:
# - W <sample> <hap> <seq_id> <start> <end> <walk> [tags...]
#   becomes
# - P <sample|hap|seq_id|start|end> <segments> * [tags...]
# - All non-W lines are printed unchanged.

BEGIN {
    FS = OFS = "\t"
}

function walk_to_p_segments(walk,    i, c, orient, node_name, out, first) {
    orient = ""
    node_name = ""
    out = ""
    first = 1

    for (i = 1; i <= length(walk); ++i) {
        c = substr(walk, i, 1)
        if (c == ">" || c == "<") {
            if (orient != "") {
                out = out (first ? "" : ",") node_name (orient == ">" ? "+" : "-")
                first = 0
            }
            orient = c
            node_name = ""
        } else {
            node_name = node_name c
        }
    }

    if (orient != "") {
        out = out (first ? "" : ",") node_name (orient == ">" ? "+" : "-")
    }

    return out
}

$1 == "W" {
    if (NF < 7) {
        print
        next
    }

    path_name = $2 "|" $3 "|" $4 "|" $5 "|" $6
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

{
    print
}
