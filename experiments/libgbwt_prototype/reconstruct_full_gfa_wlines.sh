#!/bin/bash
# Reconstruct a complete GFA (H/S/L + real W-lines) from a gfaidx-indexed
# graph that uses W-lines (walks), via gfaidx's own get_path command.
# Needed because gfaidx's community-chunked .gz output only ever contains
# H/S/L lines (see split_gfa_to_comms.cpp) -- vg gbwt needs the real P/W
# lines. Used as-is to build chr22_full.gfa; edit GRAPH/OUT/WLINES below
# for a different graph. Slow: ~1 get_path call per path (~19min for
# chr22's 6,401 walks).
set -e
cd /home/user3/tools/gfaidx
GRAPH=/home/user3/graphs/hprc_v2.0_chr22/hprc-v2.0-mc-chm13_chr22.indexed.gfa.gz
OUT=/tmp/gbwt_test/chr22_full.gfa
WLINES=/tmp/gbwt_test/chr22_wlines.tsv

echo "$(date) extracting W-line identifiers (record_type sample hap seq_id start end)"
./build/gfaidx get_path "$GRAPH" --print_path_names 2>/dev/null | grep "^W" > "$WLINES"
N=$(wc -l < "$WLINES")
echo "$(date) $N W-line identifiers found"

echo "$(date) extracting H/S/L lines as base"
zcat "$GRAPH" | grep -E "^[HSL]" > "$OUT"

echo "$(date) appending all $N path records"
i=0
while IFS=$'\t' read -r rtype sample hap seqid start end; do
  pid="${sample}|${hap}|${seqid}|${start}|${end}"
  ./build/gfaidx get_path "$GRAPH" --path_id "$pid" 2>/dev/null | grep "^[PW]" >> "$OUT"
  i=$((i+1))
  if [ $((i % 500)) -eq 0 ]; then
    echo "$(date)   $i/$N paths appended, current size: $(du -h "$OUT" | cut -f1)"
  fi
done < "$WLINES"

echo "$(date) DONE reconstructing. Final size:"
ls -la "$OUT"
echo "RECONSTRUCT_DONE"
