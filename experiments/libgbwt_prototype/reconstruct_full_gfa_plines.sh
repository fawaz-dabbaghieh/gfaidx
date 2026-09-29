#!/bin/bash
# Reconstruct a complete GFA (H/S/L + real P-lines) from a gfaidx-indexed
# graph that uses P-lines (paths), via gfaidx's own get_path command. See
# reconstruct_full_gfa_wlines.sh for the W-line (walk) equivalent, and the
# comment there on why this is needed at all. Used as-is to build
# chr1_full.gfa; edit GRAPH/OUT/PNAMES below for a different graph. Slow:
# ~1 get_path call per path (~87min for chr1's 2,915 paths).
set -e
cd /home/user3/tools/gfaidx
GRAPH=/home/user3/graphs/hprc2_chr1/20251014_hprc25272.p98-k311.chr1.indexed.gfa.gz
OUT=/tmp/gbwt_test/chr1_full.gfa
PNAMES=/tmp/gbwt_test/chr1_path_names.txt

echo "$(date) extracting path name list"
./build/gfaidx get_path "$GRAPH" --print_path_names 2>/dev/null | grep "^P" | cut -f2 > "$PNAMES"
N=$(wc -l < "$PNAMES")
echo "$(date) $N path names found"

echo "$(date) extracting H/S/L lines as base"
zcat "$GRAPH" | grep -E "^[HSL]" > "$OUT"

echo "$(date) appending all $N path records"
i=0
while IFS= read -r pname; do
  ./build/gfaidx get_path "$GRAPH" --path_id "$pname" 2>/dev/null | grep "^P" >> "$OUT"
  i=$((i+1))
  if [ $((i % 200)) -eq 0 ]; then
    echo "$(date)   $i/$N paths appended, current size: $(du -h "$OUT" | cut -f1)"
  fi
done < "$PNAMES"

echo "$(date) DONE reconstructing. Final size:"
ls -la "$OUT"
echo "RECONSTRUCT_DONE"
