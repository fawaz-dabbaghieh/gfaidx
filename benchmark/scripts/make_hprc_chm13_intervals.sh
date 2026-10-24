#!/usr/bin/env bash
# Build the stratified CHM13 interval manifests used to benchmark gfaidx against
# gbz-base on the HPRC Minigraph-Cactus graph.
#
# Downloads the annotations it needs into the cache directory, then writes:
#
#   <out>/hprc_chm13_intervals.tsv   full manifest for benchmark_gfaidx_gbz_intervals.sh
#   <out>/hprc_chm13_loci.tsv        stratum and annotation of every locus
#   <out>/hprc_chm13_pilot.tsv       five loci for a short calibration run
#
# Strata, in sampling order (each BED stratum excludes the earlier ones):
#
#   named           fixed loci with well-known complex structural variation
#   random          uniform over chr1-22 and chrX, ignoring annotations
#   centromere_hor  live alpha-satellite HOR arrays (T2T censat v2.1), with the
#                   1 Mb window inside the array
#   pericentromere  other satellite arrays: HSat1-3, beta and gamma satellite,
#                   monomeric, diverged and inactive HORs
#   rdna            rDNA arrays, excluded only
#   acrocentric     short arms of chr13, 14, 15, 21 and 22 outside the above
#   mhc, kir, vdj   immune loci (GIAB v3.3 OtherDifficult)
#   segdup          segmental duplications of at least 10 kb (GIAB v3.3)
#   tandem          tandem repeats longer than 200 bp (GIAB v3.3)
#
# Every locus gets nested windows from 1 kb to 10 Mb. Loci are at least 10 Mb
# apart, so their largest windows do not overlap. Named loci are the midpoints
# of the CHM13 v2.0 liftoff gene spans (UCSC hs1 catLiftOffGenesV1 track);
# AMY1A spans the AMY1 copies.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CACHE_DIR=${1:-chm13_annotations}
OUT_DIR=${2:-$SCRIPT_DIR/intervals}
SEED=${SEED:-20261014}
PYTHON=${PYTHON:-python3}

GIAB=https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/genome-stratifications/v3.3/CHM13%40all
CENSAT=https://s3-us-west-2.amazonaws.com/human-pangenomics/T2T/CHM13/assemblies/annotation/chm13v2.0_censat_v2.1.bed
SIZES=https://hgdownload.soe.ucsc.edu/goldenPath/hs1/bigZips/hs1.chrom.sizes

mkdir -p "$CACHE_DIR" "$OUT_DIR"

fetch() {
    local url=$1 file=$CACHE_DIR/$(basename "$1")
    if [[ ! -s "$file" ]]; then
        # NCBI rejects bursts of requests, so downloads stay sequential.
        curl -fsSL --retry 3 -o "$file.part" "$url"
        mv "$file.part" "$file"
    fi
}

fetch "$SIZES"
fetch "$CENSAT"
for path in \
    OtherDifficult/CHM13_MHC.bed.gz \
    OtherDifficult/CHM13_KIR.bed.gz \
    OtherDifficult/CHM13_VDJ.bed.gz \
    SegmentalDuplications/CHM13_segdups_gt10kb.bed.gz \
    LowComplexity/CHM13_AllTandemRepeats_201to10000bp_slop5.bed.gz \
    LowComplexity/CHM13_AllTandemRepeats_ge10001bp_slop5.bed.gz; do
    fetch "$GIAB/$path"
done

censat=$CACHE_DIR/chm13v2.0_censat_v2.1.bed
# Live arrays carry an L in their HOR name, for example hor_1_5(S1C1/5/19H1L).
awk -F'\t' '$4 ~ /^hor_/ && $4 ~ /L[,)]/' "$censat" > "$CACHE_DIR/centromere_hor.bed"
awk -F'\t' '$4 ~ /^(hor|dhor|mon|hsat|bsat|gsat|censat)/ && !($4 ~ /^hor_/ && $4 ~ /L[,)]/)' \
    "$censat" > "$CACHE_DIR/pericentromere.bed"
awk -F'\t' '$4 ~ /^rDNA/' "$censat" > "$CACHE_DIR/rdna.bed"
# A short arm runs from the chromosome start to its first live HOR array.
awk -F'\t' '$1 ~ /^chr(13|14|15|21|22)$/ && !($1 in arm) {arm[$1] = $2}
            END {for (c in arm) printf "%s\t0\t%d\t%s_p_arm\n", c, arm[c], c}' \
    "$CACHE_DIR/centromere_hor.bed" > "$CACHE_DIR/acrocentric.bed"

generate() {
    "$PYTHON" "$SCRIPT_DIR/make_benchmark_intervals.py" \
        --chrom-sizes "$CACHE_DIR/hs1.chrom.sizes" \
        --reference CHM13 \
        --sizes 1000,10000,100000,1000000,2000000,5000000,10000000 \
        --seed "$SEED" \
        --locus AMY1A chr1 103618756 \
        --locus SMN1 chr5 71396274 \
        --locus LPA chr6 161897466 \
        --locus KANSL1 chr17 46989292 \
        --locus CYP2D6 chr22 42608180 \
        --stratum random 10 1000 genome \
        --stratum centromere_hor 3 1000000 "$CACHE_DIR/centromere_hor.bed" \
        --stratum pericentromere 2 10000 "$CACHE_DIR/pericentromere.bed" \
        --stratum rdna 0 1000 "$CACHE_DIR/rdna.bed" \
        --stratum acrocentric 2 10000 "$CACHE_DIR/acrocentric.bed" \
        --stratum mhc 1 1000 "$CACHE_DIR/CHM13_MHC.bed.gz" \
        --stratum kir 1 1000 "$CACHE_DIR/CHM13_KIR.bed.gz" \
        --stratum vdj 1 1000 "$CACHE_DIR/CHM13_VDJ.bed.gz" \
        --stratum segdup 4 10000 "$CACHE_DIR/CHM13_segdups_gt10kb.bed.gz" \
        --stratum tandem 4 1000 \
            "$CACHE_DIR/CHM13_AllTandemRepeats_201to10000bp_slop5.bed.gz,$CACHE_DIR/CHM13_AllTandemRepeats_ge10001bp_slop5.bed.gz" \
        "$@"
}

generate --output "$OUT_DIR/hprc_chm13_intervals.tsv" --loci-out "$OUT_DIR/hprc_chm13_loci.tsv"

# The pilot covers easy and difficult loci at every size, enough to check
# output parity and to extrapolate the run time of the full manifest.
awk -F'\t' 'NR == 1 || $2 ~ /^(random-0[12]|centromere_hor-01|mhc-01|segdup-01)$/' \
    "$OUT_DIR/hprc_chm13_intervals.tsv" > "$OUT_DIR/hprc_chm13_pilot.tsv"
echo "wrote $OUT_DIR/hprc_chm13_intervals.tsv, hprc_chm13_loci.tsv, and hprc_chm13_pilot.tsv" >&2
