// Standalone prototype: given a raw segment-ID range, find which haplotype
// paths pass through it and reconstruct the contiguous run(s), using
// libgbwt directly. Deliberately simple (full extract() + in-memory scan)
// -- correctness first, not performance. See docs/gbwt-path-index-exploration.md.
//
// Node identity note: this graph's segment names are numeric, and GBWT's
// internal node id for the forward strand of segment S is 2*S (bidirectional
// GBWT convention: forward = 2*v, reverse = 2*v+1). Confirmed against this
// file's own header (offset/alphabet_size = 2x the known segment id range).
// This shortcut does NOT generalize to non-numeric segment IDs.
//
// Orientation note (found via a benchmark diff against gfaidx at larger N,
// see docs/gbwt-path-index-exploration.md): a segment must be treated as
// "in the target set" under BOTH its forward (2*S) and reverse (2*S+1) GBWT
// node ids. A haplotype can traverse any segment in either orientation, and
// an earlier version of this file only ever inserted 2*S -- silently
// missing (not just mis-scanning) any sequence that touched a target
// segment exclusively via its reverse strand, and truncating runs at any
// reverse-oriented step even in sequences that were otherwise discovered.

#include <gbwt/gbwt.h>

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <vector>

using namespace gbwt;

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <gbwt-file> <node-id-list-file>\n";
    std::cerr << "  node-id-list-file: one raw segment id per line\n";
    return 1;
  }

  std::string gbwt_path = argv[1];
  std::string node_list_path = argv[2];

  GBWT index;
  {
    std::ifstream in(gbwt_path, std::ios::binary);
    if (!in) {
      std::cerr << "Could not open " << gbwt_path << "\n";
      return 1;
    }
    index.simple_sds_load(in);
  }
  std::cerr << "Loaded GBWT: " << index.sequences() << " sequences, "
            << index.size() << " total length\n";

  // Build target node set (BOTH orientations per segment -- see the
  // orientation note above) from an arbitrary list of raw segment ids --
  // real BFS neighborhoods are not contiguous ranges.
  std::set<node_type> target_nodes;
  {
    std::ifstream nl(node_list_path);
    size_type seg;
    while (nl >> seg) {
      for (node_type n : {2 * seg, 2 * seg + 1}) {
        if (index.contains(n)) { target_nodes.insert(n); }
      }
    }
  }
  std::cerr << "Target node list: " << target_nodes.size() << " nodes present in the index\n";

  // For each target node, locate() all touching sequences.
  std::set<size_type> touching_sequences;
  for (node_type n : target_nodes) {
    size_type sz = index.nodeSize(n);
    if (sz == 0) { continue; }
    std::vector<size_type> hits = index.locate(n, range_type(0, sz - 1));
    for (size_type s : hits) { touching_sequences.insert(s); }
  }
  std::cerr << "Distinct GBWT sequences (both strands) touching target range: "
            << touching_sequences.size() << "\n";

  // For each touching FORWARD-strand sequence, extract the full path and
  // report contiguous run(s) within the target node set.
  int runs_reported = 0;
  for (size_type seq : touching_sequences) {
    if (seq % 2 != 0) { continue; }  // skip reverse-strand duplicates
    vector_type path = index.extract(seq);

    size_type i = 0;
    while (i < path.size()) {
      if (target_nodes.count(path[i])) {
        size_type start = i;
        while (i < path.size() && target_nodes.count(path[i])) { i++; }
        // run is path[start, i)
        FullPathName fpn = index.metadata.fullPath(seq / 2);
        std::cout << fpn.sample_name << "\t" << fpn.haplotype << "\t" << fpn.contig_name
                  << "\tpath_seq=" << (seq / 2)
                  << "\tstep_range=[" << start << "," << i << ")"
                  << "\tfirst_segment=" << (path[start] / 2)
                  << "\tlast_segment=" << (path[i - 1] / 2)
                  << "\trun_length=" << (i - start) << "\n";
        runs_reported++;
      } else {
        i++;
      }
    }
  }
  std::cerr << "Total contiguous runs reported: " << runs_reported << "\n";

  return 0;
}
