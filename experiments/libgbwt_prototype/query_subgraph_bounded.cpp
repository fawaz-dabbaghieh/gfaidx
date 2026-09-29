// Fourth experiment: bounded/local extraction instead of GBWT's own
// unconditional extract() -- which walks an ENTIRE haplotype path from its
// true start to its true end regardless of how much of it actually falls
// inside the query's target node set. That unbounded walk was diagnosed
// (via the benchmark in docs/gbwt-path-index-exploration.md) as the
// dominant cost behind GBWT being 30-350x slower than gfaidx at the same
// query. This prototype replaces it with a walk bounded by the query
// itself, to test directly whether that was fixable-bad-fit-code or an
// inherent property of GBWT-style indexes.
//
// Design: for each occurrence (target_node, offset) -- i.e. every position
// where a target node appears in the BWT, both orientations -- walk
// forward via LF() only as long as the next node is also in the target
// set. This produces the SUFFIX of the true maximal run starting at that
// occurrence (a run of length L produces L such overlapping suffixes, one
// per starting occurrence within it -- deliberately not optimized further
// yet, see the design doc). Resolve the occurrence's sequence identity
// with a single-position locate() (bounded by the sample interval, not by
// path length -- this is the same primitive GBWT's own locate(SearchState)
// uses internally, just called once per occurrence instead of batched).
//
// Dedup: all suffixes of the same true maximal run share the same
// sequence id AND the same exit position (the first off-target-set
// position reached, which is identical regardless of which occurrence in
// the run you start walking from, since they all converge onto the same
// forward path). Keying on (sequence, exit_node, exit_offset) and keeping
// only the longest candidate per key recovers the exact true run -- not
// an approximation -- because the longest suffix is exactly the one that
// started at the run's true beginning.
//
// Redundancy fix (added after first measurement showed this can be SLOWER
// than unbounded extract() at larger N): a true run of length L naively
// produces L overlapping suffix-walks -- O(L^2) total LF hops. Fixed with
// a `visited` set of (node, offset) BWT positions: skip starting a walk
// from any position already touched by an earlier walk (from this run or
// any other), marking every position touched as we go. This doesn't
// change which run wins the dedup step (a walk that reaches an
// already-visited-but-not-yet-processed position still completes
// normally; only the OUTER loop's decision to start a fresh walk is
// affected), so correctness is unchanged, but total LF hops become
// O(total occurrences) instead of O(occurrences x average run length).
//
// Known limitation vs. the unbounded prototypes: no absolute step_range
// is available (that would require counting hops from the sequence's true
// start, which is exactly the cost this design avoids). Comparison
// against gfaidx uses (sample, haplotype, contig, first_segment,
// last_segment, run_length) as before, which doesn't need it.

#include <gbwt/gbwt.h>

#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

static inline uint64_t pack_position(gbwt::node_type node, gbwt::size_type offset) {
  return (static_cast<uint64_t>(node) << 32) | (static_cast<uint64_t>(offset) & 0xFFFFFFFFULL);
}

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

  struct Candidate { node_type first_node; node_type last_node; size_type run_length; };
  std::map<std::tuple<size_type, node_type, size_type>, Candidate> best;
  std::unordered_set<uint64_t> visited;

  size_t occurrences_processed = 0;
  size_t occurrences_skipped_visited = 0;
  size_t walk_lf_hops = 0;

  for (node_type n : target_nodes) {
    size_type sz = index.nodeSize(n);
    for (size_type i = 0; i < sz; i++) {
      occurrences_processed++;
      if (visited.count(pack_position(n, i))) { occurrences_skipped_visited++; continue; }

      edge_type pos(n, i);

      // Bounded forward walk: stop the instant we leave the target set.
      // Mark every position touched so no later occurrence re-walks it.
      node_type first_node = n;
      node_type last_node = n;
      size_type run_length = 1;
      edge_type cur = pos;
      edge_type exit_pos;
      visited.insert(pack_position(cur.first, cur.second));
      while (true) {
        edge_type nxt = index.LF(cur);
        walk_lf_hops++;
        if (nxt == invalid_edge() || nxt.first == ENDMARKER) {
          exit_pos = edge_type(ENDMARKER, nxt.second);
          break;
        }
        if (!target_nodes.count(nxt.first)) {
          exit_pos = nxt;
          break;
        }
        visited.insert(pack_position(nxt.first, nxt.second));
        last_node = nxt.first;
        run_length++;
        cur = nxt;
      }

      // Resolve identity for THIS occurrence -- bounded by the sample
      // interval, independent of how long the run or the full path is.
      size_type seq = index.locate(pos);
      if (seq == invalid_sequence() || seq % 2 != 0) { continue; }  // forward-strand only

      auto key = std::make_tuple(seq, exit_pos.first, exit_pos.second);
      auto it = best.find(key);
      if (it == best.end() || it->second.run_length < run_length) {
        best[key] = Candidate{ first_node, last_node, run_length };
      }
    }
  }

  std::cerr << "Occurrences processed: " << occurrences_processed
            << " (skipped as already-visited: " << occurrences_skipped_visited << ")"
            << ", forward-walk LF hops: " << walk_lf_hops << "\n";

  int runs_reported = 0;
  for (auto& kv : best) {
    size_type seq = std::get<0>(kv.first);
    const Candidate& c = kv.second;
    FullPathName fpn = index.metadata.fullPath(seq / 2);
    std::cout << fpn.sample_name << "\t" << fpn.haplotype << "\t" << fpn.contig_name
              << "\tpath_seq=" << (seq / 2)
              << "\tfirst_segment=" << (c.first_node / 2)
              << "\tlast_segment=" << (c.last_node / 2)
              << "\trun_length=" << c.run_length << "\n";
    runs_reported++;
  }
  std::cerr << "Total contiguous runs reported: " << runs_reported << "\n";

  return 0;
}
