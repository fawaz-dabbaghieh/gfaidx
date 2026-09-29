// Disk-seeking variant of query_subgraph.cpp -- proves that GBWT's default
// (version <=5, non-zstd) on-disk format is genuinely seekable, not just
// seekable in theory. See docs/gbwt-path-index-exploration.md, "Attempt 2 /
// Problem B" for the source-level argument this implements, and the
// "Disk-resident reader -- validated" section for the result of running
// this file.
//
// Design: load only the structures whose size is O(node_count) or
// O(path_count) into RAM eagerly -- the header, tags, the bwt's `index`
// (an Elias-Fano sd_vector giving each node's byte range, not its bytes),
// da_samples, metadata, and the endmarker record. The bwt's flat `data`
// byte blob -- the one thing that scales with total graph+path-collection
// size -- is never materialized. Each record is fetched with exactly one
// pread() into a small per-record buffer on demand, then handed to GBWT's
// own real `CompressedRecord` to decode (its constructor only reads bytes
// within [start, limit) of whatever buffer it's given -- confirmed from
// jltsiren/gbwt's src/support.cpp -- so a small on-demand buffer works
// exactly like the full eager one).
//
// SeekableGBWT::load() and ::locate() are close, deliberate ports of
// GBWT::load() and GBWT::locate(SearchState) (src/gbwt.cpp), and ::extract()
// mirrors gbwt::extract() (include/gbwt/algorithms.h). The only functional
// change in each is replacing GBWT::record()'s
// `CompressedRecord(this->bwt.data, start, limit)` (indexing into the fully
// loaded blob) with SeekableGBWT::fetchRecord()'s pread() into a small
// buffer. Everything else -- the batched multi-position locate() loop, the
// LF-mapping walk in extract() -- is copied as-is so the disk-seeking
// version has the same semantics and the same performance shape (one
// record fetch per node per round, not per offset) as the real thing.

#include <gbwt/gbwt.h>

#include <algorithm>
#include <cstdint>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

using namespace gbwt;

class SeekableGBWT
{
public:
  GBWTHeader header;
  Tags tags;
  RecordArray bwt_shell;   // index loaded; data left empty, never materialized
  DASamples da_samples;
  Metadata metadata;
  DecompressedRecord endmarker_record;

  size_t bwt_data_file_offset = 0;
  size_t bwt_data_len = 0;
  int fd = -1;

  size_t record_fetches = 0;
  size_t bytes_fetched = 0;

  ~SeekableGBWT() { if (this->fd >= 0) { ::close(this->fd); } }

  void load(const std::string& filename)
  {
    std::ifstream in(filename, std::ios::binary);
    if (!in) { throw std::runtime_error("SeekableGBWT: cannot open " + filename); }

    GBWTHeader h = sdsl::simple_sds::load_value<GBWTHeader>(in);
    h.check();
    bool simple_sds = h.get(GBWTHeader::FLAG_SIMPLE_SDS);
    bool has_tags = h.version >= GBWTHeader::TAGS_VERSION;
    bool zstd_bwt = h.version >= GBWTHeader::ZSTD_VERSION;
    if (!simple_sds) { throw std::runtime_error("SeekableGBWT: only simple-sds format files are supported"); }
    if (zstd_bwt) { throw std::runtime_error("SeekableGBWT: zstd-compressed BWT (version >= 6) is not seekable; rebuild with the default version (<=5)"); }
    h.unset(GBWTHeader::FLAG_SIMPLE_SDS);
    h.setVersion();
    this->header = h;

    bool source_is_familiar = false;
    if (has_tags)
    {
      this->tags.simple_sds_load(in);
      std::string source = this->tags.get(Version::SOURCE_KEY);
      if (source == Version::SOURCE_VALUE || source == Version::SOURCE_GBWT_RS) { source_is_familiar = true; }
    }

    // --- bwt: load only the small index; skip the flat data blob ---
    this->bwt_shell.index.simple_sds_load(in);
    this->bwt_shell.records = this->bwt_shell.index.ones();
    sdsl::util::init_support(this->bwt_shell.select, &(this->bwt_shell.index));
    {
      size_t n = sdsl::simple_sds::load_value<size_t>(in);  // byte length of the data blob
      this->bwt_data_len = n;
      this->bwt_data_file_offset = static_cast<size_t>(in.tellg());
      size_t padded = 8 * ((n + 7) / 8);  // simple-sds pads data to a multiple of 8 bytes
      in.seekg(static_cast<std::streamoff>(padded), std::ios::cur);
    }
    if (this->bwt_shell.size() != this->effective())
    {
      throw std::runtime_error("SeekableGBWT: BWT record count / alphabet size mismatch");
    }

    this->fd = ::open(filename.c_str(), O_RDONLY);
    if (this->fd < 0) { throw std::runtime_error("SeekableGBWT: cannot open " + filename + " for pread"); }

    // Cache the endmarker (proportional to sequence count, not data size --
    // same cost GBWT::cacheEndmarker() eagerly pays on every load).
    {
      std::vector<byte_type> buf;
      this->endmarker_record = DecompressedRecord(this->fetchRecord(ENDMARKER, buf));
    }

    // --- da_samples: small (O(node_count + sample_count)), load eagerly ---
    bool found_samples = false;
    if (source_is_familiar) { found_samples = sdsl::simple_sds::load_option(this->da_samples, in); }
    else { sdsl::simple_sds::skip_option(in); }
    if (!found_samples)
    {
      throw std::runtime_error("SeekableGBWT: no DA samples from a recognized source; resampling not implemented in this prototype");
    }
    if (this->da_samples.records() != this->effective())
    {
      throw std::runtime_error("SeekableGBWT: sample record count / alphabet size mismatch");
    }

    // --- metadata: small (O(path_count)), load eagerly ---
    sdsl::simple_sds::load_option(this->metadata, in);
  }

  size_type effective() const { return this->header.alphabet_size - this->header.offset; }
  size_type sequences() const { return this->header.sequences; }
  size_type sigma() const { return this->header.alphabet_size; }
  comp_type toComp(node_type node) const { return (node == 0 ? node : node - this->header.offset); }

  bool contains(node_type node) const
  {
    return ((node < this->sigma() && node > this->header.offset) || node == ENDMARKER);
  }

  // The one place this reader touches the file outside of load(): one
  // pread() for exactly this record's bytes, then GBWT's own CompressedRecord
  // decodes them. `buf` is caller-owned and must outlive the returned
  // CompressedRecord (its `body` pointer aliases buf.data()).
  CompressedRecord fetchRecord(node_type node, std::vector<byte_type>& buf)
  {
    comp_type comp = this->toComp(node);
    std::pair<size_type, size_type> range = this->bwt_shell.getRange(comp);
    size_t len = range.second - range.first;
    buf.resize(len);
    if (len > 0)
    {
      ssize_t got = ::pread(this->fd, buf.data(), len, static_cast<off_t>(this->bwt_data_file_offset + range.first));
      if (got < 0 || static_cast<size_t>(got) != len)
      {
        throw std::runtime_error("SeekableGBWT: pread failed or short read while fetching a BWT record");
      }
    }
    this->record_fetches++;
    this->bytes_fetched += len;
    return CompressedRecord(buf, 0, buf.size());
  }

  size_type nodeSize(node_type node)
  {
    std::vector<byte_type> buf;
    CompressedRecord rec = this->fetchRecord(node, buf);
    return rec.size();
  }

  edge_type LF(edge_type position)
  {
    if (position.first == ENDMARKER) { return this->endmarker_record.LF(position.second); }
    std::vector<byte_type> buf;
    CompressedRecord rec = this->fetchRecord(position.first, buf);
    return rec.LF(position.second);
  }

  edge_type start(size_type sequence) { return this->LF(edge_type(ENDMARKER, sequence)); }

  // Port of GBWT::locate(SearchState) (src/gbwt.cpp): batches all offsets in
  // the range together and re-sorts by (node, offset) after each LF() round,
  // so a node touched by several in-flight offsets is fetched once per
  // round, not once per offset -- same record-fetch shape as the real thing.
  std::vector<size_type> locate(node_type node, range_type range)
  {
    std::vector<size_type> result;
    SearchState state(node, range);
    if (!(this->contains(node) && !state.empty() && state.range.second < this->nodeSize(node)))
    {
      return result;
    }

    std::vector<edge_type> positions(state.size());
    for (size_type i = state.range.first; i <= state.range.second; i++)
    {
      positions[i - state.range.first] = edge_type(state.node, i);
    }

    while (!positions.empty())
    {
      size_type tail = 0;
      node_type curr = invalid_node();
      std::vector<byte_type> current_buf;
      CompressedRecord current;
      sample_type sample;
      edge_type LF_result;
      range_type LF_range;

      for (size_type i = 0; i < positions.size(); i++)
      {
        if (positions[i].first != curr)
        {
          curr = positions[i].first;
          current = this->fetchRecord(curr, current_buf);
          sample = this->da_samples.nextSample(this->toComp(curr), positions[i].second);
          LF_range.first = positions[i].second;
          LF_result = current.runLF(positions[i].second, LF_range.second);
        }
        if (sample.first < positions[i].second)
        {
          sample = this->da_samples.nextSample(this->toComp(curr), positions[i].second);
        }
        if (sample.first > positions[i].second)
        {
          if (positions[i].second > LF_range.second)
          {
            LF_range.first = positions[i].second;
            LF_result = current.runLF(positions[i].second, LF_range.second);
          }
          positions[tail] = edge_type(LF_result.first, LF_result.second + positions[i].second - LF_range.first);
          tail++;
        }
        else
        {
          result.push_back(sample.second);
        }
      }
      positions.resize(tail);
      std::sort(positions.begin(), positions.end());
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
  }

  // Port of gbwt::extract(index, sequence) (include/gbwt/algorithms.h).
  vector_type extract(size_type sequence)
  {
    vector_type result;
    if (sequence >= this->sequences()) { return result; }
    edge_type position = this->start(sequence);
    while (position.first != ENDMARKER)
    {
      result.push_back(position.first);
      position = this->LF(position);
    }
    return result;
  }
};

int main(int argc, char** argv)
{
  if (argc != 3)
  {
    std::cerr << "Usage: " << argv[0] << " <gbwt-file> <node-id-list-file>\n";
    std::cerr << "  node-id-list-file: one raw segment id per line\n";
    return 1;
  }

  std::string gbwt_path = argv[1];
  std::string node_list_path = argv[2];

  SeekableGBWT index;
  index.load(gbwt_path);
  std::cerr << "Loaded SeekableGBWT (index/samples/metadata only): "
            << index.sequences() << " sequences, header-reported length "
            << index.header.size << "\n";
  std::cerr << "BWT data blob left on disk: " << index.bwt_data_len << " bytes at file offset "
            << index.bwt_data_file_offset << "\n";

  // Both orientations per segment: a haplotype can traverse any segment
  // forward (2*seg) or reverse (2*seg+1), and a segment must count as "in
  // the target set" under either. Found via a benchmark diff against
  // gfaidx at larger N -- see the orientation note in query_subgraph.cpp
  // and docs/gbwt-path-index-exploration.md.
  std::set<node_type> target_nodes;
  {
    std::ifstream nl(node_list_path);
    size_type seg;
    while (nl >> seg)
    {
      for (node_type n : {2 * seg, 2 * seg + 1})
      {
        if (index.contains(n)) { target_nodes.insert(n); }
      }
    }
  }
  std::cerr << "Target node list: " << target_nodes.size() << " nodes present in the index\n";

  std::set<size_type> touching_sequences;
  for (node_type n : target_nodes)
  {
    size_type sz = index.nodeSize(n);
    if (sz == 0) { continue; }
    std::vector<size_type> hits = index.locate(n, range_type(0, sz - 1));
    for (size_type s : hits) { touching_sequences.insert(s); }
  }
  std::cerr << "Distinct GBWT sequences (both strands) touching target range: "
            << touching_sequences.size() << "\n";

  int runs_reported = 0;
  for (size_type seq : touching_sequences)
  {
    if (seq % 2 != 0) { continue; }
    vector_type path = index.extract(seq);

    size_type i = 0;
    while (i < path.size())
    {
      if (target_nodes.count(path[i]))
      {
        size_type start = i;
        while (i < path.size() && target_nodes.count(path[i])) { i++; }
        FullPathName fpn = index.metadata.fullPath(seq / 2);
        std::cout << fpn.sample_name << "\t" << fpn.haplotype << "\t" << fpn.contig_name
                  << "\tpath_seq=" << (seq / 2)
                  << "\tstep_range=[" << start << "," << i << ")"
                  << "\tfirst_segment=" << (path[start] / 2)
                  << "\tlast_segment=" << (path[i - 1] / 2)
                  << "\trun_length=" << (i - start) << "\n";
        runs_reported++;
      }
      else { i++; }
    }
  }
  std::cerr << "Total contiguous runs reported: " << runs_reported << "\n";
  std::cerr << "Record fetches (pread calls): " << index.record_fetches
            << ", bytes fetched: " << index.bytes_fetched
            << " (" << (100.0 * index.bytes_fetched / std::max<size_t>(index.bwt_data_len, 1))
            << "% of the " << index.bwt_data_len << "-byte data blob)\n";

  return 0;
}
