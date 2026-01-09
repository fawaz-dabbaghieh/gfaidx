// split_text_then_multimember_gz.cpp
#include <zlib.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

struct IndexEntry {
  std::uint32_t community_id{};
  std::uint64_t gz_offset{};
  std::uint64_t gz_size{};
  std::uint64_t uncompressed_size{};
  std::uint32_t line_count{};
};

static void throw_zlib(const char* where, int zret) {
  throw std::runtime_error(std::string(where) + " (zlib ret=" + std::to_string(zret) + ")");
}

// LRU cache for raw text temp files (safe to close/reopen: no compression state).
class TextHandleCache {
public:
  TextHandleCache(std::vector<fs::path> paths, std::size_t max_open)
      : paths_(std::move(paths)), max_open_(max_open) {}

  void write_line(std::uint32_t cid, std::string_view line_no_nl) {
    std::ofstream& f = get_handle_(cid);
    f.write(line_no_nl.data(), static_cast<std::streamsize>(line_no_nl.size()));
    f.put('\n');
    if (!f) throw std::runtime_error("write failed for " + paths_.at(cid).string());
  }

  void close_all() {
    for (auto& kv : open_) {
      kv.second.file.close();
    }
    open_.clear();
    lru_.clear();
  }

private:
  struct OpenRec {
    std::ofstream file;
    std::list<std::uint32_t>::iterator it;
  };

  std::ofstream& get_handle_(std::uint32_t cid) {
    auto it = open_.find(cid);
    if (it != open_.end()) {
      lru_.erase(it->second.it);
      lru_.push_front(cid);
      it->second.it = lru_.begin();
      return it->second.file;
    }

    // Evict if needed
    if (open_.size() >= max_open_) {
      std::uint32_t evict = lru_.back();
      lru_.pop_back();
      auto eit = open_.find(evict);
      if (eit != open_.end()) {
        eit->second.file.close();
        open_.erase(eit);
      }
    }

    const fs::path& p = paths_.at(cid);
    fs::create_directories(p.parent_path());

    // Append mode; create if missing.
    std::ofstream f(p, std::ios::binary | std::ios::out | std::ios::app);
    if (!f) throw std::runtime_error("Failed to open temp text file: " + p.string());

    lru_.push_front(cid);
    OpenRec rec{std::move(f), lru_.begin()};
    auto [ins_it, ok] = open_.emplace(cid, std::move(rec));
    return ins_it->second.file;
  }

  std::vector<fs::path> paths_;
  std::size_t max_open_;
  std::unordered_map<std::uint32_t, OpenRec> open_;
  std::list<std::uint32_t> lru_;
};

// Stream-compress a whole text file into ONE gzip member appended to `out`.
static void append_one_gzip_member_from_file(std::ofstream& out,
                                            const fs::path& in_path,
                                            int level = 6,
                                            int memLevel = 8) {
  std::ifstream in(in_path, std::ios::binary);
  if (!in) throw std::runtime_error("Failed to open temp text for read: " + in_path.string());

  z_stream strm;
  std::memset(&strm, 0, sizeof(strm));

  // 15+16 => gzip wrapper; produces header+trailer => one gzip member.
  int ret = deflateInit2(&strm, level, Z_DEFLATED, 15 + 16, memLevel, Z_DEFAULT_STRATEGY);
  if (ret != Z_OK) throw_zlib("deflateInit2", ret);

  std::vector<unsigned char> inbuf(1 << 20);
  unsigned char outbuf[1u << 16];

  while (true) {
    in.read(reinterpret_cast<char*>(inbuf.data()), static_cast<std::streamsize>(inbuf.size()));
    std::streamsize got = in.gcount();
    if (got < 0) got = 0;

    strm.next_in = inbuf.data();
    strm.avail_in = static_cast<uInt>(got);

    int flush = in.eof() ? Z_FINISH : Z_NO_FLUSH;

    do {
      strm.next_out = outbuf;
      strm.avail_out = sizeof(outbuf);

      ret = deflate(&strm, flush);
      if (ret == Z_STREAM_ERROR) {
        deflateEnd(&strm);
        throw_zlib("deflate", ret);
      }

      std::size_t have = sizeof(outbuf) - strm.avail_out;
      if (have) out.write(reinterpret_cast<const char*>(outbuf), static_cast<std::streamsize>(have));

    } while (strm.avail_out == 0);

    if (flush == Z_FINISH) break;
  }

  if (ret != Z_STREAM_END) {
    deflateEnd(&strm);
    throw std::runtime_error("deflate did not reach Z_STREAM_END");
  }

  deflateEnd(&strm);
}

int main(int argc, char** argv) {
  try {
    if (argc < 5) {
      std::cerr
          << "Usage:\n"
          << "  " << argv[0]
          << " <input.gfa> <output.gfa.gz> <index.tsv> <num_communities> [max_open_text] [seed]\n";
      return 2;
    }

    const std::string in_path = argv[1];
    const std::string out_gz = argv[2];
    const std::string out_idx = argv[3];
    const std::uint32_t ncom = static_cast<std::uint32_t>(std::stoul(argv[4]));
    const std::size_t max_open_text = (argc >= 6) ? static_cast<std::size_t>(std::stoull(argv[5])) : 256;
    const std::uint64_t seed = (argc >= 7) ? std::stoull(argv[6]) : 123456789ULL;

    if (ncom == 0) throw std::runtime_error("num_communities must be > 0");

    // Temp directory next to output.
    fs::path out_path(out_gz);
    fs::path tmp_dir = out_path;
    tmp_dir += ".parts_text";
    fs::create_directories(tmp_dir);

    // Per-community raw temp files.
    std::vector<fs::path> part_txt;
    part_txt.reserve(ncom);
    for (std::uint32_t c = 0; c < ncom; ++c) {
      part_txt.push_back(tmp_dir / ("comm_" + std::to_string(c) + ".gfa"));
      // remove if exists from previous runs
      if (fs::exists(part_txt.back())) fs::remove(part_txt.back());
    }

    // Stats
    std::vector<std::uint64_t> uncomp(ncom, 0);
    std::vector<std::uint32_t> lines(ncom, 0);

    // PASS 1: split into raw text files
    {
      std::ifstream in(in_path);
      if (!in) throw std::runtime_error("Failed to open " + in_path);

      TextHandleCache cache(part_txt, max_open_text);

      std::mt19937_64 rng(seed);
      std::uniform_int_distribution<std::uint32_t> dist(0, ncom - 1);

      std::string line;
      while (std::getline(in, line)) {
        std::uint32_t c = dist(rng);  // TODO: replace with your real line->community_id logic
        cache.write_line(c, line);
        uncomp[c] += (line.size() + 1);
        lines[c] += 1;
      }
      cache.close_all();
    }

    // PASS 2: compress each comm_X.gfa into exactly ONE gzip member, append to final .gz
    std::ofstream out(out_gz, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open " + out_gz);

    std::ofstream idx(out_idx);
    if (!idx) throw std::runtime_error("Failed to open " + out_idx);
    idx << "#community_id\tgz_offset\tgz_size\tuncompressed_size\tline_count\n";

    // Choose compression level: 6 ~ gzip default, 9 best compression
    const int level = 9;
    const int memLevel = 9; // 8 is typical; 9 may slightly improve, uses more memory during compression

    std::vector<IndexEntry> entries;
    entries.reserve(ncom);

    for (std::uint32_t c = 0; c < ncom; ++c) {
      IndexEntry e;
      e.community_id = c;
      e.gz_offset = static_cast<std::uint64_t>(out.tellp());
      e.uncompressed_size = uncomp[c];
      e.line_count = lines[c];

      if (fs::exists(part_txt[c]) && fs::file_size(part_txt[c]) > 0) {
        append_one_gzip_member_from_file(out, part_txt[c], level, memLevel);
        e.gz_size = static_cast<std::uint64_t>(out.tellp()) - e.gz_offset;
      } else {
        e.gz_size = 0;
      }

      idx << e.community_id << '\t' << e.gz_offset << '\t' << e.gz_size << '\t'
          << e.uncompressed_size << '\t' << e.line_count << "\n";
      entries.push_back(e);
    }

    // Cleanup temp files (comment out if you want to inspect)
    // for (const auto& p : part_txt) {
      // if (fs::exists(p)) fs::remove(p);
    // }
    // fs::remove(tmp_dir);

    std::cerr << "Wrote multi-member gzip: " << out_gz << "\n";
    std::cerr << "Wrote index: " << out_idx << "\n";
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 1;
  }
}
