// Round-trip tests for the .pdx v5 step codec, locality node-id assignment
// and the .ndx v2 format. Checks use CHECK rather than assert so they still
// run in Release builds, where NDEBUG removes assert().

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "indexer/node_hash_index.h"
#include "paths/pdx_codec.h"

namespace {

#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            std::cerr << __FILE__ << ":" << __LINE__ << ": CHECK failed: " #cond "\n"; \
            std::exit(1);                                                            \
        }                                                                            \
    } while (0)

template <typename Fn>
bool throws(Fn&& fn) {
    try {
        fn();
    } catch (const std::exception&) {
        return true;
    }
    return false;
}

using gfaidx::paths::codec::DeltaStepWriter;
using gfaidx::paths::codec::StepSuperblockDisk;
using gfaidx::paths::codec::kStepBlockSteps;

struct Step {
    std::uint32_t node;
    bool reverse;
};

// Decode global steps [first, first + count) from an encoded table the same
// way PathIndexReader does: superblock lookup, then one block-aligned decode.
std::vector<Step> decode_range(const std::string& data,
                               const std::string& index,
                               std::uint64_t first,
                               std::uint64_t count,
                               std::uint32_t node_limit) {
    std::vector<Step> out;
    if (count == 0) return out;
    const auto* superblocks = reinterpret_cast<const StepSuperblockDisk*>(index.data());
    const std::uint64_t superblock_count = index.size() / sizeof(StepSuperblockDisk);
    const auto begin = gfaidx::paths::codec::step_block_offset(
        superblocks, superblock_count, data.size(), first / kStepBlockSteps);
    const auto end = gfaidx::paths::codec::step_block_offset(
        superblocks, superblock_count, data.size(), (first + count - 1) / kStepBlockSteps + 1);
    const auto* base = reinterpret_cast<const unsigned char*>(data.data());
    gfaidx::paths::codec::decode_delta_steps(base + begin, base + end, first, count, node_limit,
                                             [&](std::uint32_t node, bool reverse) {
                                                 out.push_back(Step{node, reverse});
                                             });
    return out;
}

void test_step_codec() {
    std::mt19937_64 rng(42);
    const std::uint32_t node_limit = 0x7fffffffu;
    for (const std::uint64_t n : {0ULL, 1ULL, 127ULL, 128ULL, 129ULL, 4095ULL, 4096ULL, 4097ULL,
                                  8192ULL, 50000ULL}) {
        // A random walk with mostly small moves and occasional jumps to the
        // extremes of the 31-bit id range exercises every varint length.
        std::vector<Step> steps;
        std::uint32_t node = 1000;
        for (std::uint64_t i = 0; i < n; ++i) {
            const auto r = rng() % 100;
            if (r < 70) node = std::min(node + static_cast<std::uint32_t>(rng() % 3), node_limit - 1);
            else if (r < 90) node = static_cast<std::uint32_t>(rng() % 5000);
            else if (r < 95) node = node_limit - 1;
            else node = 0;
            steps.push_back(Step{node, (rng() & 1) != 0});
        }

        std::ostringstream data_out;
        std::ostringstream index_out;
        DeltaStepWriter writer(data_out, index_out);
        for (const auto& step : steps) writer.add(step.node, step.reverse);
        writer.finish();
        const std::string data = data_out.str();
        const std::string index = index_out.str();
        CHECK(writer.step_count() == n);
        CHECK(writer.data_bytes() == data.size());
        CHECK(writer.superblock_count() == (n + 4095) / 4096);
        CHECK(index.size() == writer.superblock_count() * sizeof(StepSuperblockDisk));

        const auto all = decode_range(data, index, 0, n, node_limit);
        CHECK(all.size() == n);
        for (std::uint64_t i = 0; i < n; ++i) {
            CHECK(all[i].node == steps[i].node && all[i].reverse == steps[i].reverse);
        }
        for (int trial = 0; trial < 200 && n > 0; ++trial) {
            const std::uint64_t first = rng() % n;
            const std::uint64_t count = 1 + rng() % (n - first);
            const auto part = decode_range(data, index, first, count, node_limit);
            CHECK(part.size() == count);
            for (std::uint64_t i = 0; i < count; ++i) {
                CHECK(part[i].node == steps[first + i].node &&
                      part[i].reverse == steps[first + i].reverse);
            }
        }
        // A tighter node limit must be enforced while decoding.
        if (n > 0) {
            CHECK(throws([&] { decode_range(data, index, 0, n, 10); }));
        }
    }
}

void test_slot_postings() {
    using gfaidx::paths::codec::PathSlotPostingDecoder;
    using gfaidx::paths::codec::PathSlotPostingEncoder;
    using gfaidx::paths::codec::Posting;
    std::mt19937_64 rng(7);
    const std::uint32_t path_count = 40;

    // Simulate haplotypes walking forward through consecutive node ids with
    // bubbles, skips, backward jumps and loops, then collect node postings.
    const std::uint32_t node_count = 3000;
    std::vector<std::vector<Posting>> nodes(node_count);
    for (std::uint32_t path = 0; path < path_count; ++path) {
        if (path % 7 == 3) continue;  // some paths never appear
        std::uint32_t rank = 0;
        std::uint32_t node = static_cast<std::uint32_t>(rng() % 50);
        while (node < node_count && rank < 20000) {
            nodes[node].push_back(Posting{path, rank++});
            const auto r = rng() % 100;
            if (r < 75) node += 1;
            else if (r < 90) node += 2 + static_cast<std::uint32_t>(rng() % 3);
            else if (r < 95 && node > 40) node -= static_cast<std::uint32_t>(rng() % 40);  // loop back
            else node += static_cast<std::uint32_t>(rng() % 200);
        }
    }
    for (auto& postings : nodes) std::sort(postings.begin(), postings.end(), gfaidx::paths::codec::posting_less);

    for (const std::uint32_t block_nodes : {1u, 3u, 32u, 64u}) {
        PathSlotPostingEncoder encoder(block_nodes);
        std::string blob;
        std::vector<std::size_t> starts;
        for (std::uint32_t n = 0; n < node_count; ++n) {
            starts.push_back(blob.size());
            encoder.encode_node(n, nodes[n].data(), nodes[n].data() + nodes[n].size(), blob);
            if (nodes[n].empty()) CHECK(blob.size() == starts.back());
        }
        starts.push_back(blob.size());

        // Decode every block from its first node, as the reader does.
        const auto* base = reinterpret_cast<const unsigned char*>(blob.data());
        PathSlotPostingDecoder decoder;
        std::vector<Posting> out;
        for (std::uint32_t n = 0; n < node_count; ++n) {
            if (n % block_nodes == 0) decoder.reset();
            if (nodes[n].empty()) continue;
            out.clear();
            const unsigned char* p = base + starts[n];
            decoder.decode_node(p, base + starts[n + 1], nodes[n].size(), path_count);
            decoder.for_each_node_posting([&](std::uint32_t path, std::uint32_t rank) {
                out.push_back(Posting{path, rank});
            });
            CHECK(p == base + starts[n + 1]);
            CHECK(out.size() == nodes[n].size());
            for (std::size_t i = 0; i < out.size(); ++i) {
                CHECK(out[i].path_id == nodes[n][i].path_id && out[i].step_rank == nodes[n][i].step_rank);
            }
        }
        if (block_nodes == 32) {
            // Locality pays off: well under the ~3 bytes of an explicit posting.
            std::size_t total = 0;
            for (const auto& postings : nodes) total += postings.size();
            CHECK(blob.size() < total * 2);
        }

        // Truncated input and an impossible posting count must be rejected.
        for (std::uint32_t n = 1; n < node_count; ++n) {
            if (nodes[n].size() < 2 || n % block_nodes == 0) continue;
            PathSlotPostingDecoder fresh;
            const std::uint32_t head = n - n % block_nodes;
            for (std::uint32_t h = head; h < n; ++h) {
                if (nodes[h].empty()) continue;
                const unsigned char* p = base + starts[h];
                fresh.decode_node(p, base + starts[h + 1], nodes[h].size(), path_count);
            }
            CHECK(throws([&] {
                PathSlotPostingDecoder copy = fresh;
                const unsigned char* p = base + starts[n];
                copy.decode_node(p, base + starts[n + 1] - 1, nodes[n].size(), path_count);
                if (p != base + starts[n + 1] - 1) throw std::runtime_error("trailing bytes");
            }));
            break;
        }
    }
}

// Build an internal-id map in insertion order, like generate_edgelist does.
std::unordered_map<std::string, unsigned int> make_ids(const std::vector<std::string>& names) {
    std::unordered_map<std::string, unsigned int> ids;
    for (const auto& name : names) ids.emplace(name, static_cast<unsigned int>(ids.size()));
    return ids;
}

void test_assign_node_ids() {
    using gfaidx::indexer::NodeOrderKey;
    using gfaidx::indexer::assign_node_ids;
    {
        // Integer names: community first, then numeric (not lexicographic) order.
        const auto ids = make_ids({"30", "2", "100", "7", "11"});
        const std::vector<std::uint32_t> comm = {1, 0, 1, 0, 0};
        const auto a = assign_node_ids(ids, comm, 3);
        CHECK(a.key == NodeOrderKey::integer_name);
        CHECK((a.community_starts == std::vector<std::uint32_t>{0, 3, 5, 5}));
        CHECK(a.node_ids[ids.at("2")] == 0);
        CHECK(a.node_ids[ids.at("7")] == 1);
        CHECK(a.node_ids[ids.at("11")] == 2);
        CHECK(a.node_ids[ids.at("30")] == 3);
        CHECK(a.node_ids[ids.at("100")] == 4);

        const auto f = assign_node_ids(ids, comm, 3, false);
        CHECK(f.key == NodeOrderKey::first_appearance);
        CHECK(f.node_ids[ids.at("2")] == 0);
        CHECK(f.node_ids[ids.at("7")] == 1);
        CHECK(f.node_ids[ids.at("11")] == 2);
        CHECK(f.node_ids[ids.at("30")] == 3);
        CHECK(f.node_ids[ids.at("100")] == 4);
    }
    {
        // Shared non-digit prefix, values beyond 32 bits, and a leading zero tie.
        const auto ids = make_ids({"s20", "s3", "s0003", "s99999999999"});
        const std::vector<std::uint32_t> comm = {0, 0, 0, 0};
        const auto a = assign_node_ids(ids, comm, 1);
        CHECK(a.key == NodeOrderKey::prefixed_integer);
        CHECK(a.node_ids[ids.at("s3")] == 0);     // ties keep internal-id order
        CHECK(a.node_ids[ids.at("s0003")] == 1);
        CHECK(a.node_ids[ids.at("s20")] == 2);
        CHECK(a.node_ids[ids.at("s99999999999")] == 3);
    }
    {
        // Any name outside the pattern falls back to first appearance.
        for (const auto& names : std::vector<std::vector<std::string>>{
                 {"10", "s2", "3"},              // integer mixed with prefixed
                 {"s10", "t2"},                  // two prefixes
                 {"a", "b"},                     // no digits
                 {"5", "99999999999999999999"},  // overflows uint64
                 {"10", "3x"}}) {
            const auto ids = make_ids(names);
            const std::vector<std::uint32_t> comm(names.size(), 0);
            const auto a = assign_node_ids(ids, comm, 1);
            CHECK(a.key == NodeOrderKey::first_appearance);
            for (std::size_t i = 0; i < names.size(); ++i) {
                CHECK(a.node_ids[ids.at(names[i])] == i);
            }
        }
    }
    {
        const auto ids = make_ids({"1", "2"});
        CHECK(throws([&] { assign_node_ids(ids, {0, 5}, 2); }));
        CHECK(throws([&] { assign_node_ids(ids, {0}, 2); }));
    }
}

void test_node_hash_index(const std::filesystem::path& dir) {
    using gfaidx::indexer::NodeHashIndex;
    // Enough nodes and communities (including empty ones) to exercise the
    // community start search and every padding residue of the v2 file.
    for (const std::uint32_t community_count : {1u, 2u, 3u, 4u, 7u}) {
        std::vector<std::string> names;
        std::vector<std::uint32_t> comm;
        for (std::uint32_t i = 0; i < 200; ++i) {
            names.push_back(std::to_string(1000 - i));
            comm.push_back((i * 7) % community_count == 1 ? 1 % community_count
                                                           : (i * 3) % community_count);
        }
        const auto ids = make_ids(names);
        const auto assignment = gfaidx::indexer::assign_node_ids(ids, comm, community_count);

        const auto v2_path = (dir / ("v2_" + std::to_string(community_count) + ".ndx")).string();
        gfaidx::indexer::write_node_hash_index(ids, assignment, v2_path);
        CHECK(std::filesystem::file_size(v2_path) % 16 == 8);
        NodeHashIndex v2(v2_path);
        CHECK(!v2.is_legacy());
        CHECK(v2.node_id_space() != 0);
        CHECK(v2.size() == names.size());
        for (std::size_t i = 0; i < names.size(); ++i) {
            std::uint32_t node_id = 0;
            std::uint32_t community = 0;
            CHECK(v2.lookup_rank(names[i], node_id));
            CHECK(node_id == assignment.node_ids[ids.at(names[i])]);
            CHECK(v2.lookup(names[i], community));
            CHECK(community == comm[ids.at(names[i])]);
            CHECK(v2.community_id_by_rank(node_id) == community);
        }
        std::uint32_t unused = 0;
        CHECK(!v2.lookup_rank("not-a-node", unused));
        CHECK(throws([&] { (void)v2.community_id_by_rank(static_cast<std::uint32_t>(names.size())); }));

        const auto legacy_path = (dir / ("legacy_" + std::to_string(community_count) + ".ndx")).string();
        gfaidx::indexer::write_node_hash_index(ids, comm, legacy_path);
        NodeHashIndex legacy(legacy_path);
        CHECK(legacy.is_legacy());
        CHECK(legacy.node_id_space() == 0);
        for (std::size_t i = 0; i < names.size(); ++i) {
            std::uint32_t rank = 0;
            std::uint32_t community = 0;
            CHECK(legacy.lookup_rank(names[i], rank));
            CHECK(legacy.lookup(names[i], community));
            CHECK(community == comm[ids.at(names[i])]);
        }

        // Path indexes must pair with the numbering they were built against.
        CHECK(!throws([&] {
            gfaidx::indexer::require_matching_node_ids(v2, names.size(), v2.node_id_space(), ".pdx");
        }));
        CHECK(throws([&] {
            gfaidx::indexer::require_matching_node_ids(v2, names.size(), 0, ".pdx");
        }));
        CHECK(throws([&] {
            gfaidx::indexer::require_matching_node_ids(legacy, names.size(), v2.node_id_space(), ".pdx");
        }));
        CHECK(throws([&] {
            gfaidx::indexer::require_matching_node_ids(legacy, names.size() + 1, 0, ".pdx");
        }));
    }

    // Damaged v2 files are rejected instead of being read as legacy tables.
    const auto bad_path = (dir / "bad.ndx").string();
    {
        std::ofstream out(bad_path, std::ios::binary);
        const std::string junk(72, 'x');
        out.write(junk.data(), static_cast<std::streamsize>(junk.size()));
    }
    CHECK(throws([&] { NodeHashIndex bad(bad_path); }));
    {
        std::ofstream out(bad_path, std::ios::binary);
        const std::string junk(70, 'x');
        out.write(junk.data(), static_cast<std::streamsize>(junk.size()));
    }
    CHECK(throws([&] { NodeHashIndex bad(bad_path); }));
}

}  // namespace

int main() {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("gfaidx_pdx_codec_test_" + std::to_string(std::random_device{}()));
    std::filesystem::create_directories(dir);
    try {
        test_step_codec();
        test_slot_postings();
        test_assign_node_ids();
        test_node_hash_index(dir);
    } catch (const std::exception& err) {
        std::cerr << "unexpected exception: " << err.what() << "\n";
        std::filesystem::remove_all(dir);
        return 1;
    }
    std::filesystem::remove_all(dir);
    std::cout << "pdx codec tests passed\n";
    return 0;
}
