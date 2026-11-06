#ifndef GFAIDX_PDX_CODEC_H
#define GFAIDX_PDX_CODEC_H

// Byte-level encoders and decoders for the .pdx step table and posting blob.
// Kept separate from path_index.cpp so the codecs can be round-trip tested on
// their own and reused by the builder, the reader and the posting cursor.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace gfaidx::paths::codec {

// ---------------------------------------------------------------------------
// LEB128 varints and zigzag
// ---------------------------------------------------------------------------

inline void append_varint(std::string& out, std::uint64_t value) {
    while (value >= 0x80) {
        out.push_back(static_cast<char>((value & 0x7f) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
}

// Decode one varint from [p, end). Throws on truncation or overlong input.
inline std::uint64_t read_varint(const unsigned char*& p, const unsigned char* end) {
    if (p < end && *p < 0x80) return *p++;
    std::uint64_t value = 0;
    unsigned shift = 0;
    while (p < end) {
        const auto byte = static_cast<std::uint64_t>(*p++);
        value |= (byte & 0x7f) << shift;
        if ((byte & 0x80) == 0) return value;
        shift += 7;
        if (shift >= 64) throw std::runtime_error("Malformed varint in .pdx");
    }
    throw std::runtime_error("Unexpected end of encoded .pdx data");
}

inline std::uint64_t zigzag_encode(std::int64_t value) {
    return (static_cast<std::uint64_t>(value) << 1) ^ static_cast<std::uint64_t>(value >> 63);
}

inline std::int64_t zigzag_decode(std::uint64_t value) {
    return static_cast<std::int64_t>(value >> 1) ^ -static_cast<std::int64_t>(value & 1);
}

// ---------------------------------------------------------------------------
// Delta-varint step table (.pdx v5, step_encoding = delta_varint)
//
// Each step is LEB128((zigzag(node_id - prev) << 1) | is_reverse). Blocks are
// global over the whole step table: at every global step index that is a
// multiple of kStepBlockSteps, prev resets to 0, so that value is absolute
// and a block can be decoded on its own. Path boundaries need no special case.
//
// Block start offsets are stored in superblocks of kStepSuperblockBlocks
// blocks: one u64 byte offset per superblock plus one u16 offset per block
// relative to it (32 * 128 steps * 5 bytes fits a u16). That costs 72 bytes
// per 4096 steps (~0.018 B/step) and gives O(128) random access by step.
// ---------------------------------------------------------------------------

inline constexpr std::uint64_t kStepBlockSteps = 128;
inline constexpr std::uint64_t kStepSuperblockBlocks = 32;
inline constexpr std::uint64_t kStepSuperblockSteps = kStepBlockSteps * kStepSuperblockBlocks;

struct StepSuperblockDisk {
    std::uint64_t base_offset{};
    std::uint16_t block_offsets[kStepSuperblockBlocks]{};
};

static_assert(sizeof(StepSuperblockDisk) == 72, "Unexpected step superblock size");

// Streaming encoder: steps are appended in global step order. Encoded bytes go
// to data_out and superblock records to index_out, both written sequentially.
class DeltaStepWriter {
public:
    DeltaStepWriter(std::ostream& data_out, std::ostream& index_out)
        : data_out_(data_out), index_out_(index_out) {
        buffer_.reserve(kFlushBytes + 16);
    }

    void add(std::uint32_t node_id, bool is_reverse) {
        const std::uint64_t in_superblock = step_count_ % kStepSuperblockSteps;
        if (in_superblock == 0) {
            if (step_count_ != 0) emit_superblock();
            superblock_.base_offset = data_bytes_ + buffer_.size();
        }
        if (in_superblock % kStepBlockSteps == 0) {
            superblock_.block_offsets[in_superblock / kStepBlockSteps] = static_cast<std::uint16_t>(
                data_bytes_ + buffer_.size() - superblock_.base_offset);
            prev_ = 0;
        }
        const auto delta = static_cast<std::int64_t>(node_id) - static_cast<std::int64_t>(prev_);
        append_varint(buffer_, (zigzag_encode(delta) << 1) | (is_reverse ? 1u : 0u));
        prev_ = node_id;
        ++step_count_;
        if (buffer_.size() >= kFlushBytes) flush();
    }

    void finish() {
        if (step_count_ % kStepSuperblockSteps != 0) {
            // Blocks past the end of a partial superblock point at the end.
            const std::uint64_t used_blocks =
                (step_count_ % kStepSuperblockSteps + kStepBlockSteps - 1) / kStepBlockSteps;
            for (std::uint64_t b = used_blocks; b < kStepSuperblockBlocks; ++b) {
                superblock_.block_offsets[b] = static_cast<std::uint16_t>(
                    data_bytes_ + buffer_.size() - superblock_.base_offset);
            }
        }
        if (step_count_ != 0) emit_superblock();
        flush();
    }

    [[nodiscard]] std::uint64_t data_bytes() const { return data_bytes_ + buffer_.size(); }
    [[nodiscard]] std::uint64_t step_count() const { return step_count_; }
    [[nodiscard]] std::uint64_t superblock_count() const { return superblock_count_; }

private:
    static constexpr std::size_t kFlushBytes = 1u << 20;

    void emit_superblock() {
        index_out_.write(reinterpret_cast<const char*>(&superblock_), sizeof(superblock_));
        if (!index_out_.good()) throw std::runtime_error("Failed while writing step superblock index");
        superblock_ = StepSuperblockDisk{};
        ++superblock_count_;
    }

    void flush() {
        if (buffer_.empty()) return;
        data_out_.write(buffer_.data(), static_cast<std::streamsize>(buffer_.size()));
        if (!data_out_.good()) throw std::runtime_error("Failed while writing encoded step table");
        data_bytes_ += buffer_.size();
        buffer_.clear();
    }

    std::ostream& data_out_;
    std::ostream& index_out_;
    std::string buffer_;
    StepSuperblockDisk superblock_{};
    std::uint64_t data_bytes_{};
    std::uint64_t step_count_{};
    std::uint64_t superblock_count_{};
    std::uint32_t prev_{};
};

// Byte offset of block `block` inside the encoded step data, given the mapped
// superblock table. `block == block_count` returns data_bytes.
inline std::uint64_t step_block_offset(const StepSuperblockDisk* superblocks,
                                       std::uint64_t superblock_count,
                                       std::uint64_t data_bytes,
                                       std::uint64_t block) {
    const std::uint64_t sb = block / kStepSuperblockBlocks;
    if (sb >= superblock_count) return data_bytes;
    return superblocks[sb].base_offset + superblocks[sb].block_offsets[block % kStepSuperblockBlocks];
}

// Decode steps [first_step, first_step + count) where `p` points at the start
// of block first_step / kStepBlockSteps. fn(node_id, is_reverse) is called for
// each decoded step in order.
template <typename Fn>
inline void decode_delta_steps(const unsigned char* p,
                               const unsigned char* end,
                               std::uint64_t first_step,
                               std::uint64_t count,
                               std::uint32_t node_limit,
                               Fn&& fn) {
    if (count == 0) return;
    std::uint64_t global = first_step - first_step % kStepBlockSteps;
    const std::uint64_t stop = first_step + count;
    std::int64_t prev = 0;
    for (; global < stop; ++global) {
        if (global % kStepBlockSteps == 0) prev = 0;
        const std::uint64_t value = read_varint(p, end);
        const std::int64_t node = prev + zigzag_decode(value >> 1);
        if (node < 0 || node >= static_cast<std::int64_t>(node_limit)) {
            throw std::runtime_error("Encoded .pdx step decodes to an invalid node id");
        }
        prev = node;
        if (global >= first_step) fn(static_cast<std::uint32_t>(node), (value & 1u) != 0);
    }
}

// ---------------------------------------------------------------------------
// Path-slot postings (.pdx v5, posting_encoding = path_slots)
//
// Nodes are coded in blocks of block_nodes consecutive node ids. Inside a
// block the coder keeps one "slot" per occurrence: for every path seen so far
// in the block, the ranks of that path at the most recent node it visited
// (slots are sorted by path, then rank). With locality-ordered node ids a
// path usually steps from one node of the block to the next, so an
// occurrence (p, r) is "predicted" when (p, r - 1) is a slot, and only the
// subset of predicted slots has to be stored. Loops predict as well, because
// every occurrence has its own slot. The first node of a block has no slots,
// so each block decodes on its own.
//
// One non-empty node is:
//   mode byte: 0 none, 1 all slots, 2 bitmap, 3 index list, 4 complement list
//   subset:    bitmap = ceil(slots / 8) bytes; list / complement = varint
//              count then varint gaps (index - previous index - 1)
//   explicit:  the remaining postings sorted by (path, rank), each
//              varint(path - previous explicit path) then the rank as
//              varint(rank - previous rank - 1) after an explicit posting of
//              the same path, varint(zigzag(rank - (last slot rank + 1)))
//              when the path has slots, or varint(rank) otherwise.
// Empty nodes take no bytes; their count comes from the node table.
// ---------------------------------------------------------------------------

struct Posting {
    std::uint32_t path_id{};
    std::uint32_t step_rank{};
};

inline bool posting_less(const Posting& a, const Posting& b) {
    return a.path_id != b.path_id ? a.path_id < b.path_id : a.step_rank < b.step_rank;
}

enum class SubsetMode : unsigned char { none = 0, all = 1, bitmap = 2, list = 3, complement = 4 };

inline std::size_t varint_size(std::uint64_t value) {
    std::size_t n = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++n;
    }
    return n;
}

// Shared slot bookkeeping for the encoder and the decoder. Slots are kept in
// one (path, rank)-sorted vector because the coded subset indexes into that
// order; each active path owns a contiguous range of it. The common update -
// every path at the node keeps its occurrence count - overwrites those ranges
// in place; only a new path or a changed count rebuilds the vector. A node's
// postings are read back from the updated slots instead of being kept
// separately, so decoding a node with N occurrences needs about 16 bytes per
// occurrence (repeat nodes can have tens of millions).
class PathSlotState {
public:
    static constexpr std::uint32_t kNoSlot = 0xffffffffu;

    void reset() {
        for (const auto& slot : slots_) first_slot_[slot.path_id] = kNoSlot;
        slots_.clear();
        node_paths_.clear();
    }

    [[nodiscard]] std::size_t slot_count() const { return slots_.size(); }
    [[nodiscard]] std::uint32_t rank(std::size_t slot) const { return slots_[slot].step_rank; }

    // Slots of path_id as [first, first + count); count 0 when it has none.
    void path_slots(std::uint32_t path_id, std::uint32_t& first, std::uint32_t& count) const {
        if (path_id >= first_slot_.size() || first_slot_[path_id] == kNoSlot) {
            first = 0;
            count = 0;
            return;
        }
        first = first_slot_[path_id];
        count = slot_count_[path_id];
    }

    // Largest slot rank of path_id, or false when the path has no slot.
    bool last_rank(std::uint32_t path_id, std::uint32_t& out) const {
        std::uint32_t first = 0;
        std::uint32_t count = 0;
        path_slots(path_id, first, count);
        if (count == 0) return false;
        out = slots_[first + count - 1].step_rank;
        return true;
    }

    // Move to the next node. Its occurrences are the predicted slots (each
    // slot rank + 1), given as sorted slot indices, merged with the explicit
    // postings, which are sorted by (path, rank). Paths at the node replace
    // their slots with its ranks; other paths keep theirs.
    void advance(const std::uint32_t* predicted, std::size_t predicted_count,
                 const Posting* explicit_begin, const Posting* explicit_end) {
        node_ranks_.clear();
        node_paths_.clear();
        node_counts_.clear();
        bool in_place = true;
        std::size_t pi = 0;
        const Posting* e = explicit_begin;
        while (pi < predicted_count || e < explicit_end) {
            std::uint32_t path = kNoSlot;
            if (pi < predicted_count) {
                if (predicted[pi] >= slots_.size()) {
                    throw std::runtime_error("Encoded .pdx posting index exceeds the slot count");
                }
                path = slots_[predicted[pi]].path_id;
            }
            if (e < explicit_end && e->path_id < path) path = e->path_id;
            std::uint32_t first = 0;
            std::uint32_t count = 0;
            path_slots(path, first, count);
            const std::uint32_t slot_end = first + count;
            const std::size_t start = node_ranks_.size();
            while (true) {
                const bool have_predicted = pi < predicted_count && predicted[pi] < slot_end;
                const bool have_explicit = e < explicit_end && e->path_id == path;
                if (!have_predicted && !have_explicit) break;
                std::uint64_t rank = 0;
                const std::uint64_t predicted_rank =
                    have_predicted ? static_cast<std::uint64_t>(slots_[predicted[pi]].step_rank) + 1 : 0;
                if (have_predicted && (!have_explicit || predicted_rank <= e->step_rank)) {
                    if (have_explicit && predicted_rank == e->step_rank) {
                        throw std::runtime_error("Encoded .pdx postings contain a duplicate occurrence");
                    }
                    rank = predicted_rank;
                    ++pi;
                } else {
                    rank = e->step_rank;
                    ++e;
                }
                if (rank > 0xffffffffULL ||
                    (node_ranks_.size() > start && node_ranks_.back() >= rank)) {
                    throw std::runtime_error("Encoded .pdx postings are out of order");
                }
                node_ranks_.push_back(static_cast<std::uint32_t>(rank));
            }
            const auto node_count = static_cast<std::uint32_t>(node_ranks_.size() - start);
            node_paths_.push_back(path);
            node_counts_.push_back(node_count);
            in_place = in_place && node_count == count;
        }

        if (in_place) {
            // Every path at the node keeps its slot count: overwrite in place.
            std::size_t offset = 0;
            for (std::size_t i = 0; i < node_paths_.size(); ++i) {
                Posting* slot = slots_.data() + first_slot_[node_paths_[i]];
                for (std::uint32_t j = 0; j < node_counts_[i]; ++j) slot[j].step_rank = node_ranks_[offset + j];
                offset += node_counts_[i];
            }
            return;
        }

        // A path appeared or changed its occurrence count: rebuild the slots,
        // keeping the ranges of paths that are not at the node.
        next_.clear();
        next_.reserve(slots_.size() + node_ranks_.size());
        std::size_t a = 0;
        std::size_t offset = 0;
        for (std::size_t i = 0; i < node_paths_.size(); ++i) {
            const auto path = node_paths_[i];
            while (a < slots_.size() && slots_[a].path_id < path) next_.push_back(slots_[a++]);
            while (a < slots_.size() && slots_[a].path_id == path) ++a;
            for (std::uint32_t j = 0; j < node_counts_[i]; ++j) {
                next_.push_back(Posting{path, node_ranks_[offset + j]});
            }
            offset += node_counts_[i];
        }
        next_.insert(next_.end(), slots_.begin() + static_cast<std::ptrdiff_t>(a), slots_.end());
        slots_.swap(next_);
        for (std::uint32_t i = 0; i < slots_.size();) {
            const auto path = slots_[i].path_id;
            if (path >= first_slot_.size()) {
                first_slot_.resize(static_cast<std::size_t>(path) + 1, kNoSlot);
                slot_count_.resize(static_cast<std::size_t>(path) + 1, 0);
            }
            std::uint32_t j = i + 1;
            while (j < slots_.size() && slots_[j].path_id == path) ++j;
            first_slot_[path] = i;
            slot_count_[path] = j - i;
            i = j;
        }
    }

    // Postings of the node passed to the last advance(), by (path, rank).
    template <typename Fn>
    void for_each_node_posting(Fn&& fn) const {
        for (const auto path : node_paths_) {
            const Posting* slot = slots_.data() + first_slot_[path];
            for (std::uint32_t j = 0; j < slot_count_[path]; ++j) fn(path, slot[j].step_rank);
        }
    }

private:
    std::vector<Posting> slots_;
    std::vector<Posting> next_;
    std::vector<std::uint32_t> node_paths_;
    std::vector<std::uint32_t> node_counts_;
    std::vector<std::uint32_t> node_ranks_;
    std::vector<std::uint32_t> first_slot_;
    std::vector<std::uint32_t> slot_count_;
};

class PathSlotPostingEncoder {
public:
    explicit PathSlotPostingEncoder(std::uint32_t block_nodes) : block_nodes_(block_nodes) {
        if (block_nodes_ == 0) throw std::runtime_error("Posting block size must be positive");
    }

    // Append one node's postings, sorted by (path, rank) without duplicates.
    // Nodes must arrive in increasing id order; empty nodes may be skipped.
    void encode_node(std::uint32_t node_id, const Posting* begin, const Posting* end, std::string& out) {
        const std::uint64_t block = node_id / block_nodes_;
        if (!started_ || block != block_) {
            state_.reset();
            block_ = block;
            started_ = true;
        }
        if (begin == end) return;

        predicted_.clear();
        explicit_.clear();
        for (const Posting* q = begin; q < end;) {
            std::uint32_t si = 0;
            std::uint32_t count = 0;
            state_.path_slots(q->path_id, si, count);
            const std::uint32_t slot_end = si + count;
            const auto path = q->path_id;
            for (; q < end && q->path_id == path; ++q) {
                while (si < slot_end && static_cast<std::uint64_t>(state_.rank(si)) + 1 < q->step_rank) ++si;
                if (si < slot_end && static_cast<std::uint64_t>(state_.rank(si)) + 1 == q->step_rank) {
                    predicted_.push_back(si++);
                } else {
                    explicit_.push_back(*q);
                }
            }
        }
        // Predicted slot indices must be in slot order for the subset coding.
        std::sort(predicted_.begin(), predicted_.end());

        // Pick the cheapest description of the predicted subset.
        const std::size_t m = state_.slot_count();
        const std::size_t k = predicted_.size();
        SubsetMode mode = SubsetMode::none;
        if (k == m && m > 0) {
            mode = SubsetMode::all;
        } else if (k > 0) {
            const std::size_t bitmap = (m + 7) / 8;
            std::size_t list = varint_size(k);
            std::size_t complement = varint_size(m - k);
            std::uint32_t next = 0;
            for (const auto index : predicted_) {
                list += varint_size(index - next);
                next = index + 1;
            }
            std::size_t pi = 0;
            next = 0;
            for (std::uint32_t index = 0; index < m; ++index) {
                if (pi < k && predicted_[pi] == index) {
                    ++pi;
                    continue;
                }
                complement += varint_size(index - next);
                next = index + 1;
            }
            mode = SubsetMode::bitmap;
            std::size_t best = bitmap;
            if (list < best) { mode = SubsetMode::list; best = list; }
            if (complement < best) { mode = SubsetMode::complement; }
        }

        out.push_back(static_cast<char>(mode));
        if (mode == SubsetMode::bitmap) {
            const std::size_t base = out.size();
            out.append((m + 7) / 8, '\0');
            for (const auto index : predicted_) {
                out[base + index / 8] = static_cast<char>(
                    static_cast<unsigned char>(out[base + index / 8]) | (1u << (index % 8)));
            }
        } else if (mode == SubsetMode::list) {
            append_varint(out, k);
            std::uint32_t next = 0;
            for (const auto index : predicted_) {
                append_varint(out, index - next);
                next = index + 1;
            }
        } else if (mode == SubsetMode::complement) {
            append_varint(out, m - k);
            std::size_t pi = 0;
            std::uint32_t next = 0;
            for (std::uint32_t index = 0; index < m; ++index) {
                if (pi < k && predicted_[pi] == index) {
                    ++pi;
                    continue;
                }
                append_varint(out, index - next);
                next = index + 1;
            }
        }

        std::uint32_t prev_path = 0;
        for (std::size_t i = 0; i < explicit_.size(); ++i) {
            const auto& q = explicit_[i];
            append_varint(out, q.path_id - prev_path);
            std::uint32_t last = 0;
            if (i > 0 && explicit_[i - 1].path_id == q.path_id) {
                append_varint(out, q.step_rank - explicit_[i - 1].step_rank - 1);
            } else if (state_.last_rank(q.path_id, last)) {
                append_varint(out, zigzag_encode(static_cast<std::int64_t>(q.step_rank) -
                                                 static_cast<std::int64_t>(last) - 1));
            } else {
                append_varint(out, q.step_rank);
            }
            prev_path = q.path_id;
        }

        // The node's occurrences become the slots of its paths.
        state_.advance(nullptr, 0, begin, end);
    }

private:
    std::uint32_t block_nodes_;
    std::uint64_t block_{};
    bool started_{false};
    PathSlotState state_;
    std::vector<std::uint32_t> predicted_;
    std::vector<Posting> explicit_;
};

class PathSlotPostingDecoder {
public:
    // Call at the first node of every block (whether or not it is empty).
    void reset() { state_.reset(); }

    // Decode one non-empty node of posting_count postings from [p, end) and
    // advance the slot state; read the postings with for_each_node_posting().
    void decode_node(const unsigned char*& p,
                     const unsigned char* end,
                     std::uint64_t posting_count,
                     std::uint32_t path_limit) {
        if (p >= end) throw std::runtime_error("Unexpected end of encoded .pdx postings");
        const auto mode = static_cast<SubsetMode>(*p++);
        const std::size_t m = state_.slot_count();

        predicted_.clear();
        std::size_t predicted_count = 0;
        switch (mode) {
            case SubsetMode::none:
                break;
            case SubsetMode::all:
                for (std::uint32_t i = 0; i < m; ++i) predicted_.push_back(i);
                break;
            case SubsetMode::bitmap: {
                const std::size_t bytes = (m + 7) / 8;
                if (static_cast<std::size_t>(end - p) < bytes) {
                    throw std::runtime_error("Unexpected end of encoded .pdx postings");
                }
                for (std::size_t byte = 0; byte < bytes; ++byte) {
                    unsigned bits = p[byte];
                    while (bits != 0) {
                        const auto bit = static_cast<std::uint32_t>(__builtin_ctz(bits));
                        predicted_.push_back(static_cast<std::uint32_t>(byte * 8 + bit));
                        bits &= bits - 1;
                    }
                }
                p += bytes;
                if (!predicted_.empty() && predicted_.back() >= m) {
                    throw std::runtime_error("Encoded .pdx posting bitmap exceeds the slot count");
                }
                break;
            }
            case SubsetMode::list: {
                const auto k = read_varint(p, end);
                if (k > m) throw std::runtime_error("Encoded .pdx posting list exceeds the slot count");
                std::uint64_t next = 0;
                for (std::uint64_t i = 0; i < k; ++i) {
                    next += read_varint(p, end);
                    if (next >= m) throw std::runtime_error("Encoded .pdx posting index exceeds the slot count");
                    predicted_.push_back(static_cast<std::uint32_t>(next++));
                }
                break;
            }
            case SubsetMode::complement: {
                const auto missing = read_varint(p, end);
                if (missing > m) throw std::runtime_error("Encoded .pdx posting list exceeds the slot count");
                std::uint64_t next = 0;
                std::uint64_t index = 0;
                for (std::uint64_t i = 0; i < missing; ++i) {
                    const auto skip = next + read_varint(p, end);
                    if (skip >= m) throw std::runtime_error("Encoded .pdx posting index exceeds the slot count");
                    for (; index < skip; ++index) predicted_.push_back(static_cast<std::uint32_t>(index));
                    index = skip + 1;
                    next = skip + 1;
                }
                for (; index < m; ++index) predicted_.push_back(static_cast<std::uint32_t>(index));
                break;
            }
            default:
                throw std::runtime_error("Unknown .pdx posting subset mode");
        }
        predicted_count = predicted_.size();
        if (predicted_count > posting_count) {
            throw std::runtime_error("Encoded .pdx postings predict more than the node posting count");
        }

        // Explicit postings, already in (path, rank) order.
        explicit_.clear();
        const std::uint64_t explicit_count = posting_count - predicted_count;
        std::uint64_t path = 0;
        for (std::uint64_t i = 0; i < explicit_count; ++i) {
            path += read_varint(p, end);
            if (path >= path_limit) throw std::runtime_error("Encoded .pdx posting path id out of range");
            const auto path_id = static_cast<std::uint32_t>(path);
            std::int64_t rank = 0;
            std::uint32_t last = 0;
            if (i > 0 && explicit_.back().path_id == path_id) {
                rank = static_cast<std::int64_t>(explicit_.back().step_rank) + 1 +
                       static_cast<std::int64_t>(read_varint(p, end));
            } else if (state_.last_rank(path_id, last)) {
                rank = static_cast<std::int64_t>(last) + 1 + zigzag_decode(read_varint(p, end));
            } else {
                rank = static_cast<std::int64_t>(read_varint(p, end));
            }
            if (rank < 0 || rank > 0xffffffffLL) {
                throw std::runtime_error("Encoded .pdx posting step rank out of range");
            }
            explicit_.push_back(Posting{path_id, static_cast<std::uint32_t>(rank)});
        }

        state_.advance(predicted_.data(), predicted_.size(),
                       explicit_.data(), explicit_.data() + explicit_.size());
    }

    // Postings of the last decoded node, sorted by (path, rank).
    template <typename Fn>
    void for_each_node_posting(Fn&& fn) const {
        state_.for_each_node_posting(fn);
    }

private:
    PathSlotState state_;
    std::vector<std::uint32_t> predicted_;
    std::vector<Posting> explicit_;
};

}  // namespace gfaidx::paths::codec

#endif  // GFAIDX_PDX_CODEC_H
