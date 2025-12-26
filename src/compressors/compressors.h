//
// Created by Fawaz Dabbaghie on 19/12/2025.
//

#ifndef GFAIDX_COMPRESSORS_H
#define GFAIDX_COMPRESSORS_H

#include <iostream>
#include <string>
#include <string_view>
#include <cstdint>
#include <vector>
#include <stdexcept>
#include <utility>


// runlength encoding of the sequences in the S line
// NOTE: I guess depending on the type of graphs, but for pangenome graphs, after testing, noticed that the sequences
//       are short enough that rle result is bigger than the actual sequence, probably won't be using it
//       for example, in the PGGB graph, the average node sequence length is only 75, so not much compression will happen
//       TODO: I should separate rle encodeing and decoding to its own file later
inline std::string rle_encode(const std::string_view s_line, const std::size_t seq_start, const std::size_t seq_end) {

    if (seq_start > seq_end || seq_end > s_line.size()) {
        std::cerr << "Out of range for the runlength encoding, start: " << seq_start << ", end: " << seq_end << std::endl;
        exit(1);
    }
    const std::size_t len = seq_end - seq_start;
    if (len == 0) return std::string{};

    std::string out_rle;
    // The worst case (no repeats) we'll have 1a1b1c1d...
    out_rle.reserve(2 * len);

    char current = s_line[seq_start];
    std::size_t count = 1;

    // lambda for flushing the run
    // [&out_rle] to use out from the outer scope and modify it
    auto flush = [&out_rle](const std::size_t c, const char ch) {
        // big enough to hold any integer
        char buf[32];
        // to avoid making a copy, we are adding to the buffer directly.
        auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), c);
        //appending from buf to p, and p points to after the digits
        out_rle.append(buf, p);
        out_rle.push_back(ch);
    };

    for (std::size_t i = seq_start + 1; i < seq_end; ++i) {
        char ch = s_line[i];
        if (ch == current) {
            ++count;
        } else {
            flush(count, current);
            current = ch;
            count = 1;
        }
    }
    flush(count, current);  //final run

    return out_rle;
}


// variable length encoding of uint32, split into 8 bit chunks, 7 bits info, 1 bit continuation (most significant)
inline void var_encode_uint32(uint32_t x, std::vector<uint8_t>& out) {
    // 0x80 is 1000 000
    while (x >= 0x80u) {
        // extract the lowest 7 bits with 0x7F (0111 1111) and set the continuation bit
        out.push_back(static_cast<uint8_t>((x & 0x7Fu) | 0x80u));
        // process next 7 bits
        x >>= 7;
    }
    out.push_back(static_cast<uint8_t>(x));
}


inline std::pair<uint32_t, size_t> var_decode_uint32(const std::vector<uint8_t>& in, size_t i) {
    uint32_t value = 0;
    unsigned shift = 0;

    while (true) {
        if (i >= in.size()) throw std::runtime_error("truncated varint");

        uint8_t byte = in[i++];
        value |= (byte & 0x7Fu) << shift;

        if ((byte & 0x80u) == 0) break;   // last byte

        shift += 7;
        if (shift > 28) { // 5th byte would exceed uint32 in normal encodings
            throw std::runtime_error("varint overflow/corrupt");
        }
    }
    return {value, i};
}


// --------------------
// Pack/unpack (id, orientation) into one uint32_t
// dir: 0 => '+', 1 => '-'
// --------------------
inline uint32_t pack_node(const uint32_t id, const bool is_reverse) {
    // Ensure shifting doesn't overflow (should be fine for now, unless graphs have grown to be over 4.2 billion nodes)
    if (id > (UINT32_MAX >> 1)) {
        std::cerr << "Node id too large: " << id << std::endl;
        exit(1);
    }
    return (id << 1) | (is_reverse ? 1u : 0u);
}

inline void unpack_node(const uint32_t packed, uint32_t& id, bool& is_reverse) {
    is_reverse = (packed & 1u) != 0;
    id = (packed >> 1);
}

// Encode a list of path nodes (string ids with orientations) into a byte stream.
inline void encode_path_string_ids_u32(const std::vector<std::string>& path_nodes,
    const std::unordered_map<std::string,
    uint32_t>& id_map,
    std::vector<uint8_t>& encoded_list) {

    for (auto node : path_nodes) {
        if (node.empty()) {
            std::cerr << "Empty node id: " << node << std::endl;
            exit(1);
        }

        // Parse string_id and orientation
        const char orientation = node.back();

        if (orientation != '+' && orientation != '-') {
            std::cerr << "Invalid orientation: " << orientation << " For node " << node << std::endl;
            exit(1);
        }
        node.pop_back();

        const bool is_rev = (orientation == '-');

        // Look up integer id
        auto it = id_map.find(node);
        if (it == id_map.end()) {
            throw std::runtime_error("unknown node id: " + node);
        }
        const uint32_t int_id = it->second;
        // Pack and encode
        const uint32_t packed = pack_node(int_id, is_rev);
        var_encode_uint32(packed, encoded_list);
    }
}

// Decode the byte stream back into (id, is_reverse) pairs.
inline std::vector<std::pair<uint32_t, bool>> decode_path_bytes_u32(const std::vector<uint8_t>& in) {
    std::vector<std::pair<uint32_t, bool>> result;
    size_t i = 0;

    while (i < in.size()) {
        auto [packed, next] = var_decode_uint32(in, i);
        i = next;

        uint32_t id;
        bool is_rev;
        unpack_node(packed, id, is_rev);
        result.emplace_back(id, is_rev);
    }
    return result;
}


#endif //GFAIDX_COMPRESSORS_H