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



std::string rle_encode(std::string_view s_line, std::size_t seq_start, std::size_t seq_end);

inline void var_encode_uint32(uint32_t x, std::vector<uint8_t>& out);

inline std::pair<uint32_t, size_t> var_decode_uint32(const std::vector<uint8_t>& in, size_t i);

inline uint32_t pack_node(uint32_t id, bool is_reverse);

inline void unpack_node(uint32_t packed, uint32_t& id, bool& is_reverse);

inline void encode_path_string_ids_u32(const std::string& path_string, const std::unordered_map<std::string, uint32_t>& id_map, std::vector<uint8_t>& encoded_list);

inline std::vector<std::pair<uint32_t, bool>> decode_path_bytes_u32(const std::vector<uint8_t>& in);

#endif //GFAIDX_COMPRESSORS_H