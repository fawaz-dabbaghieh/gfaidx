//
// Created by Fawaz Dabbaghie on 22/12/2025.
//

#ifndef GFAIDX_GFA_LINE_PARSERS_H
#define GFAIDX_GFA_LINE_PARSERS_H


#include <string>
#include <iostream>
#include <vector>

static constexpr size_t npos = -1; // size_type(-1);

inline void offending_line(const std::string_view line) {
    std::cerr << "Offending line: " << line << std::endl;
    exit(1);
}

std::pair<std::string, std::string> extract_L_nodes(std::string_view line);

void extract_P_nodes(std::string_view line, std::string& path_name,
    std::vector<std::string>& node_list);

void extract_S_node(std::string_view line, std::string& seq_name, std::string& seq);

#endif //GFAIDX_GFA_LINE_PARSERS_H