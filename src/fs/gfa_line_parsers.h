//
// Created by Fawaz Dabbaghie on 22/12/2025.
//

#ifndef GFAIDX_GFA_LINE_PARSERS_H
#define GFAIDX_GFA_LINE_PARSERS_H


#include <string>
#include <iostream>
#include <vector>

inline void offending_line(const std::string_view line);

std::pair<std::string, std::string> extract_L_nodes(std::string_view line);

void extract_P_endpoints(std::string_view line, std::string& path_name,
    std::vector<std::string>& node_list);

#endif //GFAIDX_GFA_LINE_PARSERS_H