//
// Created by Fawaz Dabbaghie on 22/12/2025.
//

#include "gfa_line_parsers.h"

static constexpr size_t npos = -1; // size_type(-1);


inline void offending_line(const std::string_view line) {
    std::cerr << "Offending line: " << line << std::endl;
    exit(1);
}

std::pair<std::string, std::string> extract_L_nodes(std::string_view line) {

    size_t t1 = line.find('\t');
    if (t1 == npos) offending_line(line);

    size_t t2 = line.find('\t', t1 + 1);
    if (t2 == npos) offending_line(line);

    size_t t3 = line.find('\t', t2 + 1);
    if (t3 == npos) offending_line(line);

    size_t t4 = line.find('\t', t3 + 1);
    if (t4 == npos) offending_line(line);

    // token[1] = (t1+1 .. t2-1), token[3] = (t3+1 .. t4-1)
    std::string_view from = line.substr(t1 + 1, t2 - (t1 + 1));
    std::string_view to   = line.substr(t3 + 1, t4 - (t3 + 1));

    return {std::string(from), std::string(to)};
}


void extract_P_endpoints(std::string_view line, std::string& path_name,
    std::vector<std::string>& node_list) {

    // the hardcoded structure is P\t<path_name>\t<node_list>\t<overlaps>\t<tag1>\t<tag2>...\n
    // However, the line can end after overlaps, so strictly we need to find 3 tabs

    const size_t t1 = line.find('\t');
    if (t1 == npos) offending_line(line);

    const size_t t2 = line.find('\t', t1 + 1);
    if (t2 == npos) offending_line(line);

    const size_t t3 = line.find('\t', t2 + 1);
    if (t3 == npos) offending_line(line);

    path_name = std::string(line.substr(t1 + 1, t2 - (t1 + 1)));

    for (size_t i = t2; i < t3 - 1; i++) {
        size_t comma_pos = line.find(',', i + 1);
        if (comma_pos == npos || comma_pos > t3) {
            comma_pos = t3;
        }
        auto node = std::string(line.substr(i + 1, comma_pos - (i + 1)));
        node_list.push_back(node);
        i = comma_pos;
    }
    // token[1] = (t1+1 .. t2-1), token[3] = (t3+1 .. t4-1)
    // std::string_view from = line.substr(t1 + 1, t2 - (t1 + 1));
    // std::string_view to   = line.substr(t3 + 1, t4 - (t3 + 1));

    // return {std::string(from), std::string(to)};
}