//
// Created by Fawaz Dabbaghie on 22/12/2025.
//

#include "gfa_line_parsers.h"

static constexpr size_t npos = -1; // size_type(-1);


inline void offending_line(const std::string_view line) {
    std::cerr << "Offending line: " << line << std::endl;
    exit(1);
}

void extract_S_node(std::string_view line, std::string& seq_name, std::string& seq) {

    const size_t t1 = line.find('\t');
    if (t1 == npos) offending_line(line);

    const size_t t2 = line.find('\t', t1 + 1);
    if (t2 == npos) offending_line(line);

    size_t t3 = line.find('\t', t2 + 1);
    if (t3 == npos) {
        t3 = line.find('\n', t2 + 1);
    }

    seq_name = std::string(line.substr(t1 + 1, t2 - (t1 + 1)));
    seq = std::string(line.substr(t2 + 1, t3 - (t2 + 1)));

}


std::pair<std::string, std::string> extract_L_nodes(std::string_view line) {

    const size_t t1 = line.find('\t');
    if (t1 == npos) offending_line(line);

    const size_t t2 = line.find('\t', t1 + 1);
    if (t2 == npos) offending_line(line);

    const size_t t3 = line.find('\t', t2 + 1);
    if (t3 == npos) offending_line(line);

    const size_t t4 = line.find('\t', t3 + 1);
    if (t4 == npos) offending_line(line);

    // token[1] = (t1+1 .. t2-1), token[3] = (t3+1 .. t4-1)
    const std::string_view from = line.substr(t1 + 1, t2 - (t1 + 1));
    const std::string_view to   = line.substr(t3 + 1, t4 - (t3 + 1));

    return {std::string(from), std::string(to)};
}


void extract_P_nodes(std::string_view line, std::string& path_name,
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

    // t2 is the position of the tab before the node list
    for (size_t i = t2 + 1; i < t3; ) {
        size_t comma_pos = line.find(',', i);
        if (comma_pos == npos || comma_pos > t3) {
            comma_pos = t3;
        }
        auto node = std::string(line.substr(i, comma_pos - i));
        node_list.push_back(node);
        i = comma_pos + 1;
    }
}