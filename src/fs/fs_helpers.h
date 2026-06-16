//
// Created by Fawaz Dabbaghie on 17/12/2025.
//

#ifndef GFAIDX_FS_HELPERS_H
#define GFAIDX_FS_HELPERS_H


#include <fstream>
#include <iostream>
#include <string>
#include <sys/stat.h>

bool file_exists(const char* file_name);
bool dir_exists(const char* dir_name);
bool file_writable(const char* file_name);
bool remove_file(const char* file_name);
// Remove a file if it exists, but do not fail if it is already gone.
void remove_path_if_exists(const std::string& path);
// Allocate a hidden temp path next to the final output so rename stays on one filesystem.
std::string make_temp_output_path(const std::string& final_path);
// Promote a fully written temp file into its final location, or throw on failure.
void rename_path_or_throw(const std::string& from_path, const std::string& to_path);

std::string create_temp_dir(const std::string& base_dir,
                            const std::string& prefix = "gfaidx_tmp_",
                            const std::string& latest_name = "latest",
                            bool keep_latest = true);

#endif //GFAIDX_FS_HELPERS_H
