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
// Allocate an unused temp path named after the final output, inside work_dir
// when given, otherwise next to the final output.
std::string make_temp_output_path(const std::string& final_path,
                                  const std::string& work_dir = std::string());
// Promote a fully written temp file into its final location, or throw on failure.
// Across filesystems, copies to a temp file beside to_path and renames that.
void rename_path_or_throw(const std::string& from_path, const std::string& to_path);

std::string create_temp_dir(const std::string& base_dir,
                            const std::string& prefix = "gfaidx_tmp_",
                            const std::string& latest_name = "latest",
                            bool keep_latest = true);

// Remove a run directory and its convenience symlink without deleting a
// symlink that a newer concurrent run has already replaced.
void cleanup_temp_dir(const std::string& temp_dir,
                      const std::string& latest_path);

#endif //GFAIDX_FS_HELPERS_H
