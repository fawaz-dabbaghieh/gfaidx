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

std::string create_temp_dir(const std::string& base_dir,
                            const std::string& prefix = "gfaidx_tmp_",
                            const std::string& latest_name = "latest",
                            bool keep_latest = true);

#endif //GFAIDX_FS_HELPERS_H
