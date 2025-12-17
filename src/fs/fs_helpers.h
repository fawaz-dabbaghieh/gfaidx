//
// Created by Fawaz Dabbaghie on 17/12/2025.
//

#ifndef GFAIDX_FS_HELPERS_H
#define GFAIDX_FS_HELPERS_H


#include <fstream>
#include <sys/stat.h>
#include <iostream>


bool file_exists(const char* file_name);
bool dir_exists(const char* dir_name);
bool file_writable(const char* file_name);
bool remove_file(const char* file_name);

#endif //GFAIDX_FS_HELPERS_H