//
// Created by Fawaz Dabbaghie on 17/12/2025.
//

#include "fs_helpers.h"

bool file_exists(const char* file_name) {
    struct stat st{};
    return stat(file_name, &st) == 0;
}

bool dir_exists(const char* dir_name) {
    struct stat st{};
    return stat(dir_name, &st) == 0 && S_ISDIR(st.st_mode);
}

bool file_writable(const char* file_name) {
    std::ofstream f(file_name);
    return f.good();
}

bool remove_file(const char* file_name) {
    if (std::remove(file_name) != 0){
        std::cerr << "Error: could not remove file " << file_name << std::endl;
        return false;
    }
    return true;
}
