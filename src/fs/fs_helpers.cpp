//
// Created by Fawaz Dabbaghie on 17/12/2025.
//

#include "fs_helpers.h"

#include <chrono>
#include <filesystem>
#include <random>
#include <sstream>

bool file_exists(const char* file_name) {
    struct stat st{};
    return stat(file_name, &st) == 0;
}

bool dir_exists(const char* dir_name) {
    struct stat st{};
    return stat(dir_name, &st) == 0 && S_ISDIR(st.st_mode);
}

bool file_writable(const char* file_name) {
    const std::ofstream f(file_name);
    return f.good();
}

bool remove_file(const char* file_name) {
    if (std::remove(file_name) != 0){
        std::cerr << "Error: could not remove file " << file_name << std::endl;
        return false;
    }
    return true;
}

std::string create_temp_dir(const std::string& base_dir,
                            const std::string& prefix,
                            const std::string& latest_name ,
                            bool keep_latest) {
    // checks for tmp dir existence and creates it if not present
    // makes a lates symlink in case the user used the same tmp dir again
    std::filesystem::path base_path = base_dir.empty()
        ? std::filesystem::current_path()
        : std::filesystem::path(base_dir);

    if (!std::filesystem::exists(base_path)) {
        std::filesystem::create_directories(base_path);
    }

    // random directory name generation
    // std::random_device rd;
    // std::mt19937_64 gen(rd());
    // std::uniform_int_distribution<unsigned long long> dist;
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();


    std::filesystem::path tmp_path;
    for (int attempt = 0; attempt < 10; ++attempt) {
        std::ostringstream name;
        name << prefix << now;
        tmp_path = base_path / name.str();
        if (!std::filesystem::exists(tmp_path)) {
            std::filesystem::create_directories(tmp_path);
            break;
        }
    }

    if (tmp_path.empty() || !std::filesystem::exists(tmp_path)) {
        throw std::runtime_error("Failed to create temporary directory after several tries in: " + base_path.string());
    }

    if (keep_latest) {
        std::filesystem::path latest_path = base_path / latest_name;
        std::error_code ec;
        if (std::filesystem::exists(latest_path) || std::filesystem::is_symlink(latest_path)) {
            std::filesystem::remove(latest_path, ec);
            ec.clear();
        }
        std::filesystem::create_directory_symlink(std::filesystem::absolute(tmp_path),
                                                  latest_path,
                                                  ec);
        if (ec) {
            std::cerr << "Warning: could not create latest symlink at "
                      << latest_path.string() << std::endl;
        }
    }

    return tmp_path.string();
}
