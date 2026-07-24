//
// Created by Fawaz Dabbaghie on 17/12/2025.
//

#include "fs_helpers.h"

#include <chrono>
#include <filesystem>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <unistd.h>

// Short alias so the helper code below stays readable.
namespace fs = std::filesystem;

bool file_exists(const char* file_name) {
    struct stat st{};
    return stat(file_name, &st) == 0;
}

bool dir_exists(const char* dir_name) {
    struct stat st{};
    return stat(dir_name, &st) == 0 && S_ISDIR(st.st_mode);
}

bool file_writable(const char* file_name) {
    // Resolve the requested output path and the directory that must accept the write.
    fs::path path(file_name);
    fs::path parent = path.has_parent_path() ? path.parent_path() : fs::current_path();

    // If the parent directory is missing or cannot be inspected, the path is not writable.
    std::error_code ec;
    if (!fs::exists(parent, ec) || ec) {
        return false;
    }
    // Check directory write permission without creating or truncating the target file.
    if (::access(parent.c_str(), W_OK) != 0) {
        return false;
    }

    // A not-yet-created file is acceptable as long as its parent directory is writable.
    if (!fs::exists(path, ec) || ec) {
        return true;
    }
    // Existing files must also be writable themselves.
    return ::access(path.c_str(), W_OK) == 0;
}

bool remove_file(const char* file_name) {
    if (std::remove(file_name) != 0){
        std::cerr << "Error: could not remove file " << file_name << std::endl;
        return false;
    }
    return true;
}

void remove_path_if_exists(const std::string& path) {
    // Cleanup paths are best-effort; ignore "already removed" style failures.
    std::error_code ec;
    fs::remove(path, ec);
}

std::string make_temp_output_path(const std::string& final_path) {
    // Keep temp outputs beside the final file so a later rename is atomic on the same filesystem.
    fs::path target(final_path);
    fs::path parent = target.has_parent_path() ? target.parent_path() : fs::current_path();
    // Reuse the final basename so staged files are still recognizable during debugging.
    const std::string base_name = target.filename().string();
    // Use a timestamp suffix to reduce collisions across concurrent or repeated runs.
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();

    // Probe a bounded set of candidate names until we find one that does not already exist.
    for (int attempt = 0; attempt < 100; ++attempt) {
        std::ostringstream candidate_name;
        candidate_name << "." << base_name << ".gfaidx_tmp_" << stamp << "_" << attempt;
        fs::path candidate = parent / candidate_name.str();
        std::error_code ec;
        // Return the first unused sibling path.
        if (!fs::exists(candidate, ec) && !ec) {
            return candidate.string();
        }
    }

    // If every candidate collided, surface that as a hard error to the caller.
    throw std::runtime_error("Failed to allocate a temporary output path next to: " + final_path);
}

void rename_path_or_throw(const std::string& from_path, const std::string& to_path) {
    // Publish the staged file into place only after the full write has already succeeded.
    std::error_code ec;
    fs::rename(from_path, to_path, ec);
    if (ec) {
        throw std::runtime_error("Failed to rename '" + from_path + "' to '" + to_path + "': " + ec.message());
    }
}

std::string create_temp_dir(const std::string& base_dir,
                            const std::string& prefix,
                            const std::string& latest_name ,
                            bool keep_latest) {
    // checks for tmp dir existence and creates it if not present
    // makes a lates symlink in case the user used the same tmp dir again
    fs::path base_path = base_dir.empty()
        ? fs::current_path()
        : fs::path(base_dir);

    if (!fs::exists(base_path)) {
        fs::create_directories(base_path);
    }

    // random directory name generation
    // std::random_device rd;
    // std::mt19937_64 gen(rd());
    // std::uniform_int_distribution<unsigned long long> dist;
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();


    fs::path tmp_path;
    for (int attempt = 0; attempt < 10; ++attempt) {
        std::ostringstream name;
        name << prefix << now;
        if (attempt != 0) {
            // Preserve the readable timestamp name while still making later
            // collision attempts distinct.
            name << "_" << attempt;
        }
        tmp_path = base_path / name.str();
        if (!fs::exists(tmp_path)) {
            fs::create_directories(tmp_path);
            break;
        }
    }

    if (tmp_path.empty() || !fs::exists(tmp_path)) {
        throw std::runtime_error("Failed to create temporary directory after several tries in: " + base_path.string());
    }

    if (keep_latest) {
        fs::path latest_path = base_path / latest_name;
        std::error_code ec;
        if (fs::exists(latest_path) || fs::is_symlink(latest_path)) {
            fs::remove(latest_path, ec);
            ec.clear();
        }
        fs::create_directory_symlink(fs::absolute(tmp_path), latest_path, ec);
        if (ec) {
            std::cerr << "Warning: could not create latest symlink at "
                      << latest_path.string() << std::endl;
        }
    }

    return tmp_path.string();
}

void cleanup_temp_dir(const std::string& temp_dir,
                      const std::string& latest_path) {
    std::error_code ec;
    const fs::path link_path(latest_path);

    // Only remove the convenience link when it still names this run. Another
    // process may have replaced it with a link to a newer temp directory.
    if (fs::is_symlink(link_path, ec) && !ec) {
        const auto raw_target = fs::read_symlink(link_path, ec);
        if (!ec) {
            std::error_code resolved_ec;
            const auto resolved_target = fs::absolute(
                raw_target.is_absolute()
                    ? raw_target
                    : link_path.parent_path() / raw_target,
                resolved_ec).lexically_normal();
            std::error_code expected_ec;
            const auto expected_target =
                fs::absolute(fs::path(temp_dir), expected_ec).lexically_normal();
            if (!resolved_ec && !expected_ec &&
                resolved_target == expected_target) {
                fs::remove(link_path, ec);
            }
        }
    }

    // Removing the run directory also removes any partially written files
    // after a normal completion or a handled error.
    ec.clear();
    fs::remove_all(temp_dir, ec);
}
