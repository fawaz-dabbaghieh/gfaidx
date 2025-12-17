/*
 * Created by Fawaz Dabbaghie on 17/12/2025.
 *
 *
 * Fast buffered line reader (POSIX). Inspired by strangepg's GFA file reading design:
 * - fixed-size buffer
 * - memmove remainder to front on refill
 * - read() into tail
 * - memchr('\n') to locate EOL
 *
 * readLine() returns a string_view valid until the next readLine() call.
 * Long lines (> buffer) are supported via an internal fallback string.
 */


#ifndef GFAIDX_READER_H
#define GFAIDX_READER_H


#include <cstddef>
#include <string>
#include <string_view>
#include <vector>


class Reader {
public:
    struct Options {
        std::size_t read_size = 64 * 1024;    // similar to IOUNIT-ish defaults
        bool strip_cr = true;            // handle Windows CRLF files
    };

    Reader();
    explicit Reader(const Options& opt);

    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;

    Reader(Reader&&) noexcept;
    Reader& operator=(Reader&&) noexcept;

    ~Reader();

    // Open a file for reading. Returns false on failure (errno available via lastErrno()).
    bool open(const std::string& path);

    // Close the file (safe to call multiple times).
    void close();

    [[nodiscard]] bool is_open() const { return fd_ >= 0; }

    // Reads the next line. Returns false on EOF (or error; check lastErrno()).
    // The returned view excludes the trailing '\n' (and optional '\r').
    bool read_line(std::string_view& out);

    // Convenience overload returning a view.
    std::string_view read_line_view(bool& ok);

    // 0 if no error, else errno from the last failing syscall.
    [[nodiscard]] int last_error_no() const {
        return last_errno_;
    }

    // Number of lines successfully produced by readLine().
    [[nodiscard]] std::uint64_t line_number() const {
        return line_no_;
    }

    // Approximate file offset of the start of the *next unread* byte.
    // (Useful for debugging/progress; not required for parsing.)
    [[nodiscard]] std::uint64_t file_offset() const {
        return file_off_;
    }

private:
    void ensure_buffer_allocated();   // <-- add this

    bool refill();    // "slurp": move remainder to front, read more
    bool ensure_Eol_or_EoF(); // make sure we either find '\n' or hit EOF (or build long line)

    Options opt_;

    int fd_ = -1;
    int last_errno_ = 0;
    bool assembling_long_ = false;   // we are currently assembling a long line spanning refills
    bool long_ready_ = false;        // last call returned a view into long_line_; clear on next call

    // Buffer layout:
    // [0 .. end_) valid bytes, cur_ is the current cursor within that.
    std::vector<char> buf_;
    std::size_t cur_ = 0;
    std::size_t end_ = 0;
    bool eof_ = false;

    // strangepg tracks file offsets; we do similarly for observability
    std::uint64_t file_off_ = 0;
    std::uint64_t line_no_ = 0;

    // For lines longer than the buffer.
    std::string long_line_;
};


#endif //GFAIDX_READER_H