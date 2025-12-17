/*
 * Created by Fawaz Dabbaghie on 17/12/2025.
 *
 *
 * Fast buffered line reader (POSIX). Inspired by strangepg's File/slurp/geteol design:
 * - fixed-size buffer
 * - memmove remainder to front on refill
 * - read() into tail
 * - memchr('\n') to locate EOL
 *
 * readLine() returns a string_view valid until the next readLine() call.
 * Long lines (> buffer) are supported via an internal fallback string.
 */

#include "Reader.h"

#include <cerrno>
#include <cstring>

#include <fcntl.h>
#include <unistd.h>



void Reader::ensureBufferAllocated() {
    if (!buf_.empty()) return;

    if (opt_.readSize == 0) opt_.readSize = 8192; // safety
    buf_.resize(opt_.readSize * 2 + 1);
}

// Reader::Reader() {
//     // opt_ already has defaults; just allocate
//     ensureBufferAllocated();
// }

Reader::Reader(const Options& opt) : opt_(opt) {
    ensureBufferAllocated();
}

// Reader::Reader(const Options& opt) : opt_(opt) {
//     // Use ~2x readSize like strangepg does (it uses 2*Readsz + 1).
//     buf_.resize(opt_.readSize * 2 + 1);
// }

Reader::Reader(Reader&& other) noexcept {
    *this = std::move(other);
}

Reader& Reader::operator=(Reader&& other) noexcept {
    if (this == &other) return *this;
    close();

    opt_ = other.opt_;
    fd_ = other.fd_;
    last_errno_ = other.last_errno_;
    buf_ = std::move(other.buf_);
    cur_ = other.cur_;
    end_ = other.end_;
    eof_ = other.eof_;
    file_off_ = other.file_off_;
    line_no_ = other.line_no_;
    long_line_ = std::move(other.long_line_);

    other.fd_ = -1;
    other.last_errno_ = 0;
    other.cur_ = other.end_ = 0;
    other.eof_ = false;
    other.file_off_ = 0;
    other.line_no_ = 0;
    other.long_line_.clear();
    return *this;
}

Reader::~Reader() { close(); }

bool Reader::open(const std::string& path) {
    close();
    ensureBufferAllocated();

    last_errno_ = 0;
    eof_ = false;
    cur_ = end_ = 0;
    file_off_ = 0;
    line_no_ = 0;
    long_line_.clear();

    fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd_ < 0) {
        last_errno_ = errno;
        return false;
    }
    return true;
}

void Reader::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool Reader::refill() {
    // Equivalent to strangepg's slurp():
    // - if there is remainder [cur_, end_), memmove it to front
    // - read more into the tail
    if (eof_) return true;

    const std::size_t rem = (end_ > cur_) ? (end_ - cur_) : 0;
    if (rem && cur_ > 0) {
        std::memmove(buf_.data(), buf_.data() + cur_, rem);
    }
    cur_ = 0;
    end_ = rem;

    const std::size_t cap = buf_.size() - end_;
    const std::size_t want = opt_.readSize < cap ? opt_.readSize : cap;

    ssize_t n = ::read(fd_, buf_.data() + end_, want);
    if (n < 0) {
        last_errno_ = errno;
        return false;
    }
    if (n == 0) {
        eof_ = true;
        return true;
    }
    end_ += static_cast<std::size_t>(n);
    return true;
}

bool Reader::ensureEolOrEof() {
    const char* base = buf_.data();
    const std::size_t avail = (end_ > cur_) ? (end_ - cur_) : 0;

    // Fast path: newline already present in buffer remainder
    if (avail > 0) {
        if (std::memchr(base + cur_, '\n', avail) != nullptr) return true;
        if (eof_) return true; // final unterminated line
    }

    // If no newline and EOF, we're done (unterminated final line)
    if (eof_) return true;

    // Start assembling a long line across refills
    assembling_long_ = true;
    long_line_.clear();

    for (;;) {
        // append remainder
        const std::size_t r = (end_ > cur_) ? (end_ - cur_) : 0;
        if (r > 0) {
            long_line_.append(base + cur_, r);
            cur_ = end_;
        }

        if (!refill()) return false;
        if (cur_ >= end_ && eof_) return true;

        const std::size_t a = (end_ > cur_) ? (end_ - cur_) : 0;
        const void* p = (a > 0) ? std::memchr(base + cur_, '\n', a) : nullptr;
        if (p) return true;

        if (eof_) return true;
    }
}


bool Reader::readLine(std::string_view& out) {
    out = {};

    // If last call returned a view into long_line_, it's now safe to clear it.
    if (long_ready_) {
        long_line_.clear();
        long_ready_ = false;
    }

    if (fd_ < 0) { last_errno_ = EBADF; return false; }
    last_errno_ = 0;

    // If buffer exhausted, refill
    if (cur_ >= end_) {
        if (!refill()) return false;
        if (cur_ >= end_ && eof_) return false;
    }

    // Ensure we have an EOL or EOF (possibly long-line assembly)
    if (!ensureEolOrEof()) return false;

    const char* base = buf_.data();

    // If long_line_ is in use, we are assembling a line spanning refills
    if (assembling_long_) {
        // Find '\n' in current buffer chunk (or EOF)
        const std::size_t avail = (end_ > cur_) ? (end_ - cur_) : 0;
        const char* nl = (avail > 0) ? static_cast<const char*>(std::memchr(base + cur_, '\n', avail)) : nullptr;

        if (nl) {
            const std::size_t take = static_cast<std::size_t>(nl - (base + cur_));
            long_line_.append(base + cur_, take);
            file_off_ += take;

            // Consume '\n'
            cur_ += take + 1;
            file_off_ += 1;
        } else {
            // EOF-terminated
            long_line_.append(base + cur_, avail);
            file_off_ += avail;
            cur_ = end_;
        }

        // Strip CR if requested
        if (opt_.stripCR && !long_line_.empty() && long_line_.back() == '\r') {
            long_line_.pop_back();
        }

        ++line_no_;
        out = std::string_view(long_line_.data(), long_line_.size());
        assembling_long_ = false;
        long_ready_ = true;
        return true;
    }

    // Normal fast path: the whole line is in the buffer
    const std::size_t avail = (end_ > cur_) ? (end_ - cur_) : 0;
    const char* nl = (avail > 0) ? static_cast<const char*>(std::memchr(base + cur_, '\n', avail)) : nullptr;

    if (nl) {
        const std::size_t len = static_cast<std::size_t>(nl - (base + cur_));
        std::size_t out_len = len;

        // Handle CRLF
        if (opt_.stripCR && out_len > 0 && base[cur_ + out_len - 1] == '\r') {
            --out_len;
        }

        out = std::string_view(base + cur_, out_len);

        // Consume line + '\n'
        cur_ += len + 1;
        file_off_ += (len + 1);

        ++line_no_;
        return true;
    }

    // No '\n' found: must be EOF and unterminated last line
    if (eof_) {
        std::size_t len = avail;
        if (opt_.stripCR && len > 0 && base[cur_ + len - 1] == '\r') --len;

        out = std::string_view(base + cur_, len);

        cur_ = end_;
        file_off_ += avail;

        ++line_no_;
        return true;
    }

    // Should not happen due to ensureEolOrEof(), but keep it safe:
    return readLine(out);
}

std::string_view Reader::readLineView(bool& ok) {
    std::string_view v;
    ok = readLine(v);
    return v;
}
