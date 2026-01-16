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
 *
 * I need to check if I can change the lon_line_ string assembly and not use append
 * Maybe I can just keep adding to the buffer, however, this will introduce other edge cases:
 * e.g., the buffer can get way too big if there's a really long line, so I need to limit the buffer size
 * which will introduce too much complexity to the code...hmmm, I think for now I'll leave it as is with the append
 * In the end, majority of lines are smaller than the buffer, so it's only the annoying path lines that are very long
 */

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>

#include "Reader.h"
#include "utils/Timer.h"


void Reader::ensure_buffer_allocated() {
    if (!buf_.empty()) return;

    if (opt_.read_size == 0) opt_.read_size = 64 * 1024; // safety
    buf_.resize(opt_.read_size * 2 + 1);
}

void Reader::report_progress() const{
    if (opt_.progress_every == 0) return;
    if (line_no_ % opt_.progress_every == 0) {
        std::cout << get_time() << ": Read " << line_no_ << " lines" << std::endl;
    }
}

Reader::Reader() {
    // opt_ already has defaults; just allocate
    ensure_buffer_allocated();
}

Reader::Reader(const Options& opt) : opt_(opt) {
    ensure_buffer_allocated();
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
    is_gzip_ = other.is_gzip_;
    gzip_eof_ = other.gzip_eof_;
    z_init_ = other.z_init_;
    strm_ = other.strm_;
    gz_inbuf_ = std::move(other.gz_inbuf_);

    other.fd_ = -1;
    other.last_errno_ = 0;
    other.cur_ = other.end_ = 0;
    other.eof_ = false;
    other.file_off_ = 0;
    other.line_no_ = 0;
    other.long_line_.clear();
    other.is_gzip_ = false;
    other.gzip_eof_ = false;
    other.z_init_ = false;
    other.strm_ = {};
    other.gz_inbuf_.clear();
    return *this;
}

Reader::~Reader() {
    close();
}

bool Reader::open(const std::string& path) {
    close();
    ensure_buffer_allocated();

    last_errno_ = 0;
    eof_ = false;
    cur_ = end_ = 0;
    file_off_ = 0;
    line_no_ = 0;
    long_line_.clear();
    is_gzip_ = false;
    gzip_eof_ = false;
    z_init_ = false;
    strm_ = {};

    // O_RDONLY open for reading only
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ < 0) {
        last_errno_ = errno;
        return false;
    }
    // Peek at the first two bytes to detect gzip magic (0x1f, 0x8b).
    unsigned char magic[2] = {0, 0};
    const ssize_t n = ::read(fd_, magic, sizeof(magic));
    if (n < 0) {
        last_errno_ = errno;
        close();
        return false;
    }
    // checking the magic bytes to see if it's gzip file
    if (n == static_cast<ssize_t>(sizeof(magic)) && magic[0] == 0x1f && magic[1] == 0x8b) {
        is_gzip_ = true;
    }
    // Reset file descriptor to the start for normal reading.
    if (::lseek(fd_, 0, SEEK_SET) < 0) {
        last_errno_ = errno;
        close();
        return false;
    }
    if (is_gzip_) {
        // Initialize zlib for gzip decoding (15 + 16 enables gzip wrapper).
        if (inflateInit2(&strm_, 15 + 16) != Z_OK) {
            last_errno_ = EINVAL;  // invalid argument
            close();
            return false;
        }
        z_init_ = true;
        // Buffer for compressed input chunks.
        if (opt_.read_size == 0) opt_.read_size = 64 * 1024;
        gz_inbuf_.resize(opt_.read_size);
    }
    return true;
}

void Reader::close() {
    if (z_init_) {
        inflateEnd(&strm_);
        z_init_ = false;
    }
    is_gzip_ = false;
    gzip_eof_ = false;
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool Reader::refill() {
    return is_gzip_ ? refill_gzip() : refill_plain();
}

bool Reader::refill_plain() {
    // Equivalent to strangepg's slurp():
    // - if there is remainder [cur_, end_), memmove it to front
    // - read more into the tail

    if (eof_) return true;  // nothing more to read

    const std::size_t remainder = (end_ > cur_) ? (end_ - cur_) : 0;
    if (remainder && cur_ > 0) {
        std::memmove(buf_.data(), buf_.data() + cur_, remainder);
    }
    cur_ = 0;
    end_ = remainder;

    // when empty, the cap is the complete size
    const std::size_t cap = buf_.size() - end_;
    const std::size_t want = opt_.read_size < cap ? opt_.read_size : cap;

    const ssize_t n = ::read(fd_, buf_.data() + end_, want);
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

bool Reader::refill_gzip() {
    if (eof_) return true;

    // Preserve any unread bytes by moving them to the front of the buffer.
    const std::size_t remainder = (end_ > cur_) ? (end_ - cur_) : 0;
    if (remainder && cur_ > 0) {
        std::memmove(buf_.data(), buf_.data() + cur_, remainder);
    }
    cur_ = 0;
    end_ = remainder;

    while (end_ < buf_.size()) {
        // If the inflater has no input, read more compressed bytes.
        if (strm_.avail_in == 0 && !gzip_eof_) {
            const ssize_t n = ::read(fd_, gz_inbuf_.data(), gz_inbuf_.size());
            if (n < 0) {
                last_errno_ = errno;
                return false;
            }
            if (n == 0) {
                gzip_eof_ = true;
            } else {
                strm_.next_in = gz_inbuf_.data();
                strm_.avail_in = static_cast<uInt>(n);
            }
        }

        // No more compressed input and nothing left to feed the inflater.
        if (strm_.avail_in == 0 && gzip_eof_) {
            eof_ = true;
            return true;
        }

        // Inflate into the remaining space in the output buffer.
        strm_.next_out = reinterpret_cast<Bytef*>(buf_.data() + end_);
        strm_.avail_out = static_cast<uInt>(buf_.size() - end_);

        int ret = inflate(&strm_, Z_NO_FLUSH);
        std::size_t produced = (buf_.size() - end_) - strm_.avail_out;
        end_ += produced;

        if (ret == Z_STREAM_END) {
            // End of a gzip member. If there are leftover input bytes, start a new member.
            const uInt remaining = strm_.avail_in;
            Bytef* next_in = strm_.next_in;
            inflateEnd(&strm_);
            z_init_ = false;
            if (remaining > 0) {
                strm_ = {};
                if (inflateInit2(&strm_, 15 + 16) != Z_OK) {
                    last_errno_ = EINVAL;
                    return false;
                }
                z_init_ = true;
                strm_.next_in = next_in;
                strm_.avail_in = remaining;
                // If we already produced output, let the caller consume it first.
                if (produced > 0) return true;
                continue;
            }
            gzip_eof_ = true;
            // Return if we produced output before hitting end-of-stream.
            if (produced > 0) return true;
            continue;
        }

        if (ret != Z_OK) {
            last_errno_ = EINVAL;
            return false;
        }

        // Return as soon as we have some decompressed data to parse.
        if (produced > 0) return true;
    }

    return true;
}

bool Reader::ensure_Eol_or_EoF() {
    const char* base = buf_.data();
    const std::size_t available = (end_ > cur_) ? (end_ - cur_) : 0;

    // Fast path: newline already present in buffer remainder
    if (available > 0) {
        if (std::memchr(base + cur_, '\n', available) != nullptr) return true;
        if (eof_) return true; // final unterminated line
    }

    // If no newline and EOF, we're done (unterminated final line)
    if (eof_) return true;

    // Start assembling a long line across refills
    assembling_long_ = true;
    long_line_.clear();

    // dealing with long file lines
    for (;;) {
        // append remainder
        const std::size_t remainder = (end_ > cur_) ? (end_ - cur_) : 0;
        if (remainder > 0) {
            long_line_.append(base + cur_, remainder);
            cur_ = end_;
        }

        if (!refill()) return false;
        if (cur_ >= end_ && eof_) return true;

        // how much available in buffer
        const std::size_t a = (end_ > cur_) ? (end_ - cur_) : 0;
        // is there \n
        const void* p = (a > 0) ? std::memchr(base + cur_, '\n', a) : nullptr;
        if (p) return true;

        if (eof_) return true;
    }
}


bool Reader::read_line(std::string_view& out) {
    // We have several cases here:
    // 1: Buffer is exhausted (empty) so we'll go into refill, e.g., first line
    // 2: buffer already have data, we need to look for next \n and adjust current and end
    // 3: We have a long line that doesn't fit in the buffer, so we need use the long_line_ string
    // Note: I think I need to look into the long_line_ string, maybe there's a better way to do this
    out = {};

    // If last call returned a view into long_line_, it's now safe to clear it.
    if (long_ready_) {
        long_line_.clear();
        long_ready_ = false;
    }

    // EBADF is a Bad file descriptor
    if (fd_ < 0) {
        last_errno_ = EBADF; return false;
    }
    last_errno_ = 0;

    // If buffer exhausted, refill
    if (cur_ >= end_) {
        if (!refill()) return false;
        if (cur_ >= end_ && eof_) return false;
    }

    // Ensure we have an \n or we reached the end of the file
    // possible long line
    if (!ensure_Eol_or_EoF()) return false;

    const char* base = buf_.data();

    // If long_line_ is in use, we are assembling a line spanning refills
    if (assembling_long_) {
        // Find '\n' in current buffer chunk (or EOF)
        const std::size_t available = (end_ > cur_) ? (end_ - cur_) : 0;
        const char* nl = (available > 0) ? static_cast<const char*>(std::memchr(base + cur_, '\n', available)) : nullptr;

        if (nl) {
            const auto take = static_cast<std::size_t>(nl - (base + cur_));
            long_line_.append(base + cur_, take);
            file_off_ += take;

            // Consume '\n'
            cur_ += take + 1;
            file_off_ += 1;
        } else {
            // EOF-terminated
            long_line_.append(base + cur_, available);
            file_off_ += available;
            cur_ = end_;
        }

        // Strip CR if requested
        if (opt_.strip_cr && !long_line_.empty() && long_line_.back() == '\r') {
            long_line_.pop_back();
        }

        ++line_no_;
        report_progress();
        out = std::string_view(long_line_.data(), long_line_.size());
        assembling_long_ = false;
        long_ready_ = true;

        return true;
    }

    // Normal fast path: the whole line is in the buffer
    const std::size_t available = (end_ > cur_) ? (end_ - cur_) : 0;
    const char* nl = (available > 0) ? static_cast<const char*>(std::memchr(base + cur_, '\n', available)) : nullptr;

    // line is in buffer, process it
    if (nl) {
        const auto len = static_cast<std::size_t>(nl - (base + cur_));
        std::size_t out_len = len;

        // Handle CRLF
        if (opt_.strip_cr && out_len > 0 && base[cur_ + out_len - 1] == '\r') {
            --out_len;
        }
        out = std::string_view(base + cur_, out_len);
        // Consume line + '\n'
        cur_ += len + 1;
        file_off_ += (len + 1);

        ++line_no_;
        report_progress();
        return true;
    }

    // No '\n' found: must be EOF and unterminated last line
    if (eof_) {
        std::size_t len = available;
        if (opt_.strip_cr && len > 0 && base[cur_ + len - 1] == '\r') --len;

        out = std::string_view(base + cur_, len);

        cur_ = end_;
        file_off_ += available;

        ++line_no_;
        report_progress();
        return true;
    }

    // just to be safe, but shouldn't go in here
    return read_line(out);
}

std::string_view Reader::read_line_view(bool& ok) {
    std::string_view v;
    ok = read_line(v);
    return v;
}
