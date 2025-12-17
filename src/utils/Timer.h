//
// Created by Fawaz Dabbaghie on 17/12/2025.
//

#ifndef GFAIDX_TIMER_H
#define GFAIDX_TIMER_H

#include <chrono>


class Timer {
    using Clock = std::chrono::steady_clock;

    Clock::time_point m_beg {
        Clock::now()
    };

public:
    void reset() {
        m_beg = Clock::now();
    }
    // Returns elapsed time in seconds as a double
    [[nodiscard]] double elapsed() const {
        return std::chrono::duration_cast<std::chrono::duration<double>>(Clock::now() - m_beg).count();
    }
};


inline std::string get_time() {
    std::chrono::time_point now = std::chrono::system_clock::now();
    time_t in_time_t = std::chrono::system_clock::to_time_t(now);
    std::string time_str = std::ctime(&in_time_t);
    time_str.pop_back();
    return time_str;
}


#endif //GFAIDX_TIMER_H