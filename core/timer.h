//
//  timer.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_TIMER_H_
#define YCSB_C_TIMER_H_

#include <sys/time.h>

#include <chrono>

inline uint64_t get_now_micros() {
  using Clock = std::chrono::high_resolution_clock;
  return std::chrono::duration_cast<std::chrono::microseconds>(
             Clock::now().time_since_epoch())
      .count();
}

namespace utils {

template <typename Duration = std::chrono::milliseconds>
class Timer {
 public:
  void Start() { time_ = Clock::now(); }

  typename Duration::rep End() {
    auto elapsed = Elapsed();
    Reset();
    return elapsed;
  }

  typename Duration::rep Elapsed() const {
    return std::chrono::duration_cast<Duration>(Clock::now() - time_).count();
  }

  void Reset() { time_ = Clock::now(); }

 private:
  using Clock = std::chrono::high_resolution_clock;
  Clock::time_point time_;
};

}  // namespace utils

#endif  // YCSB_C_TIMER_H_
