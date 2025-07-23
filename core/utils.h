//
//  utils.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/5/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_UTILS_H_
#define YCSB_C_UTILS_H_

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <exception>
#include <random>

namespace utils {

const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325;
const uint64_t kFNVPrime64 = 1099511628211;

inline uint64_t FNVHash64(uint64_t val) {
  uint64_t hash = kFNVOffsetBasis64;

  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hash = hash ^ octet;
    hash = hash * kFNVPrime64;
  }
  return hash;
}

inline uint64_t Hash(uint64_t val) { return FNVHash64(val); }

inline double RandomDouble(double min = 0.0, double max = 1.0) {
  static std::default_random_engine generator;
  static std::uniform_real_distribution<double> uniform(min, max);
  return uniform(generator);
}

///
/// Returns an ASCII code that can be printed to desplay
///
inline char RandomPrintChar() { return rand() % 94 + 33; }

class Exception : public std::exception {
 public:
  Exception(const std::string &message) : message_(message) {}
  const char *what() const noexcept { return message_.c_str(); }

 private:
  std::string message_;
};

inline bool StrToBool(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
  if (str == "true" || str == "1") {
    return true;
  } else if (str == "false" || str == "0") {
    return false;
  } else {
    throw Exception("Invalid bool string: " + str);
  }
}

inline std::string Trim(const std::string &str) {
  auto front = std::find_if_not(str.begin(), str.end(),
                                [](int c) { return std::isspace(c); });
  return std::string(
      front,
      std::find_if_not(str.rbegin(), std::string::const_reverse_iterator(front),
                       [](int c) { return std::isspace(c); })
          .base());
}

inline void ReverseRTE_IPV4(uint32_t ip, std::string &result) {
  uint8_t a = (ip >> 24) & 0xFF;
  uint8_t b = (ip >> 16) & 0xFF;
  uint8_t c = (ip >> 8) & 0xFF;
  uint8_t d = ip & 0xFF;

  result = std::to_string(d) + "." + std::to_string(c) + "." +
           std::to_string(b) + "." + std::to_string(a);
}

class RequestIDGenerator {
 private:
  static std::atomic<uint32_t> counter;

 public:
  static uint32_t next() {
    return counter.fetch_add(1, std::memory_order_relaxed);
  }

  static void reset() { counter.store(0, std::memory_order_relaxed); }
};

static inline uint32_t global_request_id() {
  static std::atomic<uint32_t> counter{0};
  return counter.fetch_add(1, std::memory_order_relaxed);
}

static inline uint32_t generate_request_id(uint32_t thread_id) {
  constexpr uint32_t MAX_REQ_PER_THREAD = 10'000'000;

  thread_local static uint32_t local_counter = 0;

  return thread_id * MAX_REQ_PER_THREAD + local_counter++;
}

}  // namespace utils

#endif  // YCSB_C_UTILS_H_
