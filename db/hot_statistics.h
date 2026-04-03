#pragma once

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "core/db.h"
#include "core/utils.h"
#include "lib/consistent_hash.h"

namespace ycsbc {

class HotStatistics : public DB {
 public:
  HotStatistics()
      : outfile_("hot_statistics.output"),
        consistent_hash_("conf/server_ips.conf") {
    key_counter_.reserve(40000);
  }
  ~HotStatistics() {
    try {
      PrintLoadedKVs();
      PrintHotKeys(40000);
    } catch (const std::exception &e) {
      std::cerr << "Exception during Print: " << e.what() << std::endl;
    }

    if (outfile_.is_open()) {
      outfile_.close();
    }

    {
      std::lock_guard<std::mutex> lock1(counter_mutex_);
      std::unordered_map<std::string, std::atomic<int>>().swap(key_counter_);
    }

    {
      std::lock_guard<std::mutex> lock2(load_mutex_);
      std::unordered_map<std::string, std::vector<KVPair>>().swap(loaded_kvs_);
    }
  }

  void Init(const int /*thread_id*/) {
    std::lock_guard<std::mutex> lock(output_mutex_);
    std::cout << "A new thread begins working." << std::endl;
  }

  int Read(const std::string & /*table*/, const std::string &key,
           const std::vector<std::string> * /*fields*/,
           std::vector<KVPair> & /*result*/) override {
    IncrementKeyAccess(key);
    return 0;
  }

  int Insert(const std::string & /*table*/, const std::string &key,
             std::vector<KVPair> &values) override {
    std::lock_guard<std::mutex> lock(load_mutex_);
    loaded_kvs_[key] = values;
    return 0;
  }

  int Update(const std::string & /*table*/, const std::string &key,
             std::vector<KVPair> & /*values*/) override {
    IncrementKeyAccess(key);
    return 0;
  }

  int Scan(const std::string & /*table*/, const std::string &key, int /*len*/,
           const std::vector<std::string> * /*fields*/,
           std::vector<std::vector<KVPair>> & /*result*/) override {
    IncrementKeyAccess(key);
    return 0;
  }

  int Delete(const std::string & /*table*/, const std::string &key) override {
    IncrementKeyAccess(key);
    return 0;
  }

  void PrintLoadedKVs() {
    std::lock_guard<std::mutex> lock(load_mutex_);
    if (outfile_.is_open()) {
      outfile_ << "================ ALL KVs ================\n";
      for (auto &kv : loaded_kvs_) {
        std::string ip_str;
        utils::ReverseRTE_IPV4(uint32_t(consistent_hash_.GetServerIp(kv.first)),
                               ip_str);
        outfile_ << ip_str << " " << kv.first << " ";
        for (auto &v : kv.second) outfile_ << v.second << " ";
        outfile_ << "\n";
      }
      outfile_ << "=========================================\n";
    }
  }

  void PrintHotKeys(size_t top_n) {
    std::vector<std::pair<std::string, int>> vec;
    for (const auto &kv : key_counter_) {
      vec.emplace_back(kv.first, kv.second.load(std::memory_order_relaxed));
    }

    std::sort(vec.begin(), vec.end(),
              [](const auto &a, const auto &b) { return a.second > b.second; });

    if (outfile_.is_open()) {
      outfile_ << "\n+++++ Top " << top_n << " hot keys +++++\n";
      for (size_t i = 0; i < std::min(top_n, vec.size()); ++i) {
        std::string &key = vec[i].first;
        std::string ip_str;
        utils::ReverseRTE_IPV4(uint32_t(consistent_hash_.GetServerIp(key)),
                               ip_str);
        outfile_ << ip_str << " " << vec[i].first << " ";
        std::vector<KVPair> val;
        auto it = loaded_kvs_.find(key);
        if (it != loaded_kvs_.end()) {
          val = it->second;
          for (auto &v : val) {
            outfile_ << v.second << " ";
          }
        }
        outfile_ << vec[i].second << "\n";
      }
      outfile_ << "+++++++++++++++++++++++++++++++\n";
    }
  }

 private:
  void IncrementKeyAccess(const std::string &key) {
    auto it = key_counter_.find(key);
    if (it != key_counter_.end()) {
      it->second.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    std::lock_guard<std::mutex> lock(counter_mutex_);
    auto [iter, inserted] = key_counter_.emplace(key, 0);
    iter->second.fetch_add(1, std::memory_order_relaxed);
  }

  std::unordered_map<std::string, std::vector<KVPair>> loaded_kvs_;
  std::unordered_map<std::string, std::atomic<int>> key_counter_;
  std::mutex load_mutex_;
  std::mutex counter_mutex_;
  std::mutex output_mutex_;
  std::ofstream outfile_;
  const size_t CACHE_SIZE = 8192 * 3;
  ConsistentHash consistent_hash_;
};
}  // namespace ycsbc
