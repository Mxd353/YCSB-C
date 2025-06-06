#ifndef YCSB_C_CACHE_MIGRATION_H__
#define YCSB_C_CACHE_MIGRATION_H__

#include <atomic>
#include <boost/asio.hpp>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "core/core_workload.h"
#include "core/db.h"
#include "core/properties.h"
#include "utils/cache_migration_socket.h"
#include "utils/consistent_hash.h"

namespace ycsbc {
class CacheMigration : public DB {
  using VethInfo = CacheMigrationSocket::VethInfo;

 private:
  ConsistentHash consistent_hash_;
  static thread_local std::shared_ptr<CacheMigrationSocket> thread_socket_;

  std::ofstream logfile_;
  const int num_threads_;
  std::atomic<size_t> read_count_{0};
  std::atomic<size_t> read_success_{0};
  std::atomic<size_t> update_count_{0};
  std::atomic<size_t> update_success_{0};
  std::atomic<size_t> no_result_{0};
  std::atomic<size_t> update_failed_{0};

  std::mutex veth_pool_mutex_;

  std::vector<VethInfo> available_veths_;

  static thread_local VethInfo thread_veth_config_;

  std::vector<KVPair> DEFAULT_VALUES = {{"field0", "read"},
                                        {"field1", "read"},
                                        {"field2", "read"},
                                        {"field3", "read"}};

  void PrintStats() override;

 public:
  explicit CacheMigration(int num_threads);
  virtual ~CacheMigration();
  void Init() override;
  void Close() override;
  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) override;
  int Scan(const std::string & /*table*/, const std::string & /*key*/,
           int /*record_count*/, const std::vector<std::string> * /*fields*/,
           std::vector<std::vector<KVPair>> & /*result*/) override {
    return DB::kOK;  // Not implemented for now
  };
  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) override;
  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) override {
    return Update(table, key, values);
  };
  int Delete(const std::string &table, const std::string &key) override {
    return Update(table, key, DEFAULT_VALUES);
  };
};
}  // namespace ycsbc

#endif  // YCSB_C_CACHE_MIGRATION_H__
