#ifndef __CACHE_MIGRATION_DPDK_H__
#define __CACHE_MIGRATION_DPDK_H__
#include <rte_eal.h>
#include <rte_mbuf.h>

#include <atomic>
#include <memory>
#include <vector>

#include "core/db.h"
#include "core/properties.h"
#include "lib/c_m_proto.h"
#include "lib/consistent_hash.h"

// TX_NUM_MBUFS: Maximum packet templates that can be built
// Actual packets built = min(operationcount, TX_NUM_MBUFS)
// More requests are sent by reusing these templates cyclically
#define TX_NUM_MBUFS 1'048'575
#define RX_NUM_MBUFS 32'767
#define MBUF_CACHE_SIZE 256
#define TX_MBUF_DATA_SIZE 256
#define RX_MBUF_DATA_SIZE 512
#define RX_RING_SIZE 2048
#define TX_RING_SIZE 1024
#define TX_RING_COUNT 32
#define BURST_SIZE 32
#define REQ_SIZE 100'000'000.0

#define RTE_LOGTYPE_CORE RTE_LOGTYPE_USER3
#define RTE_LOGTYPE_PACKET RTE_LOGTYPE_USER4
#define RTE_LOGTYPE_PORT RTE_LOGTYPE_USER5

extern std::atomic<bool> running;

struct RequestInfo {
  std::atomic<uint64_t> start_time{0};

  std::atomic<int> retry_count{0};
  std::atomic<bool> completed{false};
  std::atomic<bool> time_out{false};

  RequestInfo() = default;

  RequestInfo(RequestInfo&& other) noexcept
      : start_time(other.start_time.load(std::memory_order_relaxed)),
        retry_count(other.retry_count.load(std::memory_order_relaxed)),
        completed(other.completed.load(std::memory_order_relaxed)),
        time_out(other.time_out.load(std::memory_order_relaxed)) {
    other.start_time.store(0, std::memory_order_relaxed);
    other.retry_count.store(0, std::memory_order_relaxed);
    other.completed.store(false, std::memory_order_relaxed);
    other.time_out.store(false, std::memory_order_relaxed);
  }

  RequestInfo& operator=(RequestInfo&& other) noexcept {
    if (this != &other) {
      start_time.store(other.start_time.load(std::memory_order_relaxed),
                       std::memory_order_relaxed);
      retry_count.store(other.retry_count.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
      completed.store(other.completed.load(std::memory_order_relaxed),
                      std::memory_order_relaxed);
      time_out.store(other.time_out.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);

      other.start_time.store(0, std::memory_order_relaxed);
      other.retry_count.store(0, std::memory_order_relaxed);
      other.completed.store(false, std::memory_order_relaxed);
      other.time_out.store(false, std::memory_order_relaxed);
    }
    return *this;
  }

  void clear() {
    start_time.store(0, std::memory_order_relaxed);
    retry_count.store(0, std::memory_order_relaxed);
    completed.store(false, std::memory_order_relaxed);
    time_out.store(false, std::memory_order_relaxed);
  }

  RequestInfo(const RequestInfo&) = delete;
  RequestInfo& operator=(const RequestInfo&) = delete;
};

struct TxConf {
  uint lcore_id;
  uint16_t queue_id = 0;
  std::pair<size_t, size_t> interval{0, 0};
};

struct RxConf {
  uint lcore_id;
  uint16_t queue_id = 0;
};

struct TimeoutConf {
  uint lcore_id;
  uint16_t queue_id = 0;
  std::pair<size_t, size_t> interval{0, 0};
};

namespace ycsbc {
class CacheMigrationDpdk : public DB {
 public:
  struct ThreadStats {
    uint64_t total_latency_us = 0;
    uint64_t completed_requests = 0;
    uint64_t iops = 0;
  };

  CacheMigrationDpdk(utils::Properties& props);
  ~CacheMigrationDpdk();
  void AllocateSpace(size_t total_ops, size_t req_size) override;
  void Init(const int thread_id) override;
  void Close() override;
  void StartDpdk();
  int Read(const std::string& table, const std::string& key,
           const std::vector<std::string>* fields,
           std::vector<KVPair>& result) override;
  int Insert(const std::string& table, const std::string& key,
             std::vector<KVPair>& values) override;
  int Update(const std::string& table, const std::string& key,
             std::vector<KVPair>& values) override;
  int Delete(const std::string& table, const std::string& key) override;
  int Scan(const std::string& table, const std::string& key, int record_count,
           const std::vector<std::string>* fields,
           std::vector<std::vector<KVPair>>& result) override;

  void PrintStats() override;

 private:
  const int num_threads_;
  static thread_local int thread_id_;
  ConsistentHash consistent_hash_;
  rte_mempool* tx_mbufpool_;
  rte_mempool* rx_mbufpool_;
  uint8_t port_id_ = 0;
  rte_ether_addr s_eth_addr_;
  rte_ether_addr d_eth_addr_;

  static thread_local rte_be32_t src_ip_;
  static thread_local uint16_t dev_id_;

  std::vector<KVPair> DEFAULT_VALUES = {{"field0", "read"},
                                        {"field1", "read"},
                                        {"field2", "read"},
                                        {"field3", "read"}};

  std::vector<TxConf> tx_cores_;
  std::vector<RxConf> rx_cores_;
  std::vector<TimeoutConf> timeout_cores_;

  struct RxArgs {
    uint16_t queue_id;
    CacheMigrationDpdk* instance;
  };

  std::vector<std::unique_ptr<RxArgs>> rx_args_;
  std::vector<std::unique_ptr<TxConf>> tx_args_;
  std::vector<std::unique_ptr<TimeoutConf>> timeout_args_;

  struct DevInfo {
    rte_be32_t src_ip;
    uint16_t dev_id;
  };

  std::vector<DevInfo> dev_infos_;

  inline void AssignCores();
  int PortInit(uint16_t port);
  inline void LaunchThreads();

  void DoRx(uint16_t queue_id);

  static inline int RxMain(void* arg);
  static inline int TxMain(void* arg);
  static inline int RunTimeoutMonitor(void* arg);

  rte_mbuf* BuildRequestPacket(const std::string& key, uint8_t op,
                               const std::vector<KVPair>& values);
};
}  // namespace ycsbc

#endif  // __CACHE_MIGRATION_DPDK_H__
