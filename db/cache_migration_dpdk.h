#ifndef __CACHE_MIGRATION_DPDK_H__
#define __CACHE_MIGRATION_DPDK_H__
#include <rte_eal.h>
#include <rte_mbuf.h>

#include <atomic>
#include <future>
#include <memory>
#include <vector>

#include "core/db.h"
#include "core/properties.h"
#include "lib/c_m_proto.h"
#include "lib/consistent_hash.h"
#include "lib/request_map.h"

#define TX_NUM_MBUFS 1000000
#define RX_NUM_MBUFS 262144
#define MBUF_CACHE_SIZE 512
#define TX_MBUF_DATA_SIZE 256
#define RX_MBUF_DATA_SIZE 2048
#define RX_RING_SIZE 8192
#define TX_RING_SIZE 4096
#define TX_RING_COUNT 32
#define BURST_SIZE 32

extern std::atomic<bool> running;

struct RequestInfo {
  rte_mbuf *mbuf = nullptr;
  uint64_t start_time = 0;
  uint64_t completed_time = 0;
  int retry_count = 0;
  uint8_t op = c_m_proto::NO_REQUEST;

  std::atomic<bool> completed;

  RequestInfo() : completed(false) {}

  RequestInfo(RequestInfo &&other) noexcept
      : mbuf(other.mbuf),
        start_time(other.start_time),
        completed_time(other.completed_time),
        retry_count(other.retry_count),
        op(other.op),
        completed(other.completed.load(std::memory_order_relaxed)) {
    other.mbuf = nullptr;
    other.start_time = 0;
    other.completed_time = 0;
    other.retry_count = 0;
    other.op = c_m_proto::NO_REQUEST;
    other.completed.store(false, std::memory_order_relaxed);
  }

  RequestInfo &operator=(RequestInfo &&other) noexcept {
    if (this != &other) {
      mbuf = other.mbuf;
      start_time = other.start_time;
      completed_time = other.completed_time;
      retry_count = other.retry_count;
      op = other.op;
      completed.store(other.completed.load(std::memory_order_relaxed));

      other.mbuf = nullptr;
      other.start_time = 0;
      other.completed_time = 0;
      other.retry_count = 0;
      other.op = c_m_proto::NO_REQUEST;
      other.completed.store(false, std::memory_order_relaxed);
    }
    return *this;
  }

  void clear() {
    if (mbuf) {
      rte_pktmbuf_free(mbuf);
      mbuf = nullptr;
    }
    start_time = 0;
    completed_time = 0;
    retry_count = 0;
    op = c_m_proto::NO_REQUEST;
    completed.store(false, std::memory_order_relaxed);
  }

  RequestInfo(const RequestInfo &) = delete;
  RequestInfo &operator=(const RequestInfo &) = delete;
};

struct TxConf {
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

  CacheMigrationDpdk(utils::Properties &props);
  ~CacheMigrationDpdk();
  void AllocateSpace(size_t total_ops) override;
  void Init(const int thread_id) override;
  void Close() override;
  void StartDpdk();
  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) override;
  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) override;
  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) override;
  int Delete(const std::string &table, const std::string &key) override;
  int Scan(const std::string &table, const std::string &key, int record_count,
           const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) override;

  void PrintStats() override;

 private:
  const int num_threads_;
  static thread_local int thread_id_;
  ConsistentHash consistent_hash_;
  std::vector<std::pair<uint, uint16_t>> rx_cores_;
  std::vector<TxConf> tx_cores_;
  size_t num_tx_cores_ = 0;
  rte_mempool *tx_mbufpool_;
  rte_mempool *rx_mbufpool_;
  uint8_t port_id_ = 0;
  rte_ether_addr s_eth_addr_;
  rte_ether_addr d_eth_addr_;
  std::vector<std::pair<rte_be32_t, uint>> src_ips_;
  uint64_t src_ips_size_ = 0;

  static thread_local rte_be32_t src_ip_;
  static thread_local uint dev_id_;

  std::thread timeout_thread_;
  uint timeout_core_ = UINT_MAX;
  uint16_t timeout_queue_ = 0;

  std::atomic<size_t> read_count_{0};
  std::atomic<size_t> read_success_{0};
  std::atomic<size_t> update_count_{0};
  std::atomic<size_t> update_success_{0};
  std::atomic<size_t> no_result_{0};
  std::atomic<size_t> update_failed_{0};

  std::vector<KVPair> DEFAULT_VALUES = {{"field0", "read"},
                                        {"field1", "read"},
                                        {"field2", "read"},
                                        {"field3", "read"}};

  struct RxArgs {
    uint16_t queue_id;
    CacheMigrationDpdk *instance;
  };

  std::vector<std::unique_ptr<RxArgs>> rx_args_;
  std::vector<std::unique_ptr<TxConf>> tx_args_;

  inline void AssignCores();
  int PortInit(uint16_t port);
  inline void LaunchThreads();
  rte_mbuf *BuildRequestPacket(const std::string &key, uint8_t op,
                               uint32_t req_id,
                               const std::vector<KVPair> &values);

  // inline void ProcessReceivedPacket(rte_mbuf *mbuf);
  // void RunTimeoutMonitor();
  static inline int RunTimeoutMonitor(void *arg);
  void DoRx(uint16_t queue_id);

  static inline int RxMain(void *arg);
  static inline int TxMain(void *arg);
  // static inline int TimeoutMonitorThread(void *arg);
};
}  // namespace ycsbc

#endif  // __CACHE_MIGRATION_DPDK_H__
