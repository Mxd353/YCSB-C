#ifndef __CACHE_MIGRATION_DPDK_H__
#define __CACHE_MIGRATION_DPDK_H__
#include <rte_eal.h>
#include <rte_mbuf.h>

#include <atomic>
#include <future>
#include <memory>
#include <vector>

#include "core/db.h"
#include "lib/c_m_proto.h"
#include "lib/consistent_hash.h"
#include "lib/request_map.h"

#define NUM_MBUFS 16384
#define MBUF_CACHE_SIZE 512
#define RX_RING_SIZE 1024
#define TX_RING_SIZE 4096
#define TX_RING_COUNT 32
#define BURST_SIZE 32

extern std::atomic<bool> running;

struct TxConf {
  uint lcore_id;
  uint16_t queue_id = 0;
  std::vector<rte_ring *> rings{};
};

namespace ycsbc {
class CacheMigrationDpdk : public DB {
 public:
  struct ThreadStats {
    uint64_t total_latency_us = 0;
    uint64_t completed_requests = 0;
    uint64_t iops = 0;
  };

  CacheMigrationDpdk(int num_threads);
  ~CacheMigrationDpdk();
  void Init() override;
  void Close() override;
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
  ConsistentHash consistent_hash_;
  std::vector<std::pair<uint, uint16_t>> rx_cores_;
  std::vector<TxConf> tx_cores_;
  int num_tx_cores_ = 0;
  struct rte_mempool *tx_mbufpool_;
  struct rte_mempool *rx_mbufpool_;
  uint8_t port_id_ = 0;
  struct rte_ether_addr s_eth_addr_;
  struct rte_ether_addr d_eth_addr_;
  std::vector<std::pair<rte_be32_t, uint>> src_ips_;
  uint64_t src_ips_size_ = 0;

  static thread_local rte_be32_t src_ip_;
  static thread_local uint dev_id_;

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

  std::mutex stats_mutex;
  std::vector<ThreadStats> all_thread_stats_;

  struct RxArgs {
    uint16_t queue_id;
    CacheMigrationDpdk *instance;
  };

  std::vector<std::unique_ptr<RxArgs>> rx_args_;
  std::vector<std::unique_ptr<TxConf>> tx_args_;

  struct RequestInfo {
    std::shared_ptr<std::promise<std::vector<KVPair>>> read_promise =
        std::make_shared<std::promise<std::vector<KVPair>>>();
    std::shared_ptr<std::promise<bool>> write_promise =
        std::make_shared<std::promise<bool>>();

    bool completed = false;
  };

  RequestMap<uint32_t, RequestInfo> request_map_;

  inline void AssignCores();
  int PortInit(uint16_t port);
  inline void LaunchThreads();
  struct rte_mbuf *BuildRequestPacket(const std::string &key, uint8_t op,
                                      uint32_t req_id,
                                      const std::vector<KVPair> &values);

  void ProcessReceivedPacket(struct rte_mbuf *mbuf);

  static inline int RxMain(void *arg) {
    RxArgs *args = static_cast<RxArgs *>(arg);
    args->instance->DoRx(args->queue_id);
    return 0;
  }

  void DoRx(uint16_t queue_id);

  static inline int TxMain(void *arg);
};
}  // namespace ycsbc

#endif  // __CACHE_MIGRATION_DPDK_H__
