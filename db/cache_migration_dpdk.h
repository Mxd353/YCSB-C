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

// High performance pipeline mode: build packets on-the-fly
// Batch size tuned for 1M QPS target
#define TX_NUM_MBUFS 4'194'303
#define RX_NUM_MBUFS 262'143
#define MBUF_CACHE_SIZE 512
#define TX_MBUF_DATA_SIZE 256
#define RX_MBUF_DATA_SIZE 512
#define RX_RING_SIZE 8192
#define TX_RING_SIZE 4096
#define TX_RING_COUNT 32
#define BURST_SIZE 32

// Batch size for pipeline: alloc -> build -> send
// Larger batch = better throughput, but higher latency
// 64 is optimal for 10G/25G networks
#define PIPELINE_BATCH_SIZE 64

// Total requests to send (100M for 1MQPS sustained test)
#define REQ_SIZE 100'000'000

#define RTE_LOGTYPE_CORE RTE_LOGTYPE_USER3
#define RTE_LOGTYPE_PACKET RTE_LOGTYPE_USER4

extern std::atomic<bool> running;

struct RequestInfo {
  uint64_t start_time = 0;

  std::atomic<int> retry_count;
  std::atomic<bool> completed;
  std::atomic<bool> time_out;

  RequestInfo() : retry_count(0), completed(false), time_out(false) {}

  RequestInfo(RequestInfo&& other) noexcept
      : start_time(other.start_time),
        retry_count(other.retry_count.load(std::memory_order_relaxed)),
        completed(other.completed.load(std::memory_order_relaxed)),
        time_out(other.time_out.load(std::memory_order_relaxed)) {
    other.start_time = 0;

    other.retry_count.store(0, std::memory_order_relaxed);
    other.completed.store(false, std::memory_order_relaxed);
    other.time_out.store(false, std::memory_order_relaxed);
  }

  RequestInfo& operator=(RequestInfo&& other) noexcept {
    if (this != &other) {
      start_time = other.start_time;

      retry_count.store(other.retry_count.load(std::memory_order_relaxed));
      completed.store(other.completed.load(std::memory_order_relaxed));
      time_out.store(other.time_out.load(std::memory_order_relaxed));

      other.start_time = 0;

      other.retry_count.store(0, std::memory_order_relaxed);
      other.completed.store(false, std::memory_order_relaxed);
      other.time_out.store(false, std::memory_order_relaxed);
    }
    return *this;
  }

  void clear() {
    start_time = 0;

    retry_count.store(0, std::memory_order_relaxed);
    completed.store(false, std::memory_order_relaxed);
  }

  RequestInfo(const RequestInfo&) = delete;
  RequestInfo& operator=(const RequestInfo&) = delete;
};

// Forward declaration
namespace ycsbc {
  class CacheMigrationDpdk;
}

struct TxConf {
  uint lcore_id;
  uint16_t queue_id = 0;
  std::pair<size_t, size_t> interval{0, 0};
  ycsbc::CacheMigrationDpdk* instance = nullptr;  // For pipeline mode
};

struct RxConf {
  uint lcore_id;
  uint16_t queue_id = 0;
};

// Pre-generated key template for fast packet construction
struct KeyTemplate {
  char data[c_m_proto::KEY_LENGTH];
  rte_be32_t dst_ip;  // Pre-computed destination IP
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
  
  // Configuration: use high-performance pipeline mode
  // Pipeline mode: build packets on-the-fly, no pre-built templates
  // Traditional mode: pre-build packet templates and reuse
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

  // High performance pipeline: build and send packets on-the-fly
  // No pre-built packet templates - construct in pipeline

  // Friend declaration for wrapper function
  friend int TxMainPipelineWrapper(void* arg);

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

  struct RxArgs {
    uint16_t queue_id;
    CacheMigrationDpdk* instance;
  };

  std::vector<std::unique_ptr<RxArgs>> rx_args_;
  std::vector<std::unique_ptr<TxConf>> tx_args_;

  struct DevInfo {
    rte_be32_t src_ip;
    uint16_t dev_id;
  };

  std::vector<DevInfo> dev_infos_;

  // Pre-generated key templates for fast packet construction
  std::vector<KeyTemplate> key_templates_;
  size_t num_key_templates_ = 0;
  
  // Configuration
  bool use_pipeline_ = false;  // Use high-performance pipeline mode

  inline void AssignCores();
  int PortInit(uint16_t port);
  inline void LaunchThreads();

  // Build single packet (for load phase)
  rte_mbuf* BuildRequestPacket(const std::string& key, uint8_t op,
                               uint32_t req_id,
                               const std::vector<KVPair>& values);

  // High performance: build packet using pre-generated template
  void BuildPacketFromTemplate(rte_mbuf* mbuf, const KeyTemplate& key_template,
                               uint8_t op, uint32_t req_id);

  // Batch allocate and build packets
  uint16_t BuildAndSendBatch(uint16_t queue_id, size_t start_req_id,
                             uint16_t batch_size);

  static inline int RunTimeoutMonitor(void* arg);
  void DoRx(uint16_t queue_id);

  static inline int RxMain(void* arg);
  static inline int TxMain(void* arg);

  // Optimized TxMain for pipeline mode
  // Static member function with instance pointer in TxConf
  static inline int TxMainPipeline(void* arg);
};
}  // namespace ycsbc

#endif  // __CACHE_MIGRATION_DPDK_H__
