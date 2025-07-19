#ifndef __CACHE_MIGRATE_PER_CORE_H__
#define __CACHE_MIGRATE_PER_CORE_H__

#include <rte_eal.h>
#include <rte_mbuf.h>

#include <atomic>

#include "core/db.h"
#include "lib/c_m_proto.h"
#include "lib/consistent_hash.h"

#define RX_RING_SIZE 1024
#define TX_RING_SIZE 4096
#define NUM_MBUFS 4096 * 64
#define MBUF_CACHE_SIZE 512
namespace ycsbc {
class CacheMigratePerCore : public DB {
 public:
  CacheMigratePerCore(int num_threads);
  ~CacheMigratePerCore();

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

  struct rte_mempool *tx_mbufpool_;
  struct rte_mempool *rx_mbufpool_;

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

  int PortInit(uint16_t portid, uint16_t n_txring, uint16_t n_rxring);
  void QueueSetup(uint16_t portid, uint16_t queueid);
  int PortStart(uint16_t portid);
  struct rte_mbuf *BuildRequestPacket(const std::string &key, uint8_t op,
                                      uint32_t req_id,
                                      const std::vector<KVPair> &values);
};
}  // namespace ycsbc

#endif  // __CACHE_MIGRATE_PER_CORE_H__
