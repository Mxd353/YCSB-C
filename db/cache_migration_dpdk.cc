#include "cache_migration_dpdk.h"

#include <bitset>
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>

#include "core/utils.h"

extern char *__progname;

static const std::string field_names[4] = {"field0", "field1", "field2",
                                           "field3"};
std::atomic<bool> running{false};

thread_local static int assigned_ring_id = -1;
thread_local static rte_ring *assigned_ring = nullptr;

static inline uint32_t generate_request_id() {
  static std::atomic<uint32_t> counter{0};
  return counter.fetch_add(1, std::memory_order_relaxed);
}

static inline void exponentialBackoff(int attempt) {
  int wait_ms = std::min(500 * (1 << (attempt - 1)), 4000);
  if (attempt == 0) wait_ms = 0;

  std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
}

namespace ycsbc {
thread_local rte_be32_t CacheMigrationDpdk::src_ip_ = 0;
thread_local uint CacheMigrationDpdk::dev_id_ = 0;

CacheMigrationDpdk::CacheMigrationDpdk(int num_threads)
    : num_threads_(num_threads), consistent_hash_("conf/server_ips.conf") {
  std::vector<std::string> args;
  std::string dpdk_conf = "conf/dpdk.conf";
  std::ifstream file(dpdk_conf);
  std::string veth_config = "conf/veths_config.conf";
  std::ifstream veth_file(veth_config);
  std::string token;

  char *program_name = __progname;

  args.push_back(program_name);

  // 动态核数
  //  args.push_back("-l");
  //  args.push_back("0-" + std::to_string(num_threads));

  while (file >> token) {
    args.push_back(token);
  }
  std::vector<char *> argv;
  for (auto &arg : args) {
    argv.push_back(const_cast<char *>(arg.c_str()));
  }

  int ret = rte_eal_init(argv.size(), argv.data());
  if (ret < 0) {
    std::cerr << "DPDK EAL initialization failed\n";
    rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");
    return;
  }

  std::cout << "++++++++db initialization++++++++\n";
  std::string line;
  while (std::getline(veth_file, line)) {
    std::string veth, ip_str;
    u_int dev_id;
    std::istringstream iss(line);
    if (iss >> veth >> ip_str >> dev_id) {
      src_ips_.emplace_back(inet_addr(ip_str.c_str()), dev_id);
      std::cout << "Add ip: " << ip_str << " | dev_id: " << dev_id << "\n";
    } else {
      std::cerr << "Warning: Invalid line format: " << line << std::endl;
    }
  }
  src_ips_size_ = static_cast<uint64_t>(src_ips_.size());

  uint16_t nb_ports = rte_eth_dev_count_avail();
  if (nb_ports < 1) {
    std::cerr << "No available ports\n";
    rte_exit(EXIT_FAILURE, "Error: need at least one port\n");
    return;
  }

  mbuf_pool_ = rte_pktmbuf_pool_create(
      "MBUF_POOL", NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
      RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
  if (mbuf_pool_ == NULL) {
    std::cerr << "Failed to create mbuf pool , Error: "
              << rte_strerror(rte_errno) << std::endl;
    rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");
  }

  AssignCores();

  rte_ether_unformat_addr("00:11:22:33:44:55", &s_eth_addr_);
  rte_ether_unformat_addr("aa:bb:cc:dd:ee:ff", &d_eth_addr_);

  uint16_t port = 0;
  if (PortInit(port) != 0) {
    std::cerr << "Failed to initialize port " << (unsigned)port
              << ", Error: " << rte_strerror(rte_errno) << std::endl;
    rte_exit(EXIT_FAILURE, "Cannot init port %" PRIu16 "\n", port);
  }

  std::cout << "+++++++++++++++++++++++++++++++++\n";
  running = true;
  LaunchThreads();
}

CacheMigrationDpdk::~CacheMigrationDpdk() {
  running = false;
  rte_eal_mp_wait_lcore();

  uint16_t port = 0;
  rte_eth_dev_stop(port);
  rte_eth_dev_close(port);

  for (auto &core : tx_cores_) {
    for (auto &ring : core.rings) {
      if (ring) {
        rte_ring_free(ring);
        ring = nullptr;
      }
    }
    core.rings.clear();
  }

  if (mbuf_pool_) {
    rte_mempool_free(mbuf_pool_);
    mbuf_pool_ = nullptr;
  }

  request_map_.Clear();

  std::cout << "CacheMigrationDpdk resources cleaned up." << std::endl;
};

void CacheMigrationDpdk::Init() {
  static thread_local bool initialized = false;
  if (initialized) return;

  auto selected = src_ips_[rte_rand_max(src_ips_size_)];
  src_ip_ = selected.first;
  dev_id_ = selected.second;

  auto t_id = std::this_thread::get_id();
  std::hash<std::thread::id> hasher;

  int tx_core_index = hasher(t_id) % tx_cores_.size();
  TxConf &core_conf = tx_cores_[tx_core_index];

  assigned_ring_id = hasher(t_id) % core_conf.rings.size();
  assigned_ring = core_conf.rings[assigned_ring_id];

  if (!assigned_ring) {
    std::cerr << "Failed to get ring[" << assigned_ring_id << "] from TX core "
              << tx_core_index << ": " << rte_strerror(rte_errno) << "\n";
    return;
  }

  initialized = true;
}

void CacheMigrationDpdk::Close() {}

inline void CacheMigrationDpdk::AssignCores() {
  uint lcore_id;
  uint count = 0;
  uint core_count = rte_lcore_count();
  RTE_LCORE_FOREACH_WORKER(lcore_id) {
    if (count++ < (core_count - 1) / 2) {
      rx_cores_.push_back(std::make_pair(lcore_id, 0));
    } else {
      tx_cores_.push_back(TxConf{lcore_id});
    }
  }
  num_tx_cores_ = tx_cores_.size();
}

int CacheMigrationDpdk::PortInit(uint16_t port) {
  uint16_t nb_rxd = RX_RING_SIZE;
  uint16_t nb_txd = TX_RING_SIZE;

  uint16_t nb_rx_cores = rx_cores_.size();
  uint16_t nb_tx_cores = tx_cores_.size();

  if (mbuf_pool_ == NULL) {
    printf("mbuf_pool is NULL!\n");
    return -1;
  }

  struct rte_eth_conf port_conf;
  if (!rte_eth_dev_is_valid_port(port)) return -1;
  memset(&port_conf, 0, sizeof(struct rte_eth_conf));

  int retval;
  struct rte_eth_dev_info dev_info;
  retval = rte_eth_dev_info_get(port, &dev_info);
  if (retval != 0) {
    std::cerr << "Error getting device info: " << rte_strerror(retval) << "\n";
    return retval;
  }

  if (dev_info.flow_type_rss_offloads & RTE_ETH_RSS_IP) {
    port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_RSS;
    port_conf.rx_adv_conf.rss_conf.rss_key = nullptr;
    port_conf.rx_adv_conf.rss_conf.rss_hf = RTE_ETH_RSS_IP;
    printf("RSS enabled: RTE_ETH_RSS_IP\n");
  } else {
    port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_NONE;
    printf("WARNING: RSS_IP not supported, falling back to single queue.\n");
  }

  if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
    port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;

  retval = rte_eth_dev_configure(port, nb_rx_cores, nb_tx_cores, &port_conf);
  if (retval != 0) return retval;

  retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
  if (retval != 0) return retval;

  for (uint16_t q = 0; q < nb_rx_cores; ++q) {
    retval = rte_eth_rx_queue_setup(
        port, q, nb_rxd, rte_eth_dev_socket_id(port), nullptr, mbuf_pool_);
    if (retval < 0) return retval;
    rx_cores_[q].second = q;
  }

  struct rte_eth_txconf txconf;
  memset(&txconf, 0, sizeof(txconf));
  for (uint16_t q = 0; q < nb_tx_cores; ++q) {
    retval = rte_eth_tx_queue_setup(port, q, nb_txd,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) return retval;

    tx_cores_[q].queue_id = q;
    const int rings_per_core = (TX_RING_COUNT + nb_tx_cores - 1) / nb_tx_cores;
    for (int i = 0; i < rings_per_core; ++i) {
      std::string ring_name = "tx_ring_p" + std::to_string(getpid()) + "_c" +
                              std::to_string(q) + "_i" + std::to_string(i);
      struct rte_ring *old_ring = rte_ring_lookup(ring_name.c_str());
      if (old_ring) rte_ring_free(old_ring);

      rte_ring *ring =
          rte_ring_create(ring_name.c_str(), TX_RING_SIZE, rte_socket_id(),
                          RING_F_MP_HTS_ENQ | RING_F_SC_DEQ);

      if (ring == nullptr) {
        std::cerr << "Failed to create tx ring: " << ring_name
                  << ", Error: " << rte_strerror(rte_errno) << "\n";
        for (auto &r : tx_cores_[q].rings) rte_ring_free(r);
        return -1;
      }
      tx_cores_[q].rings.push_back(ring);
    }
    std::cout << "Create " << tx_cores_[q].rings.size()
              << " rings for lcore: " << tx_cores_[q].lcore_id << "\n";
  }

  retval = rte_eth_dev_start(port);
  if (retval < 0) return retval;

  printf("Port %u MAC: " RTE_ETHER_ADDR_PRT_FMT "\n", (unsigned)port,
         RTE_ETHER_ADDR_BYTES(&s_eth_addr_));

  retval = rte_eth_promiscuous_enable(port);
  if (retval != 0) return retval;
  return 0;
}

void CacheMigrationDpdk::ProcessReceivedPacket(struct rte_mbuf *mbuf) {
  struct rte_ether_hdr *eth_hdr =
      rte_pktmbuf_mtod(mbuf, struct rte_ether_hdr *);
  if (eth_hdr->ether_type != rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4)) return;

  struct rte_ipv4_hdr *ip_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 1);
  if (ip_hdr->next_proto_id != IP_PROTOCOLS_NETCACHE) return;

  struct KVHeader *kv_header = (struct KVHeader *)(ip_hdr + 1);
  rte_prefetch0(kv_header);

  const uint32_t request_id = rte_be_to_cpu_32(kv_header->request_id);

  const uint8_t is_req = GET_IS_REQ(kv_header->combined);

  if (is_req != CACHE_REPLY && is_req != SERVER_REPLY) {
    // std::cerr << "Server" : "Cache" << "Server rejected request_id = " <<
    // request_id << std::endl;
    return;
  }

  bool exist = request_map_.Modify(request_id, [&](auto &req) {
    if (req->completed) {
      std::cerr << "Duplicate response for request_id = " << request_id
                << std::endl;
      return;
    }
    req->completed = true;
    std::string key_str(kv_header->key.begin(), kv_header->key.end());
    const uint8_t op = GET_OP(kv_header->combined);
    try {
      if (op == READ_REQUEST && req->read_promise) {
        std::vector<KVPair> data;
        data.reserve(4);
        const char *base = kv_header->value1.data();
        for (uint i = 0; i < 4; i++) {
          data.emplace_back(field_names[i],
                            std::string(base + i * VALUE_LENGTH, VALUE_LENGTH));
        }
        req->read_promise->set_value(std::move(data));
      } else if (op == WRITE_REQUEST && req->write_promise) {
        req->write_promise->set_value(true);
      } else {
        throw std::runtime_error("The op and the promise do not match");
      }

      if (ip_hdr->src_addr != consistent_hash_.GetServerIp(key_str)) {
        consistent_hash_.MigrateKey(kv_header->key, ip_hdr->src_addr);
      }
    } catch (const std::future_error &e) {
      std::cerr << "[Packet] Future error: " << e.what() << std::endl;
    }
  });

  if (!exist) {
    // std::cerr << "[Packet] Request not exist id: " << request_id <<
    // std::endl;
  }
}

void CacheMigrationDpdk::DoRx(uint16_t queue_id) {
  uint lcore_id = rte_lcore_id();

  if (lcore_id == RTE_MAX_LCORE || lcore_id == (unsigned)LCORE_ID_ANY) {
    printf("Invalid lcore_id=%u\n", lcore_id);
    return;
  }
  if (rte_lcore_is_enabled(lcore_id) && lcore_id != rte_get_main_lcore()) {
    uint16_t port = 0;
    uint16_t nb_rx;

    if (rte_eth_dev_socket_id(port) >= 0 &&
        rte_eth_dev_socket_id(port) != (int)rte_socket_id())
      printf(
          "WARNING, port %u is on remote NUMA node to "
          "polling thread.\n\tPerformance will "
          "not be optimal.\n",
          port);

    struct rte_mbuf *bufs[BURST_SIZE];

    while (running.load(std::memory_order_acquire)) {
      nb_rx = rte_eth_rx_burst(port, queue_id, bufs, BURST_SIZE);

      if (unlikely(nb_rx == 0)) {
        rte_delay_us(10);
        continue;
      }

      if (nb_rx > 0) {
        for (uint16_t i = 0; i < nb_rx; i++) {
          if (bufs[i] != NULL) {
            ProcessReceivedPacket(bufs[i]);
            rte_pktmbuf_free(bufs[i]);
          }
        }
      }
    }
    return;
  } else {
    printf("Skip main lcore %u\n", lcore_id);
  }
  return;
}

inline int CacheMigrationDpdk::TxMain(void *arg) {
  TxConf *ctx = static_cast<TxConf *>(arg);
  struct rte_mbuf *bufs[BURST_SIZE];
  uint nb_deq;
  uint16_t nb_tx;
  auto rings = ctx->rings;
  for (auto &ring : rings) {
    if (ring == nullptr) {
      std::cerr << "Ring is null in TxMain\n";
      return -1;
    }
  }
  uint16_t queue_id = ctx->queue_id;
  while (running.load(std::memory_order_acquire)) {
    bool got_pkts = false;
    for (auto *ring : rings) {
      nb_deq = rte_ring_dequeue_burst(ring, (void **)bufs, BURST_SIZE, NULL);
      if (nb_deq == 0) {
        continue;
      }
      got_pkts = true;

      nb_tx = rte_eth_tx_burst(0, queue_id, bufs, nb_deq);
      for (uint16_t i = nb_tx; i < nb_deq; i++) {
        rte_pktmbuf_free(bufs[i]);
      }
    }

    if (!got_pkts) {
      rte_pause();
    }
  }
  return 0;
}

inline void CacheMigrationDpdk::LaunchThreads() {
  for (auto &core : rx_cores_) {
    try {
      uint core_id = core.first;
      uint16_t queue_id = core.second;
      auto args = std::make_unique<RxArgs>();
      args->instance = this;
      args->queue_id = queue_id;

      rx_args_.push_back(std::move(args));
      int ret = rte_eal_remote_launch(RxMain, rx_args_.back().get(), core_id);
      if (ret < 0) {
        std::cerr << "Failed to launch RX thread on core " << core_id
                  << ", error: " << rte_strerror(-ret) << std::endl;
        return;
      }
    } catch (const std::exception &e) {
      std::cerr << "Exception launching RX thread: " << e.what() << std::endl;
      return;
    }
  }

  for (auto &core : tx_cores_) {
    uint lcore_id = core.lcore_id;
    uint16_t queue_id = core.queue_id;
    auto rings = core.rings;
    for (auto *ring : rings) {
      if (ring == nullptr) {
        std::cerr << "Ring is null in TxMain\n";
        return;
      }
    }
    auto tx_conf = std::make_unique<TxConf>();
    tx_conf->lcore_id = lcore_id;
    tx_conf->queue_id = queue_id;
    tx_conf->rings = std::move(rings);

    tx_args_.push_back(std::move(tx_conf));
    int ret = rte_eal_remote_launch(TxMain, tx_args_.back().get(), lcore_id);
    if (ret < 0) {
      std::cerr << "Failed to launch TX thread on core " << lcore_id
                << ", error: " << rte_strerror(-ret) << std::endl;
      return;
    }
  }
}

struct rte_mbuf *CacheMigrationDpdk::BuildRequestPacket(
    const std::string &key, uint8_t op, uint32_t req_id,
    const std::vector<KVPair> &values) {
  struct rte_mbuf *mbuf = rte_pktmbuf_alloc(mbuf_pool_);
  if (!mbuf) return nullptr;

  char *pkt_data = rte_pktmbuf_append(mbuf, TOTAL_LEN);
  if (!pkt_data) {
    rte_pktmbuf_free(mbuf);
    std::cerr << "Failed to append packet data\n";
    return nullptr;
  }

  struct rte_ether_hdr *eth_hdr = reinterpret_cast<rte_ether_hdr *>(pkt_data);
  rte_ether_addr_copy(&s_eth_addr_, &eth_hdr->src_addr);
  rte_ether_addr_copy(&d_eth_addr_, &eth_hdr->dst_addr);
  eth_hdr->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

  struct rte_ipv4_hdr *ip_hdr =
      reinterpret_cast<rte_ipv4_hdr *>(pkt_data + RTE_ETHER_HDR_LEN);
  ip_hdr->ihl = 5;
  ip_hdr->version = 4;
  ip_hdr->type_of_service = 0;
  ip_hdr->total_length = rte_cpu_to_be_16(TOTAL_LEN - RTE_ETHER_HDR_LEN);
  ip_hdr->packet_id = rte_cpu_to_be_16(54321);
  ip_hdr->next_proto_id = IP_PROTOCOLS_NETCACHE;
  ip_hdr->time_to_live = 64;
  ip_hdr->src_addr = src_ip_;
  ip_hdr->dst_addr = consistent_hash_.GetServerIp(key);
  ip_hdr->hdr_checksum = 0;
  ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

  struct KVHeader *kv_header =
      reinterpret_cast<KVHeader *>(pkt_data + RTE_ETHER_HDR_LEN + IPV4_HDR_LEN);
  uint16_t combined = ENCODE_COMBINED(dev_id_, op);
  kv_header->request_id = rte_cpu_to_be_32(req_id);
  kv_header->combined = rte_cpu_to_be_16(combined);
  memcpy(kv_header->key.data(), key.data(), KEY_LENGTH);
  memcpy(kv_header->value1.data(), values[0].second.data(), VALUE_LENGTH);
  memcpy(kv_header->value2.data(), values[1].second.data(), VALUE_LENGTH);
  memcpy(kv_header->value3.data(), values[2].second.data(), VALUE_LENGTH);
  memcpy(kv_header->value4.data(), values[3].second.data(), VALUE_LENGTH);
  return mbuf;
}

int CacheMigrationDpdk::Read(const std::string & /*table*/,
                             const std::string &key,
                             const std::vector<std::string> * /*fields*/,
                             std::vector<KVPair> &result) {
  const uint32_t req_id = generate_request_id();

  read_count_.fetch_add(1, std::memory_order_relaxed);

  auto read_promise = std::make_shared<std::promise<std::vector<KVPair>>>();
  auto future = read_promise->get_future();

  auto request = std::make_shared<RequestInfo>();
  request->read_promise = read_promise;

  if (!request_map_.Insert(req_id, request)) {
    no_result_.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "[Read]Error to insert request: " << req_id << std::endl;
    return DB::kErrorConflict;
  }

  RequestCleaner cleaner(request_map_, req_id);

  try {
    for (int attempt = 0; attempt < RETRIES; ++attempt) {
      rte_mbuf *mbuf =
          BuildRequestPacket(key, READ_REQUEST, req_id, DEFAULT_VALUES);
      if (!mbuf) {
        throw std::runtime_error("Failed to build request packet");
      }

      std::unique_ptr<rte_mbuf, decltype(&rte_pktmbuf_free)> mbuf_guard(
          mbuf, rte_pktmbuf_free);
      if (rte_ring_enqueue(assigned_ring, mbuf) < 0) {
        throw std::runtime_error("Failed to enqueue packet");
      }
      mbuf_guard.release();

      if (future.wait_for(std::chrono::milliseconds(100)) ==
          std::future_status::ready) {
        result = future.get();
        read_success_.fetch_add(1, std::memory_order_relaxed);
        return DB::kOK;
      }
      exponentialBackoff(attempt);
    }
    throw std::runtime_error("Max retries exceeded");
  } catch (const std::exception &e) {
    std::cerr << "Error in Read: " << e.what() << ", req_id: " << req_id
              << std::endl;
    no_result_.fetch_add(1, std::memory_order_relaxed);
    return DB::kErrorNoData;
  }
}

int CacheMigrationDpdk::Insert(const std::string & /*table*/,
                               const std::string &key,
                               std::vector<KVPair> &values) {
  const uint32_t req_id = generate_request_id();
  update_count_.fetch_add(1, std::memory_order_relaxed);
  // const rte_be32_t dst_ip = consistent_hash_.GetServerIp(key);

  auto write_promise = std::make_shared<std::promise<bool>>();
  auto future = write_promise->get_future();

  auto request = std::make_shared<RequestInfo>();
  request->write_promise = write_promise;

  if (!request_map_.Insert(req_id, request)) {
    update_failed_.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "[Insert]Error to insert request: " << req_id << std::endl;
    return DB::kErrorConflict;
  }

  RequestCleaner cleaner(request_map_, req_id);

  try {
    for (int attempt = 0; attempt < RETRIES; ++attempt) {
      rte_mbuf *mbuf = BuildRequestPacket(key, WRITE_REQUEST, req_id, values);
      if (!mbuf) {
        throw std::runtime_error("Failed to build request packet");
      }

      std::unique_ptr<rte_mbuf, decltype(&rte_pktmbuf_free)> mbuf_guard(
          mbuf, rte_pktmbuf_free);
      if (rte_ring_enqueue(assigned_ring, mbuf) < 0) {
        throw std::runtime_error("Failed to enqueue packet");
      }
      mbuf_guard.release();

      if (future.wait_for(std::chrono::milliseconds(100)) ==
          std::future_status::ready) {
        if (future.get()) {
          update_success_.fetch_add(1, std::memory_order_relaxed);
          return DB::kOK;
        } else {
          return DB::kErrorRejected;
        }
      }
      exponentialBackoff(attempt);
    }
    throw std::runtime_error("Max retries exceeded");
  } catch (const std::exception &e) {
    std::cerr << "Error in Insert: " << e.what() << ", req_id: " << req_id
              << std::endl;
    update_failed_.fetch_add(1, std::memory_order_relaxed);
    return DB::kErrorNoData;
  }
}

int CacheMigrationDpdk::Update(const std::string &table, const std::string &key,
                               std::vector<KVPair> &values) {
  return Insert(table, key, values);
}

int CacheMigrationDpdk::Scan(const std::string & /*table*/,
                             const std::string & /*key*/, int /*record_count*/,
                             const std::vector<std::string> * /*key*/,
                             std::vector<std::vector<KVPair>> & /*result*/) {
  return DB::kOK;  // Not implemented for now
}

int CacheMigrationDpdk::Delete(const std::string &table,
                               const std::string &key) {
  return Update(table, key, DEFAULT_VALUES);
}

void CacheMigrationDpdk::PrintStats() {
  std::cout << "[Stats] CacheMigrationDpdk Statistics:\n"
            << "  Total Reads: " << read_count_.load() << "\n"
            << "  Successful Reads: " << read_success_.load() << "\n"
            << "  No Result Reads: " << no_result_.load() << "\n"
            << "  Total Updates: " << update_count_.load() << "\n"
            << "  Successful Updates: " << update_success_.load() << "\n"
            << "  Failed Updates: " << update_failed_.load() << "\n";
}

}  // namespace ycsbc
