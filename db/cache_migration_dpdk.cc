#include "cache_migration_dpdk.h"

#include <algorithm>
#include <bitset>
#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>

#include "core/timer.h"
#include "core/utils.h"

extern char *__progname;

static const std::string field_names[4] = {"field0", "field1", "field2",
                                           "field3"};

static std::vector<std::vector<rte_mbuf *>> packets;
uint64_t send_start_us = 0;
std::atomic<bool> running{false};

static inline void exponentialBackoff(int attempt) {
  int wait_ms = std::min(20 * (1 << (attempt - 1)), 500);
  std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
}

namespace ycsbc {
thread_local rte_be32_t CacheMigrationDpdk::src_ip_ = 0;
thread_local uint CacheMigrationDpdk::dev_id_ = 0;
thread_local int CacheMigrationDpdk::thread_id_ = 0;
thread_local CacheMigrationDpdk::ThreadStats thread_stats;

CacheMigrationDpdk::CacheMigrationDpdk(int num_threads)
    : num_threads_(num_threads), consistent_hash_("conf/server_ips.conf") {
  std::vector<std::string> args;
  std::string dpdk_conf = "conf/dpdk.conf";
  std::ifstream dpdk_file(dpdk_conf);
  std::string client_conf = "conf/client_config.conf";
  std::ifstream client_file(client_conf);
  std::string token;

  all_thread_stats_.reserve(num_threads);
  packets.reserve(num_threads);

  char *program_name = __progname;
  args.push_back(program_name);

  // 动态核数
  //  args.push_back("-l");
  //  args.push_back("0-" + std::to_string(num_threads));

  while (dpdk_file >> token) {
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
  while (std::getline(client_file, line)) {
    std::string ip_str;
    u_int dev_id;
    std::istringstream iss(line);
    if (iss >> ip_str >> dev_id) {
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

  tx_mbufpool_ = rte_pktmbuf_pool_create("TX_MBUF_POOL", NUM_MBUFS,
                                         MBUF_CACHE_SIZE, RTE_MBUF_PRIV_ALIGN,
                                         MBUF_DATA_SIZE, rte_socket_id());
  if (tx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "Cannot create TX_MBUF_POOL\n");

  rx_mbufpool_ = rte_pktmbuf_pool_create("RX_MBUF_POOL", NUM_MBUFS,
                                         MBUF_CACHE_SIZE, RTE_MBUF_PRIV_ALIGN,
                                         MBUF_DATA_SIZE, rte_socket_id());
  if (rx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "Cannot create RX_MBUF_POOL\n");

  AssignCores();

  rte_ether_unformat_addr("00:11:22:33:44:55", &s_eth_addr_);
  rte_ether_unformat_addr("aa:bb:cc:dd:ee:ff", &d_eth_addr_);

  std::cout << "+++++++++++++++++++++++++++++++++\n";
}

CacheMigrationDpdk::~CacheMigrationDpdk() {
  running = false;
  rte_eal_mp_wait_lcore();

  uint16_t port = 0;
  rte_eth_dev_stop(port);
  rte_eth_dev_close(port);

  if (tx_mbufpool_) {
    rte_mempool_free(tx_mbufpool_);
    tx_mbufpool_ = nullptr;
  }
  if (rx_mbufpool_) {
    rte_mempool_free(rx_mbufpool_);
    rx_mbufpool_ = nullptr;
  }
  request_map_.Clear();

  std::cout << "CacheMigrationDpdk resources cleaned up." << std::endl;
}

void CacheMigrationDpdk::Init(const int thread_id, const int num_ops) {
  static thread_local bool initialized = false;
  if (initialized) return;
  thread_id_ = thread_id;
  auto &pkts = packets[thread_id_];
  pkts.reserve(num_ops);

  total_request_count_.fetch_add(num_ops, std::memory_order_relaxed);

  auto selected = src_ips_[rte_rand_max(src_ips_size_)];
  src_ip_ = selected.first;
  dev_id_ = selected.second;

  initialized = true;
}

void CacheMigrationDpdk::Close() {}

void CacheMigrationDpdk::StartDpdk() {
  std::cout << "start DPDK.." << std::endl;
  running = true;
  uint16_t port = 0;

  if (PortInit(port) != 0) {
    std::cerr << "Failed to initialize port " << (unsigned)port
              << ", Error: " << rte_strerror(rte_errno) << std::endl;
    rte_exit(EXIT_FAILURE, "Cannot init port %" PRIu16 "\n", port);
  }

  if (timeout_core_ == UINT_MAX)
    timeout_thread_ =
        std::thread(&CacheMigrationDpdk::TimeoutMonitorThread, this);

  LaunchThreads();

  while (completed_count_ + timeout_count_ < total_request_count_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

inline void CacheMigrationDpdk::AssignCores() {
  uint lcore_id;
  std::vector<uint> workers;

  RTE_LCORE_FOREACH_WORKER(lcore_id) { workers.push_back(lcore_id); }

  size_t total_workers = workers.size();
  if (total_workers < 3) {
    rte_exit(EXIT_FAILURE,
             "Need at least 3 worker cores (got %zu): RX, TX, and "
             "TimeoutMonitorThread\n",
             total_workers);
  }

  rx_cores_.clear();
  tx_cores_.clear();

  bool odd = (total_workers % 2 == 1);
  timeout_core_ = odd ? workers.back() : UINT_MAX;

  size_t usable_cores = odd ? total_workers - 1 : total_workers;
  size_t rx_count = (usable_cores * 2) / 3;

  for (size_t i = 0; i < rx_count; ++i) {
    rx_cores_.push_back(std::make_pair(workers[i], 0));
  }

  for (size_t i = rx_count; i < usable_cores; ++i) {
    tx_cores_.push_back(TxConf{workers[i]});
  }

  num_tx_cores_ = tx_cores_.size();
  printf("Assigned %zu RX cores, %zu TX cores\n", rx_cores_.size(),
         tx_cores_.size());
  if (timeout_core_ == UINT_MAX) {
    printf("No dedicated timeout core assigned (timeout_core_ = UINT_MAX)\n");
  } else {
    printf("Timeout core assigned: %u\n", timeout_core_);
  }
}

int CacheMigrationDpdk::PortInit(uint16_t port) {
  uint16_t nb_rxd = RX_RING_SIZE;
  uint16_t nb_txd = TX_RING_SIZE;

  uint16_t nb_rx_cores = rx_cores_.size();
  uint16_t nb_tx_cores = tx_cores_.size();

  if (tx_mbufpool_ == NULL || rx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "mbuf_pool is NULL!\n");

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

  uint16_t max_supported_tx_queues = dev_info.max_tx_queues;

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
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot configure port\n");

  retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot adjust desc number\n");
  printf("Adjusted nb_rxd = %u, nb_txd = %u\n", nb_rxd, nb_txd);

  for (uint16_t q = 0; q < nb_rx_cores; ++q) {
    retval = rte_eth_rx_queue_setup(
        port, q, nb_rxd, rte_eth_dev_socket_id(port), nullptr, rx_mbufpool_);
    if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup RX queue\n");
    rx_cores_[q].second = q;
  }

  struct rte_eth_txconf txconf;
  memset(&txconf, 0, sizeof(txconf));
  uint16_t q = 0;
  for (q = 0; q < nb_tx_cores; ++q) {
    retval = rte_eth_tx_queue_setup(port, q, nb_txd,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup TX queue\n");

    tx_cores_[q].queue_id = q;
  }

  if ((nb_tx_cores + (timeout_core_ != UINT_MAX ? 1 : 0)) >
      max_supported_tx_queues) {
    rte_exit(EXIT_FAILURE, "Too many TX queues requested\n");
  }

  if (timeout_core_ != UINT_MAX) {
    uint16_t timeout_q = q++;
    retval = rte_eth_tx_queue_setup(port, timeout_q, nb_txd,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup timeout TX queue\n");
    timeout_queue_ = timeout_q;
  }

  retval = rte_eth_promiscuous_enable(port);
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot set promiscuous\n");

  retval = rte_eth_dev_start(port);
  if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot start port\n");

  printf("Port %u MAC: " RTE_ETHER_ADDR_PRT_FMT "\n", (unsigned)port,
         RTE_ETHER_ADDR_BYTES(&s_eth_addr_));

  return 0;
}

void CacheMigrationDpdk::ProcessReceivedPacket(struct rte_mbuf *mbuf) {
  uint64_t recv_tsc = get_now_micros();
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
    completed_count_.fetch_add(1, std::memory_order_relaxed);
    req->completed = true;
    req->completed_us = recv_tsc;
    std::string key_str(kv_header->key.begin(), kv_header->key.end());
    const uint8_t op = GET_OP(kv_header->combined);
    try {
      if (op == READ_REQUEST) {
        read_success_.fetch_add(1, std::memory_order_relaxed);
      } else if (op == WRITE_REQUEST) {
        update_success_.fetch_add(1, std::memory_order_relaxed);
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
  if (exist) {
    request_map_.Erase(request_id);
  }
}

void CacheMigrationDpdk::TimeoutMonitorThread() {
  const uint64_t timeout_us = 20000;

  while (running) {
    uint64_t now = get_now_micros();
    std::vector<uint32_t> expired_requests;

    request_map_.ForEach([&](uint32_t req_id, const RequestInfo &req) {
      if (!req.completed && now - req.start_us > timeout_us) {
        expired_requests.push_back(req_id);
      }
    });

    for (auto req_id : expired_requests) {
      request_map_.Modify(req_id, [&](std::shared_ptr<RequestInfo> &req) {
        if (req->completed) return;

        if (req->retry_count >= RETRIES) {
          timeout_count_.fetch_add(1, std::memory_order_relaxed);
          req->completed = true;
          request_map_.Erase(req_id);
          std::cerr << "Request timeout: " << req_id << std::endl;
          if (req->mbuf) {
            rte_pktmbuf_free(req->mbuf);
            req->mbuf = nullptr;
          }
        } else {
          int sent = rte_eth_tx_burst(0, 0, &req->mbuf, 1);
          if (sent == 1) {
            req->retry_count++;
            req->start_us = now;
          }
        }
      });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
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
  struct rte_mbuf *burst[BURST_SIZE];

  auto &pkts = packets[ctx->packets_index];

  uint16_t queue_id = ctx->queue_id;
  size_t sent_count = 0;
  size_t total_pkts = pkts.size();

  send_start_us = get_now_micros();

  for (size_t i = 0; i < total_pkts; i += BURST_SIZE) {
    size_t this_burst =
        std::min(static_cast<unsigned long>(BURST_SIZE), total_pkts - i);

    std::memcpy(burst, &pkts[i], this_burst * sizeof(rte_mbuf *));

    uint16_t nb_tx = rte_eth_tx_burst(0, queue_id, burst, this_burst);
    for (uint16_t i = nb_tx; i < this_burst; i++) {
      rte_pktmbuf_free(burst[i]);
    }
    sent_count += this_burst;
  }
  pkts.erase(pkts.begin(), pkts.begin() + sent_count);
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

  int packets_index = 0;
  for (auto &core : tx_cores_) {
    uint lcore_id = core.lcore_id;
    uint16_t queue_id = core.queue_id;

    auto tx_conf = std::make_unique<TxConf>();
    tx_conf->lcore_id = lcore_id;
    tx_conf->queue_id = queue_id;
    tx_conf->packets_index = packets_index++;

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
  struct rte_mbuf *mbuf = rte_pktmbuf_alloc(tx_mbufpool_);
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
                             std::vector<KVPair> & /*result*/) {
  const uint32_t req_id = utils::generate_request_id();
  auto &pkts = packets[thread_id_];

  read_count_.fetch_add(1, std::memory_order_relaxed);

  rte_mbuf *mbuf =
      BuildRequestPacket(key, READ_REQUEST, req_id, DEFAULT_VALUES);
  if (!mbuf) {
    no_result_.fetch_add(1, std::memory_order_relaxed);
    return DB::kErrorNoData;
  }

  std::unique_ptr<rte_mbuf, decltype(&rte_pktmbuf_free)> mbuf_guard(
      mbuf, rte_pktmbuf_free);
  pkts.push_back(mbuf);
  mbuf_guard.release();

  auto request = std::make_shared<RequestInfo>();
  request->start_us = get_now_micros();
  request->mbuf = mbuf;

  if (!request_map_.Insert(req_id, request)) {
    no_result_.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "[Read] Error to insert request: " << req_id << std::endl;
    return DB::kErrorConflict;
  }
  return DB::kOK;
}

int CacheMigrationDpdk::Insert(const std::string & /*table*/,
                               const std::string &key,
                               std::vector<KVPair> &values) {
  const uint32_t req_id = utils::generate_request_id();
  auto &pkts = packets[thread_id_];

  update_count_.fetch_add(1, std::memory_order_relaxed);

  rte_mbuf *mbuf = BuildRequestPacket(key, WRITE_REQUEST, req_id, values);
  if (!mbuf) {
    throw std::runtime_error("Failed to build request packet");
  }

  std::unique_ptr<rte_mbuf, decltype(&rte_pktmbuf_free)> mbuf_guard(
      mbuf, rte_pktmbuf_free);
  pkts.push_back(mbuf);
  mbuf_guard.release();

  auto request = std::make_shared<RequestInfo>();
  request->start_us = get_now_micros();
  request->mbuf = mbuf;

  if (!request_map_.Insert(req_id, request)) {
    update_failed_.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "[Insert] Error to insert request: " << req_id << std::endl;
    return DB::kErrorConflict;
  }
  return DB::kOK;
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
  std::vector<double> iopss(all_thread_stats_.size(), 0.0);
  std::vector<double> latency_uss(all_thread_stats_.size(), 0.0);
  double total_iops = 0.0;
  uint64_t total_completed = 0;
  uint64_t max_thread_latency_us = 0;
  uint64_t total_latency_us_all_threads = 0;

  for (size_t i = 0; i < all_thread_stats_.size(); ++i) {
    const auto &s = all_thread_stats_[i];
    double iops = s.total_latency_us > 0
                      ? s.completed_requests * 1'000'000.0 / s.total_latency_us
                      : 0.0;
    double latency_us =
        s.completed_requests > 0
            ? static_cast<double>(s.total_latency_us) / s.completed_requests
            : 0.0;

    iopss[i] = iops;
    latency_uss[i] = latency_us;

    std::cout << " Thread[ " << i << " ]\n"
              << "total_latency_us = " << s.total_latency_us << "\n"
              << "completed_requests = " << s.completed_requests << "\n"
              << "iops = " << iops << "\n";

    total_iops += iops;

    total_completed += s.completed_requests;
    total_latency_us_all_threads += s.total_latency_us;

    if (s.total_latency_us > max_thread_latency_us) {
      max_thread_latency_us = s.total_latency_us;
    }
  }

  double conservative_total_iops =
      max_thread_latency_us > 0
          ? total_completed / (max_thread_latency_us / 1'000'000.0)
          : 0.0;

  double average_latency_us =
      total_completed > 0
          ? static_cast<double>(max_thread_latency_us) / total_completed
          : 0.0;

  all_thread_stats_.clear();

  std::cout << "[Stats] CacheMigrationDpdk Statistics:\n"
            << "Per-thread IOPS sum: " << total_iops << " ops/sec\n"
            << "Total completed requests: " << total_completed << "\n"
            << "Max thread run time: " << max_thread_latency_us / 1e6 << " s\n"
            << "System IOPS (total_requests / max_thread_time): "
            << conservative_total_iops << " ops/sec\n"
            << "Average latency per request: " << average_latency_us << " us\n"
            << "  Total Reads: " << read_count_.load() << "\n"
            << "  Successful Reads: " << read_success_.load() << "\n"
            << "  No Result Reads: " << no_result_.load() << "\n"
            << "  Total Updates: " << update_count_.load() << "\n"
            << "  Successful Updates: " << update_success_.load() << "\n"
            << "  Failed Updates: " << update_failed_.load() << "\n";
}

}  // namespace ycsbc
