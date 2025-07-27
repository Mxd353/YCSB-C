#include "cache_migration_dpdk.h"

#include <algorithm>
#include <bitset>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>

#include "core/core_workload.h"
#include "core/timer.h"
#include "core/utils.h"

extern char *__progname;

static std::vector<RequestInfo> requests;

std::atomic<uint32_t> utils::RequestIDGenerator::counter{0};

uint64_t send_start_us = 0;
uint64_t use_time_us = 0;
std::atomic<bool> running{false};

std::atomic<uint64_t> total_latency_us{0};
std::atomic<size_t> total_request_count{0};
std::atomic<size_t> completed_count{0};
std::atomic<size_t> timeout_count{0};
std::atomic<size_t> timeout_send{0};
std::atomic<size_t> false_count{0};

static inline void exponentialBackoff(int attempt) {
  int wait_ms = std::min(20 * (1 << (attempt - 1)), 500);
  std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
}

static inline uint64_t time_out_us(int retry_count) {
  constexpr uint64_t BASE_TIMEOUT_US = 2'000'000;
  constexpr uint64_t MAX_TIMEOUT_US = 50'000'000;

  if (retry_count <= 0) {
    return BASE_TIMEOUT_US;
  }

  uint64_t exponential_backoff = BASE_TIMEOUT_US * (1ULL << (retry_count - 1));
  return std::min(exponential_backoff, MAX_TIMEOUT_US);
}

namespace ycsbc {
thread_local rte_be32_t CacheMigrationDpdk::src_ip_ = 0;
thread_local uint CacheMigrationDpdk::dev_id_ = 0;
thread_local int CacheMigrationDpdk::thread_id_ = 0;
thread_local CacheMigrationDpdk::ThreadStats thread_stats;

CacheMigrationDpdk::CacheMigrationDpdk(utils::Properties &props)
    : num_threads_(std::stoi(props.GetProperty("threadcount", "1"))),
      consistent_hash_("conf/server_ips.conf") {
  std::vector<std::string> args;
  std::string dpdk_conf = "conf/dpdk.conf";
  std::ifstream dpdk_file(dpdk_conf);
  std::string client_conf = "conf/client_config.conf";
  std::ifstream client_file(client_conf);
  std::string token;

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

  tx_mbufpool_ =
      rte_pktmbuf_pool_create("TX_MBUF_POOL", TX_NUM_MBUFS, MBUF_CACHE_SIZE, 0,
                              TX_MBUF_DATA_SIZE, rte_socket_id());
  if (tx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "Cannot create TX_MBUF_POOL\n");

  rx_mbufpool_ =
      rte_pktmbuf_pool_create("RX_MBUF_POOL", RX_NUM_MBUFS, MBUF_CACHE_SIZE, 0,
                              RX_MBUF_DATA_SIZE, rte_socket_id());
  if (rx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "Cannot create RX_MBUF_POOL\n");

  AssignCores();

  uint16_t port = 0;
  if (PortInit(port) != 0) {
    std::cerr << "Failed to initialize port " << (unsigned)port
              << ", Error: " << rte_strerror(rte_errno) << std::endl;
    rte_exit(EXIT_FAILURE, "Cannot init port %" PRIu16 "\n", port);
  }

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
  requests.clear();

  std::cout << "CacheMigrationDpdk resources cleaned up." << std::endl;
}

void CacheMigrationDpdk::AllocateSpace(size_t total_ops) {
  if (total_ops == 0) {
    throw std::invalid_argument("total_ops must be greater than 0");
  }

  total_latency_us.store(0, std::memory_order_relaxed);
  completed_count.store(0, std::memory_order_relaxed);
  timeout_count.store(0, std::memory_order_relaxed);
  timeout_send.store(0, std::memory_order_relaxed);
  false_count.store(0, std::memory_order_relaxed);
  total_request_count.store(total_ops, std::memory_order_relaxed);

  send_start_us = 0;
  use_time_us = 0;

  requests.clear();
  requests.resize(total_request_count);
}

void CacheMigrationDpdk::Init(const int thread_id) {
  static thread_local bool initialized = false;
  if (initialized) return;
  thread_id_ = thread_id;

  auto selected = src_ips_[rte_rand_max(src_ips_size_)];
  src_ip_ = selected.first;
  dev_id_ = selected.second;

  initialized = true;
}

void CacheMigrationDpdk::Close() {}

void CacheMigrationDpdk::StartDpdk() {
  for (size_t i = 0; i < requests.size(); ++i) {
    RequestInfo &req = requests[i];

    if (req.completed.load(std::memory_order_acquire) || req.mbuf == nullptr) {
      rte_exit(EXIT_FAILURE,
               "Request %zu is not completed and mbuf is nullptr!", i);
    }
  }

  std::cerr << "All " << requests.size() << " requests valid." << std::endl;

  running = true;

  // if (timeout_core_ == UINT_MAX)
  //   rte_exit(EXIT_FAILURE,
  //            "No dedicated timeout core assigned (timeout_core_ =
  //            UINT_MAX)");

  std::cerr << "start DPDK.." << std::endl;
  LaunchThreads();

  while (completed_count + timeout_count + false_count < total_request_count) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  use_time_us = get_now_micros() - send_start_us;
  running = false;

  // if (timeout_thread_.joinable()) {
  //   timeout_thread_.join();
  // }

  // if (timeout_core_ != UINT_MAX) {
  //   rte_eal_wait_lcore(timeout_core_);
  // }

  std::cout << "All requests completed or timed out." << std::endl;
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

  size_t rx_count = (total_workers * 2) / 3;
  size_t tx_count = total_workers - rx_count;

  for (size_t i = 0; i < rx_count; ++i) {
    rx_cores_.push_back(std::make_pair(workers[i], 0));
  }

  for (size_t i = rx_count; i < rx_count + tx_count; ++i) {
    tx_cores_.push_back(TxConf{workers[i]});
  }

  num_tx_cores_ = tx_cores_.size();
  printf("Assigned %zu RX cores, %zu TX cores\n", rx_cores_.size(),
         tx_cores_.size());

  // timeout_core_ = rx_count + tx_count + 1;
  // if (timeout_core_ == UINT_MAX) {
  //   printf("No dedicated timeout core assigned (timeout_core_ =
  //   UINT_MAX)\n");
  // } else {
  //   printf("Timeout core assigned: %u\n", timeout_core_);
  // }
}

int CacheMigrationDpdk::PortInit(uint16_t port) {
  uint16_t nb_rxd = RX_RING_SIZE;
  uint16_t nb_txd = TX_RING_SIZE;

  uint16_t nb_rx_cores = rx_cores_.size();
  uint16_t nb_tx_cores = tx_cores_.size();

  if (tx_mbufpool_ == NULL || rx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "mbuf_pool is NULL!\n");

  rte_eth_conf port_conf;
  if (!rte_eth_dev_is_valid_port(port)) return -1;
  memset(&port_conf, 0, sizeof(rte_eth_conf));

  int retval;
  rte_eth_dev_info dev_info;
  retval = rte_eth_dev_info_get(port, &dev_info);
  if (retval != 0) {
    std::cerr << "Error getting device info: " << rte_strerror(retval) << "\n";
    return retval;
  }

  uint16_t max_supported_tx_queues = dev_info.max_tx_queues;
  printf("max_supported_tx_queues: %u\n", max_supported_tx_queues);

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

  retval =
      rte_eth_dev_configure(port, nb_rx_cores, nb_tx_cores + 1, &port_conf);
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

  rte_eth_txconf txconf;
  memset(&txconf, 0, sizeof(txconf));
  for (uint16_t q = 0; q < nb_tx_cores; ++q) {
    retval = rte_eth_tx_queue_setup(port, q, nb_txd,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup TX queue\n");

    tx_cores_[q].queue_id = q;
  }

  // uint16_t timeout_q = nb_tx_cores + (timeout_core_ != UINT_MAX ? 1 : 0) - 1;
  // if (timeout_q > max_supported_tx_queues) {
  //   rte_exit(EXIT_FAILURE, "Too many TX queues requested\n");
  // }

  // if (timeout_core_ != UINT_MAX) {
  //   retval = rte_eth_tx_queue_setup(port, timeout_q, nb_txd,
  //                                   rte_eth_dev_socket_id(port), &txconf);
  //   if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup timeout TX
  //   queue\n");
  //   // timeout_queue_ = nb_tx_cores - 1;
  // }

  retval = rte_eth_promiscuous_enable(port);
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot set promiscuous\n");

  retval = rte_eth_dev_start(port);
  if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot start port\n");

  printf("Port %u MAC: " RTE_ETHER_ADDR_PRT_FMT "\n", (unsigned)port,
         RTE_ETHER_ADDR_BYTES(&s_eth_addr_));

  return 0;
}

inline int CacheMigrationDpdk::RunTimeoutMonitor(void *arg) {
  TxConf *ctx = static_cast<TxConf *>(arg);
  uint16_t queue_id = ctx->queue_id;
  size_t start = ctx->interval.first;
  size_t end = ctx->interval.second;

  constexpr uint64_t kSleepIntervalUs = 1000;
  const uint16_t kMaxRetries = c_m_proto::RETRIES;
  size_t kBatchSize = (end - start) / 10;

  while (running.load(std::memory_order_acquire)) {
    for (size_t i = start; i < end; i += kBatchSize) {
      size_t batch_end = std::min(i + kBatchSize, end);
      uint64_t now = get_now_micros();

      for (size_t j = i; j < batch_end; ++j) {
        RequestInfo &req = requests[j];

        if (req.completed.load(std::memory_order_acquire)) continue;

        uint64_t elapsed_time =
            (now >= req.start_time) ? (now - req.start_time) : 0;
        uint64_t expected_timeout = time_out_us(req.retry_count);
        if (elapsed_time < expected_timeout) continue;

        if (req.retry_count > kMaxRetries) {
          if (req.completed.exchange(true, std::memory_order_acq_rel)) continue;
          timeout_count.fetch_add(1, std::memory_order_relaxed);
          continue;
        }

        if (req.mbuf && !req.completed.load(std::memory_order_acquire)) {
          int sent = rte_eth_tx_burst(0, queue_id, &req.mbuf, 1);
          if (sent == 1) {
            timeout_send.fetch_add(1, std::memory_order_relaxed);
            req.retry_count++;
            req.start_time = now;
          }
        }
      }
      rte_delay_us(kSleepIntervalUs);
    }
  }
  return 0;
}

void CacheMigrationDpdk::DoRx(uint16_t queue_id) {
  uint lcore_id = rte_lcore_id();

  if (lcore_id == RTE_MAX_LCORE || lcore_id == (unsigned)LCORE_ID_ANY) {
    printf("Invalid lcore_id = %u\n", lcore_id);
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

    rte_mbuf *bufs[BURST_SIZE];

    while (running.load(std::memory_order_acquire)) {
      nb_rx = rte_eth_rx_burst(port, queue_id, bufs, BURST_SIZE);

      if (unlikely(nb_rx <= 0)) {
        continue;
      } else {
        for (uint16_t i = 0; i < nb_rx; i++) {
          uint64_t recv_tsc = get_now_micros();

          rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(bufs[i], rte_ether_hdr *);
          rte_ipv4_hdr *ip_hdr = reinterpret_cast<rte_ipv4_hdr *>(eth_hdr + 1);

          rte_prefetch0(ip_hdr + 1);
          if (unlikely(ip_hdr->next_proto_id != IP_PROTOCOLS_NETCACHE)) {
            continue;
          }
          c_m_proto::KVHeader *kv_header =
              reinterpret_cast<c_m_proto::KVHeader *>(ip_hdr + 1);

          const uint32_t request_id = rte_be_to_cpu_32(kv_header->request_id);
          const uint8_t is_req = GET_IS_REQ(kv_header->combined);

          if (is_req != c_m_proto::CACHE_REPLY &&
              is_req != c_m_proto::SERVER_REPLY) {
            std::cerr << "Warning: Received SERVER_REJECT: " << request_id
                      << std::endl;
            continue;
          }

          if (request_id < requests.size()) {
            RequestInfo &req = requests[request_id];
            if (!req.completed.load()) {
              req.completed.store(true);
              req.completed_time = recv_tsc;

              // completed_count.fetch_add(1, std::memory_order_relaxed);
              total_latency_us.fetch_add(recv_tsc - req.start_time,
                                         std::memory_order_relaxed);
              if (req.op == c_m_proto::READ_REQUEST)
                read_success_.fetch_add(1, std::memory_order_relaxed);
              else if (req.op == c_m_proto::WRITE_REQUEST)
                update_success_.fetch_add(1, std::memory_order_relaxed);
            }
          } else {
            std::cerr << "Warning: Received req_id out of range: " << request_id
                      << std::endl;
          }
        }

        completed_count.fetch_add(nb_rx, std::memory_order_relaxed);

        rte_pktmbuf_free_bulk(bufs, nb_rx);
      }
    }
    return;
  } else {
    printf("Skip main lcore %u\n", lcore_id);
  }
  return;
}

inline int CacheMigrationDpdk::RxMain(void *arg) {
  RxArgs *args = static_cast<RxArgs *>(arg);
  args->instance->DoRx(args->queue_id);
  return 0;
}

inline int CacheMigrationDpdk::TxMain(void *arg) {
  TxConf *ctx = static_cast<TxConf *>(arg);

  uint64_t tx_success_count = 0;
  uint64_t tx_drop_count = 0;

  uint16_t queue_id = ctx->queue_id;
  size_t start = ctx->interval.first;
  size_t end = ctx->interval.second;

  send_start_us = get_now_micros();
  for (size_t i = start; i < end; ++i) {
    if (requests[i].completed.load(std::memory_order_acquire)) continue;

    if (requests[i].mbuf) {
      requests[i].start_time = get_now_micros();
    } else {
      std::cerr << "Request " << i << " has no mbuf, skipping..." << std::endl;
      continue;
    }

    uint16_t nb_tx = rte_eth_tx_burst(0, queue_id, &requests[i].mbuf, 1);
    if (nb_tx < 1) {
      requests[i].completed.store(true, std::memory_order_release);
      false_count.fetch_add(1, std::memory_order_relaxed);
      std::cerr << " send error " << std::endl;
    } else {
      tx_success_count += nb_tx;
      rte_delay_us(1);
    }

    tx_drop_count += (1 - nb_tx);
  }
  std::cout << "TX thread [" << rte_lcore_id() << "] sent " << tx_success_count
            << " drop " << tx_drop_count << " requests (queue " << queue_id
            << ")" << std::endl;

  // RunTimeoutMonitor(arg);
  return 0;
}

// inline int CacheMigrationDpdk::TimeoutMonitorThread(void *arg) {
//   auto *self = static_cast<CacheMigrationDpdk *>(arg);
//   self->RunTimeoutMonitor();
//   return 0;
// }

inline void CacheMigrationDpdk::LaunchThreads() {
  rx_args_.clear();
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

  tx_args_.clear();
  size_t total = requests.size();
  size_t per_core = total / num_tx_cores_;
  size_t remainder = total % num_tx_cores_;

  size_t offset = 0;
  for (size_t i = 0; i < num_tx_cores_; ++i) {
    uint lcore_id = tx_cores_[i].lcore_id;
    uint16_t queue_id = tx_cores_[i].queue_id;

    auto tx_conf = std::make_unique<TxConf>();
    tx_conf->lcore_id = lcore_id;
    tx_conf->queue_id = queue_id;

    size_t length = per_core + (i < remainder ? 1 : 0);
    tx_conf->interval = {offset, offset + length};
    offset += length;

    tx_args_.push_back(std::move(tx_conf));
    int ret = rte_eal_remote_launch(TxMain, tx_args_.back().get(), lcore_id);
    if (ret < 0) {
      std::cerr << "Failed to launch TX thread on core " << lcore_id
                << ", error: " << rte_strerror(-ret) << std::endl;
      return;
    }
  }

  // if (timeout_core_ != UINT_MAX) {
  //   int ret = rte_eal_remote_launch(TimeoutMonitorThread, this,
  //   timeout_core_); if (ret < 0) {
  //     std::cerr << "Failed to launch TimeoutMonitor thread on core "
  //               << timeout_core_ << ", error: " << rte_strerror(-ret)
  //               << std::endl;
  //     return;
  //   }
  // }
}

rte_mbuf *CacheMigrationDpdk::BuildRequestPacket(
    const std::string &key, uint8_t op, uint32_t req_id,
    const std::vector<KVPair> &values) {
  rte_mbuf *mbuf = rte_pktmbuf_alloc(tx_mbufpool_);
  if (!mbuf) return nullptr;

  char *pkt_data = rte_pktmbuf_append(mbuf, c_m_proto::TOTAL_LEN);
  if (!pkt_data) {
    rte_pktmbuf_free(mbuf);
    std::cerr << "Failed to append packet data\n";
    return nullptr;
  }

  rte_ether_hdr *eth_hdr = reinterpret_cast<rte_ether_hdr *>(pkt_data);
  rte_ether_addr_copy(&s_eth_addr_, &eth_hdr->src_addr);
  rte_ether_addr_copy(&d_eth_addr_, &eth_hdr->dst_addr);
  eth_hdr->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

  rte_ipv4_hdr *ip_hdr =
      reinterpret_cast<rte_ipv4_hdr *>(pkt_data + RTE_ETHER_HDR_LEN);
  ip_hdr->ihl = 5;
  ip_hdr->version = 4;
  ip_hdr->type_of_service = 0;
  ip_hdr->total_length =
      rte_cpu_to_be_16(c_m_proto::TOTAL_LEN - RTE_ETHER_HDR_LEN);
  ip_hdr->packet_id = rte_cpu_to_be_16(54321);
  ip_hdr->next_proto_id = IP_PROTOCOLS_NETCACHE;
  ip_hdr->time_to_live = 64;
  ip_hdr->src_addr = src_ip_;
  ip_hdr->dst_addr = consistent_hash_.GetServerIp(key);
  ip_hdr->hdr_checksum = 0;
  ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

  c_m_proto::KVHeader *kv_header = reinterpret_cast<c_m_proto::KVHeader *>(
      pkt_data + RTE_ETHER_HDR_LEN + c_m_proto::IPV4_HDR_LEN);
  uint16_t combined = ENCODE_COMBINED(dev_id_, op);
  kv_header->request_id = rte_cpu_to_be_32(req_id);
  kv_header->combined = rte_cpu_to_be_16(combined);
  memcpy(kv_header->key.data(), key.data(), c_m_proto::KEY_LENGTH);
  memcpy(kv_header->value1.data(), values[0].second.data(),
         c_m_proto::VALUE_LENGTH);
  memcpy(kv_header->value2.data(), values[1].second.data(),
         c_m_proto::VALUE_LENGTH);
  memcpy(kv_header->value3.data(), values[2].second.data(),
         c_m_proto::VALUE_LENGTH);
  memcpy(kv_header->value4.data(), values[3].second.data(),
         c_m_proto::VALUE_LENGTH);
  return mbuf;
}

int CacheMigrationDpdk::Read(const std::string & /*table*/,
                             const std::string &key,
                             const std::vector<std::string> * /*fields*/,
                             std::vector<KVPair> & /*result*/) {
  const uint32_t req_id = utils::RequestIDGenerator::next();

  read_count_.fetch_add(1, std::memory_order_relaxed);

  rte_mbuf *mbuf =
      BuildRequestPacket(key, c_m_proto::READ_REQUEST, req_id, DEFAULT_VALUES);
  if (!mbuf) {
    false_count.fetch_add(1, std::memory_order_relaxed);
    no_result_.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "Fail to creat mbuf for: " << req_id << std::endl;
    return DB::kErrorNoData;
  }

  if (req_id >= requests.size()) {
    false_count.fetch_add(1, std::memory_order_relaxed);
    no_result_.fetch_add(1, std::memory_order_relaxed);
    rte_pktmbuf_free(mbuf);
    std::cerr << "req_id: " << req_id << " over the limte." << std::endl;
    return DB::kErrorNoData;
  }

  requests[req_id].mbuf = mbuf;
  requests[req_id].op = c_m_proto::READ_REQUEST;

  return DB::kOK;
}

int CacheMigrationDpdk::Insert(const std::string & /*table*/,
                               const std::string &key,
                               std::vector<KVPair> &values) {
  const uint32_t req_id = utils::RequestIDGenerator::next();

  update_count_.fetch_add(1, std::memory_order_relaxed);

  rte_mbuf *mbuf =
      BuildRequestPacket(key, c_m_proto::WRITE_REQUEST, req_id, values);
  if (!mbuf) {
    false_count.fetch_add(1, std::memory_order_relaxed);
    update_failed_.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "Fail to creat mbuf for: " << req_id << std::endl;
    return DB::kErrorNoData;
  }

  if (req_id >= requests.size()) {
    false_count.fetch_add(1, std::memory_order_relaxed);
    update_failed_.fetch_add(1, std::memory_order_relaxed);
    rte_pktmbuf_free(mbuf);

    return DB::kErrorNoData;
  }

  requests[req_id].mbuf = mbuf;
  requests[req_id].op = c_m_proto::WRITE_REQUEST;

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
  utils::RequestIDGenerator::reset();

  for (auto &req : requests) {
    req.clear();
  }
  requests.clear();

  uint64_t completed = completed_count.load(std::memory_order_relaxed);
  uint64_t total_latency = total_latency_us.load(std::memory_order_relaxed);

  double iops = use_time_us > 0
                    ? static_cast<double>(completed) * 1'000'000.0 / use_time_us
                    : 0.0;

  double latency_us_per_request =
      completed > 0 ? static_cast<double>(total_latency) / completed : 0.0;

  double average_latency_us =
      completed > 0 ? static_cast<double>(use_time_us) / completed : 0.0;

  std::cout << std::fixed << std::setprecision(2);
  std::cout << "[Stats] CacheMigrationDpdk Statistics:\n"
            << "Total Requests Completed: " << completed << "\n"
            << "Total Time (s): " << use_time_us / 1'000'000.0 << "\n"
            << "IOPS: " << iops << "( " << iops / 1'000'000.0 << " M)"
            << " ops/sec\n"
            << "Average Latency per Request (ms/op): "
            << latency_us_per_request / 1'000.0 << "\n"
            << "Average Time per Request (us/op): " << average_latency_us
            << "\n"
            << "  Total Reads: " << read_count_.load() << "\n"
            << "  Successful Reads: " << read_success_.load() << "\n"
            << "  No Result Reads: " << no_result_.load() << "\n"
            << "  Total Updates: " << update_count_.load() << "\n"
            << "  Successful Updates: " << update_success_.load() << "\n"
            << "  Failed Updates: " << update_failed_.load() << "\n"
            << "  Time Out: " << timeout_count.load() << "\n"
            << "  TimeoutMonitor Send: " << timeout_send.load() << std::endl;
}

}  // namespace ycsbc
