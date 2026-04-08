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

using namespace c_m_proto;
using namespace utils;

extern char* __progname;

static std::vector<RequestInfo> requests;
static std::vector<rte_mbuf*> packets;

// std::atomic<uint32_t> utils::RequestIDGenerator::counter{0};

uint64_t send_start_us = 0;
uint64_t use_time_us = 0;
std::atomic<bool> running{false};

std::atomic<uint64_t> total_latency_us{0};
std::atomic<size_t> total_request_count{0};
std::atomic<size_t> completed_count{0};
std::atomic<size_t> timeout_count{0};
std::atomic<size_t> timeout_send{0};
std::atomic<size_t> false_count{0};

std::atomic<size_t> read_count{0};
// std::atomic<size_t> read_success{0};
std::atomic<size_t> update_count{0};
// std::atomic<size_t> update_success{0};

static inline void exponentialBackoff(int attempt) {
  int wait_ms = std::min(20 * (1 << (attempt - 1)), 500);
  std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
}

static inline uint64_t time_out_us(int retry_count) {
  constexpr uint64_t BASE_TIMEOUT_US = 20'000;
  constexpr uint64_t MAX_TIMEOUT_US = 500'000;

  if (retry_count <= 0) {
    return BASE_TIMEOUT_US;
  }

  uint64_t exponential_backoff = BASE_TIMEOUT_US * (1ULL << (retry_count - 1));
  return std::min(exponential_backoff, MAX_TIMEOUT_US);
}

namespace ycsbc {
thread_local rte_be32_t CacheMigrationDpdk::src_ip_ = 0;
thread_local uint16_t CacheMigrationDpdk::dev_id_ = 0;
thread_local int CacheMigrationDpdk::thread_id_ = 0;
thread_local CacheMigrationDpdk::ThreadStats thread_stats;

CacheMigrationDpdk::CacheMigrationDpdk(utils::Properties& props)
    : num_threads_(std::stoi(props.GetProperty("threadcount", "1"))),
      consistent_hash_("conf/server_ips.conf") {
  std::vector<std::string> args;
  std::string dpdk_conf = "conf/dpdk.conf";
  std::ifstream dpdk_file(dpdk_conf);
  std::string client_conf = "conf/client_config.conf";
  std::ifstream client_file(client_conf);
  std::string token;

  char* program_name = __progname;
  args.push_back(program_name);

  // 动态核数
  //  args.push_back("-l");
  //  args.push_back("0-" + std::to_string(num_threads));

  while (dpdk_file >> token) {
    args.push_back(token);
  }
  std::vector<char*> argv;
  for (auto& arg : args) {
    argv.push_back(const_cast<char*>(arg.c_str()));
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
    uint16_t dev_id;
    std::istringstream iss(line);
    if (iss >> ip_str >> dev_id) {
      dev_infos_.emplace_back(DevInfo{inet_addr(ip_str.c_str()), dev_id});
      std::cout << "Add ip: " << ip_str << " | dev_id: " << dev_id << "\n";
    } else {
      std::cerr << "Warning: Invalid line format: " << line << std::endl;
    }
  }

  uint16_t nb_ports = rte_eth_dev_count_avail();
  if (nb_ports < 1) {
    std::cerr << "No available ports\n";
    rte_exit(EXIT_FAILURE, "Error: need at least one port\n");
    return;
  }

  uint32_t data_size = (uint32_t)TOTAL_LEN + sizeof(rte_mbuf);
  data_size = 1 << (32 - __builtin_clz(data_size - 1));

  tx_mbufpool_ =
      rte_pktmbuf_pool_create("TX_MBUF_POOL", TX_NUM_MBUFS, MBUF_CACHE_SIZE, 0,
                              data_size, rte_socket_id());
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
  rte_ether_unformat_addr("b8:3f:d2:08:ab:d2", &d_eth_addr_);

  std::cout << "+++++++++++++++++++++++++++++++++\n";
}

CacheMigrationDpdk::~CacheMigrationDpdk() {
  running = false;
  rte_eal_mp_wait_lcore();

  uint16_t port = 0;
  rte_eth_dev_stop(port);
  rte_eth_dev_close(port);

  requests.clear();
  for (auto pkt : packets) {
    if (pkt == nullptr) {
      continue;
    }

    rte_pktmbuf_free(pkt);
    pkt = nullptr;
  }
  packets.clear();

  if (tx_mbufpool_) {
    rte_mempool_free(tx_mbufpool_);
    tx_mbufpool_ = nullptr;
  }
  if (rx_mbufpool_) {
    rte_mempool_free(rx_mbufpool_);
    rx_mbufpool_ = nullptr;
  }

  std::cout << "CacheMigrationDpdk resources cleaned up." << std::endl;
}

void CacheMigrationDpdk::AllocateSpace(size_t total_ops, size_t req_size) {
  if (total_ops == 0) {
    throw std::invalid_argument("total_ops must be greater than 0");
  }

  // Note: total_ops = number of packet templates to build (limited by TX_NUM_MBUFS)
  //       req_size = total number of requests to send (can be much larger)
  // The packet templates are reused cyclically to send all requests

  total_latency_us.store(0, std::memory_order_relaxed);
  completed_count.store(0, std::memory_order_relaxed);
  timeout_count.store(0, std::memory_order_relaxed);
  timeout_send.store(0, std::memory_order_relaxed);
  false_count.store(0, std::memory_order_relaxed);
  total_request_count.store(req_size, std::memory_order_relaxed);

  send_start_us = 0;
  use_time_us = 0;

  requests.clear();
  requests.resize(req_size);

  for (auto pkt : packets) {
    if (pkt == nullptr) {
      std::cerr << "Error: Attempt to free a null pointer" << std::endl;
      continue;
    }

    rte_pktmbuf_free(pkt);
    pkt = nullptr;
  }
  packets.clear();
  packets.reserve(total_ops);  // Only build 'total_ops' packet templates
}

void CacheMigrationDpdk::Init(const int thread_id) {
  static thread_local bool initialized = false;
  if (initialized) return;
  thread_id_ = thread_id;

  auto selected =
      dev_infos_[rte_rand_max(static_cast<uint64_t>(dev_infos_.size()))];
  src_ip_ = selected.src_ip;
  dev_id_ = selected.dev_id;

  initialized = true;
}

void CacheMigrationDpdk::Close() {}

void CacheMigrationDpdk::StartDpdk() {
  for (auto& req : requests) {
    if (req.completed.load(std::memory_order_acquire)) {
      rte_exit(EXIT_FAILURE, "Have not completed!");
    }
  }

  for (auto pkt : packets) {
    if (!pkt) {
      rte_exit(EXIT_FAILURE, "Have mbuf is nullptr!");
    }
  }

  std::cerr << "All " << requests.size() << " requests valid." << std::endl;

  running = true;

  std::cerr << "start DPDK.." << std::endl;
  LaunchThreads();

  while (completed_count + timeout_count + false_count < total_request_count) {
    rte_delay_us_sleep(1'000);
  }

  use_time_us = get_now_micros() - send_start_us;
  running = false;

  std::cerr << "All requests completed or timed out." << std::endl;
}

size_t CalculateRXCores(size_t total) {
  if (total <= 1) return 1;
  return (total * 2) / 3;
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

  size_t rx_count = CalculateRXCores(total_workers);
  size_t tx_count = total_workers - rx_count;

  if (tx_count == 0 && total_workers > 1) {
    rx_count--;
    tx_count = 1;
  }

  rx_cores_.reserve(rx_count);
  tx_cores_.reserve(tx_count);

  for (auto i : range(rx_count)) {
    rx_cores_.emplace_back(RxConf{workers[i], 0});
  }

  for (auto i : range(rx_count, rx_count + tx_count)) {
    tx_cores_.emplace_back(TxConf{workers[i]});
  }

  printf("Assigned %zu RX cores, %zu TX cores\n", rx_cores_.size(),
         tx_cores_.size());
}

int CacheMigrationDpdk::PortInit(uint16_t port) {
  uint16_t nb_rxd = RX_RING_SIZE;
  uint16_t nb_txd = TX_RING_SIZE;

  uint16_t nb_rx_cores = rx_cores_.size();
  uint16_t nb_tx_cores = tx_cores_.size();

  if (tx_mbufpool_ == NULL || rx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "mbuf_pool is NULL!\n");

  if (!rte_eth_dev_is_valid_port(port)) return -1;

  static struct rte_eth_conf port_conf_default;

  struct rte_eth_rxmode tmp_rxmode;
  memset(&tmp_rxmode, 0, sizeof(rte_eth_rxmode));
  tmp_rxmode.mq_mode = RTE_ETH_MQ_RX_RSS;
  tmp_rxmode.mtu = RX_MBUF_DATA_SIZE;

  rte_eth_rss_conf rss_conf;
  rss_conf.rss_hf = RTE_ETH_RSS_IP | RTE_ETH_RSS_UDP;
  rss_conf.rss_key_len = 40;
  rss_conf.rss_key = NULL;

  port_conf_default.rxmode = tmp_rxmode;
  port_conf_default.rx_adv_conf.rss_conf = rss_conf;

  rte_eth_conf port_conf = port_conf_default;

  int retval;
  rte_eth_dev_info dev_info;
  retval = rte_eth_dev_info_get(port, &dev_info);
  if (retval != 0) {
    RTE_LOG(ERR, EAL, "Error during getting device (port %u) info: %s\n", port,
            strerror(-retval));
    return retval;
  }

  if (dev_info.rx_offload_capa & RTE_ETH_RX_OFFLOAD_RSS_HASH)
    port_conf.rxmode.offloads |= RTE_ETH_RX_OFFLOAD_RSS_HASH;

  if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_UDP_CKSUM)
    port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_UDP_CKSUM;

  // 确保关闭
  // MBUF_FAST_FREE，因为我们需要在发送后修改数据包（如重发时更新request_id）
  if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
    port_conf.txmode.offloads &= ~RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;

  port_conf.rx_adv_conf.rss_conf.rss_hf &= dev_info.flow_type_rss_offloads;
  if (port_conf.rx_adv_conf.rss_conf.rss_hf !=
      port_conf_default.rx_adv_conf.rss_conf.rss_hf) {
    RTE_LOG(ERR, EAL,
            "Port %u modified RSS hash function based on hardware support,"
            "requested:%#" PRIx64 " configured:%#" PRIx64 "\n",
            port, port_conf_default.rx_adv_conf.rss_conf.rss_hf,
            port_conf.rx_adv_conf.rss_conf.rss_hf);
  }

  retval = rte_eth_dev_configure(port, nb_rx_cores, nb_tx_cores, &port_conf);
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot configure port\n");

  retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot adjust desc number\n");
  RTE_LOG(NOTICE, EAL, "Adjusted nb_rxd = %u, nb_txd = %u\n", nb_rxd, nb_txd);

  struct rte_eth_rxconf rxconf = dev_info.default_rxconf;
  rxconf.offloads = port_conf.rxmode.offloads;

  for (auto q : range(nb_rx_cores)) {
    retval = rte_eth_rx_queue_setup(
        port, q, nb_rxd, rte_eth_dev_socket_id(port), &rxconf, rx_mbufpool_);
    if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup RX queue\n");
    rx_cores_[q].queue_id = q;
  }

  uint16_t max_supported_tx_queues = dev_info.max_tx_queues;
  RTE_LOG(NOTICE, EAL, "max_supported_tx_queues: %u\n",
          max_supported_tx_queues);

  rte_eth_txconf txconf = dev_info.default_txconf;
  txconf.offloads = port_conf.txmode.offloads;

  for (auto q : range(nb_tx_cores)) {
    retval = rte_eth_tx_queue_setup(port, q, nb_txd,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup TX queue\n");

    tx_cores_[q].queue_id = q;
  }

  retval = rte_eth_promiscuous_enable(port);
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot set promiscuous\n");

  retval = rte_eth_dev_start(port);
  if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot start port\n");

  RTE_LOG(NOTICE, EAL, "Port %u MAC: " RTE_ETHER_ADDR_PRT_FMT "\n",
          (unsigned)port, RTE_ETHER_ADDR_BYTES(&s_eth_addr_));

  return 0;
}

/**
 * @brief 超时监控函数，负责检查请求是否超时并处理超时请求
 * @param arg 包含发送配置信息的参数
 * @return 0 表示正常退出
 *
 * 该函数在指定的核心上运行，持续监控指定范围内的请求，
 * 检查是否有请求超时，并对超时的请求进行重试或标记为失败。
 * 实现了指数退避的超时重试机制。
 */
inline int CacheMigrationDpdk::RunTimeoutMonitor(void* arg) {
  // 解析参数，获取发送配置信息
  TxConf* ctx = static_cast<TxConf*>(arg);
  uint16_t queue_id = ctx->queue_id;   // 获取队列ID
  size_t start = ctx->interval.first;  // 起始请求ID
  size_t end = ctx->interval.second;   // 结束请求ID

  // 定义睡眠间隔时间（微秒）
  constexpr uint64_t kSleepIntervalUs = 1000;
  // 最大重试次数限制
  const uint16_t kMaxRetries = RETRIES;
  // 批处理大小，将请求分批处理以提高效率
  size_t kBatchSize = (end - start) / 10;

  // 获取数据包总数
  size_t total_packets = packets.size();

  // 可选调试输出
  // std::cerr << "Start Timeout Monitor on lcore: " << rte_lcore_id()
  //           << std::endl;

  // 主循环：持续监控直到系统停止运行
  while (running.load(std::memory_order_acquire)) {
    // 将请求范围分批处理，避免单次处理过多请求
    for (auto i : range(start, end, kBatchSize)) {
      // 计算当前批次的结束位置
      size_t batch_end = std::min(i + kBatchSize, end);
      // 获取当前时间戳
      uint64_t now = get_now_micros();

      // 遍历当前批次中的每个请求
      for (auto request_id : range(i, batch_end)) {
        // 获取请求信息的引用
        RequestInfo& req = requests[request_id];

        // 跳过已完成或已超时的请求
        if (req.completed.load() || req.time_out.load()) continue;

        // 计算已用时间
        uint64_t elapsed_time =
            (now >= req.start_time) ? (now - req.start_time) : 0;

        // 获取当前重试次数
        int current_retry_count =
            req.retry_count.load(std::memory_order_acquire);

        // 计算预期超时时间（根据重试次数动态调整）
        uint64_t expected_timeout = time_out_us(current_retry_count);
        // 如果未超时，跳过此请求
        if (elapsed_time < expected_timeout) continue;

        // 检查是否超过最大重试次数
        if (current_retry_count > kMaxRetries) {
          // 原子操作标记请求为超时状态
          if (req.time_out.exchange(true)) continue;
          // 更新超时计数
          timeout_count.fetch_add(1, std::memory_order_relaxed);
          continue;
        }

        // 计算数据包索引（使用取模运算循环使用数据包）
        size_t packet_index = request_id % total_packets;
        if (packet_index >= packets.size()) {
          std::cerr
              << "Error: Attempt to access an out-of-bounds packet index: "
              << packet_index << " limit: " << packets.size() << std::endl;
          continue;
        }
        // 获取要重发的数据包
        rte_mbuf* packet = packets[packet_index];

        // 检查数据包有效性并尝试重新发送
        if (packet && !req.completed.load()) {
          // 发送数据包
          int sent = rte_eth_tx_burst(0, queue_id, &packet, 1);
          if (sent == 1) {
            // 重发成功，更新统计信息
            // std::cerr << "Time out send" << std::endl;
            timeout_send.fetch_add(1,
                                   std::memory_order_relaxed);  // 增加重发计数
            req.retry_count.fetch_add(
                1, std::memory_order_relaxed);  // 增加重试次数
            req.start_time = now;               // 重置开始时间为当前时间
          }
        }
      }
      // 每处理完一批次后短暂休眠，避免过度占用CPU
      rte_delay_us_block(kSleepIntervalUs);
    }
  }
  return 0;
}

/**
 * @brief 处理网络接收操作的函数
 * @param queue_id 要处理的接收队列ID
 *
 * 该函数在指定的核心上运行，负责从网络接口接收数据包，
 * 处理接收到的响应，并标记相应的请求为已完成状态。
 */
void CacheMigrationDpdk::DoRx(uint16_t queue_id) {
  // 获取当前核心ID
  uint lcore_id = rte_lcore_id();

  // 检查核心ID是否有效
  if (lcore_id == RTE_MAX_LCORE || lcore_id == (unsigned)LCORE_ID_ANY) {
    printf("Invalid lcore_id = %u\n", lcore_id);
    return;
  }

  // 检查核心是否启用且不是主核心
  if (rte_lcore_is_enabled(lcore_id) && lcore_id != rte_get_main_lcore()) {
    uint16_t port = 0;  // 网络端口号
    uint16_t nb_rx;     // 接收到的数据包数量

    // 检查端口是否在远程NUMA节点上，可能影响性能
    if (rte_eth_dev_socket_id(port) >= 0 &&
        rte_eth_dev_socket_id(port) != (int)rte_socket_id())
      printf(
          "WARNING, port %u is on remote NUMA node to "
          "polling thread.\n\tPerformance will "
          "not be optimal.\n",
          port);

    // 记录核心正在轮询的队列信息
    RTE_LOG(NOTICE, CORE, "[RX] %u polling queue: %hu\n", lcore_id, queue_id);

    // 用于存储接收到的数据包的缓冲区
    rte_mbuf* bufs[BURST_SIZE];

    // 主循环：持续接收数据包直到系统停止运行
    while (running.load(std::memory_order_acquire)) {
      // 从网络端口批量接收数据包
      nb_rx = rte_eth_rx_burst(port, queue_id, bufs, BURST_SIZE);

      // 如果没有接收到数据包，继续轮询
      if (unlikely(nb_rx == 0)) {
        continue;
      } else {
        // 更新完成的请求计数
        completed_count.fetch_add(nb_rx, std::memory_order_relaxed);

        // 处理每个接收到的数据包
        for (auto i : range(nb_rx)) {
          // 从数据包中提取KV请求头部
          KVRequest* kv_header =
              rte_pktmbuf_mtod_offset(bufs[i], KVRequest*, KV_HEADER_OFFSET);

          // 标记对应的请求为已完成状态
          requests[rte_be_to_cpu_32(kv_header->request_id)].completed.exchange(
              true);

          // 释放数据包内存
          rte_pktmbuf_free(bufs[i]);
        }
      }
    }
  } else {
    // 跳过主核心，避免干扰主核心的工作
    printf("Skip main lcore %u\n", lcore_id);
  }
  return;
}

inline int CacheMigrationDpdk::RxMain(void* arg) {
  RxArgs* args = static_cast<RxArgs*>(arg);
  args->instance->DoRx(args->queue_id);
  return 0;
}

/**
 * @brief 处理网络发送操作的主函数
 * @param arg 包含发送配置信息的参数
 * @return 0 表示成功
 *
 * 该函数在指定的核心上运行，负责发送网络数据包，
 * 处理发送结果，统计发送成功和失败的数量，
 * 并在发送完成后启动超时监控。
 */
inline int CacheMigrationDpdk::TxMain(void* arg) {
  // 解析参数，获取发送配置信息
  TxConf* ctx = static_cast<TxConf*>(arg);

  // 初始化发送统计计数器
  uint64_t tx_success_count = 0;  // 成功发送的数据包数量
  uint64_t tx_drop_count = 0;     // 丢弃的数据包数量

  // 从配置中获取队列ID和请求处理范围
  uint16_t queue_id = ctx->queue_id;
  size_t start = ctx->interval.first;  // 起始请求ID
  size_t end = ctx->interval.second;   // 结束请求ID

  // 获取数据包总数
  size_t total_packets_num = packets.size();

  // 记录发送开始时间
  send_start_us = get_now_micros();

  // 批量发送数据包
  constexpr uint16_t BATCH_SIZE =
      BURST_SIZE;  // 使用定义的BURST_SIZE作为批量大小
  rte_mbuf* batch_packets[BATCH_SIZE];
  size_t batch_index = 0;

  // 遍历处理指定范围内的请求
  for (auto request_id : range(start, end)) {
    // 设置请求开始时间
    requests[request_id].start_time = get_now_micros();

    // 计算数据包索引（使用取模运算循环复用数据包模板）
    // This allows sending more requests than packet templates built
    size_t packet_index = request_id % total_packets_num;
    if (packet_index >= packets.size()) {
      std::cerr << "Error: Attempt to access an out-of-bounds packet index: "
                << packet_index << " limit: " << packets.size() << std::endl;
      continue;
    }

    // 获取数据包并解析网络头部
    rte_mbuf* packet = packets[packet_index];

    // 解析KV请求头部
    auto* kv_header =
        rte_pktmbuf_mtod_offset(packet, KVRequest*, KV_HEADER_OFFSET);
    if (unlikely(kv_header == nullptr)) {
      std::cerr << "Error: Invalid KVHeader pointer" << std::endl;
      continue;
    }

    // 设置请求ID（转换为网络字节序）
    kv_header->request_id = rte_cpu_to_be_32(request_id);

    // 获取操作类型（读或写）
    const uint8_t op = GET_OP(kv_header->combined);

    // 更新操作类型统计
    (op == READ_REQUEST ? read_count : update_count)
        .fetch_add(1, std::memory_order_relaxed);

    // 将数据包添加到批处理数组
    batch_packets[batch_index++] = packet;

    // 当批处理数组满或到达请求末尾时发送数据包
    if (batch_index == BATCH_SIZE || request_id == end - 1) {
      if (batch_index > 0) {
        // 发送批量数据包
        uint16_t nb_tx =
            rte_eth_tx_burst(0, queue_id, batch_packets, batch_index);

        // 更新统计信息
        tx_success_count += nb_tx;
        tx_drop_count += (batch_index - nb_tx);

        // 处理发送失败的数据包
        for (auto i : range(nb_tx, batch_index)) {
          // 计算对应的请求ID
          size_t failed_request_id = request_id - (batch_index - 1) + i;
          if (failed_request_id >= start && failed_request_id < end) {
            requests[failed_request_id].completed.store(true);
            false_count.fetch_add(1, std::memory_order_relaxed);
          }
        }

        // 重置批处理索引
        batch_index = 0;
      }
    }
  }

  // 打印发送统计信息
  std::cout << "TX thread [" << rte_lcore_id() << "] sent " << tx_success_count
            << " drop " << tx_drop_count << " requests (queue " << queue_id
            << ")" << std::endl;

  // 启动超时监控
  RunTimeoutMonitor(arg);
  return 0;
}

inline void CacheMigrationDpdk::LaunchThreads() {
  rx_args_.clear();
  for (auto& core : rx_cores_) {
    try {
      uint core_id = core.lcore_id;
      uint16_t queue_id = core.queue_id;
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
    } catch (const std::exception& e) {
      std::cerr << "Exception launching RX thread: " << e.what() << std::endl;
      return;
    }
  }

  tx_args_.clear();
  size_t total = requests.size();
  size_t num_tx_cores = tx_cores_.size();
  size_t per_core = total / num_tx_cores;
  size_t remainder = total % num_tx_cores;

  size_t offset = 0;
  for (auto i : range(num_tx_cores)) {
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
}

rte_mbuf* CacheMigrationDpdk::BuildRequestPacket(
    const std::string& key, uint8_t op, uint32_t req_id,
    const std::vector<KVPair>& values) {
  if (dev_infos_.empty()) {
    RTE_LOG(ERR, PACKET, "No source IP available for packet construction\n");
    return nullptr;
  }

  const size_t value_count = values.size();
  if (value_count == 0) {
    RTE_LOG(WARNING, PACKET,
            "BuildRequestPacket called with empty values vector\n");
  }
  auto selected = dev_infos_[0];
  rte_be32_t src_ip = selected.src_ip;
  uint16_t dev_id = selected.dev_id;

  rte_mbuf* mbuf = rte_pktmbuf_alloc(tx_mbufpool_);
  if (!mbuf) return nullptr;

  char* pkt_data = rte_pktmbuf_append(mbuf, TOTAL_LEN);
  if (!pkt_data) {
    rte_pktmbuf_free(mbuf);
    std::cerr << "Failed to append packet data\n";
    return nullptr;
  }

  rte_ether_hdr* eth_hdr = reinterpret_cast<rte_ether_hdr*>(pkt_data);
  rte_ether_addr_copy(&s_eth_addr_, &eth_hdr->src_addr);
  rte_ether_addr_copy(&d_eth_addr_, &eth_hdr->dst_addr);
  eth_hdr->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

  rte_ipv4_hdr* ip_hdr = reinterpret_cast<rte_ipv4_hdr*>(eth_hdr + 1);
  ip_hdr->ihl = 5;
  ip_hdr->version = 4;
  ip_hdr->type_of_service = 0;
  ip_hdr->total_length = rte_cpu_to_be_16(TOTAL_LEN - RTE_ETHER_HDR_LEN);
  ip_hdr->packet_id = rte_cpu_to_be_16(54321);
  ip_hdr->next_proto_id = IPPROTO_UDP;
  ip_hdr->time_to_live = 64;
  ip_hdr->src_addr = src_ip;
  ip_hdr->dst_addr = consistent_hash_.GetServerIp(key);
  ip_hdr->hdr_checksum = 0;
  ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);

  rte_udp_hdr* udp_hdr = reinterpret_cast<rte_udp_hdr*>(ip_hdr + 1);
  udp_hdr->src_port = rte_cpu_to_be_16(UDP_PORT_KV);
  udp_hdr->dst_port = rte_cpu_to_be_16(UDP_PORT_KV);
  udp_hdr->dgram_len = rte_cpu_to_be_16(UDP_HDR_LEN + KV_HDR_LEN);
  udp_hdr->dgram_cksum = rte_ipv4_udptcp_cksum(ip_hdr, udp_hdr);

  KVRequest* kv_request = reinterpret_cast<KVRequest*>(udp_hdr + 1);
  kv_request->dev_info =
      static_cast<uint8_t>(ENCODE_DEV_INFO(dev_id, DEV_CLIENT));
  kv_request->request_id = rte_cpu_to_be_32(req_id);
  kv_request->combined =
      static_cast<uint8_t>(ENCODE_COMBINED(CLIENT_REQUEST, op));
  rte_memcpy(kv_request->key.data(), key.data(), KEY_LENGTH);
  rte_memcpy(kv_request->value1.data(), values[0].second.data(), VALUE_LENGTH);
  rte_memcpy(kv_request->value2.data(), values[1].second.data(), VALUE_LENGTH);
  rte_memcpy(kv_request->value3.data(), values[2].second.data(), VALUE_LENGTH);
  rte_memcpy(kv_request->value4.data(), values[3].second.data(), VALUE_LENGTH);
  return mbuf;
}

int CacheMigrationDpdk::Read(const std::string& /*table*/,
                             const std::string& key,
                             const std::vector<std::string>* /*fields*/,
                             std::vector<KVPair>& /*result*/) {
  const uint32_t req_id = 0;

  rte_mbuf* mbuf =
      BuildRequestPacket(key, READ_REQUEST, req_id, DEFAULT_VALUES);
  if (!mbuf) {
    false_count.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "Fail to creat mbuf for: " << req_id << std::endl;
    return DB::kErrorNoData;
  }

  packets.push_back(mbuf);

  return DB::kOK;
}

int CacheMigrationDpdk::Insert(const std::string& /*table*/,
                               const std::string& key,
                               std::vector<KVPair>& values) {
  const uint32_t req_id = 0;

  rte_mbuf* mbuf = BuildRequestPacket(key, WRITE_REQUEST, req_id, values);
  if (!mbuf) {
    false_count.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "Fail to creat mbuf for: " << req_id << std::endl;
    return DB::kErrorNoData;
  }

  packets.push_back(mbuf);

  return DB::kOK;
}

int CacheMigrationDpdk::Update(const std::string& table, const std::string& key,
                               std::vector<KVPair>& values) {
  return Insert(table, key, values);
}

int CacheMigrationDpdk::Scan(const std::string& /*table*/,
                             const std::string& /*key*/, int /*record_count*/,
                             const std::vector<std::string>* /*key*/,
                             std::vector<std::vector<KVPair>>& /*result*/) {
  return DB::kOK;  // Not implemented for now
}

int CacheMigrationDpdk::Delete(const std::string& table,
                               const std::string& key) {
  return Update(table, key, DEFAULT_VALUES);
}

void CacheMigrationDpdk::PrintStats() {
  uint64_t total_request = total_request_count.load(std::memory_order_relaxed);
  uint64_t completed = completed_count.load(std::memory_order_relaxed);
  completed = completed <= total_request_count ? completed : total_request;
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
            << "Total Requests: " << total_request << "\n"
            << "Total Requests Completed: " << completed << "\n"
            << "Total Time (s): " << use_time_us / 1'000'000.0 << "\n"
            << "IOPS: " << iops << "( " << iops / 1'000'000.0 << " M)"
            << " ops/sec\n"
            << "Average Latency per Request (ms/op): "
            << latency_us_per_request / 1'000.0 << "\n"
            << "Average Time per Request (us/op): " << average_latency_us
            << "\n"
            << "  Total Reads: " << read_count.load() << "\n"
            << "  Total Updates: " << update_count.load() << "\n"
            << "  Time Out: " << timeout_count.load() << "\n"
            << "  TimeoutMonitor Send: " << timeout_send.load() << std::endl;
}

}  // namespace ycsbc
