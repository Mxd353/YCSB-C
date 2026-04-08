#include "cache_migration_dpdk.h"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "core/timer.h"
#include "core/utils.h"

using namespace c_m_proto;
using namespace utils;

extern char* __progname;

static std::vector<RequestInfo> requests;
static std::vector<rte_mbuf*> packets;

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
std::atomic<size_t> update_count{0};

namespace ycsbc {

thread_local rte_be32_t CacheMigrationDpdk::src_ip_ = 0;
thread_local uint16_t CacheMigrationDpdk::dev_id_ = 0;
thread_local int CacheMigrationDpdk::thread_id_ = 0;
thread_local CacheMigrationDpdk::ThreadStats thread_stats;

// ============================================================================
// 第1组：静态辅助函数（工具函数）
// ============================================================================

// 根据重试次数计算超时时间（微秒），使用指数退避策略
static inline uint64_t time_out_us(int retry_count) {
  constexpr uint64_t BASE_TIMEOUT_US = 20'000;
  constexpr uint64_t MAX_TIMEOUT_US = 500'000;

  if (retry_count <= 0) {
    return BASE_TIMEOUT_US;
  }

  uint64_t exponential_backoff = BASE_TIMEOUT_US * (1ULL << (retry_count - 1));
  return std::min(exponential_backoff, MAX_TIMEOUT_US);
}

// ============================================================================
// 第2组：构造函数/析构函数（对象生命周期管理）
// ============================================================================

// 构造函数 - 初始化DPDK环境、内存池和网络端口
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

  // 分配核心并初始化端口
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

// 析构函数 - 清理DPDK资源，停止端口，释放内存池
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

// ============================================================================
// 第3组：DB接口实现（YCSB标准接口）
// ============================================================================

// 为请求分配空间并重置统计计数器
void CacheMigrationDpdk::AllocateSpace(size_t total_ops, size_t req_size) {
  if (total_ops == 0) {
    throw std::invalid_argument("total_ops must be greater than 0");
  }

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
  packets.reserve(total_ops);
}

// 初始化线程本地状态，设置源IP和设备ID
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

// 关闭数据库连接
void CacheMigrationDpdk::Close() {}

// 构建并入队读请求数据包
int CacheMigrationDpdk::Read(const std::string& /*table*/,
                             const std::string& key,
                             const std::vector<std::string>* /*fields*/,
                             std::vector<KVPair>& /*result*/) {
  rte_mbuf* mbuf = BuildRequestPacket(key, READ_REQUEST, DEFAULT_VALUES);
  if (!mbuf) {
    false_count.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "[Read] Fail to creat mbuf for: " << key << std::endl;
    return DB::kErrorNoData;
  }

  packets.push_back(mbuf);

  return DB::kOK;
}

// 构建并入队插入请求数据包
int CacheMigrationDpdk::Insert(const std::string& /*table*/,
                               const std::string& key,
                               std::vector<KVPair>& values) {
  rte_mbuf* mbuf = BuildRequestPacket(key, WRITE_REQUEST, values);
  if (!mbuf) {
    false_count.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "[Insert] Fail to creat mbuf for: " << key << std::endl;
    return DB::kErrorNoData;
  }

  packets.push_back(mbuf);

  return DB::kOK;
}

// 将更新操作委托给Insert处理
int CacheMigrationDpdk::Update(const std::string& table, const std::string& key,
                               std::vector<KVPair>& values) {
  return Insert(table, key, values);
}

// 扫描操作（未实现）
int CacheMigrationDpdk::Scan(const std::string& /*table*/,
                             const std::string& /*key*/, int /*record_count*/,
                             const std::vector<std::string>* /*fields*/,
                             std::vector<std::vector<KVPair>>& /*result*/) {
  return DB::kOK;
}

// 将删除操作委托给Update处理，使用默认值
int CacheMigrationDpdk::Delete(const std::string& table,
                               const std::string& key) {
  return Update(table, key, DEFAULT_VALUES);
}

// ============================================================================
// 第4组：初始化相关（核心分配和端口初始化）
// ============================================================================

// 根据总可用核心数按照 2:1:1 比例分配 RX、TX 和超时监控核心
// 要求：总核心数 >= 4
// 分配规则：RX = total/2, TX = total/4, Timeout = total/4
// 剩余核心分配给 RX
// TX队列分配：前 tx_count 个队列给 TX 线程，后 timeout_count 个队列给 Timeout
// 线程（独立队列）
inline void CacheMigrationDpdk::AssignCores() {
  uint lcore_id;
  std::vector<uint> workers;

  RTE_LCORE_FOREACH_WORKER(lcore_id) { workers.push_back(lcore_id); }

  size_t total_workers = workers.size();
  if (total_workers < 4) {
    rte_exit(EXIT_FAILURE,
             "Need at least 4 worker cores (got %zu): RX=total/2, TX=total/4, "
             "Timeout=total/4\n",
             total_workers);
  }

  // 核心分配比例: RX=1/2, TX=1/4, Timeout=1/4
  size_t rx_count = total_workers / 2;
  size_t tx_count = total_workers / 4;
  size_t timeout_count = total_workers / 4;

  // 处理剩余核心（当 total_workers 不是 4 的倍数时）
  // 剩余核心分配给 RX，因为 RX 处理压力最大
  size_t remainder = total_workers - (rx_count + tx_count + timeout_count);
  rx_count += remainder;

  rx_cores_.reserve(rx_count);
  tx_cores_.reserve(tx_count);
  timeout_cores_.reserve(timeout_count);

  size_t idx = 0;
  for ([[maybe_unused]] auto i : range(rx_count)) {
    rx_cores_.emplace_back(RxConf{workers[idx++], 0});
  }

  for ([[maybe_unused]] auto i : range(tx_count)) {
    tx_cores_.emplace_back(TxConf{workers[idx++]});
  }

  for ([[maybe_unused]] auto i : range(timeout_count)) {
    // Timeout线程使用独立的TX队列，队列ID从tx_count开始
    uint16_t queue_id = tx_count + i;
    timeout_cores_.emplace_back(TimeoutConf{workers[idx++], queue_id});
  }

  printf(
      "Assigned %zu RX cores (50%%), %zu TX cores (25%%), "
      "%zu Timeout cores (25%%), total %zu TX queues\n",
      rx_cores_.size(), tx_cores_.size(), timeout_cores_.size(),
      tx_cores_.size() + timeout_cores_.size());
}

// 初始化以太网端口，配置RSS和硬件卸载功能
int CacheMigrationDpdk::PortInit(uint16_t port) {
  uint16_t nb_rxd = RX_RING_SIZE;
  uint16_t nb_txd = TX_RING_SIZE;

  uint16_t nb_rx_cores = rx_cores_.size();
  uint16_t nb_tx_cores =
      tx_cores_.size() +
      timeout_cores_.size();  // TX队列 = TX核心数 + Timeout核心数

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

  if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
    port_conf.txmode.offloads &= ~RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;

  // 检查网卡是否支持所需的队列数
  if (nb_rx_cores > dev_info.max_rx_queues) {
    rte_exit(EXIT_FAILURE,
             "Error: Requested %u RX queues exceed hardware limit %u. "
             "Please reduce RX cores or use a NIC with more RX queues.\n",
             nb_rx_cores, dev_info.max_rx_queues);
  }
  if (nb_tx_cores > dev_info.max_tx_queues) {
    rte_exit(EXIT_FAILURE,
             "Error: Requested %u TX queues (TX=%zu + Timeout=%zu) exceed "
             "hardware limit %u. Please reduce worker cores or use a NIC "
             "with more TX queues.\n",
             nb_tx_cores, tx_cores_.size(), timeout_cores_.size(),
             dev_info.max_tx_queues);
  }

  RTE_LOG(NOTICE, EAL,
          "Hardware supports: max_rx_queues=%u, max_tx_queues=%u\n",
          dev_info.max_rx_queues, dev_info.max_tx_queues);
  RTE_LOG(NOTICE, EAL, "Requesting: rx_queues=%u, tx_queues=%u\n", nb_rx_cores,
          nb_tx_cores);

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

  rte_eth_txconf txconf = dev_info.default_txconf;
  txconf.offloads = port_conf.txmode.offloads;

  // 为TX核心创建TX队列（队列ID 0 ~ tx_count-1）
  for (auto q : range(tx_cores_.size())) {
    retval = rte_eth_tx_queue_setup(port, q, nb_txd,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup TX queue\n");

    tx_cores_[q].queue_id = q;
  }

  // 为Timeout监控核心创建独立的TX队列（队列ID tx_count ~
  // tx_count+timeout_count-1）
  for (auto q : range(timeout_cores_.size())) {
    uint16_t queue_id = tx_cores_.size() + q;
    retval = rte_eth_tx_queue_setup(port, queue_id, nb_txd,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) {
      RTE_LOG(ERR, EAL, "Cannot setup Timeout TX queue %u\n", queue_id);
      rte_exit(EXIT_FAILURE, "Cannot setup Timeout TX queue\n");
    }
    // queue_id已经在AssignCores中设置到timeout_cores_
  }

  RTE_LOG(
      NOTICE, EAL,
      "Setup %zu TX queues for TX threads, %zu TX queues for Timeout threads\n",
      tx_cores_.size(), timeout_cores_.size());

  retval = rte_eth_promiscuous_enable(port);
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot set promiscuous\n");

  retval = rte_eth_dev_start(port);
  if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot start port\n");

  RTE_LOG(NOTICE, EAL, "Port %u MAC: " RTE_ETHER_ADDR_PRT_FMT "\n",
          (unsigned)port, RTE_ETHER_ADDR_BYTES(&s_eth_addr_));

  return 0;
}

// ============================================================================
// 第5组：核心运行逻辑（线程函数）
// ============================================================================

// 启动DPDK处理线程并等待完成
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

// 在指定的核心上启动RX、TX和超时监控线程
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

  // 在独立核心上启动多个超时监控线程
  timeout_args_.clear();
  size_t num_timeout_cores = timeout_cores_.size();
  if (num_timeout_cores > 0) {
    size_t per_timeout_core = total / num_timeout_cores;
    size_t timeout_remainder = total % num_timeout_cores;

    size_t timeout_offset = 0;
    for (auto i : range(num_timeout_cores)) {
      uint lcore_id = timeout_cores_[i].lcore_id;

      auto timeout_conf = std::make_unique<TimeoutConf>();
      timeout_conf->lcore_id = lcore_id;
      timeout_conf->queue_id = timeout_cores_[i].queue_id;

      size_t length = per_timeout_core + (i < timeout_remainder ? 1 : 0);
      timeout_conf->interval = {timeout_offset, timeout_offset + length};
      timeout_offset += length;

      timeout_args_.push_back(std::move(timeout_conf));
      int ret = rte_eal_remote_launch(RunTimeoutMonitor,
                                      timeout_args_.back().get(), lcore_id);
      if (ret < 0) {
        std::cerr << "Failed to launch timeout monitor thread on core "
                  << lcore_id << ", error: " << rte_strerror(-ret) << std::endl;
        return;
      }
    }

    std::cout << "Launched " << num_timeout_cores
              << " timeout monitor threads on dedicated cores" << std::endl;
  }
}

// 批量发送请求并处理超时监控
inline int CacheMigrationDpdk::TxMain(void* arg) {
  TxConf* ctx = static_cast<TxConf*>(arg);

  uint64_t tx_success_count = 0;
  uint64_t tx_drop_count = 0;

  uint16_t queue_id = ctx->queue_id;
  size_t start = ctx->interval.first;
  size_t end = ctx->interval.second;

  size_t total_packets_num = packets.size();

  send_start_us = get_now_micros();

  constexpr uint16_t BATCH_SIZE = BURST_SIZE;
  rte_mbuf* batch_packets[BATCH_SIZE];
  size_t batch_index = 0;

  for (auto request_id : range(start, end)) {
    requests[request_id].start_time.store(get_now_micros(),
                                          std::memory_order_relaxed);

    size_t packet_index = request_id % total_packets_num;
    if (packet_index >= packets.size()) {
      std::cerr << "Error: Attempt to access an out-of-bounds packet index: "
                << packet_index << " limit: " << packets.size() << std::endl;
      continue;
    }

    rte_mbuf* packet = packets[packet_index];

    auto* kv_header =
        rte_pktmbuf_mtod_offset(packet, KVRequest*, KV_HEADER_OFFSET);
    if (unlikely(kv_header == nullptr)) {
      std::cerr << "Error: Invalid KVHeader pointer" << std::endl;
      continue;
    }

    kv_header->request_id = rte_cpu_to_be_32(request_id);

    const uint8_t op = GET_OP(kv_header->combined);

    (op == READ_REQUEST ? read_count : update_count)
        .fetch_add(1, std::memory_order_relaxed);

    batch_packets[batch_index++] = packet;

    if (batch_index == BATCH_SIZE || request_id == end - 1) {
      if (batch_index > 0) {
        uint16_t nb_tx =
            rte_eth_tx_burst(0, queue_id, batch_packets, batch_index);

        tx_success_count += nb_tx;
        tx_drop_count += (batch_index - nb_tx);

        for (auto i : range(nb_tx, batch_index)) {
          size_t failed_request_id = request_id - (batch_index - 1) + i;
          if (failed_request_id >= start && failed_request_id < end) {
            // 发送失败时，不标记为完成，让超时机制处理重试
            false_count.fetch_add(1, std::memory_order_relaxed);
          }
        }

        batch_index = 0;
      }
    }
  }

  std::cout << "TX thread [" << rte_lcore_id() << "] sent " << tx_success_count
            << " drop " << tx_drop_count << " requests (queue " << queue_id
            << ")" << std::endl;

  return 0;
}

// 从网络接收数据包，并将对应的请求标记为已完成
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

    RTE_LOG(NOTICE, CORE, "[RX] %u polling queue: %hu\n", lcore_id, queue_id);

    rte_mbuf* bufs[BURST_SIZE];

    while (running.load(std::memory_order_acquire)) {
      nb_rx = rte_eth_rx_burst(port, queue_id, bufs, BURST_SIZE);

      if (unlikely(nb_rx == 0)) {
        continue;
      } else {
        completed_count.fetch_add(nb_rx, std::memory_order_relaxed);

        for (auto i : range(nb_rx)) {
          KVRequest* kv_header =
              rte_pktmbuf_mtod_offset(bufs[i], KVRequest*, KV_HEADER_OFFSET);

          requests[rte_be_to_cpu_32(kv_header->request_id)].completed.exchange(
              true);

          rte_pktmbuf_free(bufs[i]);
        }
      }
    }
  } else {
    printf("Skip main lcore %u\n", lcore_id);
  }
  return;
}

// RX主循环的包装函数
inline int CacheMigrationDpdk::RxMain(void* arg) {
  RxArgs* args = static_cast<RxArgs*>(arg);
  args->instance->DoRx(args->queue_id);
  return 0;
}

// 监控请求超时情况，对超时的请求进行重试
inline int CacheMigrationDpdk::RunTimeoutMonitor(void* arg) {
  TimeoutConf* ctx = static_cast<TimeoutConf*>(arg);
  uint16_t queue_id = ctx->queue_id;
  size_t start = ctx->interval.first;
  size_t end = ctx->interval.second;

  constexpr uint64_t kSleepIntervalUs = 1000;
  const uint16_t kMaxRetries = RETRIES;
  size_t kBatchSize = (end - start) / 10;

  size_t total_packets = packets.size();

  while (running.load(std::memory_order_acquire)) {
    for (auto i : range(start, end, kBatchSize)) {
      size_t batch_end = std::min(i + kBatchSize, end);
      uint64_t now = get_now_micros();

      for (auto request_id : range(i, batch_end)) {
        RequestInfo& req = requests[request_id];

        if (req.completed.load(std::memory_order_acquire) ||
            req.time_out.load(std::memory_order_acquire))
          continue;

        uint64_t start_time =
            req.start_time.load(std::memory_order_relaxed);
        uint64_t elapsed_time = (now >= start_time) ? (now - start_time) : 0;

        int current_retry_count =
            req.retry_count.load(std::memory_order_acquire);

        uint64_t expected_timeout = time_out_us(current_retry_count);
        if (elapsed_time < expected_timeout) continue;

        if (current_retry_count > kMaxRetries) {
          if (req.time_out.exchange(true)) continue;
          timeout_count.fetch_add(1, std::memory_order_relaxed);
          continue;
        }

        size_t packet_index = request_id % total_packets;
        if (packet_index >= packets.size()) {
          std::cerr
              << "Error: Attempt to access an out-of-bounds packet index: "
              << packet_index << " limit: " << packets.size() << std::endl;
          continue;
        }
        rte_mbuf* packet = packets[packet_index];

        if (packet) {
          // 双重检查：发送前再次确认请求状态
          if (req.completed.load(std::memory_order_acquire) ||
              req.time_out.load(std::memory_order_acquire)) {
            continue;
          }
          int sent = rte_eth_tx_burst(0, queue_id, &packet, 1);
          if (sent == 1) {
            timeout_send.fetch_add(1, std::memory_order_relaxed);
            req.retry_count.fetch_add(1, std::memory_order_relaxed);
            req.start_time.store(now, std::memory_order_relaxed);
          }
        }
      }
      rte_delay_us_block(kSleepIntervalUs);
    }
  }
  return 0;
}

// ============================================================================
// 第6组：数据包构建（被DB接口调用）
// ============================================================================

// 构建包含以太网、IP、UDP头部和KV负载的请求数据包
rte_mbuf* CacheMigrationDpdk::BuildRequestPacket(
    const std::string& key, uint8_t op, const std::vector<KVPair>& values) {
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
  kv_request->request_id = 0;
  kv_request->combined =
      static_cast<uint8_t>(ENCODE_COMBINED(CLIENT_REQUEST, op));
  rte_memcpy(kv_request->key.data(), key.data(), KEY_LENGTH);
  rte_memcpy(kv_request->value1.data(), values[0].second.data(), VALUE_LENGTH);
  rte_memcpy(kv_request->value2.data(), values[1].second.data(), VALUE_LENGTH);
  rte_memcpy(kv_request->value3.data(), values[2].second.data(), VALUE_LENGTH);
  rte_memcpy(kv_request->value4.data(), values[3].second.data(), VALUE_LENGTH);
  return mbuf;
}

// ============================================================================
// 第7组：统计输出
// ============================================================================

// 打印基准测试统计信息，包括IOPS、延迟和操作计数
void CacheMigrationDpdk::PrintStats() {
  uint64_t total_request = total_request_count.load(std::memory_order_relaxed);
  uint64_t completed = completed_count.load(std::memory_order_relaxed);
  completed = completed <= total_request_count ? completed : total_request;
  uint64_t total_latency = total_latency_us.load(std::memory_order_relaxed);

  double iops = use_time_us > 0
                    ? static_cast<double>(completed) * REQ_SIZE / use_time_us
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
            << " ops per sec\n"
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
