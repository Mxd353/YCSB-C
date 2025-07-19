#include "cache_migrate_per_core.h"

#include <fstream>
#include <iostream>
#include <sstream>

#include "core/utils.h"

extern char *__progname;

std::atomic<bool> running{false};

namespace ycsbc {

thread_local rte_be32_t CacheMigratePerCore::src_ip_ = 0;
thread_local uint CacheMigratePerCore::dev_id_ = 0;

CacheMigratePerCore::CacheMigratePerCore(int num_threads)
    : num_threads_(num_threads), consistent_hash_("conf/server_ips.conf") {
  std::vector<std::string> args;
  std::string dpdk_conf = "conf/dpdk.conf";
  std::ifstream dpdk_file(dpdk_conf);
  std::string client_conf = "conf/client_config.conf";
  std::ifstream client_file(client_conf);
  std::string token;

  char *program_name = __progname;

  args.push_back(program_name);

  while (dpdk_file >> token) {
    args.push_back(token);
  }
  std::vector<char *> argv;
  for (auto &arg : args) {
    argv.push_back(const_cast<char *>(arg.c_str()));
  }

  std::cout << "++++++++db initialization++++++++\n";

  int ret = rte_eal_init(argv.size(), argv.data());
  if (ret < 0) {
    std::cerr << "DPDK EAL initialization failed\n";
    rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");
    return;
  }

  rte_ether_unformat_addr("00:11:22:33:44:55", &s_eth_addr_);
  rte_ether_unformat_addr("aa:bb:cc:dd:ee:ff", &d_eth_addr_);

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

  PortInit(0, 1, 1);
  QueueSetup(0, 0);
  PortStart(0);

  std::cout << "+++++++++++++++++++++++++++++++++\n";
  running = true;
}

void CacheMigratePerCore::Init() {
  static thread_local bool initialized = false;
  if (initialized) return;

  auto selected = src_ips_[rte_rand_max(src_ips_size_)];
  src_ip_ = selected.first;
  dev_id_ = selected.second;
}

int CacheMigratePerCore::Read(const std::string &table, const std::string &key,
                              const std::vector<std::string> *fields,
                              std::vector<KVPair> &result) {
  const uint32_t req_id = utils::generate_request_id();
  update_count_.fetch_add(1, std::memory_order_relaxed);

  rte_mbuf *mbuf = BuildRequestPacket(key, WRITE_REQUEST, req_id, DEFAULT_VALUES);
  int res = rte_eth_tx_burst(0, 0, &mbuf, 1);

  
}

int CacheMigratePerCore::PortInit(uint16_t portid, uint16_t n_txring,
                                  uint16_t n_rxring) {
  uint16_t nb_ports = rte_eth_dev_count_avail();
  if (nb_ports == 0) rte_exit(EXIT_FAILURE, "No available DPDK port\n");
  printf("Available number of ports: %u, while we only use port %d\n", nb_ports,
         int(portid));

  if (!rte_eth_dev_is_valid_port(portid))
    rte_exit(EXIT_FAILURE, "Invalid port\n");
  struct rte_eth_conf port_conf;
  memset(&port_conf, 0, sizeof(rte_eth_conf));

  int retval;
  struct rte_eth_dev_info dev_info;
  retval = rte_eth_dev_info_get(portid, &dev_info);
  if (retval != 0) {
    std::cerr << "Error getting device info: " << rte_strerror(retval) << "\n";
    return retval;
  }

  if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
    port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;

  uint16_t nb_rxd = RX_RING_SIZE;
  uint16_t nb_txd = TX_RING_SIZE;
  retval = rte_eth_dev_adjust_nb_rx_tx_desc(portid, &nb_rxd, &nb_txd);
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot adjust desc number\n");
  printf("Initialize port %u with %u TX rings and %u RX rings\n", portid,
         n_txring, n_rxring);

  retval = rte_eth_dev_configure(portid, n_rxring, n_txring, &port_conf);
  if (retval != 0) rte_exit(EXIT_FAILURE, "Cannot configure port\n");

  return retval;
}

void CacheMigratePerCore::QueueSetup(uint16_t portid, uint16_t queueid) {
  int retval = 0;

  char txname[256];
  memset(txname, '\0', 256);
  sprintf(txname, "MBUF_POOL_TX_QUEUE_%d", int(queueid));

  tx_mbufpool_ = rte_pktmbuf_pool_create(
      txname, NUM_MBUFS, MBUF_CACHE_SIZE, RTE_MBUF_PRIV_ALIGN,
      RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
  if (tx_mbufpool_ == NULL) {
    std::cerr << "Failed to create mbuf pool , Error: "
              << rte_strerror(rte_errno) << std::endl;
    rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");
  }

  struct rte_eth_txconf txconf;
  memset(&txconf, 0, sizeof(txconf));
  retval = rte_eth_tx_queue_setup(portid, queueid, TX_RING_SIZE,
                                  rte_eth_dev_socket_id(portid), &txconf);
  if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup TX queue\n");

  char rxname[256];
  memset(rxname, '\0', 256);
  sprintf(rxname, "MBUF_POOL_RX_QUEUE_%d", int(queueid));

  rx_mbufpool_ = rte_pktmbuf_pool_create(
      rxname, NUM_MBUFS, MBUF_CACHE_SIZE, RTE_MBUF_PRIV_ALIGN,
      RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
  if (rx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "Cannot create mbuf pool for RX queue\n");

  retval = rte_eth_rx_queue_setup(portid, queueid, RX_RING_SIZE,
                                  rte_eth_dev_socket_id(portid), nullptr,
                                  rx_mbufpool_);
  if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot setup RX queue\n");
}

int CacheMigratePerCore::PortStart(uint16_t portid) {
  int retval;

  rte_eth_promiscuous_enable(portid);

  retval = rte_eth_dev_start(portid);
  if (retval < 0) rte_exit(EXIT_FAILURE, "Cannot start port\n");

  struct rte_eth_link link;
  memset(&link, 0, sizeof(struct rte_eth_link));
  uint32_t max_repeat_times = 1000;
  uint32_t check_interval_ms = 10;
  for (uint32_t i = 0; i <= max_repeat_times; i++) {
    rte_eth_link_get(portid, &link);
    if (link.link_status == RTE_ETH_LINK_UP) break;
    rte_delay_ms(check_interval_ms);
  }
  if (link.link_status == RTE_ETH_LINK_DOWN) {
    rte_exit(EXIT_FAILURE, "Link is down for port %u\n", portid);
  }
  printf("Initialize port %u done!\n", portid);
}

struct rte_mbuf *CacheMigratePerCore::BuildRequestPacket(
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
}  // namespace ycsbc
