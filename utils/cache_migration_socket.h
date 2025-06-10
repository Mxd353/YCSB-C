#ifndef CACHE_MIGRATION_SOCKET_H
#define CACHE_MIGRATION_SOCKET_H

#include <linux/if_packet.h>
#include <net/ethernet.h>
#include <netinet/ip.h>

#include <boost/asio.hpp>
#include <cstring>
#include <future>
#include <mutex>
#include <queue>
#include <string>
#include <tuple>
#include <unordered_map>
#include <variant>
#include <vector>

#include "consistent_hash.h"
#include "lib/c_m_proto.h"

// namespace asio = boost::asio;

class CacheMigrationSocket
    : public std::enable_shared_from_this<CacheMigrationSocket> {
 public:
  using KVPair = std::pair<std::string, std::string>;
  using VethInfo = std::tuple<std::string, std::string, int>;

  CacheMigrationSocket() = delete;
  static std::shared_ptr<CacheMigrationSocket> Create(
      ConsistentHash &ch, const VethInfo &veth_ip_id) {
    return std::shared_ptr<CacheMigrationSocket>(
        new CacheMigrationSocket(ch, veth_ip_id));
  }

  virtual ~CacheMigrationSocket();

  bool Initialize();
  void Close();
  bool SendReadPacket(const std::string &key, std::vector<KVPair> &result,
                      rte_be32_t dst_ip, int retries = RETRIES);
  bool SendWritePacket(const std::string &key,
                       const std::vector<KVPair> &values, rte_be32_t dst_ip,
                       int retries = RETRIES);

 private:
#pragma pack(push, 1)
  struct CacheMigrationHeader {
    uint32_t request_id;
    uint16_t combined;  // dev_id(8 bit) | is_req(4 bit) | op(2 bit) |
                        // hot_quwey(2 bit)
    uint32_t count;
    uint8_t key[16];
    uint8_t value1[4];
    uint8_t value2[4];
    uint8_t value3[4];
    uint8_t value4[4];
  };

  struct CustomPacket {
    ether_header eth_hdr;
    iphdr ip_hdr;
    CacheMigrationHeader cache_migration_header;
  };
#pragma pack(pop)

  uint16_t ip_tot_len_ = htons(sizeof(iphdr) + sizeof(CacheMigrationHeader));

  int sockfd_ = 0;
  std::mutex send_mutex_;
  ConsistentHash &consistent_hash_;
  std::string iface_;
  std::string src_ip_;
  int dev_id_;
  in_addr_t src_ip_in_;
  int ifindex_;
  uint8_t src_mac_[ETH_ALEN];
  std::unordered_map<std::string, std::shared_ptr<uint8_t[]>> mac_cache_;
  std::atomic<bool> stop_receive_thread_{false};
  std::thread recv_thread_;
  std::mutex request_map_mutex_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::queue<std::vector<char>> packet_queue_;
  std::vector<std::thread> worker_threads_;
  bool stop_workers_ = false;

  struct RequestInfo {
    std::promise<std::vector<KVPair>> *read_promise = nullptr;
    std::promise<bool> *write_promise = nullptr;
    std::string key;
    uint32_t daddr;
    uint8_t op;

    RequestInfo() : daddr(0), op(0) {}
  };
  std::unordered_map<uint32_t, RequestInfo> request_map_;
  std::condition_variable request_cv_;

  const std::vector<KVPair> DEFAULT_VALUES = {{"field0", "read"},
                                              {"field1", "read"},
                                              {"field2", "read"},
                                              {"field3", "read"}};

  const std::vector<std::string> ip_list_ = {
      "192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.2.1", "192.168.2.2",
      "192.168.2.3", "192.168.3.1", "192.168.3.2", "192.168.3.3"};

  const std::vector<std::string> iface_list_ = {"veth15", "veth17", "veth19",
                                                "veth21", "veth23", "veth25",
                                                "veth27", "veth29", "veth31"};

  struct ServerGroup {
    std::vector<std::string> ips;
    std::vector<std::string> ifaces;
    std::unordered_map<std::string, std::string> ipToIface;
    std::unordered_map<std::string, std::string> ifaceToIp;

    ServerGroup(const std::vector<std::string> &ipList,
                const std::vector<std::string> &ifaceList)
        : ips(ipList), ifaces(ifaceList) {
      initMappings();
    }

    void initMappings() {
      if (ips.size() != ifaces.size()) {
        throw std::runtime_error("IPs and interfaces count mismatch!");
      }

      for (size_t i = 0; i < ips.size(); ++i) {
        ipToIface[ips[i]] = ifaces[i];
        ifaceToIp[ifaces[i]] = ips[i];
      }
    }

    std::string getInterface(const std::string &ip) const {
      auto it = ipToIface.find(ip);
      return (it != ipToIface.end()) ? it->second : "Not Found";
    }

    std::string getIp(const std::string &iface) const {
      auto it = ifaceToIp.find(iface);
      return (it != ifaceToIp.end()) ? it->second : "Not Found";
    }
  };

  ServerGroup server_group_;

  CacheMigrationSocket(ConsistentHash &ch, const VethInfo &veth_ip_id)
      : consistent_hash_(ch),
        iface_(std::get<0>(veth_ip_id)),
        src_ip_(std::get<1>(veth_ip_id)),
        dev_id_(std::get<2>(veth_ip_id)),
        server_group_(ip_list_, iface_list_) {}
  void ReceiveThread();
  inline void StopReceiveThread() {
    stop_receive_thread_ = true;
    if (recv_thread_.joinable()) {
      recv_thread_.join();
    }
    StopWorkerThreads();
  }
  void ProcessReceivedPacket(char *buffer);
  void StartWorkerThreads(int thread_count);
  inline void StopWorkerThreads() {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      stop_workers_ = true;
    }
    queue_cv_.notify_all();

    for (auto &t : worker_threads_) {
      if (t.joinable()) t.join();
    }
  }

  void WorkerThread();
  uint16_t Checksum(uint16_t *buffer, int size);
  uint32_t GenerateRequestId();
  void ConstructPacket(CustomPacket &pkt, const std::string &key, uint8_t op,
                       rte_be32_t dst_ip, uint32_t req_id,
                       const std::vector<KVPair> &values);
  inline std::string GetMacAddress(const std::string &iface);
  uint8_t *GetCachedMac(const std::string &dst_iface);
};

#endif  // CACHE_MIGRATION_SOCKET_H
