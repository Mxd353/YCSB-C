#include "cache_migration_socket.h"

#include <fcntl.h>
#include <net/if.h>
#include <pcap.h>
#include <sys/ioctl.h>

#include <bitset>
#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <thread>

#include "core/timer.h"

const std::array<std::string, 4> fields = {"field0", "field1", "field2",
                                           "field3"};

CacheMigrationSocket::~CacheMigrationSocket() { Close(); }
void CacheMigrationSocket::Close() {
  StopReceiveThread();
  if (sockfd_ >= 0) {
    close(sockfd_);
    sockfd_ = -1;
  }
}

bool CacheMigrationSocket::Initialize() {
  sockfd_ = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_IP));
  if (sockfd_ < 0) {
    throw std::runtime_error("Failed to create raw socket");
  }

  int buf_size = 2 * 1024 * 1024;  // 2MB
  setsockopt(sockfd_, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
  int one = 1;
  int ret = setsockopt(sockfd_, SOL_PACKET, PACKET_IGNORE_OUTGOING, &one,
                       sizeof(one));
  if (ret == -1) {
    perror("setsockopt failed");
  }
  // 获取接口索引
  struct ifreq ifr;
  memset(&ifr, 0, sizeof(ifr));
  snprintf(ifr.ifr_name, sizeof(ifr.ifr_name), "%s", iface_.c_str());

  if (ioctl(sockfd_, SIOCGIFINDEX, &ifr) < 0) {
    close(sockfd_);
    throw std::runtime_error("Failed to get interface index");
  }
  ifindex_ = ifr.ifr_ifindex;
  // 获取接口 MAC 地址
  if (ioctl(sockfd_, SIOCGIFHWADDR, &ifr) < 0) {
    perror("ioctl(SIOCGIFHWADDR) failed");
    close(sockfd_);
    return false;
  }
  memcpy(src_mac_, ifr.ifr_hwaddr.sa_data, 6);
  src_ip_in_ = inet_addr(src_ip_.c_str());
  StartWorkerThreads(3);  // 处理线程
  stop_receive_thread_ = false;
  recv_thread_ =
      std::thread(&CacheMigrationSocket::ReceiveThread, this);  // 收包线程
  return true;
}

void CacheMigrationSocket::StartWorkerThreads(int thread_count) {
  for (int i = 0; i < thread_count; ++i) {
    worker_threads_.emplace_back(&CacheMigrationSocket::WorkerThread, this);
  }
}

inline void StringTo128Bit(const std::string &key, uint8_t (&result)[16]) {
  std::fill(result, result + 16, 0);
  std::memcpy(result, key.data(), std::min(key.size(), size_t(16)));
}

inline std::string StringFrom128Bit(const uint8_t (&value)[16]) {
  size_t len = 16;
  for (size_t i = 0; i < len; ++i) {
    if (value[i] == 0) {
      len = i;
      break;
    }
  }
  return std::string(reinterpret_cast<const char *>(value), len);
}

inline uint32_t fast_inet(const std::string &ip) {
  uint32_t res = 0;
  uint32_t val = 0;
  int shift = 24;

  for (char c : ip) {
    if (c == '.') {
      res |= (val << shift);
      val = 0;
      shift -= 8;
    } else {
      val = val * 10 + (c - '0');
    }
  }
  res |= val;
  return htonl(res);
}

inline std::string ip_to_string(uint32_t ip_net_order) {
  char buf[INET_ADDRSTRLEN];
  inet_ntop(AF_INET, &ip_net_order, buf, INET_ADDRSTRLEN);
  return std::string(buf);
}

inline void StringTo32Bit(const std::string &value, uint8_t (&result)[4]) {
  std::fill(result, result + 4, 0);
  std::memcpy(result, value.data(), std::min(value.size(), size_t(4)));
}

inline void StringFrom32Bit(std::vector<CacheMigrationSocket::KVPair> &result,
                            const uint8_t (&value1)[4],
                            const uint8_t (&value2)[4],
                            const uint8_t (&value3)[4],
                            const uint8_t (&value4)[4]) {
  result.reserve(4);
  const std::array<const uint8_t *, 4> values = {value1, value2, value3,
                                                 value4};
  for (size_t i = 0; i < 4; ++i) {
    result.emplace_back(CacheMigrationSocket::KVPair{
        fields[i], std::string(reinterpret_cast<const char *>(values[i]), 4)});
  }
}

inline std::string CacheMigrationSocket::GetMacAddress(
    const std::string &iface) {
  int fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    perror("socket");
    return "";
  }

  struct ifreq ifr;
  memset(&ifr, 0, sizeof(ifr));
  strncpy(ifr.ifr_name, iface.c_str(), IFNAMSIZ - 1);

  if (ioctl(fd, SIOCGIFHWADDR, &ifr) < 0) {
    perror("ioctl");
    close(fd);
    return "";
  }

  close(fd);

  unsigned char *mac = (unsigned char *)ifr.ifr_hwaddr.sa_data;
  char mac_str[18];
  snprintf(mac_str, sizeof(mac_str), "%02X:%02X:%02X:%02X:%02X:%02X", mac[0],
           mac[1], mac[2], mac[3], mac[4], mac[5]);

  return std::string(mac_str);
}

uint16_t CacheMigrationSocket::Checksum(uint16_t *buffer, int size) {
  unsigned long sum = 0;
  while (size > 1) {
    sum += *buffer++;
    size -= 2;
  }
  if (size > 0) {
    sum += htons(*(uint8_t *)buffer << 8);
  }
  sum = (sum >> 16) + (sum & 0xFFFF);
  sum += (sum >> 16);
  return static_cast<uint16_t>(~sum);
}

inline void exponentialBackoff(int attempt) {
  int wait_ms = std::min(500 * (1 << (attempt - 1)), 4000);
  if (attempt == 0) wait_ms = 0;

  std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
}

uint32_t CacheMigrationSocket::GenerateRequestId() {
  static std::atomic<uint32_t> counter{0};
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis(0, UINT32_MAX);

  return dis(gen) ^ counter.fetch_add(1, std::memory_order_relaxed);
}

uint8_t *CacheMigrationSocket::GetCachedMac(const std::string &dst_iface) {
  auto it = mac_cache_.find(dst_iface);
  if (it != mac_cache_.end()) {
    return it->second.get();
  }
  std::string mac_str = GetMacAddress(dst_iface);
  std::shared_ptr<uint8_t[]> mac_ptr(new uint8_t[ETH_ALEN]);
  sscanf(mac_str.c_str(), "%hhX:%hhX:%hhX:%hhX:%hhX:%hhX", &mac_ptr.get()[0],
         &mac_ptr.get()[1], &mac_ptr.get()[2], &mac_ptr.get()[3],
         &mac_ptr.get()[4], &mac_ptr.get()[5]);

  mac_cache_[dst_iface] = mac_ptr;
  return mac_cache_[dst_iface].get();
}

void CacheMigrationSocket::ConstructPacket(CustomPacket &pkt,
                                           const std::string &key, uint8_t op,
                                           rte_be32_t dst_ip, uint32_t req_id,
                                           const std::vector<KVPair> &values) {
  memset(&pkt, 0, sizeof(pkt));
  // uint8_t *dest_mac = GetCachedMac(server_group_.getInterface(dst_ip));

  uint8_t dest_mac[ETH_ALEN] = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66};
  // Ethernet
  memcpy(pkt.eth_hdr.ether_dhost, dest_mac, ETH_ALEN);
  memcpy(pkt.eth_hdr.ether_shost, src_mac_, ETH_ALEN);
  pkt.eth_hdr.ether_type = htons(ETHERTYPE_IP);

  // IP
  pkt.ip_hdr.ihl = 5;
  pkt.ip_hdr.version = 4;
  pkt.ip_hdr.tos = 0;
  pkt.ip_hdr.tot_len = ip_tot_len_;
  pkt.ip_hdr.id = htons(54321);
  pkt.ip_hdr.ttl = 64;
  pkt.ip_hdr.protocol = IP_PROTOCOLS_NETCACHE;
  pkt.ip_hdr.saddr = src_ip_in_;
  pkt.ip_hdr.daddr = dst_ip;
  pkt.ip_hdr.check =
      Checksum(reinterpret_cast<uint16_t *>(&pkt.ip_hdr), sizeof(iphdr));

  // cache_migration_header
  uint16_t combined = ENCODE_COMBINED(dev_id_, op);
  pkt.cache_migration_header.request_id = htonl(req_id);
  pkt.cache_migration_header.combined = htons(combined);
  pkt.cache_migration_header.count = 0;
  StringTo128Bit(key, pkt.cache_migration_header.key);
  if (values.size() == 4) {
    StringTo32Bit(values[0].second, pkt.cache_migration_header.value1);
    StringTo32Bit(values[1].second, pkt.cache_migration_header.value2);
    StringTo32Bit(values[2].second, pkt.cache_migration_header.value3);
    StringTo32Bit(values[3].second, pkt.cache_migration_header.value4);
  }
}

void CacheMigrationSocket::ProcessReceivedPacket(char *buffer) {
  struct iphdr *ip_header = (struct iphdr *)(buffer + sizeof(ethhdr));
  if (ip_header->protocol != IP_PROTOCOLS_NETCACHE) return;

  struct CacheMigrationHeader *cm_hdr =
      (struct CacheMigrationHeader *)(buffer + sizeof(ethhdr) + sizeof(iphdr));
  uint32_t request_id = ntohl(cm_hdr->request_id);

  std::lock_guard<std::mutex> lock(request_map_mutex_);
  auto it = request_map_.find(request_id);
  if (it == request_map_.end()) return;

  auto &request = it->second;
  uint8_t is_req = static_cast<uint8_t>(GET_IS_REQ(cm_hdr->combined));

  if (is_req == CACHE_REPLY || is_req == SERVER_REPLY) {
    if (request.op == READ_REQUEST) {
      try {
        std::vector<KVPair> data;
        StringFrom32Bit(data, cm_hdr->value1, cm_hdr->value2, cm_hdr->value3,
                        cm_hdr->value4);
        request.read_promise->set_value(data);

      } catch (const std::future_error &e) {
        std::cerr << "Promise already satisfied: " << e.what() << std::endl;
        request.read_promise->set_exception(std::current_exception());
      }
    } else if (request.op == WRITE_REQUEST) {
      try {
        request.write_promise->set_value(true);
      } catch (const std::future_error &e) {
        std::cerr << "Write promise already satisfied: " << e.what()
                  << std::endl;
        request.write_promise->set_exception(std::current_exception());
      }
    }

    if (ip_header->saddr != request.daddr) {
      consistent_hash_.MigrateKey(request.key, ip_header->saddr);
    }
  } else {
    std::cerr << "Server rejected request_id = " << request_id << std::endl;
    return;
  }
  request_map_.erase(it);
}

void CacheMigrationSocket::WorkerThread() {
  while (true) {
    std::vector<char> packet;
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(lock,
                     [&] { return !packet_queue_.empty() || stop_workers_; });

      if (stop_workers_ && packet_queue_.empty()) return;

      packet = std::move(packet_queue_.front());
      packet_queue_.pop();
    }

    ProcessReceivedPacket(packet.data());
  }
}

void CacheMigrationSocket::ReceiveThread() {
  struct sockaddr_ll src_addr;
  socklen_t addrlen = sizeof(src_addr);

  while (!stop_receive_thread_) {
    char buffer[1024];
    ssize_t recvlen = recvfrom(sockfd_, buffer, sizeof(buffer), 0,
                               (struct sockaddr *)&src_addr, &addrlen);

    if (recvlen > 0) {
      struct ethhdr *eth_header = (struct ethhdr *)buffer;
      if (memcmp(eth_header->h_source, src_mac_, 6) == 0) continue;
      // ProcessReceivedPacket(buffer, recvlen);
      std::vector<char> packet(buffer, buffer + recvlen);
      {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        packet_queue_.emplace(std::move(packet));
      }
      queue_cv_.notify_one();

    } else if (recvlen < 0 && errno != EAGAIN) {
      perror("recvfrom error");
    }
  }
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    stop_workers_ = true;
  }
  queue_cv_.notify_all();
}

bool CacheMigrationSocket::SendReadPacket(const std::string &key,
                                          std::vector<KVPair> &result,
                                          rte_be32_t dst_ip, int retries) {
  uint32_t req_id = GenerateRequestId();

  CustomPacket pkt;
  constexpr auto timeout = std::chrono::milliseconds(100);

  ConstructPacket(pkt, key, READ_REQUEST, dst_ip, req_id, DEFAULT_VALUES);

  struct sockaddr_ll dest_addr = {};
  socklen_t addrlen = sizeof(dest_addr);
  memset(&dest_addr, 0, addrlen);
  dest_addr.sll_family = AF_PACKET;
  dest_addr.sll_ifindex = ifindex_;
  dest_addr.sll_halen = ETH_ALEN;
  memcpy(dest_addr.sll_addr, pkt.eth_hdr.ether_dhost, 6);

  std::promise<std::vector<KVPair>> read_promise;
  auto future = read_promise.get_future();

  RequestInfo req;
  req.key = key;
  req.daddr = pkt.ip_hdr.daddr;
  req.op = READ_REQUEST;
  req.read_promise = &read_promise;
  {
    std::lock_guard<std::mutex> lock(request_map_mutex_);
    request_map_[req_id] = std::move(req);
  }

  for (int attempt = 0; attempt < retries; ++attempt) {
    {
      std::lock_guard<std::mutex> lock(send_mutex_);
      if (sendto(sockfd_, &pkt, sizeof(pkt), 0, (struct sockaddr *)&dest_addr,
                 addrlen) < 0) {
        perror("sendto failed");
        continue;
      }
    }

    if (future.wait_for(timeout) == std::future_status::ready) {
      try {
        result = future.get();
        return true;
      } catch (const std::exception &e) {
        std::cerr << "Request failed: " << e.what() << std::endl;
      }
    }

    exponentialBackoff(attempt);
  }
  {
    std::lock_guard<std::mutex> lock(request_map_mutex_);
    request_map_.erase(req_id);
  }
  throw std::runtime_error("Max retries exceeded");
}

bool CacheMigrationSocket::SendWritePacket(const std::string &key,
                                           const std::vector<KVPair> &values,
                                           rte_be32_t dst_ip, int retries) {
  uint32_t req_id = GenerateRequestId();
  CustomPacket pkt;
  constexpr auto timeout = std::chrono::milliseconds(100);

  ConstructPacket(pkt, key, WRITE_REQUEST, dst_ip, req_id, values);

  struct sockaddr_ll dest_addr = {};
  socklen_t addrlen = sizeof(dest_addr);
  memset(&dest_addr, 0, addrlen);
  dest_addr.sll_family = AF_PACKET;
  dest_addr.sll_ifindex = ifindex_;
  dest_addr.sll_halen = ETH_ALEN;
  memcpy(dest_addr.sll_addr, pkt.eth_hdr.ether_dhost, 6);

  std::promise<bool> write_promise;
  auto future = write_promise.get_future();

  RequestInfo req;
  req.key = key;
  req.daddr = pkt.ip_hdr.daddr;
  req.op = WRITE_REQUEST;
  req.write_promise = &write_promise;

  {
    std::lock_guard<std::mutex> lock(request_map_mutex_);
    request_map_[req_id] = std::move(req);
  }

  for (int attempt = 0; attempt < retries; ++attempt) {
    {
      std::lock_guard<std::mutex> lock(send_mutex_);
      if (sendto(sockfd_, &pkt, sizeof(pkt), 0, (struct sockaddr *)&dest_addr,
                 addrlen) < 0) {
        perror("Sendto failed");
        continue;
      }
    }

    if (future.wait_for(timeout) == std::future_status::ready) {
      try {
        return future.get();
      } catch (const std::exception &e) {
        std::cerr << "Write failed: " << e.what() << std::endl;
        return false;
      }
    }
    exponentialBackoff(attempt);
  }
  {
    std::lock_guard<std::mutex> lock(request_map_mutex_);
    request_map_.erase(req_id);
  }
  throw std::runtime_error("Max retries exceeded");
}
