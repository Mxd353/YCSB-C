#include "consistent_hash.h"

#include <algorithm>
#include <boost/crc.hpp>
#include <fstream>
#include <iostream>
#include <mutex>

#include "core/utils.h"

size_t ConsistentHash::Hash(const std::string_view &key) {
  boost::crc_32_type crc;
  crc.process_bytes(key.data(), key.size());
  return crc.checksum();
}

void ConsistentHash::AddServer(const std::string &ip) {
  if (ip.empty()) {
    throw std::invalid_argument("Server IP cannot be empty");
  }
  struct in_addr addr;
  if (inet_pton(AF_INET, ip.c_str(), &addr) != 1) {
    throw std::invalid_argument("Invalid IP address format: " + ip);
  }
  rte_be32_t ip_be = addr.s_addr;

  std::unique_lock lock(rw_mutex_);
  int added_nodes = 0;
  for (int i = 0; i < virtual_node_count_; ++i) {
    std::string virtualNode = ip + "#" + std::to_string(i);
    size_t hashValue = Hash(virtualNode);
    if (hash_to_server_.find(hashValue) == hash_to_server_.end()) {
      auto it =
          std::lower_bound(hash_ring_.begin(), hash_ring_.end(), hashValue);
      hash_ring_.insert(it, hashValue);
      hash_to_server_[hashValue] = ip_be;
      ++added_nodes;
    } else {
      std::cerr << "Virtual node collision: " << virtualNode
                << " (hash: " << hashValue << ") already exists." << "\n";
    }
  }
}

rte_be32_t ConsistentHash::FindServer(const std::string_view &key) {
  if (hash_ring_.empty()) {
    std::cerr << "ERROR: Hash ring is empty when locating key: " << key
              << std::endl;
    throw std::runtime_error("Hash ring is empty, no servers available.");
  }
  size_t hashValue = Hash(key);

  std::shared_lock lock(rw_mutex_);
  auto it = std::lower_bound(hash_ring_.begin(), hash_ring_.end(), hashValue);
  if (it == hash_ring_.end()) {
    it = hash_ring_.begin();
  }

  rte_be32_t server = hash_to_server_[*it];
  return server;
}

ConsistentHash::ConsistentHash(const std::string &server_ip_file,
                               int virtual_node_count)
    : virtual_node_count_(virtual_node_count) {
  std::ifstream file(server_ip_file);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open server IP file: " +
                             server_ip_file);
  }

  std::string ip;
  while (std::getline(file, ip)) {
    if (!ip.empty()) {
      AddServer(ip);
    }
  }
  std::cout << "===== Ring initialization complete ====="
            << "\nTotal virtual nodes: " << hash_ring_.size()
            << "\n========================================\n";
}

void ConsistentHash::MigrateKey(
    const std::array<char, c_m_proto::KEY_LENGTH> &key, rte_be32_t newServer) {
  std::string key_str(key.begin(), key.end());
  std::string oldip_str;
  utils::ReverseRTE_IPV4(uint32_t(FindServer(key_str)), oldip_str);
  std::string newip_str;
  utils::ReverseRTE_IPV4(uint32_t(newServer), newip_str);
  // std::cerr << "[MIGRATION] Key " << key_str
  //           << " manually assigned: " << oldip_str << " -> " << newip_str
  //           << "\n";
  key_override_.erase(key_str);
  key_override_[key_str] = newServer;
}

void ConsistentHash::RemoveMigration(const std::string &key) {
  if (key_override_.erase(key)) {
    std::cerr << "[MIGRATION] Removed override for key: " << key << "\n";
  } else {
    std::cerr << "[MIGRATION] No override found for key: " << key << "\n";
  }
}

rte_be32_t ConsistentHash::GetServerIp(const std::string &key) {
  auto override_it = key_override_.find(key);
  if (override_it != key_override_.end()) {
    return override_it->second;
  }
  return FindServer(key);
}
