#include "consistent_hash.h"

#include <algorithm>
#include <boost/crc.hpp>
#include <fstream>
#include <iostream>

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

  std::cout << "Adding " << virtual_node_count_
            << " virtual nodes for server: " << ip << "\n";

  int added_nodes = 0;
  for (int i = 0; i < virtual_node_count_; ++i) {
    std::string virtualNode = ip + "#" + std::to_string(i);
    size_t hashValue = Hash(virtualNode);
    if (hash_to_server_.find(hashValue) ==
        hash_to_server_.end())  // 避免重复插入
    {
      auto it =
          std::lower_bound(hash_ring_.begin(), hash_ring_.end(), hashValue);
      hash_ring_.insert(it, hashValue);
      hash_to_server_[hashValue] = ip_be;
      ++added_nodes;
    } else {
      std::cout << "Virtual node collision: " << virtualNode
                << " (hash: " << hashValue << ") already exists." << "\n";
    }
  }
  std::cout << "Successfully added " << added_nodes << "/"
            << virtual_node_count_ << " virtual nodes for server: " << ip
            << "\n";
}

rte_be32_t ConsistentHash::FindServer(const std::string_view &key) {
  if (hash_ring_.empty())  // 处理空环情况
  {
    std::cerr << "ERROR: Hash ring is empty when locating key: " << key
              << std::endl;
    throw std::runtime_error("Hash ring is empty, no servers available.");
  }
  size_t hashValue = Hash(key);
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

  std::cout << "\n===== Initializing Consistent Hash Ring =====\n";
  std::string ip;
  while (std::getline(file, ip)) {
    if (!ip.empty()) {
      std::cout << "Loading physical server: " << ip << "\n";
      AddServer(ip);
    }
  }
  std::cout << "===== Ring initialization complete ====="
            << "\nTotal virtual nodes: " << hash_ring_.size()
            << "\n=========================================\n";
}

void ConsistentHash::MigrateKey(const std::string &key, rte_be32_t newServer) {
  std::cout << "[MIGRATION] Key \"" << key
            << "\" manually assigned to: " << newServer << "\n";
  key_override_.erase(key);
  key_override_[key] = newServer;
}

void ConsistentHash::RemoveMigration(const std::string &key) {
  if (key_override_.erase(key)) {
    std::cout << "[MIGRATION] Removed override for key: " << key << "\n";
  } else {
    std::cout << "[MIGRATION] No override found for key: " << key << "\n";
  }
}

rte_be32_t ConsistentHash::GetServerIp(const std::string &key) {
  auto override_it = key_override_.find(key);
  if (override_it != key_override_.end()) {
    return override_it->second;
  }
  return FindServer(key);
}
