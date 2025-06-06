#ifndef __CONSISTENT_HASH_H__
#define __CONSISTENT_HASH_H__

#include <rte_ip4.h>

#include <string>
#include <unordered_map>
#include <vector>

class ConsistentHash {
 private:
  int virtual_node_count_;
  std::vector<size_t> hash_ring_;
  std::unordered_map<size_t, rte_be32_t> hash_to_server_;
  std::unordered_map<std::string, rte_be32_t> key_override_;

  size_t Hash(const std::string_view &key);
  void AddServer(const std::string &ip);
  rte_be32_t FindServer(const std::string_view &key);

 public:
  ConsistentHash(const std::string &server_ip_file, int virtual_node_count = 3);
  virtual ~ConsistentHash() = default;
  void MigrateKey(const std::string &key, rte_be32_t newServer);
  void RemoveMigration(const std::string &key);
  rte_be32_t GetServerIp(const std::string &key);
};
#endif  // __CONSISTENT_HASH_H__
