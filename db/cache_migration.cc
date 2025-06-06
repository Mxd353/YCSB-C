#include "cache_migration.h"

#include <sys/ioctl.h>
#include <unistd.h>

#include <chrono>
#include <future>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace ycsbc {
thread_local std::shared_ptr<CacheMigrationSocket>
    CacheMigration::thread_socket_ = nullptr;
thread_local CacheMigration::VethInfo CacheMigration::thread_veth_config_;

CacheMigration::CacheMigration(int num_threads)
    : consistent_hash_("conf/server_ips.conf"), num_threads_(num_threads) {
  if (num_threads_ <= 0) {
    throw std::invalid_argument("Number of threads must be greater than zero.");
  }
  const std::string veth_config_file = "conf/veths_config.conf";
  std::ifstream veth_file(veth_config_file);
  if (!veth_file.is_open()) {
    throw std::runtime_error("Failed to open veth config file: " +
                             veth_config_file);
  }
  std::cout << "\n===== Initializing Veth config =====" << std::endl;
  std::string line;
  while (std::getline(veth_file, line)) {
    std::string veth, ip_str;
    int dev_id;
    std::istringstream iss(line);
    if (iss >> veth >> ip_str >> dev_id) {
      available_veths_.emplace_back(veth, ip_str, dev_id);
      std::cout << "Add " << veth << " with ip: " << ip_str
                << " | dev_id: " << dev_id << "\n";
    } else {
      std::cerr << "Warning: Invalid line format: " << line << std::endl;
    }
  }
  std::cout << "=== Veth initialization complete ==="
            << "\nTotal veth: " << available_veths_.size()
            << "\n====================================\n";
}

CacheMigration::~CacheMigration() { Close(); }

void CacheMigration::Init() {
  if (!thread_socket_) {
    std::lock_guard<std::mutex> lock(veth_pool_mutex_);
    if (available_veths_.empty()) {
      throw std::runtime_error("No available veth configurations");
    }
    thread_veth_config_ = available_veths_.back();
    available_veths_.pop_back();
  }

  thread_socket_ =
      CacheMigrationSocket::Create(consistent_hash_, thread_veth_config_);
  thread_socket_->Initialize();

  std::cout << "[Init] Initializing CacheMigration for thread: "
            << std::this_thread::get_id() << "\n"
            << "  Network interface: " << std::get<0>(thread_veth_config_)
            << "\n"
            << "  Src ip: " << std::get<1>(thread_veth_config_) << "\n"
            << "  Dev id: " << std::get<2>(thread_veth_config_) << "\n";
}

void CacheMigration::Close() {
  if (thread_socket_) {
    thread_socket_->Close();
    thread_socket_.reset();
    {
      std::lock_guard<std::mutex> lock(veth_pool_mutex_);
      available_veths_.push_back(thread_veth_config_);
    }
    std::cout << "[Close] CacheMigration closed for thread "
              << std::this_thread::get_id() << "\n";
  }
}

void CacheMigration::PrintStats() {
  std::cout << "[Stats] CacheMigration Statistics:\n"
            << "  Total Reads: " << read_count_ << "\n"
            << "  Successful Reads: " << read_success_ << "\n"
            << "  No Result Reads: " << no_result_ << "\n"
            << "  Total Updates: " << update_count_ << "\n"
            << "  Successful Updates: " << update_success_ << "\n"
            << "  Failed Updates: " << update_failed_ << "\n";
}

int CacheMigration::Read(const std::string & /*table*/, const std::string &key,
                         const std::vector<std::string> * /*fields*/,
                         std::vector<KVPair> &result) {
  try {
    ++read_count_;
    const bool success = thread_socket_->SendReadPacket(
        key, result, consistent_hash_.GetServerIp(key));

    if (!success || result.empty()) {
      no_result_++;
    } else {
      read_success_++;
    }
  } catch (const std::exception &e) {
    std::cerr << "Read operation failed: " << e.what() << std::endl;
    return DB::kErrorNoData;
  }
  return DB::kOK;
}

int CacheMigration::Update(const std::string & /*table*/,
                           const std::string &key,
                           std::vector<KVPair> &values) {
  try {
    ++update_count_;
    const bool success = thread_socket_->SendWritePacket(
        key, values, consistent_hash_.GetServerIp(key));

    if (success) {
      ++update_success_;
    } else {
      ++update_failed_;
    }
  } catch (const std::exception &e) {
    std::cerr << "Update operation failed: " << e.what() << std::endl;
    return DB::kErrorNoData;
  }
  return DB::kOK;
}
}  // namespace ycsbc
