#pragma once

#include <tbb/concurrent_unordered_map.h>

#include <memory>
#include <shared_mutex>
#include <utility>

template <typename U, typename T>
class RequestMap {
 public:
  using MapType = tbb::concurrent_unordered_map<U, std::shared_ptr<T>>;

  bool Insert(U req_id, std::shared_ptr<T> req) {
    auto [it, inserted] = map_.insert({req_id, std::move(req)});
    return inserted;
  }

  std::shared_ptr<T> Get(U req_id) const {
    auto it = map_.find(req_id);
    return (it != map_.end()) ? it->second : nullptr;
  }

  template <typename Fn>
  bool Modify(U req_id, Fn&& fn) {
    auto it = map_.find(req_id);
    if (it != map_.end()) {
      fn(it->second);
      return true;
    }
    return false;
  }

  bool Erase(U req_id) noexcept {
    std::unique_lock<std::shared_mutex> lock(map_mutex_);
    return map_.unsafe_erase(req_id) > 0;
  }

  void Clear() noexcept { map_.clear(); }

  size_t Size() const noexcept { return map_.size(); }

 private:
  std::shared_mutex map_mutex_;
  MapType map_;
};
