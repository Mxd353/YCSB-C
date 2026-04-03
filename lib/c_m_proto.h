#pragma once

#include <rte_ethdev.h>
#include <rte_ip4.h>

#include <array>
#include <cstdint>
#include <vector>

#define ENCODE_DEV_INFO(dev_id, dev_type) \
  (((dev_id & 0x1F) << 3) | (dev_type & 0x07))

#define GET_DEV_ID(dev_info) static_cast<uint8_t>(((dev_info) >> 3) & 0x1F)
#define GET_DEV_TYPE(dev_info) static_cast<uint8_t>((dev_info) & 0x07)

#define ENCODE_COMBINED(is_req, op) \
  (((is_req & 0x0F) << 4) | ((op & 0x03) << 2) | (0x00 & 0x03))

#define GET_IS_REQ(combined) static_cast<uint8_t>(((combined) >> 4) & 0x0F)
#define GET_OP(combined) static_cast<uint8_t>(((combined) >> 2) & 0x03)
#define GET_HOT_QUERY(combined) static_cast<uint8_t>((combined) & 0x03)

#define DECODE_IP(ip) \
  ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, (ip >> 24) & 0xFF

#define UDP_PORT_KV 50000

namespace c_m_proto {

constexpr size_t KEY_LENGTH = 16;
constexpr size_t VALUE_LENGTH = 4;

constexpr uint16_t RETRIES = 5;

// DevType
constexpr uint8_t DEV_SPINE = 0;
constexpr uint8_t DEV_LEAF = 1;
constexpr uint8_t DEV_CLIENT = 2;
constexpr uint8_t DEV_UNKNOWN = 3;

// op
constexpr uint8_t READ_REQUEST = 0;
constexpr uint8_t WRITE_REQUEST = 1;
constexpr uint8_t NO_REQUEST = 0xFF;

// is_req
constexpr uint8_t CLIENT_REQUEST = 0;
constexpr uint8_t SERVER_REPLY = 1;
constexpr uint8_t CACHE_REJECT = 2;
constexpr uint8_t CACHE_REPLY = 3;

#pragma pack(push, 1)

struct KVRequest {
  uint8_t dev_info;  // DevID (5 bits) | DevType (3 bits)
  uint32_t request_id = 0;
  uint8_t combined;  // is_req (4 bit) | op (2 bit) | hot_query (2 bit)
  std::array<char, KEY_LENGTH> key{};
  std::array<char, VALUE_LENGTH> value1{};
  std::array<char, VALUE_LENGTH> value2{};
  std::array<char, VALUE_LENGTH> value3{};
  std::array<char, VALUE_LENGTH> value4{};
};

#pragma pack(pop)

constexpr uint16_t IPV4_HDR_LEN = sizeof(rte_ipv4_hdr);
constexpr uint16_t UDP_HDR_LEN = sizeof(rte_udp_hdr);
constexpr uint16_t KV_HDR_LEN = sizeof(KVRequest);
const uint16_t TOTAL_LEN =
    RTE_ETHER_HDR_LEN + IPV4_HDR_LEN + UDP_HDR_LEN + KV_HDR_LEN;

constexpr uint16_t KV_HEADER_OFFSET =
    RTE_ETHER_HDR_LEN + IPV4_HDR_LEN + UDP_HDR_LEN;

}  // namespace c_m_proto
