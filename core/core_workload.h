//
//  core_workload.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include <cmath>
#include <cstring>
#include <string>
#include <vector>

#include "counter_generator.h"
#include "db.h"
#include "discrete_generator.h"
#include "generator.h"
#include "properties.h"
#include "utils.h"

namespace ycsbc {

enum Operation { INSERT, READ, UPDATE, SCAN, READMODIFYWRITE };

class CoreWorkload {
 public:
  ///
  /// The name of the database table to run queries against.
  ///
  static const std::string TABLENAME_PROPERTY;
  static const std::string TABLENAME_DEFAULT;

  static const std::string KEY_LENGTH;
  static const std::string KEY_LENGTH_DEFAULT;

  ///
  /// The name of the property for the number of fields in a record.
  ///
  static const std::string FIELD_COUNT_PROPERTY;
  static const std::string FIELD_COUNT_DEFAULT;

  ///
  /// The name of the property for the field length distribution.
  /// Options are "uniform", "zipfian" (favoring short records), and "constant".
  ///
  static const std::string FIELD_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string FIELD_LENGTH_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the length of a field in bytes.
  ///
  static const std::string FIELD_LENGTH_PROPERTY;
  static const std::string FIELD_LENGTH_DEFAULT;

  ///
  /// The name of the property for deciding whether to read one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string READ_ALL_FIELDS_PROPERTY;
  static const std::string READ_ALL_FIELDS_DEFAULT;

  ///
  /// The name of the property for deciding whether to write one field (false)
  /// or all fields (true) of a record.
  ///
  static const std::string WRITE_ALL_FIELDS_PROPERTY;
  static const std::string WRITE_ALL_FIELDS_DEFAULT;

  ///
  /// The name of the property for the proportion of read transactions.
  ///
  static const std::string READ_PROPORTION_PROPERTY;
  static const std::string READ_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of update transactions.
  ///
  static const std::string UPDATE_PROPORTION_PROPERTY;
  static const std::string UPDATE_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of insert transactions.
  ///
  static const std::string INSERT_PROPORTION_PROPERTY;
  static const std::string INSERT_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of scan transactions.
  ///
  static const std::string SCAN_PROPORTION_PROPERTY;
  static const std::string SCAN_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of
  /// read-modify-write transactions.
  ///
  static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
  static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the the distribution of request keys.
  /// Options are "uniform", "zipfian" and "latest".
  ///
  static const std::string REQUEST_DISTRIBUTION_PROPERTY;
  static const std::string REQUEST_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for adding zero padding to record numbers in
  /// order to match string sort order. Controls the number of 0s to left pad
  /// with.
  ///
  static const std::string ZERO_PADDING_PROPERTY;
  static const std::string ZERO_PADDING_DEFAULT;

  ///
  /// The name of the property for the max scan length (number of records).
  ///
  static const std::string MAX_SCAN_LENGTH_PROPERTY;
  static const std::string MAX_SCAN_LENGTH_DEFAULT;

  ///
  /// The name of the property for the scan length distribution.
  /// Options are "uniform" and "zipfian" (favoring short scans).
  ///
  static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the order to insert records.
  /// Options are "ordered" or "hashed".
  ///
  static const std::string INSERT_ORDER_PROPERTY;
  static const std::string INSERT_ORDER_DEFAULT;

  static const std::string INSERT_START_PROPERTY;
  static const std::string INSERT_START_DEFAULT;

  static const std::string RECORD_COUNT_PROPERTY;
  static const std::string OPERATION_COUNT_PROPERTY;

  ///
  /// Initialize the scenario.
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void Init(const utils::Properties &p);

  virtual void BuildValues(std::vector<ycsbc::DB::KVPair> &values);
  virtual void BuildUpdate(std::vector<ycsbc::DB::KVPair> &update);
  virtual std::string BuildMaxKey();

  virtual std::string NextTable() { return table_name_; }
  virtual std::string NextSequenceKey();     /// Used for loading data
  virtual std::string NextTransactionKey();  /// Used for transactions
  virtual void NextTransactionScanKey(std::string &start_key,
                                      std::string &end_key);
  virtual Operation NextOperation() { return op_chooser_.Next(); }
  virtual std::string NextFieldName();
  virtual size_t NextScanLength() { return scan_len_chooser_->Next(); }

  bool read_all_fields() const { return read_all_fields_; }
  bool write_all_fields() const { return write_all_fields_; }

  CoreWorkload()
      : key_length_(16),
        field_count_(0),
        read_all_fields_(false),
        write_all_fields_(false),
        field_len_generator_(NULL),
        key_generator_(NULL),
        key_chooser_(NULL),
        field_chooser_(NULL),
        scan_len_chooser_(NULL),
        insert_key_sequence_(3),
        ordered_inserts_(true),
        record_count_(0),
        max_scan_len_(0) {}

  virtual ~CoreWorkload() {
    if (field_len_generator_) delete field_len_generator_;
    if (key_generator_) delete key_generator_;
    if (key_chooser_) delete key_chooser_;
    if (field_chooser_) delete field_chooser_;
    if (scan_len_chooser_) delete scan_len_chooser_;
  }

 protected:
  static Generator<uint64_t> *GetFieldLenGenerator(const utils::Properties &p);
  std::string BuildKeyName(uint64_t key_num);

  std::string table_name_;
  int key_length_;
  int field_count_;
  bool read_all_fields_;
  bool write_all_fields_;
  Generator<uint64_t> *field_len_generator_;
  Generator<uint64_t> *key_generator_;
  DiscreteGenerator<Operation> op_chooser_;
  Generator<uint64_t> *key_chooser_;
  Generator<uint64_t> *field_chooser_;
  Generator<uint64_t> *scan_len_chooser_;
  CounterGenerator insert_key_sequence_;
  bool ordered_inserts_;
  size_t record_count_;
  int max_scan_len_;
  int zero_padding_;
};

inline std::string CoreWorkload::NextSequenceKey() {
  uint64_t key_num = key_generator_->Next();
  return BuildKeyName(key_num);
}

inline std::string CoreWorkload::NextTransactionKey() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > insert_key_sequence_.Last());
  return BuildKeyName(key_num);
}

inline void CoreWorkload::NextTransactionScanKey(std::string &start_key,
                                                 std::string &end_key) {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > insert_key_sequence_.Last());
  start_key = BuildKeyName(key_num);
  // end_key = BuildKeyName(key_num + scan_interval_);
  end_key = start_key;
  int index = log10(record_count_ / max_scan_len_);
  index = (index - 2) > 0 ? index - 2 : 0;
  end_key[index]++;
}

static inline void fillchar8wirhint64(char *key, uint64_t value) {
  key[0] = ((char)(value >> 56)) & 0xff;
  key[1] = ((char)(value >> 48)) & 0xff;
  key[2] = ((char)(value >> 40)) & 0xff;
  key[3] = ((char)(value >> 32)) & 0xff;
  key[4] = ((char)(value >> 24)) & 0xff;
  key[5] = ((char)(value >> 16)) & 0xff;
  key[6] = ((char)(value >> 8)) & 0xff;
  key[7] = ((char)value) & 0xff;
}
 
inline std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
  if (!ordered_inserts_) {
    key_num = utils::Hash(key_num);
  }
  std::string key_buff(key_length_, '0');
  snprintf(&key_buff[0], key_length_ + 1, "%0*lx", key_length_, key_num);
  return key_buff;
}

inline std::string CoreWorkload::BuildMaxKey() {
  return std::string(key_length_, static_cast<char>(0xff));
}

inline std::string CoreWorkload::NextFieldName() {
  return std::string("field").append(std::to_string(field_chooser_->Next()));
}

}  // namespace ycsbc

#endif  // YCSB_C_CORE_WORKLOAD_H_
