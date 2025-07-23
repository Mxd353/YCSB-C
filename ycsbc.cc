//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <unistd.h>

#include <atomic>
#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <typeinfo>
#include <vector>

#include "core/client.h"
#include "core/core_workload.h"
#include "core/timer.h"
#include "core/utils.h"
#include "db/cache_migration_dpdk.h"
#include "db/db_factory.h"

using namespace std;

atomic<uint64_t> ops_cnt[ycsbc::Operation::READMODIFYWRITE + 1];
atomic<uint64_t> ops_time[ycsbc::Operation::READMODIFYWRITE + 1];

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void Init(utils::Properties &props);
void PrintInfo(utils::Properties &props);

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
                   const int thread_id, bool is_loading) {
  db->Init(thread_id);
  struct DBCleaner {
    ycsbc::DB *db;
    ~DBCleaner() { db->Close(); }
  } cleaner{db};

  ycsbc::Client client(*db, *wl);
  int oks = 0;

  for (int i = 0; i < num_ops; ++i) {
    oks += is_loading ? client.DoInsert() : client.DoTransaction();
  }

  return oks;
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  Init(props);
  string file_name = ParseCommandLine(argc, argv, props);

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  const bool load = utils::StrToBool(props.GetProperty("load", "true"));
  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  const bool print_stats = true;
  // const bool ding = utils::StrToBool(props.GetProperty("ding", "false"));
  vector<future<int>> actual_ops;
  int total_ops = 0;
  int sum = 0;
  utils::Timer<std::chrono::microseconds> timer;

  PrintInfo(props);
  ycsbc::CoreWorkload wl;
  wl.Init(props);

  if (load) {
    // Loads data
    total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    db->AllocateSpace(total_ops);
    int threads_to_launch = std::min(num_threads, total_ops);
    int ops_per_thread = total_ops / threads_to_launch;
    int remaining_ops = total_ops % threads_to_launch;

    fprintf(stderr, "Do load use %d threads...\n", threads_to_launch);

    for (int i = 0; i < threads_to_launch; ++i) {
      int ops_to_process = ops_per_thread + (i < remaining_ops ? 1 : 0);
      actual_ops.emplace_back(async(launch::async, DelegateClient, db, &wl,
                                    ops_to_process, i, true));
    }
    assert((int)actual_ops.size() == threads_to_launch);

    sum = 0;
    for (auto &n : actual_ops) {
      assert(n.valid());
      try {
        sum += n.get();
      } catch (const std::exception &e) {
        std::cerr << "Error in thread: " << e.what() << std::endl;
      }
    }
    fflush(stderr);

    if (db && db->GetName() == "cache_migration_dpdk") {
      auto *cmd = dynamic_cast<ycsbc::CacheMigrationDpdk *>(db);
      if (cmd) {
        cmd->StartDpdk();
      } else {
        std::cerr << "Error: DB name is 'cache_migration_dpdk' but "
                     "dynamic_cast failed.\n";
      }
    }

    // printf("********** load result **********\n");
    // printf(
    //     "Total ops  : %5d  use time: %6.3f s  IOPS: %6.2f iops (%6.2f
    //     us/op)\n", sum, use_time / 1e6, sum * 1e6 / use_time,
    //     (double)use_time / sum);

    // printf("*********************************\n");

    if (print_stats) {
      printf("-------------- db statistics --------------\n");
      db->PrintStats();
      printf("-------------------------------------------\n");
    }

    fprintf(stderr, "Wait for db saving...\n");
    sleep(2);
  }

  // Peforms transactions
  for (int j = 0; j < ycsbc::Operation::READMODIFYWRITE + 1; j++) {
    ops_cnt[j].store(0);
    ops_time[j].store(0);
  }

  actual_ops.clear();
  total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
  db->AllocateSpace(total_ops);
  int threads_to_launch = std::min(num_threads, total_ops);
  int ops_per_thread = total_ops / threads_to_launch;

  fprintf(stderr, "Do run use %d threads...\n", threads_to_launch);
  timer.Start();

  for (int i = 0; i < threads_to_launch; ++i) {
    actual_ops.emplace_back(async(launch::async, DelegateClient, db, &wl,
                                  ops_per_thread, i, false));
  }
  assert((int)actual_ops.size() == threads_to_launch);
  sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    try {
      sum += n.get();
    } catch (const std::exception &e) {
      std::cerr << "Error in thread: " << e.what() << std::endl;
    }
  }
  // auto use_time = timer.End();
  fflush(stderr);

  // uint64_t temp_cnt[ycsbc::Operation::READMODIFYWRITE + 1];
  // uint64_t temp_time[ycsbc::Operation::READMODIFYWRITE + 1];

  // for (int j = 0; j < ycsbc::Operation::READMODIFYWRITE + 1; j++) {
  //   temp_cnt[j] = ops_cnt[j].load(std::memory_order_relaxed);
  //   temp_time[j] = ops_time[j].load(std::memory_order_relaxed);
  // }

  // printf("********** run result **********\n");
  // printf("Total ops      : %d\n", sum);
  // printf("Total wall time: %7.3f s\n", use_time / 1e6);
  // printf("Overall IOPS   : %7.2f\n", sum * 1e6 / use_time);
  // printf("Avg Latency    : %6.2f us\n\n", (double)use_time / sum);

  // for (int op = 0; op <= ycsbc::Operation::READMODIFYWRITE; ++op) {
  //   if (temp_cnt[op] == 0) continue;

  //   const char *opname = nullptr;
  //   switch (op) {
  //     case ycsbc::READ:
  //       opname = "READ";
  //       break;
  //     case ycsbc::UPDATE:
  //       opname = "UPDATE";
  //       break;
  //     case ycsbc::INSERT:
  //       opname = "INSERT";
  //       break;
  //     case ycsbc::SCAN:
  //       opname = "SCAN";
  //       break;
  //     case ycsbc::READMODIFYWRITE:
  //       opname = "RMW";
  //       break;
  //     default:
  //       opname = "UNKNOWN";
  //       break;
  //   }

  //   double cpu_time_sec = temp_time[op] / 1e6;
  //   double avg_latency_us = (double)temp_time[op] / temp_cnt[op];
  //   double op_iops = temp_cnt[op] * 1e6 / temp_time[op];

  //   printf(
  //       "[%-6s] ops: %7lu, avg: %7.2f us, IOPS: %8.2f  (CPU time total: %4.1f
  //       " "s)\n", opname, temp_cnt[op], avg_latency_us, op_iops,
  //       cpu_time_sec);
  // }

  // printf(
  //     "\nNote: total op time = thread-summed CPU time, may exceed "
  //     "wall-clock\n");
  // printf("********************************\n");

  if (db && db->GetName() == "cache_migration_dpdk") {
    auto *cmd = dynamic_cast<ycsbc::CacheMigrationDpdk *>(db);
    if (cmd) {
      cmd->StartDpdk();
    } else {
      std::cerr << "Error: DB name is 'cache_migration_dpdk' but "
                   "dynamic_cast failed.\n";
    }
  }

  if (print_stats) {
    printf("-------------- db statistics --------------\n");
    db->PrintStats();
    printf("-------------------------------------------\n");
  }
  // if (ding) {
  //   int ret = system("mpg123 -q conf/ding.mp3");
  //   if (ret != 0) {
  //     std::cerr << "播放失败: " << ret << std::endl;
  //   }
  // }

  delete db;
  return 0;
}

string ParseCommandLine(int argc, const char *argv[],
                        utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-T") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-l") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("load", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-ding") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("ding", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -T n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)"
       << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple "
          "files can"
       << endl;
  cout << "                   be specified, and will be processed in the order "
          "specified"
       << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

void Init(utils::Properties &props) {
  props.SetProperty("dbname", "cache_migration_dpdk");
  props.SetProperty("threadcount", "1");
  props.SetProperty("load", "true");
  props.SetProperty("ding", "false");
}

void PrintInfo(utils::Properties &props) {
  printf("---- dbname:%s ----\n", props["dbname"].c_str());
  printf("%s", props.DebugString().c_str());
  printf("----------------------------------------\n");
  fflush(stdout);
}
