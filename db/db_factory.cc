//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>

#include "db/basic_db.h"
#include "db/cache_migration_dpdk.h"
#include "db/hot_statistics.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties& props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "cache_migration_dpdk") {
    const int num_threads = stoi(props.GetProperty("threadcount", "1"));
    auto* db = new CacheMigrationDpdk(num_threads);
    return db;
  } else if (props["dbname"] == "hot_statistics") {
    auto* db = new HotStatistics();
    return db;
  }
  return NULL;
}
