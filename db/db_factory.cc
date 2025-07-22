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
  const std::string dbname = props["dbname"];
  DB* db = nullptr;

  if (dbname == "basic") {
    db = new BasicDB();
  } else if (dbname == "cache_migration_dpdk") {
    const int num_threads = std::stoi(props.GetProperty("threadcount", "1"));
    db = new CacheMigrationDpdk(num_threads);
  } else if (dbname == "hot_statistics") {
    db = new HotStatistics();
  }

  if (db != nullptr) {
    db->SetName(dbname);
  }
  return db;
}
