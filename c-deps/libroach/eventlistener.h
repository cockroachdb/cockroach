// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <atomic>

#include <rocksdb/db.h>

// DBEventListener is an implementation of RocksDB's EventListener interface
// used to collect information on RocksDB events that could be of interest
// to CockroachDB users.
class DBEventListener : public rocksdb::EventListener {
 public:
  DBEventListener();
  virtual ~DBEventListener() {}

  uint64_t GetFlushes() const;
  uint64_t GetCompactions() const;

  // EventListener methods.
  virtual void OnFlushCompleted(rocksdb::DB* db,
                                const rocksdb::FlushJobInfo& flush_job_info) override;
  virtual void OnCompactionCompleted(rocksdb::DB* db,
                                     const rocksdb::CompactionJobInfo& ci) override;

 private:
  std::atomic<uint64_t> flushes_;
  std::atomic<uint64_t> compactions_;
};
