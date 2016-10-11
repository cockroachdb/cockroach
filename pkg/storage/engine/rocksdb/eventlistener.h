// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Cuong Do <cdo@cockroachlabs.com>

#ifndef ROACHLIB_EVENTLISTENER_H
#define ROACHLIB_EVENTLISTENER_H

#include <atomic>

#include <rocksdb/db.h>

// DBEventListener is an implementation of RocksDB's EventListener interface
// used to collect information on RocksDB events that could be of interest
// to CockroachDB users.
class DBEventListener : public rocksdb::EventListener {
 public:
  DBEventListener();
  virtual ~DBEventListener() { }

  uint64_t GetFlushes() const;
  uint64_t GetCompactions() const;

  // EventListener methods.
  virtual void OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_job_info) override;
  virtual void OnCompactionCompleted(rocksdb::DB* db, const rocksdb::CompactionJobInfo& ci) override;

 private:
  std::atomic<uint64_t> flushes_;
  std::atomic<uint64_t> compactions_;
};


#endif // ROACHLIB_EVENTLISTENER_H
