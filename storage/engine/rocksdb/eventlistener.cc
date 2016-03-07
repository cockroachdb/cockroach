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

#include "eventlistener.h"

DBEventListener::DBEventListener()
  : flushes_(0),
    compactions_(0) {
}

void DBEventListener::OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_job_info) {
  ++flushes_;
}

void DBEventListener::OnCompactionCompleted(rocksdb::DB* db, const rocksdb::CompactionJobInfo& ci) {
  ++compactions_;
}

uint64_t DBEventListener::GetFlushes() const {
  return flushes_.load();
}

uint64_t DBEventListener::GetCompactions() const {
  return compactions_.load();
}

