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

#include <rocksdb/table_properties.h>
#include "eventlistener.h"

static const bool kDebug = false;

DBEventListener::DBEventListener()
  : flushes_(0),
    compactions_(0) {
}

void DBEventListener::OnFlushCompleted(rocksdb::DB* db, const rocksdb::FlushJobInfo& flush_job_info) {
  ++flushes_;

  if (kDebug) {
    const rocksdb::TableProperties &p = flush_job_info.table_properties;
    fprintf(stderr, "OnFlushCompleted:\n  %40s:  index=%.1f  filter=%.1f\n",
            flush_job_info.file_path.c_str(),
            p.index_size / float(p.num_entries),
            p.filter_size / float(p.num_entries));
  }
}

void DBEventListener::OnCompactionCompleted(rocksdb::DB* db, const rocksdb::CompactionJobInfo& ci) {
  ++compactions_;

  if (kDebug) {
    fprintf(stderr, "OnCompactionCompleted:\n");
    for (auto iter = ci.table_properties.begin(); iter != ci.table_properties.end(); ++iter) {
      const rocksdb::TableProperties &p = *iter->second;
      fprintf(stderr, "  %40s: index=%.1f  filter=%.1f\n",
              iter->first.c_str(),
              p.index_size / float(p.num_entries),
              p.filter_size / float(p.num_entries));
    }
  }
}

uint64_t DBEventListener::GetFlushes() const {
  return flushes_.load();
}

uint64_t DBEventListener::GetCompactions() const {
  return compactions_.load();
}
