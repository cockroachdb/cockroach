// Copyright 2018 The Cockroach Authors.
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

#pragma once

#include <rocksdb/table_properties.h>

namespace cockroach {

// DBMakeTimeBoundCollector returns a TablePropertiesCollector hook to store the
// min and max MVCC timestamps present in each sstable in the metadata for that
// sstable. Used by the time bounded iterator optimization.
rocksdb::TablePropertiesCollectorFactory* DBMakeTimeBoundCollector();

// DBMakeDeleteRangeCollector returns a TablePropertiesCollector hook
// to mark sstables for compaction that contain range deletion
// tombstones. This ensures that range deletion tombstones are quickly
// compacted out of existence and the space for deleted data is
// reclaimed.
rocksdb::TablePropertiesCollectorFactory* DBMakeDeleteRangeCollector();

}  // namespace cockroach
