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

#include <libroach.h>
#include <rocksdb/merge_operator.h>
#include "defines.h"
#include "protos/roachpb/internal.pb.h"
#include "protos/storage/engine/enginepb/mvcc.pb.h"

namespace cockroach {

WARN_UNUSED_RESULT bool MergeValues(cockroach::storage::engine::enginepb::MVCCMetadata* left,
                                    const cockroach::storage::engine::enginepb::MVCCMetadata& right,
                                    bool full_merge, rocksdb::Logger* logger);
DBStatus MergeResult(cockroach::storage::engine::enginepb::MVCCMetadata* meta, DBString* result);
rocksdb::MergeOperator* NewMergeOperator();
void sortAndDeduplicateColumns(roachpb::InternalTimeSeriesData* data, int first_unsorted);
void convertToColumnar(roachpb::InternalTimeSeriesData* data);

}  // namespace cockroach
