// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <libroach.h>
#include <rocksdb/merge_operator.h>
#include "defines.h"
#include "protos/roachpb/internal.pb.h"
#include "protos/storage/enginepb/mvcc.pb.h"

namespace cockroach {

WARN_UNUSED_RESULT bool MergeValues(cockroach::storage::enginepb::MVCCMetadata* left,
                                    const cockroach::storage::enginepb::MVCCMetadata& right,
                                    bool full_merge, rocksdb::Logger* logger);
DBStatus MergeResult(cockroach::storage::enginepb::MVCCMetadata* meta, DBString* result);
rocksdb::MergeOperator* NewMergeOperator();
void sortAndDeduplicateColumns(roachpb::InternalTimeSeriesData* data, int first_unsorted);
void convertToColumnar(roachpb::InternalTimeSeriesData* data);

}  // namespace cockroach
