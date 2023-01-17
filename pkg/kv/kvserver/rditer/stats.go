// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rditer

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
)

// ComputeStatsForRange computes the stats for a given range by iterating over
// all key spans for the given range that should be accounted for in its stats.
func ComputeStatsForRange(
	d *roachpb.RangeDescriptor, reader storage.Reader, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsForRangeWithVisitors(d, reader, nowNanos, nil, nil)
}

// ComputeStatsForRangeWithVisitors is like ComputeStatsForRange but also
// calls the given callbacks for every key.
func ComputeStatsForRangeWithVisitors(
	d *roachpb.RangeDescriptor,
	reader storage.Reader,
	nowNanos int64,
	pointKeyVisitor func(storage.MVCCKey, []byte) error,
	rangeKeyVisitor func(storage.MVCCRangeKeyValue) error,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats
	for _, keySpan := range makeReplicatedKeySpansExceptLockTable(d) {
		msDelta, err := storage.ComputeStatsWithVisitors(reader, keySpan.Key, keySpan.EndKey, nowNanos,
			pointKeyVisitor, rangeKeyVisitor)
		if err != nil {
			return enginepb.MVCCStats{}, err
		}
		ms.Add(msDelta)
	}
	return ms, nil
}
