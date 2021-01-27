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

// ComputeStatsForRange computes the stats for a given range by
// iterating over all key ranges for the given range that should
// be accounted for in its stats.
func ComputeStatsForRange(
	d *roachpb.RangeDescriptor, reader storage.Reader, nowNanos int64,
) (enginepb.MVCCStats, error) {
	ms := enginepb.MVCCStats{}
	var err error
	for _, keyRange := range MakeReplicatedKeyRangesExceptLockTable(d) {
		func() {
			iter := reader.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind,
				storage.IterOptions{UpperBound: keyRange.End.Key})
			defer iter.Close()

			var msDelta enginepb.MVCCStats
			if msDelta, err = iter.ComputeStats(keyRange.Start.Key, keyRange.End.Key, nowNanos); err != nil {
				return
			}
			ms.Add(msDelta)
		}()
		if err != nil {
			return enginepb.MVCCStats{}, err
		}
	}
	return ms, nil
}
