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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

// ComputeStatsForRange computes the stats for a given range by
// iterating over all key ranges for the given range that should
// be accounted for in its stats.
func ComputeStatsForRange(
	d *roachpb.RangeDescriptor, e engine.Reader, nowNanos int64,
) (enginepb.MVCCStats, error) {
	iter := e.NewIterator(engine.IterOptions{UpperBound: d.EndKey.AsRawKey()})
	defer iter.Close()

	ms := enginepb.MVCCStats{}
	for _, keyRange := range MakeReplicatedKeyRanges(d) {
		msDelta, err := iter.ComputeStats(keyRange.Start, keyRange.End, nowNanos)
		if err != nil {
			return enginepb.MVCCStats{}, err
		}
		ms.Add(msDelta)
	}
	return ms, nil
}
