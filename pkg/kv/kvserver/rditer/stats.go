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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
)

// ComputeStatsForRange computes the stats for a given range by iterating over
// all key spans for the given range that should be accounted for in its stats.
func ComputeStatsForRange(
	ctx context.Context, d *roachpb.RangeDescriptor, reader storage.Reader, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsForRangeWithVisitors(
		ctx, d, reader, nowNanos, storage.ComputeStatsVisitors{})
}

func ComputeStatsForRangeUserOnly(
	ctx context.Context, d *roachpb.RangeDescriptor, reader storage.Reader, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsForRangeWithVisitorsUserOnly(
		ctx, d, reader, nowNanos, storage.ComputeStatsVisitors{})
}

func ComputeStatsForRangeExcludingUser(
	ctx context.Context, d *roachpb.RangeDescriptor, reader storage.Reader, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsForRangeWithVisitorsExcludingUser(
		ctx, d, reader, nowNanos, storage.ComputeStatsVisitors{})
}

func ComputeStatsForRangeWithVisitors(
	ctx context.Context,
	d *roachpb.RangeDescriptor,
	reader storage.Reader,
	nowNanos int64,
	visitors storage.ComputeStatsVisitors,
) (enginepb.MVCCStats, error) {
	return computeStatsForSpansWithVisitors(ctx, MakeReplicatedKeySpans(d), reader, nowNanos, visitors)
}

func ComputeStatsForRangeWithVisitorsUserOnly(
	ctx context.Context,
	d *roachpb.RangeDescriptor,
	reader storage.Reader,
	nowNanos int64,
	visitors storage.ComputeStatsVisitors,
) (enginepb.MVCCStats, error) {
	return computeStatsForSpansWithVisitors(ctx, MakeReplicatedKeySpansUserOnly(d), reader, nowNanos, visitors)
}

func ComputeStatsForRangeWithVisitorsExcludingUser(
	ctx context.Context,
	d *roachpb.RangeDescriptor,
	reader storage.Reader,
	nowNanos int64,
	visitors storage.ComputeStatsVisitors,
) (enginepb.MVCCStats, error) {
	return computeStatsForSpansWithVisitors(ctx, MakeReplicatedKeySpansExcludingUser(d), reader, nowNanos, visitors)
}

func computeStatsForSpansWithVisitors(
	ctx context.Context,
	spans []roachpb.Span,
	reader storage.Reader,
	nowNanos int64,
	visitors storage.ComputeStatsVisitors,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats
	for _, keySpan := range spans {
		msDelta, err := storage.ComputeStatsWithVisitors(
			ctx, reader, keySpan.Key, keySpan.EndKey, nowNanos, visitors)
		if err != nil {
			return enginepb.MVCCStats{}, err
		}
		ms.Add(msDelta)
	}
	return ms, nil
}
