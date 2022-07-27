// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanstatsconsumer

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SpanStatsConsumer interacts with the key visualizer subsystem in KV and persists
// collected statistics to the tenant's system tables.
type SpanStatsConsumer struct {
	tenantID   roachpb.TenantID
	kvAccessor keyvisualizer.KVAccessor
	ie         *sql.InternalExecutor
}

var _ keyvisualizer.SpanStatsConsumer = &SpanStatsConsumer{}

// New constructs a new SpanStatsConsumer.
func New(
	tenantID roachpb.TenantID, accessor keyvisualizer.KVAccessor, executor *sql.InternalExecutor,
) *SpanStatsConsumer {
	return &SpanStatsConsumer{
		tenantID:   tenantID,
		kvAccessor: accessor,
		ie:         executor,
	}
}

// FetchStats requests span statistics from KV,
// downsamples them to an acceptable cardinality,
// and persists them to the tenant's key visualizer system tables.
func (s *SpanStatsConsumer) FetchStats(ctx context.Context) error {

	samples, err := s.kvAccessor.GetKeyVisualizerSamples(
		ctx,
		s.tenantID,
		hlc.Timestamp{},
		hlc.Timestamp{},
	)

	if err != nil {
		return err
	}

	for _, sample := range samples.Samples {
		sample.SpanStats = downsample(sample.SpanStats, 512)
	}

	return keyvisstorage.WriteSamples(ctx, s.ie, samples)
}

// DecideBoundaries tells KV the key spans that this tenant wants statistics
// for. For now, it will tell KV to collect statistics over the tenant's own
// ranges.
func (s *SpanStatsConsumer) DecideBoundaries(ctx context.Context) error {

	resp, err := s.kvAccessor.GetTenantRanges(ctx, s.tenantID)

	if err != nil {
		return err
	}

	_, err = s.kvAccessor.UpdateBoundaries(ctx, s.tenantID, resp.Ranges)

	return err
}

// DeleteOldestSamples deletes historical samples older than 2 weeks.
func (s *SpanStatsConsumer) DeleteOldestSamples(ctx context.Context) error {
	const TwoWeeks = time.Hour * 24 * 14
	watermark := timeutil.Now().Unix() - int64(TwoWeeks.Seconds())
	return keyvisstorage.DeleteSamplesOlderThanUnixTime(ctx, s.ie, watermark)
}
