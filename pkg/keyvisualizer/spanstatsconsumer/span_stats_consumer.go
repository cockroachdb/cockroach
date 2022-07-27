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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisstorage"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/spanstatskvaccessor"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SpanStatsConsumer interacts with the key visualizer subsystem in KV and persists
// collected statistics to the tenant's system tables.
type SpanStatsConsumer struct {
	tenantID   roachpb.TenantID
	kvAccessor *spanstatskvaccessor.SpanStatsKVAccessor
	distSender *kvcoord.DistSender
	ie         *sql.InternalExecutor
}

// New constructs a new SpanStatsConsumer.
func New(
	tenantID roachpb.TenantID,
	accessor *spanstatskvaccessor.SpanStatsKVAccessor,
	distSender *kvcoord.DistSender,
	executor *sql.InternalExecutor,
) *SpanStatsConsumer {
	return &SpanStatsConsumer{
		tenantID:   tenantID,
		kvAccessor: accessor,
		distSender: distSender,
		ie:         executor,
	}
}

func (s *SpanStatsConsumer) UpdateBoundaries(ctx context.Context) error {
	boundaries, err := s.decideBoundaries(ctx)
	if err != nil {
		return err
	}
	_, err = s.kvAccessor.UpdateBoundaries(ctx, boundaries, &hlc.Timestamp{})
	return err
}

func (s *SpanStatsConsumer) GetSamples(ctx context.Context) error {
	mostRecentSampleTime, err := keyvisstorage.MostRecentSampleTime(ctx, s.ie)
	if err != nil {
		return err
	}

	samplesRes, err := s.kvAccessor.GetSamples(ctx, mostRecentSampleTime)
	if err != nil {
		return err
	}

	for _, sample := range samplesRes.Samples {
		sample.SpanStats = downsample(sample.SpanStats, 512 /* max buckets */)
	}

	return keyvisstorage.WriteSamples(ctx, s.ie, samplesRes.Samples)
}

// decideBoundaries decides the key spans that we want statistics
// for. For now, it will tell KV to collect statistics over [Min, Max).
func (s *SpanStatsConsumer) decideBoundaries(ctx context.Context) ([]*roachpb.Span, error) {
	boundaries := make([]*roachpb.Span, 0)

	tenantPrefix := keys.MakeTenantPrefix(s.tenantID)
	tenantSpan := roachpb.RSpan{
		Key:    roachpb.RKey(tenantPrefix),
		EndKey: roachpb.RKey(tenantPrefix.PrefixEnd()),
	}

	ri := kvcoord.MakeRangeIterator(s.distSender)
	ri.Seek(ctx, tenantSpan.Key, kvcoord.Ascending)

	for {
		if !ri.Valid() {
			return nil, ri.Error()
		}

		boundaries = append(boundaries, &roachpb.Span{
			Key:    roachpb.Key(ri.Desc().StartKey),
			EndKey: roachpb.Key(ri.Desc().EndKey),
		})

		if !ri.NeedAnother(tenantSpan) {
			break
		}

		ri.Next(ctx)
	}

	return boundaries, nil
}

// DeleteOldestSamples deletes historical samples older than 2 weeks.
func (s *SpanStatsConsumer) DeleteOldestSamples(ctx context.Context) error {
	const TwoWeeks = time.Hour * 24 * 14
	watermark := timeutil.Now().Unix() - int64(TwoWeeks.Seconds())
	return keyvisstorage.DeleteSamplesOlderThanUnixTime(ctx, s.ie, watermark)
}
