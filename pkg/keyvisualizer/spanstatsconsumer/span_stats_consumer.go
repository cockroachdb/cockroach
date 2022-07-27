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
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisstorage"
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
	kvAccessor keyvisualizer.KVAccessor
	distSender *kvcoord.DistSender
	ie         *sql.InternalExecutor
}

var _ keyvisualizer.SpanStatsConsumer = &SpanStatsConsumer{}

// New constructs a new SpanStatsConsumer.
func New(
	tenantID roachpb.TenantID,
	accessor keyvisualizer.KVAccessor,
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

// FetchStats requests span statistics from KV,
// downsamples them to an acceptable cardinality,
// and persists them to the tenant's key visualizer system tables.
func (s *SpanStatsConsumer) FetchStats(ctx context.Context) error {

	samples, err := s.kvAccessor.GetKeyVisualizerSamples(
		ctx,
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

	boundaries := make([]*roachpb.Span, 0)

	tenantPrefix := keys.MakeTenantPrefix(s.tenantID)
	tenantSpan := roachpb.RSpan{
		Key:    roachpb.RKey(tenantPrefix),
		EndKey: roachpb.RKey(tenantPrefix.PrefixEnd()),
	}

	// TODO(zachlite): We'll need higher resolution for secondary tenants,
	// because a tenant's keyspace tends to exist within a single range.
	ri := kvcoord.MakeRangeIterator(s.distSender)
	ri.Seek(ctx, tenantSpan.Key, kvcoord.Ascending)

	for  {
		if !ri.Valid() {
			return ri.Error()
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

	_, err := s.kvAccessor.UpdateBoundaries(ctx, boundaries)
	return err
}

// DeleteOldestSamples deletes historical samples older than 2 weeks.
func (s *SpanStatsConsumer) DeleteOldestSamples(ctx context.Context) error {
	const TwoWeeks = time.Hour * 24 * 14
	watermark := timeutil.Now().Unix() - int64(TwoWeeks.Seconds())
	return keyvisstorage.DeleteSamplesOlderThanUnixTime(ctx, s.ie, watermark)
}
