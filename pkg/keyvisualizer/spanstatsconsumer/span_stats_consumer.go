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
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
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

// FlushSamples implements the keyvisualizer.SpanStatsConsumer interface.
func (s *SpanStatsConsumer) FlushSamples(ctx context.Context) error {

	boundaries, err := s.decideBoundaries(ctx)

	if err != nil {
		return err
	}

	sample, err := s.kvAccessor.FlushSamples(ctx, boundaries)

	if err != nil {
		return err
	}

	sample.Samples.SpanStats = downsample(sample.Samples.SpanStats, 512)

	// WriteSamples expects multiple samples, so honor that contract.
	samples := []*keyvispb.Sample{sample.Samples}
	return keyvisstorage.WriteSamples(ctx, s.ie, samples)
}

// decideBoundaries decides the key spans that this tenant wants statistics
// for. For now, it will tell KV to collect statistics over the tenant's own
// ranges.
func (s *SpanStatsConsumer) decideBoundaries(ctx context.Context) ([]*roachpb.
	Span, error) {

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
