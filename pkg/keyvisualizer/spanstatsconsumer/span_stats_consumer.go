// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanstatsconsumer

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissettings"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvisstorage"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/spanstatskvaccessor"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// SpanStatsConsumer interacts with the key visualizer subsystem in KV and persists
// collected statistics to the tenant's system tables.
type SpanStatsConsumer struct {
	kvAccessor *spanstatskvaccessor.SpanStatsKVAccessor
	ri         *kvcoord.RangeIterator
	settings   *cluster.Settings
	ie         *sql.InternalExecutor
}

// New constructs a new SpanStatsConsumer.
func New(
	accessor *spanstatskvaccessor.SpanStatsKVAccessor,
	iterator *kvcoord.RangeIterator,
	settings *cluster.Settings,
	executor *sql.InternalExecutor,
) *SpanStatsConsumer {
	return &SpanStatsConsumer{
		kvAccessor: accessor,
		ri:         iterator,
		settings:   settings,
		ie:         executor,
	}
}

// UpdateBoundaries is part of the keyvisualizer.SpanStatsConsumer interface.
func (s *SpanStatsConsumer) UpdateBoundaries(ctx context.Context) error {
	boundaries, err := s.decideBoundaries(ctx)
	if err != nil {
		return err
	}
	updateTime := timeutil.Now().Add(10 * time.Second) // Arbitrary, but long enough for the payload to propagate to all nodes.
	_, err = s.kvAccessor.UpdateBoundaries(ctx, boundaries, updateTime)
	return err
}

// GetSamples is part of the keyvisualizer.SpanStatsConsumer interface.
func (s *SpanStatsConsumer) GetSamples(ctx context.Context) error {
	mostRecentSampleTime, err := keyvisstorage.MostRecentSampleTime(ctx, s.ie)
	if err != nil {
		return err
	}

	samplesRes, err := s.kvAccessor.GetSamples(ctx, mostRecentSampleTime)
	if err != nil {
		return err
	}

	return keyvisstorage.WriteSamples(ctx, s.ie, samplesRes.Samples)
}

// maybeAggregateBoundaries aggregates boundaries if len(boundaries) <= max.
func maybeAggregateBoundaries(boundaries []roachpb.Span, max int) ([]roachpb.Span, error) {
	if len(boundaries) <= max {
		return boundaries, nil
	}

	// combineFactor is the factor that the length of the boundaries slice is reduced by. For example,
	// if len(boundaries) == 1000, and max == 100, combineFactor == 10.
	// if len(boundaries) == 1001, and max == 100, combineFactor == 11.
	combineFactor := int(math.Ceil(float64(len(boundaries)) / float64(max)))
	combinedLength := int(math.Ceil(float64(len(boundaries)) / float64(combineFactor)))
	combined := make([]roachpb.Span, combinedLength)

	// Iterate through boundaries, incrementing by combineFactor.
	for i := 0; i < combinedLength; i++ {
		startIdx := i * combineFactor
		if startIdx >= len(boundaries) {
			return nil, errors.New("could not aggregate boundaries")
		}
		startSpan := boundaries[startIdx]
		endIndex := startIdx + combineFactor - 1
		if endIndex >= len(boundaries) {
			combined[i] = startSpan
		} else {
			combined[i] = startSpan.Combine(boundaries[endIndex])
		}
	}
	return combined, nil
}

// decideBoundaries decides the key spans that we want statistics
// for. For now, it will tell KV to collect statistics for all
// ranges from [Min, Max).
func (s *SpanStatsConsumer) decideBoundaries(ctx context.Context) ([]roachpb.Span, error) {
	var boundaries []roachpb.Span

	tenantSpan := roachpb.RSpan{
		Key:    roachpb.RKeyMin,
		EndKey: roachpb.RKeyMax,
	}

	s.ri.Seek(ctx, tenantSpan.Key, kvcoord.Ascending)

	for {
		if !s.ri.Valid() {
			return nil, s.ri.Error()
		}

		boundaries = append(boundaries, roachpb.Span{
			Key:    roachpb.Key(s.ri.Desc().StartKey),
			EndKey: roachpb.Key(s.ri.Desc().EndKey),
		})

		if !s.ri.NeedAnother(tenantSpan) {
			break
		}

		s.ri.Next(ctx)
	}

	maxBuckets := keyvissettings.MaxBuckets.Get(&s.settings.SV)
	return maybeAggregateBoundaries(boundaries, int(maxBuckets))
}

// DeleteExpiredSamples deletes historical samples older than 2 weeks.
func (s *SpanStatsConsumer) DeleteExpiredSamples(ctx context.Context) error {
	oneWeekAgo := timeutil.Now().AddDate(0, 0, -7)
	return keyvisstorage.DeleteSamplesBeforeTime(ctx, s.ie, oneWeekAgo)
}
