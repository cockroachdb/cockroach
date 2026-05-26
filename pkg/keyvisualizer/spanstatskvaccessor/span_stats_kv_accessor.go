// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanstatskvaccessor

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// SpanStatsKVAccessor provides an API for co-located system tenant to interact with KV.
type SpanStatsKVAccessor struct {
	server keyvispb.KeyVisualizerServer
}

// New constructs a new SpanStatsKVAccessor.
func New(server keyvispb.KeyVisualizerServer) *SpanStatsKVAccessor {
	return &SpanStatsKVAccessor{server: server}
}

// UpdateBoundaries invokes the keyvispb.UpdateBoundaries rpc.
func (s *SpanStatsKVAccessor) UpdateBoundaries(
	ctx context.Context, boundaries []roachpb.Span, timestamp time.Time,
) (*keyvispb.UpdateBoundariesResponse, error) {

	return s.server.UpdateBoundaries(ctx, &keyvispb.UpdateBoundariesRequest{
		Boundaries: boundaries,
		Time:       timestamp,
	})

}

// GetSamples invokes the keyvispb.GetSamples rpc.
func (s *SpanStatsKVAccessor) GetSamples(
	ctx context.Context, timestamp time.Time,
) (*keyvispb.GetSamplesResponse, error) {

	return s.server.GetSamples(ctx, &keyvispb.GetSamplesRequest{
		NodeID:             0, // 0 indicates the server should be a fan-out.
		CollectedOnOrAfter: timestamp,
	})

}
