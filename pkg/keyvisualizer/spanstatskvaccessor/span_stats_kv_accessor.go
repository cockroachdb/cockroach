// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanstatskvaccessor

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// spanStatsKVAccessor provides an API for co-located system tenant to interact with KV.
// It's a concrete implementation of the keyvisualizer.KVAccessor interface.
type spanStatsKVAccessor struct {
	server keyvispb.KeyVisualizerServer
}

// New constructs a new spanStatsKVAccessor.
func New(server keyvispb.KeyVisualizerServer) keyvisualizer.KVAccessor {
	return &spanStatsKVAccessor{server: server}
}

// FlushSamples implements the keyvisualizer.KVAccessor interface.
func (s *spanStatsKVAccessor) FlushSamples(
	ctx context.Context, boundaries []*roachpb.Span,
) (*keyvispb.FlushSamplesResponse, error) {

	req := &keyvispb.FlushSamplesRequest{
		NodeID: 0, // 0 indicates the server should begin a fan-out to all nodes.
		Boundaries: boundaries,
	}

	return s.server.FlushSamples(ctx, req)
}
