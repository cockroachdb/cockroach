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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

// GetKeyVisualizerSamples implements the keyvisualizer.KVAccessor interface.
func (s *spanStatsKVAccessor) GetKeyVisualizerSamples(
	ctx context.Context, tenantID roachpb.TenantID, start hlc.Timestamp, end hlc.Timestamp,
) (*keyvispb.GetSamplesResponse, error) {

	req := &keyvispb.GetSamplesRequest{
		Tenant: &tenantID,
		NodeID: 0, // 0 indicates the server should begin a fan-out to all nodes.
	}

	return s.server.GetSamples(ctx, req)
}

// GetTenantRanges implements the keyvisualizer.KVAccessor interface.
func (s *spanStatsKVAccessor) GetTenantRanges(
	ctx context.Context, tenantID roachpb.TenantID,
) (*keyvispb.GetTenantRangesResponse, error) {

	return s.server.GetTenantRanges(ctx,
		&keyvispb.GetTenantRangesRequest{Tenant: &tenantID})
}

// UpdateBoundaries implements the keyvisualizer.KVAccessor interface.
func (s *spanStatsKVAccessor) UpdateBoundaries(
	ctx context.Context, tenantID roachpb.TenantID, boundaries []*roachpb.Span,
) (*keyvispb.SaveBoundariesResponse, error) {

	return s.server.SaveBoundaries(ctx, &keyvispb.SaveBoundariesRequest{
		Tenant:     &tenantID,
		Boundaries: boundaries,
	})
}
