// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keyvisualizer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanstats/spanstatspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// KVAccessor provides a tenant with access to the keyvispb.KeyVisualizerServer
type KVAccessor interface {
	GetKeyVisualizerSamples(
		ctx context.Context,
		tenantID roachpb.TenantID,
		start hlc.Timestamp,
		end hlc.Timestamp,
	) (*keyvispb.GetSamplesResponse, error)

	GetTenantRanges(
		ctx context.Context,
		tenantID roachpb.TenantID) (*keyvispb.GetTenantRangesResponse, error)

	UpdateBoundaries(
		ctx context.Context,
		tenantID roachpb.TenantID,
		boundaries []*roachpb.Span,
	) (*keyvispb.SaveBoundariesResponse, error)
}

// Why is this an interface? Is there more than one?

// SpanStatsCollector collects request statistics for tenants.
// SpanStatsCollector will only collect statistics for a tenant after the
// tenant has called an implementation of KVAccessor.UpdateBoundaries.
type SpanStatsCollector interface {

	// Increment tracks the number of requests for a given tenant and span.
	// An error is returned if the tenant has not previously called
	// KVAccessor.UpdateBoundaries.
	Increment(id roachpb.TenantID, span roachpb.Span) error

	// GetSamples returns the requests for a given tenant.
	// An error is returned if  the tenant has not previously called
	// KVAccessor.UpdateBoundaries.
	GetSamples(id roachpb.TenantID) ([]spanstatspb.Sample, error)

	SaveBoundaries(id roachpb.TenantID, boundaries []*roachpb.Span)
}

// SpanStatsConsumer runs in the tenant keyvisualizer job.
// It is the tenant's API to control statistic collection in KV.
type SpanStatsConsumer interface {

	// FetchStats issues keyvispb.GetSamplesFromAllNodes
	// to a KV node via KVAccessor.GetKeyVisualizerSamples.
	// When it receives the response, it will perform downsampling,
	// normalization, and ultimately write the sample to the tenant's system tables.
	FetchStats(ctx context.Context) error

	// DecideBoundaries decides which spans KV should collect statistics for.
	// After deciding, it communicates this to KV via KVAccessor.UpdateBoundaries.
	DecideBoundaries(ctx context.Context) error

	// DeleteOldestSamples will delete samples that are older than 2 weeks.
	DeleteOldestSamples(ctx context.Context) error
}
