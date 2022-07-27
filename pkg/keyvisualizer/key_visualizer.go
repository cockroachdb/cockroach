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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// KVAccessor provides a tenant with access to the keyvispb.KeyVisualizerServer.
type KVAccessor interface {
	GetKeyVisualizerSamples(
		ctx context.Context,
		start hlc.Timestamp,
		end hlc.Timestamp,
	) (*keyvispb.GetSamplesResponse, error)

	UpdateBoundaries(
		ctx context.Context,
		boundaries []*roachpb.Span,
	) (*keyvispb.SaveBoundariesResponse, error)
}


// SpanStatsConsumer runs in the tenant keyvisualizer job.
// It is the tenant's API to control statistic collection in KV.
type SpanStatsConsumer interface {

	// FetchStats requests span statistics from KV,
	// downsamples them to an acceptable cardinality,
	// and persists them to the tenant's key visualizer system tables.
	FetchStats(ctx context.Context) error

	// DecideBoundaries tells KV the key spans that this tenant wants statistics
	// for. For now, it will tell KV to collect statistics over the tenant's own
	// ranges.
	DecideBoundaries(ctx context.Context) error

	// DeleteOldestSamples deletes historical samples older than 2 weeks.
 	DeleteOldestSamples(ctx context.Context) error
}
