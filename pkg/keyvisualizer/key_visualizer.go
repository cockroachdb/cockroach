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
)

// KVAccessor provides a tenant with access to the keyvispb.KeyVisualizerServer.
type KVAccessor interface {

	// FlushSamples gets the tenant's collected statistics from KV. It
	// initiates a fan-out to all stores when FlushSamplesRequest.node_id is set
	// to 0. Otherwise, it returns samples obtained from stores on the desired
	// node. A FlushSamples call will reset the sample collection done in KV, and
	// in effect marks the end of a sample period, and the beginning of the
	// next one.
	//
	// Additionally, it provides the boundaries that the tenant wants
	// statistics collected for. Samples returned are for the boundaries sent
	// by the preceding FlushSamples call.
	FlushSamples(ctx context.Context, boundaries []*roachpb.Span) (*keyvispb.FlushSamplesResponse,	error)
}

// SpanStatsConsumer runs in the tenant key visualizer job.
// It is the tenant's API to control statistic collection in KV.
type SpanStatsConsumer interface {

	// FlushSamples requests samples from KV,
	// and provides collection boundaries for the subsequent sample.
	// After it receives samples, it downsamples them to an acceptable cardinality,
	// and persists them to the tenant's key visualizer system tables.
	FlushSamples(context.Context) error

	// DeleteOldestSamples deletes historical samples older than 2 weeks.
	DeleteOldestSamples(context.Context) error
}
