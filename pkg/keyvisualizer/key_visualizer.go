// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvisualizer

import "context"

// SpanStatsConsumer runs in the tenant key visualizer job.
// It is the tenant's API to control statistic collection in KV.
type SpanStatsConsumer interface {

	// UpdateBoundaries decides the boundaries that statistics should be collected for, and communicates them to KV.
	// Updates are applied on every node after the following events occur:
	// 1) The update payload propagates to collectors.
	// 2) The collector rolls over its current sample and the boundary update time is in the past.
	// The timestamp helps us make sure that rangefeed propagation delays don't result in collectors
	// installing different boundaries.
	UpdateBoundaries(context.Context) error

	// GetSamples retrieves the latest samples from KV, and then downsamples and persists them.
	GetSamples(context.Context) error

	// DeleteExpiredSamples deletes historical samples older than 2 weeks.
	DeleteExpiredSamples(context.Context) error
}
