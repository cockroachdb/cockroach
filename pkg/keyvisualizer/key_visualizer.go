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

import "context"

// SpanStatsConsumer runs in the tenant key visualizer job.
// It is the tenant's API to control statistic collection in KV.
type SpanStatsConsumer interface {
	UpdateBoundaries(context.Context) error
	GetSamples(context.Context) error

	// DeleteOldestSamples deletes historical samples older than 2 weeks.
	DeleteOldestSamples(context.Context) error
}
