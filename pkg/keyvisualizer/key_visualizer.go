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

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// SpanStatsConsumer manages the tenant's periodic polling for,
// and processing of `spanstatspb.Sample`.
// It also manages the tenant's desired collection boundaries,
// and communicates this to KV.
type SpanStatsConsumer interface {
	// FetchStats will issue the `GetSamplesFromAllNodes` RPC to a KV node.
	// When it receives the response, it will perform downsampling,
	// data normalization, and ultimately write to the tenant's system table.
	FetchStats(start, end hlc.Timestamp) error

	// DecideBoundaries will issue the `UpdateBoundaries` RPC to a KV node.
	// It has the ability to decide 1) if new boundaries should be installed
	// and 2) what those new installed boundaries should be.
	DecideBoundaries()
}
