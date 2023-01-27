// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanstats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// Accessor mediates access to SpanStats by consumers across the KV/Tenant
// boundary.
type Accessor interface {
	// SpanStats returns the span stats between startKey and endKey.
	// The caller can request span stats from across the cluster by setting
	// nodeId to 0. The caller can request span stats from a specific node by
	// setting the value of nodeID accordingly.
	SpanStats(
		ctx context.Context,
		startKey,
		endKey roachpb.Key,
		nodeID roachpb.NodeID,
	) (*serverpb.InternalSpanStatsResponse, error)
}
