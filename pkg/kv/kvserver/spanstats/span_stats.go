// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanstats
//
//import (
//	"context"
//
//	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanstats/spanstatspb"
//	"github.com/cockroachdb/cockroach/pkg/roachpb"
//	"github.com/cockroachdb/cockroach/pkg/util/hlc"
//	"github.com/cockroachdb/cockroach/pkg/util/stop"
//)
//
//// BoundaryUpdate contains the boundaries that should be installed for a tenant
//type BoundaryUpdate struct {
//	roachpb.TenantID
//	boundaries []roachpb.Span
//}
//
//// Collector manages the collection of store-level requests
//// per tenant.
//type Collector interface {
//
//	// UpdateBoundaries will stash a tenant's desired collection boundaries to be
//	// installed at the beginning of the next sample period.
//	UpdateBoundaries(update BoundaryUpdate) error
//
//	// Increment will increase the counter for a tenant's collection boundary
//	// that matches `sp`
//	Increment(id roachpb.TenantID, sp roachpb.Span) error
//
//	// GetSamples will return previous samples collected on behalf of a tenant
//	// between `start` and `end` inclusive.
//	// The histogram for the current sample period will not be returned.
//	GetSamples(id roachpb.TenantID, start, end hlc.Timestamp) []spanstatspb.Sample
//}
//
//// BoundarySubscriber manages the rangefeed on desired tenant
//// collection boundaries.
//type BoundarySubscriber interface {
//	Start(ctx context.Context, s *stop.Stopper) error
//
//	// Subscribe installs the function to be called whenever the rangefeed
//	// produces a new event.
//	Subscribe(func(ctx context.Context, update BoundaryUpdate))
//}
