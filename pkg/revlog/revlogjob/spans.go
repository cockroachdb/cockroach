// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// SpanResolver returns the spans the log should currently cover. The
// implementation owns descriptor access — it is not handed a
// descriptor set, only the time at which to resolve and an optional
// hint about which descriptors recently changed.
//
// The log job calls this once at startup (with changedDescIDs=nil) to
// learn its initial scope, and (in a later iteration of the
// coordinator) again whenever its descriptor rangefeed reports a
// change. Implementations are typically a closure over the parent
// BACKUP's target spec (cluster / database list / table list /
// tenant) plus access to descriptor storage; resolving each call
// from live state keeps the log honest as schema evolves under it.
//
// changedDescIDs is a hint: when non-empty, the implementation may
// short-circuit if none of the listed IDs intersect its target spec.
// nil/empty means "compute unconditionally" — used at startup and any
// time the prior result is suspect (e.g. on resume).
//
// The returned spans must be authoritative for asOf. Callers diff
// against the prior result to decide whether the span set actually
// changed; an implementation that always recomputes is correct, just
// slower than one that uses the hint to skip work.
type DescSpanResolver func(
	ctx context.Context,
	asOf hlc.Timestamp,
	changedDescIDs []catid.DescID,
) ([]roachpb.Span, error)
