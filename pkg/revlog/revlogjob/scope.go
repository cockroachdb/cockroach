// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Scope is the seam between the revlog job and the parent
// BACKUP's notion of "what should I cover." Implementations live
// in pkg/backup, which already imports revlogjob for the resumer
// fork — keeping the dep direction one-way.
type Scope interface {
	// Matches reports whether a descriptor change is in-scope.
	// Drives the rangefeed-event filter: schema-delta and
	// coverage writes only fire on matches.
	Matches(desc *descpb.Descriptor) bool

	// Spans resolves the current span set at asOf. Callers diff
	// successive results to decide whether to write a new
	// coverage entry.
	Spans(ctx context.Context, asOf hlc.Timestamp) ([]roachpb.Span, error)

	// String is the human-readable scope spec embedded in
	// Coverage entries for inspection.
	String() string

	// Terminated reports whether every root in the scope is in
	// DROP or gone from KV at asOf. When true, the writer exits
	// successfully.
	Terminated(ctx context.Context, asOf hlc.Timestamp) (bool, error)
}
