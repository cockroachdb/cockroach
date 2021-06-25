// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Accessor mediates access to the subset of the cluster's span configs
// applicable to a given tenant.
//
// Implementations are expected to be thread safe.
type Accessor interface {
	// GetSpanConfigsFor retrieves the span configurations over the requested
	// span.
	GetSpanConfigsFor(ctx context.Context, span roachpb.Span) ([]roachpb.SpanConfigEntry, error)

	// UpdateSpanConfigEntries updates the span configurations over the given
	// spans.
	UpdateSpanConfigEntries(ctx context.Context, update []roachpb.SpanConfigEntry, delete []roachpb.Span) error
}

// Update is the the unit of what the span config watcher emits.
type Update struct {
	// Entry captures the keyspan and corresponding config that has been
	// updated. If deleted is false, the embedded config is what the keyspan was
	// updated with.
	Entry roachpb.SpanConfigEntry

	// Deleted is true if the span config entry has been deleted.
	Deleted bool
}

func (e *Update) String() string {
	return fmt.Sprintf("span config update: span=%s/deleted=%t", e.Entry.Span, e.Deleted)
}

// TestingKnobs provide fine-grained control over the span config manager for
// testing.
type TestingKnobs struct {
	PreJobCreationInterceptor func()
}

// ModuleTestingKnobs makes TestingKnobs a base.ModuleTestingKnobs.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
