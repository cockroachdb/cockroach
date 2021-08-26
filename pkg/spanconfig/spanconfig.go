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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// KVAccessor mediates access to KV span configurations pertaining to a given
// tenant.
type KVAccessor interface {
	// GetSpanConfigEntriesFor returns the span configurations that overlap with
	// the given spans.
	GetSpanConfigEntriesFor(ctx context.Context, spans []roachpb.Span) ([]roachpb.SpanConfigEntry, error)

	// UpdateSpanConfigEntries updates configurations for the given spans. This
	// is a "targeted" API: the spans being deleted are expected to have been
	// present with the exact same bounds; if spans are being updated with new
	// configs, they're expected to have been present with the same bounds. When
	// divvying up an existing span into multiple others with distinct configs,
	// callers are to issue a delete for the previous span and upserts for the
	// new ones.
	UpdateSpanConfigEntries(ctx context.Context, toDelete []roachpb.Span, toUpsert []roachpb.SpanConfigEntry) error
}

// ReconciliationDependencies captures what's needed by the span config
// reconciliation job to perform its task. The job is responsible for
// reconciling a tenant's zone configurations with the clusters span
// configurations.
type ReconciliationDependencies interface {
	KVAccessor

	// TODO(irfansharif): We'll also want access to a "SQLWatcher", something
	// that watches for changes to system.{descriptor,zones} and be responsible
	// for generating corresponding span config updates. Put together, the
	// reconciliation job will react to these updates by installing them into KV
	// through the KVAccessor.
}
