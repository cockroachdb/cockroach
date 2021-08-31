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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

// SQLWatcher can be used to incrementally process zone configuration updates in
// the SQL layer.
type SQLWatcher interface {
	// WatchForSQLUpdates watches for SQL layer updates, starting from the given
	// timestamp, that may imply a change to the span configuration state.
	// Descriptor IDs that may imply a change are passed to the callback handler
	// along with a checkpoint timestamp. The checkpoint timestamp guarantees that
	// all IDs in the window [previous checkpoint timestamp, checkpoint timestamp]
	// that could have implied a span configuration changes have been passed to
	// the callback. The very first time the callback is invoked previous
	// checkpoint timestmap is simply the start timestamp.
	WatchForSQLUpdates(ctx context.Context, startTS hlc.Timestamp, f SQLWatcherHandleFunc) error
}

type SQLWatcherHandleFunc func(ids descpb.IDs, checkpointTS hlc.Timestamp) error

// SQLReconciler reconciles SQL descriptors and their corresponding zone
// configurations to span configurations. The SQLReconciler merely constructs
// the implied span configuration state from SQL's perspective -- it is agnostic
// to the actual span configuration state in KV.
type SQLReconciler interface {
	// Reconcile generates the implied span configuration state given a list of
	// {descriptor, named zone} IDs.
	Reconcile(ctx context.Context, ids descpb.IDs) ([]Update, error)
	// FullReconcile performs a full reconciliation of the SQL zone configuration
	// state to the implied span configuration state. The implied span
	// configuration state is agnostic of the actual span configuration state. The
	// timestamp at which the full reconciliation occurred is returned.
	FullReconcile(ctx context.Context) ([]Update, hlc.Timestamp, error)
}

// ReconciliationDependencies captures what's needed by the span config
// reconciliation job to perform its task. The job is responsible for
// reconciling a tenant's zone configurations with the clusters span
// configurations.
type ReconciliationDependencies interface {
	KVAccessor

	SQLWatcher

	SQLReconciler
}

// Update captures what span has seen a config change. It's the unit of what a
// {SQL,KV}Watcher emits.
type Update struct {
	// Entry captures the keyspan and the corresponding config that has been
	// updated. If deleted is true, the config over that span has been deleted
	// (those keys no longer exist). If false, the embedded config is what the
	// keyspan was updated to.
	Entry roachpb.SpanConfigEntry
	// Deleted is true if the span config entry has been deleted.
	Deleted bool
}

// Store is a data structure used to store span configs.
type Store interface {
	StoreReader

	// TODO(irfansharif): We'll want to add a StoreWriter interface here once we
	// implement a data structure to store span configs. We expect this data
	// structure to be used in KV to eventually replace the use of the
	// gossip-backed system config span.
}

// Silence the unused linter.
var _ Store = nil

// StoreReader is the read-only portion of the Store interface. It's an adaptor
// interface implemented by config.SystemConfig to let us later swap out the
// source with one backed by a view of `system.span_configurations`.
type StoreReader interface {
	NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool
	ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey
	GetSpanConfigForKey(ctx context.Context, key roachpb.RKey) (roachpb.SpanConfig, error)
}
