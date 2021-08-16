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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// KVAccessor mediates access to KV span configurations pertaining to a given
// tenant.
type KVAccessor interface {
	// GetSpanConfigEntriesFor retrieves the span configurations over the
	// requested span.
	GetSpanConfigEntriesFor(ctx context.Context, span roachpb.Span) ([]roachpb.SpanConfigEntry, error)

	// UpdateSpanConfigEntries updates the span configurations over the given
	// spans.
	UpdateSpanConfigEntries(ctx context.Context, update []roachpb.SpanConfigEntry, delete []roachpb.Span) error
}

// KVWatcher emits KV span configuration updates.
type KVWatcher interface {
	WatchForKVUpdates(ctx context.Context, stopper *stop.Stopper) (<-chan Update, error)
}

// SQLWatcher emits SQL span configuration updates.
type SQLWatcher interface {
	WatchForSQLUpdates(ctx context.Context) (<-chan Update, error)
}

// ReconciliationDependencies captures what's needed by the span config
// reconciliation process. The reconciliation process reconciles a tenant's span
// configs with the cluster's.
type ReconciliationDependencies interface {
	KVAccessor
	SQLWatcher
}

// Update captures what span has seen a config change. It's the unit of what a
// {SQL,KV}Watcher emits.
type Update struct {
	// Entry captures the keyspan and the corresponding config that has been
	// updated. If deleted is true, the config over that span has been deleted
	// (those keys no longer exist) -- the embedded config captures the last
	// config. If false, the embedded config is what the keyspan was updated to.
	Entry roachpb.SpanConfigEntry

	// Deleted is true if the span config entry has been deleted.
	Deleted bool
}

// Store is a data structure used to store span configs.
type Store interface {
	StoreReader
	StoreWriter
}

// StoreWriter is the write-only portion of the Store interface.
type StoreWriter interface {
	Apply(update Update) // TODO(zcfgs-pod): Could instead split into into two methods -- delete and update.
}

// StoreReader is the read-only portion of the Store interface. It's an adaptor
// interface implemented by both config.SystemConfig and spanconfig.Store.
type StoreReader interface {
	NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool
	ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey
	GetSpanConfigForKey(ctx context.Context, key roachpb.RKey) (roachpb.SpanConfig, error)
}

// TODO(zcfgs-pod): In the restore path we need to make sure we do a full
// reconciliation pass pre-restore (?). Was mentioned in some pod meeting.

// TODO(zcfgs-pod): De-dup away sql updates by maintaining a similar
// spanconfig.Store on the tenant/SQL watcher. Alternatively, fetch span configs
// from KV each time to compare against.
