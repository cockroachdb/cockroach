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

// KVWatcher emits KV span configuration updates.
type KVWatcher interface {
	WatchForKVUpdates(ctx context.Context) (<-chan Update, error)
}

// ReconciliationDependencies captures what's needed by the span config
// reconciliation job to perform its task. The job is responsible for
// reconciling a tenant's zone configurations with the clusters span
// configurations.
type ReconciliationDependencies interface {
	KVAccessor
	StoreWriter

	// TODO(irfansharif): We'll also want access to a "SQLWatcher", something
	// that watches for changes to system.{descriptor,zones} and be responsible
	// for generating corresponding span config updates. Put together, the
	// reconciliation job will react to these updates by installing them into KV
	// through the KVAccessor.
}

// Store is a data structure used to store spans and their corresponding
// configs.
type Store interface {
	StoreReader
	StoreWriter
}

// StoreWriter is the write-only portion of the Store interface.
type StoreWriter interface {
	// Apply
	// XXX: Document semantics. It applies the update and returns what spans
	// were deleted and what entries were then newly added (if the update
	// overlaps with existing entries, those entries are first deleted; if the
	// overlap is partial, non-overlapping bits are re-added). If dry run is
	// true, it does not actually execute the implied actions.
	//
	// This interface's relation to KVAccessor is a "look-aside" thing. We
	// consult this datastructure, figure out the diffs using the dry run mode,
	// and then persist it using the KVAccessor, and come back to re-apply those
	// diffs here. Can we simplify? If we forgot this dry run thing and wrapped
	// "around" a KVAccessor, we could do the diffing internally, do the
	// kvaccess, and apply. On the kvserver.Store side we wouldn't have this
	// diffing business, cause we'd be seeing a stream of events with the right
	// diffs. Sounds like two entirely different things -- though both backed by
	// a span config store. In SQL we should create a Store+KVAccessor object
	// that the job would use instead of doing everything out in the open.
	//
	// How does the full reconciliation work to spit out keys that need to
	// get deleted? Start off creating a span config store full of only what we
	// have in system.{descriptors,zones} -- "A". Do the same for whatever's in
	// KV -- "B".
	// Option 3. Collect both lists of spans (from kv and from sql), remove
	// duplicates from both lists. What's remaining in the KV list needs to be
	// deleted, what's remaining in sql list needs to be added.
	// Option 1. Iterate through both and accumulate extraneous entries, and
	// delete them.
	// Option 2. For all spans in "A", delete them from "B". If single delete
	// results in single delete+no add, good. Else ("corresponds to multiple sub
	// spans in KV, or part of larger in KV"): collect overlapping spans and add
	// them to delete list, and add what's we're trying to add to add list.
	// Whatever's remaining, delete in KV.
	// TL;DR - for each update on the full pass, try dry run update, if it
	// corresponds to single delete + add and they're identical (as in no
	// change), continue. If there's a diff, add all overlapping to delete set
	// and add the config we're diffing against to the upsert set.
	// Delete everything in the delete set, upsert everything in the upsert set.
	//
	// Option 4: Two lists, two span config stores. For everything in "A", try
	// update to "B". It'll delete + re-add some. Make those changes in KV. Then
	// do it again, except delete everything from "A" in (new) "B". What's
	// remaining in B is the left-over.
	// Option 5: Two sets, "A" and "B". For every span config in A, dry run an
	// update in B. Ignore the to-adds, that's just be our own span if there are
	// diffs. But accumulate all the deletes. Delete all the deletes, add all
	// the adds (everything in "A").
	//
	// Option 6: If we had a "get all overlapping for span" API. Two sets, "A"
	// and "B". For every span config in A, get all overlapping. If just one,
	// and same as what we're querying with, continue. If not, accumulate in
	// delete list, add our own to add list. At the end delete everything in
	// delete list from "B". Find what's remaining, and delete those too in KV.
	//
	// XXX: How do I paginate through some subset of results? Do we want to add
	// a limit to the 'get spans from kv' request? Say we had that, we would
	// then get the configs for some subset of keys, do our diff thing and
	// continue.
	Apply(ctx context.Context, update Update, dryrun bool) (
		deleted []roachpb.Span, added []roachpb.SpanConfigEntry,
	)
}

// StoreReader is the read-only portion of the Store interface. It doubles as an
// adaptor interface for config.SystemConfig.
type StoreReader interface {
	NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool
	ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey
	GetSpanConfigForKey(ctx context.Context, key roachpb.RKey) (roachpb.SpanConfig, error)
}

// Update captures what span has seen a config change. It's the unit of what a
// {SQL,KV}Watcher emits, and what can be applied to a StoreWriter.
type Update struct {
	// Span captures the key span that has been updated.
	Span roachpb.Span

	// Config captures the span config the key span was updated to. If the span
	// config was deleted, it's the empty config.
	Config roachpb.SpanConfig
}

// Deletion returns true if the update corresponds to a span config being
// deleted.
func (u Update) Deletion() bool {
	return u.Config.IsEmpty()
}

// Addition returns true if the update corresponds to a span config being
// added.
func (u Update) Addition() bool {
	return !u.Deletion()
}
