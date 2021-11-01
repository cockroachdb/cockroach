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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// KVAccessor mediates access to KV span configurations pertaining to a given
// tenant.
type KVAccessor interface {
	// GetSpanConfigEntriesFor returns the span configurations that overlap with
	// the given spans.
	GetSpanConfigEntriesFor(
		ctx context.Context,
		spans []roachpb.Span,
	) ([]roachpb.SpanConfigEntry, error)

	// UpdateSpanConfigEntries updates configurations for the given spans. This
	// is a "targeted" API: the spans being deleted are expected to have been
	// present with the exact same bounds; if spans are being updated with new
	// configs, they're expected to have been present with the same bounds. When
	// divvying up an existing span into multiple others with distinct configs,
	// callers are to issue a delete for the previous span and upserts for the
	// new ones.
	UpdateSpanConfigEntries(
		ctx context.Context,
		toDelete []roachpb.Span,
		toUpsert []roachpb.SpanConfigEntry,
	) error
}

// KVSubscriber presents a consistent[1] snapshot of a StoreReader that's
// incrementally maintained with changes made to the global span configurations
// state (system.span_configurations). The maintenance happens transparently;
// callers can subscribe to learn about what key spans may have seen a
// configuration change. After learning about a span update through a callback
// invocation, callers can consult the embedded StoreReader to retrieve an
// up-to-date[2] config for the updated span. The callback is called in a single
// goroutine; it should avoid doing long-running/blocking work.
//
// When the callback is first installed (there can only be one), it's invoked
// with the [min,max) span -- shorthand to indicate that callers should consult
// the StoreReader for all spans of interest. Subsequent updates are of the more
// incremental kind. It's possible that the span updates received are no-ops,
// i.e. consulting the StoreReader for the given span would retrieve the last
// config observed for the span[3].
//
// [1]: The contents of the StoreReader at t1 corresponds exactly to the
//      contents of the global span configuration state at t0 where t0 <= t1. If
//      the StoreReader is read from at t2 where t2 > t1, it's guaranteed to
//      observe a view of the global state at t >= t0.
// [2]: For the canonical KVSubscriber implementation, this is typically the
//      closed timestamp target duration.
// [3]: The canonical KVSubscriber implementation is bounced whenever errors
//      occur. This may result in the re-transmission of earlier updates
//      (usually through a lazily targeted [min,max) span).
type KVSubscriber interface {
	StoreReader
	OnSpanConfigUpdate(context.Context, func(updated roachpb.Span))
}

// SQLTranslator translates SQL descriptors and their corresponding zone
// configurations to constituent spans and span configurations.
//
// Concretely, for the following zone configuration hierarchy:
//
//    CREATE DATABASE db;
//    CREATE TABLE db.t1();
//    ALTER DATABASE db CONFIGURE ZONE USING num_replicas=7;
//    ALTER TABLE db.t1 CONFIGURE ZONE USING num_voters=5;
//
// The SQLTranslator produces the following translation (represented as a diff
// against RANGE DEFAULT for brevity):
//
// 		Table/5{3-4}                  num_replicas=7 num_voters=5
type SQLTranslator interface {
	// Translate generates the span configuration state given a list of
	// {descriptor, named zone} IDs. No entry is returned for an ID if it
	// doesn't exist or if it's dropped. The timestamp at which the translation
	// is valid is also returned.
	//
	// For every ID we first descend the zone configuration hierarchy with the
	// ID as the root to accumulate IDs of all leaf objects. Leaf objects are
	// tables and named zones (other than RANGE DEFAULT) which have actual span
	// configurations associated with them (as opposed to non-leaf nodes that
	// only serve to hold zone configurations for inheritance purposes). Then,
	// for each one of these accumulated IDs, we generate <span, span
	// config> tuples by following up the inheritance chain to fully hydrate the
	// span configuration. Translate also accounts for and negotiates subzone
	// spans.
	Translate(ctx context.Context, ids descpb.IDs) ([]roachpb.SpanConfigEntry, hlc.Timestamp, error)
}

// FullTranslate translates the entire SQL zone configuration state to the
// span configuration state. The timestamp at which such a translation is valid
// is also returned.
func FullTranslate(
	ctx context.Context, s SQLTranslator,
) ([]roachpb.SpanConfigEntry, hlc.Timestamp, error) {
	// As RANGE DEFAULT is the root of all zone configurations (including
	// other named zones for the system tenant), we can construct the entire
	// span configuration state by starting from RANGE DEFAULT.
	return s.Translate(ctx, descpb.IDs{keys.RootNamespaceID})
}

// ReconciliationDependencies captures what's needed by the span config
// reconciliation job to perform its task. The job is responsible for
// reconciling a tenant's zone configurations with the clusters span
// configurations.
type ReconciliationDependencies interface {
	KVAccessor

	SQLTranslator

	// TODO(arul): We'll also want access to a "SQLWatcher", something that
	// watches for changes to system.{descriptors, zones} to feed IDs to the
	// SQLTranslator. These interfaces will be used by the "Reconciler to perform
	// full/partial reconciliation, checkpoint the span config job, and update KV
	// with the tenants span config state.
}

// Store is a data structure used to store spans and their corresponding
// configs.
type Store interface {
	StoreWriter
	StoreReader
}

// StoreWriter is the write-only portion of the Store interface.
type StoreWriter interface {
	// Apply applies the given update[1]. It also returns the existing spans that
	// were deleted and entries that were newly added to make room for the
	// update. The deleted list can double as a list of overlapping spans in the
	// Store, provided the update is not a no-op[2].
	//
	// Span configs are stored in non-overlapping fashion. When an update
	// overlaps with existing configs, the existing configs are deleted. If the
	// overlap is only partial, the non-overlapping components of the existing
	// configs are re-added. If the update itself is adding an entry, that too
	// is added. This is best illustrated with the following example:
	//
	//                                         [--- X --) is a span with config X
	//
	//  Store    | [--- A ----)[------------- B -----------)[---------- C -----)
	//  Update   |             [------------------ D -------------)
	//           |
	//  Deleted  |             [------------- B -----------)[---------- C -----)
	//  Added    |             [------------------ D -------------)[--- C -----)
	//  Store*   | [--- A ----)[------------------ D -------------)[--- C -----)
	//
	// TODO(irfansharif): We'll make use of the dryrun option in a future PR
	// when wiring up the reconciliation job to use the KVAccessor. Since the
	// KVAccessor is a "targeted" API (the spans being deleted/upserted
	// have to already be present with the exact same bounds), we'll dryrun an
	// update against a StoreWriter (pre-populated with the entries present in
	// KV) to generate the targeted deletes and upserts we'd need to issue.
	// After successfully installing them in KV, we can keep our StoreWriter
	// up-to-date by actually applying the update.
	//
	// There's also the question of a "full reconciliation pass". We'll be
	// generating updates reactively listening in on changes to
	// system.{descriptor,zones} (see SQLWatcher). It's possible then for a
	// suspended tenant's table history to be GC-ed away and for its SQLWatcher
	// to never detect that a certain table/index/partition has been deleted.
	// Left as is, this results in us never issuing a corresponding span config
	// deletion request. We'd be leaving a bunch of delete-able span configs
	// lying around, and a bunch of empty ranges as a result of those. A "full
	// reconciliation pass" is our attempt to find all these extraneous entries
	// in KV and to delete them.
	//
	// We can use a StoreWriter here too (one that's pre-populated with the
	// contents of KVAccessor, as before). We'd iterate through all descriptors,
	// find all overlapping spans, issue KVAccessor deletes for them, and upsert
	// the descriptor's span config[3]. As for the StoreWriter itself, we'd
	// simply delete the overlapping entries. After iterating through all the
	// descriptors, we'd finally issue KVAccessor deletes for all span configs
	// still remaining in the Store.
	//
	// TODO(irfansharif): The descriptions above presume holding the entire set
	// of span configs in memory, but we could break away from that by adding
	// pagination + retrieval limit to the GetSpanConfigEntriesFor API. We'd
	// then paginate through chunks of the keyspace at a time, do a "full
	// reconciliation pass" over just that chunk, and continue.
	//
	// [1]: Unless dryrun is true. We'll still generate the same {deleted,added}
	//      lists.
	// [2]: We could alternatively expose a GetAllOverlapping() API to make
	//      things clearer.
	// [3]: We could skip the delete + upsert dance if the descriptor's exact
	//      span config entry already exists in KV. Using Apply (dryrun=true)
	//      against a StoreWriter (populated using KVAccessor contents) using
	//      the descriptor's span config entry would return empty lists,
	//      indicating a no-op.
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

// Update captures a span and the corresponding config change. It's the unit of
// what can be applied to a StoreWriter.
type Update struct {
	// Span captures the key span being updated.
	Span roachpb.Span

	// Config captures the span config the key span was updated to. An empty
	// config indicates the span config being deleted.
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
