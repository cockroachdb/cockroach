// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Reconciler is a concrete implementation of the spanconfig.Reconciler
// interface.
type Reconciler struct {
	sqlWatcher    spanconfig.SQLWatcher
	sqlTranslator spanconfig.SQLTranslator
	kvAccessor    spanconfig.KVAccessor

	execCfg *sql.ExecutorConfig
	codec   keys.SQLCodec
	tenID   roachpb.TenantID
	knobs   *spanconfig.TestingKnobs

	mu struct {
		syncutil.RWMutex
		lastCheckpoint hlc.Timestamp
	}
}

var _ spanconfig.Reconciler = &Reconciler{}

// New constructs a new Reconciler.
func New(
	sqlWatcher spanconfig.SQLWatcher,
	sqlTranslator spanconfig.SQLTranslator,
	kvAccessor spanconfig.KVAccessor,
	execCfg *sql.ExecutorConfig,
	codec keys.SQLCodec,
	tenID roachpb.TenantID,
	knobs *spanconfig.TestingKnobs,
) *Reconciler {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Reconciler{
		sqlWatcher:    sqlWatcher,
		sqlTranslator: sqlTranslator,
		kvAccessor:    kvAccessor,

		execCfg: execCfg,
		codec:   codec,
		tenID:   tenID,
		knobs:   knobs,
	}
}

// Reconcile is part of the spanconfig.Reconciler interface; it's responsible
// for reconciling a tenant's zone configs with the cluster's span configs (KV
// construct). It does so incrementally and continuously, internally leveraging
// SQL{Watcher,Translator}, KVAccessor, and Store to make progress.
//
// Let's walk through what it does. At a high-level, we maintain an in-memory
// data structure that's up-to-date with the contents of the KV (for the
// subset of spans we have access to, i.e. the keyspace carved out for our
// tenant ID, host or otherwise). We watch for changes to SQL state
// (descriptors, zone configs), translate the SQL updates to the flattened
// span+config form, "diff" the updates against the data structure to see if
// there are any changes we need to inform KV of. If so, we do, and ensure that
// the data structure is kept up-to-date. We continue watching for future
// updates, repeating as necessary.
//
// There's a single instance of the Reconciler running for a given tenant at a
// given point it time (mutual exclusion/leasing is provided by the jobs
// subsystem and the span config manager). We needn't worry about contending
// writers, or the KV state being changed from underneath us. What we do have to
// worry about, however, is suspended tenants' not being reconciling while
// suspended. It's possible for a suspended tenant's SQL state to be GC-ed away
// at older MVCC timestamps; when watching for changes, we could fail to observe
// tables/indexes/partitions getting deleted[1]. Left as is, this would result
// in us never issuing a corresponding deletion requests for the dropped span
// configs -- we'd be leaving orphaned span configs lying around (taking up
// storage space and creating pointless empty ranges). A "full reconciliation
// pass" is our attempt to find all these extraneous entries in KV and to delete
// them.
//
// We can use our span config data structure here too, one that's pre-populated
// with the contents of KV. We translate the entire SQL state into constituent
// spans and configs, diff against our data structure to generate KV updates
// that we then apply. We follow this with clearing out all these spans in our
// data structure, leaving behind all extraneous entries to be found in KV --
// entries we can then simply issue deletes for.
//
// [1]: #73399 proposes a new KV request type that would let us more rapidly
//      trigger reconciliation after a tenant's SQL transaction. If we're able
//      to do this fast enough, it would be reasonable to wait for
//      reconciliation to happen before returning to the client. We could
//      alternatively use it as part of a handshake protocol during pod
//      suspension, to ensure that all outstanding work ("reconciliation" has
//      been done before a pod is suspended.
//
// TODO(irfansharif): The descriptions above presume holding the entire set of
// span configs in memory, but we could break away from that by adding
// pagination + retrieval limit to the GetSpanConfigEntriesFor API. We'd then
// paginate through chunks of the keyspace at a time, do a "full reconciliation
// pass" over just that chunk, and continue.
//
// TODO(irfansharif): We probably want some degree of batching when issuing RPCs
// to KV -- right now we sent forth the entire set of updates since our last
// checkpoint. For changes to, say, RANGE DEFAULT, the RPC request proto is
// proportional to the number of schema objects.
func (r *Reconciler) Reconcile(
	ctx context.Context, startTS hlc.Timestamp, onCheckpoint func() error,
) error {
	// TODO(irfansharif): Check system.{zones,descriptors} for last GC timestamp
	// and avoid the full reconciliation pass if the startTS provided is
	// visible to the rangefeed. Right now we're doing a full reconciliation
	// pass every time the reconciliation job kicks us off.
	_ = startTS

	if fn := r.knobs.ReconcilerInitialInterceptor; fn != nil {
		fn()
	}

	full := fullReconciler{
		sqlTranslator: r.sqlTranslator,
		kvAccessor:    r.kvAccessor,
		codec:         r.codec,
		tenID:         r.tenID,
		knobs:         r.knobs,
	}
	latestStore, reconciledUpUntil, err := full.reconcile(ctx)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.mu.lastCheckpoint = reconciledUpUntil
	r.mu.Unlock()

	if err := onCheckpoint(); err != nil {
		return err
	}

	incrementalStartTS := reconciledUpUntil
	incremental := incrementalReconciler{
		sqlTranslator:       r.sqlTranslator,
		sqlWatcher:          r.sqlWatcher,
		kvAccessor:          r.kvAccessor,
		storeWithKVContents: latestStore,
		execCfg:             r.execCfg,
		codec:               r.codec,
		knobs:               r.knobs,
	}
	return incremental.reconcile(ctx, incrementalStartTS, func(reconciledUpUntil hlc.Timestamp) error {
		r.mu.Lock()
		r.mu.lastCheckpoint = reconciledUpUntil
		r.mu.Unlock()

		return onCheckpoint()
	})
}

// Checkpoint is part of the spanconfig.Reconciler interface.
func (r *Reconciler) Checkpoint() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.mu.lastCheckpoint
}

// fullReconciler is a single-use orchestrator for the full reconciliation
// process.
type fullReconciler struct {
	sqlTranslator spanconfig.SQLTranslator
	kvAccessor    spanconfig.KVAccessor

	codec keys.SQLCodec
	tenID roachpb.TenantID
	knobs *spanconfig.TestingKnobs
}

// reconcile runs the full reconciliation process, returning:
// - a store with the latest set of span configs under our purview;
// - the timestamp we've reconciled up until.
func (f *fullReconciler) reconcile(
	ctx context.Context,
) (storeWithLatestSpanConfigs *spanconfigstore.Store, reconciledUpUntil hlc.Timestamp, _ error) {
	storeWithExistingSpanConfigs, err := f.fetchExistingSpanConfigs(ctx)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}

	// Translate the entire SQL state to ensure KV reflects the most up-to-date
	// view of things.
	var entries []roachpb.SpanConfigEntry
	entries, reconciledUpUntil, err = spanconfig.FullTranslate(ctx, f.sqlTranslator)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}

	updates := make([]spanconfig.Update, len(entries))
	for i, entry := range entries {
		updates[i] = spanconfig.Update(entry)
	}

	toDelete, toUpsert := storeWithExistingSpanConfigs.Apply(ctx, false /* dryrun */, updates...)
	if len(toDelete) != 0 || len(toUpsert) != 0 {
		if err := f.kvAccessor.UpdateSpanConfigEntries(ctx, toDelete, toUpsert); err != nil {
			return nil, hlc.Timestamp{}, err
		}
	}

	// Keep a copy of the current view of the world (i.e. KVAccessor
	// contents). We could also fetch everything from KV, but making a copy here
	// is cheaper (and saves an RTT). We'll later mutate
	// storeWithExistingSpanConfigs to determine what extraneous entries are in
	// KV, in order to delete them. After doing so, we'll issue those same
	// deletions against this copy in order for it to reflect an up-to-date view
	// of span configs.
	storeWithLatestSpanConfigs = storeWithExistingSpanConfigs.Copy(ctx)

	// Delete all updated spans in a store populated with all current entries.
	// Because our translation above captures the entire SQL state, deleting all
	// "updates" will leave behind only the extraneous entries in KV -- we'll
	// get rid of them below.
	var storeWithExtraneousSpanConfigs *spanconfigstore.Store
	{
		for _, u := range updates {
			storeWithExistingSpanConfigs.Apply(ctx, false /* dryrun */, spanconfig.Deletion(u.Span))
		}
		storeWithExtraneousSpanConfigs = storeWithExistingSpanConfigs
	}

	deletedSpans, err := f.deleteExtraneousSpanConfigs(ctx, storeWithExtraneousSpanConfigs)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}

	// Update the store that's supposed to reflect the latest span config
	// contents. As before, we could've fetched this state from KV directly, but
	// doing it this way is cheaper.
	for _, d := range deletedSpans {
		storeWithLatestSpanConfigs.Apply(ctx, false /* dryrun */, spanconfig.Deletion(d))
	}

	return storeWithLatestSpanConfigs, reconciledUpUntil, nil
}

// fetchExistingSpanConfigs returns a store populated with all span configs
// under our purview.
func (f *fullReconciler) fetchExistingSpanConfigs(
	ctx context.Context,
) (*spanconfigstore.Store, error) {
	var tenantSpan roachpb.Span
	if f.codec.ForSystemTenant() {
		// The system tenant governs all system keys (meta, liveness, timeseries
		// ranges, etc.) and system tenant tables.
		//
		// TODO(irfansharif): Should we include the scratch range here? Some
		// tests make use of it; we may want to declare configs over it and have
		// it considered all the same.
		tenantSpan = roachpb.Span{
			Key:    keys.EverythingSpan.Key,
			EndKey: keys.TableDataMax,
		}
		if f.knobs.ConfigureScratchRange {
			tenantSpan.EndKey = keys.ScratchRangeMax
		}
	} else {
		// Secondary tenants govern everything prefixed by their tenant ID.
		tenPrefix := keys.MakeTenantPrefix(f.tenID)
		tenantSpan = roachpb.Span{
			Key:    tenPrefix,
			EndKey: tenPrefix.PrefixEnd(),
		}
	}

	store := spanconfigstore.New(roachpb.SpanConfig{})
	{
		// Fully populate the store with KVAccessor contents.
		entries, err := f.kvAccessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{
			tenantSpan,
		})
		if err != nil {
			return nil, err
		}

		for _, entry := range entries {
			store.Apply(ctx, false /* dryrun */, spanconfig.Update(entry))
		}
	}
	return store, nil
}

// deleteExtraneousSpanConfigs deletes all extraneous span configs from KV.
func (f *fullReconciler) deleteExtraneousSpanConfigs(
	ctx context.Context, storeWithExtraneousSpanConfigs *spanconfigstore.Store,
) ([]roachpb.Span, error) {
	var extraneousSpans []roachpb.Span
	if err := storeWithExtraneousSpanConfigs.ForEachOverlapping(ctx, keys.EverythingSpan,
		func(entry roachpb.SpanConfigEntry) error {
			extraneousSpans = append(extraneousSpans, entry.Span)
			return nil
		},
	); err != nil {
		return nil, err
	}

	// Delete the extraneous entries, if any.
	if len(extraneousSpans) != 0 {
		if err := f.kvAccessor.UpdateSpanConfigEntries(ctx, extraneousSpans, nil); err != nil {
			return nil, err
		}
	}
	return extraneousSpans, nil
}

// incrementalReconciler is a single orchestrator for the incremental
// reconciliation process.
type incrementalReconciler struct {
	sqlTranslator       spanconfig.SQLTranslator
	sqlWatcher          spanconfig.SQLWatcher
	kvAccessor          spanconfig.KVAccessor
	storeWithKVContents *spanconfigstore.Store

	execCfg *sql.ExecutorConfig
	codec   keys.SQLCodec
	knobs   *spanconfig.TestingKnobs
}

// reconcile runs the incremental reconciliation process. It takes in:
// - the timestamp to start the incremental process from (typically a timestamp
//   we've already reconciled up until);
// - a callback that it invokes periodically with timestamps that it's
//   reconciled up until.
func (r *incrementalReconciler) reconcile(
	ctx context.Context, startTS hlc.Timestamp, callback func(reconciledUpUntil hlc.Timestamp) error,
) error {
	// Watch for incremental updates, applying KV as things change.
	return r.sqlWatcher.WatchForSQLUpdates(ctx, startTS,
		func(ctx context.Context, descriptorUpdates []spanconfig.DescriptorUpdate, checkpoint hlc.Timestamp) error {
			if len(descriptorUpdates) == 0 {
				return callback(checkpoint) // nothing to do; propagate the checkpoint
			}

			var allIDs descpb.IDs
			for _, update := range descriptorUpdates {
				allIDs = append(allIDs, update.ID)
			}

			// TODO(irfansharif): Would it be easier to just have the translator
			// return the set of missing table IDs? We're using two transactions
			// here, somewhat wastefully. An alternative would be to have a
			// txn-scoped translator.

			missingTableIDs, err := r.filterForMissingTableIDs(ctx, descriptorUpdates)
			if err != nil {
				return err
			}

			entries, _, err := r.sqlTranslator.Translate(ctx, allIDs)
			if err != nil {
				return err
			}

			updates := make([]spanconfig.Update, 0, len(missingTableIDs)+len(entries))
			for _, entry := range entries {
				// Update span configs for SQL state that changed.
				updates = append(updates, spanconfig.Update(entry))
			}
			for _, missingID := range missingTableIDs {
				// Delete span configs for missing tables.
				tableSpan := roachpb.Span{
					Key:    r.codec.TablePrefix(uint32(missingID)),
					EndKey: r.codec.TablePrefix(uint32(missingID)).PrefixEnd(),
				}
				updates = append(updates, spanconfig.Deletion(tableSpan))
			}

			toDelete, toUpsert := r.storeWithKVContents.Apply(ctx, false /* dryrun */, updates...)
			if len(toDelete) != 0 || len(toUpsert) != 0 {
				if err := r.kvAccessor.UpdateSpanConfigEntries(ctx, toDelete, toUpsert); err != nil {
					return err
				}
			}

			return callback(checkpoint)
		},
	)
}

// filterForMissingTableIDs filters the set of updates returning only the set of
// "missing" table IDs. These are descriptors that are no longer found, because
// they've been GC-ed away[1].
//
// [1]: Or if the ExcludeDroppedDescriptorsFromLookup testing knob is used,
//      this includes dropped descriptors.
func (r *incrementalReconciler) filterForMissingTableIDs(
	ctx context.Context, updates []spanconfig.DescriptorUpdate,
) (descpb.IDs, error) {
	seen := make(map[descpb.ID]struct{})
	var missingIDs descpb.IDs

	if err := sql.DescsTxn(ctx, r.execCfg,
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			for _, update := range updates {
				if update.DescriptorType != catalog.Table {
					continue // nothing to do
				}

				desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, update.ID, tree.CommonLookupFlags{
					Required:       true, // we want to error out for missing descriptors
					IncludeDropped: true,
					IncludeOffline: true,
					AvoidLeased:    true, // we want consistent reads
				})

				considerAsMissing := false
				if errors.Is(err, catalog.ErrDescriptorNotFound) {
					considerAsMissing = true
				} else if err != nil {
					return err
				} else if r.knobs.ExcludeDroppedDescriptorsFromLookup && desc.Dropped() {
					considerAsMissing = true
				}

				if considerAsMissing {
					if _, found := seen[update.ID]; !found {
						seen[update.ID] = struct{}{}
						missingIDs = append(missingIDs, update.ID) // accumulate the set of missing table IDs
					}
				}
			}

			return nil
		},
	); err != nil {
		return nil, err
	}

	return missingIDs, nil
}
