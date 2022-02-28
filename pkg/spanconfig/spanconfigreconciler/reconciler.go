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
// pagination + retrieval limit to the GetSpanConfigRecords API. We'd then
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
	var records []spanconfig.Record
	records, reconciledUpUntil, err = spanconfig.FullTranslate(ctx, f.sqlTranslator)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}

	updates := make([]spanconfig.Update, len(records))
	for i, record := range records {
		updates[i] = spanconfig.Update(record)
	}

	toDelete, toUpsert := storeWithExistingSpanConfigs.Apply(ctx, false /* dryrun */, updates...)
	if len(toDelete) != 0 || len(toUpsert) != 0 {
		if err := f.kvAccessor.UpdateSpanConfigRecords(ctx, toDelete, toUpsert); err != nil {
			return nil, hlc.Timestamp{}, err
		}
	}

	// Keep a copy of the current view of the world (i.e. KVAccessor
	// contents). We could also fetch everything from KV, but making a copy here
	// is cheaper (and saves an RTT). We'll later mutate
	// storeWithExistingSpanConfigs to determine what extraneous records are in
	// KV, in order to delete them. After doing so, we'll issue those same
	// deletions against this copy in order for it to reflect an up-to-date view
	// of span configs.
	storeWithLatestSpanConfigs = storeWithExistingSpanConfigs.Copy(ctx)

	// Delete all updated spans in a store populated with all current records.
	// Because our translation above captures the entire SQL state, deleting all
	// "updates" will leave behind only the extraneous records in KV -- we'll
	// get rid of them below.
	var storeWithExtraneousSpanConfigs *spanconfigstore.Store
	{
		for _, u := range updates {
			storeWithExistingSpanConfigs.Apply(ctx, false /* dryrun */, spanconfig.Deletion(u.Target))
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
	var targets []spanconfig.Target
	if f.codec.ForSystemTenant() {
		// The system tenant governs all system keys (meta, liveness, timeseries
		// ranges, etc.) and system tenant tables.
		//
		// TODO(irfansharif): Should we include the scratch range here? Some
		// tests make use of it; we may want to declare configs over it and have
		// it considered all the same.
		//
		// We don't want to request configs that are part of the
		// SystemSpanConfigSpan, as the host tenant reserves that part of the
		// keyspace to translate and persist SystemSpanConfigs. At the level of the
		// reconciler we shouldn't be requesting these configs directly, instead,
		// they should be targeted through SystemSpanConfigTargets instead.
		targets = append(targets, spanconfig.MakeTargetFromSpan(roachpb.Span{
			Key:    keys.EverythingSpan.Key,
			EndKey: keys.SystemSpanConfigSpan.Key,
		}))
		targets = append(targets, spanconfig.MakeTargetFromSpan(roachpb.Span{
			Key:    keys.TableDataMin,
			EndKey: keys.TableDataMax,
		}))

		// The system tenant also governs all SystemSpanConfigs set on its entire
		// keyspace (including secondary tenants), on its tenant keyspace, and on
		// other secondary tenant keyspaces.
		targets = append(targets,
			spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()))
		targets = append(targets,
			spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeAllTenantKeyspaceTargetsSet(f.tenID)))
		if f.knobs.ConfigureScratchRange {
			sp := targets[1].GetSpan()
			targets[1] = spanconfig.MakeTargetFromSpan(roachpb.Span{Key: sp.Key, EndKey: keys.ScratchRangeMax})
		}
	} else {
		// Secondary tenants govern everything prefixed by their tenant ID.
		tenPrefix := keys.MakeTenantPrefix(f.tenID)
		targets = append(targets, spanconfig.MakeTargetFromSpan(roachpb.Span{
			Key:    tenPrefix,
			EndKey: tenPrefix.PrefixEnd(),
		}))
		// Secondary tenants also govern all SystemSpanConfigs set by the tenant on
		// its entire keyspace.
		targets = append(targets,
			spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeAllTenantKeyspaceTargetsSet(f.tenID)))
	}
	store := spanconfigstore.New(roachpb.SpanConfig{})
	{
		// Fully populate the store with KVAccessor contents.
		records, err := f.kvAccessor.GetSpanConfigRecords(ctx, targets)
		if err != nil {
			return nil, err
		}

		for _, record := range records {
			store.Apply(ctx, false /* dryrun */, spanconfig.Update(record))
		}
	}
	return store, nil
}

// deleteExtraneousSpanConfigs deletes all extraneous span configs from KV.
func (f *fullReconciler) deleteExtraneousSpanConfigs(
	ctx context.Context, storeWithExtraneousSpanConfigs *spanconfigstore.Store,
) ([]spanconfig.Target, error) {
	var extraneousTargets []spanconfig.Target
	if err := storeWithExtraneousSpanConfigs.Iterate(func(record spanconfig.Record) error {
		extraneousTargets = append(extraneousTargets, record.Target)
		return nil
	},
	); err != nil {
		return nil, err
	}

	// Delete the extraneous entries, if any.
	if len(extraneousTargets) != 0 {
		if err := f.kvAccessor.UpdateSpanConfigRecords(ctx, extraneousTargets, nil); err != nil {
			return nil, err
		}
	}
	return extraneousTargets, nil
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
		func(ctx context.Context, sqlUpdates []spanconfig.SQLUpdate, checkpoint hlc.Timestamp) error {
			if len(sqlUpdates) == 0 {
				return callback(checkpoint) // nothing to do; propagate the checkpoint
			}

			// Process the SQLUpdates and identify all descriptor IDs that require
			// translation. If the SQLUpdates includes ProtectedTimestampUpdates then
			// instruct the translator to generate all records that apply to
			// spanconfig.SystemTargets as well.
			var generateSystemSpanConfigurations bool
			var allIDs descpb.IDs
			for _, update := range sqlUpdates {
				if update.IsDescriptorUpdate() {
					allIDs = append(allIDs, update.GetDescriptorUpdate().ID)
				} else if update.IsProtectedTimestampUpdate() {
					generateSystemSpanConfigurations = true
				}
			}

			// TODO(irfansharif): Would it be easier to just have the translator
			// return the set of missing table IDs? We're using two transactions
			// here, somewhat wastefully. An alternative would be to have a
			// txn-scoped translator.

			missingTableIDs, err := r.filterForMissingTableIDs(ctx, sqlUpdates)
			if err != nil {
				return err
			}

			missingProtectedTimestampTargets, err := r.filterForMissingProtectedTimestampSystemTargets(
				ctx, sqlUpdates)
			if err != nil {
				return err
			}

			entries, _, err := r.sqlTranslator.Translate(ctx, allIDs, generateSystemSpanConfigurations)
			if err != nil {
				return err
			}

			updates := make([]spanconfig.Update, 0,
				len(missingTableIDs)+len(missingProtectedTimestampTargets)+len(entries))
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
				updates = append(updates, spanconfig.Deletion(spanconfig.MakeTargetFromSpan(tableSpan)))
			}
			for _, missingSystemTarget := range missingProtectedTimestampTargets {
				updates = append(updates, spanconfig.Deletion(
					spanconfig.MakeTargetFromSystemTarget(missingSystemTarget)))
			}

			toDelete, toUpsert := r.storeWithKVContents.Apply(ctx, false /* dryrun */, updates...)
			if len(toDelete) != 0 || len(toUpsert) != 0 {
				if err := r.kvAccessor.UpdateSpanConfigRecords(ctx, toDelete, toUpsert); err != nil {
					return err
				}
			}

			return callback(checkpoint)
		},
	)
}

// filterForMissingProtectedTimestampSystemTargets filters the set of updates
// returning only the set of "missing" protected timestamp system targets. These
// correspond to cluster or tenant target protected timestamp records that are
// no longer found, because they've been released.
func (r *incrementalReconciler) filterForMissingProtectedTimestampSystemTargets(
	ctx context.Context, updates []spanconfig.SQLUpdate,
) ([]spanconfig.SystemTarget, error) {
	seen := make(map[spanconfig.SystemTarget]struct{})
	var missingSystemTargets []spanconfig.SystemTarget
	tenantPrefix := r.codec.TenantPrefix()
	_, sourceTenantID, err := keys.DecodeTenantPrefix(tenantPrefix)
	if err != nil {
		return nil, err
	}

	if err := sql.DescsTxn(ctx, r.execCfg,
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			// Construct an in-memory view of the system.protected_ts_records table to
			// populate the protected timestamp field on the emitted span configs.
			//
			// TODO(adityamaru): This does a full table scan of the
			// `system.protected_ts_records` table. While this is not assumed to be very
			// expensive given the limited number of concurrent users of the protected
			// timestamp subsystem, and the internal limits to limit the size of this
			// table, there is scope for improvement in the future. One option could be
			// a rangefeed-backed materialized view of the system table.
			ptsState, err := r.execCfg.ProtectedTimestampProvider.GetState(ctx, txn)
			if err != nil {
				return errors.Wrap(err, "failed to get protected timestamp state")
			}
			ptsStateReader := spanconfig.NewProtectedTimestampStateReader(ctx, ptsState)
			clusterProtections := ptsStateReader.GetProtectionPoliciesForCluster()
			missingClusterProtection := len(clusterProtections) == 0
			for _, update := range updates {
				if update.IsDescriptorUpdate() {
					continue // nothing to do
				}

				ptsUpdate := update.GetProtectedTimestampUpdate()
				missingSystemTarget := spanconfig.SystemTarget{}
				if ptsUpdate.IsClusterUpdate() && missingClusterProtection {
					// For the host tenant a Cluster ProtectedTimestampUpdate corresponds
					// to the entire keyspace (including secondary tenants).
					if r.codec.ForSystemTenant() {
						missingSystemTarget = spanconfig.MakeEntireKeyspaceTarget()
					} else {
						// For a secondary tenant a Cluster ProtectedTimestampUpdate
						// corresponds to the tenants keyspace.
						missingSystemTarget, err = spanconfig.MakeTenantKeyspaceTarget(sourceTenantID,
							sourceTenantID)
						if err != nil {
							return err
						}
					}
				}

				if ptsUpdate.IsTenantsUpdate() {
					if !ptsStateReader.ProtectionExistsForTenant(ptsUpdate.TenantTarget) {
						missingSystemTarget, err = spanconfig.MakeTenantKeyspaceTarget(sourceTenantID,
							ptsUpdate.TenantTarget)
						if err != nil {
							return err
						}
					}
				}

				if !missingSystemTarget.IsEmpty() {
					if _, found := seen[missingSystemTarget]; !found {
						seen[missingSystemTarget] = struct{}{}
						missingSystemTargets = append(missingSystemTargets, missingSystemTarget)
					}
				}
			}
			return nil
		}); err != nil {
		return missingSystemTargets, err
	}
	return missingSystemTargets, nil
}

// filterForMissingTableIDs filters the set of updates returning only the set of
// "missing" table IDs. These are descriptors that are no longer found, because
// they've been GC-ed away[1].
//
// [1]: Or if the ExcludeDroppedDescriptorsFromLookup testing knob is used,
//      this includes dropped descriptors.
func (r *incrementalReconciler) filterForMissingTableIDs(
	ctx context.Context, updates []spanconfig.SQLUpdate,
) (descpb.IDs, error) {
	seen := make(map[descpb.ID]struct{})
	var missingIDs descpb.IDs

	if err := sql.DescsTxn(ctx, r.execCfg,
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			for _, update := range updates {
				if update.IsProtectedTimestampUpdate() {
					continue // nothing to do
				}
				descriptorUpdate := update.GetDescriptorUpdate()
				if descriptorUpdate.Type != catalog.Table {
					continue // nothing to do
				}

				desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, descriptorUpdate.ID, tree.CommonLookupFlags{
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
					if _, found := seen[descriptorUpdate.ID]; !found {
						seen[descriptorUpdate.ID] = struct{}{}
						missingIDs = append(missingIDs, descriptorUpdate.ID) // accumulate the set of missing table IDs
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
