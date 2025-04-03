// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigreconciler

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqltranslator"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Reconciler is a concrete implementation of the spanconfig.Reconciler
// interface.
type Reconciler struct {
	sqlWatcher           spanconfig.SQLWatcher
	sqlTranslatorFactory *spanconfigsqltranslator.Factory
	kvAccessor           spanconfig.KVAccessor

	execCfg  *sql.ExecutorConfig
	codec    keys.SQLCodec
	tenID    roachpb.TenantID
	settings *cluster.Settings
	knobs    *spanconfig.TestingKnobs

	mu struct {
		syncutil.RWMutex
		lastCheckpoint hlc.Timestamp
	}
}

var _ spanconfig.Reconciler = &Reconciler{}

// New constructs a new Reconciler.
func New(
	sqlWatcher spanconfig.SQLWatcher,
	sqlTranslatorFactory *spanconfigsqltranslator.Factory,
	kvAccessor spanconfig.KVAccessor,
	execCfg *sql.ExecutorConfig,
	codec keys.SQLCodec,
	tenID roachpb.TenantID,
	settings *cluster.Settings,
	knobs *spanconfig.TestingKnobs,
) *Reconciler {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Reconciler{
		sqlWatcher:           sqlWatcher,
		sqlTranslatorFactory: sqlTranslatorFactory,
		kvAccessor:           kvAccessor,

		settings: settings,
		execCfg:  execCfg,
		codec:    codec,
		tenID:    tenID,
		knobs:    knobs,
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
//
//	trigger reconciliation after a tenant's SQL transaction. If we're able
//	to do this fast enough, it would be reasonable to wait for
//	reconciliation to happen before returning to the client. We could
//	alternatively use it as part of a handshake protocol during pod
//	suspension, to ensure that all outstanding work ("reconciliation" has
//	been done before a pod is suspended.
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
	ctx context.Context,
	startTS hlc.Timestamp,
	session sqlliveness.Session,
	onCheckpoint func() error,
) error {
	// TODO(irfansharif): Avoid the full reconciliation pass if the startTS
	// provided is visible to the rangefeed. Right now we're doing a full
	// reconciliation pass every time the reconciliation job kicks us off.
	if fn := r.knobs.ReconcilerInitialInterceptor; fn != nil {
		fn(startTS)
	}

	full := fullReconciler{
		sqlTranslatorFactory: r.sqlTranslatorFactory,
		kvAccessor:           r.kvAccessor,
		session:              session,
		settings:             r.settings,
		execCfg:              r.execCfg,
		codec:                r.codec,
		tenID:                r.tenID,
		knobs:                r.knobs,
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
		sqlTranslatorFactory: r.sqlTranslatorFactory,
		sqlWatcher:           r.sqlWatcher,
		kvAccessor:           r.kvAccessor,
		storeWithKVContents:  latestStore,
		session:              session,
		execCfg:              r.execCfg,
		codec:                r.codec,
		knobs:                r.knobs,
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
	sqlTranslatorFactory *spanconfigsqltranslator.Factory
	kvAccessor           spanconfig.KVAccessor
	session              sqlliveness.Session

	settings *cluster.Settings
	execCfg  *sql.ExecutorConfig
	codec    keys.SQLCodec
	tenID    roachpb.TenantID
	knobs    *spanconfig.TestingKnobs
}

// reconcile runs the full reconciliation process, returning:
// - a store with the latest set of span configs under our purview;
// - the timestamp we've reconciled up until.
func (f *fullReconciler) reconcile(
	ctx context.Context,
) (storeWithLatestSpanConfigs *spanconfigstore.Store, _ hlc.Timestamp, _ error) {
	if f.knobs != nil && f.knobs.OnFullReconcilerStart != nil {
		f.knobs.OnFullReconcilerStart()
	}

	storeWithExistingSpanConfigs, err := f.fetchExistingSpanConfigs(ctx)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}

	// Translate the entire SQL state to ensure KV reflects the most up-to-date
	// view of things.
	var records []spanconfig.Record

	var kvTxn *kv.Txn
	if err := f.execCfg.InternalDB.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		kvTxn = txn.KV()
		translator := f.sqlTranslatorFactory.NewSQLTranslator(txn)
		records, err = spanconfig.FullTranslate(ctx, translator)
		return err
	}); err != nil {
		return nil, hlc.Timestamp{}, err
	}
	readTimestamp, err := kvTxn.CommitTimestamp()
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}

	updates := make([]spanconfig.Update, len(records))
	for i, record := range records {
		updates[i] = spanconfig.Update(record)
	}

	toDelete, toUpsert := storeWithExistingSpanConfigs.Apply(ctx, updates...)
	if len(toDelete) != 0 || len(toUpsert) != 0 {
		if err := updateSpanConfigRecords(
			ctx, f.kvAccessor, toDelete, toUpsert, f.session,
		); err != nil {
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
	storeWithLatestSpanConfigs = storeWithExistingSpanConfigs.Clone()

	// Delete all updated spans in a store populated with all current records.
	// Because our translation above captures the entire SQL state, deleting all
	// "updates" will leave behind only the extraneous records in KV -- we'll
	// get rid of them below.
	var storeWithExtraneousSpanConfigs *spanconfigstore.Store
	{
		for _, u := range updates {
			del, err := spanconfig.Deletion(u.GetTarget())
			if err != nil {
				return nil, hlc.Timestamp{}, err
			}
			storeWithExistingSpanConfigs.Apply(ctx, del)
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
		del, err := spanconfig.Deletion(d)
		if err != nil {
			return nil, hlc.Timestamp{}, err
		}
		storeWithLatestSpanConfigs.Apply(ctx, del)
	}

	if !f.codec.ForSystemTenant() {
		found := false
		tenantPrefixKey := f.codec.TenantPrefix()
		// We want to ensure tenant ranges do not straddle tenant boundaries. As
		// such, a full reconciliation should always include a SpanConfig where the
		// start key is keys.MakeTenantPrefix(tenantID). This ensures there is a
		// split point right at the start of the tenant's keyspace, so that the
		// last range of the previous tenant doesn't straddle across into this
		// tenant. Also, sql.CreateTenantRecord relies on such a SpanConfigs
		// existence to ensure the same thing for newly created tenants.
		if err := storeWithLatestSpanConfigs.Iterate(func(record spanconfig.Record) error {
			if record.GetTarget().IsSystemTarget() {
				return nil // skip over system span configurations,
			}
			spanConfigStartKey := record.GetTarget().GetSpan().Key
			if tenantPrefixKey.Compare(spanConfigStartKey) == 0 {
				found = true
				return iterutil.StopIteration()
			}
			return nil
		}); err != nil {
			return nil, hlc.Timestamp{}, err
		}

		if !found {
			return nil, hlc.Timestamp{}, errors.AssertionFailedf(
				"did not find split point at the start of the tenant's keyspace during full reconciliation",
			)
		}
	}

	return storeWithLatestSpanConfigs, readTimestamp, nil
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
	// The reconciler doesn't do any bounds checks or clamping, so it shouldn't
	// need access to tenant capabilities (and by extension span config bounds).
	store := spanconfigstore.New(
		roachpb.SpanConfig{}, f.settings,
		spanconfigstore.NewEmptyBoundsReader(), f.knobs,
	)
	{
		// Fully populate the store with KVAccessor contents.
		records, err := f.kvAccessor.GetSpanConfigRecords(ctx, targets)
		if err != nil {
			return nil, err
		}

		for _, record := range records {
			store.Apply(ctx, spanconfig.Update(record))
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
		extraneousTargets = append(extraneousTargets, record.GetTarget())
		return nil
	},
	); err != nil {
		return nil, err
	}

	// Delete the extraneous entries, if any.
	if len(extraneousTargets) != 0 {
		if err := updateSpanConfigRecords(
			ctx, f.kvAccessor, extraneousTargets, nil, f.session,
		); err != nil {
			return nil, err
		}
	}
	return extraneousTargets, nil
}

// updateSpanConfigRecords calls the given KVAccessor's UpdateSpanConfigRecords
// method with the provided upsert/delete args. The call is wrapped inside a
// retry loop to account for any retryable errors that may occur.
func updateSpanConfigRecords(
	ctx context.Context,
	kvAccessor spanconfig.KVAccessor,
	toDelete []spanconfig.Target,
	toUpsert []spanconfig.Record,
	session sqlliveness.Session,
) error {
	retryOpts := retry.Options{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     5 * time.Second,
		MaxRetries:     5,
	}

	for retrier := retry.StartWithCtx(ctx, retryOpts); retrier.Next(); {
		sessionStart, sessionExpiration := session.Start(), session.Expiration()
		if sessionExpiration.IsEmpty() {
			return errors.Errorf("sqlliveness session has expired")
		}

		err := kvAccessor.UpdateSpanConfigRecords(
			ctx, toDelete, toUpsert, sessionStart, sessionExpiration,
		)
		if err != nil {
			if spanconfig.IsCommitTimestampOutOfBoundsError(err) {
				// We expect the underlying sqlliveness session's expiration to be
				// extended automatically, which makes this retry loop effective in the
				// face of these retryable lease expired errors from the RPC.
				log.Infof(ctx, "lease expired while updating span config records, retrying..")
				continue
			}
			return err // not a retryable error, bubble up
		}

		if log.V(3) {
			log.Infof(ctx, "successfully updated span config records: deleted = %+#v; upserted = %+#v", toDelete, toUpsert)
		}
		return nil // we performed the update; we're done here
	}
	return nil
}

// incrementalReconciler is a single orchestrator for the incremental
// reconciliation process.
type incrementalReconciler struct {
	sqlTranslatorFactory *spanconfigsqltranslator.Factory
	sqlWatcher           spanconfig.SQLWatcher
	kvAccessor           spanconfig.KVAccessor
	storeWithKVContents  *spanconfigstore.Store
	session              sqlliveness.Session

	execCfg *sql.ExecutorConfig
	codec   keys.SQLCodec
	knobs   *spanconfig.TestingKnobs
}

// reconcile runs the incremental reconciliation process. It takes in:
//   - the timestamp to start the incremental process from (typically a timestamp
//     we've already reconciled up until);
//   - a callback that it invokes periodically with timestamps that it's
//     reconciled up until.
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

			var missingTableIDs []descpb.ID
			var missingProtectedTimestampTargets []spanconfig.SystemTarget
			var records []spanconfig.Record

			if err := r.execCfg.InternalDB.DescsTxn(ctx, func(
				ctx context.Context, txn descs.Txn,
			) error {
				var err error

				// Using a fixed timestamp prevents this background job from contending
				// with foreground schema change traffic. Schema changes modify system
				// objects like system.descriptor, system.descriptor_id_seq, and
				// system.span_count. The spanconfig reconciler needs to read these
				// objects also. A fixed timestamp is a defensive measure to help
				// avoid contention caused by this background job.
				err = txn.KV().SetFixedTimestamp(ctx, checkpoint)
				if err != nil {
					return err
				}
				// TODO(irfansharif): Instead of these filter methods for missing
				// tables and system targets that live on the Reconciler, we could
				// move this to the SQLTranslator instead, now that the SQLTranslator
				// is transaction scoped.
				missingTableIDs, err = r.filterForMissingTableIDs(ctx, txn.KV(), txn.Descriptors(), sqlUpdates)
				if err != nil {
					return err
				}

				missingProtectedTimestampTargets, err = r.filterForMissingProtectedTimestampSystemTargets(
					ctx, txn, sqlUpdates,
				)
				if err != nil {
					return err
				}

				translator := r.sqlTranslatorFactory.NewSQLTranslator(txn)
				records, err = translator.Translate(ctx, allIDs, generateSystemSpanConfigurations)
				return err
			}); err != nil {
				return err
			}

			updates := make([]spanconfig.Update, 0,
				len(missingTableIDs)+len(missingProtectedTimestampTargets)+len(records))
			for _, entry := range records {
				// Update span configs for SQL state that changed.
				updates = append(updates, spanconfig.Update(entry))
			}
			for _, missingID := range missingTableIDs {
				// Delete span configs for missing tables.
				tableSpan := roachpb.Span{
					Key:    r.codec.TablePrefix(uint32(missingID)),
					EndKey: r.codec.TablePrefix(uint32(missingID)).PrefixEnd(),
				}
				del, err := spanconfig.Deletion(spanconfig.MakeTargetFromSpan(tableSpan))
				if err != nil {
					return err
				}
				updates = append(updates, del)
			}
			for _, missingSystemTarget := range missingProtectedTimestampTargets {
				del, err := spanconfig.Deletion(spanconfig.MakeTargetFromSystemTarget(missingSystemTarget))
				if err != nil {
					return err
				}
				updates = append(updates, del)
			}

			toDelete, toUpsert := r.storeWithKVContents.Apply(ctx, updates...)
			if len(toDelete) != 0 || len(toUpsert) != 0 {
				if err := updateSpanConfigRecords(
					ctx, r.kvAccessor, toDelete, toUpsert, r.session,
				); err != nil {
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
	ctx context.Context, txn isql.Txn, updates []spanconfig.SQLUpdate,
) ([]spanconfig.SystemTarget, error) {
	seen := make(map[spanconfig.SystemTarget]struct{})
	var missingSystemTargets []spanconfig.SystemTarget
	tenantPrefix := r.codec.TenantPrefix()
	_, sourceTenantID, err := keys.DecodeTenantPrefix(tenantPrefix)
	if err != nil {
		return nil, err
	}

	// Construct an in-memory view of the system.protected_ts_records table to
	// populate the protected timestamp field on the emitted span configs.
	//
	// TODO(adityamaru): This does a full table scan of the
	// `system.protected_ts_records` table. While this is not assumed to be very
	// expensive given the limited number of concurrent users of the protected
	// timestamp subsystem, and the internal limits to limit the size of this
	// table, there is scope for improvement in the future. One option could be
	// a rangefeed-backed materialized view of the system table.
	ptsState, err := r.execCfg.ProtectedTimestampProvider.WithTxn(txn).GetState(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get protected timestamp state")
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
					return nil, err
				}
			}
		}

		if ptsUpdate.IsTenantsUpdate() {
			noProtectionsOnTenant :=
				len(ptsStateReader.GetProtectionsForTenant(ptsUpdate.TenantTarget)) == 0
			if noProtectionsOnTenant {
				missingSystemTarget, err = spanconfig.MakeTenantKeyspaceTarget(sourceTenantID,
					ptsUpdate.TenantTarget)
				if err != nil {
					return nil, err
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

	return missingSystemTargets, nil
}

// filterForMissingTableIDs filters the set of updates returning only the set of
// "missing" table IDs. These are descriptors that are no longer found, because
// they've been GC-ed away[1].
//
// [1]: Or if the ExcludeDroppedDescriptorsFromLookup testing knob is used,
//
//	this includes dropped descriptors.
func (r *incrementalReconciler) filterForMissingTableIDs(
	ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, updates []spanconfig.SQLUpdate,
) (descpb.IDs, error) {
	seen := make(map[descpb.ID]struct{})
	var missingIDs descpb.IDs

	for _, update := range updates {
		if update.IsProtectedTimestampUpdate() {
			continue // nothing to do
		}
		descriptorUpdate := update.GetDescriptorUpdate()
		if descriptorUpdate.Type != catalog.Table {
			continue // nothing to do
		}

		desc, err := descsCol.ByIDWithoutLeased(txn).Get().Desc(ctx, descriptorUpdate.ID)

		considerAsMissing := false
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			considerAsMissing = true
		} else if err != nil {
			return nil, err
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

	return missingIDs, nil
}
