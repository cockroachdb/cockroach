// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// maybeAdjustEndTimeForRevisionLog checks whether the collection has a
// revision log and, if so, adjusts the restore end time to the latest
// backup in the specified chain whose end time is at or before endTime.
// The original endTime is returned as the revision log replay target so
// that the caller can replay log entries from the backup's end through
// the requested AOST.
//
// fullSubdir identifies the backup chain (e.g. "2026/04/20-150405.00")
// so that only backups from that chain are considered.
//
// The end time is returned unchanged (with an empty revlog timestamp)
// when any of the following are true:
//   - this is a release build (revlog restore is prototype-only)
//   - no revision log exists at the collection root
//   - a backup in the chain exactly matches endTime
func maybeAdjustEndTimeForRevisionLog(
	ctx context.Context, store cloud.ExternalStorage, endTime hlc.Timestamp, fullSubdir string,
) (adjustedEndTime, revlogReplayTarget hlc.Timestamp, _ error) {
	if build.IsRelease() {
		return endTime, hlc.Timestamp{}, nil
	}

	hasLog, err := revlog.HasLog(ctx, store)
	if err != nil {
		return endTime, hlc.Timestamp{}, err
	}
	if !hasLog {
		return endTime, hlc.Timestamp{}, nil
	}

	// Find the latest backup whose end time is at or before endTime.
	// The backups returned by ListRestorableBackups may belong to a
	// different subdir or be slightly newer than endTime, so we list
	// enough to ensure we find a match.
	// TODO (kev-cao): This is slightly awkward, but I think the
	// introduction of revision log restore somewhat changes the
	// semantics of the restore command, which is a separate discussion.
	backups, _, err := backupinfo.ListRestorableBackups(
		ctx, store,
		time.Time{},                        /* newerThan */
		timeutil.Unix(0, endTime.WallTime), /* olderThan */
		4,                                  /* maxCount */
		true,                               /* openIndex */
	)
	if err != nil {
		return endTime, hlc.Timestamp{},
			errors.Wrap(err, "finding backup for revision log restore")
	}
	for _, b := range backups {
		if b.FullSubdir != fullSubdir || endTime.Less(b.EndTime) {
			continue
		}
		if endTime.Equal(b.EndTime) {
			// The AOST matches this backup exactly; a normal
			// restore is sufficient and no log replay is needed.
			return endTime, hlc.Timestamp{}, nil
		}
		return b.EndTime, endTime, nil
	}
	return endTime, hlc.Timestamp{},
		errors.New(
			"no backup found with end time at or before " +
				"the specified AS OF SYSTEM TIME",
		)
}

// assignTicksToNodes shuffles tick manifests using Fisher-Yates and
// distributes them round-robin across numNodes buckets. The returned
// slice has length numNodes; element i contains the manifests assigned
// to the i-th node.
//
// Shuffling prevents hot-spot skew from traffic spikes that would
// cause uneven work if ticks were assigned contiguously by time.
// Round-robin ensures each node gets roughly the same number of ticks.
//
// The input slice is not modified.
func assignTicksToNodes(ticks []revlogpb.Manifest, numNodes int) [][]revlogpb.Manifest {
	shuffled := make([]revlogpb.Manifest, len(ticks))
	copy(shuffled, ticks)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	assignments := make([][]revlogpb.Manifest, numNodes)
	for i, t := range shuffled {
		node := i % numNodes
		assignments[node] = append(assignments[node], t)
	}
	return assignments
}

// restoreFromRevisionLog replays revision log entries from the
// backup's end time through the target AOST timestamp, ingesting the
// mutations on top of the already-restored backup data.
//
// It discovers closed ticks in the revision log, shuffles and
// distributes them across SQL instances, and runs a DistSQL flow with
// one RevlogLocalMerge processor per node.
func (r *restoreResumer) restoreFromRevisionLog(
	ctx context.Context, execCtx sql.JobExecContext,
) error {
	details := r.job.Details().(jobspb.RestoreDetails)

	store, err := execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI(
		ctx, details.DefaultCollectionURI, execCtx.User(),
	)
	if err != nil {
		return errors.Wrap(err, "opening collection for revision log")
	}
	defer store.Close()

	// Discover ticks covering (backup end time, revlog replay target].
	lr := revlog.NewLogReader(store)
	var manifests []revlogpb.Manifest
	for tick, tickErr := range lr.Ticks(
		ctx, details.EndTime, details.RevisionLogTimestamp,
	) {
		if tickErr != nil {
			return errors.Wrap(tickErr, "discovering revision log ticks")
		}
		manifests = append(manifests, tick.Manifest)
	}
	if len(manifests) == 0 {
		log.Dev.Infof(
			ctx,
			"no revision log ticks found in (%s, %s]",
			details.EndTime, details.RevisionLogTimestamp,
		)
		return nil
	}
	log.Dev.Infof(
		ctx, "discovered %d revision log ticks to replay", len(manifests),
	)

	dsp := execCtx.DistSQLPlanner()
	evalCtx := execCtx.ExtendedEvalContext()

	planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanningWithOracle(
		ctx, evalCtx, execCtx.ExecCfg(),
		physicalplan.DefaultReplicaChooser,
		sql.SingleLocalityFilter(details.ExecutionLocality),
		sql.NoStrictLocalityFiltering,
	)
	if err != nil {
		return errors.Wrap(err, "setting up nodes for revlog restore")
	}

	// Shuffle and distribute ticks across nodes.
	assignments := assignTicksToNodes(manifests, len(sqlInstanceIDs))

	corePlacements := make(
		[]physicalplan.ProcessorCorePlacement, len(sqlInstanceIDs),
	)
	for i, id := range sqlInstanceIDs {
		corePlacements[i] = physicalplan.ProcessorCorePlacement{
			SQLInstanceID: id,
			Core: execinfrapb.ProcessorCoreUnion{
				RevlogLocalMerge: &execinfrapb.RevlogLocalMergeSpec{
					CollectionURI: details.DefaultCollectionURI,
					Ticks:         assignments[i],
					JobID:         int64(r.job.ID()),
					UserProto:     execCtx.User().EncodeProto(),
				},
			},
		}
	}

	plan := planCtx.NewPhysicalPlan()
	plan.AddNoInputStage(
		corePlacements,
		execinfrapb.PostProcessSpec{},
		[]*types.T{},
		execinfrapb.Ordering{},
		nil, /* finalizeLastStageCb */
	)
	sql.FinalizePlan(ctx, planCtx, plan)

	rowResultWriter := sql.NewRowResultWriter(nil)
	recv := sql.MakeDistSQLReceiver(
		ctx,
		rowResultWriter,
		tree.Rows,
		nil, /* rangeCache */
		nil, /* txn */
		nil, /* clockUpdater */
		evalCtx.Tracing,
	)
	defer recv.Release()

	evalCtxCopy := evalCtx.Copy()
	dsp.Run(
		ctx, planCtx, nil /* txn */, plan, recv, evalCtxCopy, nil, /* finishedSetupFn */
	)
	return errors.Wrap(rowResultWriter.Err(), "running revlog restore flow")
}

// validateRevlogResolved checks that the revision log has sealed
// ticks covering the requested AOST. It scans closed ticks after
// backupEndTime and returns as soon as it finds one whose end time
// is at or past revlogTimestamp.
func validateRevlogResolved(
	ctx context.Context, es cloud.ExternalStorage, backupEndTime, revlogTimestamp hlc.Timestamp,
) error {
	lr := revlog.NewLogReader(es)
	var maxTickEnd hlc.Timestamp
	for tick, tickErr := range lr.Ticks(
		ctx, backupEndTime, hlc.MaxTimestamp,
	) {
		if tickErr != nil {
			return errors.Wrap(tickErr, "listing revision log ticks")
		}
		if maxTickEnd.Less(tick.EndTime) {
			maxTickEnd = tick.EndTime
		}
		if !maxTickEnd.Less(revlogTimestamp) {
			// The log has resolved past the requested AOST.
			return nil
		}
	}
	if maxTickEnd.IsEmpty() {
		return errors.Newf(
			"revision log has no resolved ticks after backup end time %s; "+
				"cannot restore to AS OF SYSTEM TIME %s",
			backupEndTime, revlogTimestamp,
		)
	}
	return errors.Newf(
		"revision log has not resolved through the requested "+
			"AS OF SYSTEM TIME %s; latest resolved: %s",
		revlogTimestamp, maxTickEnd,
	)
}

// applyRevlogDescriptorChanges merges backup descriptors with revision
// log schema changes to produce the correct descriptor set at the
// requested AOST. Schema changes that occurred between
// backupEndTime and revlogTimestamp are applied on top of the
// backup's descriptors: new descriptors are added, modified
// descriptors are updated, and dropped/tombstoned descriptors are
// removed.
func applyRevlogDescriptorChanges(
	ctx context.Context,
	es cloud.ExternalStorage,
	backupDescs []catalog.Descriptor,
	backupEndTime, revlogTimestamp hlc.Timestamp,
) ([]catalog.Descriptor, error) {
	// Index backup descriptors by ID.
	byID := make(map[descpb.ID]catalog.Descriptor, len(backupDescs))
	for _, d := range backupDescs {
		byID[d.GetID()] = d
	}

	// Track the latest schema change per descriptor ID. Since
	// IterSchemaChanges yields in (changedAt, descID) ascending
	// order, the last entry per ID is the latest.
	type changeState struct {
		desc *descpb.Descriptor // nil = tombstone
	}
	latestByID := make(map[descpb.ID]changeState)
	for sc, err := range revlog.IterSchemaChanges(
		ctx, es, backupEndTime, revlogTimestamp,
	) {
		if err != nil {
			return nil, errors.Wrap(err, "reading revlog schema changes")
		}
		log.Dev.Infof(ctx,
			"revlog schema change: desc %d at %s (tombstone=%t)",
			sc.DescID, sc.ChangedAt, sc.Descriptor == nil,
		)
		latestByID[sc.DescID] = changeState{desc: sc.Descriptor}
	}
	log.Dev.Infof(ctx,
		"revlog descriptor resolution: %d backup descs, %d schema changes in (%s, %s]",
		len(backupDescs), len(latestByID), backupEndTime, revlogTimestamp,
	)

	// Apply changes.
	for id, change := range latestByID {
		if change.desc == nil {
			// Tombstone: descriptor was deleted from KV.
			delete(byID, id)
			continue
		}
		desc := backupinfo.NewDescriptorForManifest(change.desc)
		if desc == nil {
			continue
		}
		// Filter out descriptors in the DROP state.
		if tbl, ok := desc.(catalog.TableDescriptor); ok &&
			tbl.GetState() == descpb.DescriptorState_DROP {
			delete(byID, id)
			continue
		}
		byID[id] = desc
	}

	result := make([]catalog.Descriptor, 0, len(byID))
	for _, desc := range byID {
		result = append(result, desc)
	}
	return result, nil
}

// selectTargetsWithRevlog loads backup descriptors, merges them with
// revision log schema changes through revlogTimestamp, and runs
// target matching against the merged set. This is the revlog
// counterpart of selectTargets used during restore planning.
func selectTargetsWithRevlog(
	ctx context.Context,
	p sql.PlanHookState,
	es cloud.ExternalStorage,
	backupManifests []backuppb.BackupManifest,
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
	targets tree.BackupTargetList,
	descriptorCoverage tree.DescriptorCoverage,
	endTime, revlogTimestamp hlc.Timestamp,
) (
	[]catalog.Descriptor,
	[]catalog.DatabaseDescriptor,
	map[tree.TablePattern]catalog.Descriptor,
	[]mtinfopb.TenantInfoWithUsage,
	bool,
	error,
) {
	allBackupDescs, lastManifest, err := backupinfo.LoadSQLDescsFromBackupsAtTime(
		ctx, backupManifests, layerToIterFactory, endTime,
	)
	if err != nil {
		return nil, nil, nil, nil, false,
			errors.Wrap(err, "loading backup descriptors for revlog merge")
	}
	mergedDescs, err := applyRevlogDescriptorChanges(
		ctx, es, allBackupDescs, endTime, revlogTimestamp,
	)
	if err != nil {
		return nil, nil, nil, nil, false, err
	}
	return selectTargetsFromDescs(
		ctx, p, mergedDescs, lastManifest,
		targets, descriptorCoverage, endTime,
	)
}

// buildRevlogRekeys constructs table and tenant rekeys from the
// restore details. This mirrors the rekey construction in
// createRestoreFlows (restore_job.go).
func buildRevlogRekeys(
	details jobspb.RestoreDetails, execCfg *sql.ExecutorConfig,
) ([]execinfrapb.TableRekey, []execinfrapb.TenantRekey, error) {
	newIDToOldID := make(map[descpb.ID]descpb.ID)
	for oldID, rewrite := range details.DescriptorRewrites {
		newIDToOldID[rewrite.ID] = oldID
	}

	var tableRekeys []execinfrapb.TableRekey
	for i := range details.TableDescs {
		desc := tabledesc.NewBuilder(details.TableDescs[i]).
			BuildImmutableTable()
		newDescBytes, err := protoutil.Marshal(desc.DescriptorProto())
		if err != nil {
			return nil, nil, errors.NewAssertionErrorWithWrappedErrf(
				err, "marshaling descriptor",
			)
		}
		tableRekeys = append(tableRekeys, execinfrapb.TableRekey{
			OldID:   uint32(newIDToOldID[desc.GetID()]),
			NewDesc: newDescBytes,
		})
	}

	// Tenant rekeys: signal that this is a system-tenant-made backup
	// if the backup codec is for the system tenant.
	var tenantRekeys []execinfrapb.TenantRekey
	if execCfg.Codec.ForSystemTenant() {
		tenantRekeys = append(tenantRekeys, execinfrapb.TenantRekey{
			OldID: roachpb.SystemTenantID,
			NewID: roachpb.SystemTenantID,
		})
	}

	return tableRekeys, tenantRekeys, nil
}
