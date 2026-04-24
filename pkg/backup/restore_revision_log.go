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
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
// The input slice is shuffled in place.
func assignTicksToNodes(
	rng *rand.Rand, ticks []revlogpb.Manifest, numNodes int,
) [][]revlogpb.Manifest {
	rng.Shuffle(len(ticks), func(i, j int) {
		ticks[i], ticks[j] = ticks[j], ticks[i]
	})
	assignments := make([][]revlogpb.Manifest, numNodes)
	for i, t := range ticks {
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
	rng := rand.New(rand.NewSource(rand.Int63()))
	assignments := assignTicksToNodes(rng, manifests, len(sqlInstanceIDs))

	if r.testingKnobs.afterRevlogTickAssignment != nil {
		r.testingKnobs.afterRevlogTickAssignment(
			sqlInstanceIDs, assignments,
		)
	}

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
