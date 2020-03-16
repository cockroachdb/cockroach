// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

const (
	// RunningStatusWaitingGC is for jobs that are currently in progress and
	// are waiting for the GC interval to expire
	RunningStatusWaitingGC jobs.RunningStatus = "waiting for GC TTL"
	// RunningStatusDeleteOnly is for jobs that are currently waiting on
	// the cluster to converge to seeing the schema element in the DELETE_ONLY
	// state.
	RunningStatusDeleteOnly jobs.RunningStatus = "waiting in DELETE-ONLY"
	// RunningStatusDeleteAndWriteOnly is for jobs that are currently waiting on
	// the cluster to converge to seeing the schema element in the
	// DELETE_AND_WRITE_ONLY state.
	RunningStatusDeleteAndWriteOnly jobs.RunningStatus = "waiting in DELETE-AND-WRITE_ONLY"
	// RunningStatusBackfill is for jobs that are currently running a backfill
	// for a schema element.
	RunningStatusBackfill jobs.RunningStatus = "populating schema"
	// RunningStatusValidation is for jobs that are currently validating
	// a schema element.
	RunningStatusValidation jobs.RunningStatus = "validating schema"
)

// SchemaChanger is used to change the schema on a table.
type SchemaChanger struct {
	tableID           sqlbase.ID
	mutationID        sqlbase.MutationID
	droppedDatabaseID sqlbase.ID
	nodeID            roachpb.NodeID
	db                *kv.DB
	leaseMgr          *LeaseManager

	testingKnobs   *SchemaChangerTestingKnobs
	distSQLPlanner *DistSQLPlanner
	jobRegistry    *jobs.Registry
	// Keep a reference to the job related to this schema change
	// so that we don't need to read the job again while updating
	// the status of the job.
	job *jobs.Job
	// Caches updated by DistSQL.
	rangeDescriptorCache *kvcoord.RangeDescriptorCache
	leaseHolderCache     *kvcoord.LeaseHolderCache
	clock                *hlc.Clock
	settings             *cluster.Settings
	execCfg              *ExecutorConfig
	ieFactory            sqlutil.SessionBoundInternalExecutorFactory
}

// NewSchemaChangerForTesting only for tests.
func NewSchemaChangerForTesting(
	tableID sqlbase.ID,
	mutationID sqlbase.MutationID,
	nodeID roachpb.NodeID,
	db kv.DB,
	leaseMgr *LeaseManager,
	jobRegistry *jobs.Registry,
	execCfg *ExecutorConfig,
	settings *cluster.Settings,
) SchemaChanger {
	return SchemaChanger{
		tableID:     tableID,
		mutationID:  mutationID,
		nodeID:      nodeID,
		db:          &db,
		leaseMgr:    leaseMgr,
		jobRegistry: jobRegistry,
		settings:    settings,
		execCfg:     execCfg,
	}
}

// isPermanentSchemaChangeError returns true if the error results in
// a permanent failure of a schema change. This function is a whitelist
// instead of a blacklist: only known safe errors are confirmed to not be
// permanent errors. Anything unknown is assumed to be permanent.
func isPermanentSchemaChangeError(err error) bool {
	if err == nil {
		return false
	}

	if grpcutil.IsClosedConnection(err) {
		return false
	}

	// Ignore error thrown because of a read at a very old timestamp.
	// The Backfill will grab a new timestamp to read at for the rest
	// of the backfill.
	// TODO(knz): this should really use errors.Is(). However until/unless
	// we are not receiving errors from 19.1 any more, a string
	// comparison must remain.
	if strings.Contains(err.Error(), "must be after replica GC threshold") {
		return false
	}

	if pgerror.IsSQLRetryableError(err) {
		return false
	}

	if errors.IsAny(err,
		context.Canceled,
		context.DeadlineExceeded,
		errExistingSchemaChangeLease,
		errExpiredSchemaChangeLease,
		errNotHitGCTTLDeadline,
		errSchemaChangeDuringDrain,
		errSchemaChangeNotFirstInLine,
		errTableVersionMismatchSentinel,
	) {
		return false
	}

	switch pgerror.GetPGCode(err) {
	case pgcode.SerializationFailure, pgcode.InternalConnectionFailure, pgcode.DeprecatedInternalConnectionFailure:
		return false

	case pgcode.Internal, pgcode.RangeUnavailable, pgcode.DeprecatedRangeUnavailable:
		if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			return false
		}
	}

	return true
}

var (
	errExistingSchemaChangeLease  = errors.Newf("an outstanding schema change lease exists")
	errExpiredSchemaChangeLease   = errors.Newf("the schema change lease has expired")
	errSchemaChangeNotFirstInLine = errors.Newf("schema change not first in line")
	errNotHitGCTTLDeadline        = errors.Newf("not hit gc ttl deadline")
	errSchemaChangeDuringDrain    = errors.Newf("a schema change ran during the drain phase, re-increment")
)

type errTableVersionMismatch struct {
	version  sqlbase.DescriptorVersion
	expected sqlbase.DescriptorVersion
}

var errTableVersionMismatchSentinel = errTableVersionMismatch{}

func makeErrTableVersionMismatch(version, expected sqlbase.DescriptorVersion) error {
	return errors.Mark(errors.WithStack(errTableVersionMismatch{
		version:  version,
		expected: expected,
	}), errTableVersionMismatchSentinel)
}

func (e errTableVersionMismatch) Error() string {
	return fmt.Sprintf("table version mismatch: %d, expected: %d", e.version, e.expected)
}

// maybe backfill a created table by executing the AS query. Return nil if
// successfully backfilled.
//
// Note that this does not connect to the tracing settings of the
// surrounding SQL transaction. This should be OK as (at the time of
// this writing) this code path is only used for standalone CREATE
// TABLE AS statements, which cannot be traced.
func (sc *SchemaChanger) maybeBackfillCreateTableAs(
	ctx context.Context, table *sqlbase.TableDescriptor,
) error {
	if !(table.Adding() && table.IsAs()) {
		return nil
	}

	return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetFixedTimestamp(ctx, table.CreateAsOfTime)

		// Create an internal planner as the planner used to serve the user query
		// would have committed by this point.
		p, cleanup := NewInternalPlanner("ctasBackfill", txn, security.RootUser, &MemoryMetrics{}, sc.execCfg)
		defer cleanup()
		localPlanner := p.(*planner)
		stmt, err := parser.ParseOne(table.CreateQuery)
		if err != nil {
			return err
		}

		// Construct an optimized logical plan of the AS source stmt.
		localPlanner.stmt = &Statement{Statement: stmt}
		localPlanner.optPlanningCtx.init(localPlanner)

		localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
			err = localPlanner.makeOptimizerPlan(ctx)
		})

		if err != nil {
			return err
		}
		defer localPlanner.curPlan.close(ctx)

		res := roachpb.BulkOpSummary{}
		rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
			// TODO(adityamaru): Use the BulkOpSummary for either telemetry or to
			// return to user.
			var counts roachpb.BulkOpSummary
			if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &counts); err != nil {
				return err
			}
			res.Add(counts)
			return nil
		})
		recv := MakeDistSQLReceiver(
			ctx,
			rw,
			tree.Rows,
			sc.execCfg.RangeDescriptorCache,
			sc.execCfg.LeaseHolderCache,
			txn,
			func(ts hlc.Timestamp) {
				_ = sc.clock.Update(ts)
			},
			// Make a session tracing object on-the-fly. This is OK
			// because it sets "enabled: false" and thus none of the
			// other fields are used.
			&SessionTracing{},
		)
		defer recv.Release()

		rec, err := sc.distSQLPlanner.checkSupportForNode(localPlanner.curPlan.plan)
		var planAndRunErr error
		localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
			// Resolve subqueries before running the queries' physical plan.
			if len(localPlanner.curPlan.subqueryPlans) != 0 {
				if !sc.distSQLPlanner.PlanAndRunSubqueries(
					ctx, localPlanner, localPlanner.ExtendedEvalContextCopy,
					localPlanner.curPlan.subqueryPlans, recv, rec == canDistribute,
				) {
					if planAndRunErr = rw.Err(); planAndRunErr != nil {
						return
					}
					if planAndRunErr = recv.commErr; planAndRunErr != nil {
						return
					}
				}
			}

			isLocal := err != nil || rec == cannotDistribute
			out := execinfrapb.ProcessorCoreUnion{BulkRowWriter: &execinfrapb.BulkRowWriterSpec{
				Table: *table,
			}}

			PlanAndRunCTAS(ctx, sc.distSQLPlanner, localPlanner,
				txn, isLocal, localPlanner.curPlan.plan, out, recv)
			if planAndRunErr = rw.Err(); planAndRunErr != nil {
				return
			}
			if planAndRunErr = recv.commErr; planAndRunErr != nil {
				return
			}
		})

		return planAndRunErr
	})
}

// maybe make a table PUBLIC if it's in the ADD state.
func (sc *SchemaChanger) maybeMakeAddTablePublic(
	ctx context.Context, table *sqlbase.TableDescriptor,
) error {
	if table.Adding() {
		fks := table.AllActiveAndInactiveForeignKeys()
		for _, fk := range fks {
			if err := sc.waitToUpdateLeases(ctx, fk.ReferencedTableID); err != nil {
				return err
			}
		}

		if _, err := sc.leaseMgr.Publish(
			ctx,
			table.ID,
			func(tbl *sqlbase.MutableTableDescriptor) error {
				if !tbl.Adding() {
					return errDidntUpdateDescriptor
				}
				tbl.State = sqlbase.TableDescriptor_PUBLIC
				return nil
			},
			func(txn *kv.Txn) error { return nil },
		); err != nil {
			return err
		}
	}

	return nil
}

// Drain old names from the cluster.
func (sc *SchemaChanger) drainNames(ctx context.Context) error {
	// Publish a new version with all the names drained after everyone
	// has seen the version with the new name. All the draining names
	// can be reused henceforth.
	var namesToReclaim []sqlbase.TableDescriptor_NameInfo
	_, err := sc.leaseMgr.Publish(
		ctx,
		sc.tableID,
		func(desc *sqlbase.MutableTableDescriptor) error {
			if sc.testingKnobs.OldNamesDrainedNotification != nil {
				sc.testingKnobs.OldNamesDrainedNotification()
			}
			// Free up the old name(s) for reuse.
			namesToReclaim = desc.DrainingNames
			desc.DrainingNames = nil
			return nil
		},
		// Reclaim all the old names.
		func(txn *kv.Txn) error {
			b := txn.NewBatch()
			for _, drain := range namesToReclaim {
				err := sqlbase.RemoveObjectNamespaceEntry(ctx, txn, drain.ParentID, drain.ParentSchemaID,
					drain.Name, false /* KVTrace */)
				if err != nil {
					return err
				}
			}
			return txn.Run(ctx, b)
		},
	)
	return err
}

func startGCJob(
	ctx context.Context,
	db *kv.DB,
	jobRegistry *jobs.Registry,
	username string,
	schemaChangeDescription string,
	details jobspb.SchemaChangeGCDetails,
) error {
	var sj *jobs.StartableJob
	descriptorIDs := make([]sqlbase.ID, 0)
	if len(details.Indexes) > 0 {
		if len(descriptorIDs) == 0 {
			descriptorIDs = []sqlbase.ID{details.ParentID}
		}
	} else if len(details.Tables) > 0 {
		for _, table := range details.Tables {
			descriptorIDs = append(descriptorIDs, table.ID)
		}
	} else {
		// Nothing to GC.
		return nil
	}

	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		jobRecord := jobs.Record{
			Description:   fmt.Sprintf("GC for %s", schemaChangeDescription),
			Username:      username,
			DescriptorIDs: descriptorIDs,
			Details:       details,
			Progress:      jobspb.SchemaChangeGCProgress{},
			NonCancelable: true,
		}
		var err error
		if sj, err = jobRegistry.CreateStartableJobWithTxn(ctx, jobRecord, txn, nil /* resultCh */); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if _, err := sj.Start(ctx); err != nil {
		return err
	}
	return nil
}

func (sc *SchemaChanger) startGCJob(
	ctx context.Context, details jobspb.SchemaChangeGCDetails, isRollback bool,
) error {
	description := sc.job.Payload().Description
	if isRollback {
		description = "ROLLBACK of " + description
	}
	return startGCJob(ctx, sc.db, sc.jobRegistry, sc.job.Payload().Username, description, details)
}

// Execute the entire schema change in steps.
// inSession is set to false when this is called from the asynchronous
// schema change execution path.
//
// If the txn that queued the schema changer did not commit, this will be a
// no-op, as we'll fail to find the job for our mutation in the jobs registry.
func (sc *SchemaChanger) exec(ctx context.Context) error {
	ctx = logtags.AddTag(ctx, "scExec", nil)

	// TODO (lucy): Now that marking a schema change job as succeeded doesn't
	// happen in the same transaction as removing mutations from a table
	// descriptor, it seems possible for a job to be resumed after the mutation
	// has already been removed. If there's a mutation provided, we should check
	// whether it actually exists on the table descriptor and exit the job if not.
	tableDesc, notFirst, err := sc.notFirstInLine(ctx)
	if err != nil {
		return err
	}
	if notFirst {
		log.Infof(ctx,
			"schema change on %s (%d v%d) mutation %d: another change is still in progress",
			tableDesc.Name, sc.tableID, tableDesc.Version, sc.mutationID,
		)
		return errSchemaChangeNotFirstInLine
	}

	log.Infof(ctx,
		"schema change on %s (%d v%d) mutation %d starting execution...",
		tableDesc.Name, sc.tableID, tableDesc.Version, sc.mutationID,
	)

	if tableDesc.HasDrainingNames() {
		if err := sc.drainNames(ctx); err != nil {
			return err
		}
	}

	if tableDesc.Dropped() && sc.droppedDatabaseID == sqlbase.InvalidID {
		// We've dropped this table, let's kick off a GC job.
		dropTime := timeutil.Now().UnixNano()
		if tableDesc.DropTime > 0 {
			dropTime = tableDesc.DropTime
		}
		gcDetails := jobspb.SchemaChangeGCDetails{
			Tables: []jobspb.SchemaChangeGCDetails_DroppedID{
				{
					ID:       tableDesc.ID,
					DropTime: dropTime,
				},
			},
		}
		if err := sc.startGCJob(ctx, gcDetails, false /* isRollback */); err != nil {
			return err
		}
	}

	if err := sc.maybeBackfillCreateTableAs(ctx, tableDesc); err != nil {
		return err
	}

	if err := sc.maybeMakeAddTablePublic(ctx, tableDesc); err != nil {
		return err
	}

	// Wait for the schema change to propagate to all nodes after this function
	// returns, so that the new schema is live everywhere. This is not needed for
	// correctness but is done to make the UI experience/tests predictable.
	waitToUpdateLeases := func(refreshStats bool) error {
		if err := sc.waitToUpdateLeases(ctx, sc.tableID); err != nil {
			if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
				return err
			}
			log.Warningf(ctx, "waiting to update leases: %+v", err)
			// As we are dismissing the error, go through the recording motions.
			// This ensures that any important error gets reported to Sentry, etc.
			sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
		}
		// We wait to trigger a stats refresh until we know the leases have been
		// updated.
		if refreshStats {
			sc.refreshStats()
		}
		return nil
	}

	if sc.mutationID == sqlbase.InvalidMutationID {
		// Nothing more to do.
		isCreateTableAs := tableDesc.Adding() && tableDesc.IsAs()
		return waitToUpdateLeases(isCreateTableAs /* refreshStats */)
	}

	if err := sc.initJobRunningStatus(ctx); err != nil {
		if log.V(2) {
			log.Infof(ctx, "failed to update job status: %+v", err)
		}
		// Go through the recording motions. See comment above.
		sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
	}

	// Run through mutation state machine and backfill.
	err = sc.runStateMachineAndBackfill(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := waitToUpdateLeases(err == nil /* refreshStats */); err != nil && !errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			// We only expect ErrDescriptorNotFound to be returned. This happens
			// when the table descriptor was deleted. We can ignore this error.

			log.Warningf(ctx, "unexpected error while waiting for leases to update: %+v", err)
			// As we are dismissing the error, go through the recording motions.
			// This ensures that any important error gets reported to Sentry, etc.
			sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
		}
	}()

	return err
}

// handlePermanentSchemaChangeError cleans up schema changes that cannot
// be completed successfully. For schema changes with mutations, it reverses the
// direction of the mutations so that we can step through the state machine
// backwards. Note that schema changes which don't have mutations are meant to
// run quickly and aren't truly cancellable in the small window they require to
// complete. In that case, cleanup consists of simply resuming the same schema
// change.
// TODO (lucy): This is how "rolling back" has always worked for non-mutation
// schema change jobs, but it's unnatural for the job API and we should rethink
// it.
func (sc *SchemaChanger) handlePermanentSchemaChangeError(
	ctx context.Context, err error, evalCtx *extendedEvalContext,
) error {
	if rollbackErr := sc.rollbackSchemaChange(ctx, err); rollbackErr != nil {
		// Note: the "err" object is captured by rollbackSchemaChange(), so
		// it does not simply disappear.
		return errors.Wrap(rollbackErr, "while rolling back schema change")
	}

	// TODO (lucy): This is almost the same as in exec(), maybe refactor.
	// Wait for the schema change to propagate to all nodes after this function
	// returns, so that the new schema is live everywhere. This is not needed for
	// correctness but is done to make the UI experience/tests predictable.
	waitToUpdateLeases := func(refreshStats bool) error {
		if err := sc.waitToUpdateLeases(ctx, sc.tableID); err != nil {
			if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
				return err
			}
			log.Warningf(ctx, "waiting to update leases: %+v", err)
			// As we are dismissing the error, go through the recording motions.
			// This ensures that any important error gets reported to Sentry, etc.
			sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
		}
		// We wait to trigger a stats refresh until we know the leases have been
		// updated.
		if refreshStats {
			sc.refreshStats()
		}
		return nil
	}

	defer func() {
		if err := waitToUpdateLeases(false /* refreshStats */); err != nil && !errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			// We only expect ErrDescriptorNotFound to be returned. This happens
			// when the table descriptor was deleted. We can ignore this error.

			log.Warningf(ctx, "unexpected error while waiting for leases to update: %+v", err)
			// As we are dismissing the error, go through the recording motions.
			// This ensures that any important error gets reported to Sentry, etc.
			sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
		}
	}()

	return nil
}

// initialize the job running status.
func (sc *SchemaChanger) initJobRunningStatus(ctx context.Context) error {
	return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		desc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		var runStatus jobs.RunningStatus
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}

			switch mutation.Direction {
			case sqlbase.DescriptorMutation_ADD:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					runStatus = RunningStatusDeleteOnly
				}

			case sqlbase.DescriptorMutation_DROP:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					runStatus = RunningStatusDeleteAndWriteOnly
				}
			}
		}
		if runStatus != "" && !desc.Dropped() {
			if err := sc.job.WithTxn(txn).RunningStatus(
				ctx, func(ctx context.Context, details jobspb.Details) (jobs.RunningStatus, error) {
					return runStatus, nil
				}); err != nil {
				return errors.Wrapf(err, "failed to update job status")
			}
		}
		return nil
	})
}

func (sc *SchemaChanger) rollbackSchemaChange(ctx context.Context, err error) error {
	log.Warningf(ctx, "reversing schema change %d due to irrecoverable error: %s", *sc.job.ID(), err)
	if errReverse := sc.reverseMutations(ctx, err); errReverse != nil {
		// Although the backfill did hit an integrity constraint violation
		// and made a decision to reverse the mutations,
		// reverseMutations() failed. If exec() is called again the entire
		// schema change will be retried.

		// Note: we capture the original error as "secondary" to ensure it
		// does not fully disappear.
		// However, since it is not in the main causal chain any more,
		// it will become invisible to further telemetry. So before
		// we relegate it to a secondary, go through the recording motions.
		// This ensures that any important error gets reported to Sentry, etc.
		secondary := errors.Wrap(err, "original error when reversing mutations")
		sqltelemetry.RecordError(ctx, secondary, &sc.settings.SV)
		return errors.WithSecondaryError(errReverse, secondary)
	}

	// After this point the schema change has been reversed and any retry
	// of the schema change will act upon the reversed schema change.
	if errPurge := sc.runStateMachineAndBackfill(ctx); errPurge != nil {
		// Don't return this error because we do want the caller to know
		// that an integrity constraint was violated with the original
		// schema change. The reversed schema change will be
		// retried via the async schema change manager.

		// Since the errors are going to disappear, do the recording
		// motions on them. This ensures that any assertion failure or
		// other important error underneath gets recorded properly.
		log.Warningf(ctx, "error purging mutation: %+v\nwhile handling error: %+v", errPurge, err)
		sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
		sqltelemetry.RecordError(ctx, errPurge, &sc.settings.SV)
	}
	return nil
}

// RunStateMachineBeforeBackfill moves the state machine forward
// and wait to ensure that all nodes are seeing the latest version
// of the table.
func (sc *SchemaChanger) RunStateMachineBeforeBackfill(ctx context.Context) error {
	var runStatus jobs.RunningStatus
	if _, err := sc.leaseMgr.Publish(ctx, sc.tableID, func(desc *sqlbase.MutableTableDescriptor) error {

		runStatus = ""
		// Apply mutations belonging to the same version.
		for i, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			switch mutation.Direction {
			case sqlbase.DescriptorMutation_ADD:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					// TODO(vivek): while moving up the state is appropriate,
					// it will be better to run the backfill of a unique index
					// twice: once in the DELETE_ONLY state to confirm that
					// the index can indeed be created, and subsequently in the
					// DELETE_AND_WRITE_ONLY state to fill in the missing elements of the
					// index (INSERT and UPDATE that happened in the interim).
					desc.Mutations[i].State = sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY
					runStatus = RunningStatusDeleteAndWriteOnly

				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					// The state change has already moved forward.
				}

			case sqlbase.DescriptorMutation_DROP:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					// The state change has already moved forward.

				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					desc.Mutations[i].State = sqlbase.DescriptorMutation_DELETE_ONLY
					runStatus = RunningStatusDeleteOnly
				}
			}
		}
		if doNothing := runStatus == "" || desc.Dropped(); doNothing {
			// Return error so that Publish() doesn't increment the version.
			return errDidntUpdateDescriptor
		}
		return nil
	}, func(txn *kv.Txn) error {
		if sc.job != nil {
			if err := sc.job.WithTxn(txn).RunningStatus(ctx, func(ctx context.Context, details jobspb.Details) (jobs.RunningStatus, error) {
				return runStatus, nil
			}); err != nil {
				return errors.Wrap(err, "failed to update job status")
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// wait for the state change to propagate to all leases.
	return sc.waitToUpdateLeases(ctx, sc.tableID)
}

// Wait until the entire cluster has been updated to the latest version
// of the table descriptor.
func (sc *SchemaChanger) waitToUpdateLeases(ctx context.Context, tableID sqlbase.ID) error {
	// Aggressively retry because there might be a user waiting for the
	// schema change to complete.
	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	log.Infof(ctx, "waiting for a single version of table %d...", tableID)
	version, err := sc.leaseMgr.WaitForOneVersion(ctx, tableID, retryOpts)
	log.Infof(ctx, "waiting for a single version of table %d... done (at v %d)", tableID, version)
	return err
}

// done finalizes the mutations (adds new cols/indexes to the table).
// It ensures that all nodes are on the current (pre-update) version of the
// schema.
// It also kicks off GC jobs as needed.
// Returns the updated descriptor.
func (sc *SchemaChanger) done(ctx context.Context) (*sqlbase.ImmutableTableDescriptor, error) {
	isRollback := false

	// Get the other tables whose foreign key backreferences need to be removed.
	// We make a call to PublishMultiple to handle the situation to add Foreign Key backreferences.
	var fksByBackrefTable map[sqlbase.ID][]*sqlbase.ConstraintToUpdate
	var interleaveParents map[sqlbase.ID]struct{}
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		fksByBackrefTable = make(map[sqlbase.ID][]*sqlbase.ConstraintToUpdate)
		interleaveParents = make(map[sqlbase.ID]struct{})

		desc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}
			if constraint := mutation.GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
				mutation.Direction == sqlbase.DescriptorMutation_ADD &&
				constraint.ForeignKey.Validity == sqlbase.ConstraintValidity_Unvalidated {
				// Add backref table to referenced table with an unvalidated foreign key constraint
				fk := &constraint.ForeignKey
				if fk.ReferencedTableID != desc.ID {
					fksByBackrefTable[constraint.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[constraint.ForeignKey.ReferencedTableID], constraint)
				}
			} else if swap := mutation.GetPrimaryKeySwap(); swap != nil {
				// If any old indexes (including the old primary index) being rewritten are interleaved
				// children, we will have to update their parents as well.
				for _, idxID := range append([]sqlbase.IndexID{swap.OldPrimaryIndexId}, swap.OldIndexes...) {
					oldIndex, err := desc.FindIndexByID(idxID)
					if err != nil {
						return err
					}
					if len(oldIndex.Interleave.Ancestors) != 0 {
						ancestor := oldIndex.Interleave.Ancestors[len(oldIndex.Interleave.Ancestors)-1]
						if ancestor.TableID != desc.ID {
							interleaveParents[ancestor.TableID] = struct{}{}
						}
					}
				}
				// Because we are not currently supporting primary key changes on tables/indexes
				// that are interleaved parents, we don't check oldPrimaryIndex.InterleavedBy.
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	tableIDsToUpdate := make([]sqlbase.ID, 0, len(fksByBackrefTable)+1)
	tableIDsToUpdate = append(tableIDsToUpdate, sc.tableID)
	for id := range fksByBackrefTable {
		tableIDsToUpdate = append(tableIDsToUpdate, id)
	}
	for id := range interleaveParents {
		if _, ok := fksByBackrefTable[id]; !ok {
			tableIDsToUpdate = append(tableIDsToUpdate, id)
		}
	}

	update := func(txn *kv.Txn, descs map[sqlbase.ID]*sqlbase.MutableTableDescriptor) error {
		// Reset vars here because update function can be called multiple times in a retry.
		isRollback = false

		i := 0
		scDesc, ok := descs[sc.tableID]
		if !ok {
			return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
		}
		for _, mutation := range scDesc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			isRollback = mutation.Rollback
			if indexDesc := mutation.GetIndex(); mutation.Direction == sqlbase.DescriptorMutation_DROP &&
				indexDesc != nil {
				if canClearRangeForDrop(indexDesc) {
					// how we keep track of dropped index names (for, e.g., zone config
					// lookups), even though in the absence of a GC job there's nothing to
					// clean them up.
					scDesc.GCMutations = append(
						scDesc.GCMutations,
						sqlbase.TableDescriptor_GCDescriptorMutation{
							IndexID: indexDesc.ID,
						})

					dropTime := timeutil.Now().UnixNano()
					indexGCDetails := jobspb.SchemaChangeGCDetails{
						Indexes: []jobspb.SchemaChangeGCDetails_DroppedIndex{
							{
								IndexID:  indexDesc.ID,
								DropTime: dropTime,
							},
						},
						ParentID: sc.tableID,
					}
					if err := sc.startGCJob(ctx, indexGCDetails, isRollback); err != nil {
						return err
					}
				}
			}
			if constraint := mutation.GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
				mutation.Direction == sqlbase.DescriptorMutation_ADD &&
				constraint.ForeignKey.Validity == sqlbase.ConstraintValidity_Unvalidated {
				// Add backreference on the referenced table (which could be the same table)
				backrefTable, ok := descs[constraint.ForeignKey.ReferencedTableID]
				if !ok {
					return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
				}
				backrefTable.InboundFKs = append(backrefTable.InboundFKs, constraint.ForeignKey)
			}
			if err := scDesc.MakeMutationComplete(mutation); err != nil {
				return err
			}
			if pkSwap := mutation.GetPrimaryKeySwap(); pkSwap != nil {
				if fn := sc.testingKnobs.RunBeforePrimaryKeySwap; fn != nil {
					fn()
				}
				// If any old index had an interleaved parent, remove the
				// backreference from the parent.
				// N.B. This logic needs to be kept up to date with the
				// corresponding piece in runSchemaChangesInTxn.
				for _, idxID := range append(
					[]sqlbase.IndexID{pkSwap.OldPrimaryIndexId}, pkSwap.OldIndexes...) {
					oldIndex, err := scDesc.FindIndexByID(idxID)
					if err != nil {
						return err
					}
					if len(oldIndex.Interleave.Ancestors) != 0 {
						ancestorInfo := oldIndex.Interleave.Ancestors[len(oldIndex.Interleave.Ancestors)-1]
						ancestor := descs[ancestorInfo.TableID]
						ancestorIdx, err := ancestor.FindIndexByID(ancestorInfo.IndexID)
						if err != nil {
							return err
						}
						foundAncestor := false
						for k, ref := range ancestorIdx.InterleavedBy {
							if ref.Table == scDesc.ID && ref.Index == oldIndex.ID {
								if foundAncestor {
									return errors.AssertionFailedf(
										"ancestor entry in %s for %s@%s found more than once",
										ancestor.Name, scDesc.Name, oldIndex.Name)
								}
								ancestorIdx.InterleavedBy = append(
									ancestorIdx.InterleavedBy[:k], ancestorIdx.InterleavedBy[k+1:]...)
								foundAncestor = true
							}
						}
					}
				}

				// If we performed MakeMutationComplete on a PrimaryKeySwap mutation, then we need to start
				// a job for the index deletion mutations that the primary key swap mutation added, if any.
				mutationID := scDesc.ClusterVersion.NextMutationID
				span := scDesc.PrimaryIndexSpan()
				var spanList []jobspb.ResumeSpanList
				for j := len(scDesc.ClusterVersion.Mutations); j < len(scDesc.Mutations); j++ {
					spanList = append(spanList,
						jobspb.ResumeSpanList{
							ResumeSpans: roachpb.Spans{span},
						},
					)
				}
				// Only start a job if spanList has any spans. If len(spanList) == 0, then
				// no mutations were enqueued by the primary key change.
				if len(spanList) > 0 {
					jobRecord := jobs.Record{
						Description:   fmt.Sprintf("CLEANUP JOB for '%s'", sc.job.Payload().Description),
						Username:      sc.job.Payload().Username,
						DescriptorIDs: sqlbase.IDs{scDesc.GetID()},
						Details: jobspb.SchemaChangeDetails{
							TableID:        sc.tableID,
							MutationID:     mutationID,
							ResumeSpanList: spanList,
						},
						Progress:      jobspb.SchemaChangeProgress{},
						NonCancelable: true,
					}
					job, err := sc.jobRegistry.CreateJobWithTxn(ctx, jobRecord, txn)
					if err != nil {
						return err
					}
					scDesc.MutationJobs = append(scDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
						MutationID: mutationID,
						JobID:      *job.ID(),
					})
				}
			}
			i++
		}
		if i == 0 {
			// The table descriptor is unchanged. Don't let Publish() increment
			// the version.
			return errDidntUpdateDescriptor
		}
		// Trim the executed mutations from the descriptor.
		scDesc.Mutations = scDesc.Mutations[i:]

		for i, g := range scDesc.MutationJobs {
			if g.MutationID == sc.mutationID {
				// Trim the executed mutation group from the descriptor.
				scDesc.MutationJobs = append(scDesc.MutationJobs[:i], scDesc.MutationJobs[i+1:]...)
				break
			}
		}
		return nil
	}

	descs, err := sc.leaseMgr.PublishMultiple(ctx, tableIDsToUpdate, update, func(txn *kv.Txn) error {
		schemaChangeEventType := EventLogFinishSchemaChange
		if isRollback {
			schemaChangeEventType = EventLogFinishSchemaRollback
		}

		// Log "Finish Schema Change" or "Finish Schema Change Rollback"
		// event. Only the table ID and mutation ID are logged; this can
		// be correlated with the DDL statement that initiated the change
		// using the mutation id.
		return MakeEventLogger(sc.execCfg).InsertEventRecord(
			ctx,
			txn,
			schemaChangeEventType,
			int32(sc.tableID),
			int32(sc.nodeID),
			struct {
				MutationID uint32
			}{uint32(sc.mutationID)},
		)
	})
	if err != nil {
		return nil, err
	}
	return descs[sc.tableID], nil
}

// notFirstInLine returns true whenever the schema change has been queued
// up for execution after another schema change.
func (sc *SchemaChanger) notFirstInLine(
	ctx context.Context,
) (*sqlbase.TableDescriptor, bool, error) {
	var notFirst bool
	var desc *sqlbase.TableDescriptor
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		notFirst = false
		var err error
		desc, err = sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		for i, mutation := range desc.Mutations {
			if mutation.MutationID == sc.mutationID {
				notFirst = i != 0
				break
			}
		}
		return nil
	})
	return desc, notFirst, err
}

// runStateMachineAndBackfill runs the schema change state machine followed by
// the backfill.
func (sc *SchemaChanger) runStateMachineAndBackfill(ctx context.Context) error {
	if fn := sc.testingKnobs.RunBeforePublishWriteAndDelete; fn != nil {
		fn()
	}
	// Run through mutation state machine before backfill.
	if err := sc.RunStateMachineBeforeBackfill(ctx); err != nil {
		return err
	}

	// Run backfill(s).
	if err := sc.runBackfill(ctx); err != nil {
		return err
	}

	// Mark the mutations as completed.
	_, err := sc.done(ctx)
	return err
}

func (sc *SchemaChanger) refreshStats() {
	// Initiate an asynchronous run of CREATE STATISTICS. We use a large number
	// for rowsAffected because we want to make sure that stats always get
	// created/refreshed here.
	sc.execCfg.StatsRefresher.NotifyMutation(sc.tableID, math.MaxInt32 /* rowsAffected */)
}

// reverseMutations reverses the direction of all the mutations with the
// mutationID. This is called after hitting an irrecoverable error while
// applying a schema change. If a column being added is reversed and dropped,
// all new indexes referencing the column will also be dropped.
func (sc *SchemaChanger) reverseMutations(ctx context.Context, causingError error) error {
	// Get the other tables whose foreign key backreferences need to be removed.
	var fksByBackrefTable map[sqlbase.ID][]*sqlbase.ConstraintToUpdate
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		fksByBackrefTable = make(map[sqlbase.ID][]*sqlbase.ConstraintToUpdate)
		var err error
		desc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}
			if constraint := mutation.GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
				mutation.Direction == sqlbase.DescriptorMutation_ADD &&
				constraint.ForeignKey.Validity == sqlbase.ConstraintValidity_Validating {
				fk := &constraint.ForeignKey
				if fk.ReferencedTableID != desc.ID {
					fksByBackrefTable[constraint.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[constraint.ForeignKey.ReferencedTableID], constraint)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	tableIDsToUpdate := make([]sqlbase.ID, 0, len(fksByBackrefTable)+1)
	tableIDsToUpdate = append(tableIDsToUpdate, sc.tableID)
	for id := range fksByBackrefTable {
		tableIDsToUpdate = append(tableIDsToUpdate, id)
	}

	// Create update closure for the table and all other tables with backreferences
	var droppedMutations map[sqlbase.MutationID]struct{}
	update := func(_ *kv.Txn, descs map[sqlbase.ID]*sqlbase.MutableTableDescriptor) error {
		scDesc, ok := descs[sc.tableID]
		if !ok {
			return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
		}
		// Keep track of the column mutations being reversed so that indexes
		// referencing them can be dropped.
		columns := make(map[string]struct{})
		droppedMutations = nil

		for i, mutation := range scDesc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Only reverse the first set of mutations if they have the
				// mutation ID we're looking for.
				if i == 0 {
					return errDidntUpdateDescriptor
				}
				break
			}

			if mutation.Rollback {
				// Can actually never happen. This prevents a rollback of
				// an already rolled back mutation.
				return errors.AssertionFailedf("mutation already rolled back: %v", mutation)
			}

			log.Warningf(ctx, "reverse schema change mutation: %+v", mutation)
			scDesc.Mutations[i], columns = sc.reverseMutation(mutation, false /*notStarted*/, columns)

			// If the mutation is for validating a constraint that is being added,
			// drop the constraint because validation has failed
			if constraint := mutation.GetConstraint(); constraint != nil &&
				mutation.Direction == sqlbase.DescriptorMutation_ADD {
				log.Warningf(ctx, "dropping constraint %+v", constraint)
				if err := sc.maybeDropValidatingConstraint(ctx, scDesc, constraint); err != nil {
					return err
				}
				// Get the foreign key backreferences to remove.
				if constraint.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY {
					fk := &constraint.ForeignKey
					backrefTable, ok := descs[fk.ReferencedTableID]
					if !ok {
						return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
					}
					if err := removeFKBackReferenceFromTable(backrefTable, fk.Name, scDesc.TableDesc()); err != nil {
						return err
					}
				}
			}
			scDesc.Mutations[i].Rollback = true
		}

		// Delete all mutations that reference any of the reversed columns
		// by running a graph traversal of the mutations.
		if len(columns) > 0 {
			var err error
			droppedMutations, err = sc.deleteIndexMutationsWithReversedColumns(ctx, scDesc, columns)
			if err != nil {
				return err
			}
		}

		// PublishMultiple() will increment the version.
		return nil
	}

	_, err = sc.leaseMgr.PublishMultiple(ctx, tableIDsToUpdate, update, func(txn *kv.Txn) error {
		// Read the table descriptor from the store. The Version of the
		// descriptor has already been incremented in the transaction and
		// this descriptor can be modified without incrementing the version.
		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		// Mark the schema change job as failed and create a rollback job.
		err = sc.updateJobForRollback(ctx, txn, tableDesc)
		if err != nil {
			return err
		}

		// Mark other reversed mutation jobs as failed.
		for m := range droppedMutations {
			_, err := markJobFailed(ctx, txn, tableDesc, m, sc.jobRegistry, causingError)
			if err != nil {
				return err
			}
		}

		// Log "Reverse Schema Change" event. Only the causing error and the
		// mutation ID are logged; this can be correlated with the DDL statement
		// that initiated the change using the mutation id.
		return MakeEventLogger(sc.execCfg).InsertEventRecord(
			ctx,
			txn,
			EventLogReverseSchemaChange,
			int32(sc.tableID),
			int32(sc.nodeID),
			struct {
				Error      string
				MutationID uint32
			}{fmt.Sprintf("%+v", causingError), uint32(sc.mutationID)},
		)
	})
	if err != nil {
		return err
	}

	if err := sc.waitToUpdateLeases(ctx, sc.tableID); err != nil {
		return err
	}
	for id := range fksByBackrefTable {
		if err := sc.waitToUpdateLeases(ctx, id); err != nil {
			return err
		}
	}

	return nil
}

// Mark the job associated with the mutation as failed.
func markJobFailed(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *sqlbase.TableDescriptor,
	mutationID sqlbase.MutationID,
	jobRegistry *jobs.Registry,
	causingError error,
) (*jobs.Job, error) {
	// Mark job as failed.
	jobID, err := getJobIDForMutationWithDescriptor(ctx, tableDesc, mutationID)
	if err != nil {
		return nil, err
	}
	job, err := jobRegistry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return nil, err
	}
	err = job.WithTxn(txn).Failed(ctx, causingError, nil)
	return job, err
}

// updateJobForRollback updates the schema change job in the case of a rollback.
func (sc *SchemaChanger) updateJobForRollback(
	ctx context.Context, txn *kv.Txn, tableDesc *sqlbase.TableDescriptor,
) error {
	// Initialize refresh spans to scan the entire table.
	span := tableDesc.PrimaryIndexSpan()
	var spanList []jobspb.ResumeSpanList
	for _, m := range tableDesc.Mutations {
		if m.MutationID == sc.mutationID {
			spanList = append(spanList,
				jobspb.ResumeSpanList{
					ResumeSpans: []roachpb.Span{span},
				},
			)
		}
	}
	if err := sc.job.WithTxn(txn).SetDetails(
		ctx, jobspb.SchemaChangeDetails{
			TableID:        sc.tableID,
			MutationID:     sc.mutationID,
			ResumeSpanList: spanList,
		},
	); err != nil {
		return err
	}
	if err := sc.job.WithTxn(txn).SetProgress(ctx, jobspb.SchemaChangeProgress{}); err != nil {
		return err
	}
	// Set the transaction back to nil so that this job can be used in other
	// transactions.
	sc.job.WithTxn(nil)

	return nil
}

func (sc *SchemaChanger) maybeDropValidatingConstraint(
	ctx context.Context, desc *MutableTableDescriptor, constraint *sqlbase.ConstraintToUpdate,
) error {
	switch constraint.ConstraintType {
	case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
		if constraint.Check.Validity == sqlbase.ConstraintValidity_Unvalidated {
			return nil
		}
		for j, c := range desc.Checks {
			if c.Name == constraint.Check.Name {
				desc.Checks = append(desc.Checks[:j], desc.Checks[j+1:]...)
				return nil
			}
		}
		if log.V(2) {
			log.Infof(
				ctx,
				"attempted to drop constraint %s, but it hadn't been added to the table descriptor yet",
				constraint.Check.Name,
			)
		}
	case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
		for i, fk := range desc.OutboundFKs {
			if fk.Name == constraint.ForeignKey.Name {
				desc.OutboundFKs = append(desc.OutboundFKs[:i], desc.OutboundFKs[i+1:]...)
				return nil
			}
		}
		if log.V(2) {
			log.Infof(
				ctx,
				"attempted to drop constraint %s, but it hadn't been added to the table descriptor yet",
				constraint.ForeignKey.Name,
			)
		}
	default:
		return errors.AssertionFailedf("unsupported constraint type: %d", errors.Safe(constraint.ConstraintType))
	}
	return nil
}

// deleteIndexMutationsWithReversedColumns deletes mutations with a
// different mutationID than the schema changer and with an index that
// references one of the reversed columns. Execute this as a breadth
// first search graph traversal.
func (sc *SchemaChanger) deleteIndexMutationsWithReversedColumns(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor, columns map[string]struct{},
) (map[sqlbase.MutationID]struct{}, error) {
	dropMutations := make(map[sqlbase.MutationID]struct{})
	// Run breadth first search traversal that reverses mutations
	for {
		start := len(dropMutations)
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				if idx := mutation.GetIndex(); idx != nil {
					for _, name := range idx.ColumnNames {
						if _, ok := columns[name]; ok {
							// Such an index mutation has to be with direction ADD and
							// in the DELETE_ONLY state. Live indexes referencing live
							// columns cannot be deleted and thus never have direction
							// DROP. All mutations with the ADD direction start off in
							// the DELETE_ONLY state.
							if mutation.Direction != sqlbase.DescriptorMutation_ADD ||
								mutation.State != sqlbase.DescriptorMutation_DELETE_ONLY {
								panic(fmt.Sprintf("mutation in bad state: %+v", mutation))
							}
							log.Warningf(ctx, "drop schema change mutation: %+v", mutation)
							dropMutations[mutation.MutationID] = struct{}{}
							break
						}
					}
				}
			}
		}

		if len(dropMutations) == start {
			// No more mutations to drop.
			break
		}
		// Drop mutations.
		newMutations := make([]sqlbase.DescriptorMutation, 0, len(desc.Mutations))
		for _, mutation := range desc.Mutations {
			if _, ok := dropMutations[mutation.MutationID]; ok {
				// Reverse mutation. Update columns to reflect additional
				// columns that have been purged. This mutation doesn't need
				// a rollback because it was not started.
				mutation, columns = sc.reverseMutation(mutation, true /*notStarted*/, columns)
				// Mark as complete because this mutation needs no backfill.
				if err := desc.MakeMutationComplete(mutation); err != nil {
					return nil, err
				}
			} else {
				newMutations = append(newMutations, mutation)
			}
		}
		// Reset mutations.
		desc.Mutations = newMutations
	}
	return dropMutations, nil
}

// Reverse a mutation. Returns the updated mutation and updated columns.
// notStarted is set to true only if the schema change state machine
// was not started for the mutation.
func (sc *SchemaChanger) reverseMutation(
	mutation sqlbase.DescriptorMutation, notStarted bool, columns map[string]struct{},
) (sqlbase.DescriptorMutation, map[string]struct{}) {
	switch mutation.Direction {
	case sqlbase.DescriptorMutation_ADD:
		mutation.Direction = sqlbase.DescriptorMutation_DROP
		// A column ADD being reversed gets placed in the map.
		if col := mutation.GetColumn(); col != nil {
			columns[col.Name] = struct{}{}
		}
		// PrimaryKeySwap doesn't have a concept of the state machine.
		if pkSwap := mutation.GetPrimaryKeySwap(); pkSwap != nil {
			return mutation, columns
		}
		if notStarted && mutation.State != sqlbase.DescriptorMutation_DELETE_ONLY {
			panic(fmt.Sprintf("mutation in bad state: %+v", mutation))
		}

	case sqlbase.DescriptorMutation_DROP:
		mutation.Direction = sqlbase.DescriptorMutation_ADD
		if notStarted && mutation.State != sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
			panic(fmt.Sprintf("mutation in bad state: %+v", mutation))
		}
	}
	return mutation, columns
}

// GCJobTestingKnobs is for testing the Schema Changer GC job.
// Note that this is defined here for testing purposes to avoid cyclic
// dependencies.
type GCJobTestingKnobs struct {
	RunBeforeResume func()
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*GCJobTestingKnobs) ModuleTestingKnobs() {}

// SchemaChangerTestingKnobs for testing the schema change execution path
// through both the synchronous and asynchronous paths.
type SchemaChangerTestingKnobs struct {
	// SchemaChangeJobNoOp returning true will cause the job to be a no-op.
	SchemaChangeJobNoOp func() bool

	// RunBeforePublishWriteAndDelete is called just before publishing the
	// write+delete state for the schema change.
	RunBeforePublishWriteAndDelete func()

	// RunBeforeBackfill is called just before starting the backfill.
	RunBeforeBackfill func() error

	// RunBeforeIndexBackfill is called just before starting the index backfill, after
	// fixing the index backfill scan timestamp.
	RunBeforeIndexBackfill func()

	// RunBeforePrimaryKeySwap is called just before the primary key swap is committed.
	RunBeforePrimaryKeySwap func()

	// RunBeforeIndexValidation is called just before starting the index validation,
	// after setting the job status to validating.
	RunBeforeIndexValidation func() error

	// RunBeforeConstraintValidation is called just before starting the checks validation,
	// after setting the job status to validating.
	RunBeforeConstraintValidation func() error

	// OldNamesDrainedNotification is called during a schema change,
	// after all leases on the version of the descriptor with the old
	// names are gone, and just before the mapping of the old names to the
	// descriptor id are about to be deleted.
	OldNamesDrainedNotification func()

	// WriteCheckpointInterval is the interval after which a checkpoint is
	// written.
	WriteCheckpointInterval time.Duration

	// BackfillChunkSize is to be used for all backfill chunked operations.
	BackfillChunkSize int64

	// TwoVersionLeaseViolation is called whenever a schema change
	// transaction is unable to commit because it is violating the two
	// version lease invariant.
	TwoVersionLeaseViolation func()
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*SchemaChangerTestingKnobs) ModuleTestingKnobs() {}

// createSchemaChangeEvalCtx creates an extendedEvalContext() to be used for backfills.
//
// TODO(andrei): This EvalContext() will be broken for backfills trying to use
// functions marked with distsqlBlacklist.
// Also, the SessionTracing inside the context is unrelated to the one
// used in the surrounding SQL session, so session tracing is unable
// to capture schema change activity.
func createSchemaChangeEvalCtx(
	ctx context.Context, ts hlc.Timestamp, ieFactory sqlutil.SessionBoundInternalExecutorFactory,
) extendedEvalContext {
	dummyLocation := time.UTC

	sd := &sessiondata.SessionData{
		SearchPath: sqlbase.DefaultSearchPath,
		// The database is not supposed to be needed in schema changes, as there
		// shouldn't be unqualified identifiers in backfills, and the pure functions
		// that need it should have already been evaluated.
		//
		// TODO(andrei): find a way to assert that this field is indeed not used.
		// And in fact it is used by `current_schemas()`, which, although is a pure
		// function, takes arguments which might be impure (so it can't always be
		// pre-evaluated).
		Database:      "",
		SequenceState: sessiondata.NewSequenceState(),
		DataConversion: sessiondata.DataConversionConfig{
			Location: dummyLocation,
		},
		User: security.NodeUser,
	}

	evalCtx := extendedEvalContext{
		// Make a session tracing object on-the-fly. This is OK
		// because it sets "enabled: false" and thus none of the
		// other fields are used.
		Tracing: &SessionTracing{},
		EvalContext: tree.EvalContext{
			SessionData:      sd,
			InternalExecutor: ieFactory(ctx, sd),
			// TODO(andrei): This is wrong (just like on the main code path on
			// setupFlow). Each processor should override Ctx with its own context.
			Context:            ctx,
			Sequence:           &sqlbase.DummySequenceOperators{},
			Planner:            &sqlbase.DummyEvalPlanner{},
			SessionAccessor:    &sqlbase.DummySessionAccessor{},
			PrivilegedAccessor: &sqlbase.DummyPrivilegedAccessor{},
		},
	}
	// The backfill is going to use the current timestamp for the various
	// functions, like now(), that need it.  It's possible that the backfill has
	// been partially performed already by another SchemaChangeManager with
	// another timestamp.
	//
	// TODO(andrei): Figure out if this is what we want, and whether the
	// timestamp from the session that enqueued the schema change
	// is/should be used for impure functions like now().
	evalCtx.SetTxnTimestamp(timeutil.Unix(0 /* sec */, ts.WallTime))
	evalCtx.SetStmtTimestamp(timeutil.Unix(0 /* sec */, ts.WallTime))

	return evalCtx
}

type schemaChangeResumer struct {
	job *jobs.Job
}

func (r schemaChangeResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(PlanHookState)
	details := r.job.Details().(jobspb.SchemaChangeDetails)
	if p.ExecCfg().SchemaChangerTestingKnobs.SchemaChangeJobNoOp != nil &&
		p.ExecCfg().SchemaChangerTestingKnobs.SchemaChangeJobNoOp() {
		return nil
	}

	execSchemaChange := func(tableID sqlbase.ID, mutationID sqlbase.MutationID, droppedDatabaseID sqlbase.ID) error {
		sc := SchemaChanger{
			tableID:              tableID,
			mutationID:           mutationID,
			droppedDatabaseID:    droppedDatabaseID,
			nodeID:               p.ExecCfg().NodeID.Get(),
			db:                   p.ExecCfg().DB,
			leaseMgr:             p.ExecCfg().LeaseManager,
			testingKnobs:         p.ExecCfg().SchemaChangerTestingKnobs,
			distSQLPlanner:       p.DistSQLPlanner(),
			jobRegistry:          p.ExecCfg().JobRegistry,
			job:                  r.job,
			rangeDescriptorCache: p.ExecCfg().RangeDescriptorCache,
			leaseHolderCache:     p.ExecCfg().LeaseHolderCache,
			clock:                p.ExecCfg().Clock,
			settings:             p.ExecCfg().Settings,
			execCfg:              p.ExecCfg(),
			ieFactory: func(ctx context.Context, sd *sessiondata.SessionData) sqlutil.InternalExecutor {
				return r.job.MakeSessionBoundInternalExecutor(ctx, sd)
			},
		}
		if err := sc.exec(ctx); err != nil {
			switch {
			case errors.Is(err, sqlbase.ErrDescriptorNotFound):
				// If the table descriptor for the ID can't be found, we assume that
				// another job to drop the table got to it first, and consider this job
				// finished.
				log.Infof(
					ctx,
					"descriptor %d not found for schema change processing mutation %d;"+
						"assuming it was dropped, and exiting",
					tableID, mutationID,
				)
				return nil
			case ctx.Err() != nil:
				// If the context was canceled, the job registry will retry the job.
				// We check for this case so that we can just return the error without
				// wrapping it in a retry error.
				return err
			case !isPermanentSchemaChangeError(err):
				// Check if the error is on a whitelist of errors we should retry on,
				// including the schema change not having the first mutation in line,
				// and have the job registry retry.
				return jobs.NewRetryJobError(err.Error())
			default:
				// All other errors lead to a failed job.
				return err
			}
		}
		return nil
	}

	// If a database is being dropped, handle this separately by draining names
	// for all the tables.
	if details.DroppedDatabaseID != sqlbase.InvalidID {
		for i := range details.DroppedTables {
			droppedTable := &details.DroppedTables[i]
			if err := execSchemaChange(droppedTable.ID, sqlbase.InvalidMutationID, details.DroppedDatabaseID); err != nil {
				return err
			}
		}
		dropTime := timeutil.Now().UnixNano()
		tablesToGC := make([]jobspb.SchemaChangeGCDetails_DroppedID, len(details.DroppedTables))
		for i, table := range details.DroppedTables {
			tablesToGC[i] = jobspb.SchemaChangeGCDetails_DroppedID{ID: table.ID, DropTime: dropTime}
		}
		databaseGCDetails := jobspb.SchemaChangeGCDetails{
			Tables:   tablesToGC,
			ParentID: details.DroppedDatabaseID,
		}
		return startGCJob(ctx, p.ExecCfg().DB, p.ExecCfg().JobRegistry, r.job.Payload().Username, r.job.Payload().Description, databaseGCDetails)
	}
	if details.TableID == sqlbase.InvalidID {
		return errors.AssertionFailedf("job has no database ID or table ID")
	}
	return execSchemaChange(details.TableID, details.MutationID, details.DroppedDatabaseID)
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r schemaChangeResumer) OnFailOrCancel(ctx context.Context, phs interface{}) error {
	p := phs.(PlanHookState)
	details := r.job.Details().(jobspb.SchemaChangeDetails)

	if details.DroppedDatabaseID != sqlbase.InvalidID {
		// TODO (lucy): Do we need to do anything here?
		return nil
	}
	if details.TableID == sqlbase.InvalidID {
		return errors.AssertionFailedf("job has no database ID or table ID")
	}
	sc := SchemaChanger{
		tableID:              details.TableID,
		mutationID:           details.MutationID,
		nodeID:               p.ExecCfg().NodeID.Get(),
		db:                   p.ExecCfg().DB,
		leaseMgr:             p.ExecCfg().LeaseManager,
		testingKnobs:         p.ExecCfg().SchemaChangerTestingKnobs,
		distSQLPlanner:       p.DistSQLPlanner(),
		jobRegistry:          p.ExecCfg().JobRegistry,
		job:                  r.job,
		rangeDescriptorCache: p.ExecCfg().RangeDescriptorCache,
		leaseHolderCache:     p.ExecCfg().LeaseHolderCache,
		clock:                p.ExecCfg().Clock,
		settings:             p.ExecCfg().Settings,
		execCfg:              p.ExecCfg(),
		ieFactory: func(ctx context.Context, sd *sessiondata.SessionData) sqlutil.InternalExecutor {
			return r.job.MakeSessionBoundInternalExecutor(ctx, sd)
		},
	}

	if r.job.Payload().FinalResumeError == nil {
		return errors.AssertionFailedf("job failed but had no recorded error")
	}
	scErr := errors.DecodeError(ctx, *r.job.Payload().FinalResumeError)

	if rollbackErr := sc.handlePermanentSchemaChangeError(ctx, scErr, p.ExtendedEvalContext()); rollbackErr != nil {
		switch {
		case errors.Is(rollbackErr, sqlbase.ErrDescriptorNotFound):
			// If the table descriptor for the ID can't be found, we assume that
			// another job to drop the table got to it first, and consider this job
			// finished.
			log.Infof(
				ctx,
				"descriptor %d not found for rollback of schema change processing mutation %d;"+
					"assuming it was dropped, and exiting",
				details.TableID, details.MutationID,
			)
		case ctx.Err() != nil:
			// If the context was canceled, the job registry will retry the job.
			// We check for this case so that we can just return the error without
			// wrapping it in a retry error.
			return rollbackErr
		default:
			// Always retry when we get any other error. Otherwise we risk leaving the
			// in-progress schema change state on the table descriptor indefinitely.
			// Note that, in theory, this could mean retrying the job forever even for
			// an error we can't recover from, if there's a bug.
			return jobs.NewRetryJobError(rollbackErr.Error())
		}
	}
	return nil
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &schemaChangeResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeSchemaChange, createResumerFn)
}
