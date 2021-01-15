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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
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
	descID            descpb.ID
	mutationID        descpb.MutationID
	droppedDatabaseID descpb.ID
	sqlInstanceID     base.SQLInstanceID
	db                *kv.DB
	leaseMgr          *lease.Manager

	metrics *SchemaChangerMetrics

	testingKnobs   *SchemaChangerTestingKnobs
	distSQLPlanner *DistSQLPlanner
	jobRegistry    *jobs.Registry
	// Keep a reference to the job related to this schema change
	// so that we don't need to read the job again while updating
	// the status of the job.
	job *jobs.Job
	// Caches updated by DistSQL.
	rangeDescriptorCache *rangecache.RangeCache
	clock                *hlc.Clock
	settings             *cluster.Settings
	execCfg              *ExecutorConfig
	ieFactory            sqlutil.SessionBoundInternalExecutorFactory
}

// NewSchemaChangerForTesting only for tests.
func NewSchemaChangerForTesting(
	tableID descpb.ID,
	mutationID descpb.MutationID,
	sqlInstanceID base.SQLInstanceID,
	db kv.DB,
	leaseMgr *lease.Manager,
	jobRegistry *jobs.Registry,
	execCfg *ExecutorConfig,
	settings *cluster.Settings,
) SchemaChanger {
	return SchemaChanger{
		descID:        tableID,
		mutationID:    mutationID,
		sqlInstanceID: sqlInstanceID,
		db:            &db,
		leaseMgr:      leaseMgr,
		jobRegistry:   jobRegistry,
		settings:      settings,
		execCfg:       execCfg,
		// Note that this doesn't end up actually being session-bound but that's
		// good enough for testing.
		ieFactory: func(
			ctx context.Context, sd *sessiondata.SessionData,
		) sqlutil.InternalExecutor {
			return execCfg.InternalExecutor
		},
		metrics: NewSchemaChangerMetrics(),
	}
}

// isPermanentSchemaChangeError returns true if the error results in
// a permanent failure of a schema change. This function is a allowlist
// instead of a blocklist: only known safe errors are confirmed to not be
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
	if errors.HasType(err, (*roachpb.BatchTimestampBeforeGCError)(nil)) {
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
	version  descpb.DescriptorVersion
	expected descpb.DescriptorVersion
}

var errTableVersionMismatchSentinel = errTableVersionMismatch{}

func makeErrTableVersionMismatch(version, expected descpb.DescriptorVersion) error {
	return errors.Mark(errors.WithStack(errTableVersionMismatch{
		version:  version,
		expected: expected,
	}), errTableVersionMismatchSentinel)
}

func (e errTableVersionMismatch) Error() string {
	return fmt.Sprintf("table version mismatch: %d, expected: %d", e.version, e.expected)
}

// refreshMaterializedView updates the physical data for a materialized view.
func (sc *SchemaChanger) refreshMaterializedView(
	ctx context.Context, table *tabledesc.Mutable, refresh *descpb.MaterializedViewRefresh,
) error {
	// If we aren't requested to backfill any data, then return immediately.
	if !refresh.ShouldBackfill {
		return nil
	}
	// The data for the materialized view is stored under the current set of
	// indexes in table. We want to keep all of that data untouched, and write
	// out all the data into the new set of indexes denoted by refresh. So, just
	// perform some surgery on the input table to denote it as having the desired
	// set of indexes. We then backfill into this modified table, which writes
	// data only to the new desired indexes. In SchemaChanger.done(), we'll swap
	// the indexes from the old versions into the new ones.
	tableToRefresh := protoutil.Clone(table.TableDesc()).(*descpb.TableDescriptor)
	tableToRefresh.PrimaryIndex = refresh.NewPrimaryIndex
	tableToRefresh.Indexes = refresh.NewIndexes
	return sc.backfillQueryIntoTable(ctx, tableToRefresh, table.ViewQuery, refresh.AsOf, "refreshView")
}

func (sc *SchemaChanger) backfillQueryIntoTable(
	ctx context.Context, table *descpb.TableDescriptor, query string, ts hlc.Timestamp, desc string,
) error {
	if fn := sc.testingKnobs.RunBeforeQueryBackfill; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}

	return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetFixedTimestamp(ctx, ts)

		// Create an internal planner as the planner used to serve the user query
		// would have committed by this point.
		p, cleanup := NewInternalPlanner(desc, txn, security.RootUserName(), &MemoryMetrics{}, sc.execCfg, sessiondatapb.SessionData{})
		defer cleanup()
		localPlanner := p.(*planner)
		stmt, err := parser.ParseOne(query)
		if err != nil {
			return err
		}

		// Construct an optimized logical plan of the AS source stmt.
		localPlanner.stmt = makeStatement(stmt, ClusterWideID{} /* queryID */)
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
			txn,
			sc.clock,
			// Make a session tracing object on-the-fly. This is OK
			// because it sets "enabled: false" and thus none of the
			// other fields are used.
			&SessionTracing{},
		)
		defer recv.Release()

		var planAndRunErr error
		localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
			// Resolve subqueries before running the queries' physical plan.
			if len(localPlanner.curPlan.subqueryPlans) != 0 {
				if !sc.distSQLPlanner.PlanAndRunSubqueries(
					ctx, localPlanner, localPlanner.ExtendedEvalContextCopy,
					localPlanner.curPlan.subqueryPlans, recv,
				) {
					if planAndRunErr = rw.Err(); planAndRunErr != nil {
						return
					}
					if planAndRunErr = recv.commErr; planAndRunErr != nil {
						return
					}
				}
			}

			isLocal := !getPlanDistribution(
				ctx, localPlanner, localPlanner.execCfg.NodeID,
				localPlanner.extendedEvalCtx.SessionData.DistSQLMode,
				localPlanner.curPlan.main,
			).WillDistribute()
			out := execinfrapb.ProcessorCoreUnion{BulkRowWriter: &execinfrapb.BulkRowWriterSpec{
				Table: *table,
			}}

			PlanAndRunCTAS(ctx, sc.distSQLPlanner, localPlanner,
				txn, isLocal, localPlanner.curPlan.main, out, recv)
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

// maybe backfill a created table by executing the AS query. Return nil if
// successfully backfilled.
//
// Note that this does not connect to the tracing settings of the
// surrounding SQL transaction. This should be OK as (at the time of
// this writing) this code path is only used for standalone CREATE
// TABLE AS statements, which cannot be traced.
func (sc *SchemaChanger) maybeBackfillCreateTableAs(
	ctx context.Context, table *tabledesc.Immutable,
) error {
	if !(table.Adding() && table.IsAs()) {
		return nil
	}
	log.Infof(ctx, "starting backfill for CREATE TABLE AS with query %q", table.CreateQuery)

	return sc.backfillQueryIntoTable(ctx, table.TableDesc(), table.CreateQuery, table.CreateAsOfTime, "ctasBackfill")
}

func (sc *SchemaChanger) maybeBackfillMaterializedView(
	ctx context.Context, table *tabledesc.Immutable,
) error {
	if !(table.Adding() && table.MaterializedView()) {
		return nil
	}
	log.Infof(ctx, "starting backfill for CREATE MATERIALIZED VIEW with query %q", table.ViewQuery)

	return sc.backfillQueryIntoTable(ctx, table.TableDesc(), table.ViewQuery, table.CreateAsOfTime, "materializedViewBackfill")
}

// maybe make a table PUBLIC if it's in the ADD state.
func (sc *SchemaChanger) maybeMakeAddTablePublic(
	ctx context.Context, table *tabledesc.Immutable,
) error {
	if !table.Adding() {
		return nil
	}
	log.Info(ctx, "making table public")

	fks := table.AllActiveAndInactiveForeignKeys()
	for _, fk := range fks {
		if err := WaitToUpdateLeases(ctx, sc.leaseMgr, fk.ReferencedTableID); err != nil {
			return err
		}
	}

	return sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		mut, err := descsCol.GetMutableTableVersionByID(ctx, table.ID, txn)
		if err != nil {
			return err
		}
		if !mut.Adding() {
			return nil
		}
		mut.State = descpb.DescriptorState_PUBLIC
		return descsCol.WriteDesc(ctx, true /* kvTrace */, mut, txn)
	})
}

// drainNamesForDescriptor will drain remove the draining names from the
// descriptor with the specified ID. If it is a schema, it will also remove the
// names from the parent database.
//
// If there are no draining names, this call will not update any descriptors.
func drainNamesForDescriptor(
	ctx context.Context,
	settings *cluster.Settings,
	descID descpb.ID,
	db *kv.DB,
	ie sqlutil.InternalExecutor,
	leaseMgr *lease.Manager,
	codec keys.SQLCodec,
	beforeDrainNames func(),
) error {
	log.Info(ctx, "draining previous names")
	// Publish a new version with all the names drained after everyone
	// has seen the version with the new name. All the draining names
	// can be reused henceforth.
	run := func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		if beforeDrainNames != nil {
			beforeDrainNames()
		}

		// Free up the old name(s) for reuse.
		mutDesc, err := descsCol.GetMutableDescriptorByID(ctx, descID, txn)
		if err != nil {
			return err
		}
		namesToReclaim := mutDesc.GetDrainingNames()
		if len(namesToReclaim) == 0 {
			return nil
		}
		b := txn.NewBatch()
		mutDesc.SetDrainingNames(nil)

		// Reclaim all old names.
		for _, drain := range namesToReclaim {
			catalogkv.WriteObjectNamespaceEntryRemovalToBatch(
				ctx, b, codec, drain.ParentID, drain.ParentSchemaID, drain.Name, false, /* KVTrace */
			)
		}

		// If the descriptor to drain is a schema, then we need to delete the
		// draining names from the parent database's schema mapping.
		if _, isSchema := mutDesc.(catalog.SchemaDescriptor); isSchema {
			mutDB, err := descsCol.GetMutableDescriptorByID(ctx, mutDesc.GetParentID(), txn)
			if err != nil {
				return err
			}
			db := mutDB.(*dbdesc.Mutable)
			for _, name := range namesToReclaim {
				delete(db.Schemas, name.Name)
			}
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, db, b,
			); err != nil {
				return err
			}
		}
		if err := descsCol.WriteDescToBatch(
			ctx, false /* kvTrace */, mutDesc, b,
		); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	}
	return descs.Txn(ctx, settings, leaseMgr, ie, db, run)
}

func startGCJob(
	ctx context.Context,
	db *kv.DB,
	jobRegistry *jobs.Registry,
	username security.SQLUsername,
	schemaChangeDescription string,
	details jobspb.SchemaChangeGCDetails,
) error {
	var sj *jobs.StartableJob
	jobRecord := CreateGCJobRecord(schemaChangeDescription, username, details)
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		if sj, err = jobRegistry.CreateStartableJobWithTxn(ctx, jobRecord, txn, nil /* resultCh */); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	log.Infof(ctx, "starting GC job %d", *sj.ID())
	if _, err := sj.Start(ctx); err != nil {
		return err
	}
	return nil
}

func (sc *SchemaChanger) execLogTags() *logtags.Buffer {
	buf := &logtags.Buffer{}
	buf = buf.Add("scExec", nil)

	buf = buf.Add("id", sc.descID)
	if sc.mutationID != descpb.InvalidMutationID {
		buf = buf.Add("mutation", sc.mutationID)
	}
	if sc.droppedDatabaseID != descpb.InvalidID {
		buf = buf.Add("db", sc.droppedDatabaseID)
	}
	return buf
}

// notFirstInLine checks if that this schema changer is at the front of the line
// to execute if the target descriptor is a table. It returns an error if this
// schema changer needs to wait.
func (sc *SchemaChanger) notFirstInLine(ctx context.Context, desc catalog.Descriptor) error {
	if tableDesc, ok := desc.(catalog.TableDescriptor); ok {
		// TODO (lucy): Now that marking a schema change job as succeeded doesn't
		// happen in the same transaction as removing mutations from a table
		// descriptor, it seems possible for a job to be resumed after the mutation
		// has already been removed. If there's a mutation provided, we should check
		// whether it actually exists on the table descriptor and exit the job if not.
		for i, mutation := range tableDesc.TableDesc().Mutations {
			if mutation.MutationID == sc.mutationID {
				if i != 0 {
					log.Infof(ctx,
						"schema change on %q (v%d): another change is still in progress",
						desc.GetName(), desc.GetVersion(),
					)
					return errSchemaChangeNotFirstInLine
				}
				break
			}
		}
	}
	return nil
}

func (sc *SchemaChanger) getTargetDescriptor(ctx context.Context) (catalog.Descriptor, error) {
	// Retrieve the descriptor that is being changed.
	var desc catalog.Descriptor
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		desc, err = catalogkv.GetDescriptorByID(
			ctx,
			txn,
			sc.execCfg.Codec,
			sc.descID,
			catalogkv.Immutable,
			catalogkv.AnyDescriptorKind,
			true, /* required */
		)
		return err
	}); err != nil {
		return nil, err
	}
	return desc, nil
}

// Execute the entire schema change in steps.
// inSession is set to false when this is called from the asynchronous
// schema change execution path.
//
// If the txn that queued the schema changer did not commit, this will be a
// no-op, as we'll fail to find the job for our mutation in the jobs registry.
func (sc *SchemaChanger) exec(ctx context.Context) error {
	sc.metrics.RunningSchemaChanges.Inc(1)
	defer sc.metrics.RunningSchemaChanges.Dec(1)

	ctx = logtags.AddTags(ctx, sc.execLogTags())

	// Pull out the requested descriptor.
	desc, err := sc.getTargetDescriptor(ctx)
	if err != nil {
		return err
	}

	// Check that we aren't queued behind another schema changer.
	if err := sc.notFirstInLine(ctx, desc); err != nil {
		return err
	}

	log.Infof(ctx,
		"schema change on %q (v%d) starting execution...",
		desc.GetName(), desc.GetVersion(),
	)

	// If there are any names to drain, then drain them.
	if len(desc.GetDrainingNames()) > 0 {
		if err := drainNamesForDescriptor(
			ctx, sc.settings, desc.GetID(), sc.db, sc.execCfg.InternalExecutor, sc.leaseMgr,
			sc.execCfg.Codec, sc.testingKnobs.OldNamesDrainedNotification,
		); err != nil {
			return err
		}
	}

	// Wait for the schema change to propagate to all nodes after this function
	// returns, so that the new schema is live everywhere. This is not needed for
	// correctness but is done to make the UI experience/tests predictable.
	waitToUpdateLeases := func(refreshStats bool) error {
		if err := WaitToUpdateLeases(ctx, sc.leaseMgr, sc.descID); err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
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

	tableDesc, ok := desc.(*tabledesc.Immutable)
	if !ok {
		// If our descriptor is not a table, then just drain leases.
		if err := waitToUpdateLeases(false /* refreshStats */); err != nil {
			return err
		}
		// Some descriptors should be deleted if they are in the DROP state.
		switch desc.(type) {
		case catalog.SchemaDescriptor, catalog.DatabaseDescriptor:
			if desc.Dropped() {
				if err := sc.execCfg.DB.Del(ctx, catalogkeys.MakeDescMetadataKey(sc.execCfg.Codec, desc.GetID())); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// Otherwise, continue with the rest of the schema change state machine.
	if tableDesc.Dropped() && sc.droppedDatabaseID == descpb.InvalidID {
		if tableDesc.IsPhysicalTable() {
			// We've dropped this physical table, let's kick off a GC job.
			dropTime := timeutil.Now().UnixNano()
			if tableDesc.TableDesc().DropTime > 0 {
				dropTime = tableDesc.TableDesc().DropTime
			}
			gcDetails := jobspb.SchemaChangeGCDetails{
				Tables: []jobspb.SchemaChangeGCDetails_DroppedID{
					{
						ID:       tableDesc.GetID(),
						DropTime: dropTime,
					},
				},
			}
			if err := startGCJob(
				ctx, sc.db, sc.jobRegistry, sc.job.Payload().UsernameProto.Decode(), sc.job.Payload().Description, gcDetails,
			); err != nil {
				return err
			}
		} else {
			// We've dropped a non-physical table, no need for a GC job, let's delete
			// its descriptor and zone config immediately.
			if err := DeleteTableDescAndZoneConfig(ctx, sc.db, sc.execCfg.Codec, tableDesc); err != nil {
				return err
			}
		}
	}

	if err := sc.maybeBackfillCreateTableAs(ctx, tableDesc); err != nil {
		return err
	}

	if err := sc.maybeBackfillMaterializedView(ctx, tableDesc); err != nil {
		return err
	}

	if err := sc.maybeMakeAddTablePublic(ctx, tableDesc); err != nil {
		return err
	}

	if sc.mutationID == descpb.InvalidMutationID {
		// Nothing more to do.
		isCreateTableAs := tableDesc.Adding() && tableDesc.TableDesc().IsAs()
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
	if err := sc.runStateMachineAndBackfill(ctx); err != nil {
		return err
	}

	defer func() {
		if err := waitToUpdateLeases(err == nil /* refreshStats */); err != nil && !errors.Is(err, catalog.ErrDescriptorNotFound) {
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

	// Ensure that this mutation is first in line prior to reverting.
	{
		// Pull out the requested descriptor.
		desc, descErr := sc.getTargetDescriptor(ctx)
		if descErr != nil {
			return descErr
		}

		// Check that we aren't queued behind another schema changer.
		if err := sc.notFirstInLine(ctx, desc); err != nil {
			return err
		}
	}

	if rollbackErr := sc.rollbackSchemaChange(ctx, err); rollbackErr != nil {
		// From now on, the returned error will be a secondary error of the returned
		// error, so we'll record the original error now.
		secondary := errors.Wrap(err, "original error when rolling back mutations")
		sqltelemetry.RecordError(ctx, secondary, &sc.settings.SV)
		return errors.WithSecondaryError(rollbackErr, secondary)
	}

	// TODO (lucy): This is almost the same as in exec(), maybe refactor.
	// Wait for the schema change to propagate to all nodes after this function
	// returns, so that the new schema is live everywhere. This is not needed for
	// correctness but is done to make the UI experience/tests predictable.
	waitToUpdateLeases := func(refreshStats bool) error {
		if err := WaitToUpdateLeases(ctx, sc.leaseMgr, sc.descID); err != nil {
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
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
		if err := waitToUpdateLeases(false /* refreshStats */); err != nil && !errors.Is(err, catalog.ErrDescriptorNotFound) {
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
		desc, err := catalogkv.MustGetTableDescByID(ctx, txn, sc.execCfg.Codec, sc.descID)
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
			case descpb.DescriptorMutation_ADD:
				switch mutation.State {
				case descpb.DescriptorMutation_DELETE_ONLY:
					runStatus = RunningStatusDeleteOnly
				}

			case descpb.DescriptorMutation_DROP:
				switch mutation.State {
				case descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY:
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
	if errReverse := sc.maybeReverseMutations(ctx, err); errReverse != nil {
		return errReverse
	}

	if fn := sc.testingKnobs.RunAfterMutationReversal; fn != nil {
		if err := fn(*sc.job.ID()); err != nil {
			return err
		}
	}

	// After this point the schema change has been reversed and any retry
	// of the schema change will act upon the reversed schema change.
	if err := sc.runStateMachineAndBackfill(ctx); err != nil {
		return err
	}

	// Check if the target table needs to be cleaned up at all. If the target
	// table was in the ADD state and the schema change failed, then we need to
	// clean up the descriptor.
	var cleanupJob *jobs.StartableJob
	if err := sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		scTable, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}
		if !scTable.Adding() {
			return nil
		}

		b := txn.NewBatch()
		scTable.SetDropped()
		if err := descsCol.WriteDescToBatch(ctx, false /* kvTrace */, scTable, b); err != nil {
			return err
		}
		catalogkv.WriteObjectNamespaceEntryRemovalToBatch(
			ctx,
			b,
			sc.execCfg.Codec,
			scTable.GetParentID(),
			scTable.GetParentSchemaID(),
			scTable.GetName(),
			false, /* kvTrace */
		)

		// Queue a GC job.
		jobRecord := CreateGCJobRecord(
			"ROLLBACK OF "+sc.job.Payload().Description,
			sc.job.Payload().UsernameProto.Decode(),
			jobspb.SchemaChangeGCDetails{
				Tables: []jobspb.SchemaChangeGCDetails_DroppedID{
					{
						ID:       scTable.GetID(),
						DropTime: timeutil.Now().UnixNano(),
					},
				},
			},
		)
		job, err := sc.jobRegistry.CreateStartableJobWithTxn(ctx, jobRecord, txn, nil /* resultsCh */)
		if err != nil {
			return err
		}
		cleanupJob = job
		return txn.Run(ctx, b)
	}); err != nil {
		if cleanupJob != nil {
			if rollbackErr := cleanupJob.CleanupOnRollback(ctx); rollbackErr != nil {
				log.Warningf(ctx, "failed to clean up job: %v", rollbackErr)
			}
		}
		return err
	}
	if cleanupJob != nil {
		if _, err := cleanupJob.Start(ctx); err != nil {
			log.Warningf(ctx, "starting job %d failed with error: %v", *cleanupJob.ID(), err)
		}
		log.VEventf(ctx, 2, "started job %d", *cleanupJob.ID())
	}
	return nil
}

// RunStateMachineBeforeBackfill moves the state machine forward
// and wait to ensure that all nodes are seeing the latest version
// of the table.
func (sc *SchemaChanger) RunStateMachineBeforeBackfill(ctx context.Context) error {
	log.Info(ctx, "stepping through state machine")

	var runStatus jobs.RunningStatus
	if err := sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		tbl, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}
		runStatus = ""
		// Apply mutations belonging to the same version.
		for i, mutation := range tbl.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			switch mutation.Direction {
			case descpb.DescriptorMutation_ADD:
				switch mutation.State {
				case descpb.DescriptorMutation_DELETE_ONLY:
					// TODO(vivek): while moving up the state is appropriate,
					// it will be better to run the backfill of a unique index
					// twice: once in the DELETE_ONLY state to confirm that
					// the index can indeed be created, and subsequently in the
					// DELETE_AND_WRITE_ONLY state to fill in the missing elements of the
					// index (INSERT and UPDATE that happened in the interim).
					tbl.Mutations[i].State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
					runStatus = RunningStatusDeleteAndWriteOnly

				case descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					// The state change has already moved forward.
				}

			case descpb.DescriptorMutation_DROP:
				switch mutation.State {
				case descpb.DescriptorMutation_DELETE_ONLY:
					// The state change has already moved forward.

				case descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					tbl.Mutations[i].State = descpb.DescriptorMutation_DELETE_ONLY
					runStatus = RunningStatusDeleteOnly
				}
			}
		}
		if doNothing := runStatus == "" || tbl.Dropped(); doNothing {
			return nil
		}
		if err := descsCol.WriteDesc(
			ctx, true /* kvTrace */, tbl, txn,
		); err != nil {
			return err
		}
		if sc.job != nil {
			if err := sc.job.WithTxn(txn).RunningStatus(ctx, func(
				ctx context.Context, details jobspb.Details,
			) (jobs.RunningStatus, error) {
				return runStatus, nil
			}); err != nil {
				return errors.Wrap(err, "failed to update job status")
			}
		}
		return nil
	}); err != nil {
		return err
	}

	log.Info(ctx, "finished stepping through state machine")

	// wait for the state change to propagate to all leases.
	return WaitToUpdateLeases(ctx, sc.leaseMgr, sc.descID)
}

func (sc *SchemaChanger) createIndexGCJob(
	ctx context.Context, index *descpb.IndexDescriptor, txn *kv.Txn, jobDesc string,
) (*jobs.StartableJob, error) {
	dropTime := timeutil.Now().UnixNano()
	indexGCDetails := jobspb.SchemaChangeGCDetails{
		Indexes: []jobspb.SchemaChangeGCDetails_DroppedIndex{
			{
				IndexID:  index.ID,
				DropTime: dropTime,
			},
		},
		ParentID: sc.descID,
	}

	gcJobRecord := CreateGCJobRecord(jobDesc, sc.job.Payload().UsernameProto.Decode(), indexGCDetails)
	indexGCJob, err := sc.jobRegistry.CreateStartableJobWithTxn(ctx, gcJobRecord, txn, nil /* resultsCh */)
	if err != nil {
		return nil, err
	}
	log.VEventf(ctx, 2, "created index GC job %d", *indexGCJob.ID())
	return indexGCJob, nil
}

// WaitToUpdateLeases until the entire cluster has been updated to the latest
// version of the descriptor.
func WaitToUpdateLeases(ctx context.Context, leaseMgr *lease.Manager, descID descpb.ID) error {
	// Aggressively retry because there might be a user waiting for the
	// schema change to complete.
	retryOpts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     1.5,
	}
	log.Infof(ctx, "waiting for a single version...")
	version, err := leaseMgr.WaitForOneVersion(ctx, descID, retryOpts)
	log.Infof(ctx, "waiting for a single version... done (at v %d)", version)
	return err
}

// done finalizes the mutations (adds new cols/indexes to the table).
// It ensures that all nodes are on the current (pre-update) version of
// sc.descID and that all nodes are on the new (post-update) version of
// any other modified descriptors.
//
// It also kicks off GC jobs as needed.
func (sc *SchemaChanger) done(ctx context.Context) error {

	// Get the other tables whose foreign key backreferences need to be removed.
	// We also have to handle the situation to add Foreign Key backreferences.
	var fksByBackrefTable map[descpb.ID][]*descpb.ConstraintToUpdate
	var interleaveParents map[descpb.ID]struct{}
	var referencedTypeIDs []descpb.ID
	// Jobs (for GC, etc.) that need to be started immediately after the table
	// descriptor updates are published.
	var childJobs []*jobs.StartableJob
	var didUpdate bool
	modified, err := sc.txnWithModified(ctx, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		childJobs = nil
		fksByBackrefTable = make(map[descpb.ID][]*descpb.ConstraintToUpdate)
		interleaveParents = make(map[descpb.ID]struct{})

		scTable, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}

		referencedTypeIDs, err = scTable.GetAllReferencedTypeIDs(func(id descpb.ID) (catalog.TypeDescriptor, error) {
			desc, err := descsCol.GetImmutableTypeByID(ctx, txn, id, tree.ObjectLookupFlags{})
			if err != nil {
				return nil, err
			}
			return desc, nil
		})
		if err != nil {
			return err
		}
		b := txn.NewBatch()
		for _, mutation := range scTable.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}
			if constraint := mutation.GetConstraint(); constraint != nil &&
				constraint.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY &&
				mutation.Direction == descpb.DescriptorMutation_ADD &&
				constraint.ForeignKey.Validity == descpb.ConstraintValidity_Unvalidated {
				// Add backref table to referenced table with an unvalidated foreign key constraint
				fk := &constraint.ForeignKey
				if fk.ReferencedTableID != scTable.ID {
					fksByBackrefTable[constraint.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[constraint.ForeignKey.ReferencedTableID], constraint)
				}
			} else if swap := mutation.GetPrimaryKeySwap(); swap != nil {
				// If any old indexes (including the old primary index) being rewritten are interleaved
				// children, we will have to update their parents as well.
				for _, idxID := range append([]descpb.IndexID{swap.OldPrimaryIndexId}, swap.OldIndexes...) {
					oldIndex, err := scTable.FindIndexWithID(idxID)
					if err != nil {
						return err
					}
					if oldIndex.NumInterleaveAncestors() != 0 {
						ancestor := oldIndex.GetInterleaveAncestor(oldIndex.NumInterleaveAncestors() - 1)
						if ancestor.TableID != scTable.ID {
							interleaveParents[ancestor.TableID] = struct{}{}
						}
					}
				}
				// Because we are not currently supporting primary key changes on tables/indexes
				// that are interleaved parents, we don't check oldPrimaryIndex.InterleavedBy.
			}
		}

		const kvTrace = true

		var i int           // set to determine whether there is a mutation
		var isRollback bool // set based on the mutation
		for _, mutation := range scTable.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			// Add scTable to the collection as an uncommitted descriptor so that
			// future attempts to resolve a mutable copy find the same pointer.
			//
			// TODO(ajwerner): The need to do this implies that we should cache all
			// mutable descriptors inside of the collection when they are resolved
			// such that all attempts to resolve a mutable descriptor from a
			// collection will always give you the same exact pointer.
			scTable.MaybeIncrementVersion()
			if err := descsCol.AddUncommittedDescriptor(scTable); err != nil {
				return err
			}
			isRollback = mutation.Rollback
			if indexDesc := mutation.GetIndex(); mutation.Direction == descpb.DescriptorMutation_DROP &&
				indexDesc != nil {
				if canClearRangeForDrop(indexDesc) {
					// how we keep track of dropped index names (for, e.g., zone config
					// lookups), even though in the absence of a GC job there's nothing to
					// clean them up.
					scTable.GCMutations = append(
						scTable.GCMutations,
						descpb.TableDescriptor_GCDescriptorMutation{
							IndexID: indexDesc.ID,
						})

					description := sc.job.Payload().Description
					if isRollback {
						description = "ROLLBACK of " + description
					}

					childJob, err := sc.createIndexGCJob(ctx, indexDesc, txn, description)
					if err != nil {
						return err
					}
					childJobs = append(childJobs, childJob)
				}
			}
			if constraint := mutation.GetConstraint(); constraint != nil &&
				constraint.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY &&
				mutation.Direction == descpb.DescriptorMutation_ADD &&
				constraint.ForeignKey.Validity == descpb.ConstraintValidity_Unvalidated {
				// Add backreference on the referenced table (which could be the same table)
				backrefTable, err := descsCol.GetMutableTableVersionByID(ctx,
					constraint.ForeignKey.ReferencedTableID, txn)
				if err != nil {
					return err
				}
				backrefTable.InboundFKs = append(backrefTable.InboundFKs, constraint.ForeignKey)
				if err := descsCol.WriteDescToBatch(ctx, kvTrace, backrefTable, b); err != nil {
					return err
				}
			}

			// Some primary key change specific operations need to happen before
			// and after the index swap occurs.
			if pkSwap := mutation.GetPrimaryKeySwap(); pkSwap != nil {
				// We might have to update some zone configs for indexes that are
				// being rewritten. It is important that this is done _before_ the
				// index swap occurs. The logic that generates spans for subzone
				// configurations removes spans for indexes in the dropping state,
				// which we don't want. So, set up the zone configs before we swap.
				if err := maybeUpdateZoneConfigsForPKChange(
					ctx, txn, sc.execCfg, scTable, pkSwap); err != nil {
					return err
				}
			}

			// If we are refreshing a materialized view, then create GC jobs for all
			// of the existing indexes in the view. We do this before the call to
			// MakeMutationComplete, which swaps out the existing indexes for the
			// backfilled ones.
			if refresh := mutation.GetMaterializedViewRefresh(); refresh != nil {
				if fn := sc.testingKnobs.RunBeforeMaterializedViewRefreshCommit; fn != nil {
					if err := fn(); err != nil {
						return err
					}
				}
				// If we are mutation is in the ADD state, then start GC jobs for the
				// existing indexes on the table.
				if mutation.Direction == descpb.DescriptorMutation_ADD {
					desc := fmt.Sprintf("REFRESH MATERIALIZED VIEW %q cleanup", scTable.Name)
					pkJob, err := sc.createIndexGCJob(ctx, scTable.GetPrimaryIndex().IndexDesc(), txn, desc)
					if err != nil {
						return err
					}
					childJobs = append(childJobs, pkJob)
					for _, idx := range scTable.PublicNonPrimaryIndexes() {
						idxJob, err := sc.createIndexGCJob(ctx, idx.IndexDesc(), txn, desc)
						if err != nil {
							return err
						}
						childJobs = append(childJobs, idxJob)
					}
				} else if mutation.Direction == descpb.DescriptorMutation_DROP {
					// Otherwise, the refresh job ran into an error and is being rolled
					// back. So, we need to GC all of the indexes that were going to be
					// created, in case any data was written to them.
					desc := fmt.Sprintf("ROLLBACK OF REFRESH MATERIALIZED VIEW %q", scTable.Name)
					pkJob, err := sc.createIndexGCJob(ctx, &refresh.NewPrimaryIndex, txn, desc)
					if err != nil {
						return err
					}
					childJobs = append(childJobs, pkJob)
					for i := range refresh.NewIndexes {
						idxJob, err := sc.createIndexGCJob(ctx, &refresh.NewIndexes[i], txn, desc)
						if err != nil {
							return err
						}
						childJobs = append(childJobs, idxJob)
					}
				}
			}

			if err := scTable.MakeMutationComplete(mutation); err != nil {
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
					[]descpb.IndexID{pkSwap.OldPrimaryIndexId}, pkSwap.OldIndexes...) {
					oldIndex, err := scTable.FindIndexWithID(idxID)
					if err != nil {
						return err
					}
					if oldIndex.NumInterleaveAncestors() != 0 {
						ancestorInfo := oldIndex.GetInterleaveAncestor(oldIndex.NumInterleaveAncestors() - 1)
						ancestor, err := descsCol.GetMutableTableVersionByID(ctx, ancestorInfo.TableID, txn)
						if err != nil {
							return err
						}
						ancestorIdxI, err := ancestor.FindIndexWithID(ancestorInfo.IndexID)
						if err != nil {
							return err
						}
						ancestorIdx := ancestorIdxI.IndexDesc()
						foundAncestor := false
						for k, ref := range ancestorIdx.InterleavedBy {
							if ref.Table == scTable.ID && ref.Index == oldIndex.GetID() {
								if foundAncestor {
									return errors.AssertionFailedf(
										"ancestor entry in %s for %s@%s found more than once",
										ancestor.Name, scTable.Name, oldIndex.GetName())
								}
								ancestorIdx.InterleavedBy = append(
									ancestorIdx.InterleavedBy[:k], ancestorIdx.InterleavedBy[k+1:]...)
								foundAncestor = true
								if err := descsCol.WriteDescToBatch(ctx, kvTrace, ancestor, b); err != nil {
									return err
								}
							}
						}
					}
				}
				// If we performed MakeMutationComplete on a PrimaryKeySwap mutation, then we need to start
				// a job for the index deletion mutations that the primary key swap mutation added, if any.
				if childJobs, err = sc.queueCleanupJobs(ctx, scTable, txn, childJobs); err != nil {
					return err
				}
			}

			if computedColumnSwap := mutation.GetComputedColumnSwap(); computedColumnSwap != nil {
				if fn := sc.testingKnobs.RunBeforeComputedColumnSwap; fn != nil {
					fn()
				}

				// If we performed MakeMutationComplete on a computed column swap, then
				// we need to start a job for the column deletion that the swap mutation
				// added if any.
				if childJobs, err = sc.queueCleanupJobs(ctx, scTable, txn, childJobs); err != nil {
					return err
				}
			}
			didUpdate = true
			i++
		}
		if didUpdate = i > 0; !didUpdate {
			// The table descriptor is unchanged, return without writing anything.
			return nil
		}
		// Trim the executed mutations from the descriptor.
		scTable.Mutations = scTable.Mutations[i:]

		for i, g := range scTable.MutationJobs {
			if g.MutationID == sc.mutationID {
				// Trim the executed mutation group from the descriptor.
				scTable.MutationJobs = append(scTable.MutationJobs[:i], scTable.MutationJobs[i+1:]...)
				break
			}
		}

		// Now that all mutations have been applied, find the new set of referenced
		// type descriptors. If this table has been dropped in the mean time, then
		// don't install any backreferences.
		if !scTable.Dropped() {
			newReferencedTypeIDs, err := scTable.GetAllReferencedTypeIDs(func(id descpb.ID) (catalog.TypeDescriptor, error) {
				typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
				if err != nil {
					return nil, err
				}
				return typ, err
			})
			if err != nil {
				return err
			}

			// Update the set of back references.
			for _, id := range referencedTypeIDs {
				typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
				if err != nil {
					return err
				}
				typ.RemoveReferencingDescriptorID(scTable.ID)
				if err := descsCol.WriteDescToBatch(ctx, kvTrace, typ, b); err != nil {
					return err
				}
			}
			for _, id := range newReferencedTypeIDs {
				typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
				if err != nil {
					return err
				}
				typ.AddReferencingDescriptorID(scTable.ID)
				if err := descsCol.WriteDescToBatch(ctx, kvTrace, typ, b); err != nil {
					return err
				}
			}
		}

		if err := descsCol.WriteDescToBatch(ctx, kvTrace, scTable, b); err != nil {
			return err
		}
		if err := txn.Run(ctx, b); err != nil {
			return err
		}

		var info eventpb.EventPayload
		if isRollback {
			info = &eventpb.FinishSchemaChangeRollback{}
		} else {
			info = &eventpb.FinishSchemaChange{}
		}

		// Log "Finish Schema Change" or "Finish Schema Change Rollback"
		// event. Only the table ID and mutation ID are logged; this can
		// be correlated with the DDL statement that initiated the change
		// using the mutation id.
		return logEventInternalForSchemaChanges(
			ctx, sc.execCfg, txn,
			sc.sqlInstanceID,
			sc.descID,
			sc.mutationID,
			info)
	})
	if fn := sc.testingKnobs.RunBeforeChildJobs; fn != nil {
		if len(childJobs) != 0 {
			fn()
		}
	}
	if err != nil {
		for _, job := range childJobs {
			if rollbackErr := job.CleanupOnRollback(ctx); rollbackErr != nil {
				log.Warningf(ctx, "failed to clean up job: %v", rollbackErr)
			}
		}
		return err
	}
	for _, job := range childJobs {
		if _, err := job.Start(ctx); err != nil {
			log.Warningf(ctx, "starting job %d failed with error: %v", *job.ID(), err)
		}
		log.VEventf(ctx, 2, "started job %d", *job.ID())
	}
	// Wait for the modified versions of tables other than the table we're
	// updating to have their leases updated.
	for _, desc := range modified {
		// sc.descID gets waited for above this call in sc.exec().
		if desc.ID == sc.descID {
			continue
		}
		if err := WaitToUpdateLeases(ctx, sc.leaseMgr, desc.ID); err != nil {
			return err
		}
	}
	return nil
}

// maybeUpdateZoneConfigsForPKChange moves zone configs for any rewritten
// indexes from the old index over to the new index. Noop if run on behalf of a
// tenant.
func maybeUpdateZoneConfigsForPKChange(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	table *tabledesc.Mutable,
	swapInfo *descpb.PrimaryKeySwap,
) error {
	if !execCfg.Codec.ForSystemTenant() {
		// Tenants are agnostic to zone configs.
		return nil
	}
	zone, err := getZoneConfigRaw(ctx, txn, execCfg.Codec, table.ID)
	if err != nil {
		return err
	}

	// If this table doesn't have a zone attached to it, don't do anything.
	if zone == nil {
		return nil
	}

	// For each rewritten index, point its subzones for the old index at the
	// new index.
	for i, oldID := range swapInfo.OldIndexes {
		for j := range zone.Subzones {
			subzone := &zone.Subzones[j]
			if subzone.IndexID == uint32(oldID) {
				// If we find a subzone matching an old index, copy its subzone
				// into a new subzone with the new index's ID.
				subzoneCopy := *subzone
				subzoneCopy.IndexID = uint32(swapInfo.NewIndexes[i])
				zone.SetSubzone(subzoneCopy)
			}
		}
	}

	// Write the zone back. This call regenerates the index spans that apply
	// to each partition in the index.
	_, err = writeZoneConfig(ctx, txn, table.ID, table, zone, execCfg, false)
	if err != nil && !sqlerrors.IsCCLRequiredError(err) {
		return err
	}

	return nil
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
	log.Info(ctx, "marking schema change as complete")
	return sc.done(ctx)
}

func (sc *SchemaChanger) refreshStats() {
	// Initiate an asynchronous run of CREATE STATISTICS. We use a large number
	// for rowsAffected because we want to make sure that stats always get
	// created/refreshed here.
	sc.execCfg.StatsRefresher.NotifyMutation(sc.descID, math.MaxInt32 /* rowsAffected */)
}

// maybeReverseMutations reverses the direction of all the mutations with the
// mutationID. This is called after hitting an irrecoverable error while
// applying a schema change. If a column being added is reversed and dropped,
// all new indexes referencing the column will also be dropped.
func (sc *SchemaChanger) maybeReverseMutations(ctx context.Context, causingError error) error {
	if fn := sc.testingKnobs.RunBeforeMutationReversal; fn != nil {
		if err := fn(*sc.job.ID()); err != nil {
			return err
		}
	}
	// There's nothing to do if the mutation is InvalidMutationID.
	if sc.mutationID == descpb.InvalidMutationID {
		return nil
	}

	// Get the other tables whose foreign key backreferences need to be removed.
	var fksByBackrefTable map[descpb.ID][]*descpb.ConstraintToUpdate
	alreadyReversed := false
	const kvTrace = true // TODO(ajwerner): figure this out
	err := sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		fksByBackrefTable = make(map[descpb.ID][]*descpb.ConstraintToUpdate)
		scTable, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}
		// TODO(ajwerner): The need to do this implies that we should cache all
		// mutable descriptors inside of the collection when they are resolved
		// such that all attempts to resolve a mutable descriptor from a
		// collection will always give you the same exact pointer.
		scTable.MaybeIncrementVersion()
		if err := descsCol.AddUncommittedDescriptor(scTable); err != nil {
			return err
		}

		// If this mutation exists, it should be the first mutation. Assert that.
		if len(scTable.Mutations) == 0 {
			alreadyReversed = true
		} else if scTable.Mutations[0].MutationID != sc.mutationID {
			var found bool
			for i := range scTable.Mutations {
				if found = scTable.Mutations[i].MutationID == sc.mutationID; found {
					break
				}
			}
			if alreadyReversed = !found; !alreadyReversed {
				return errors.AssertionFailedf("expected mutation %d to be the"+
					" first mutation when reverted, found %d in descriptor %d",
					sc.mutationID, scTable.Mutations[0].MutationID, scTable.ID)
			}
		} else if scTable.Mutations[0].Rollback {
			alreadyReversed = true
		}

		// Mutation is already reversed, so we don't need to do any more work.
		// This can happen if the mutations were already reversed, but before
		// the rollback completed the job was adopted.
		if alreadyReversed {
			return nil
		}

		for _, mutation := range scTable.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}
			if constraint := mutation.GetConstraint(); constraint != nil &&
				constraint.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY &&
				mutation.Direction == descpb.DescriptorMutation_ADD &&
				constraint.ForeignKey.Validity == descpb.ConstraintValidity_Validating {
				fk := &constraint.ForeignKey
				if fk.ReferencedTableID != scTable.ID {
					fksByBackrefTable[constraint.ForeignKey.ReferencedTableID] =
						append(fksByBackrefTable[constraint.ForeignKey.ReferencedTableID], constraint)
				}
			}
		}

		// Create update closure for the table and all other tables with backreferences
		var droppedMutations map[descpb.MutationID]struct{}

		// Keep track of the column mutations being reversed so that indexes
		// referencing them can be dropped.
		columns := make(map[string]struct{})
		droppedMutations = nil
		b := txn.NewBatch()
		for i, mutation := range scTable.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}

			if mutation.Rollback {
				// Can actually never happen. Since we should have checked for this case
				// above.
				return errors.AssertionFailedf("mutation already rolled back: %v", mutation)
			}

			log.Warningf(ctx, "reverse schema change mutation: %+v", mutation)
			scTable.Mutations[i], columns = sc.reverseMutation(mutation, false /*notStarted*/, columns)

			// If the mutation is for validating a constraint that is being added,
			// drop the constraint because validation has failed.
			if constraint := mutation.GetConstraint(); constraint != nil &&
				mutation.Direction == descpb.DescriptorMutation_ADD {
				log.Warningf(ctx, "dropping constraint %+v", constraint)
				if err := sc.maybeDropValidatingConstraint(ctx, scTable, constraint); err != nil {
					return err
				}
				// Get the foreign key backreferences to remove.
				if constraint.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY {
					fk := &constraint.ForeignKey
					backrefTable, err := descsCol.GetMutableTableVersionByID(ctx, fk.ReferencedTableID, txn)
					if err != nil {
						return err
					}
					if err := removeFKBackReferenceFromTable(backrefTable, fk.Name, scTable); err != nil {
						// The function being called will return an assertion error if the
						// backreference was not found, but it may not have been installed
						// during the incomplete schema change, so we swallow the error.
						log.Infof(ctx,
							"error attempting to remove backreference %s during rollback: %s", fk.Name, err)
					}
					if err := descsCol.WriteDescToBatch(ctx, kvTrace, backrefTable, b); err != nil {
						return err
					}
				}
			}
			scTable.Mutations[i].Rollback = true
		}

		// Delete all mutations that reference any of the reversed columns
		// by running a graph traversal of the mutations.
		if len(columns) > 0 {
			var err error
			droppedMutations, err = sc.deleteIndexMutationsWithReversedColumns(ctx, scTable, columns)
			if err != nil {
				return err
			}
		}

		// Read the table descriptor from the store. The Version of the
		// descriptor has already been incremented in the transaction and
		// this descriptor can be modified without incrementing the version.
		if err := descsCol.WriteDescToBatch(ctx, kvTrace, scTable, b); err != nil {
			return err
		}
		if err := txn.Run(ctx, b); err != nil {
			return err
		}

		tableDesc := scTable.ImmutableCopy().(*tabledesc.Immutable)
		// Mark the schema change job as failed and create a rollback job.
		err = sc.updateJobForRollback(ctx, txn, tableDesc)
		if err != nil {
			return err
		}

		// Mark other reversed mutation jobs as failed.
		for m := range droppedMutations {
			jobID, err := getJobIDForMutationWithDescriptor(ctx, tableDesc, m)
			if err != nil {
				return err
			}
			if err := sc.jobRegistry.Failed(ctx, txn, jobID, causingError); err != nil {
				return err
			}
		}

		// Log "Reverse Schema Change" event. Only the causing error and the
		// mutation ID are logged; this can be correlated with the DDL statement
		// that initiated the change using the mutation id.
		return logEventInternalForSchemaChanges(
			ctx, sc.execCfg, txn,
			sc.sqlInstanceID,
			sc.descID,
			sc.mutationID,
			&eventpb.ReverseSchemaChange{
				Error:    fmt.Sprintf("%+v", causingError),
				SQLSTATE: pgerror.GetPGCode(causingError).String(),
			})
	})
	if err != nil || alreadyReversed {
		return err
	}

	if err := WaitToUpdateLeases(ctx, sc.leaseMgr, sc.descID); err != nil {
		return err
	}
	for id := range fksByBackrefTable {
		if err := WaitToUpdateLeases(ctx, sc.leaseMgr, id); err != nil {
			return err
		}
	}

	return nil
}

// updateJobForRollback updates the schema change job in the case of a rollback.
func (sc *SchemaChanger) updateJobForRollback(
	ctx context.Context, txn *kv.Txn, tableDesc *tabledesc.Immutable,
) error {
	// Initialize refresh spans to scan the entire table.
	span := tableDesc.PrimaryIndexSpan(sc.execCfg.Codec)
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
	oldDetails := sc.job.Details().(jobspb.SchemaChangeDetails)
	if err := sc.job.WithTxn(txn).SetDetails(
		ctx, jobspb.SchemaChangeDetails{
			DescID:          sc.descID,
			TableMutationID: sc.mutationID,
			ResumeSpanList:  spanList,
			FormatVersion:   oldDetails.FormatVersion,
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
	ctx context.Context, desc *tabledesc.Mutable, constraint *descpb.ConstraintToUpdate,
) error {
	switch constraint.ConstraintType {
	case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
		if constraint.Check.Validity == descpb.ConstraintValidity_Unvalidated {
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
	case descpb.ConstraintToUpdate_FOREIGN_KEY:
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
	ctx context.Context, desc *tabledesc.Mutable, columns map[string]struct{},
) (map[descpb.MutationID]struct{}, error) {
	dropMutations := make(map[descpb.MutationID]struct{})
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
							if mutation.Direction != descpb.DescriptorMutation_ADD ||
								mutation.State != descpb.DescriptorMutation_DELETE_ONLY {
								panic(errors.AssertionFailedf("mutation in bad state: %+v", mutation))
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
		newMutations := make([]descpb.DescriptorMutation, 0, len(desc.Mutations))
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
	mutation descpb.DescriptorMutation, notStarted bool, columns map[string]struct{},
) (descpb.DescriptorMutation, map[string]struct{}) {
	switch mutation.Direction {
	case descpb.DescriptorMutation_ADD:
		mutation.Direction = descpb.DescriptorMutation_DROP
		// A column ADD being reversed gets placed in the map.
		if col := mutation.GetColumn(); col != nil {
			columns[col.Name] = struct{}{}
		}
		// PrimaryKeySwap, ComputedColumnSwap and MaterializedViewRefresh don't
		// have a concept of the state machine.
		if pkSwap, computedColumnsSwap, refresh :=
			mutation.GetPrimaryKeySwap(),
			mutation.GetComputedColumnSwap(),
			mutation.GetMaterializedViewRefresh(); pkSwap != nil || computedColumnsSwap != nil || refresh != nil {
			return mutation, columns
		}

		if notStarted && mutation.State != descpb.DescriptorMutation_DELETE_ONLY {
			panic(errors.AssertionFailedf("mutation in bad state: %+v", mutation))
		}

	case descpb.DescriptorMutation_DROP:
		mutation.Direction = descpb.DescriptorMutation_ADD
		if notStarted && mutation.State != descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY {
			panic(errors.AssertionFailedf("mutation in bad state: %+v", mutation))
		}
	}
	return mutation, columns
}

// CreateGCJobRecord creates the job record for a GC job, setting some
// properties which are common for all GC jobs.
func CreateGCJobRecord(
	originalDescription string, username security.SQLUsername, details jobspb.SchemaChangeGCDetails,
) jobs.Record {
	descriptorIDs := make([]descpb.ID, 0)
	if len(details.Indexes) > 0 {
		if len(descriptorIDs) == 0 {
			descriptorIDs = []descpb.ID{details.ParentID}
		}
	} else {
		for _, table := range details.Tables {
			descriptorIDs = append(descriptorIDs, table.ID)
		}
	}
	return jobs.Record{
		Description:   fmt.Sprintf("GC for %s", originalDescription),
		Username:      username,
		DescriptorIDs: descriptorIDs,
		Details:       details,
		Progress:      jobspb.SchemaChangeGCProgress{},
		RunningStatus: RunningStatusWaitingGC,
		NonCancelable: true,
	}
}

// GCJobTestingKnobs is for testing the Schema Changer GC job.
// Note that this is defined here for testing purposes to avoid cyclic
// dependencies.
type GCJobTestingKnobs struct {
	RunBeforeResume func(jobID int64) error
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

	// RunAfterBackfill is called after completing a backfill.
	RunAfterBackfill func(jobID int64) error

	// RunBeforeQueryBackfill is called before a query based backfill.
	RunBeforeQueryBackfill func() error

	// RunBeforeIndexBackfill is called just before starting the index backfill, after
	// fixing the index backfill scan timestamp.
	RunBeforeIndexBackfill func()

	// RunBeforeMaterializedViewRefreshCommit is called before committing a
	// materialized view refresh.
	RunBeforeMaterializedViewRefreshCommit func() error

	// RunBeforePrimaryKeySwap is called just before the primary key swap is committed.
	RunBeforePrimaryKeySwap func()

	// RunBeforeComputedColumnSwap is called just before the computed column swap is committed.
	RunBeforeComputedColumnSwap func()

	// RunBeforeChildJobs is called just before child jobs are run to clean up
	// dropped schema elements after a mutation.
	RunBeforeChildJobs func()

	// RunBeforeIndexValidation is called just before starting the index validation,
	// after setting the job status to validating.
	RunBeforeIndexValidation func() error

	// RunBeforeConstraintValidation is called just before starting the checks validation,
	// after setting the job status to validating.
	RunBeforeConstraintValidation func() error

	// RunBeforeMutationReversal runs at the beginning of maybeReverseMutations.
	RunBeforeMutationReversal func(jobID int64) error

	// RunAfterMutationReversal runs in OnFailOrCancel after the mutations have
	// been reversed.
	RunAfterMutationReversal func(jobID int64) error

	// RunAtStartOfOnFailOrCancel runs at the start of the OnFailOrCancel hook.
	RunBeforeOnFailOrCancel func(jobID int64) error

	// RunAfterOnFailOrCancel runs after the OnFailOrCancel hook.
	RunAfterOnFailOrCancel func(jobID int64) error

	// RunBeforeResume runs at the start of the Resume hook.
	RunBeforeResume func(jobID int64) error

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

	// AlwaysUpdateIndexBackfillDetails indicates whether the index backfill
	// schema change job details should be updated everytime the coordinator
	// receives an update from the backfill processor.
	AlwaysUpdateIndexBackfillDetails bool

	// AlwaysUpdateIndexBackfillProgress indicates whether the index backfill
	// schema change job fraction completed should be updated everytime the
	// coordinator receives an update from the backfill processor.
	AlwaysUpdateIndexBackfillProgress bool

	// TwoVersionLeaseViolation is called whenever a schema change transaction is
	// unable to commit because it is violating the two version lease invariant.
	TwoVersionLeaseViolation func()
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*SchemaChangerTestingKnobs) ModuleTestingKnobs() {}

// txn is a convenient wrapper around descs.Txn().
func (sc *SchemaChanger) txn(
	ctx context.Context, f func(context.Context, *kv.Txn, *descs.Collection) error,
) error {
	_, err := sc.txnWithModified(ctx, f)
	return err
}

// txnWithModified is a convenient wrapper around descs.Txn() which additionally
// returns the set of modified descriptors.
func (sc *SchemaChanger) txnWithModified(
	ctx context.Context, f func(context.Context, *kv.Txn, *descs.Collection) error,
) (descsWithNewVersions []lease.IDVersion, _ error) {
	ie := sc.ieFactory(ctx, newFakeSessionData())
	if err := descs.Txn(ctx, sc.settings, sc.leaseMgr, ie, sc.db, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		if err := f(ctx, txn, descsCol); err != nil {
			return err
		}
		descsWithNewVersions = descsCol.GetDescriptorsWithNewVersion()
		return nil
	}); err != nil {
		return nil, err
	}
	return descsWithNewVersions, nil
}

// createSchemaChangeEvalCtx creates an extendedEvalContext() to be used for backfills.
//
// TODO(andrei): This EvalContext() will be broken for backfills trying to use
// functions marked with distsqlBlocklist.
// Also, the SessionTracing inside the context is unrelated to the one
// used in the surrounding SQL session, so session tracing is unable
// to capture schema change activity.
func createSchemaChangeEvalCtx(
	ctx context.Context,
	execCfg *ExecutorConfig,
	ts hlc.Timestamp,
	ieFactory sqlutil.SessionBoundInternalExecutorFactory,
) extendedEvalContext {

	sd := newFakeSessionData()

	evalCtx := extendedEvalContext{
		// Make a session tracing object on-the-fly. This is OK
		// because it sets "enabled: false" and thus none of the
		// other fields are used.
		Tracing: &SessionTracing{},
		ExecCfg: execCfg,
		EvalContext: tree.EvalContext{
			SessionData:      sd,
			InternalExecutor: ieFactory(ctx, sd),
			// TODO(andrei): This is wrong (just like on the main code path on
			// setupFlow). Each processor should override Ctx with its own context.
			Context:            ctx,
			Planner:            &faketreeeval.DummyEvalPlanner{},
			PrivilegedAccessor: &faketreeeval.DummyPrivilegedAccessor{},
			SessionAccessor:    &faketreeeval.DummySessionAccessor{},
			ClientNoticeSender: &faketreeeval.DummyClientNoticeSender{},
			Sequence:           &faketreeeval.DummySequenceOperators{},
			Tenant:             &faketreeeval.DummyTenantOperator{},
			Settings:           execCfg.Settings,
			TestingKnobs:       execCfg.EvalContextTestingKnobs,
			ClusterID:          execCfg.ClusterID(),
			ClusterName:        execCfg.RPCContext.ClusterName(),
			NodeID:             execCfg.NodeID,
			Codec:              execCfg.Codec,
			Locality:           execCfg.Locality,
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

func newFakeSessionData() *sessiondata.SessionData {
	sd := &sessiondata.SessionData{
		SessionData: sessiondatapb.SessionData{
			// The database is not supposed to be needed in schema changes, as there
			// shouldn't be unqualified identifiers in backfills, and the pure functions
			// that need it should have already been evaluated.
			//
			// TODO(andrei): find a way to assert that this field is indeed not used.
			// And in fact it is used by `current_schemas()`, which, although is a pure
			// function, takes arguments which might be impure (so it can't always be
			// pre-evaluated).
			Database:  "",
			UserProto: security.NodeUserName().EncodeProto(),
		},
		SearchPath:    sessiondata.DefaultSearchPathForUser(security.NodeUserName()),
		SequenceState: sessiondata.NewSequenceState(),
		Location:      time.UTC,
	}
	return sd
}

type schemaChangeResumer struct {
	job *jobs.Job
}

func (r schemaChangeResumer) Resume(
	ctx context.Context, execCtx interface{}, resultsCh chan<- tree.Datums,
) error {
	p := execCtx.(JobExecContext)
	details := r.job.Details().(jobspb.SchemaChangeDetails)
	if p.ExecCfg().SchemaChangerTestingKnobs.SchemaChangeJobNoOp != nil &&
		p.ExecCfg().SchemaChangerTestingKnobs.SchemaChangeJobNoOp() {
		return nil
	}
	if fn := p.ExecCfg().SchemaChangerTestingKnobs.RunBeforeResume; fn != nil {
		if err := fn(*r.job.ID()); err != nil {
			return err
		}
	}

	execSchemaChange := func(descID descpb.ID, mutationID descpb.MutationID, droppedDatabaseID descpb.ID) error {
		sc := SchemaChanger{
			descID:               descID,
			mutationID:           mutationID,
			droppedDatabaseID:    droppedDatabaseID,
			sqlInstanceID:        p.ExecCfg().NodeID.SQLInstanceID(),
			db:                   p.ExecCfg().DB,
			leaseMgr:             p.ExecCfg().LeaseManager,
			testingKnobs:         p.ExecCfg().SchemaChangerTestingKnobs,
			distSQLPlanner:       p.DistSQLPlanner(),
			jobRegistry:          p.ExecCfg().JobRegistry,
			job:                  r.job,
			rangeDescriptorCache: p.ExecCfg().RangeDescriptorCache,
			clock:                p.ExecCfg().Clock,
			settings:             p.ExecCfg().Settings,
			execCfg:              p.ExecCfg(),
			ieFactory: func(ctx context.Context, sd *sessiondata.SessionData) sqlutil.InternalExecutor {
				return r.job.MakeSessionBoundInternalExecutor(ctx, sd)
			},
			metrics: p.ExecCfg().SchemaChangerMetrics,
		}
		opts := retry.Options{
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     20 * time.Second,
			Multiplier:     1.5,
		}

		// The schema change may have to be retried if it is not first in line or
		// for other retriable reasons so we run it in an exponential backoff retry
		// loop. The loop terminates only if the context is canceled.
		var scErr error
		for r := retry.StartWithCtx(ctx, opts); r.Next(); {
			// Note that r.Next always returns true on first run so exec will be
			// called at least once before there is a chance for this loop to exit.
			scErr = sc.exec(ctx)
			switch {
			case scErr == nil:
				sc.metrics.Successes.Inc(1)
				return nil
			case errors.Is(scErr, catalog.ErrDescriptorNotFound):
				// If the table descriptor for the ID can't be found, we assume that
				// another job to drop the table got to it first, and consider this job
				// finished.
				log.Infof(
					ctx,
					"descriptor %d not found for schema change processing mutation %d;"+
						"assuming it was dropped, and exiting",
					descID, mutationID,
				)
				return nil
			case !isPermanentSchemaChangeError(scErr):
				// Check if the error is on a allowlist of errors we should retry on,
				// including the schema change not having the first mutation in line.
				log.Warningf(ctx, "error while running schema change, retrying: %v", scErr)
				sc.metrics.RetryErrors.Inc(1)
			default:
				if ctx.Err() == nil {
					sc.metrics.PermanentErrors.Inc(1)
				}
				// All other errors lead to a failed job.
				return scErr
			}

		}
		// If the context was canceled, the job registry will retry the job. We can
		// just return the error without wrapping it in a retry error.
		return scErr
	}

	// If a database or a set of schemas is being dropped, drop all objects as
	// part of this schema change job.
	// TODO (lucy): Now that the schema change job is responsible for removing
	// namespace entries for every type of descriptor and we specify exactly which
	// descriptors need to be dropped, we should consider unconditionally removing
	// namespace entries even when the descriptor no longer exists.

	// Drop the child types in the dropped database or schemas.
	for i := range details.DroppedTypes {
		ts := &typeSchemaChanger{
			typeID:  details.DroppedTypes[i],
			execCfg: p.ExecCfg(),
		}
		if err := ts.execWithRetry(ctx); err != nil {
			return err
		}
	}

	// Drop the child tables.
	for i := range details.DroppedTables {
		droppedTable := &details.DroppedTables[i]
		if err := execSchemaChange(droppedTable.ID, descpb.InvalidMutationID, details.DroppedDatabaseID); err != nil {
			return err
		}
	}

	// Drop all schemas.
	for _, id := range details.DroppedSchemas {
		if err := execSchemaChange(id, descpb.InvalidMutationID, descpb.InvalidID); err != nil {
			return err
		}
	}

	// Drop the database, if applicable.
	if details.FormatVersion >= jobspb.DatabaseJobFormatVersion {
		if dbID := details.DroppedDatabaseID; dbID != descpb.InvalidID {
			if err := execSchemaChange(dbID, descpb.InvalidMutationID, descpb.InvalidID); err != nil {
				return err
			}
			// If there are no tables to GC, the zone config needs to be deleted now.
			if p.ExecCfg().Codec.ForSystemTenant() && len(details.DroppedTables) == 0 {
				zoneKeyPrefix := config.MakeZoneKeyPrefix(config.SystemTenantObjectID(dbID))
				if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
					log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
				}
				// Delete the zone config entry for this database.
				if err := p.ExecCfg().DB.DelRange(ctx, zoneKeyPrefix, zoneKeyPrefix.PrefixEnd()); err != nil {
					return err
				}
			}
		}
	}

	// Queue the GC job for any dropped tables. This should happen after the
	// database (if applicable) has been dropped. Currently the table GC job is
	// responsible for deleting the database zone config at the end.
	if len(details.DroppedTables) > 0 {
		dropTime := timeutil.Now().UnixNano()
		tablesToGC := make([]jobspb.SchemaChangeGCDetails_DroppedID, len(details.DroppedTables))
		for i, table := range details.DroppedTables {
			tablesToGC[i] = jobspb.SchemaChangeGCDetails_DroppedID{ID: table.ID, DropTime: dropTime}
		}
		multiTableGCDetails := jobspb.SchemaChangeGCDetails{
			Tables:   tablesToGC,
			ParentID: details.DroppedDatabaseID,
		}

		if err := startGCJob(
			ctx,
			p.ExecCfg().DB,
			p.ExecCfg().JobRegistry,
			r.job.Payload().UsernameProto.Decode(),
			r.job.Payload().Description,
			multiTableGCDetails,
		); err != nil {
			return err
		}
	}

	// Finally, if there's a main descriptor undergoing a schema change, run the
	// schema changer. This can be any single-table schema change or any change to
	// a database or schema other than a drop.
	if details.DescID != descpb.InvalidID {
		return execSchemaChange(details.DescID, details.TableMutationID, details.DroppedDatabaseID)
	}
	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r schemaChangeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	details := r.job.Details().(jobspb.SchemaChangeDetails)

	if details.DroppedDatabaseID != descpb.InvalidID {
		// TODO (lucy): Do we need to do anything here?
		return nil
	}
	if details.DescID == descpb.InvalidID {
		return errors.AssertionFailedf("job has no database ID or table ID")
	}
	sc := SchemaChanger{
		descID:               details.DescID,
		mutationID:           details.TableMutationID,
		sqlInstanceID:        p.ExecCfg().NodeID.SQLInstanceID(),
		db:                   p.ExecCfg().DB,
		leaseMgr:             p.ExecCfg().LeaseManager,
		testingKnobs:         p.ExecCfg().SchemaChangerTestingKnobs,
		distSQLPlanner:       p.DistSQLPlanner(),
		jobRegistry:          p.ExecCfg().JobRegistry,
		job:                  r.job,
		rangeDescriptorCache: p.ExecCfg().RangeDescriptorCache,
		clock:                p.ExecCfg().Clock,
		settings:             p.ExecCfg().Settings,
		execCfg:              p.ExecCfg(),
		ieFactory: func(ctx context.Context, sd *sessiondata.SessionData) sqlutil.InternalExecutor {
			return r.job.MakeSessionBoundInternalExecutor(ctx, sd)
		},
	}

	if fn := sc.testingKnobs.RunBeforeOnFailOrCancel; fn != nil {
		if err := fn(*r.job.ID()); err != nil {
			return err
		}
	}

	if r.job.Payload().FinalResumeError == nil {
		return errors.AssertionFailedf("job failed but had no recorded error")
	}
	scErr := errors.DecodeError(ctx, *r.job.Payload().FinalResumeError)

	if rollbackErr := sc.handlePermanentSchemaChangeError(ctx, scErr, p.ExtendedEvalContext()); rollbackErr != nil {
		switch {
		case errors.Is(rollbackErr, catalog.ErrDescriptorNotFound):
			// If the table descriptor for the ID can't be found, we assume that
			// another job to drop the table got to it first, and consider this job
			// finished.
			log.Infof(
				ctx,
				"descriptor %d not found for rollback of schema change processing mutation %d;"+
					"assuming it was dropped, and exiting",
				details.DescID, details.TableMutationID,
			)
		case ctx.Err() != nil:
			// If the context was canceled, the job registry will retry the job.
			// We check for this case so that we can just return the error without
			// wrapping it in a retry error.
			return rollbackErr
		case !isPermanentSchemaChangeError(rollbackErr):
			// Check if the error is on a allowlist of errors we should retry on, and
			// have the job registry retry.
			return jobs.NewRetryJobError(rollbackErr.Error())
		default:
			// All other errors lead to a failed job.
			//
			// TODO (lucy): We have a problem where some schema change rollbacks will
			// never succeed because the backfiller can't handle rolling back schema
			// changes that involve dropping a column; see #46541. (This is probably
			// not the only bug that could cause rollbacks to fail.) For historical
			// context: This was the case in 19.2 and probably earlier versions as
			// well, and in those earlier versions, the old async schema changer would
			// keep retrying the rollback and failing in the background because the
			// mutation would still be left on the table descriptor. In the present
			// schema change job, we return an error immediately and put the job in a
			// terminal state instead of retrying indefinitely, basically to make the
			// behavior similar to 19.2: If the rollback fails, we end up returning
			// immediately (instead of retrying and blocking indefinitely), and the
			// table descriptor is left in a bad state with some mutations that we
			// can't clean up.
			//
			// Ultimately, this is untenable, and we should figure out some better way
			// of dealing with failed rollbacks. Part of the solution is just making
			// rollbacks (especially of dropped columns) more robust, but part of it
			// will likely involve some sort of medium-term solution for cleaning up
			// mutations that we can't make any progress on (see #47456). In the long
			// term we'll hopefully be rethinking what it even means to "roll back" a
			// (transactional) schema change.
			return rollbackErr
		}
	}

	if fn := sc.testingKnobs.RunAfterOnFailOrCancel; fn != nil {
		if err := fn(*r.job.ID()); err != nil {
			return err
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

// queueCleanupJobs checks if the completed schema change needs to start a
// child job to clean up dropped schema elements.
func (sc *SchemaChanger) queueCleanupJobs(
	ctx context.Context, scDesc *tabledesc.Mutable, txn *kv.Txn, childJobs []*jobs.StartableJob,
) ([]*jobs.StartableJob, error) {
	// Create jobs for dropped columns / indexes to be deleted.
	mutationID := scDesc.ClusterVersion.NextMutationID
	span := scDesc.PrimaryIndexSpan(sc.execCfg.Codec)
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
			Username:      sc.job.Payload().UsernameProto.Decode(),
			DescriptorIDs: descpb.IDs{scDesc.GetID()},
			Details: jobspb.SchemaChangeDetails{
				DescID:          sc.descID,
				TableMutationID: mutationID,
				ResumeSpanList:  spanList,
				// The version distinction for database jobs doesn't matter for jobs on
				// tables.
				FormatVersion: jobspb.DatabaseJobFormatVersion,
			},
			Progress:      jobspb.SchemaChangeProgress{},
			NonCancelable: true,
		}
		job, err := sc.jobRegistry.CreateStartableJobWithTxn(ctx, jobRecord, txn, nil /* resultsCh */)
		if err != nil {
			return nil, err
		}
		log.VEventf(ctx, 2, "created job %d to drop previous columns "+
			"and indexes.", *job.ID())
		childJobs = append(childJobs, job)
		scDesc.MutationJobs = append(scDesc.MutationJobs, descpb.TableDescriptor_MutationJob{
			MutationID: mutationID,
			JobID:      *job.ID(),
		})
	}
	return childJobs, nil
}

// DeleteTableDescAndZoneConfig removes a table's descriptor and zone config from the KV database.
func DeleteTableDescAndZoneConfig(
	ctx context.Context, db *kv.DB, codec keys.SQLCodec, tableDesc *tabledesc.Immutable,
) error {
	log.Infof(ctx, "removing table descriptor and zone config for table %d", tableDesc.ID)
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(codec.ForSystemTenant()); err != nil {
			return err
		}
		b := &kv.Batch{}

		// Delete the descriptor.
		descKey := catalogkeys.MakeDescMetadataKey(codec, tableDesc.ID)
		b.Del(descKey)
		// Delete the zone config entry for this table, if necessary.
		if codec.ForSystemTenant() {
			zoneKeyPrefix := config.MakeZoneKeyPrefix(config.SystemTenantObjectID(tableDesc.ID))
			b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		}
		return txn.Run(ctx, b)
	})
}
