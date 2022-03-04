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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
	// RunningStatusMerging is for jobs that are currently waiting on
	// the cluster to converge to seeing the schema element in the
	// MERGING state.
	RunningStatusMerging jobs.RunningStatus = "waiting in MERGING"
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

	// mvccCompliantAddIndex is set to true early in exec if we
	// find that the schema change was created under the
	// mvcc-compliant regime.
	mvccCompliantAddIndex bool
}

// NewSchemaChangerForTesting only for tests.
func NewSchemaChangerForTesting(
	tableID descpb.ID,
	mutationID descpb.MutationID,
	sqlInstanceID base.SQLInstanceID,
	db *kv.DB,
	leaseMgr *lease.Manager,
	jobRegistry *jobs.Registry,
	execCfg *ExecutorConfig,
	settings *cluster.Settings,
) SchemaChanger {
	return SchemaChanger{
		descID:        tableID,
		mutationID:    mutationID,
		sqlInstanceID: sqlInstanceID,
		db:            db,
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
		metrics:        NewSchemaChangerMetrics(),
		clock:          db.Clock(),
		distSQLPlanner: execCfg.DistSQLPlanner,
		testingKnobs:   &SchemaChangerTestingKnobs{},
	}
}

// IsConstraintError returns true if the error is considered as
// an error introduced by the user. For example a constraint
// violation.
func IsConstraintError(err error) bool {
	pgCode := pgerror.GetPGCode(err)
	return pgCode == pgcode.CheckViolation ||
		pgCode == pgcode.UniqueViolation ||
		pgCode == pgcode.ForeignKeyViolation ||
		pgCode == pgcode.NotNullViolation ||
		pgCode == pgcode.IntegrityConstraintViolation
}

// IsPermanentSchemaChangeError returns true if the error results in
// a permanent failure of a schema change. This function is a allowlist
// instead of a blocklist: only known safe errors are confirmed to not be
// permanent errors. Anything unknown is assumed to be permanent.
func IsPermanentSchemaChangeError(err error) bool {
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

	// Clock sync problems should not lead to permanently failed schema changes.
	if hlc.IsUntrustworthyRemoteWallTimeError(err) {
		return false
	}

	if pgerror.IsSQLRetryableError(err) {
		return false
	}

	if errors.IsAny(err,
		context.Canceled,
		context.DeadlineExceeded,
		errSchemaChangeNotFirstInLine,
		errTableVersionMismatchSentinel,
	) {
		return false
	}

	if flowinfra.IsFlowRetryableError(err) {
		return false
	}

	switch pgerror.GetPGCode(err) {
	case pgcode.SerializationFailure, pgcode.InternalConnectionFailure:
		return false

	case pgcode.Internal, pgcode.RangeUnavailable:
		if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			return false
		}
	}

	return true
}

var errSchemaChangeNotFirstInLine = errors.Newf("schema change not first in line")

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
	ctx context.Context, table catalog.TableDescriptor, refresh catalog.MaterializedViewRefresh,
) error {
	// If we aren't requested to backfill any data, then return immediately.
	if !refresh.ShouldBackfill() {
		return nil
	}
	// The data for the materialized view is stored under the current set of
	// indexes in table. We want to keep all of that data untouched, and write
	// out all the data into the new set of indexes denoted by refresh. So, just
	// perform some surgery on the input table to denote it as having the desired
	// set of indexes. We then backfill into this modified table, which writes
	// data only to the new desired indexes. In SchemaChanger.done(), we'll swap
	// the indexes from the old versions into the new ones.
	tableToRefresh := refresh.TableWithNewIndexes(table)
	return sc.backfillQueryIntoTable(ctx, tableToRefresh, table.GetViewQuery(), refresh.AsOf(), "refreshView")
}

func (sc *SchemaChanger) backfillQueryIntoTable(
	ctx context.Context, table catalog.TableDescriptor, query string, ts hlc.Timestamp, desc string,
) error {
	if fn := sc.testingKnobs.RunBeforeQueryBackfill; fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}

	return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetFixedTimestamp(ctx, ts); err != nil {
			return err
		}

		// Create an internal planner as the planner used to serve the user query
		// would have committed by this point.
		p, cleanup := NewInternalPlanner(
			desc,
			txn,
			security.RootUserName(),
			&MemoryMetrics{},
			sc.execCfg,
			sessiondatapb.SessionData{},
		)

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
		rw := NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
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
			sc.execCfg.ContentionRegistry,
			nil, /* testingPushCallback */
		)
		defer recv.Release()

		var planAndRunErr error
		localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
			// Resolve subqueries before running the queries' physical plan.
			if len(localPlanner.curPlan.subqueryPlans) != 0 {
				// Create a separate memory account for the results of the
				// subqueries. Note that we intentionally defer the closure of
				// the account until we return from this method (after the main
				// query is executed).
				subqueryResultMemAcc := localPlanner.EvalContext().Mon.MakeBoundAccount()
				defer subqueryResultMemAcc.Close(ctx)
				if !sc.distSQLPlanner.PlanAndRunSubqueries(
					ctx, localPlanner, localPlanner.ExtendedEvalContextCopy,
					localPlanner.curPlan.subqueryPlans, recv, &subqueryResultMemAcc,
				) {
					if planAndRunErr = rw.Err(); planAndRunErr != nil {
						return
					}
				}
			}

			isLocal := !getPlanDistribution(
				ctx, localPlanner, localPlanner.execCfg.NodeID,
				localPlanner.extendedEvalCtx.SessionData().DistSQLMode,
				localPlanner.curPlan.main,
			).WillDistribute()
			out := execinfrapb.ProcessorCoreUnion{BulkRowWriter: &execinfrapb.BulkRowWriterSpec{
				Table: *table.TableDesc(),
			}}

			PlanAndRunCTAS(ctx, sc.distSQLPlanner, localPlanner,
				txn, isLocal, localPlanner.curPlan.main, out, recv)
			if planAndRunErr = rw.Err(); planAndRunErr != nil {
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
	ctx context.Context, table catalog.TableDescriptor,
) error {
	if !(table.Adding() && table.IsAs()) {
		return nil
	}
	log.Infof(ctx, "starting backfill for CREATE TABLE AS with query %q", table.GetCreateQuery())

	return sc.backfillQueryIntoTable(ctx, table, table.GetCreateQuery(), table.GetCreateAsOfTime(), "ctasBackfill")
}

// maybeUpdateScheduledJobsForRowLevelTTL ensures the scheduled jobs related to the
// table's row level TTL are appropriately configured.
func (sc *SchemaChanger) maybeUpdateScheduledJobsForRowLevelTTL(
	ctx context.Context, tableDesc catalog.TableDescriptor,
) error {
	// Drop the scheduled job if one exists and the table descriptor is being dropped.
	if tableDesc.Dropped() && tableDesc.GetRowLevelTTL() != nil {
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			scheduleID := tableDesc.GetRowLevelTTL().ScheduleID
			if scheduleID > 0 {
				log.Infof(ctx, "dropping TTL schedule %d", scheduleID)
				return DeleteSchedule(ctx, sc.execCfg, txn, scheduleID)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (sc *SchemaChanger) maybeBackfillMaterializedView(
	ctx context.Context, table catalog.TableDescriptor,
) error {
	if !(table.Adding() && table.MaterializedView()) {
		return nil
	}
	log.Infof(ctx, "starting backfill for CREATE MATERIALIZED VIEW with query %q", table.GetViewQuery())

	return sc.backfillQueryIntoTable(ctx, table, table.GetViewQuery(), table.GetCreateAsOfTime(), "materializedViewBackfill")
}

// maybe make a table PUBLIC if it's in the ADD state.
func (sc *SchemaChanger) maybeMakeAddTablePublic(
	ctx context.Context, table catalog.TableDescriptor,
) error {
	if !table.Adding() {
		return nil
	}
	log.Info(ctx, "making table public")

	return sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		mut, err := descsCol.GetMutableTableVersionByID(ctx, table.GetID(), txn)
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

// ignoreRevertedDropIndex finds all add index mutations that are the
// result of a rollback and changes their direction to DROP.
//
// Prior to 22.1 we would attempt to revert failed DROP INDEX
// mutations. However, not all dependent objects were reverted and it
// required an expensive, full index rebuild.
//
// In 22.1+, we no longer revert failed DROP INDEX mutations, but we
// need to account for mutations created on earlier versions.
//
// In a mixed-version state, if this code runs once, then the mutation
// will have been converted to a DROP and the index will be dropped
// regardless of which node resumes the job after this function
// returns. If the job is resumed only on an older node, then the
// reverted schema change will continue as it would have previously.
//
// TODO(ssd): Once we install a version gate and migration that drains
// in-flight schema changes and disallows any old-style index
// backfills, we can remove this extra transaction since we will know
// that any reverted DROP INDEX mutations will either have already
// been processed or will fail (since after the new gate, we will fail
// any ADD INDEX mutations generated on old code).
func (sc *SchemaChanger) ignoreRevertedDropIndex(
	ctx context.Context, table catalog.TableDescriptor,
) error {
	if !table.IsPhysicalTable() {
		return nil
	}
	return sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		mut, err := descsCol.GetMutableTableVersionByID(ctx, table.GetID(), txn)
		if err != nil {
			return err
		}
		mutationsModified := false
		for _, m := range mut.AllMutations() {
			if m.MutationID() != sc.mutationID {
				break
			}

			if !m.IsRollback() || !m.Adding() || m.AsIndex() == nil {
				continue
			}
			log.Warningf(ctx, "ignoring rollback of index drop; index %q will be dropped", m.AsIndex().GetName())
			mut.Mutations[m.MutationOrdinal()].Direction = descpb.DescriptorMutation_DROP
			mutationsModified = true
		}
		if mutationsModified {
			return descsCol.WriteDesc(ctx, true /* kvTrace */, mut, txn)
		}
		return nil
	})
}

// drainNamesForDescriptor will drain remove the draining names from the
// descriptor with the specified ID. If it is a schema, it will also remove the
// names from the parent database.
//
// If there are no draining names, this call will not update any descriptors.
func drainNamesForDescriptor(
	ctx context.Context,
	descID descpb.ID,
	cf *descs.CollectionFactory,
	db *kv.DB,
	ie sqlutil.InternalExecutor,
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
		mutDesc, err := descsCol.GetMutableDescriptorByID(ctx, txn, descID)
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
			b.Del(catalogkeys.EncodeNameKey(codec, drain))
		}

		// If the descriptor to drain is a schema, then we need to delete the
		// draining names from the parent database's schema mapping.
		if _, isSchema := mutDesc.(catalog.SchemaDescriptor); isSchema {
			mutDB, err := descsCol.GetMutableDescriptorByID(ctx, txn, mutDesc.GetParentID())
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
	return cf.Txn(ctx, ie, db, run)
}

func startGCJob(
	ctx context.Context,
	db *kv.DB,
	jobRegistry *jobs.Registry,
	username security.SQLUsername,
	schemaChangeDescription string,
	details jobspb.SchemaChangeGCDetails,
) error {
	jobRecord := CreateGCJobRecord(schemaChangeDescription, username, details)
	jobID := jobRegistry.MakeJobID()
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := jobRegistry.CreateJobWithTxn(ctx, jobRecord, jobID, txn)
		return err
	}); err != nil {
		return err
	}
	log.Infof(ctx, "created GC job %d", jobID)
	jobRegistry.NotifyToResume(ctx, jobID)
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
		for i, mutation := range tableDesc.AllMutations() {
			if mutation.MutationID() == sc.mutationID {
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
	if err := sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) (err error) {
		flags := tree.CommonLookupFlags{
			AvoidLeased:    true,
			Required:       true,
			IncludeOffline: true,
			IncludeDropped: true,
		}
		desc, err = descriptors.GetImmutableDescriptorByID(ctx, txn, sc.descID, flags)
		return err
	}); err != nil {
		return nil, err
	}
	return desc, nil
}

func (sc *SchemaChanger) checkForMVCCCompliantAddIndexMutations(
	ctx context.Context, desc catalog.Descriptor,
) error {
	tableDesc, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return nil
	}

	nonTempAddingIndexes := 0
	tempIndexes := 0

	for _, m := range tableDesc.AllMutations() {
		if m.MutationID() != sc.mutationID {
			break
		}

		idx := m.AsIndex()
		if idx == nil {
			continue
		}

		if idx.IsTemporaryIndexForBackfill() {
			tempIndexes++
		} else if m.Adding() {
			nonTempAddingIndexes++
		}
	}

	if tempIndexes > 0 {
		sc.mvccCompliantAddIndex = true

		if tempIndexes != nonTempAddingIndexes {
			return errors.Newf("expected %d temporary indexes, but found %d; schema change may have been constructed during cluster version upgrade",
				tempIndexes,
				nonTempAddingIndexes)
		}

		settings := sc.execCfg.Settings
		mvccCompliantBackfillSupported := settings.Version.IsActive(ctx, clusterversion.MVCCIndexBackfiller) && tabledesc.UseMVCCCompliantIndexCreation.Get(&settings.SV)
		if !mvccCompliantBackfillSupported {
			return errors.Newf("schema change requires MVCC-compliant backfiller, but MVCC-compliant backfiller is not supported")
		}
	}
	return nil
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

	if err := sc.checkForMVCCCompliantAddIndexMutations(ctx, desc); err != nil {
		return err
	}

	log.Infof(ctx,
		"schema change on %q (v%d) starting execution...",
		desc.GetName(), desc.GetVersion(),
	)

	// If there are any names to drain, then drain them.
	if len(desc.GetDrainingNames()) > 0 {
		if err := drainNamesForDescriptor(
			ctx, desc.GetID(), sc.execCfg.CollectionFactory, sc.db, sc.execCfg.InternalExecutor,
			sc.execCfg.Codec, sc.testingKnobs.OldNamesDrainedNotification,
		); err != nil {
			return err
		}
	}

	// Wait for the schema change to propagate to all nodes after this function
	// returns, so that the new schema is live everywhere. This is not needed for
	// correctness but is done to make the UI experience/tests predictable.
	waitToUpdateLeases := func(refreshStats bool) error {
		latestDesc, err := WaitToUpdateLeases(ctx, sc.leaseMgr, sc.descID)
		if err != nil {
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
			sc.refreshStats(latestDesc)
		}
		return nil
	}

	tableDesc, ok := desc.(catalog.TableDescriptor)
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
			if tableDesc.GetDropTime() > 0 {
				dropTime = tableDesc.GetDropTime()
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
			if err := DeleteTableDescAndZoneConfig(
				ctx, sc.db, sc.settings, sc.execCfg.Codec, tableDesc,
			); err != nil {
				return err
			}
		}
	}

	if err := sc.ignoreRevertedDropIndex(ctx, tableDesc); err != nil {
		return err
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

	if err := sc.maybeUpdateScheduledJobsForRowLevelTTL(ctx, tableDesc); err != nil {
		return err
	}

	if sc.mutationID == descpb.InvalidMutationID {
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
		if jobs.IsPauseSelfError(err) {
			// For testing only
			return err
		}
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

	// Ensure that this is a table descriptor and that the mutation is first in
	// line prior to reverting.
	{
		// Pull out the requested descriptor.
		desc, descErr := sc.getTargetDescriptor(ctx)
		if descErr != nil {
			return descErr
		}
		// Currently we don't attempt to roll back schema changes for anything other
		// than tables. For jobs intended to drop other types of descriptors, we do
		// nothing.
		if _, ok := desc.(catalog.TableDescriptor); !ok {
			return jobs.MarkAsPermanentJobError(errors.Newf("schema change jobs on databases and schemas cannot be reverted"))
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
		desc, err := WaitToUpdateLeases(ctx, sc.leaseMgr, sc.descID)
		if err != nil {
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
			sc.refreshStats(desc)
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
	return sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
		flags := tree.ObjectLookupFlagsWithRequired()
		flags.AvoidLeased = true
		desc, err := descriptors.GetImmutableTableByID(ctx, txn, sc.descID, flags)
		if err != nil {
			return err
		}

		var runStatus jobs.RunningStatus
		for _, mutation := range desc.AllMutations() {
			if mutation.MutationID() != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}

			if mutation.Adding() && mutation.DeleteOnly() {
				runStatus = RunningStatusDeleteOnly
			} else if mutation.Dropped() && mutation.WriteAndDeleteOnly() {
				runStatus = RunningStatusDeleteAndWriteOnly
			}
		}
		if runStatus != "" && !desc.Dropped() {
			if err := sc.job.RunningStatus(
				ctx, txn, func(ctx context.Context, details jobspb.Details) (jobs.RunningStatus, error) {
					return runStatus, nil
				}); err != nil {
				return errors.Wrapf(err, "failed to update job status")
			}
		}
		return nil
	})
}

func (sc *SchemaChanger) rollbackSchemaChange(ctx context.Context, err error) error {
	log.Warningf(ctx, "reversing schema change %d due to irrecoverable error: %s", sc.job.ID(), err)
	if errReverse := sc.maybeReverseMutations(ctx, err); errReverse != nil {
		return errReverse
	}

	if fn := sc.testingKnobs.RunAfterMutationReversal; fn != nil {
		if err := fn(sc.job.ID()); err != nil {
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
	gcJobID := sc.jobRegistry.MakeJobID()
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
		b.Del(catalogkeys.EncodeNameKey(sc.execCfg.Codec, scTable))

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
		if _, err := sc.jobRegistry.CreateJobWithTxn(ctx, jobRecord, gcJobID, txn); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	}); err != nil {
		return err
	}
	log.Infof(ctx, "starting GC job %d", gcJobID)
	sc.jobRegistry.NotifyToResume(ctx, gcJobID)
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
		_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
			ctx,
			txn,
			tbl.GetParentID(),
			tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return err
		}
		runStatus = ""
		// Apply mutations belonging to the same version.
		for _, m := range tbl.AllMutations() {
			if m.MutationID() != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			if m.Adding() {
				if m.DeleteOnly() {
					// TODO(vivek): while moving up the state is appropriate,
					// it will be better to run the backfill of a unique index
					// twice: once in the DELETE_ONLY state to confirm that
					// the index can indeed be created, and subsequently in the
					// DELETE_AND_WRITE_ONLY state to fill in the missing elements of the
					// index (INSERT and UPDATE that happened in the interim).
					tbl.Mutations[m.MutationOrdinal()].State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
					runStatus = RunningStatusDeleteAndWriteOnly
				}
				// else if DELETE_AND_WRITE_ONLY, then the state change has already moved forward.
			} else if m.Dropped() {
				if m.WriteAndDeleteOnly() || m.Merging() {
					tbl.Mutations[m.MutationOrdinal()].State = descpb.DescriptorMutation_DELETE_ONLY
					runStatus = RunningStatusDeleteOnly
				}
				// else if DELETE_ONLY, then the state change has already moved forward.
			}
			// We might have to update some zone configs for indexes that are
			// being rewritten. It is important that this is done _before_ the
			// index swap occurs. The logic that generates spans for subzone
			// configurations removes spans for indexes in the dropping state,
			// which we don't want. So, set up the zone configs before we swap.
			if err := sc.applyZoneConfigChangeForMutation(
				ctx,
				txn,
				dbDesc,
				tbl,
				m,
				false, // isDone
				descsCol,
			); err != nil {
				return err
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
			if err := sc.job.RunningStatus(ctx, txn, func(
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
	return nil
}

// RunStateMachineAfterIndexBackfill moves the state machine forward and
// wait to ensure that all nodes are seeing the latest version of the
// table.
//
// Adding Mutations in BACKFILLING state move through DELETE ->
// MERGING.
func (sc *SchemaChanger) RunStateMachineAfterIndexBackfill(ctx context.Context) error {
	// Step through the state machine twice:
	//  - BACKFILLING -> DELETE
	//  - DELETE -> MERGING
	log.Info(ctx, "stepping through state machine after index backfill")
	if err := sc.stepStateMachineAfterIndexBackfill(ctx); err != nil {
		return err
	}
	if err := sc.stepStateMachineAfterIndexBackfill(ctx); err != nil {
		return err
	}
	log.Info(ctx, "finished stepping through state machine")
	return nil
}

func (sc *SchemaChanger) stepStateMachineAfterIndexBackfill(ctx context.Context) error {
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
		for _, m := range tbl.AllMutations() {
			if m.MutationID() != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			idx := m.AsIndex()
			if idx == nil {
				// Don't touch anything but indexes
				continue
			}

			if m.Adding() {
				if m.Backfilling() {
					tbl.Mutations[m.MutationOrdinal()].State = descpb.DescriptorMutation_DELETE_ONLY
					runStatus = RunningStatusDeleteOnly
				} else if m.DeleteOnly() {
					tbl.Mutations[m.MutationOrdinal()].State = descpb.DescriptorMutation_MERGING
					runStatus = RunningStatusMerging
				}
			}
		}
		if runStatus == "" || tbl.Dropped() {
			return nil
		}
		if err := descsCol.WriteDesc(
			ctx, true /* kvTrace */, tbl, txn,
		); err != nil {
			return err
		}
		if sc.job != nil {
			if err := sc.job.RunningStatus(ctx, txn, func(
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
	return nil
}

func (sc *SchemaChanger) createTemporaryIndexGCJob(
	ctx context.Context, indexID descpb.IndexID, txn *kv.Txn, jobDesc string,
) error {
	minimumDropTime := int64(1)
	return sc.createIndexGCJobWithDropTime(ctx, indexID, txn, jobDesc, minimumDropTime)
}

func (sc *SchemaChanger) createIndexGCJob(
	ctx context.Context, indexID descpb.IndexID, txn *kv.Txn, jobDesc string,
) error {
	dropTime := timeutil.Now().UnixNano()
	return sc.createIndexGCJobWithDropTime(ctx, indexID, txn, jobDesc, dropTime)
}

func (sc *SchemaChanger) createIndexGCJobWithDropTime(
	ctx context.Context, indexID descpb.IndexID, txn *kv.Txn, jobDesc string, dropTime int64,
) error {
	indexGCDetails := jobspb.SchemaChangeGCDetails{
		Indexes: []jobspb.SchemaChangeGCDetails_DroppedIndex{
			{
				IndexID:  indexID,
				DropTime: dropTime,
			},
		},
		ParentID: sc.descID,
	}

	gcJobRecord := CreateGCJobRecord(jobDesc, sc.job.Payload().UsernameProto.Decode(), indexGCDetails)
	jobID := sc.jobRegistry.MakeJobID()
	if _, err := sc.jobRegistry.CreateJobWithTxn(ctx, gcJobRecord, jobID, txn); err != nil {
		return err
	}
	log.Infof(ctx, "created index GC job %d", jobID)
	sc.jobRegistry.NotifyToResume(ctx, jobID)
	return nil
}

// WaitToUpdateLeases until the entire cluster has been updated to the latest
// version of the descriptor.
func WaitToUpdateLeases(
	ctx context.Context, leaseMgr *lease.Manager, descID descpb.ID,
) (catalog.Descriptor, error) {
	// Aggressively retry because there might be a user waiting for the
	// schema change to complete.
	retryOpts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     1.5,
	}
	start := timeutil.Now()
	log.Infof(ctx, "waiting for a single version...")
	desc, err := leaseMgr.WaitForOneVersion(ctx, descID, retryOpts)
	var version descpb.DescriptorVersion
	if desc != nil {
		version = desc.GetVersion()
	}
	log.Infof(ctx, "waiting for a single version... done (at v %d), took %v", version, timeutil.Since(start))
	return desc, err
}

// done finalizes the mutations (adds new cols/indexes to the table).
// It ensures that all nodes are on the current (pre-update) version of
// sc.descID and that all nodes are on the new (post-update) version of
// any other modified descriptors.
//
// It also kicks off GC jobs as needed.
func (sc *SchemaChanger) done(ctx context.Context) error {
	// Gathers ant comments that need to be swapped/cleaned.
	type commentToDelete struct {
		id          int64
		subID       int64
		commentType keys.CommentType
	}
	type commentToSwap struct {
		id          int64
		oldSubID    int64
		newSubID    int64
		commentType keys.CommentType
	}
	var commentsToDelete []commentToDelete
	var commentsToSwap []commentToSwap
	// Jobs (for GC, etc.) that need to be started immediately after the table
	// descriptor updates are published.
	var didUpdate bool
	var depMutationJobs []jobspb.JobID
	var otherJobIDs []jobspb.JobID
	err := sc.execCfg.CollectionFactory.Txn(ctx, sc.execCfg.InternalExecutor, sc.db, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		depMutationJobs = depMutationJobs[:0]
		otherJobIDs = otherJobIDs[:0]
		var err error
		scTable, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}

		_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
			ctx,
			txn,
			scTable.GetParentID(),
			tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return err
		}

		collectReferencedTypeIDs := func() (catalog.DescriptorIDSet, error) {
			typeLookupFn := func(id descpb.ID) (catalog.TypeDescriptor, error) {
				desc, err := descsCol.GetImmutableTypeByID(ctx, txn, id, tree.ObjectLookupFlags{})
				if err != nil {
					return nil, err
				}
				return desc, nil
			}
			ids, _, err := scTable.GetAllReferencedTypeIDs(dbDesc, typeLookupFn)
			return catalog.MakeDescriptorIDSet(ids...), err
		}
		referencedTypeIDs, err := collectReferencedTypeIDs()

		collectReferencedSequenceIDs := func() map[descpb.ID]descpb.ColumnIDs {
			m := make(map[descpb.ID]descpb.ColumnIDs)
			for _, col := range scTable.AllColumns() {
				for i := 0; i < col.NumUsesSequences(); i++ {
					id := col.GetUsesSequenceID(i)
					m[id] = append(m[id], col.GetID())
				}
			}
			return m
		}
		referencedSequenceIDs := collectReferencedSequenceIDs()

		if err != nil {
			return err
		}
		b := txn.NewBatch()
		const kvTrace = true

		var i int           // set to determine whether there is a mutation
		var isRollback bool // set based on the mutation
		for _, m := range scTable.AllMutations() {
			if m.MutationID() != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			isRollback = m.IsRollback()
			if idx := m.AsIndex(); m.Dropped() && idx != nil {
				description := sc.job.Payload().Description
				if isRollback {
					description = "ROLLBACK of " + description
				}
				if idx.IsTemporaryIndexForBackfill() {
					if err := sc.createTemporaryIndexGCJob(ctx, idx.GetID(), txn, "temporary index used during index backfill"); err != nil {
						return err
					}
				} else {
					if err := sc.createIndexGCJob(ctx, idx.GetID(), txn, description); err != nil {
						return err
					}
				}
			}
			if constraint := m.AsConstraint(); constraint != nil && constraint.Adding() {
				if constraint.IsForeignKey() && constraint.ForeignKey().Validity == descpb.ConstraintValidity_Unvalidated {
					// Add backreference on the referenced table (which could be the same table)
					backrefTable, err := descsCol.GetMutableTableVersionByID(ctx, constraint.ForeignKey().ReferencedTableID, txn)
					if err != nil {
						return err
					}
					backrefTable.InboundFKs = append(backrefTable.InboundFKs, constraint.ForeignKey())
					if backrefTable != scTable {
						if err := descsCol.WriteDescToBatch(ctx, kvTrace, backrefTable, b); err != nil {
							return err
						}
					}
				}
			}

			// Ensure that zone configurations are finalized (or rolled back) when
			// done is called.
			// This will configure the table zone config for multi-region transformations.
			if err := sc.applyZoneConfigChangeForMutation(
				ctx,
				txn,
				dbDesc,
				scTable,
				m,
				true, // isDone
				descsCol,
			); err != nil {
				return err
			}

			// If we are refreshing a materialized view, then create GC jobs for all
			// of the existing indexes in the view. We do this before the call to
			// MakeMutationComplete, which swaps out the existing indexes for the
			// backfilled ones.
			if refresh := m.AsMaterializedViewRefresh(); refresh != nil {
				if fn := sc.testingKnobs.RunBeforeMaterializedViewRefreshCommit; fn != nil {
					if err := fn(); err != nil {
						return err
					}
				}
				// If we are mutation is in the ADD state, then start GC jobs for the
				// existing indexes on the table.
				if m.Adding() {
					desc := fmt.Sprintf("REFRESH MATERIALIZED VIEW %q cleanup", scTable.Name)
					for _, idx := range scTable.ActiveIndexes() {
						if err := sc.createIndexGCJob(ctx, idx.GetID(), txn, desc); err != nil {
							return err
						}
					}
				} else if m.Dropped() {
					// Otherwise, the refresh job ran into an error and is being rolled
					// back. So, we need to GC all of the indexes that were going to be
					// created, in case any data was written to them.
					desc := fmt.Sprintf("ROLLBACK OF REFRESH MATERIALIZED VIEW %q", scTable.Name)
					err = refresh.ForEachIndexID(func(id descpb.IndexID) error {
						return sc.createIndexGCJob(ctx, id, txn, desc)
					})
					if err != nil {
						return err
					}
				}
			}

			// If a primary index swap or any indexes are being dropped clean up any
			// comments related to it.
			if pkSwap := m.AsPrimaryKeySwap(); pkSwap != nil {
				id := pkSwap.PrimaryKeySwapDesc().OldPrimaryIndexId
				commentsToDelete = append(commentsToDelete,
					commentToDelete{
						id:          int64(scTable.GetID()),
						subID:       int64(id),
						commentType: keys.IndexCommentType,
					})
				for i := range pkSwap.PrimaryKeySwapDesc().OldIndexes {
					// Skip the primary index.
					if pkSwap.PrimaryKeySwapDesc().OldIndexes[i] == id {
						continue
					}
					// Set up a swap operation for any re-created indexes.
					commentsToSwap = append(commentsToSwap,
						commentToSwap{
							id:          int64(scTable.GetID()),
							oldSubID:    int64(pkSwap.PrimaryKeySwapDesc().OldIndexes[i]),
							newSubID:    int64(pkSwap.PrimaryKeySwapDesc().NewIndexes[i]),
							commentType: keys.IndexCommentType,
						},
					)
				}
			}

			if err := scTable.MakeMutationComplete(scTable.Mutations[m.MutationOrdinal()]); err != nil {
				return err
			}

			// If we are modifying TTL, then make sure the schedules are created
			// or dropped as appropriate.
			if modify := m.AsModifyRowLevelTTL(); modify != nil {
				if fn := sc.testingKnobs.RunBeforeModifyRowLevelTTL; fn != nil {
					if err := fn(); err != nil {
						return err
					}
				}
				if m.Adding() {
					scTable.RowLevelTTL = modify.RowLevelTTL()
					shouldCreateScheduledJob := scTable.RowLevelTTL.ScheduleID == 0
					// Double check the job exists - if it does not, we need to recreate it.
					if scTable.RowLevelTTL.ScheduleID != 0 {
						_, err := jobs.LoadScheduledJob(
							ctx,
							JobSchedulerEnv(sc.execCfg),
							scTable.RowLevelTTL.ScheduleID,
							sc.execCfg.InternalExecutor,
							txn,
						)
						if err != nil {
							if !jobs.HasScheduledJobNotFoundError(err) {
								return errors.Wrapf(err, "unknown error fetching existing job for row level TTL in schema changer")
							}
							shouldCreateScheduledJob = true
						}
					}

					if shouldCreateScheduledJob {
						j, err := CreateRowLevelTTLScheduledJob(
							ctx,
							sc.execCfg,
							txn,
							getOwnerOfDesc(scTable),
							scTable.GetID(),
							modify.RowLevelTTL(),
						)
						if err != nil {
							return err
						}
						scTable.RowLevelTTL.ScheduleID = j.ScheduleID()
					}
				} else if m.Dropped() {
					if ttl := scTable.RowLevelTTL; ttl != nil {
						if err := DeleteSchedule(ctx, sc.execCfg, txn, ttl.ScheduleID); err != nil {
							return err
						}
					}
					scTable.RowLevelTTL = nil
				}
			}

			if pkSwap := m.AsPrimaryKeySwap(); pkSwap != nil {
				if fn := sc.testingKnobs.RunBeforePrimaryKeySwap; fn != nil {
					fn()
				}
				// For locality swaps, ensure the table descriptor fields are correctly filled.
				if pkSwap.HasLocalityConfig() {
					lcSwap := pkSwap.LocalityConfigSwap()
					localityConfigToSwapTo := lcSwap.NewLocalityConfig
					if m.Adding() {
						// Sanity check that locality has not been changed during backfill.
						if !scTable.LocalityConfig.Equal(lcSwap.OldLocalityConfig) {
							return errors.AssertionFailedf(
								"expected locality on table to match old locality\ngot: %s\nwant %s",
								scTable.LocalityConfig,
								lcSwap.OldLocalityConfig,
							)
						}

						// If we are adding a new REGIONAL BY ROW column, after backfilling, the
						// default expression should be switched to utilize to gateway_region.
						if colID := lcSwap.NewRegionalByRowColumnID; colID != nil {
							col, err := scTable.FindColumnWithID(*colID)
							if err != nil {
								return err
							}
							col.ColumnDesc().DefaultExpr = lcSwap.NewRegionalByRowColumnDefaultExpr
						}

					} else {
						// DROP is hit on cancellation, in which case we must roll back.
						localityConfigToSwapTo = lcSwap.OldLocalityConfig
					}

					if err := setNewLocalityConfig(
						ctx, scTable, txn, b, localityConfigToSwapTo, kvTrace, descsCol); err != nil {
						return err
					}
					switch localityConfigToSwapTo.Locality.(type) {
					case *catpb.LocalityConfig_RegionalByTable_,
						*catpb.LocalityConfig_Global_:
						scTable.PartitionAllBy = false
					case *catpb.LocalityConfig_RegionalByRow_:
						scTable.PartitionAllBy = true
					default:
						return errors.AssertionFailedf(
							"unknown locality on PK swap: %T",
							localityConfigToSwapTo,
						)
					}
				}

				// If we performed MakeMutationComplete on a PrimaryKeySwap mutation, then we need to start
				// a job for the index deletion mutations that the primary key swap mutation added, if any.
				jobID, err := sc.queueCleanupJob(ctx, scTable, txn)
				if err != nil {
					return err
				}
				if jobID > 0 {
					depMutationJobs = append(depMutationJobs, jobID)
				}
			}

			if m.AsComputedColumnSwap() != nil {
				if fn := sc.testingKnobs.RunBeforeComputedColumnSwap; fn != nil {
					fn()
				}

				// If we performed MakeMutationComplete on a computed column swap, then
				// we need to start a job for the column deletion that the swap mutation
				// added if any.
				jobID, err := sc.queueCleanupJob(ctx, scTable, txn)
				if err != nil {
					return err
				}
				if jobID > 0 {
					depMutationJobs = append(depMutationJobs, jobID)
				}
			}
			didUpdate = true
			i++
		}
		if didUpdate = i > 0; !didUpdate {
			// The table descriptor is unchanged, return without writing anything.
			return nil
		}
		committedMutations := scTable.AllMutations()[:i]
		// Trim the executed mutations from the descriptor.
		scTable.Mutations = scTable.Mutations[i:]

		// Check any jobs that we need to depend on for the current
		// job to be successful.
		existingDepMutationJobs, err := sc.getDependentMutationsJobs(ctx, scTable, committedMutations)
		if err != nil {
			return err
		}
		depMutationJobs = append(depMutationJobs, existingDepMutationJobs...)

		for i, g := range scTable.MutationJobs {
			if g.MutationID == sc.mutationID {
				// Trim the executed mutation group from the descriptor.
				scTable.MutationJobs = append(scTable.MutationJobs[:i], scTable.MutationJobs[i+1:]...)
				break
			}
		}

		// Now that all mutations have been applied, find the new set of referenced
		// type descriptors. If this table has been dropped in the meantime, then
		// don't install any back references.
		if !scTable.Dropped() {
			newReferencedTypeIDs, err := collectReferencedTypeIDs()
			if err != nil {
				return err
			}
			update := make(map[descpb.ID]bool, newReferencedTypeIDs.Len()+referencedTypeIDs.Len())
			newReferencedTypeIDs.ForEach(func(id descpb.ID) {
				if !referencedTypeIDs.Contains(id) {
					// Mark id as requiring update, `true` means addition.
					update[id] = true
				}
			})
			referencedTypeIDs.ForEach(func(id descpb.ID) {
				if !newReferencedTypeIDs.Contains(id) {
					// Mark id as requiring update, `false` means deletion.
					update[id] = false
				}
			})

			// Update the set of back references.
			for id, isAddition := range update {
				typ, err := descsCol.GetMutableTypeVersionByID(ctx, txn, id)
				if err != nil {
					return err
				}
				if isAddition {
					typ.AddReferencingDescriptorID(scTable.ID)
				} else {
					typ.RemoveReferencingDescriptorID(scTable.ID)
				}
				if err := descsCol.WriteDescToBatch(ctx, kvTrace, typ, b); err != nil {
					return err
				}
			}
		}

		// Now do the same as the above but for referenced sequences.
		if !scTable.Dropped() {
			newReferencedSequenceIDs := collectReferencedSequenceIDs()
			update := make(map[descpb.ID]catalog.TableColSet, len(newReferencedSequenceIDs)+len(referencedSequenceIDs))
			for id := range referencedSequenceIDs {
				if _, found := newReferencedSequenceIDs[id]; !found {
					// Mark id as requiring update, empty col set means deletion.
					update[id] = catalog.TableColSet{}
				}
			}
			for id, newColIDs := range newReferencedSequenceIDs {
				newColIDSet := catalog.MakeTableColSet(newColIDs...)
				var oldColIDSet catalog.TableColSet
				if oldColIDs, found := referencedSequenceIDs[id]; found {
					oldColIDSet = catalog.MakeTableColSet(oldColIDs...)
				}
				union := catalog.MakeTableColSet(newColIDs...)
				union.UnionWith(oldColIDSet)
				if union.Len() != oldColIDSet.Len() || union.Len() != newColIDSet.Len() {
					// Mark id as requiring update with new col set.
					update[id] = newColIDSet
				}
			}

			// Update the set of back references.
			for id, colIDSet := range update {
				tbl, err := descsCol.GetMutableTableVersionByID(ctx, id, txn)
				if err != nil {
					return err
				}
				tbl.UpdateColumnsDependedOnBy(scTable.ID, colIDSet)
				if err := descsCol.WriteDescToBatch(ctx, kvTrace, tbl, b); err != nil {
					return err
				}
			}
		}

		// Clean up any comments related to the mutations, specifically if we need
		// to drop them.
		metaDataUpdater := sc.execCfg.DescMetadaUpdaterFactory.NewMetadataUpdater(
			ctx,
			txn,
			NewFakeSessionData(&sc.settings.SV))
		for _, comment := range commentsToDelete {
			err := metaDataUpdater.DeleteDescriptorComment(
				comment.id,
				comment.subID,
				comment.commentType)
			if err != nil {
				return err
			}
		}
		for _, comment := range commentsToSwap {
			err := metaDataUpdater.SwapDescriptorSubComment(
				comment.id,
				comment.oldSubID,
				comment.newSubID,
				comment.commentType,
			)
			if err != nil {
				return err
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
	if err != nil {
		return err
	}

	// If any operations was skipped because a mutation was made
	// redundant due to a column getting dropped later on then we should
	// wait for those jobs to complete before returning our result back.
	if err := sc.jobRegistry.Run(ctx, sc.execCfg.InternalExecutor, depMutationJobs); err != nil {
		return errors.Wrap(err, "A dependent transaction failed for this schema change")
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
	zone, err := getZoneConfigRaw(ctx, txn, execCfg.Codec, execCfg.Settings, table.ID)
	if err != nil {
		return err
	}

	// If this table doesn't have a zone attached to it, don't do anything.
	if zone == nil {
		return nil
	}

	// For each rewritten index, point its subzones for the old index at the
	// new index.
	oldIdxToNewIdx := make(map[descpb.IndexID]descpb.IndexID)
	for i, oldID := range swapInfo.OldIndexes {
		oldIdxToNewIdx[oldID] = swapInfo.NewIndexes[i]
	}

	// It is safe to copy the zone config off a primary index of a REGIONAL BY ROW table.
	// This is because the prefix of the PK we used to partition by will stay the same,
	// so a direct copy will always work.
	if table.IsLocalityRegionalByRow() {
		oldIdxToNewIdx[swapInfo.OldPrimaryIndexId] = swapInfo.NewPrimaryIndexId
	}

	for oldIdx, newIdx := range oldIdxToNewIdx {
		for i := range zone.Subzones {
			subzone := &zone.Subzones[i]
			if subzone.IndexID == uint32(oldIdx) {
				// If we find a subzone matching an old index, copy its subzone
				// into a new subzone with the new index's ID.
				subzoneCopy := *subzone
				subzoneCopy.IndexID = uint32(newIdx)
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

	if err := sc.preSplitHashShardedIndexRanges(ctx); err != nil {
		return err
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

func (sc *SchemaChanger) refreshStats(desc catalog.Descriptor) {
	// Initiate an asynchronous run of CREATE STATISTICS. We use a large number
	// for rowsAffected because we want to make sure that stats always get
	// created/refreshed here.
	if tableDesc, ok := desc.(catalog.TableDescriptor); ok {
		sc.execCfg.StatsRefresher.NotifyMutation(tableDesc, math.MaxInt32 /* rowsAffected */)
	}
}

// maybeReverseMutations reverses the direction of all the mutations with the
// mutationID. This is called after hitting an irrecoverable error while
// applying a schema change. If a column being added is reversed and dropped,
// all new indexes referencing the column will also be dropped.
func (sc *SchemaChanger) maybeReverseMutations(ctx context.Context, causingError error) error {
	if fn := sc.testingKnobs.RunBeforeMutationReversal; fn != nil {
		if err := fn(sc.job.ID()); err != nil {
			return err
		}
	}
	// There's nothing to do if the mutation is InvalidMutationID.
	if sc.mutationID == descpb.InvalidMutationID {
		return nil
	}

	// Get the other tables whose foreign key backreferences need to be removed.
	alreadyReversed := false
	const kvTrace = true // TODO(ajwerner): figure this out
	err := sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		scTable, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
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

		// Create update closure for the table and all other tables with backreferences
		var droppedMutations map[descpb.MutationID]struct{}

		// Keep track of the column mutations being reversed so that indexes
		// referencing them can be dropped.
		columns := make(map[string]struct{})
		b := txn.NewBatch()
		for _, m := range scTable.AllMutations() {
			if m.MutationID() != sc.mutationID {
				break
			}

			if m.IsRollback() {
				// Can actually never happen. Since we should have checked for this case
				// above.
				return errors.AssertionFailedf("mutation already rolled back: %v", scTable.Mutations[m.MutationOrdinal()])
			}

			// Ignore mutations that would be skipped, nothing
			// to reverse here.
			if discarded, _ := isCurrentMutationDiscarded(scTable, m, m.MutationOrdinal()+1); discarded {
				continue
			}

			// Always move temporary indexes to dropping
			if idx := m.AsIndex(); idx != nil && idx.IsTemporaryIndexForBackfill() {
				scTable.Mutations[m.MutationOrdinal()].State = descpb.DescriptorMutation_DELETE_ONLY
				scTable.Mutations[m.MutationOrdinal()].Direction = descpb.DescriptorMutation_DROP
			} else {
				log.Warningf(ctx, "reverse schema change mutation: %+v", scTable.Mutations[m.MutationOrdinal()])
				scTable.Mutations[m.MutationOrdinal()], columns = sc.reverseMutation(scTable.Mutations[m.MutationOrdinal()], false /*notStarted*/, columns)
			}

			// If the mutation is for validating a constraint that is being added,
			// drop the constraint because validation has failed.
			if constraint := m.AsConstraint(); constraint != nil && constraint.Adding() {
				log.Warningf(ctx, "dropping constraint %+v", constraint.ConstraintToUpdateDesc())
				if err := sc.maybeDropValidatingConstraint(ctx, scTable, constraint); err != nil {
					return err
				}
				// Get the foreign key backreferences to remove.
				if constraint.IsForeignKey() {
					backrefTable, err := descsCol.GetMutableTableVersionByID(ctx, constraint.ForeignKey().ReferencedTableID, txn)
					if err != nil {
						return err
					}
					if err := removeFKBackReferenceFromTable(backrefTable, constraint.GetName(), scTable); err != nil {
						// The function being called will return an assertion error if the
						// backreference was not found, but it may not have been installed
						// during the incomplete schema change, so we swallow the error.
						log.Infof(ctx,
							"error attempting to remove backreference %s during rollback: %s", constraint.GetName(), err)
					}
					if err := descsCol.WriteDescToBatch(ctx, kvTrace, backrefTable, b); err != nil {
						return err
					}
				}
			}
			scTable.Mutations[m.MutationOrdinal()].Rollback = true
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

		tableDesc := scTable.ImmutableCopy().(catalog.TableDescriptor)
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
	return nil
}

// updateJobForRollback updates the schema change job in the case of a rollback.
func (sc *SchemaChanger) updateJobForRollback(
	ctx context.Context, txn *kv.Txn, tableDesc catalog.TableDescriptor,
) error {
	// Initialize refresh spans to scan the entire table.
	span := tableDesc.PrimaryIndexSpan(sc.execCfg.Codec)
	var spanList []jobspb.ResumeSpanList
	for _, m := range tableDesc.AllMutations() {
		if m.MutationID() == sc.mutationID {
			spanList = append(spanList,
				jobspb.ResumeSpanList{
					ResumeSpans: []roachpb.Span{span},
				},
			)
		}
	}
	oldDetails := sc.job.Details().(jobspb.SchemaChangeDetails)
	if err := sc.job.SetDetails(
		ctx, txn, jobspb.SchemaChangeDetails{
			DescID:          sc.descID,
			TableMutationID: sc.mutationID,
			ResumeSpanList:  spanList,
			FormatVersion:   oldDetails.FormatVersion,
		},
	); err != nil {
		return err
	}
	if err := sc.job.SetProgress(ctx, txn, jobspb.SchemaChangeProgress{}); err != nil {
		return err
	}

	return nil
}

func (sc *SchemaChanger) maybeDropValidatingConstraint(
	ctx context.Context, desc *tabledesc.Mutable, constraint catalog.ConstraintToUpdate,
) error {
	if constraint.IsCheck() || constraint.IsNotNull() {
		if constraint.Check().Validity == descpb.ConstraintValidity_Unvalidated {
			return nil
		}
		for j, c := range desc.Checks {
			if c.Name == constraint.Check().Name {
				desc.Checks = append(desc.Checks[:j], desc.Checks[j+1:]...)
				return nil
			}
		}
		log.Infof(
			ctx,
			"attempted to drop constraint %s, but it hadn't been added to the table descriptor yet",
			constraint.Check().Name,
		)
	} else if constraint.IsForeignKey() {
		for i, fk := range desc.OutboundFKs {
			if fk.Name == constraint.ForeignKey().Name {
				desc.OutboundFKs = append(desc.OutboundFKs[:i], desc.OutboundFKs[i+1:]...)
				return nil
			}
		}
		log.Infof(
			ctx,
			"attempted to drop constraint %s, but it hadn't been added to the table descriptor yet",
			constraint.ForeignKey().Name,
		)
	} else if constraint.IsUniqueWithoutIndex() {
		if constraint.UniqueWithoutIndex().Validity == descpb.ConstraintValidity_Unvalidated {
			return nil
		}
		for j, c := range desc.UniqueWithoutIndexConstraints {
			if c.Name == constraint.UniqueWithoutIndex().Name {
				desc.UniqueWithoutIndexConstraints = append(
					desc.UniqueWithoutIndexConstraints[:j], desc.UniqueWithoutIndexConstraints[j+1:]...,
				)
				return nil
			}
		}
		log.Infof(
			ctx,
			"attempted to drop constraint %s, but it hadn't been added to the table descriptor yet",
			constraint.UniqueWithoutIndex().Name,
		)
	} else {
		return errors.AssertionFailedf("unsupported constraint type: %d", constraint.ConstraintToUpdateDesc().ConstraintType)
	}
	return nil
}

// validStateForStartingIndex returns the correct starting state for
// add index mutations based on the whether this schema change is
// using temporary indexes or not.
func (sc *SchemaChanger) startingStateForAddIndexMutations() descpb.DescriptorMutation_State {
	if sc.mvccCompliantAddIndex {
		return descpb.DescriptorMutation_BACKFILLING
	}
	return descpb.DescriptorMutation_DELETE_ONLY
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
					for _, name := range idx.KeyColumnNames {
						if _, ok := columns[name]; ok {
							// Such an index mutation has to be with direction ADD and
							// in the DELETE_ONLY state. Live indexes referencing live
							// columns cannot be deleted and thus never have direction
							// DROP. All mutations with the ADD direction start off in
							// the DELETE_ONLY state.
							if mutation.Direction != descpb.DescriptorMutation_ADD ||
								mutation.State != sc.startingStateForAddIndexMutations() {
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
		for _, m := range desc.AllMutations() {
			mutation := desc.Mutations[m.MutationOrdinal()]
			if _, ok := dropMutations[m.MutationID()]; ok {
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

		if notStarted {
			startingState := descpb.DescriptorMutation_DELETE_ONLY
			if idx := mutation.GetIndex(); idx != nil {
				startingState = sc.startingStateForAddIndexMutations()
			}
			if mutation.State != startingState {
				panic(errors.AssertionFailedf("mutation in bad state: %+v", mutation))
			}
		}

	case descpb.DescriptorMutation_DROP:
		// DROP INDEX is not reverted.
		if mutation.GetIndex() != nil {
			return mutation, columns
		}

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
	RunBeforeResume    func(jobID jobspb.JobID) error
	RunBeforePerformGC func(jobID jobspb.JobID) error
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
	RunAfterBackfill func(jobID jobspb.JobID) error

	// RunBeforeQueryBackfill is called before a query based backfill.
	RunBeforeQueryBackfill func() error

	// RunBeforeIndexBackfill is called just before starting the index backfill, after
	// fixing the index backfill scan timestamp.
	RunBeforeIndexBackfill func()

	// RunBeforeIndexBackfill is called after the index backfill
	// process is complete (including the temporary index merge)
	// but before the final validation of the indexes.
	RunAfterIndexBackfill func()

	// RunBeforeTempIndexMerge is called just before starting the
	// the merge from the temporary index into the new index,
	// after the backfill scan timestamp has been fixed.
	RunBeforeTempIndexMerge func()

	// RunAfterTempIndexMerge is called, before validating and
	// making the next index public.
	RunAfterTempIndexMerge func()

	// RunBeforeMaterializedViewRefreshCommit is called before committing a
	// materialized view refresh.
	RunBeforeMaterializedViewRefreshCommit func() error

	// RunBeforePrimaryKeySwap is called just before the primary key swap is committed.
	RunBeforePrimaryKeySwap func()

	// RunBeforeComputedColumnSwap is called just before the computed column swap is committed.
	RunBeforeComputedColumnSwap func()

	// RunBeforeIndexValidation is called just before starting the index validation,
	// after setting the job status to validating.
	RunBeforeIndexValidation func() error

	// RunBeforeConstraintValidation is called just before starting the checks validation,
	// after setting the job status to validating.
	RunBeforeConstraintValidation func(constraints []catalog.ConstraintToUpdate) error

	// RunBeforeMutationReversal runs at the beginning of maybeReverseMutations.
	RunBeforeMutationReversal func(jobID jobspb.JobID) error

	// RunAfterMutationReversal runs in OnFailOrCancel after the mutations have
	// been reversed.
	RunAfterMutationReversal func(jobID jobspb.JobID) error

	// RunAtStartOfOnFailOrCancel runs at the start of the OnFailOrCancel hook.
	RunBeforeOnFailOrCancel func(jobID jobspb.JobID) error

	// RunAfterOnFailOrCancel runs after the OnFailOrCancel hook.
	RunAfterOnFailOrCancel func(jobID jobspb.JobID) error

	// RunBeforeResume runs at the start of the Resume hook.
	RunBeforeResume func(jobID jobspb.JobID) error

	// RunBeforeDescTxn runs at the start of every call to
	// (*schemaChanger).txn.
	RunBeforeDescTxn func(jobID jobspb.JobID) error

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

	// RunBeforeHashShardedIndexRangePreSplit is called before pre-splitting index
	// ranges for hash sharded index.
	RunBeforeHashShardedIndexRangePreSplit func(tbl *tabledesc.Mutable, kbDB *kv.DB, codec keys.SQLCodec) error

	// RunAfterHashShardedIndexRangePreSplit is called after index ranges
	// pre-splitting is done for hash sharded index.
	RunAfterHashShardedIndexRangePreSplit func(tbl *tabledesc.Mutable, kbDB *kv.DB, codec keys.SQLCodec) error

	// RunBeforeModifyRowLevelTTL is called just before the modify row level TTL is committed.
	RunBeforeModifyRowLevelTTL func() error
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*SchemaChangerTestingKnobs) ModuleTestingKnobs() {}

// txn is a convenient wrapper around descs.Txn().
func (sc *SchemaChanger) txn(
	ctx context.Context, f func(context.Context, *kv.Txn, *descs.Collection) error,
) error {
	if fn := sc.testingKnobs.RunBeforeDescTxn; fn != nil {
		if err := fn(sc.job.ID()); err != nil {
			return err
		}
	}

	return sc.execCfg.CollectionFactory.Txn(ctx, sc.execCfg.InternalExecutor, sc.db, f)
}

// createSchemaChangeEvalCtx creates an extendedEvalContext() to be used for backfills.
//
// TODO(andrei): This EvalContext() will be broken for backfills trying to use
// functions marked with distsqlBlocklist.
// Also, the SessionTracing inside the context is unrelated to the one
// used in the surrounding SQL session, so session tracing is unable
// to capture schema change activity.
func createSchemaChangeEvalCtx(
	ctx context.Context, execCfg *ExecutorConfig, ts hlc.Timestamp, descriptors *descs.Collection,
) extendedEvalContext {

	sd := NewFakeSessionData(execCfg.SV())

	evalCtx := extendedEvalContext{
		// Make a session tracing object on-the-fly. This is OK
		// because it sets "enabled: false" and thus none of the
		// other fields are used.
		Tracing: &SessionTracing{},
		ExecCfg: execCfg,
		Descs:   descriptors,
		EvalContext: tree.EvalContext{
			SessionDataStack: sessiondata.NewStack(sd),
			// TODO(andrei): This is wrong (just like on the main code path on
			// setupFlow). Each processor should override Ctx with its own context.
			Context:            ctx,
			Planner:            &faketreeeval.DummyEvalPlanner{},
			PrivilegedAccessor: &faketreeeval.DummyPrivilegedAccessor{},
			SessionAccessor:    &faketreeeval.DummySessionAccessor{},
			ClientNoticeSender: &faketreeeval.DummyClientNoticeSender{},
			Sequence:           &faketreeeval.DummySequenceOperators{},
			Tenant:             &faketreeeval.DummyTenantOperator{},
			Regions:            &faketreeeval.DummyRegionOperator{},
			Settings:           execCfg.Settings,
			TestingKnobs:       execCfg.EvalContextTestingKnobs,
			ClusterID:          execCfg.ClusterID(),
			ClusterName:        execCfg.RPCContext.ClusterName(),
			NodeID:             execCfg.NodeID,
			Codec:              execCfg.Codec,
			Locality:           execCfg.Locality,
			Tracer:             execCfg.AmbientCtx.Tracer,
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

// NewFakeSessionData returns "fake" session data for use in internal queries
// that are not run on behalf of a user session, such as those run during the
// steps of background jobs and schema changes.
func NewFakeSessionData(sv *settings.Values) *sessiondata.SessionData {
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
			Database:      "",
			UserProto:     security.NodeUserName().EncodeProto(),
			VectorizeMode: sessiondatapb.VectorizeExecMode(VectorizeClusterMode.Get(sv)),
			Internal:      true,
		},
		LocalOnlySessionData: sessiondatapb.LocalOnlySessionData{
			DistSQLMode: sessiondatapb.DistSQLExecMode(DistSQLClusterExecMode.Get(sv)),
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

func (r schemaChangeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	details := r.job.Details().(jobspb.SchemaChangeDetails)
	if p.ExecCfg().SchemaChangerTestingKnobs.SchemaChangeJobNoOp != nil &&
		p.ExecCfg().SchemaChangerTestingKnobs.SchemaChangeJobNoOp() {
		return nil
	}
	if fn := p.ExecCfg().SchemaChangerTestingKnobs.RunBeforeResume; fn != nil {
		if err := fn(r.job.ID()); err != nil {
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
			if err := p.ExecCfg().JobRegistry.CheckPausepoint("schemachanger.before.exec"); err != nil {
				return err
			}
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
			case !IsPermanentSchemaChangeError(scErr):
				// Check if the error is on a allowlist of errors we should retry on,
				// including the schema change not having the first mutation in line.
				log.Warningf(ctx, "error while running schema change, retrying: %v", scErr)
				sc.metrics.RetryErrors.Inc(1)
				if IsConstraintError(scErr) {
					telemetry.Inc(sc.metrics.ConstraintErrors)
				} else {
					telemetry.Inc(sc.metrics.UncategorizedErrors)
				}
			default:
				if ctx.Err() == nil {
					sc.metrics.PermanentErrors.Inc(1)
				}
				if IsConstraintError(scErr) {
					telemetry.Inc(sc.metrics.ConstraintErrors)
				} else {
					telemetry.Inc(sc.metrics.UncategorizedErrors)
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
			//
			// NB: Secondary tenants prior to the introduction of system.zones could
			// not set zone configurations, so there's nothing to do for them.
			if len(details.DroppedTables) == 0 {
				zoneKeyPrefix := config.MakeZoneKeyPrefix(p.ExecCfg().Codec, dbID)
				if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
					log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
				}
				// Delete the zone config entry for this database.
				if _, err := p.ExecCfg().DB.DelRange(ctx, zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */); err != nil {
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

	if fn := p.ExecCfg().SchemaChangerTestingKnobs.RunBeforeOnFailOrCancel; fn != nil {
		if err := fn(r.job.ID()); err != nil {
			return err
		}
	}

	// If this is a schema change to drop a database or schema, DescID will be
	// unset. We cannot revert such schema changes, so just exit early with an
	// error.
	if details.DescID == descpb.InvalidID {
		return errors.Newf("schema change jobs on databases and schemas cannot be reverted")
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
		case !IsPermanentSchemaChangeError(rollbackErr):
			// Check if the error is on a allowlist of errors we should retry on, and
			// have the job registry retry.
			return jobs.MarkAsRetryJobError(rollbackErr)
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
		if err := fn(r.job.ID()); err != nil {
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

// queueCleanupJob checks if the completed schema change needs to start a
// child job to clean up dropped schema elements.
func (sc *SchemaChanger) queueCleanupJob(
	ctx context.Context, scDesc *tabledesc.Mutable, txn *kv.Txn,
) (jobspb.JobID, error) {
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
	var jobID jobspb.JobID
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
		jobID = sc.jobRegistry.MakeJobID()
		if _, err := sc.jobRegistry.CreateJobWithTxn(ctx, jobRecord, jobID, txn); err != nil {
			return 0, err
		}
		log.Infof(ctx, "created job %d to drop previous columns and indexes", jobID)
		scDesc.MutationJobs = append(scDesc.MutationJobs, descpb.TableDescriptor_MutationJob{
			MutationID: mutationID,
			JobID:      jobID,
		})
	}
	return jobID, nil
}

func (sc *SchemaChanger) applyZoneConfigChangeForMutation(
	ctx context.Context,
	txn *kv.Txn,
	dbDesc catalog.DatabaseDescriptor,
	tableDesc *tabledesc.Mutable,
	mutation catalog.Mutation,
	isDone bool,
	descsCol *descs.Collection,
) error {
	if pkSwap := mutation.AsPrimaryKeySwap(); pkSwap != nil {
		if pkSwap.HasLocalityConfig() {
			// We will add up to three options - one for the table itself,
			// one for dropping any zone configs for old indexes and one
			// for all the new indexes associated with the table.
			opts := make([]applyZoneConfigForMultiRegionTableOption, 0, 3)
			lcSwap := pkSwap.LocalityConfigSwap()

			// For locality configs, we need to update the zone configs to match
			// the new multi-region locality configuration, instead of
			// copying the old zone configs over.
			if mutation.Adding() {
				// Only apply the zone configuration on the table when the mutation
				// is complete.
				if isDone {
					// The table re-writing the LocalityConfig does most of the work for
					// us here, but if we're coming from REGIONAL BY ROW, it's also
					// necessary to drop the zone configurations on the index partitions.
					if lcSwap.OldLocalityConfig.GetRegionalByRow() != nil {
						oldIndexIDs := make([]descpb.IndexID, 0, pkSwap.NumOldIndexes())
						_ = pkSwap.ForEachOldIndexIDs(func(id descpb.IndexID) error {
							oldIndexIDs = append(oldIndexIDs, id)
							return nil
						})
						opts = append(opts, dropZoneConfigsForMultiRegionIndexes(oldIndexIDs...))
					}

					opts = append(
						opts,
						applyZoneConfigForMultiRegionTableOptionTableNewConfig(
							lcSwap.NewLocalityConfig,
						),
					)
				}
				switch lcSwap.NewLocalityConfig.Locality.(type) {
				case *catpb.LocalityConfig_Global_,
					*catpb.LocalityConfig_RegionalByTable_:
				case *catpb.LocalityConfig_RegionalByRow_:
					// Apply new zone configurations for all newly partitioned indexes.
					newIndexIDs := make([]descpb.IndexID, 0, pkSwap.NumNewIndexes())
					_ = pkSwap.ForEachNewIndexIDs(func(id descpb.IndexID) error {
						newIndexIDs = append(newIndexIDs, id)
						return nil
					})
					opts = append(opts, applyZoneConfigForMultiRegionTableOptionNewIndexes(newIndexIDs...))
				default:
					return errors.AssertionFailedf(
						"unknown locality on PK swap: %T",
						lcSwap.NewLocalityConfig.Locality,
					)
				}
			} else {
				// DROP is hit on cancellation, in which case we must roll back.
				opts = append(
					opts,
					applyZoneConfigForMultiRegionTableOptionTableNewConfig(
						lcSwap.OldLocalityConfig,
					),
				)
			}

			regionConfig, err := SynthesizeRegionConfig(ctx, txn, dbDesc.GetID(), descsCol)
			if err != nil {
				return err
			}
			if err := ApplyZoneConfigForMultiRegionTable(
				ctx,
				txn,
				sc.execCfg,
				regionConfig,
				tableDesc,
				opts...,
			); err != nil {
				return err
			}
		}

		// In all cases, we now copy the zone configs over for any new indexes.
		// Note this is done even for isDone = true, though not strictly
		// necessary.
		return maybeUpdateZoneConfigsForPKChange(
			ctx, txn, sc.execCfg, tableDesc, pkSwap.PrimaryKeySwapDesc(),
		)
	}
	return nil
}

// DeleteTableDescAndZoneConfig removes a table's descriptor and zone config from the KV database.
func DeleteTableDescAndZoneConfig(
	ctx context.Context,
	db *kv.DB,
	settings *cluster.Settings,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
) error {
	log.Infof(ctx, "removing table descriptor and zone config for table %d", tableDesc.GetID())
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if !settings.Version.IsActive(
			ctx, clusterversion.DisableSystemConfigGossipTrigger,
		) {
			if err := txn.DeprecatedSetSystemConfigTrigger(codec.ForSystemTenant()); err != nil {
				return err
			}
		}
		b := &kv.Batch{}

		// Delete the descriptor.
		descKey := catalogkeys.MakeDescMetadataKey(codec, tableDesc.GetID())
		b.Del(descKey)
		// Delete the zone config entry for this table, if necessary.
		if codec.ForSystemTenant() {
			zoneKeyPrefix := config.MakeZoneKeyPrefix(codec, tableDesc.GetID())
			b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		}
		return txn.Run(ctx, b)
	})
}

// getDependentMutationJobs gets the dependent jobs that need to complete for
// the current schema change to be considered successful. For example, if column
// A will have a unique index, but it later gets dropped. Then for this mutation
// to be successful the drop column job has to be successful too.
func (sc *SchemaChanger) getDependentMutationsJobs(
	ctx context.Context, tableDesc *tabledesc.Mutable, mutations []catalog.Mutation,
) ([]jobspb.JobID, error) {
	dependentJobs := make([]jobspb.JobID, 0, len(tableDesc.MutationJobs))
	for _, m := range mutations {
		// Find all the mutations that we depend
		discarded, dependentID := isCurrentMutationDiscarded(tableDesc, m, 0)
		if discarded {
			jobID, err := getJobIDForMutationWithDescriptor(ctx, tableDesc, dependentID)
			if err != nil {
				return nil, err
			}
			dependentJobs = append(dependentJobs, jobID)
		}
	}
	return dependentJobs, nil
}

func (sc *SchemaChanger) shouldSplitAndScatter(
	tableDesc *tabledesc.Mutable, m catalog.Mutation, idx catalog.Index,
) bool {
	if idx == nil {
		return false
	}

	if m.Adding() && idx.IsSharded() && !idx.IsTemporaryIndexForBackfill() {
		if sc.mvccCompliantAddIndex {
			return m.Backfilling()
		}
		return m.DeleteOnly()
	}
	return false

}

func (sc *SchemaChanger) preSplitHashShardedIndexRanges(ctx context.Context) error {
	if err := sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		hour := hlc.Timestamp{WallTime: timeutil.Now().Add(time.Hour).UnixNano()}
		tableDesc, err := descsCol.GetMutableTableByID(
			ctx, txn, sc.descID,
			tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					IncludeOffline: true,
					IncludeDropped: true,
				},
			},
		)
		if err != nil {
			return err
		}

		if fn := sc.testingKnobs.RunBeforeHashShardedIndexRangePreSplit; fn != nil {
			if err := fn(tableDesc, sc.db, sc.execCfg.Codec); err != nil {
				return err
			}
		}

		for _, m := range tableDesc.AllMutations() {
			if m.MutationID() != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}

			if idx := m.AsIndex(); sc.shouldSplitAndScatter(tableDesc, m, idx) {
				// Iterate through all partitioning lists to get all possible list
				// partitioning key prefix. Hash sharded index only allows implicit
				// partitioning, and implicit partitioning does not support
				// subpartition. So it's safe not to consider subpartitions. Range
				// partition is not considered here as well, because it's hard to
				// predict the sampling points within each range to make the pre-split
				// on shard boundaries helpful.
				var partitionKeyPrefixes []roachpb.Key
				partitioning := idx.GetPartitioning()
				if err := partitioning.ForEachList(
					func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
						for _, tupleBytes := range values {
							_, key, err := rowenc.DecodePartitionTuple(
								&tree.DatumAlloc{},
								sc.execCfg.Codec,
								tableDesc,
								idx,
								partitioning,
								tupleBytes,
								tree.Datums{},
							)
							if err != nil {
								return err
							}
							partitionKeyPrefixes = append(partitionKeyPrefixes, key)
						}
						return nil
					},
				); err != nil {
					return err
				}

				splitAtShards := calculateSplitAtShards(maxHashShardedIndexRangePreSplit.Get(&sc.settings.SV), idx.GetSharded().ShardBuckets)
				if len(partitionKeyPrefixes) == 0 {
					// If there is no partitioning on the index, only pre-split on
					// selected shard boundaries.
					for _, shard := range splitAtShards {
						keyPrefix := sc.execCfg.Codec.IndexPrefix(uint32(tableDesc.GetID()), uint32(idx.GetID()))
						splitKey := encoding.EncodeVarintAscending(keyPrefix, shard)
						if _, err := sc.db.SplitAndScatter(ctx, splitKey, hour); err != nil {
							return err
						}
					}
				} else {
					// If there are partitioning prefixes, pre-split each of them.
					for _, partPrefix := range partitionKeyPrefixes {
						for _, shard := range splitAtShards {
							splitKey := encoding.EncodeVarintAscending(partPrefix, shard)
							if _, err := sc.db.SplitAndScatter(ctx, splitKey, hour); err != nil {
								return err
							}
						}
					}
				}
			}
		}

		if fn := sc.testingKnobs.RunAfterHashShardedIndexRangePreSplit; fn != nil {
			if err := fn(tableDesc, sc.db, sc.execCfg.Codec); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

// calculateSplitAtShards returns a slice of min(maxSplit, shardBucketCount)
// shard numbers. Shard numbers are sampled with a fix step within
// [0, shardBucketCount) range.
func calculateSplitAtShards(maxSplit int64, shardBucketCount int32) []int64 {
	splitCount := int(math.Min(float64(maxSplit), float64(shardBucketCount)))
	step := float64(shardBucketCount) / float64(splitCount)
	splitAtShards := make([]int64, splitCount)
	for i := 0; i < splitCount; i++ {
		splitAtShards[i] = int64(math.Floor(float64(i) * step))
	}
	return splitAtShards
}

// isCurrentMutationDiscarded returns if the current column mutation is made irrelevant
// by a later operation. The nextMutationIdx provides the index at which to check for
// later mutation.
func isCurrentMutationDiscarded(
	tableDesc catalog.TableDescriptor, currentMutation catalog.Mutation, nextMutationIdx int,
) (bool, descpb.MutationID) {
	if nextMutationIdx+1 > len(tableDesc.AllMutations()) {
		return false, descpb.InvalidMutationID
	}
	// Drops will never get canceled out, since we need clean up.
	if currentMutation.Dropped() {
		return false, descpb.InvalidMutationID
	}

	colToCheck := make([]descpb.ColumnID, 0, 1)
	// Both NOT NULL related updates and check constraint updates
	// involving this column will get canceled out by a drop column.
	if constraint := currentMutation.AsConstraint(); constraint != nil {
		if constraint.IsNotNull() {
			colToCheck = append(colToCheck, constraint.NotNullColumnID())
		} else if constraint.IsCheck() {
			colToCheck = constraint.Check().ColumnIDs
		}
	}

	for _, m := range tableDesc.AllMutations()[nextMutationIdx:] {
		col := m.AsColumn()
		if col != nil && col.Dropped() && !col.IsRollback() {
			// Column was dropped later on, so this operation
			// should be a no-op.
			for _, id := range colToCheck {
				if col.GetID() == id {
					return true, m.MutationID()
				}
			}
		}
	}
	// Not discarded by any later operation.
	return false, descpb.InvalidMutationID
}
