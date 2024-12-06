// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SchemaChange provides the planNode for the new schema changer.
func (p *planner) SchemaChange(ctx context.Context, stmt tree.Statement) (planNode, error) {
	err := checkSchemaChangeEnabled(ctx, p.ExecCfg(), p.stmt.AST.StatementTag())
	if err != nil {
		return nil, err
	}
	mode := p.extendedEvalCtx.SchemaChangerState.mode
	// Lease the system database to see if schema changes are blocked on reader
	// catalogs.
	systemDB, err := p.Descriptors().ByIDWithLeased(p.txn).Get().Database(ctx, keys.SystemDatabaseID)
	if err != nil {
		return nil, err
	}
	if systemDB.GetReplicatedPCRVersion() != 0 {
		return nil, pgerror.Newf(pgcode.ReadOnlySQLTransaction, "schema changes are not allowed on a reader catalog")
	}
	// When new schema changer is on we will not support it for explicit
	// transaction, since we don't know if subsequent statements don't
	// support it.
	if mode == sessiondatapb.UseNewSchemaChangerOff ||
		((mode == sessiondatapb.UseNewSchemaChangerOn ||
			mode == sessiondatapb.UseNewSchemaChangerUnsafe) && !p.extendedEvalCtx.TxnIsSingleStmt) {
		return nil, nil
	}

	scs := p.extendedEvalCtx.SchemaChangerState
	scs.stmts = append(scs.stmts, p.stmt.SQL)
	deps := p.newSchemaChangeBuilderDependencies(scs.stmts)
	state, logSchemaChangesFn, err := scbuild.Build(ctx, deps, scs.state, stmt, &scs.memAcc)
	if scerrors.HasNotImplemented(err) &&
		mode != sessiondatapb.UseNewSchemaChangerUnsafeAlways {
		return nil, nil
	}
	if err != nil {
		// If we need to wait for a concurrent schema change to finish, release our
		// leases, and then return the error to wait and retry.
		if scerrors.ConcurrentSchemaChangeDescID(err) != descpb.InvalidID {
			p.Descriptors().ReleaseLeases(ctx)
		}
		return nil, err
	}

	// If we successfully planned a schema change here, then update telemetry
	// to indicate that we used the new schema changer.
	telemetry.Inc(sqltelemetry.DeclarativeSchemaChangerCounter)
	p.curPlan.instrumentation.schemaChangerMode = schemaChangerModeDeclarative

	return &schemaChangePlanNode{
		stmt:               stmt,
		sql:                p.stmt.SQL,
		lastState:          scs.state,
		plannedState:       state,
		logSchemaChangesFn: logSchemaChangesFn,
	}, nil
}

func (p *planner) newSchemaChangeBuilderDependencies(statements []string) scbuild.Dependencies {
	return scdeps.NewBuilderDependencies(
		p.ExecCfg().NodeInfo.LogicalClusterID(),
		p.ExecCfg().Codec,
		p.InternalSQLTxn(),
		NewSkippingCacheSchemaResolver, /* schemaResolverFactory */
		p,                              /* authAccessor */
		p,                              /* astFormatter */
		p,                              /* featureChecker */
		p.SessionData(),
		p.ExecCfg().Settings,
		statements,
		p,
		NewSchemaChangerBuildEventLogger(p.InternalSQLTxn(), p.ExecCfg()),
		NewReferenceProviderFactory(p),
		p.EvalContext().DescIDGenerator,
		p, /* temporarySchemaProvider */
		p, /* nodesStatusInfo */
		p, /* regionProvider */
		p.SemaCtx(),
		p.EvalContext(),
		p.execCfg.DefaultZoneConfig,
	)
}

// waitForDescriptorSchemaChanges polls the specified descriptor (in separate
// transactions) until all its ongoing schema changes have completed.
// Internally, this call will restart the planner's underlying transaction and
// clean up any locks it might currently be holding. If it did not, deadlocks
// involving the current transaction might occur. The caller is expected to
// make any attempt at retrying a timestamp after the call returns.
func (p *planner) waitForDescriptorSchemaChanges(
	ctx context.Context, descID descpb.ID, scs SchemaChangerState,
) error {

	knobs := p.ExecCfg().DeclarativeSchemaChangerTestingKnobs
	if knobs != nil && knobs.BeforeWaitingForConcurrentSchemaChanges != nil {
		err := knobs.BeforeWaitingForConcurrentSchemaChanges(scs.stmts)
		if err != nil {
			return err
		}
	}

	// Drop all leases and locks due to the current transaction, and, in the
	// process, abort the transaction.
	p.Descriptors().ReleaseAll(ctx)
	if err := p.txn.Rollback(ctx); err != nil {
		return err
	}

	// Wait for the descriptor to no longer be claimed by a schema change.
	start := timeutil.Now()
	logEvery := log.Every(10 * time.Second)
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		now := p.ExecCfg().Clock.Now()
		var isBlocked bool
		var blockingJobIDs []catpb.JobID
		if err := p.ExecCfg().InternalDB.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			if err := txn.KV().SetFixedTimestamp(ctx, now); err != nil {
				return err
			}
			desc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Desc(ctx, descID)
			if err != nil {
				return err
			}
			isBlocked = desc.HasConcurrentSchemaChanges()
			blockingJobIDs = desc.ConcurrentSchemaChangeJobIDs()
			return nil
		}); err != nil {
			log.Infof(ctx, "done schema change wait on concurrent jobs due"+
				" to error on descriptor (%d): %s", descID, err)
			return err
		}
		if !isBlocked {
			break
		}
		if logEvery.ShouldLog() {
			log.Infof(ctx,
				"schema change waiting for %v concurrent schema change job(s) %v on descriptor %d,"+
					" waited %v so far", len(blockingJobIDs), blockingJobIDs, descID, timeutil.Since(start),
			)
		}
		if knobs != nil && knobs.WhileWaitingForConcurrentSchemaChanges != nil {
			knobs.WhileWaitingForConcurrentSchemaChanges(scs.stmts)
		}
	}

	log.Infof(
		ctx,
		"done waiting for concurrent schema changes on descriptor %d after %v",
		descID, timeutil.Since(start),
	)
	return nil
}

// schemaChangePlanNode is the planNode utilized by the new schema changer to
// perform all schema changes, unified in the new schema changer.
type schemaChangePlanNode struct {
	zeroInputPlanNode
	sql  string
	stmt tree.Statement
	// lastState was the state observed so far while planning for the current
	// transaction, for all the statements under it.
	lastState scpb.CurrentState
	// plannedState contains the state produced by the builder combining
	// the nodes that existed preceding the current statement with the output of
	// the built current statement. There maybe cases like CTE's, where we will
	// need to re-plan if the lastState and the plannedState do not match, since
	// we are executing DDL statements in an unexpected way.
	plannedState scpb.CurrentState
	// logSchemaChangesFn is used to log schema change events before execution.
	logSchemaChangesFn scbuild.LogSchemaChangerEventsFn
}

func (s *schemaChangePlanNode) startExec(params runParams) error {
	p := params.p
	scs := p.ExtendedEvalContext().SchemaChangerState
	// Current schema change state (as tracked in `scs.state` in the planner)
	// does not match that when we previously planned and created `s` (as tracked
	// in `s.lastState` and was previously set to `scs.state` in the planner).
	// This is possible with CTEs with schema changes (e.g. builtin `AddGeometryColumn`
	// can be in used in both a CTE and the main query), in which case we need to
	// re-build the plan with an updated incumbent state.
	if !reflect.DeepEqual(s.lastState.Current, scs.state.Current) {
		deps := p.newSchemaChangeBuilderDependencies(scs.stmts)
		state, logSchemaChangesFn, err := scbuild.Build(params.ctx, deps, scs.state, s.stmt, &scs.memAcc)
		if err != nil {
			return err
		}
		// Update with the re-planned state.
		scs.memAcc.Shrink(params.ctx, s.plannedState.ByteSize())
		s.plannedState = state
		s.logSchemaChangesFn = logSchemaChangesFn
	}

	// First log events from the statement we just built.
	if s.logSchemaChangesFn != nil {
		err := s.logSchemaChangesFn(params.ctx)
		if err != nil {
			return err
		}
	}
	// Disable KV tracing for statement phase execution.
	// Operation side effects are in-memory only.
	const kvTrace = false
	runDeps := newSchemaChangerTxnRunDependencies(
		params.ctx,
		p.SessionData(),
		p.User(),
		p.ExecCfg(),
		p.InternalSQLTxn(),
		p.Descriptors(),
		p.EvalContext(),
		kvTrace,
		scs.jobID,
		scs.stmts,
	)
	after, jobID, err := scrun.RunStatementPhase(
		params.ctx, p.ExecCfg().DeclarativeSchemaChangerTestingKnobs, runDeps, s.plannedState,
	)
	if err != nil {
		return err
	}
	scs.state = after
	scs.jobID = jobID
	return nil
}

func newSchemaChangerTxnRunDependencies(
	ctx context.Context,
	sessionData *sessiondata.SessionData,
	user username.SQLUsername,
	execCfg *ExecutorConfig,
	txn isql.Txn,
	descriptors *descs.Collection,
	evalContext *eval.Context,
	kvTrace bool,
	schemaChangerJobID jobspb.JobID,
	stmts []string,
) scexec.Dependencies {
	metaDataUpdater := descmetadata.NewMetadataUpdater(
		ctx,
		txn,
		descriptors,
		&execCfg.Settings.SV,
		sessionData,
	)
	return scdeps.NewExecutorDependencies(
		execCfg.Settings,
		execCfg.Codec,
		sessionData,
		txn,
		user,
		descriptors,
		execCfg.JobRegistry,
		execCfg.IndexBackfiller,
		execCfg.IndexSpanSplitter,
		execCfg.IndexMerger,
		// Use a no-op tracker and flusher because while backfilling in a
		// transaction because we know there's no existing progress and there's
		// nothing to save because nobody will ever try to resume.
		scdeps.NewNoOpBackfillerTracker(execCfg.Codec),
		scdeps.NewNoopPeriodicProgressFlusher(),
		execCfg.Validator,
		scdeps.NewConstantClock(evalContext.GetTxnTimestamp(time.Microsecond).Time),
		metaDataUpdater,
		evalContext.Planner,
		execCfg.StatsRefresher,
		execCfg.DeclarativeSchemaChangerTestingKnobs,
		kvTrace,
		schemaChangerJobID,
		stmts,
	)
}

func (s schemaChangePlanNode) Next(params runParams) (bool, error) { return false, nil }
func (s schemaChangePlanNode) Values() tree.Datums                 { return tree.Datums{} }
func (s schemaChangePlanNode) Close(ctx context.Context)           {}

var _ (planNode) = (*schemaChangePlanNode)(nil)
