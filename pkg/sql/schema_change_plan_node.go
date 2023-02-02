// Copyright 2021 The Cockroach Authors.
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
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

// FormatAstAsRedactableString implements scbuild.AstFormatter
func (p *planner) FormatAstAsRedactableString(
	statement tree.Statement, annotations *tree.Annotations,
) redact.RedactableString {
	return formatStmtKeyAsRedactableString(p.getVirtualTabler(),
		statement,
		annotations, tree.FmtSimple)
}

// SchemaChange provides the planNode for the new schema changer.
func (p *planner) SchemaChange(ctx context.Context, stmt tree.Statement) (planNode, error) {

	// TODO(ajwerner): Call featureflag.CheckEnabled appropriately.
	mode := p.extendedEvalCtx.SchemaChangerState.mode
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
	state, err := scbuild.Build(ctx, deps, scs.state, stmt)
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
	return &schemaChangePlanNode{
		stmt:         stmt,
		sql:          p.stmt.SQL,
		lastState:    scs.state,
		plannedState: state,
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
	)
}

// waitForDescriptorIDGeneratorMigration polls the system.descriptor table (in
// separate transactions) until the descriptor_id_seq record is present, which
// indicates that the system tenant's descriptor ID generator has successfully
// been migrated.
func (p *planner) waitForDescriptorIDGeneratorMigration(ctx context.Context) error {
	// Drop all leases and locks due to the current transaction, and, in the
	// process, abort the transaction.
	p.Descriptors().ReleaseAll(ctx)
	if err := p.txn.Rollback(ctx); err != nil {
		return err
	}

	// Wait for the system.descriptor_id_gen descriptor to appear.
	start := timeutil.Now()
	logEvery := log.Every(30 * time.Second)
	blocked := true
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); blocked && r.Next(); {
		if knobs := p.ExecCfg().TenantTestingKnobs; knobs != nil {
			if fn := knobs.BeforeCheckingForDescriptorIDSequence; fn != nil {
				fn(ctx)
			}
		}
		now := p.ExecCfg().Clock.Now()
		if logEvery.ShouldLog() {
			log.Infof(
				ctx,
				"waiting for system tenant descriptor ID generator migration, waited %v so far",
				timeutil.Since(start),
			)
		}
		if err := p.ExecCfg().InternalDB.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			kvTxn := txn.KV()
			if err := kvTxn.SetFixedTimestamp(ctx, now); err != nil {
				return err
			}
			k := catalogkeys.MakeDescMetadataKey(p.ExecCfg().Codec, keys.DescIDSequenceID)
			result, err := txn.KV().Get(ctx, k)
			if err != nil {
				return err
			}
			if result.Exists() {
				blocked = false
			}
			return nil
		}); err != nil {
			return err
		}
	}
	log.Infof(
		ctx,
		"done waiting for system tenant descriptor ID generator migration after %v",
		timeutil.Since(start),
	)
	return nil
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

	if knobs := p.ExecCfg().DeclarativeSchemaChangerTestingKnobs; knobs != nil &&
		knobs.BeforeWaitingForConcurrentSchemaChanges != nil {
		knobs.BeforeWaitingForConcurrentSchemaChanges(scs.stmts)
	}

	// Drop all leases and locks due to the current transaction, and, in the
	// process, abort the transaction.
	p.Descriptors().ReleaseAll(ctx)
	if err := p.txn.Rollback(ctx); err != nil {
		return err
	}

	// Wait for the descriptor to no longer be claimed by a schema change.
	start := timeutil.Now()
	logEvery := log.Every(30 * time.Second)
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		now := p.ExecCfg().Clock.Now()
		if logEvery.ShouldLog() {
			log.Infof(ctx,
				"schema change waiting for concurrent schema changes on descriptor %d,"+
					" waited %v so far", descID, timeutil.Since(start),
			)
		}
		blocked := false
		if err := p.ExecCfg().InternalDB.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			if err := txn.KV().SetFixedTimestamp(ctx, now); err != nil {
				return err
			}
			desc, err := txn.Descriptors().ByID(txn.KV()).WithoutNonPublic().Get().Desc(ctx, descID)
			if err != nil {
				return err
			}
			blocked = desc.HasConcurrentSchemaChanges()
			return nil
		}); err != nil {
			return err
		}
		if !blocked {
			break
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
	sql  string
	stmt tree.Statement
	// lastState was the state observed so far while planning for the current
	// transaction, for all the statements under it.
	lastState scpb.CurrentState
	// plannedState contains the state produced by the builder combining
	// the nodes that existed preceding the current statement with the output of
	// the built current statement. There maybe cases like CTE's, where we will
	// need to re-plan if the lastState that the plannedState do not match, since
	// we are executing DDL statements in an unexpected way.
	plannedState scpb.CurrentState
}

func (s *schemaChangePlanNode) startExec(params runParams) error {
	p := params.p
	scs := p.ExtendedEvalContext().SchemaChangerState

	// Our current state does not match what was previously planned for, which means
	// that potentially we are running CTE's with ALTER statements. So, we are going
	// to re-plan the state to include the current statement since the statement
	// phase was not executed.
	if !reflect.DeepEqual(s.lastState.Current, scs.state.Current) {
		deps := p.newSchemaChangeBuilderDependencies(scs.stmts)
		state, err := scbuild.Build(params.ctx, deps, scs.state, s.stmt)
		if err != nil {
			return err
		}
		// Update with the re-planned state.
		s.plannedState = state
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
