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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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
func (p *planner) SchemaChange(ctx context.Context, stmt tree.Statement) (planNode, bool, error) {

	// TODO(ajwerner): Call featureflag.CheckEnabled appropriately.
	mode := p.extendedEvalCtx.SchemaChangerState.mode
	// When new schema changer is on we will not support it for explicit
	// transaction, since we don't know if subsequent statements don't
	// support it.
	if mode == sessiondatapb.UseNewSchemaChangerOff ||
		((mode == sessiondatapb.UseNewSchemaChangerOn ||
			mode == sessiondatapb.UseNewSchemaChangerUnsafe) && !p.extendedEvalCtx.TxnImplicit) {
		return nil, false, nil
	}
	scs := p.extendedEvalCtx.SchemaChangerState
	scs.stmts = append(scs.stmts, p.stmt.SQL)
	deps := scdeps.NewBuilderDependencies(
		p.ExecCfg().ClusterID(),
		p.ExecCfg().Codec,
		p.Txn(),
		p.Descriptors(),
		p,
		p,
		p,
		p,
		p.SessionData(),
		p.ExecCfg().Settings,
		scs.stmts,
	)
	state, err := scbuild.Build(ctx, deps, scs.state, stmt)
	if scerrors.HasNotImplemented(err) &&
		mode != sessiondatapb.UseNewSchemaChangerUnsafeAlways {
		return nil, false, nil
	}
	if err != nil {
		// If we need to wait for a concurrent schema change to finish, release our
		// leases, and then return the error to wait and retry.
		if scerrors.ConcurrentSchemaChangeDescID(err) != descpb.InvalidID {
			p.Descriptors().ReleaseLeases(ctx)
		}
		return nil, false, err
	}
	return &schemaChangePlanNode{plannedState: state}, true, nil
}

// WaitForDescriptorSchemaChanges polls the specified descriptor (in separate
// transactions) until all its ongoing schema changes have completed.
func (p *planner) WaitForDescriptorSchemaChanges(
	ctx context.Context, descID descpb.ID, scs SchemaChangerState,
) error {

	if knobs := p.ExecCfg().DeclarativeSchemaChangerTestingKnobs; knobs != nil &&
		knobs.BeforeWaitingForConcurrentSchemaChanges != nil {
		knobs.BeforeWaitingForConcurrentSchemaChanges(scs.stmts)
	}

	everySecond := log.Every(time.Second)
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		now := p.ExecCfg().Clock.Now()
		if everySecond.ShouldLog() {
			log.Infof(ctx, "schema change waiting for concurrent schema changes on descriptor %d", descID)
		}
		blocked := false
		if err := p.ExecCfg().CollectionFactory.Txn(
			ctx, p.ExecCfg().InternalExecutor, p.ExecCfg().DB,
			func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
				if err := txn.SetFixedTimestamp(ctx, now); err != nil {
					return err
				}
				table, err := descriptors.GetImmutableTableByID(ctx, txn, descID,
					tree.ObjectLookupFlags{
						CommonLookupFlags: tree.CommonLookupFlags{
							Required:    true,
							AvoidLeased: true,
						},
					})
				if err != nil {
					return err
				}
				blocked = catalog.HasConcurrentSchemaChanges(table)
				return nil
			}); err != nil {
			return err
		}
		if !blocked {
			break
		}
	}
	log.Infof(ctx, "done waiting for concurrent schema changes on descriptor %d", descID)
	return nil
}

// schemaChangePlanNode is the planNode utilized by the new schema changer to
// perform all schema changes, unified in the new schema changer.
type schemaChangePlanNode struct {
	// plannedState contains the state produced by the builder combining
	// the nodes that existed preceding the current statement with the output of
	// the built current statement.
	plannedState scpb.CurrentState
}

func (s *schemaChangePlanNode) startExec(params runParams) error {
	p := params.p
	scs := p.ExtendedEvalContext().SchemaChangerState
	runDeps := newSchemaChangerTxnRunDependencies(
		p.SessionData(),
		p.User(),
		p.ExecCfg(),
		p.Txn(),
		p.Descriptors(),
		p.EvalContext(),
		p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
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
	sessionData *sessiondata.SessionData,
	user security.SQLUsername,
	execCfg *ExecutorConfig,
	txn *kv.Txn,
	descriptors *descs.Collection,
	evalContext *tree.EvalContext,
	kvTrace bool,
	schemaChangerJobID jobspb.JobID,
	stmts []string,
) scexec.Dependencies {
	return scdeps.NewExecutorDependencies(
		execCfg.Codec,
		sessionData,
		txn,
		user,
		descriptors,
		execCfg.JobRegistry,
		execCfg.IndexBackfiller,
		// Use a no-op tracker and flusher because while backfilling in a
		// transaction because we know there's no existing progress and there's
		// nothing to save because nobody will ever try to resume.
		scdeps.NewNoOpBackfillTracker(execCfg.Codec),
		scdeps.NewNoopPeriodicProgressFlusher(),
		execCfg.IndexValidator,
		scdeps.NewConstantClock(evalContext.GetTxnTimestamp(time.Microsecond).Time),
		execCfg.DescMetadaUpdaterFactory,
		NewSchemaChangerEventLogger(txn, execCfg, 1),
		kvTrace,
		schemaChangerJobID,
		stmts,
	)
}

func (s schemaChangePlanNode) Next(params runParams) (bool, error) { return false, nil }
func (s schemaChangePlanNode) Values() tree.Datums                 { return tree.Datums{} }
func (s schemaChangePlanNode) Close(ctx context.Context)           {}

var _ (planNode) = (*schemaChangePlanNode)(nil)
