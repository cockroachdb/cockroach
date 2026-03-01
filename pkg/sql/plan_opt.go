// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/prep"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var queryCacheEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.query_cache.enabled", "enable the query cache", true,
)

// prepareUsingOptimizer builds a memo for a prepared statement and populates
// the following stmt.Prepared fields:
//   - Columns
//   - Types
//   - AnonymizedStr
//   - BaseMemo (for reuse during exec, if appropriate).
func (p *planner) prepareUsingOptimizer(
	ctx context.Context, origin prep.StatementOrigin,
) (planFlags, []*types.T, error) {
	opc := &p.optPlanningCtx

	// If there are externally-injected hints, first try preparing with the
	// injected hints.
	if p.stmt.ASTWithInjectedHints != nil {
		opc.log(ctx, "trying preparing with injected hints")
		p.usingHintInjection = true
		opc.reset(ctx)
		flags, udts, err := p.prepareUsingOptimizerInternal(ctx, origin)
		p.usingHintInjection = false
		if !errorDueToInjectedHint(err) {
			return flags, udts, err
		}
		// Do not return the error. If semantic analysis failed, try preparing again
		// without injected hints.
		log.Eventf(ctx, "preparing with injected hints failed with: %v", err)
		opc.log(ctx, "falling back to preparing without injected hints")
	}
	opc.reset(ctx)
	return p.prepareUsingOptimizerInternal(ctx, origin)
}

func (p *planner) prepareUsingOptimizerInternal(
	ctx context.Context, origin prep.StatementOrigin,
) (planFlags, []*types.T, error) {
	stmt := &p.stmt
	opc := &p.optPlanningCtx

	if origin == prep.StatementOriginSessionMigration {
		opc.flags.Set(planFlagSessionMigration)
	}

	switch t := stmt.AST.(type) {
	case *tree.AlterIndex, *tree.AlterIndexVisible, *tree.AlterTable, *tree.AlterSequence,
		*tree.Analyze,
		*tree.BeginTransaction,
		*tree.CommentOnColumn, *tree.CommentOnConstraint, *tree.CommentOnDatabase, *tree.CommentOnIndex, *tree.CommentOnTable, *tree.CommentOnSchema,
		*tree.CommitPrepared, *tree.CommitTransaction,
		*tree.CopyFrom, *tree.CopyTo, *tree.CreateDatabase, *tree.CreateIndex, *tree.CreateView,
		*tree.CreateSequence,
		*tree.CreateStats,
		*tree.Deallocate, *tree.Discard, *tree.DropDatabase, *tree.DropIndex,
		*tree.DropTable, *tree.DropView, *tree.DropSequence, *tree.DropType,
		*tree.Grant, *tree.GrantRole,
		*tree.Prepare, *tree.PrepareTransaction,
		*tree.ReleaseSavepoint, *tree.RenameColumn, *tree.RenameDatabase,
		*tree.RenameIndex, *tree.RenameTable, *tree.Revoke, *tree.RevokeRole,
		*tree.RollbackPrepared, *tree.RollbackToSavepoint, *tree.RollbackTransaction,
		*tree.Savepoint, *tree.SetTransaction, *tree.SetTracing, *tree.SetSessionAuthorizationDefault,
		*tree.SetSessionCharacteristics:
		// These statements do not have result columns and do not support placeholders
		// so there is no need to do anything during prepare.
		//
		// Some of these statements (like BeginTransaction) aren't supported by the
		// optbuilder so they would error out. Others (like CreateIndex) have planning
		// code that can introduce unnecessary txn retries (because of looking up
		// descriptors and such).
		return opc.flags, nil, nil

	case *tree.Execute:
		// This statement is going to execute a prepared statement. To prepare it,
		// we need to set the expected output columns to the output columns of the
		// prepared statement that the user is trying to execute.
		name := string(t.Name)
		prepared, ok := p.preparedStatements.Get(name)
		if !ok {
			// We're trying to prepare an EXECUTE of a statement that doesn't exist.
			// Let's just give up at this point.
			// Postgres doesn't fail here, instead it produces an EXECUTE that returns
			// no columns. This seems like dubious behavior at best.
			return opc.flags, nil, pgerror.Newf(pgcode.UndefinedPreparedStatement,
				"no such prepared statement %s", name)
		}
		stmt.Prepared.Columns = prepared.Columns
		return opc.flags, prepared.UDTs, nil

	case *tree.ExplainAnalyze:
		// This statement returns result columns but does not support placeholders,
		// and we don't want to do anything during prepare.
		if len(p.semaCtx.Placeholders.Types) != 0 {
			return 0, nil, errors.Errorf("%s does not support placeholders", stmt.AST.StatementTag())
		}
		stmt.Prepared.Columns = colinfo.ExplainPlanColumns
		return opc.flags, nil, nil

	case *tree.ShowCommitTimestamp:
		stmt.Prepared.Columns = colinfo.ShowCommitTimestampColumns
		return opc.flags, nil, nil

	case *tree.DeclareCursor:
		// Build memo for the purposes of typing placeholders.
		// TODO(jordan): converting DeclareCursor to not be an opaque statement
		// would be a better way to accomplish this goal. See CREATE TABLE for an
		// example.
		f := opc.optimizer.Factory()
		bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), opc.catalog, f, t.Select)
		if err := bld.Build(); err != nil {
			return opc.flags, nil, err
		}
	}

	var udts []*types.T
	if opc.useCache {
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, stmt.SQL)
		if ok && cachedData.Metadata != nil {
			pm := cachedData.Metadata
			// Check that the type hints match (the type hints affect type checking).
			if !pm.TypeHints.Identical(p.semaCtx.Placeholders.TypeHints) {
				opc.log(ctx, "query cache hit but type hints don't match")
			} else if pm.HintsGeneration != stmt.Prepared.HintsGeneration && !slices.Equal(pm.HintIDs, stmt.Prepared.HintIDs) {
				opc.log(ctx, "query cache hit but external statement hints don't match")
			} else {
				isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), opc.catalog)
				if err != nil {
					return 0, nil, err
				}
				if !isStale {
					opc.log(ctx, "query cache hit (prepare)")
					opc.flags.Set(planFlagOptCacheHit)
					stmt.Prepared.StatementNoConstants = pm.StatementNoConstants
					stmt.Prepared.Columns = pm.Columns
					stmt.Prepared.Types = pm.Types
					stmt.Hints = pm.Hints
					stmt.HintIDs = pm.HintIDs
					stmt.HintsGeneration = pm.HintsGeneration
					stmt.ASTWithInjectedHints = pm.ASTWithInjectedHints
					if cachedData.Memo.IsOptimized() {
						// A cached, fully optimized memo is an "ideal generic
						// memo".
						stmt.Prepared.GenericMemo = cachedData.Memo
						stmt.Prepared.IdealGenericPlan = true
					} else {
						stmt.Prepared.BaseMemo = cachedData.Memo
					}

					if md := cachedData.Memo.Metadata(); md != nil {
						udts = md.AllUserDefinedTypes()
					}
					return opc.flags, udts, nil
				}
				opc.log(ctx, "query cache hit but memo is stale (prepare)")
			}
		} else if ok {
			opc.log(ctx, "query cache hit but there is no prepare metadata")
		} else {
			opc.log(ctx, "query cache miss (prepare)")
		}
		opc.flags.Set(planFlagOptCacheMiss)
	}

	// Build the memo. Do not attempt to build a non-ideal generic plan at PREPARE-time.
	memo, _, err := opc.buildReusableMemo(ctx, false /* allowNonIdealGeneric */)
	if err != nil {
		return 0, nil, err
	}

	md := memo.Metadata()
	physical := memo.RootProps()
	resultCols := make(colinfo.ResultColumns, len(physical.Presentation))
	for i, col := range physical.Presentation {
		colMeta := md.ColumnMeta(col.ID)
		resultCols[i].Name = col.Alias
		resultCols[i].Typ = colMeta.Type
		// At PREPARE time we don't know yet which format the client will
		// request (this is only known at BIND time), so we optimistically
		// assume that it'll be TEXT (which is the default).
		fmtCode := pgwirebase.FormatText
		if err = checkResultType(resultCols[i].Typ, fmtCode); err != nil {
			return 0, nil, err
		}
		// If the column came from a table, set up the relevant metadata.
		if colMeta.Table != opt.TableID(0) {
			// Get the cat.Table that this column references.
			tab := md.Table(colMeta.Table)
			resultCols[i].TableID = descpb.ID(tab.ID())
			// Convert the metadata opt.ColumnID to its ordinal position in the table.
			colOrdinal := colMeta.Table.ColumnOrdinal(col.ID)
			// Use that ordinal position to retrieve the column's stable ID.
			var column catalog.Column
			if catTable, ok := tab.(optCatalogTableInterface); ok {
				column = catTable.getCol(colOrdinal)
			}
			if column != nil {
				resultCols[i].PGAttributeNum = uint32(column.GetPGAttributeNum())
			} else {
				resultCols[i].PGAttributeNum = uint32(tab.Column(colOrdinal).ColID())
			}
		}
	}

	// Fill blank placeholder types with the type hints.
	p.semaCtx.Placeholders.MaybeExtendTypes()

	// Verify that all placeholder types have been set.
	if err := p.semaCtx.Placeholders.Types.AssertAllSet(); err != nil {
		return 0, nil, err
	}

	if md := memo.Metadata(); md != nil {
		udts = md.AllUserDefinedTypes()
	}
	stmt.Prepared.Columns = resultCols
	stmt.Prepared.Types = p.semaCtx.Placeholders.Types
	if opc.allowMemoReuse {
		if memo.IsOptimized() {
			// A memo fully optimized at prepare time is an "ideal generic
			// memo".
			stmt.Prepared.GenericMemo = memo
			stmt.Prepared.IdealGenericPlan = true
		} else {
			stmt.Prepared.BaseMemo = memo
		}
		if opc.useCache {
			// execPrepare sets the Metadata.InferredTypes field after this
			// point. However, once the Metadata goes into the cache, it
			// can't be modified without causing race conditions. So make a copy of
			// it now.
			// TODO(radu): Determine if the extra object allocation is really
			// necessary.
			pm := stmt.Prepared.Metadata
			cachedData := querycache.CachedData{
				SQL:      stmt.SQL,
				Memo:     memo,
				Metadata: &pm,
			}
			p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		}
	}
	return opc.flags, udts, nil
}

// makeOptimizerPlan generates a plan using the cost-based optimizer.
// On success, it populates p.curPlan.
func (p *planner) makeOptimizerPlan(ctx context.Context) error {
	ctx, sp := tracing.ChildSpan(ctx, "optimizer")
	defer sp.Finish()
	p.curPlan.init(&p.stmt, &p.instrumentation)

	opc := &p.optPlanningCtx

	// If there are externally-injected hints, first try planning with the
	// injected hints.
	if p.stmt.ASTWithInjectedHints != nil {
		opc.log(ctx, "trying planning with injected hints")
		p.usingHintInjection = true
		opc.reset(ctx)
		err := p.makeOptimizerPlanInternal(ctx)
		p.usingHintInjection = false
		if !errorDueToInjectedHint(err) {
			return err
		}
		// Do not return the error. If semantic analysis or optimization failed, try
		// planning again without injected hints.
		log.Eventf(ctx, "planning with injected hints failed with: %v", err)
		opc.log(ctx, "falling back to planning without injected hints")
	}
	opc.reset(ctx)
	return p.makeOptimizerPlanInternal(ctx)
}

// errorDueToInjectedHint returns true if the error could have been caused by an
// injected hint. False positives are allowed, but false negatives are not.
func errorDueToInjectedHint(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	// TODO(michae2): Make this tighter, so that we don't retry if we fail
	// semantic checks for an unrelated reason while trying hint injection.
	return true
}

func (p *planner) makeOptimizerPlanInternal(ctx context.Context) error {
	opc := &p.optPlanningCtx

	execMemo, err := opc.buildExecMemo(ctx)
	if err != nil {
		return err
	}

	// Log and increment telemetry counters for interesting decisions made during
	// optimization.
	optStats := execMemo.GetOptimizationStats()
	if optStats.ClampedHistogramSelectivity {
		log.VEventf(ctx, 2, "clamped histogram selectivity during optimization")
		telemetry.Inc(sqltelemetry.PlanClampedHistogramSelectivityCounter)
	}
	if optStats.ClampedInequalitySelectivity {
		log.VEventf(ctx, 2, "clamped inequality selectivity during optimization")
		telemetry.Inc(sqltelemetry.PlanClampedInequalitySelectivityCounter)
	}

	// Build the plan tree.
	const disableTelemetryAndPlanGists = false
	return p.runExecBuild(ctx, execMemo, disableTelemetryAndPlanGists)
}

// runExecBuild builds the plan tree for the given memo. It assumes that the
// optPlanningCtx of the planner has been properly set up.
func (p *planner) runExecBuild(
	ctx context.Context, execMemo *memo.Memo, disableTelemetryAndPlanGists bool,
) error {
	opc := &p.optPlanningCtx
	if mode := p.SessionData().ExperimentalDistSQLPlanningMode; mode != sessiondatapb.ExperimentalDistSQLPlanningOff {
		planningMode := distSQLDefaultPlanning
		// If this transaction has modified or created any types, it is not safe to
		// distribute due to limitations around leasing descriptors modified in the
		// current transaction.
		if p.Descriptors().HasUncommittedTypes() {
			planningMode = distSQLLocalOnlyPlanning
		}
		err := opc.runExecBuilder(
			ctx,
			&p.curPlan,
			&p.stmt,
			newDistSQLSpecExecFactory(ctx, p, planningMode),
			execMemo,
			p.SemaCtx(),
			p.EvalContext(),
			p.autoCommit,
			disableTelemetryAndPlanGists,
		)
		if err != nil {
			if mode == sessiondatapb.ExperimentalDistSQLPlanningAlways &&
				!strings.Contains(p.stmt.AST.StatementTag(), "SET") {
				// We do not fallback to the old path because experimental
				// planning is set to 'always' and we don't have a SET
				// statement, so we return an error. SET statements are
				// exceptions because we want to be able to execute them
				// regardless of whether they are supported by the new factory.
				// TODO(yuzefovich): update this once SET statements are
				// supported (see #47473).
				return err
			}
			// We will fallback to the old path.
		} else {
			// TODO(yuzefovich): think through whether subqueries or
			// postqueries can be distributed. If that's the case, we might
			// need to also look at the plan distribution of those.
			m := p.curPlan.main
			isPartiallyDistributed := m.physPlan.Distribution == physicalplan.PartiallyDistributedPlan
			if isPartiallyDistributed && p.SessionData().PartiallyDistributedPlansDisabled {
				// The planning has succeeded, but we've created a partially
				// distributed plan yet the session variable prohibits such
				// plan distribution - we need to replan with a new factory
				// that forces local planning.
				// TODO(yuzefovich): remove this logic when deleting old
				// execFactory.
				err = opc.runExecBuilder(
					ctx,
					&p.curPlan,
					&p.stmt,
					newDistSQLSpecExecFactory(ctx, p, distSQLLocalOnlyPlanning),
					execMemo,
					p.SemaCtx(),
					p.EvalContext(),
					p.autoCommit,
					disableTelemetryAndPlanGists,
				)
			}
			if err == nil {
				return nil
			}
		}
		// TODO(yuzefovich): make the logging conditional on the verbosity
		// level once new DistSQL planning is no longer experimental.
		log.Dev.Infof(
			ctx, "distSQLSpecExecFactory failed planning with %v, falling back to the old path", err,
		)
	}
	// If we got here, we did not create a plan above.
	return opc.runExecBuilder(
		ctx,
		&p.curPlan,
		&p.stmt,
		newExecFactory(ctx, p),
		execMemo,
		p.SemaCtx(),
		p.EvalContext(),
		p.autoCommit,
		disableTelemetryAndPlanGists,
	)
}

type optPlanningCtx struct {
	p *planner

	// catalog is initialized once, and reset for each query. This allows the
	// catalog objects to be reused across queries in the same session.
	catalog optPlanningCatalog

	// -- Fields below are reinitialized for each query ---

	optimizer xform.Optimizer

	// When set, we are allowed to reuse a memo, or store a memo for later reuse.
	allowMemoReuse bool

	// When set, we consult and update the query cache. Never set if
	// allowMemoReuse is false.
	useCache bool

	flags planFlags

	gf explain.PlanGistFactory
}

// init performs one-time initialization of the planning context; reset() must
// also be called before each use.
func (opc *optPlanningCtx) init(p *planner) {
	opc.p = p
	opc.catalog = &optCatalog{}
	opc.catalog.init(p)
}

// reset initializes the planning context for the statement in the planner.
func (opc *optPlanningCtx) reset(ctx context.Context) {
	p := opc.p
	opc.catalog.reset()
	opc.optimizer.Init(ctx, p.EvalContext(), opc.catalog)
	opc.flags = 0

	// We only allow memo caching for SELECT/INSERT/UPDATE/DELETE. We could
	// support it for all statements in principle, but it would increase the
	// surface of potential issues (conditions we need to detect to invalidate a
	// cached memo).
	// TODO(mgartner): Enable memo caching for CALL statements.
	switch p.stmt.AST.(type) {
	case *tree.ParenSelect, *tree.Select, *tree.SelectClause, *tree.UnionClause, *tree.ValuesClause,
		*tree.Insert, *tree.Update, *tree.Delete, *tree.CannedOptPlan:
		// If the current transaction has uncommitted DDL statements, we cannot rely
		// on descriptor versions for detecting a "stale" memo. This is because
		// descriptor versions are bumped at most once per transaction, even if there
		// are multiple DDL operations; and transactions can be aborted leading to
		// potential reuse of versions. To avoid these issues, we prevent saving a
		// memo (for prepare) or reusing a saved memo (for execute).
		// We only allow reusing memo if this plan is not going to use canary stats.
		opc.allowMemoReuse = !p.Descriptors().HasUncommittedTables() && !p.EvalContext().UseCanaryStats
		opc.useCache = opc.allowMemoReuse && queryCacheEnabled.Get(&p.execCfg.Settings.SV)

		if _, isCanned := p.stmt.AST.(*tree.CannedOptPlan); isCanned {
			// It's unsafe to use the cache, since PREPARE AS OPT PLAN doesn't track
			// dependencies and check permissions.
			opc.useCache = false
		}

	default:
		opc.allowMemoReuse = false
		opc.useCache = false
	}
}

func (opc *optPlanningCtx) log(ctx context.Context, msg string) {
	if log.VDepth(1, 1) {
		// msg is guaranteed to be a constant string by the fmtsafe linter, so
		// it is safe to convert to a redact.SafeString.
		//
		// Also, note that passing msg directly to log.Dev.InfofDepth() would cause
		// a heap allocation to box it, even if the else path is taken. With the
		// type conversion, a new implicit variable is created that only causes
		// a heap allocation if this branch is taken.
		log.Dev.InfofDepth(ctx, 1, "%s: %s", redact.SafeString(msg), opc.p.stmt)
	} else {
		log.Event(ctx, msg)
	}
}

type memoType int

const (
	memoTypeUnknown memoType = iota
	memoTypeCustom
	memoTypeGeneric
	memoTypeIdealGeneric
)

// buildReusableMemo builds the statement into a memo that can be stored for
// prepared statements and can later be used as a starting point for
// optimization. The returned memo is fully optimized if:
//
//  1. The statement does not contain placeholders nor fold-able stable
//     operators.
//  2. Or, the placeholder fast path is used.
//  3. Or, allowNonIdealGeneric is true and the plan is fully optimized as best
//     as possible in the presence of placeholders.
//
// The returned memo is fully detached from the planner and can be used with
// reuseMemo independently and concurrently by multiple threads.
func (opc *optPlanningCtx) buildReusableMemo(
	ctx context.Context, allowNonIdealGeneric bool,
) (*memo.Memo, memoType, error) {
	p := opc.p

	_, isCanned := opc.p.stmt.AST.(*tree.CannedOptPlan)
	if isCanned {
		if !p.EvalContext().SessionData().AllowPrepareAsOptPlan {
			return nil, memoTypeUnknown, pgerror.New(pgcode.InsufficientPrivilege,
				"PREPARE AS OPT PLAN is a testing facility that should not be used directly",
			)
		}

		if !p.SessionData().User().IsRootUser() {
			return nil, memoTypeUnknown, pgerror.New(pgcode.InsufficientPrivilege,
				"PREPARE AS OPT PLAN may only be used by root",
			)
		}
	}

	if p.SessionData().SaveTablesPrefix != "" && !p.SessionData().User().IsRootUser() {
		return nil, memoTypeUnknown, pgerror.New(pgcode.InsufficientPrivilege,
			"sub-expression tables creation may only be used by root",
		)
	}

	// Build the Memo (optbuild) and apply normalization rules to it. If the
	// query contains placeholders, values are not assigned during this phase,
	// as that only happens during the EXECUTE phase. If the query does not
	// contain placeholders, then also apply exploration rules to the Memo so
	// that there's even less to do during the EXECUTE phase.
	//
	f := opc.optimizer.Factory()
	ast := opc.p.stmt.AST
	if opc.p.usingHintInjection {
		ast = opc.p.stmt.ASTWithInjectedHints
	}
	f.Metadata().SetHintIDs(opc.p.GetHintIDs())
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), opc.catalog, f, ast)
	bld.KeepPlaceholders = true
	if opc.flags.IsSet(planFlagSessionMigration) {
		bld.SkipAOST = true
	}
	if err := bld.Build(); err != nil {
		return nil, memoTypeUnknown, err
	}

	if bld.DisableMemoReuse {
		// The builder encountered a statement that prevents safe reuse of the memo.
		opc.allowMemoReuse = false
		opc.useCache = false
	}

	if isCanned {
		if f.Memo().HasPlaceholders() {
			// We don't support placeholders inside the canned plan. The main reason
			// is that they would be invisible to the parser (which reports the number
			// of placeholders, used to initialize the relevant structures).
			return nil, memoTypeUnknown, pgerror.Newf(pgcode.Syntax,
				"placeholders are not supported with PREPARE AS OPT PLAN")
		}
		// With a canned plan, we don't want to optimize the memo. Since we
		// won't optimize it, we consider it an ideal generic plan.
		return opc.optimizer.DetachMemo(ctx), memoTypeIdealGeneric, nil
	}

	// If the memo doesn't have placeholders and did not encounter any stable
	// operators that can be constant-folded, then fully optimize it now - it
	// can be reused without further changes to build the execution tree.
	if !f.Memo().HasPlaceholders() && !f.FoldingControl().PreventedStableFold() {
		opc.log(ctx, "optimizing (no placeholders)")
		if _, err := opc.optimizer.Optimize(); err != nil {
			return nil, memoTypeUnknown, err
		}
		opc.flags.Set(planFlagOptimized)
		return opc.optimizer.DetachMemo(ctx), memoTypeIdealGeneric, nil
	}

	// If the memo has placeholders, first try the placeholder fast path.
	ok, err := opc.optimizer.TryPlaceholderFastPath()
	if err != nil {
		return nil, memoTypeUnknown, err
	}
	if ok {
		opc.log(ctx, "placeholder fast path")
		opc.flags.Set(planFlagOptimized)
		return opc.optimizer.DetachMemo(ctx), memoTypeIdealGeneric, nil
	} else if allowNonIdealGeneric {
		// Build a generic query plan if the placeholder fast path failed and a
		// generic plan was requested.
		opc.log(ctx, "optimizing (generic)")
		if _, err := opc.optimizer.Optimize(); err != nil {
			return nil, memoTypeUnknown, err
		}
		opc.flags.Set(planFlagOptimized)
		return opc.optimizer.DetachMemo(ctx), memoTypeGeneric, nil
	}

	// Detach the prepared memo from the factory and transfer its ownership
	// to the prepared statement. DetachMemo will re-initialize the optimizer
	// to an empty memo.
	return opc.optimizer.DetachMemo(ctx), memoTypeCustom, nil
}

// reuseMemo returns an optimized memo using a cached memo as a starting point.
//
// The cached memo is not modified; it is safe to call reuseMemo on the same
// cachedMemo from multiple threads concurrently.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.
func (opc *optPlanningCtx) reuseMemo(cachedMemo *memo.Memo) (*memo.Memo, error) {
	opc.incPlanTypeTelemetry(cachedMemo)
	if cachedMemo.IsOptimized() {
		// The query could have been already fully optimized in
		// buildReusableMemo, in which case it is considered a "generic" plan.
		opc.flags.Set(planFlagGeneric)
		return cachedMemo, nil
	}
	f := opc.optimizer.Factory()
	// Finish optimization by assigning any remaining placeholders and
	// applying exploration rules. Reinitialize the optimizer and construct a
	// new memo that is copied from the prepared memo, but with placeholders
	// assigned. Stable operators can be constant-folded at this time.
	f.FoldingControl().AllowStableFolds()
	if err := f.AssignPlaceholders(cachedMemo); err != nil {
		return nil, err
	}
	if _, err := opc.optimizer.Optimize(); err != nil {
		return nil, err
	}
	opc.flags.Set(planFlagOptimized)
	mem := f.Memo()
	if prep := opc.p.stmt.Prepared; opc.allowMemoReuse && prep != nil {
		costWithOptimizationCost := mem.RootExpr().Cost()
		costWithOptimizationCost.Add(mem.OptimizationCost())
		prep.Costs.AddCustom(costWithOptimizationCost)
	}
	return mem, nil
}

// incPlanTypeTelemetry increments the telemetry counters for the type of the
// plan: generic or custom.
func (opc *optPlanningCtx) incPlanTypeTelemetry(cachedMemo *memo.Memo) {
	switch opc.p.SessionData().PlanCacheMode {
	case sessiondatapb.PlanCacheModeForceCustom:
		telemetry.Inc(sqltelemetry.PlanTypeForceCustomCounter)
	case sessiondatapb.PlanCacheModeForceGeneric:
		telemetry.Inc(sqltelemetry.PlanTypeForceGenericCounter)
	case sessiondatapb.PlanCacheModeAuto:
		if cachedMemo.IsOptimized() {
			// A fully optimized memo is generic.
			telemetry.Inc(sqltelemetry.PlanTypeAutoGenericCounter)
		} else {
			telemetry.Inc(sqltelemetry.PlanTypeAutoCustomCounter)
		}
	}
}

// buildNonIdealGenericPlan returns true if we should attempt to build a
// non-ideal generic query plan.
func (opc *optPlanningCtx) buildNonIdealGenericPlan() bool {
	ps := opc.p.stmt.Prepared
	switch opc.p.SessionData().PlanCacheMode {
	case sessiondatapb.PlanCacheModeForceGeneric:
		return true
	case sessiondatapb.PlanCacheModeAuto:
		// We need to build CustomPlanThreshold custom plans before considering
		// a generic plan.
		return ps.Costs.NumCustom() >= prep.CustomPlanThreshold
	default:
		return false
	}
}

// chooseGenericPlan returns true if a generic query plan should be chosen. An
// ideal generic query plan is always chosen, if it exists. A non-ideal generic
// plan is chosen if CustomPlanThreshold custom plans have already been built
// and the generic plan is optimal or it has not yet been built.
func (opc *optPlanningCtx) chooseGenericPlan(ctx context.Context) bool {
	ps := opc.p.stmt.Prepared
	// Always use an ideal generic plan.
	if ps.IdealGenericPlan {
		opc.log(ctx, "ideal generic plan")
		return true
	}
	switch opc.p.SessionData().PlanCacheMode {
	case sessiondatapb.PlanCacheModeForceGeneric:
		return true
	case sessiondatapb.PlanCacheModeAuto:
		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Eventf(ctx, "%s", ps.Costs.Summary())
		}
		return ps.Costs.NumCustom() >= prep.CustomPlanThreshold &&
			(!ps.Costs.HasGeneric() || ps.Costs.IsGenericOptimal())
	default:
		return false
	}
}

// chooseValidPreparedMemo returns a pre-built memo. It may be an unoptimized
// base memo, a fully optimized generic memo, or nil. It returns nil if either
// memo is stale, or the memo it decides to use (base or generic) does not yet
// exist. It selects the base memo or generic memo based on the following rules,
// in order, assuming both are non-stale:
//
//  1. If the generic memo is ideal, it is returned as-is.
//  2. If plan_cache_mode=force_generic_plan is true then genericMemo is
//     returned as-is if it is not stale.
//  3. If plan_cache_mode=auto, there have been at least 5 custom plans
//     generated, and the cost of the generic memo is less than the average cost
//     of the custom plans, then the generic memo is returned as-is if it is not
//     stale. If the cost of the generic memo is greater than or equal to the
//     average cost of the custom plans, then the baseMemo is returned if it is
//     not stale.
//  4. If plan_cache_mode=force_custom_plan, baseMemo is returned if it is not
//     stale.
//  5. Otherwise, nil is returned and the caller is responsible for building a
//     new memo.
func (opc *optPlanningCtx) chooseValidPreparedMemo(ctx context.Context) (*memo.Memo, error) {
	prep := opc.p.stmt.Prepared

	if prep.GenericMemo != nil {
		isStale, err := prep.GenericMemo.IsStale(ctx, opc.p.EvalContext(), opc.catalog)
		if err != nil {
			return nil, err
		} else if isStale {
			// Clear the generic and custom costs if the memo is stale. DDL or
			// new stats could drastically change the cost of generic and custom
			// plans, so we should re-consider which to use.
			prep.GenericMemo = nil
			prep.BaseMemo = nil
			prep.Costs.Reset()
			return nil, nil
		}
	}

	if prep.BaseMemo != nil {
		isStale, err := prep.BaseMemo.IsStale(ctx, opc.p.EvalContext(), opc.catalog)
		if err != nil {
			return nil, err
		} else if isStale {
			// Clear the generic and custom costs if the memo is stale. DDL or
			// new stats could drastically change the cost of generic and custom
			// plans, so we should re-consider which to use.
			prep.GenericMemo = nil
			prep.BaseMemo = nil
			prep.Costs.Reset()
			return nil, nil
		}
	}

	// NOTE: The generic or base memos returned below could be nil if they have
	// not yet been built.
	if opc.chooseGenericPlan(ctx) {
		return prep.GenericMemo, nil
	}
	return prep.BaseMemo, nil
}

// fetchPreparedMemo attempts to fetch a memo from the prepared statement
// struct. If a valid (i.e., non-stale) memo is found, it is used. Otherwise, a
// new statement will be built.
//
// The plan_cache_mode session setting controls how this function decides
// between what type of memo to use or reuse:
//
//   - force_custom_plan: A fully optimized generic memo will be used if it
//     either has no placeholders nor fold-able stable expressions, or it
//     utilizes the placeholder fast-path. Otherwise, a normalized memo will be
//     fetched or rebuilt, copied into a new memo with placeholders replaced
//     with values, and re-optimized.
//
//   - force_generic_plan: A fully optimized generic memo will always be used.
//     The BaseMemo will be used if it is fully optimized. Otherwise, the
//     GenericMemo will be used.
//
//   - auto: A "custom plan" will be optimized for first five executions of the
//     prepared statement. On the sixth execution, a "generic plan" will be
//     generated. If its cost is less than the average cost of the custom plans
//     (plus some optimization overhead cost), then the generic plan will be
//     used. Otherwise, a custom plan will be used.
func (opc *optPlanningCtx) fetchPreparedMemo(ctx context.Context) (_ *memo.Memo, err error) {
	p := opc.p
	prep := p.stmt.Prepared
	if !opc.allowMemoReuse || prep == nil {
		return nil, nil
	}

	// If the statement was previously prepared, check for a reusable memo.
	// First check for a valid (non-stale) memo.
	validMemo, err := opc.chooseValidPreparedMemo(ctx)
	if err != nil {
		return nil, err
	}
	if validMemo != nil {
		opc.log(ctx, "reusing cached memo")
		return opc.reuseMemo(validMemo)
	}

	// Otherwise, we need to rebuild the memo.
	//
	// TODO(mgartner): If we have a non-stale, normalized base memo, we can
	// build a generic memo from it instead of building the memo from
	// scratch.
	opc.log(ctx, "rebuilding cached memo")
	buildGeneric := opc.buildNonIdealGenericPlan()
	newMemo, typ, err := opc.buildReusableMemo(ctx, buildGeneric)
	if err != nil {
		return nil, err
	}
	if opc.allowMemoReuse {
		switch typ {
		case memoTypeIdealGeneric:
			// An "ideal" generic memo will always be used regardless of
			// plan_cache_mode, so there is no need to set GenericCost.
			prep.GenericMemo = newMemo
			prep.IdealGenericPlan = true
		case memoTypeGeneric:
			prep.GenericMemo = newMemo
			prep.Costs.SetGeneric(newMemo.RootExpr().Cost())
			// Now that the cost of the generic plan is known, we need to
			// re-evaluate the decision to use a generic or custom plan.
			if !opc.chooseGenericPlan(ctx) {
				// The generic plan that we just built is too expensive, so we need
				// to build a custom plan. We recursively call fetchPreparedMemo in
				// case we have a custom plan that can be reused as a starting point
				// for optimization. The function should not recurse more than once.
				return opc.fetchPreparedMemo(ctx)
			}
		case memoTypeCustom:
			prep.BaseMemo = newMemo
		default:
			return nil, errors.AssertionFailedf("unexpected memo type %v", typ)
		}
	}

	// Re-optimize the memo, if necessary.
	return opc.reuseMemo(newMemo)
}

// buildExecMemo creates a fully optimized memo, possibly reusing a previously
// cached memo as a starting point.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.
func (opc *optPlanningCtx) buildExecMemo(ctx context.Context) (_ *memo.Memo, _ error) {
	if resumeProc := opc.p.storedProcTxnState.getResumeProc(); resumeProc != nil {
		// We are executing a stored procedure which has paused to commit or
		// rollback its transaction. Use resumeProc to resume execution in a new
		// transaction where the control statement left off.
		opc.log(ctx, "resuming stored procedure execution in a new transaction")
		return opc.reuseMemo(resumeProc)
	}

	// Fetch and reuse a memo if a valid one is available.
	m, err := opc.fetchPreparedMemo(ctx)
	if err != nil {
		return nil, err
	}
	if m != nil {
		return m, nil
	}

	p := opc.p
	if opc.useCache {
		// Consult the query cache.
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, opc.p.stmt.SQL)
		if ok {
			if isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), opc.catalog); err != nil {
				return nil, err
			} else if isStale {
				opc.log(ctx, "query cache hit but needed update")
				cachedData.Memo, _, err = opc.buildReusableMemo(ctx, false /* allowNonIdealGeneric */)
				if err != nil {
					return nil, err
				}
				// Update the plan in the cache. If the cache entry had Metadata
				// populated, it may no longer be valid.
				cachedData.Metadata = nil
				p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
				opc.flags.Set(planFlagOptCacheMiss)
			} else {
				opc.log(ctx, "query cache hit")
				opc.flags.Set(planFlagOptCacheHit)
			}
			return opc.reuseMemo(cachedData.Memo)
		}
		opc.flags.Set(planFlagOptCacheMiss)
		opc.log(ctx, "query cache miss")
	} else {
		opc.log(ctx, "not using query cache")
	}

	// We are executing a statement for which there is no reusable memo
	// available.
	f := opc.optimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	ast := opc.p.stmt.AST
	if opc.p.usingHintInjection {
		ast = opc.p.stmt.ASTWithInjectedHints
	}
	f.Metadata().SetHintIDs(opc.p.GetHintIDs())
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), opc.catalog, f, ast)
	if err := bld.Build(); err != nil {
		return nil, err
	}

	// For index recommendations, after building we must interrupt the flow to
	// find potential index candidates in the memo.
	explainModeShowsRec := func(m tree.ExplainMode) bool {
		// Only the PLAN (the default), DISTSQL, and GIST explain modes show
		// index recommendations.
		return m == tree.ExplainPlan || m == tree.ExplainDistSQL || m == tree.ExplainGist
	}
	e, isExplain := opc.p.stmt.AST.(*tree.Explain)
	if isExplain && explainModeShowsRec(e.Mode) && p.SessionData().IndexRecommendationsEnabled {
		indexRecs, err := opc.makeQueryIndexRecommendation(ctx)
		if err != nil {
			return nil, err
		}
		opc.p.instrumentation.explainIndexRecs = indexRecs
	}

	if _, isCanned := opc.p.stmt.AST.(*tree.CannedOptPlan); !isCanned {
		if _, err := opc.optimizer.Optimize(); err != nil {
			return nil, err
		}
	}

	// If this statement doesn't have placeholders and we have not constant-folded
	// any VolatilityStable operators, add it to the cache.
	// Note that non-prepared statements from pgwire clients cannot have
	// placeholders.
	if opc.useCache && !bld.HadPlaceholders && !bld.DisableMemoReuse &&
		!f.FoldingControl().PermittedStableFold() {
		opc.log(ctx, "query cache add")
		memo := opc.optimizer.DetachMemo(ctx)
		cachedData := querycache.CachedData{
			SQL:  opc.p.stmt.SQL,
			Memo: memo,
		}
		p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		return memo, nil
	}

	return f.ReleaseMemo(), nil
}

// runExecBuilder execbuilds a plan using the given factory and stores the
// result in planTop. If required, also captures explain data using the explain
// factory.
func (opc *optPlanningCtx) runExecBuilder(
	ctx context.Context,
	planTop *planTop,
	stmt *Statement,
	f exec.Factory,
	mem *memo.Memo,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	allowAutoCommit bool,
	disableTelemetryAndPlanGists bool,
) error {
	var result *planComponents
	if !opc.p.SessionData().DisablePlanGists && !disableTelemetryAndPlanGists {
		opc.gf.Init(f)
		defer opc.gf.Reset()
		f = &opc.gf
	}
	var bld *execbuilder.Builder
	if !planTop.instrumentation.ShouldBuildExplainPlan() {
		bld = execbuilder.New(
			ctx, f, &opc.optimizer, mem, opc.catalog, mem.RootExpr(),
			semaCtx, evalCtx, allowAutoCommit, statements.IsANSIDML(stmt.AST),
		)
		if disableTelemetryAndPlanGists {
			bld.DisableTelemetry()
		}
		plan, err := bld.Build()
		if err != nil {
			return err
		}
		result = plan.(*planComponents)
	} else {
		// Create an explain factory and record the explain.Plan.
		explainFactory := explain.NewFactory(f, semaCtx, evalCtx)
		bld = execbuilder.New(
			ctx, explainFactory, &opc.optimizer, mem, opc.catalog, mem.RootExpr(),
			semaCtx, evalCtx, allowAutoCommit, statements.IsANSIDML(stmt.AST),
		)
		if disableTelemetryAndPlanGists {
			bld.DisableTelemetry()
		}
		plan, err := bld.Build()
		if err != nil {
			return err
		}
		explainPlan := plan.(*explain.Plan)
		result = explainPlan.WrappedPlan.(*planComponents)
		planTop.instrumentation.RecordExplainPlan(explainPlan)
	}
	planTop.instrumentation.maxFullScanRows = bld.MaxFullScanRows
	planTop.instrumentation.totalScanRows = bld.TotalScanRows
	planTop.instrumentation.totalScanRowsWithoutForecasts = bld.TotalScanRowsWithoutForecasts
	planTop.instrumentation.nanosSinceStatsCollected = bld.NanosSinceStatsCollected
	planTop.instrumentation.nanosSinceStatsForecasted = bld.NanosSinceStatsForecasted
	planTop.instrumentation.joinTypeCounts = bld.JoinTypeCounts
	planTop.instrumentation.joinAlgorithmCounts = bld.JoinAlgorithmCounts
	planTop.instrumentation.scanCounts = bld.ScanCounts
	planTop.instrumentation.indexesUsed = bld.IndexesUsed

	if opc.gf.Initialized() {
		planTop.instrumentation.planGist = opc.gf.PlanGist()
	}
	planTop.instrumentation.costEstimate = mem.RootExpr().Cost().C
	available := mem.RootExpr().Relational().Statistics().Available
	planTop.instrumentation.statsAvailable = available
	if available {
		planTop.instrumentation.outputRows = mem.RootExpr().Relational().Statistics().RowCount
	}

	// This check is also performed during Bind for the extended protocol
	// (see connExecutor.execBind). It remains here for the simple protocol,
	// which does not go through Bind.
	if stmt.ExpectedTypes != nil {
		cols := result.main.planColumns()
		if !stmt.ExpectedTypes.TypesEqual(cols) {
			return pgerror.New(pgcode.FeatureNotSupported, "cached plan must not change result type")
		}
	}

	planTop.planComponents = *result
	planTop.stmt = stmt
	planTop.flags |= opc.flags
	if planTop.flags.IsSet(planFlagIsDDL) {
		// The declarative schema changer mode would have already been set here,
		// since all declarative schema changes are built opaquely. However, some
		// DDLs (e.g. CREATE TABLE) are built non-opaquely, so we need to set the
		// mode here if it wasn't already set.
		if planTop.instrumentation.schemaChangerMode == schemaChangerModeNone {
			if !disableTelemetryAndPlanGists {
				telemetry.Inc(sqltelemetry.LegacySchemaChangerCounter)
			}
			planTop.instrumentation.schemaChangerMode = schemaChangerModeLegacy
		}
	}
	planTop.mem = mem
	planTop.catalog = opc.catalog
	return nil
}

// DecodeGist Avoid an import cycle by keeping the cat out of the tree. If
// external is true gist is from a foreign database and we use nil catalog.
func (p *planner) DecodeGist(ctx context.Context, gist string, external bool) ([]string, error) {
	var cat cat.Catalog
	if !external {
		cat = p.optPlanningCtx.catalog
	}
	return explain.DecodePlanGistToRows(ctx, p.EvalContext(), gist, cat)
}

// makeQueryIndexRecommendation builds a statement and walks through it to find
// potential index candidates. It then optimizes the statement with those
// indexes hypothetically added to the table. An index recommendation for the
// query is outputted based on which hypothetical indexes are helpful in the
// optimal plan.
func (opc *optPlanningCtx) makeQueryIndexRecommendation(
	ctx context.Context,
) (_ []indexrec.Rec, retErr error) {
	origCtx := ctx
	ctx, sp := tracing.EnsureChildSpan(ctx, opc.p.execCfg.AmbientCtx.Tracer, "index recommendation")
	defer sp.Finish()
	defer errorutil.MaybeCatchPanic(&retErr, func(caughtErr error) {
		log.VEventf(ctx, 1, "%v", caughtErr)
	})

	// Save the normalized memo created by the optbuilder.
	savedMemo := opc.optimizer.DetachMemo(ctx)

	// Use the optimizer to fully optimize the memo. We need to do this before
	// finding index candidates because the *memo.SortExpr from the sort enforcer
	// is only added to the memo in this step. The sort expression is required to
	// determine certain index candidates.
	f := opc.optimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	f.CopyAndReplace(
		savedMemo,
		savedMemo.RootExpr(),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)
	opc.optimizer.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		return ruleName.IsNormalize()
	})
	if _, err := opc.optimizer.Optimize(); err != nil {
		return nil, err
	}

	// Walk through the fully normalized memo to determine index candidates and
	// create hypothetical tables.
	indexCandidates := indexrec.FindIndexCandidateSet(f.Memo().RootExpr(), f.Metadata())
	optTables, hypTables := indexrec.BuildOptAndHypTableMaps(opc.catalog, indexCandidates)

	// Optimize with the saved memo and hypothetical tables. Walk through the
	// optimal plan to determine index recommendations.
	opc.optimizer.Init(ctx, f.EvalContext(), opc.catalog)
	f.CopyAndReplace(
		savedMemo,
		savedMemo.RootExpr(),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)
	opc.optimizer.Memo().Metadata().UpdateTableMeta(ctx, f.EvalContext(), hypTables)
	if _, err := opc.optimizer.Optimize(); err != nil {
		return nil, err
	}

	indexRecs, err := indexrec.FindRecs(ctx, f.Memo().RootExpr(), f.Metadata())
	if err != nil {
		return nil, err
	}

	// Re-initialize the optimizer (which also re-initializes the factory) and
	// update the saved memo's metadata with the original table information.
	// Prepare to re-optimize and create an executable plan.
	// Use the origCtx instead of ctx since the optimizer will hold onto this
	// context after this function ends, and we don't want "use of Span after
	// Finish" errors.
	opc.optimizer.Init(origCtx, f.EvalContext(), opc.catalog)
	savedMemo.Metadata().UpdateTableMeta(origCtx, f.EvalContext(), optTables)
	f.CopyAndReplace(
		savedMemo,
		savedMemo.RootExpr(),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)

	return indexRecs, nil
}

// Optimizer returns the Optimizer associated with this planning context.
func (opc *optPlanningCtx) Optimizer() interface{} {
	return &opc.optimizer
}
