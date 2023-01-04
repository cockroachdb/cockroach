// Copyright 2018 The Cockroach Authors.
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
	"strings"

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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var queryCacheEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.query_cache.enabled", "enable the query cache", true,
)

// prepareUsingOptimizer builds a memo for a prepared statement and populates
// the following stmt.Prepared fields:
//   - Columns
//   - Types
//   - AnonymizedStr
//   - Memo (for reuse during exec, if appropriate).
func (p *planner) prepareUsingOptimizer(ctx context.Context) (planFlags, error) {
	stmt := &p.stmt

	opc := &p.optPlanningCtx
	opc.reset(ctx)

	switch t := stmt.AST.(type) {
	case *tree.AlterIndex, *tree.AlterIndexVisible, *tree.AlterTable, *tree.AlterSequence,
		*tree.Analyze,
		*tree.BeginTransaction,
		*tree.CommentOnColumn, *tree.CommentOnConstraint, *tree.CommentOnDatabase, *tree.CommentOnIndex, *tree.CommentOnTable, *tree.CommentOnSchema,
		*tree.CommitTransaction,
		*tree.CopyFrom, *tree.CreateDatabase, *tree.CreateIndex, *tree.CreateView,
		*tree.CreateSequence,
		*tree.CreateStats,
		*tree.Deallocate, *tree.Discard, *tree.DropDatabase, *tree.DropIndex,
		*tree.DropTable, *tree.DropView, *tree.DropSequence, *tree.DropType,
		*tree.Grant, *tree.GrantRole,
		*tree.Prepare,
		*tree.ReleaseSavepoint, *tree.RenameColumn, *tree.RenameDatabase,
		*tree.RenameIndex, *tree.RenameTable, *tree.Revoke, *tree.RevokeRole,
		*tree.RollbackToSavepoint, *tree.RollbackTransaction,
		*tree.Savepoint, *tree.SetTransaction, *tree.SetTracing, *tree.SetSessionAuthorizationDefault,
		*tree.SetSessionCharacteristics:
		// These statements do not have result columns and do not support placeholders
		// so there is no need to do anything during prepare.
		//
		// Some of these statements (like BeginTransaction) aren't supported by the
		// optbuilder so they would error out. Others (like CreateIndex) have planning
		// code that can introduce unnecessary txn retries (because of looking up
		// descriptors and such).
		return opc.flags, nil

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
			return opc.flags, pgerror.Newf(pgcode.UndefinedPreparedStatement,
				"no such prepared statement %s", name)
		}
		stmt.Prepared.Columns = prepared.Columns
		return opc.flags, nil

	case *tree.ExplainAnalyze:
		// This statement returns result columns but does not support placeholders,
		// and we don't want to do anything during prepare.
		if len(p.semaCtx.Placeholders.Types) != 0 {
			return 0, errors.Errorf("%s does not support placeholders", stmt.AST.StatementTag())
		}
		stmt.Prepared.Columns = colinfo.ExplainPlanColumns
		return opc.flags, nil

	case *tree.ShowCommitTimestamp:
		stmt.Prepared.Columns = colinfo.ShowCommitTimestampColumns
		return opc.flags, nil

	case *tree.DeclareCursor:
		// Build memo for the purposes of typing placeholders.
		// TODO(jordan): converting DeclareCursor to not be an opaque statement
		// would be a better way to accomplish this goal. See CREATE TABLE for an
		// example.
		f := opc.optimizer.Factory()
		bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), opc.catalog, f, t.Select)
		if err := bld.Build(); err != nil {
			return opc.flags, err
		}
	}

	if opc.useCache {
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, stmt.SQL)
		if ok && cachedData.PrepareMetadata != nil {
			pm := cachedData.PrepareMetadata
			// Check that the type hints match (the type hints affect type checking).
			if !pm.TypeHints.Identical(p.semaCtx.Placeholders.TypeHints) {
				opc.log(ctx, "query cache hit but type hints don't match")
			} else {
				isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), opc.catalog)
				if err != nil {
					return 0, err
				}
				if !isStale {
					opc.log(ctx, "query cache hit (prepare)")
					opc.flags.Set(planFlagOptCacheHit)
					stmt.Prepared.StatementNoConstants = pm.StatementNoConstants
					stmt.Prepared.Columns = pm.Columns
					stmt.Prepared.Types = pm.Types
					stmt.Prepared.Memo = cachedData.Memo
					return opc.flags, nil
				}
				opc.log(ctx, "query cache hit but memo is stale (prepare)")
			}
		} else if ok {
			opc.log(ctx, "query cache hit but there is no prepare metadata")
		} else {
			opc.log(ctx, "query cache miss")
		}
		opc.flags.Set(planFlagOptCacheMiss)
	}

	memo, err := opc.buildReusableMemo(ctx)
	if err != nil {
		return 0, err
	}

	md := memo.Metadata()
	physical := memo.RootProps()
	resultCols := make(colinfo.ResultColumns, len(physical.Presentation))
	for i, col := range physical.Presentation {
		colMeta := md.ColumnMeta(col.ID)
		resultCols[i].Name = col.Alias
		resultCols[i].Typ = colMeta.Type
		if err := checkResultType(resultCols[i].Typ); err != nil {
			return 0, err
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
		return 0, err
	}

	stmt.Prepared.Columns = resultCols
	stmt.Prepared.Types = p.semaCtx.Placeholders.Types
	if opc.allowMemoReuse {
		stmt.Prepared.Memo = memo
		if opc.useCache {
			// execPrepare sets the PrepareMetadata.InferredTypes field after this
			// point. However, once the PrepareMetadata goes into the cache, it
			// can't be modified without causing race conditions. So make a copy of
			// it now.
			// TODO(radu): Determine if the extra object allocation is really
			// necessary.
			pm := stmt.Prepared.PrepareMetadata
			cachedData := querycache.CachedData{
				SQL:             stmt.SQL,
				Memo:            memo,
				PrepareMetadata: &pm,
			}
			p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		}
	}
	return opc.flags, nil
}

// makeOptimizerPlan generates a plan using the cost-based optimizer.
// On success, it populates p.curPlan.
func (p *planner) makeOptimizerPlan(ctx context.Context) error {
	ctx, sp := tracing.ChildSpan(ctx, "optimizer")
	defer sp.Finish()
	p.curPlan.init(&p.stmt, &p.instrumentation)

	opc := &p.optPlanningCtx
	opc.reset(ctx)

	execMemo, err := opc.buildExecMemo(ctx)
	if err != nil {
		return err
	}

	// Build the plan tree.
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
			p.EvalContext(),
			p.autoCommit,
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
					p.EvalContext(),
					p.autoCommit,
				)
			}
			if err == nil {
				return nil
			}
		}
		// TODO(yuzefovich): make the logging conditional on the verbosity
		// level once new DistSQL planning is no longer experimental.
		log.Infof(
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
		p.EvalContext(),
		p.autoCommit,
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
	switch p.stmt.AST.(type) {
	case *tree.ParenSelect, *tree.Select, *tree.SelectClause, *tree.UnionClause, *tree.ValuesClause,
		*tree.Insert, *tree.Update, *tree.Delete, *tree.CannedOptPlan:
		// If the current transaction has uncommitted DDL statements, we cannot rely
		// on descriptor versions for detecting a "stale" memo. This is because
		// descriptor versions are bumped at most once per transaction, even if there
		// are multiple DDL operations; and transactions can be aborted leading to
		// potential reuse of versions. To avoid these issues, we prevent saving a
		// memo (for prepare) or reusing a saved memo (for execute).
		opc.allowMemoReuse = !p.Descriptors().HasUncommittedTables()
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

func (opc *optPlanningCtx) log(ctx context.Context, msg redact.SafeString) {
	if log.VDepth(1, 1) {
		log.InfofDepth(ctx, 1, "%s: %s", msg, opc.p.stmt)
	} else {
		log.Eventf(ctx, "%s", string(msg))
	}
}

// buildReusableMemo builds the statement into a memo that can be stored for
// prepared statements and can later be used as a starting point for
// optimization. The returned memo is fully detached from the planner and can be
// used with reuseMemo independently and concurrently by multiple threads.
func (opc *optPlanningCtx) buildReusableMemo(ctx context.Context) (_ *memo.Memo, _ error) {
	p := opc.p

	_, isCanned := opc.p.stmt.AST.(*tree.CannedOptPlan)
	if isCanned {
		if !p.EvalContext().SessionData().AllowPrepareAsOptPlan {
			return nil, pgerror.New(pgcode.InsufficientPrivilege,
				"PREPARE AS OPT PLAN is a testing facility that should not be used directly",
			)
		}

		if !p.SessionData().User().IsRootUser() {
			return nil, pgerror.New(pgcode.InsufficientPrivilege,
				"PREPARE AS OPT PLAN may only be used by root",
			)
		}
	}

	if p.SessionData().SaveTablesPrefix != "" && !p.SessionData().User().IsRootUser() {
		return nil, pgerror.New(pgcode.InsufficientPrivilege,
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
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), opc.catalog, f, opc.p.stmt.AST)
	bld.KeepPlaceholders = true
	if err := bld.Build(); err != nil {
		return nil, err
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
			return nil, pgerror.Newf(pgcode.Syntax,
				"placeholders are not supported with PREPARE AS OPT PLAN")
		}
		// With a canned plan, we don't want to optimize the memo.
		return opc.optimizer.DetachMemo(ctx), nil
	}

	if f.Memo().HasPlaceholders() {
		// Try the placeholder fast path.
		_, ok, err := opc.optimizer.TryPlaceholderFastPath()
		if err != nil {
			return nil, err
		}
		if ok {
			opc.log(ctx, "placeholder fast path")
		}
	} else {
		// If the memo doesn't have placeholders and did not encounter any stable
		// operators that can be constant folded, then fully optimize it now - it
		// can be reused without further changes to build the execution tree.
		if !f.FoldingControl().PreventedStableFold() {
			opc.log(ctx, "optimizing (no placeholders)")
			if _, err := opc.optimizer.Optimize(); err != nil {
				return nil, err
			}
		}
	}

	// Detach the prepared memo from the factory and transfer its ownership
	// to the prepared statement. DetachMemo will re-initialize the optimizer
	// to an empty memo.
	return opc.optimizer.DetachMemo(ctx), nil
}

// reuseMemo returns an optimized memo using a cached memo as a starting point.
//
// The cached memo is not modified; it is safe to call reuseMemo on the same
// cachedMemo from multiple threads concurrently.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.
func (opc *optPlanningCtx) reuseMemo(
	ctx context.Context, cachedMemo *memo.Memo,
) (*memo.Memo, error) {
	if cachedMemo.IsOptimized() {
		// The query could have been already fully optimized if there were no
		// placeholders or the placeholder fast path succeeded (see
		// buildReusableMemo).
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
	return f.Memo(), nil
}

// buildExecMemo creates a fully optimized memo, possibly reusing a previously
// cached memo as a starting point.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.
func (opc *optPlanningCtx) buildExecMemo(ctx context.Context) (_ *memo.Memo, _ error) {
	prepared := opc.p.stmt.Prepared
	p := opc.p
	if opc.allowMemoReuse && prepared != nil && prepared.Memo != nil {
		// We are executing a previously prepared statement and a reusable memo is
		// available.

		// If the prepared memo has been invalidated by schema or other changes,
		// re-prepare it.
		if isStale, err := prepared.Memo.IsStale(ctx, p.EvalContext(), opc.catalog); err != nil {
			return nil, err
		} else if isStale {
			opc.log(ctx, "rebuilding cached memo")
			prepared.Memo, err = opc.buildReusableMemo(ctx)
			if err != nil {
				return nil, err
			}
		}
		opc.log(ctx, "reusing cached memo")
		memo, err := opc.reuseMemo(ctx, prepared.Memo)
		return memo, err
	}

	if opc.useCache {
		// Consult the query cache.
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, opc.p.stmt.SQL)
		if ok {
			if isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), opc.catalog); err != nil {
				return nil, err
			} else if isStale {
				opc.log(ctx, "query cache hit but needed update")
				cachedData.Memo, err = opc.buildReusableMemo(ctx)
				if err != nil {
					return nil, err
				}
				// Update the plan in the cache. If the cache entry had PrepareMetadata
				// populated, it may no longer be valid.
				cachedData.PrepareMetadata = nil
				p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
				opc.flags.Set(planFlagOptCacheMiss)
			} else {
				opc.log(ctx, "query cache hit")
				opc.flags.Set(planFlagOptCacheHit)
			}
			memo, err := opc.reuseMemo(ctx, cachedData.Memo)
			return memo, err
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
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), opc.catalog, f, opc.p.stmt.AST)
	if err := bld.Build(); err != nil {
		return nil, err
	}

	// For index recommendations, after building we must interrupt the flow to
	// find potential index candidates in the memo.
	_, isExplain := opc.p.stmt.AST.(*tree.Explain)
	if isExplain && p.SessionData().IndexRecommendationsEnabled {
		if err := opc.makeQueryIndexRecommendation(ctx); err != nil {
			return nil, err
		}
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
	evalCtx *eval.Context,
	allowAutoCommit bool,
) error {
	var result *planComponents
	var gf *explain.PlanGistFactory
	if !opc.p.SessionData().DisablePlanGists {
		gf = explain.NewPlanGistFactory(f)
		f = gf
	}
	var bld *execbuilder.Builder
	if !planTop.instrumentation.ShouldBuildExplainPlan() {
		bld = execbuilder.New(ctx, f, &opc.optimizer, mem, opc.catalog, mem.RootExpr(),
			evalCtx, allowAutoCommit, stmt.IsANSIDML())
		plan, err := bld.Build()
		if err != nil {
			return err
		}
		result = plan.(*planComponents)
	} else {
		// Create an explain factory and record the explain.Plan.
		explainFactory := explain.NewFactory(f)
		bld = execbuilder.New(ctx, explainFactory, &opc.optimizer, mem, opc.catalog, mem.RootExpr(),
			evalCtx, allowAutoCommit, stmt.IsANSIDML())
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

	if gf != nil {
		planTop.instrumentation.planGist = gf.PlanGist()
	}
	planTop.instrumentation.costEstimate = float64(mem.RootExpr().(memo.RelExpr).Cost())
	available := mem.RootExpr().(memo.RelExpr).Relational().Statistics().Available
	planTop.instrumentation.statsAvailable = available
	if available {
		planTop.instrumentation.outputRows = mem.RootExpr().(memo.RelExpr).Relational().Statistics().RowCount
	}

	if stmt.ExpectedTypes != nil {
		cols := result.main.planColumns()
		if !stmt.ExpectedTypes.TypesEqual(cols) {
			return pgerror.New(pgcode.FeatureNotSupported, "cached plan must not change result type")
		}
	}

	planTop.planComponents = *result
	planTop.stmt = stmt
	planTop.flags = opc.flags
	if bld.IsDDL {
		planTop.flags.Set(planFlagIsDDL)
	}
	if bld.ContainsFullTableScan {
		planTop.flags.Set(planFlagContainsFullTableScan)
	}
	if bld.ContainsFullIndexScan {
		planTop.flags.Set(planFlagContainsFullIndexScan)
	}
	if bld.ContainsLargeFullTableScan {
		planTop.flags.Set(planFlagContainsLargeFullTableScan)
	}
	if bld.ContainsLargeFullIndexScan {
		planTop.flags.Set(planFlagContainsLargeFullIndexScan)
	}
	if bld.ContainsMutation {
		planTop.flags.Set(planFlagContainsMutation)
	}
	if bld.ContainsNonDefaultKeyLocking {
		planTop.flags.Set(planFlagContainsNonDefaultLocking)
	}
	if planTop.instrumentation.ShouldSaveMemo() {
		planTop.mem = mem
		planTop.catalog = opc.catalog
	}
	return nil
}

// DecodeGist Avoid an import cycle by keeping the cat out of the tree. If
// external is true gist is from a foreign database and we use nil catalog.
func (p *planner) DecodeGist(gist string, external bool) ([]string, error) {
	var cat cat.Catalog
	if !external {
		cat = p.optPlanningCtx.catalog
	}
	return explain.DecodePlanGistToRows(gist, cat)
}

// makeQueryIndexRecommendation builds a statement and walks through it to find
// potential index candidates. It then optimizes the statement with those
// indexes hypothetically added to the table. An index recommendation for the
// query is outputted based on which hypothetical indexes are helpful in the
// optimal plan.
func (opc *optPlanningCtx) makeQueryIndexRecommendation(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate internal errors without having to add
			// error checks everywhere throughout the code. This is only possible
			// because the code does not update shared state and does not manipulate
			// locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
				log.VEventf(ctx, 1, "%v", err)
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
	}()

	// Save the normalized memo created by the optbuilder.
	savedMemo := opc.optimizer.DetachMemo(ctx)

	// Use the optimizer to fully normalize the memo. We need to do this before
	// finding index candidates because the *memo.SortExpr from the sort enforcer
	// is only added to the memo in this step. The sort expression is required to
	// determine certain index candidates.
	f := opc.optimizer.Factory()
	f.FoldingControl().AllowStableFolds()
	f.CopyAndReplace(
		savedMemo.RootExpr().(memo.RelExpr),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)
	opc.optimizer.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		return ruleName.IsNormalize()
	})
	if _, err = opc.optimizer.Optimize(); err != nil {
		return err
	}

	// Walk through the fully normalized memo to determine index candidates and
	// create hypothetical tables.
	indexCandidates := indexrec.FindIndexCandidateSet(f.Memo().RootExpr(), f.Metadata())
	optTables, hypTables := indexrec.BuildOptAndHypTableMaps(indexCandidates)

	// Optimize with the saved memo and hypothetical tables. Walk through the
	// optimal plan to determine index recommendations.
	opc.optimizer.Init(ctx, f.EvalContext(), opc.catalog)
	f.CopyAndReplace(
		savedMemo.RootExpr().(memo.RelExpr),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)
	opc.optimizer.Memo().Metadata().UpdateTableMeta(f.EvalContext(), hypTables)
	if _, err = opc.optimizer.Optimize(); err != nil {
		return err
	}

	opc.p.instrumentation.indexRecs = indexrec.FindRecs(f.Memo().RootExpr(), f.Metadata())

	// Re-initialize the optimizer (which also re-initializes the factory) and
	// update the saved memo's metadata with the original table information.
	// Prepare to re-optimize and create an executable plan.
	opc.optimizer.Init(ctx, f.EvalContext(), opc.catalog)
	savedMemo.Metadata().UpdateTableMeta(f.EvalContext(), optTables)
	f.CopyAndReplace(
		savedMemo.RootExpr().(memo.RelExpr),
		savedMemo.RootProps(),
		f.CopyWithoutAssigningPlaceholders,
	)

	return nil
}
