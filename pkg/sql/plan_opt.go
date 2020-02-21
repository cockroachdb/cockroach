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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var queryCacheEnabled = settings.RegisterBoolSetting(
	"sql.query_cache.enabled", "enable the query cache", true,
)

// prepareUsingOptimizer builds a memo for a prepared statement and populates
// the following stmt.Prepared fields:
//  - Columns
//  - Types
//  - AnonymizedStr
//  - Memo (for reuse during exec, if appropriate).
func (p *planner) prepareUsingOptimizer(ctx context.Context) (planFlags, error) {
	stmt := p.stmt

	opc := &p.optPlanningCtx
	opc.reset()

	// These statements do not have result columns and do not support placeholders
	// so there is no need to do anything during prepare.
	//
	// Some of these statements (like BeginTransaction) aren't supported by the
	// optbuilder so they would error out. Others (like CreateIndex) have planning
	// code that can introduce unnecessary txn retries (because of looking up
	// descriptors and such).
	switch stmt.AST.(type) {
	case *tree.AlterIndex, *tree.AlterTable, *tree.AlterSequence,
		*tree.BeginTransaction,
		*tree.CommentOnColumn, *tree.CommentOnDatabase, *tree.CommentOnIndex, *tree.CommentOnTable,
		*tree.CommitTransaction,
		*tree.CopyFrom, *tree.CreateDatabase, *tree.CreateIndex, *tree.CreateView,
		*tree.CreateSequence,
		*tree.CreateStats,
		*tree.Deallocate, *tree.Discard, *tree.DropDatabase, *tree.DropIndex,
		*tree.DropTable, *tree.DropView, *tree.DropSequence, *tree.DropRole,
		*tree.Execute,
		*tree.Grant, *tree.GrantRole,
		*tree.Prepare,
		*tree.ReleaseSavepoint, *tree.RenameColumn, *tree.RenameDatabase,
		*tree.RenameIndex, *tree.RenameTable, *tree.Revoke, *tree.RevokeRole,
		*tree.RollbackToSavepoint, *tree.RollbackTransaction,
		*tree.Savepoint, *tree.SetTransaction, *tree.SetTracing, *tree.SetSessionAuthorizationDefault,
		*tree.SetSessionCharacteristics:
		return opc.flags, nil
	}

	if opc.useCache {
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, stmt.SQL)
		if ok && cachedData.PrepareMetadata != nil {
			pm := cachedData.PrepareMetadata
			// Check that the type hints match (the type hints affect type checking).
			if !pm.TypeHints.Equals(p.semaCtx.Placeholders.TypeHints) {
				opc.log(ctx, "query cache hit but type hints don't match")
			} else {
				isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog)
				if err != nil {
					return 0, err
				}
				if !isStale {
					opc.log(ctx, "query cache hit (prepare)")
					opc.flags.Set(planFlagOptCacheHit)
					stmt.Prepared.AnonymizedStr = pm.AnonymizedStr
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

	stmt.Prepared.AnonymizedStr = anonymizeStmt(stmt.AST)

	memo, err := opc.buildReusableMemo(ctx)
	if err != nil {
		return 0, err
	}

	md := memo.Metadata()
	physical := memo.RootProps()
	resultCols := make(sqlbase.ResultColumns, len(physical.Presentation))
	for i, col := range physical.Presentation {
		resultCols[i].Name = col.Alias
		resultCols[i].Typ = md.ColumnMeta(col.ID).Type
		if err := checkResultType(resultCols[i].Typ); err != nil {
			return 0, err
		}
	}

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
	stmt := p.stmt

	opc := &p.optPlanningCtx
	opc.reset()

	execMemo, err := opc.buildExecMemo(ctx)
	if err != nil {
		return err
	}

	// Build the plan tree.
	root := execMemo.RootExpr()
	execFactory := makeExecFactory(p)
	plan, err := execbuilder.New(&execFactory, execMemo, &opc.catalog, root, p.EvalContext()).Build()
	if err != nil {
		return err
	}

	result := plan.(*planTop)
	result.stmt = stmt
	result.flags = opc.flags

	cols := planColumns(result.plan)
	if stmt.ExpectedTypes != nil {
		if !stmt.ExpectedTypes.TypesEqual(cols) {
			return pgerror.New(pgcode.FeatureNotSupported, "cached plan must not change result type")
		}
	}

	p.curPlan = *result

	return nil
}

type optPlanningCtx struct {
	p *planner

	// catalog is initialized once, and reset for each query. This allows the
	// catalog objects to be reused across queries in the same session.
	catalog optCatalog

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
	opc.catalog.init(p)
}

// reset initializes the planning context for the statement in the planner.
func (opc *optPlanningCtx) reset() {
	p := opc.p
	opc.catalog.reset()
	opc.optimizer.Init(p.EvalContext(), &opc.catalog)
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
		opc.allowMemoReuse = !p.Tables().hasUncommittedTables()
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
		log.InfofDepth(ctx, 1, "%s: %s", msg, opc.p.stmt)
	} else {
		log.Event(ctx, msg)
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
		if !p.EvalContext().SessionData.AllowPrepareAsOptPlan {
			return nil, pgerror.New(pgcode.InsufficientPrivilege,
				"PREPARE AS OPT PLAN is a testing facility that should not be used directly",
			)
		}

		if p.SessionData().User != security.RootUser {
			return nil, pgerror.New(pgcode.InsufficientPrivilege,
				"PREPARE AS OPT PLAN may only be used by root",
			)
		}
	}

	if p.SessionData().SaveTablesPrefix != "" && p.SessionData().User != security.RootUser {
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
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)
	bld.KeepPlaceholders = true
	if err := bld.Build(); err != nil {
		return nil, err
	}

	if bld.DisableMemoReuse {
		opc.allowMemoReuse = false
		opc.useCache = false
	}

	if isCanned {
		if f.Memo().HasPlaceholders() {
			// We don't support placeholders inside the canned plan. The main reason
			// is that they would be invisible to the parser (which is reports the
			// number of placeholders, used to initialize the relevant structures).
			return nil, pgerror.Newf(pgcode.Syntax,
				"placeholders are not supported with PREPARE AS OPT PLAN")
		}
		// With a canned plan, the memo is already optimized.
	} else {
		// If the memo doesn't have placeholders, then fully optimize it, since
		// it can be reused without further changes to build the execution tree.
		if !f.Memo().HasPlaceholders() {
			if _, err := opc.optimizer.Optimize(); err != nil {
				return nil, err
			}
		}
	}

	// Detach the prepared memo from the factory and transfer its ownership
	// to the prepared statement. DetachMemo will re-initialize the optimizer
	// to an empty memo.
	return opc.optimizer.DetachMemo(), nil
}

// reuseMemo returns an optimized memo using a cached memo as a starting point.
//
// The cached memo is not modified; it is safe to call reuseMemo on the same
// cachedMemo from multiple threads concurrently.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.
func (opc *optPlanningCtx) reuseMemo(cachedMemo *memo.Memo) (*memo.Memo, error) {
	if !cachedMemo.HasPlaceholders() {
		// If there are no placeholders, the query was already fully optimized
		// (see buildReusableMemo).
		return cachedMemo, nil
	}
	f := opc.optimizer.Factory()
	// Finish optimization by assigning any remaining placeholders and
	// applying exploration rules. Reinitialize the optimizer and construct a
	// new memo that is copied from the prepared memo, but with placeholders
	// assigned.
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
		if isStale, err := prepared.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog); err != nil {
			return nil, err
		} else if isStale {
			prepared.Memo, err = opc.buildReusableMemo(ctx)
			opc.log(ctx, "rebuilding cached memo")
			if err != nil {
				return nil, err
			}
		}
		opc.log(ctx, "reusing cached memo")
		memo, err := opc.reuseMemo(prepared.Memo)
		return memo, err
	}

	if opc.useCache {
		// Consult the query cache.
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, opc.p.stmt.SQL)
		if ok {
			if isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog); err != nil {
				return nil, err
			} else if isStale {
				cachedData.Memo, err = opc.buildReusableMemo(ctx)
				if err != nil {
					return nil, err
				}
				// Update the plan in the cache. If the cache entry had PrepareMetadata
				// populated, it may no longer be valid.
				cachedData.PrepareMetadata = nil
				p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
				opc.log(ctx, "query cache hit but needed update")
				opc.flags.Set(planFlagOptCacheMiss)
			} else {
				opc.log(ctx, "query cache hit")
				opc.flags.Set(planFlagOptCacheHit)
			}
			memo, err := opc.reuseMemo(cachedData.Memo)
			return memo, err
		}
		opc.flags.Set(planFlagOptCacheMiss)
		opc.log(ctx, "query cache miss")
	}

	// We are executing a statement for which there is no reusable memo
	// available.
	f := opc.optimizer.Factory()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)
	if err := bld.Build(); err != nil {
		return nil, err
	}
	if _, isCanned := opc.p.stmt.AST.(*tree.CannedOptPlan); !isCanned {
		if _, err := opc.optimizer.Optimize(); err != nil {
			return nil, err
		}
	}

	// If this statement doesn't have placeholders, add it to the cache. Note
	// that non-prepared statements from pgwire clients cannot have
	// placeholders.
	if opc.useCache && !bld.HadPlaceholders && !bld.DisableMemoReuse {
		memo := opc.optimizer.DetachMemo()
		cachedData := querycache.CachedData{
			SQL:  opc.p.stmt.SQL,
			Memo: memo,
		}
		p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		opc.log(ctx, "query cache add")
		return memo, nil
	}

	return f.Memo(), nil
}
