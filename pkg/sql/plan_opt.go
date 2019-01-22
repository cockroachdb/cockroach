// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
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
//  - Memo (for reuse during exec, if appropriate).
//
// On success, the returned flags always have planFlagOptUsed set.
//
// isCorrelated is set in error cases if we detect a correlated subquery; it is
// used in the fallback case to create a better error.
func (p *planner) prepareUsingOptimizer(
	ctx context.Context,
) (_ planFlags, isCorrelated bool, _ error) {
	stmt := p.stmt
	if err := checkOptSupportForTopStatement(stmt.AST); err != nil {
		return 0, false, err
	}

	var opc optPlanningCtx
	opc.init(p, stmt.AST)

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
					return 0, false, err
				}
				if !isStale {
					opc.log(ctx, "query cache hit (prepare)")
					opc.flags.Set(planFlagOptCacheHit)
					stmt.Prepared.Columns = pm.Columns
					stmt.Prepared.Types = pm.Types
					stmt.Prepared.Memo = cachedData.Memo
					return opc.flags, false, nil
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

	memo, isCorrelated, err := opc.buildReusableMemo(ctx)
	if err != nil {
		return 0, isCorrelated, err
	}

	md := memo.Metadata()
	physical := memo.RootProps()
	resultCols := make(sqlbase.ResultColumns, len(physical.Presentation))
	for i, col := range physical.Presentation {
		resultCols[i].Name = col.Alias
		resultCols[i].Typ = md.ColumnMeta(col.ID).Type
		if err := checkResultType(resultCols[i].Typ); err != nil {
			return 0, false, err
		}
	}

	// Verify that all placeholder types have been set.
	if err := p.semaCtx.Placeholders.Types.AssertAllSet(); err != nil {
		return 0, false, err
	}

	stmt.Prepared.Columns = resultCols
	stmt.Prepared.Types = p.semaCtx.Placeholders.Types
	if opc.allowMemoReuse {
		stmt.Prepared.Memo = memo
		if opc.useCache {
			cachedData := querycache.CachedData{
				SQL:  stmt.SQL,
				Memo: memo,
				// We rely on stmt.Prepared.PrepareMetadata not being subsequently modified.
				// TODO(radu): this also holds on to the memory referenced by other
				// PreparedStatement fields.
				PrepareMetadata: &stmt.Prepared.PrepareMetadata,
			}
			p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		}
	}
	return opc.flags, false, nil
}

// makeOptimizerPlan is an alternative to makePlan which uses the cost-based
// optimizer. On success, the returned flags always have planFlagOptUsed set.
//
// isCorrelated is set in error cases if we detect a correlated subquery; it is
// used in the fallback case to create a better error.
func (p *planner) makeOptimizerPlan(ctx context.Context) (_ *planTop, isCorrelated bool, _ error) {
	stmt := p.stmt
	if err := checkOptSupportForTopStatement(stmt.AST); err != nil {
		return nil, false, err
	}

	var opc optPlanningCtx
	opc.init(p, stmt.AST)

	execMemo, isCorrelated, err := opc.buildExecMemo(ctx)
	if err != nil {
		return nil, isCorrelated, err
	}

	// Build the plan tree.
	root := execMemo.RootExpr()
	execFactory := makeExecFactory(p)
	plan, err := execbuilder.New(&execFactory, execMemo, root, p.EvalContext()).Build()
	if err != nil {
		return nil, false, err
	}

	result := plan.(*planTop)
	result.AST = stmt.AST
	result.flags = opc.flags

	cols := planColumns(result.plan)
	if stmt.ExpectedTypes != nil {
		if !stmt.ExpectedTypes.TypesEqual(cols) {
			return nil, false, pgerror.NewError(
				pgerror.CodeFeatureNotSupportedError, "cached plan must not change result type",
			)
		}
	}

	return result, false, nil
}

func checkOptSupportForTopStatement(AST tree.Statement) error {
	// Start with fast check to see if top-level statement is supported.
	switch AST.(type) {
	case *tree.ParenSelect, *tree.Select, *tree.SelectClause,
		*tree.UnionClause, *tree.ValuesClause, *tree.Explain,
		*tree.Insert, *tree.Update, *tree.Delete, *tree.CreateTable:
		return nil

	default:
		return pgerror.Unimplemented("statement", fmt.Sprintf("unsupported statement: %T", AST))
	}
}

type optPlanningCtx struct {
	p *planner

	catalog optCatalog

	// When set, we are allowed to reuse a memo, or store a memo for later reuse.
	allowMemoReuse bool

	// When set, we consult and update the query cache. Never set if
	// allowMemoReuse is false.
	useCache bool

	flags planFlags
}

func (opc *optPlanningCtx) init(p *planner, AST tree.Statement) {
	opc.p = p
	opc.catalog.init(p.execCfg.TableStatsCache, p)
	p.optimizer.Init(p.EvalContext())
	opc.flags = planFlagOptUsed

	// We only allow memo caching for SELECT/INSERT/UPDATE/DELETE. We could
	// support it for all statements in principle, but it would increase the
	// surface of potential issues (conditions we need to detect to invalidate a
	// cached memo).
	switch AST.(type) {
	case *tree.ParenSelect, *tree.Select, *tree.SelectClause, *tree.UnionClause, *tree.ValuesClause,
		*tree.Insert, *tree.Update, *tree.Delete:
		// If the current transaction has uncommitted DDL statements, we cannot rely
		// on descriptor versions for detecting a "stale" memo. This is because
		// descriptor versions are bumped at most once per transaction, even if there
		// are multiple DDL operations; and transactions can be aborted leading to
		// potential reuse of versions. To avoid these issues, we prevent saving a
		// memo (for prepare) or reusing a saved memo (for execute).
		opc.allowMemoReuse = !p.Tables().hasUncommittedTables()
		opc.useCache = opc.allowMemoReuse && queryCacheEnabled.Get(&p.execCfg.Settings.SV)

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
//
// isCorrelated is set in error cases if we detect a correlated subquery; it is
// used in the fallback case to create a better error.
func (opc *optPlanningCtx) buildReusableMemo(
	ctx context.Context,
) (_ *memo.Memo, isCorrelated bool, _ error) {
	p := opc.p
	// Build the Memo (optbuild) and apply normalization rules to it. If the
	// query contains placeholders, values are not assigned during this phase,
	// as that only happens during the EXECUTE phase. If the query does not
	// contain placeholders, then also apply exploration rules to the Memo so
	// that there's even less to do during the EXECUTE phase.
	//
	f := p.optimizer.Factory()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)
	bld.KeepPlaceholders = true
	if err := bld.Build(); err != nil {
		return nil, bld.IsCorrelated, err
	}
	// If the memo doesn't have placeholders, then fully optimize it, since
	// it can be reused without further changes to build the execution tree.
	if !f.Memo().HasPlaceholders() {
		p.optimizer.Optimize()
	}

	// Detach the prepared memo from the factory and transfer its ownership
	// to the prepared statement. DetachMemo will re-initialize the optimizer
	// to an empty memo.
	return p.optimizer.DetachMemo(), false, nil
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
	f := opc.p.optimizer.Factory()
	// Finish optimization by assigning any remaining placeholders and
	// applying exploration rules. Reinitialize the optimizer and construct a
	// new memo that is copied from the prepared memo, but with placeholders
	// assigned.
	if err := f.AssignPlaceholders(cachedMemo); err != nil {
		return nil, err
	}
	opc.p.optimizer.Optimize()
	return f.Memo(), nil
}

// buildExecMemo creates a fully optimized memo, possibly reusing a previously
// cached memo as a starting point.
//
// The returned memo is only safe to use in one thread, during execution of the
// current statement.
//
// isCorrelated is set in error cases if we detect a correlated subquery; it is
// used in the fallback case to create a better error.
func (opc *optPlanningCtx) buildExecMemo(
	ctx context.Context,
) (_ *memo.Memo, isCorrelated bool, _ error) {
	prepared := opc.p.stmt.Prepared
	p := opc.p
	if opc.allowMemoReuse && prepared != nil && prepared.Memo != nil {
		// We are executing a previously prepared statement and a reusable memo is
		// available.

		// If the prepared memo has been invalidated by schema or other changes,
		// re-prepare it.
		if isStale, err := prepared.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog); err != nil {
			return nil, false, err
		} else if isStale {
			prepared.Memo, isCorrelated, err = opc.buildReusableMemo(ctx)
			opc.log(ctx, "rebuilding cached memo")
			if err != nil {
				return nil, isCorrelated, err
			}
		}
		opc.log(ctx, "reusing cached memo")
		memo, err := opc.reuseMemo(prepared.Memo)
		return memo, false, err
	}

	if opc.useCache {
		// Consult the query cache.
		cachedData, ok := p.execCfg.QueryCache.Find(&p.queryCacheSession, opc.p.stmt.SQL)
		if ok {
			if isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog); err != nil {
				return nil, false, err
			} else if isStale {
				cachedData.Memo, _, err = opc.buildReusableMemo(ctx)
				if err != nil {
					return nil, false, err
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
			return memo, false, err
		}
		opc.flags.Set(planFlagOptCacheMiss)
		opc.log(ctx, "query cache miss")
	}

	// We are executing a statement for which there is no reusable memo
	// available.
	f := opc.p.optimizer.Factory()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.p.stmt.AST)
	if err := bld.Build(); err != nil {
		return nil, bld.IsCorrelated, err
	}
	p.optimizer.Optimize()

	// If this statement doesn't have placeholders, add it to the cache. Note
	// that non-prepared statements from pgwire clients cannot have
	// placeholders.
	if opc.useCache && !bld.HadPlaceholders {
		memo := p.optimizer.DetachMemo()
		cachedData := querycache.CachedData{
			SQL:  opc.p.stmt.SQL,
			Memo: memo,
		}
		p.execCfg.QueryCache.Add(&p.queryCacheSession, &cachedData)
		opc.log(ctx, "query cache add")
		return memo, false, nil
	}

	return f.Memo(), false, nil
}
