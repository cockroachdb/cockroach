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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/querycache"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// makeOptimizerPlan is an alternative to makePlan which uses the cost-based
// optimizer. On success, the returned flags always have planFlagOptUsed set.
func (p *planner) makeOptimizerPlan(ctx context.Context, stmt Statement) (planFlags, error) {
	// Ensure that p.curPlan is populated in case an error occurs early,
	// so that maybeLogStatement in the error case does not find an empty AST.
	p.curPlan = planTop{AST: stmt.AST}

	// Start with fast check to see if top-level statement is supported.
	switch stmt.AST.(type) {
	case *tree.ParenSelect, *tree.Select, *tree.SelectClause,
		*tree.UnionClause, *tree.ValuesClause, *tree.Explain,
		*tree.Insert, *tree.Update, *tree.CreateTable:

	default:
		return 0, pgerror.Unimplemented("statement", fmt.Sprintf("unsupported statement: %T", stmt.AST))
	}

	var opc optPlanningCtx
	opc.init(p, stmt)

	if p.EvalContext().PrepareOnly {
		// We are preparing a statement.
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
		}
		if opc.allowMemoReuse {
			stmt.Prepared.Memo = memo
		}
		// Construct a dummy plan that has correct output columns. Only output
		// columns and placeholder types are needed.
		p.curPlan.plan = &zeroNode{columns: resultCols}
		return opc.flags, nil
	}

	// We are executing a statement.
	execMemo, err := opc.buildExecMemo(ctx)
	if err != nil {
		return 0, err
	}

	// Build the plan tree and store it in planner.curPlan.
	root := execMemo.RootExpr()
	execFactory := makeExecFactory(p)
	plan, err := execbuilder.New(&execFactory, execMemo, root, p.EvalContext()).Build()
	if err != nil {
		return 0, err
	}

	p.curPlan = *plan.(*planTop)
	// Since the assignment above just cleared the AST, we need to set it again.
	p.curPlan.AST = stmt.AST

	cols := planColumns(p.curPlan.plan)
	if stmt.ExpectedTypes != nil {
		if !stmt.ExpectedTypes.TypesEqual(cols) {
			return 0, pgerror.NewError(pgerror.CodeFeatureNotSupportedError,
				"cached plan must not change result type")
		}
	}

	return opc.flags, nil
}

type optPlanningCtx struct {
	p    *planner
	stmt Statement

	catalog optCatalog

	// When set, we are allowed to reuse a memo, or store a memo for later reuse.
	allowMemoReuse bool

	// When set, we consult and update the query cache. Never set if
	// allowMemoReuse is false.
	useCache bool

	flags planFlags
}

func (opc *optPlanningCtx) init(p *planner, stmt Statement) {
	opc.p = p
	opc.stmt = stmt
	opc.catalog.init(p.execCfg.TableStatsCache, p)
	p.optimizer.Init(p.EvalContext())

	// If the current transaction has uncommitted DDL statements, we cannot rely
	// on descriptor versions for detecting a "stale" memo. This is because
	// descriptor versions are bumped at most once per transaction, even if there
	// are multiple DDL operations; and transactions can be aborted leading to
	// potential reuse of versions. To avoid these issues, we prevent saving a
	// memo (for prepare) or reusing a saved memo (for execute).
	opc.allowMemoReuse = !p.Tables().hasUncommittedTables()
	opc.useCache = opc.allowMemoReuse && queryCacheEnabled.Get(&p.execCfg.Settings.SV)
	opc.flags = planFlagOptUsed
}

func (opc *optPlanningCtx) log(ctx context.Context, msg string) {
	if log.VDepth(1, 1) {
		log.InfofDepth(ctx, 1, "%s: %s", msg, opc.stmt)
	} else {
		log.Event(ctx, msg)
	}
}

// buildReusableMemo builds the statement into a memo that can be stored for
// prepared statements and can later be used as a starting point for
// optimization.
func (opc *optPlanningCtx) buildReusableMemo(ctx context.Context) (*memo.Memo, error) {
	p := opc.p
	// Build the Memo (optbuild) and apply normalization rules to it. If the
	// query contains placeholders, values are not assigned during this phase,
	// as that only happens during the EXECUTE phase. If the query does not
	// contain placeholders, then also apply exploration rules to the Memo so
	// that there's even less to do during the EXECUTE phase.
	//
	f := p.optimizer.Factory()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.stmt.AST)
	bld.KeepPlaceholders = true
	if err := bld.Build(); err != nil {
		// isCorrelated is used in the fallback case to create a better error.
		// TODO(radu): setting the flag here is a bit hacky, ideally we would return
		// it some other way.
		p.curPlan.isCorrelated = bld.IsCorrelated
		return nil, err
	}
	// If the memo doesn't have placeholders, then fully optimize it, since
	// it can be reused without further changes to build the execution tree.
	if !f.Memo().HasPlaceholders() {
		p.optimizer.Optimize()
	}

	// Detach the prepared memo from the factory and transfer its ownership
	// to the prepared statement. DetachMemo will re-initialize the optimizer
	// to an empty memo.
	return p.optimizer.DetachMemo(), nil
}

// reuseMemo returns an optimized memo using a cached memo as a starting point.
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

func (opc *optPlanningCtx) buildExecMemo(ctx context.Context) (*memo.Memo, error) {
	prepared := opc.stmt.Prepared
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
			if err != nil {
				return nil, err
			}
		}
		return opc.reuseMemo(prepared.Memo)
	}

	if opc.useCache {
		// Consult the query cache.
		cachedData, ok := p.execCfg.QueryCache.Find(opc.stmt.SQL)
		if ok {
			if isStale, err := cachedData.Memo.IsStale(ctx, p.EvalContext(), &opc.catalog); err != nil {
				return nil, err
			} else if isStale {
				cachedData.Memo, err = opc.buildReusableMemo(ctx)
				if err != nil {
					return nil, err
				}
				// Update the plan in the cache.
				p.execCfg.QueryCache.Add(&cachedData)
				opc.log(ctx, "query cache hit but needed update")
				opc.flags.Set(planFlagOptCacheMiss)
			} else {
				opc.log(ctx, "query cache hit")
				opc.flags.Set(planFlagOptCacheHit)
			}
			return opc.reuseMemo(cachedData.Memo)
		}
		opc.flags.Set(planFlagOptCacheMiss)
		opc.log(ctx, "query cache miss")
	}

	// We are executing a statement for which there is no reusable memo
	// available.
	f := opc.p.optimizer.Factory()
	bld := optbuilder.New(ctx, &p.semaCtx, p.EvalContext(), &opc.catalog, f, opc.stmt.AST)
	if err := bld.Build(); err != nil {
		// isCorrelated is used in the fallback case to create a better error.
		p.curPlan.isCorrelated = bld.IsCorrelated
		return nil, err
	}
	p.optimizer.Optimize()

	// If this statement doesn't have placeholders, add it to the cache. Note
	// that non-prepared statements from pgwire clients cannot have
	// placeholders.
	if opc.useCache && !bld.HadPlaceholders {
		memo := p.optimizer.DetachMemo()
		cachedData := querycache.CachedData{
			SQL:  opc.stmt.SQL,
			Memo: memo,
		}
		p.execCfg.QueryCache.Add(&cachedData)
		opc.log(ctx, "query cache add")
		return memo, nil
	}

	return f.Memo(), nil
}
