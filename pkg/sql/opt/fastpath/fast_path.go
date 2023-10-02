// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package fastpath implements fast-path rules for queries.
//
// Motivation
// ----------
//
// In general, a query that has any placeholders cannot be fully optimized at
// PREPARE time. Each time the query is executed, the cached memo must be
// copied and fully optimized with the new placeholder values. The overhead for
// this can be significant, so it is desirable to avoid the copy + re-optimize
// step.
//
// Overview
// --------
//
// Fast-path rules avoid the re-planning step for simple queries that we know
// statically will not benefit from further optimization. After a fast-path rule
// matches, the memo is fully optimized even if it contains placeholders. The
// resulting plan can be directly used from the cache without any copying or
// exploration.
//
// Unlike normalization or exploration rules, fast-path rules match the entire
// expression tree at once (or not at all). Once a fast-path rule matches, no
// others apply. This simplifies the task of that ensuring fast-path rules are
// always optimal, since they do not interact with one another.
//
// Fast-path rules may return a new expression (like a PlaceholderScan) or
// simply return the matched expression. In either case, the result is set as
// the root of the memo, and its properties are calculated. After that, the memo
// is considered fully optimized and can be cached and freely reused.
//
// Handling of placeholders depends on the query. For the PlaceholderScan case,
// the placeholders are evaluated during exec-building and used to construct
// spans for a constrained Scan. If the placeholders are in a Values or Project
// operator, evaluation will be deferred until execution-time.
// TODO(drewk): consider evaluating placeholders during exec-building.
//
// Note that queries without placeholders can already be fully optimized before
// being cached. Since queries without placeholders already skip the
// re-planning step, fast-path rules are not as useful for them. Note that
// fast-path rules may still be useful for workloads with many different query
// fingerprints, as well as for routines (which don't currently cache plans).
package fastpath

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

// TryOptimizerFastPath attempts to match fast-path rules to the normalized
// expression tree. If any match, a new root expression is returned. Note that
// the properties still need to be set for the expression tree, and it needs to
// be set as the root expression for the memo.
//
// NOTE: callers must wrap TryOptimizerFastPath with an optimizer panic-catcher.
func TryOptimizerFastPath(
	ctx context.Context, evalCtx *eval.Context, f *norm.Factory,
) (root memo.RelExpr, ok bool) {
	fp := fastPathExplorer{
		ctx:     ctx,
		evalCtx: evalCtx,
		f:       f,
	}
	fp.funcs.Init(&fp)
	root, ok = fp.tryFastPath()
	return root, ok
}

type fastPathExplorer struct {
	ctx     context.Context
	evalCtx *eval.Context
	f       *norm.Factory

	// funcs is the struct used to call all custom match and replace functions
	// used by the fast-path rules. It wraps an unnamed norm.CustomFuncs, so
	// it provides a clean interface for calling functions from both the fastpath
	// and norm packages using the same prefix.
	funcs CustomFuncs
}
