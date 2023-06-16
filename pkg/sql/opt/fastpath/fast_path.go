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
package fastpath

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
)

// TryOptimizerFastPath attempts to match fast-path rules to the normalized
// expression tree. If any match, a new root expression is returned. Note that
// the properties still need to be set for the expression tree, and it needs to
// be set as the root expression for the memo.
func TryOptimizerFastPath(
	ctx context.Context, evalCtx *eval.Context, f *norm.Factory,
) (root memo.RelExpr, ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate internal errors without having to add
			// error checks everywhere throughout the code. This is only possible
			// because the code does not update shared state and does not manipulate
			// locks.
			if shouldCatch, e := errorutil.ShouldCatch(r); shouldCatch {
				err = e
			} else {
				// Other panic objects can't be considered "safe" and thus are
				// propagated as crashes that terminate the session.
				panic(r)
			}
		}
	}()
	fp := fastPathExplorer{
		ctx:     ctx,
		evalCtx: evalCtx,
		f:       f,
	}
	fp.funcs.Init(&fp)
	root, ok = fp.tryFastPath()
	return root, ok, nil
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
