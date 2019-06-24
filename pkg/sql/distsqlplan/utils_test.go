// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlplan

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// fakeExprContext is a fake implementation of ExprContext that always behaves
// as if it were part of a non-local query.
type fakeExprContext struct{}

var _ ExprContext = fakeExprContext{}

func (fakeExprContext) EvalContext() *tree.EvalContext {
	return &tree.EvalContext{}
}

func (fakeExprContext) IsLocal() bool {
	return false
}

func (fakeExprContext) EvaluateSubqueries() bool {
	return true
}
