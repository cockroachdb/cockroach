// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
