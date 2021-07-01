// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// ComputedColumnRewritesMap stores a map of computed column expression
// rewrites. The key is a formatted AST (using tree.Serialize()).
type ComputedColumnRewritesMap map[string]tree.Expr

// ParseComputedColumnRewrites parses a string of the form:
//
//   (before expression) -> (after expression) [, (before expression) -> (after expression) ...]
//
// into a ComputedColumnRewritesMap.
//
// Used to implement the experimental_computed_column_rewrites session setting.
func ParseComputedColumnRewrites(val string) (ComputedColumnRewritesMap, error) {
	if val == "" {
		return nil, nil
	}
	stmt, err := parser.ParseOne(fmt.Sprintf("SET ROW (%s)", val))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse column rewrites")
	}
	set, ok := stmt.AST.(*tree.SetVar)
	if !ok {
		return nil, errors.AssertionFailedf("expected a SET statement, but found %T", stmt)
	}
	result := make(ComputedColumnRewritesMap, len(set.Values))
	for _, v := range set.Values {
		binExpr, ok := v.(*tree.BinaryExpr)
		if !ok || binExpr.Operator.Symbol != tree.JSONFetchVal {
			return nil, errors.Newf("invalid column rewrites expression (expected -> operator)")
		}
		left, ok := binExpr.Left.(*tree.ParenExpr)
		if !ok {
			return nil, errors.Newf("missing parens around \"before\" expression")
		}
		right, ok := binExpr.Right.(*tree.ParenExpr)
		if !ok {
			return nil, errors.Newf("missing parens around \"after\" expression")
		}
		result[tree.Serialize(left.Expr)] = right.Expr
	}
	return result, nil
}

// MaybeRewriteComputedColumn consults the experimental_computed_column_rewrites
// session setting; if the given expression matches a "before expression" in the
// setting, it is replaced to the corresponding "after expression". Otherwise,
// the given expression is returned unchanged.
func MaybeRewriteComputedColumn(expr tree.Expr, sessionData *sessiondata.SessionData) tree.Expr {
	rewritesStr := sessionData.ExperimentalComputedColumnRewrites
	if rewritesStr == "" {
		return expr
	}
	rewrites, err := ParseComputedColumnRewrites(rewritesStr)
	if err != nil {
		// This shouldn't happen - we should have validated the value.
		return expr
	}
	if newExpr, ok := rewrites[tree.Serialize(expr)]; ok {
		return newExpr
	}
	return expr
}
