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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

var computedColumnRewrites = settings.RegisterValidatedStringSetting(
	"sql.computed_column_rewrites",
	"allows rewriting computed column expressions in CREATE TABLE and ALTER TABLE statements; "+
		"the format is: '(before expression) -> (after expression) [, (before expression) -> (after expression) ...]'",
	"", /* defaultValue */
	func(_ *settings.Values, val string) error {
		_, err := parseComputedColumnRewrites(val)
		return err
	},
)

type rewritesMap map[string]tree.Expr

func parseComputedColumnRewrites(val string) (rewritesMap, error) {
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
	result := make(rewritesMap, len(set.Values))
	for _, v := range set.Values {
		binExpr, ok := v.(*tree.BinaryExpr)
		if !ok || binExpr.Operator != tree.JSONFetchVal {
			return nil, errors.Newf("invalid column rewrites expression (expected ->)")
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

// MaybeRewriteComputedColumn consults the sql.computed_column_rewrites cluster
// setting; if the given expression matches a "before expression" in the
// setting, it is replaced to the corresponding "after expression". Otherwise,
// the given expression is returned unchanged.
func MaybeRewriteComputedColumn(expr tree.Expr, sv *settings.Values) tree.Expr {
	rewritesStr := computedColumnRewrites.Get(sv)
	if rewritesStr == "" {
		return expr
	}
	rewrites, err := parseComputedColumnRewrites(rewritesStr)
	if err != nil {
		// This shouldn't happen - we should have validated the value.
		return expr
	}
	if newExpr, ok := rewrites[tree.Serialize(expr)]; ok {
		return newExpr
	}
	return expr
}
