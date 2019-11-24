// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file implements the select code that deals with column references
// and resolving column names in expressions.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// resolveNames walks the provided expression and resolves all names
// using the tableInfo and iVarHelper.
// If anything that looks like a column reference (indexed vars, star,
// etc) is encountered, or a function that may change value for every
// row in a table, the 2nd return value is true.
// If any star is expanded, the 3rd return value is true.
func (p *planner) resolveNames(
	expr tree.Expr, source *sqlbase.DataSourceInfo, ivarHelper tree.IndexedVarHelper,
) (tree.Expr, bool, error) {
	if expr == nil {
		return nil, false, nil
	}
	return sqlbase.ResolveNamesUsingVisitor(&p.nameResolutionVisitor, expr, source, ivarHelper, p.SessionData().SearchPath)
}
