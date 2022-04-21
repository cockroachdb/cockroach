// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ParseProjectionAndFilter is a helper to parse specified select and where clauses
// and return parsed select and filter expressions.
// Filter is optional, while at least 1 projection expression is required.
func ParseProjectionAndFilter(
	selectClause, whereClause string,
) (tree.SelectExprs, tree.Expr, error) {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(selectClause)
	sb.WriteString(" FROM _") // From clause doesn't matter for CDC, so use placeholder
	if whereClause != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(whereClause)
	}
	stmt, err := parser.ParseOne(sb.String())

	if err != nil {
		return nil, nil, err
	}

	sc := stmt.AST.(*tree.Select).Select.(*tree.SelectClause)
	if sc.Where == nil {
		return sc.Exprs, nil, nil
	}
	return sc.Exprs, sc.Where.Expr, nil
}
