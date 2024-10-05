// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// This file implements the select code that deals with column references
// and resolving column names in expressions.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// resolveNames walks the provided expression and resolves all names
// using the tableInfo and iVarHelper.
func (p *planner) resolveNames(
	expr tree.Expr, source *colinfo.DataSourceInfo, ivarHelper tree.IndexedVarHelper,
) (tree.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	return schemaexpr.ResolveNamesUsingVisitor(&p.nameResolutionVisitor, expr, source, ivarHelper)
}
