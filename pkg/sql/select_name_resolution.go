// Copyright 2016 The Cockroach Authors.
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
//
// This file implements the select code that deals with column references
// and resolving column names in expressions.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// resolveNamesForRender resolves the names in expr using the naming
// context of the given renderNode (FROM clause).
func (p *planner) resolveNamesForRender(
	expr tree.Expr, s *renderNode,
) (tree.Expr, bool, bool, error) {
	return p.resolveNames(expr, s.sourceInfo, s.ivarHelper)
}

// resolveNames walks the provided expression and resolves all names
// using the tableInfo and iVarHelper.
// If anything that looks like a column reference (indexed vars, star,
// etc) is encountered, or a function that may change value for every
// row in a table, the 2nd return value is true.
// If any star is expanded, the 3rd return value is true.
func (p *planner) resolveNames(
	expr tree.Expr, sources sqlbase.MultiSourceInfo, ivarHelper tree.IndexedVarHelper,
) (tree.Expr, bool, bool, error) {
	if expr == nil {
		return nil, false, false, nil
	}
	return sqlbase.ResolveNamesUsingVisitor(&p.nameResolutionVisitor, expr, sources, ivarHelper, p.SessionData().SearchPath)
}
