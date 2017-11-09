// Copyright 2017 The Cockroach Authors.
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

package transform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
)

// IsAggregateVisitor checks if walked expressions contain aggregate functions.
type IsAggregateVisitor struct {
	Aggregated bool
	// searchPath is used to search for unqualified function names.
	searchPath parser.SearchPath
}

var _ parser.Visitor = &IsAggregateVisitor{}

// VisitPre satisfies the Visitor interface.
func (v *IsAggregateVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	switch t := expr.(type) {
	case *parser.FuncExpr:
		if t.IsWindowFunctionApplication() {
			// A window function application of an aggregate builtin is not an
			// aggregate function, but it can contain aggregate functions.
			return true, expr
		}
		fd, err := t.Func.Resolve(v.searchPath)
		if err != nil {
			return false, expr
		}
		if _, ok := builtins.Aggregates[fd.Name]; ok {
			v.Aggregated = true
			return false, expr
		}
	case *parser.Subquery:
		return false, expr
	}

	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (*IsAggregateVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// Reset clear the IsAggregateVisitor's internal state.
func (v *IsAggregateVisitor) Reset() {
	v.Aggregated = false
}
