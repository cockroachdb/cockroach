// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import "testing"

// TestMarkers is a pedantic test to get nice code coverage by exercising the
// marker interface implementations. It can also act as listing of expected
// implementations.
func TestMarkers(t *testing.T) {
	t.Run("expr", func(t *testing.T) {
		for _, expr := range []expr{
			anyExpr{},
			Var(""),
			valueExpr{},
		} {
			expr.expr()
		}
	})
	t.Run("clause", func(t *testing.T) {
		for _, clause := range []Clause{
			and{},
			tripleDecl{},
			eqDecl{},
			filterDecl{},
			ruleInvocation{},
		} {
			clause.clause()
		}
	})
}
