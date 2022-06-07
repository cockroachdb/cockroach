// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
