// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import (
	"go/token"
	"testing"

	"github.com/dave/dst"
	"github.com/stretchr/testify/assert"
)

func TestTryEvalBools(t *testing.T) {
	tcs := []struct {
		expr     dst.Expr
		expected bool
		failed   bool
	}{
		{
			expr:     &dst.Ident{Name: "true"},
			expected: true,
		},
		{
			expr:     &dst.Ident{Name: "false"},
			expected: false,
		},
		{
			expr:     &dst.UnaryExpr{Op: token.NOT, X: &dst.Ident{Name: "true"}},
			expected: false,
		},
		{
			expr:     &dst.UnaryExpr{Op: token.NOT, X: &dst.Ident{Name: "false"}},
			expected: true,
		},
		{
			expr:     &dst.BinaryExpr{Op: token.LAND, X: &dst.Ident{Name: "false"}, Y: &dst.Ident{Name: "false"}},
			expected: false,
		},
		{
			expr:     &dst.BinaryExpr{Op: token.LAND, X: &dst.Ident{Name: "true"}, Y: &dst.Ident{Name: "false"}},
			expected: false,
		},
		{
			expr:     &dst.BinaryExpr{Op: token.LAND, X: &dst.Ident{Name: "false"}, Y: &dst.Ident{Name: "true"}},
			expected: false,
		},
		{
			expr:     &dst.BinaryExpr{Op: token.LAND, X: &dst.Ident{Name: "true"}, Y: &dst.Ident{Name: "true"}},
			expected: true,
		},
		{
			expr:     &dst.BinaryExpr{Op: token.LOR, X: &dst.Ident{Name: "false"}, Y: &dst.Ident{Name: "false"}},
			expected: false,
		},
		{
			expr:     &dst.BinaryExpr{Op: token.LOR, X: &dst.Ident{Name: "true"}, Y: &dst.Ident{Name: "false"}},
			expected: true,
		},
		{
			expr:     &dst.BinaryExpr{Op: token.LOR, X: &dst.Ident{Name: "false"}, Y: &dst.Ident{Name: "true"}},
			expected: true,
		},
		{
			expr:     &dst.BinaryExpr{Op: token.LOR, X: &dst.Ident{Name: "true"}, Y: &dst.Ident{Name: "true"}},
			expected: true,
		},
		{
			expr:     &dst.BinaryExpr{Op: token.ADD, X: &dst.Ident{Name: "true"}, Y: &dst.Ident{Name: "true"}},
			expected: false,
			failed:   true,
		},
	}
	for _, tc := range tcs {
		actual, ok := tryEvalBool(tc.expr)
		assert.NotEqual(t, ok, tc.failed, "unexpected value for ok")
		assert.Equal(t, actual, tc.expected, "wrong result for expr", prettyPrintExprs(tc.expr))
	}
}
