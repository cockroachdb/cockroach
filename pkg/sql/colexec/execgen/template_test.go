// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

func TestGenerateInstantiateCombinations(t *testing.T) {
	tcs := []struct {
		args     [][]string
		expected [][]string
	}{
		{
			args: [][]string{
				{"1"},
			},
			expected: [][]string{
				{"1"},
			},
		},
		{
			args: [][]string{
				{"true", "false"},
			},
			expected: [][]string{
				{"true"},
				{"false"},
			},
		},
		{
			args: [][]string{
				{"t", "f"},
				{"1"},
				{"foo", "bar", "baz"},
			},
			expected: [][]string{
				{"t", "1", "foo"},
				{"t", "1", "bar"},
				{"t", "1", "baz"},
				{"f", "1", "foo"},
				{"f", "1", "bar"},
				{"f", "1", "baz"},
			},
		},
	}
	for _, tc := range tcs {
		assert.Equal(t, tc.expected, generateInstantiateCombinations(tc.args))
	}
}
