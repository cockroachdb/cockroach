// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// TypedRows is an array of an array of TypedExprs for pre-typed values.
type TypedRows struct {
	Rows [][]tree.TypedExpr
}

var _ ScalarExpr = &TypedRows{}

// Op returns the operator type of the expression.
func (t TypedRows) Op() Operator {
	return AnyScalarOp
}

// ChildCount returns the number of children of the expression.
func (t TypedRows) ChildCount() int {
	return 0
}

// Child returns the nth child of the expression.
func (t TypedRows) Child(nth int) Expr {
	return nil
}

// Private returns operator-specific data. Callers are expected to know the
// type and format of the data, which will differ from operator to operator.
// For example, an operator may choose to return one of its fields, or perhaps
// a pointer to itself, or nil if there is nothing useful to return.
func (t TypedRows) Private() interface{} {
	return nil
}

// String returns a human-readable string representation for the expression
// that can be used for debugging and testing.
func (t TypedRows) String() string {
	return ""
}

// Rank is part of ScalarExpr interface
func (t TypedRows) Rank() ScalarRank {
	return 0
}

// DataType is part of ScalarExpr interface
func (t TypedRows) DataType() *types.T {
	return nil
}
