// Copyright 2018 The Cockroach Authors.
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

package testutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// FakeScalarExpr is a fake scalar expression used for testing logical
// properties.
type FakeScalarExpr struct {
	// We embed a nil opt.ScalarExpr, which causes any methods that are not
	// specifically overridden to panic.
	opt.ScalarExpr

	typ types.T
}

var _ opt.ScalarExpr = &FakeScalarExpr{}

// NewFakeScalarExpr constructs a new instance of a FakeScalarExpr.
func NewFakeScalarExpr(typ types.T) opt.ScalarExpr {
	return &FakeScalarExpr{typ: typ}
}

// Op is part of the ScalarExpr interface.
func (e *FakeScalarExpr) Op() opt.Operator {
	return opt.UnknownOp
}

// ChildCount is part of the ScalarExpr interface.
func (e *FakeScalarExpr) ChildCount() int {
	return 0
}

// Private is part of the ScalarExpr interface.
func (e *FakeScalarExpr) Private() interface{} {
	return nil
}

// DataType is part of the ScalarExpr interface.
func (e *FakeScalarExpr) DataType() types.T {
	return e.typ
}
