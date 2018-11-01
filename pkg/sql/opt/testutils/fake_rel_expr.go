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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// FakeRelExpr is a fake relational expression used for testing logical
// properties.
type FakeRelExpr struct {
	// We embed a nil memo.RelExpr, which causes any methods that are not
	// specifically overridden to panic.
	memo.RelExpr

	r props.Relational
}

var _ memo.RelExpr = &FakeRelExpr{}

// NewFakeRelExpr constructs a new instance of a FakeRelExpr.
func NewFakeRelExpr(r props.Relational) memo.RelExpr {
	return &FakeRelExpr{r: r}
}

// Op is part of the RelExpr interface.
func (e *FakeRelExpr) Op() opt.Operator {
	return opt.UnknownOp
}

// ChildCount is part of the RelExpr interface.
func (e *FakeRelExpr) ChildCount() int {
	return 0
}

// Relational is part of the RelExpr interface.
func (e *FakeRelExpr) Relational() *props.Relational {
	return &e.r
}

// Physical is part of the RelExpr interface.
func (e *FakeRelExpr) Physical() *props.Physical {
	return props.MinPhysProps
}
