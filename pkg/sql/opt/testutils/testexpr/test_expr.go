// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testexpr

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

// Instance is a dummy RelExpr that contains various properties that can be
// extracted via that interface. It can be initialized with whatever subset of
// fields are required for the particular test; for example:
//
//   e := &testexpr.Instance{
//     Rel: &props.Relational{...},
//     Provided: &physical.Provided{...},
//   }
//
type Instance struct {
	Rel      *props.Relational
	Required *physical.Required
	Provided *physical.Provided
	Priv     interface{}

	// We embed a RelExpr to provide implementation for the unexported methods of
	// RelExpr. This should not be initialized (resulting in a panic if any of
	// those methods are called).
	memo.RelExpr
}

var _ memo.RelExpr = &Instance{}

// Relational is part of the RelExpr interface.
func (e *Instance) Relational() *props.Relational { return e.Rel }

// RequiredPhysical is part of the RelExpr interface.
func (e *Instance) RequiredPhysical() *physical.Required { return e.Required }

// ProvidedPhysical is part of the RelExpr interface.
func (e *Instance) ProvidedPhysical() *physical.Provided { return e.Provided }

// Private is part of the RelExpr interface.
func (e *Instance) Private() interface{} { return e.Priv }

// Op is part of the RelExpr interface.
func (e *Instance) Op() opt.Operator {
	// We implement this to keep checkExpr happy. It shouldn't match a real
	// operator.
	return 0xFFFF
}

// The rest of the methods are not implemented. Fields can be added to Instance
// to implement these as necessary.

// ChildCount is part of the RelExpr interface.
func (e *Instance) ChildCount() int { panic("not implemented") }

// Child is part of the RelExpr interface.
func (e *Instance) Child(nth int) opt.Expr { panic("not implemented") }

// String is part of the RelExpr interface.
func (e *Instance) String() string { panic("not implemented") }

// SetChild is part of the RelExpr interface.
func (e *Instance) SetChild(nth int, child opt.Expr) { panic("not implemented") }

// Memo is part of the RelExpr interface.
func (e *Instance) Memo() *memo.Memo { panic("not implemented") }

// FirstExpr is part of the RelExpr interface.
func (e *Instance) FirstExpr() memo.RelExpr { panic("not implemented") }

// NextExpr is part of the RelExpr interface.
func (e *Instance) NextExpr() memo.RelExpr { panic("not implemented") }

// Cost is part of the RelExpr interface.
func (e *Instance) Cost() memo.Cost { panic("not implemented") }
