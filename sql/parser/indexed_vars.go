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
// Author: Radu Berinde (radu@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
)

// IndexedVarContainer provides the implementation of TypeCheck, Eval, and
// String for IndexedVars.
type IndexedVarContainer interface {
	IndexedVarEval(idx int, ctx *EvalContext) (Datum, error)
	IndexedVarReturnType(idx int) Datum
	IndexedVarString(idx int) string
}

// IndexedVar is a VariableExpr that can be used as a leaf in expressions; it
// represents a dynamic value. It defers calls to TypeCheck, Eval, String to an
// IndexedVarContainer.
type IndexedVar struct {
	Idx       int
	container IndexedVarContainer
}

var _ TypedExpr = &IndexedVar{}
var _ VariableExpr = &IndexedVar{}

// Variable is a dummy function part of the VariableExpr interface.
func (*IndexedVar) Variable() {}

// Walk is part of the Expr interface.
func (v *IndexedVar) Walk(_ Visitor) Expr {
	return v
}

// TypeCheck is part of the Expr interface.
func (v *IndexedVar) TypeCheck(_ *SemaContext, desired Datum) (TypedExpr, error) {
	return v, nil
}

// Eval is part of the TypedExpr interface.
func (v *IndexedVar) Eval(ctx *EvalContext) (Datum, error) {
	return v.container.IndexedVarEval(v.Idx, ctx)
}

// ReturnType is part of the TypedExpr interface.
func (v *IndexedVar) ReturnType() Datum {
	return v.container.IndexedVarReturnType(v.Idx)
}

// Format implements the NodeFormatter interface.
func (v *IndexedVar) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(v.container.IndexedVarString(v.Idx))
}

// IndexedVarHelper is a structure that helps with initialization of IndexVars.
type IndexedVarHelper struct {
	vars      []IndexedVar
	container IndexedVarContainer
}

// MakeIndexedVarHelper initializes an IndexedVarHelper structure.
func MakeIndexedVarHelper(container IndexedVarContainer, numVars int) IndexedVarHelper {
	return IndexedVarHelper{vars: make([]IndexedVar, numVars), container: container}
}

func (h *IndexedVarHelper) checkIndex(idx int) {
	if idx < 0 || idx >= len(h.vars) {
		panic(fmt.Sprintf("invalid var index %d (columns: %d)", idx, len(h.vars)))
	}
}

// NumVars returns the number of variables the IndexedVarHelper was initialized
// for.
func (h *IndexedVarHelper) NumVars() int {
	return len(h.vars)
}

// IndexedVar returns an IndexedVar for the given index. The index must be
// valid.
func (h *IndexedVarHelper) IndexedVar(idx int) *IndexedVar {
	h.checkIndex(idx)
	v := &h.vars[idx]
	if v.container == nil {
		v.Idx = idx
		v.container = h.container
	}
	return v
}

// IndexedVarUsed returns true if IndexedVar() was called for the given index.
// The index must be valid.
func (h *IndexedVarHelper) IndexedVarUsed(idx int) bool {
	h.checkIndex(idx)
	return h.vars[idx].container != nil
}
