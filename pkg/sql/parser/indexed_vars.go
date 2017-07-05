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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/pkg/errors"
)

// IndexedVarContainer provides the implementation of TypeCheck, Eval, and
// String for IndexedVars.
// If an object that wishes to implement this interface has lost the
// textual name that an IndexedVar originates from, it can use the
// ordinal column reference syntax: fmt.Fprintf(buf, "@%d", idx)
type IndexedVarContainer interface {
	IndexedVarEval(idx int, ctx *EvalContext) (Datum, error)
	IndexedVarResolvedType(idx int) Type
	IndexedVarFormat(buf *bytes.Buffer, f FmtFlags, idx int)
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
func (v *IndexedVar) TypeCheck(_ *SemaContext, desired Type) (TypedExpr, error) {
	if v.container == nil || v.container == unboundContainer {
		// A more technically correct message would be to say that the
		// reference is unbound and thus cannot be typed. However this is
		// a tad bit too technical for the average SQL use case and
		// instead we acknowledge that we only get here if someone has
		// used a column reference in a place where it's not allowed by
		// the docs, so just say that instead.
		return nil, errors.Errorf("column reference @%d not allowed in this context", v.Idx+1)
	}
	return v, nil
}

// Eval is part of the TypedExpr interface.
func (v *IndexedVar) Eval(ctx *EvalContext) (Datum, error) {
	if v.container == nil || v.container == unboundContainer {
		panic("indexed var must be bound to a container before evaluation")
	}
	return v.container.IndexedVarEval(v.Idx, ctx)
}

// ResolvedType is part of the TypedExpr interface.
func (v *IndexedVar) ResolvedType() Type {
	if v.container == nil || v.container == unboundContainer {
		panic("indexed var must be bound to a container before type resolution")
	}
	return v.container.IndexedVarResolvedType(v.Idx)
}

// Format implements the NodeFormatter interface.
func (v *IndexedVar) Format(buf *bytes.Buffer, f FmtFlags) {
	if f.indexedVarFormat != nil {
		f.indexedVarFormat(buf, f, v.container, v.Idx)
	} else if f.symbolicVars || v.container == nil || v.container == unboundContainer {
		fmt.Fprintf(buf, "@%d", v.Idx+1)
	} else {
		v.container.IndexedVarFormat(buf, f, v.Idx)
	}
}

// NewOrdinalReference is a helper routine to create a standalone
// IndexedVar with the given index value. This needs to undergo
// BindIfUnbound() below before it can be fully used.
func NewOrdinalReference(r int) *IndexedVar {
	return &IndexedVar{Idx: r, container: unboundContainer}
}

// NewIndexedVar is a helper routine to create a standalone Indexedvar
// with the given index value. This needs to undergo BindIfUnbound()
// below before it can be fully used. The difference with ordinal
// references is that vars returned by this constructor are modified
// in-place by BindIfUnbound.
//
// Do not use NewIndexedVar for AST nodes that can undergo binding two
// or more times.
func NewIndexedVar(r int) *IndexedVar {
	return &IndexedVar{Idx: r, container: nil}
}

// IndexedVarHelper wraps an IndexedVarContainer (an interface) and creates
// IndexedVars bound to that container.
//
// It also keeps track of which indexes from the container are used by
// expressions.
type IndexedVarHelper struct {
	vars      []IndexedVar
	container IndexedVarContainer
}

// BindIfUnbound ensures the IndexedVar is attached to this helper's container.
// - for freshly created IndexedVars (with a nil container) this will bind in-place.
// - for already bound IndexedVar, bound to this container, this will return the same ivar unchanged.
// - for ordinal references (with an explicit unboundContainer) this will return a new var.
// - for already bound IndexedVars, bound to another container, this will error out.
func (h *IndexedVarHelper) BindIfUnbound(ivar *IndexedVar) (*IndexedVar, error) {
	// We perform the range check always, even if the ivar is already
	// bound, as a form of safety assertion against misreuse of ivars
	// across containers.
	if ivar.Idx < 0 || ivar.Idx >= len(h.vars) {
		return ivar, errors.Errorf("invalid column ordinal: @%d", ivar.Idx+1)
	}

	if ivar.container == nil {
		// This container must also remember it has "seen" the variable
		// so that IndexedVarUsed() below returns the right results.
		// The IndexedVar() method ensures this.
		*ivar = *h.IndexedVar(ivar.Idx)
		return ivar, nil
	}
	if ivar.container == unboundContainer {
		return h.IndexedVar(ivar.Idx), nil
	}
	if ivar.container == h.container {
		return ivar, nil
	}
	return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
		"indexed var linked to different container (%T) %+v, expected (%T) %+v",
		ivar.container, ivar.container, h.container, h.container)
}

// MakeIndexedVarHelper initializes an IndexedVarHelper structure.
func MakeIndexedVarHelper(container IndexedVarContainer, numVars int) IndexedVarHelper {
	return IndexedVarHelper{vars: make([]IndexedVar, numVars), container: container}
}

// AppendSlot expands the capacity of this IndexedVarHelper by one and returns
// the index of the new slot.
func (h *IndexedVarHelper) AppendSlot() int {
	h.vars = append(h.vars, IndexedVar{})
	return len(h.vars) - 1
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
	v.Idx = idx
	v.container = h.container
	return v
}

// IndexedVarUsed returns true if IndexedVar() was called for the given index.
// The index must be valid.
func (h *IndexedVarHelper) IndexedVarUsed(idx int) bool {
	h.checkIndex(idx)
	return h.vars[idx].container != nil
}

// InvalidColIdx is the index value of a non-initialized IndexedVar.
const InvalidColIdx = -1

// GetIndexedVars transfers ownership of the array of initialized
// IndexedVars to the caller; unused vars are guaranteed to have an
// invalid index. The helper cannot be used any more after the
// ownership has been transferred.
func (h *IndexedVarHelper) GetIndexedVars() []IndexedVar {
	for i := range h.vars {
		if h.vars[i].container == nil {
			h.vars[i].Idx = InvalidColIdx
		}
	}
	ret := h.vars
	h.vars = nil
	return ret
}

// Reset re-initializes an IndexedVarHelper structure with the same
// number of slots. After a helper has been reset, all the expressions
// that were linked to the helper before it was reset must be
// re-bound, e.g. using Rebind(). Resetting is useful to ensure that
// the helper's knowledge of which IndexedVars are actually used by
// linked expressions is up to date, especially after
// optimizations/transforms which eliminate sub-expressions. The
// optimizations performed by setNeededColumns() work then best.
//
// TODO(knz): groupNode and windowNode hold on to IndexedVar's after a Reset().
func (h *IndexedVarHelper) Reset() {
	h.vars = make([]IndexedVar, len(h.vars))
}

// Rebind collects all the IndexedVars in the given expression
// and re-binds them to this helper.
func (h *IndexedVarHelper) Rebind(expr TypedExpr, alsoReset, normalizeToNonNil bool) TypedExpr {
	if alsoReset {
		h.Reset()
	}
	if expr == nil || expr == DBoolTrue {
		if normalizeToNonNil {
			return DBoolTrue
		}
		return nil
	}
	ret, _ := WalkExpr(h, expr)
	return ret.(TypedExpr)
}

var _ Visitor = &IndexedVarHelper{}

// VisitPre implements the Visitor interface.
func (h *IndexedVarHelper) VisitPre(expr Expr) (recurse bool, newExpr Expr) {
	if iv, ok := expr.(*IndexedVar); ok {
		return false, h.IndexedVar(iv.Idx)
	}
	return true, expr
}

// VisitPost implements the Visitor interface.
func (*IndexedVarHelper) VisitPost(expr Expr) Expr { return expr }

type unboundContainerType struct{}

// unboundContainer is the marker used by ordinal references (@N) in
// the input syntax. It differs from `nil` in that calling
// BindIfUnbound on an indexed var that uses unboundContainer will
// cause a new IndexedVar object to be returned, to ensure that the
// original is left unchanged (to preserve the invariant that the AST
// is constant after parse).
var unboundContainer = &unboundContainerType{}

func (*unboundContainerType) IndexedVarEval(idx int, _ *EvalContext) (Datum, error) {
	panic(fmt.Sprintf("unbound ordinal reference @%d", idx+1))
}

func (*unboundContainerType) IndexedVarResolvedType(idx int) Type {
	panic(fmt.Sprintf("unbound ordinal reference @%d", idx+1))
}

func (*unboundContainerType) IndexedVarFormat(_ *bytes.Buffer, _ FmtFlags, idx int) {
	panic(fmt.Sprintf("unbound ordinal reference @%d", idx+1))
}
