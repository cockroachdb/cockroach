// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// IndexedVarContainer provides the implementation of TypeCheck, Eval, and
// String for IndexedVars.
type IndexedVarContainer interface {
	IndexedVarEval(idx int, ctx *EvalContext) (Datum, error)
	IndexedVarResolvedType(idx int) *types.T
	// IndexedVarNodeFormatter returns a NodeFormatter; if an object that
	// wishes to implement this interface has lost the textual name that an
	// IndexedVar originates from, this function can return nil (and the
	// ordinal syntax "@1, @2, .." will be used).
	IndexedVarNodeFormatter(idx int) NodeFormatter
}

// IndexedVar is a VariableExpr that can be used as a leaf in expressions; it
// represents a dynamic value. It defers calls to TypeCheck, Eval, String to an
// IndexedVarContainer.
type IndexedVar struct {
	Idx  int
	Used bool

	col NodeFormatter

	typeAnnotation
}

var _ TypedExpr = &IndexedVar{}

// Variable is a dummy function part of the VariableExpr interface.
func (*IndexedVar) Variable() {}

// Walk is part of the Expr interface.
func (v *IndexedVar) Walk(_ Visitor) Expr {
	return v
}

// TypeCheck is part of the Expr interface.
func (v *IndexedVar) TypeCheck(
	_ context.Context, semaCtx *SemaContext, desired *types.T,
) (TypedExpr, error) {
	if semaCtx.IVarContainer == nil || semaCtx.IVarContainer == unboundContainer {
		// A more technically correct message would be to say that the
		// reference is unbound and thus cannot be typed. However this is
		// a tad bit too technical for the average SQL use case and
		// instead we acknowledge that we only get here if someone has
		// used a column reference in a place where it's not allowed by
		// the docs, so just say that instead.
		return nil, pgerror.Newf(
			pgcode.UndefinedColumn, "column reference @%d not allowed in this context", v.Idx+1)
	}
	v.typ = semaCtx.IVarContainer.IndexedVarResolvedType(v.Idx)
	return v, nil
}

// Eval is part of the TypedExpr interface.
func (v *IndexedVar) Eval(ctx *EvalContext) (Datum, error) {
	if ctx.IVarContainer == nil || ctx.IVarContainer == unboundContainer {
		return nil, errors.AssertionFailedf(
			"indexed var must be bound to a container before evaluation")
	}
	return ctx.IVarContainer.IndexedVarEval(v.Idx, ctx)
}

// ResolvedType is part of the TypedExpr interface.
func (v *IndexedVar) ResolvedType() *types.T {
	if v.typ == nil {
		panic(errors.AssertionFailedf("indexed var must be type checked first"))
	}
	return v.typ
}

// Format implements the NodeFormatter interface.
func (v *IndexedVar) Format(ctx *FmtCtx) {
	f := ctx.flags
	if ctx.indexedVarFormat != nil {
		ctx.indexedVarFormat(ctx, v.Idx)
	} else if f.HasFlags(fmtSymbolicVars) || v.col == nil {
		ctx.Printf("@%d", v.Idx+1)
	} else {
		ctx.FormatNode(v.col)
	}
}

// NewOrdinalReference is a helper routine to create a standalone
// IndexedVar with the given index value. This needs to undergo
// BindIfUnbound() below before it can be fully used.
func NewOrdinalReference(r int) *IndexedVar {
	return &IndexedVar{Idx: r}
}

// NewTypedOrdinalReference returns a new IndexedVar with the given index value
// that is verified to be well-typed.
func NewTypedOrdinalReference(r int, typ *types.T) *IndexedVar {
	return &IndexedVar{Idx: r, typeAnnotation: typeAnnotation{typ: typ}}
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

// Container returns the container associated with the helper.
func (h *IndexedVarHelper) Container() IndexedVarContainer {
	return h.container
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
		return ivar, pgerror.Newf(
			pgcode.UndefinedColumn, "invalid column ordinal: @%d", ivar.Idx+1)
	}

	if !ivar.Used {
		return h.IndexedVar(ivar.Idx), nil
	}
	return ivar, nil
}

// MakeIndexedVarHelper initializes an IndexedVarHelper structure.
func MakeIndexedVarHelper(container IndexedVarContainer, numVars int) IndexedVarHelper {
	return IndexedVarHelper{
		vars:      make([]IndexedVar, numVars),
		container: container,
	}
}

// AppendSlot expands the capacity of this IndexedVarHelper by one and returns
// the index of the new slot.
func (h *IndexedVarHelper) AppendSlot() int {
	h.vars = append(h.vars, IndexedVar{})
	return len(h.vars) - 1
}

func (h *IndexedVarHelper) checkIndex(idx int) {
	if idx < 0 || idx >= len(h.vars) {
		panic(errors.AssertionFailedf(
			"invalid var index %d (columns: %d)", redact.Safe(idx), redact.Safe(len(h.vars))))
	}
}

// IndexedVar returns an IndexedVar for the given index. The index must be
// valid.
func (h *IndexedVarHelper) IndexedVar(idx int) *IndexedVar {
	h.checkIndex(idx)
	v := &h.vars[idx]
	v.Idx = idx
	v.Used = true
	v.typ = h.container.IndexedVarResolvedType(idx)
	v.col = h.container.IndexedVarNodeFormatter(idx)
	return v
}

// IndexedVarWithType returns an IndexedVar for the given index, with the given
// type. The index must be valid. This should be used in the case where an
// indexed var is being added before its container has a corresponding entry
// for it.
func (h *IndexedVarHelper) IndexedVarWithType(idx int, typ *types.T) *IndexedVar {
	h.checkIndex(idx)
	v := &h.vars[idx]
	v.Idx = idx
	v.Used = true
	v.typ = typ
	return v
}

// IndexedVarUsed returns true if IndexedVar() was called for the given index.
// The index must be valid.
func (h *IndexedVarHelper) IndexedVarUsed(idx int) bool {
	h.checkIndex(idx)
	return h.vars[idx].Used
}

// GetIndexedVars returns the indexed var array of this helper.
// IndexedVars to the caller; unused vars are guaranteed to have
// a false Used field.
func (h *IndexedVarHelper) GetIndexedVars() []IndexedVar {
	return h.vars
}

// Rebind collects all the IndexedVars in the given expression and re-binds them
// to this helper.
func (h *IndexedVarHelper) Rebind(expr TypedExpr) TypedExpr {
	if expr == nil {
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

// IndexedVarEval is part of the IndexedVarContainer interface.
func (*unboundContainerType) IndexedVarEval(idx int, _ *EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("unbound ordinal reference @%d", redact.Safe(idx+1))
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (*unboundContainerType) IndexedVarResolvedType(idx int) *types.T {
	panic(errors.AssertionFailedf("unbound ordinal reference @%d", redact.Safe(idx+1)))
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (*unboundContainerType) IndexedVarNodeFormatter(idx int) NodeFormatter {
	panic(errors.AssertionFailedf("unbound ordinal reference @%d", redact.Safe(idx+1)))
}

type typeContainer struct {
	types []*types.T
}

var _ IndexedVarContainer = &typeContainer{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (tc *typeContainer) IndexedVarEval(idx int, ctx *EvalContext) (Datum, error) {
	return nil, errors.AssertionFailedf("no eval allowed in typeContainer")
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (tc *typeContainer) IndexedVarResolvedType(idx int) *types.T {
	return tc.types[idx]
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (tc *typeContainer) IndexedVarNodeFormatter(idx int) NodeFormatter {
	return nil
}

// MakeTypesOnlyIndexedVarHelper creates an IndexedVarHelper which provides
// the given types for indexed vars. It does not support evaluation, unless
// Rebind is used with another container which supports evaluation.
func MakeTypesOnlyIndexedVarHelper(types []*types.T) IndexedVarHelper {
	c := &typeContainer{types: types}
	return MakeIndexedVarHelper(c, len(types))
}
