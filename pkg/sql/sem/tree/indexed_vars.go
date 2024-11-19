// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// IndexedVarContainer provides type-checking for an IndexedVar.
type IndexedVarContainer interface {
	IndexedVarResolvedType(idx int) *types.T
}

// IndexedVar is a VariableExpr that can be used as a leaf in expressions; it
// represents a dynamic value. It defers calls to TypeCheck and Format to an
// IndexedVarContainer.
type IndexedVar struct {
	Idx int
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
	if semaCtx.IVarContainer == nil {
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

// ResolvedType is part of the TypedExpr interface.
func (v *IndexedVar) ResolvedType() *types.T {
	if v.typ == nil {
		panic(errors.AssertionFailedf("indexed var must be type checked first"))
	}
	return v.typ
}

// Format implements the NodeFormatter interface.
func (v *IndexedVar) Format(ctx *FmtCtx) {
	if ctx.indexedVarFormat != nil {
		ctx.indexedVarFormat(ctx, v.Idx)
	} else {
		ctx.Printf("@%d", v.Idx+1)
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
	v.typ = h.container.IndexedVarResolvedType(idx)
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
	v.typ = typ
	return v
}

type typeContainer struct {
	types []*types.T
}

var _ IndexedVarContainer = &typeContainer{}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (tc *typeContainer) IndexedVarResolvedType(idx int) *types.T {
	return tc.types[idx]
}

// MakeIndexedVarHelperWithTypes creates an IndexedVarHelper which provides
// the given types for indexed vars.
func MakeIndexedVarHelperWithTypes(types []*types.T) IndexedVarHelper {
	c := &typeContainer{types: types}
	return MakeIndexedVarHelper(c, len(types))
}
