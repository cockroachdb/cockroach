// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// BuildOpaqueFn is a handler for building the metadata for an opaque statement.
type BuildOpaqueFn func(
	context.Context, *tree.SemaContext, *tree.EvalContext, tree.Statement,
) (opt.OpaqueMetadata, error)

// OpaqueType indicates whether an opaque statement can mutate data or change
// schema.
type OpaqueType int

const (
	// OpaqueReadOnly is used for statements that do not mutate state as part of
	// the transaction, and can be run in read-only transactions.
	OpaqueReadOnly OpaqueType = iota

	// OpaqueMutation is used for statements that mutate data and cannot be run as
	// part of read-only transactions.
	OpaqueMutation

	// OpaqueDDL is used for statements that change a schema and cannot be
	// executed following a mutation in the same transaction.
	OpaqueDDL
)

// RegisterOpaque registers an opaque handler for a specific statement type.
func RegisterOpaque(stmtType reflect.Type, opaqueType OpaqueType, fn BuildOpaqueFn) {
	if _, ok := opaqueStatements[stmtType]; ok {
		panic(errors.AssertionFailedf("opaque statement %s already registered", stmtType))
	}
	opaqueStatements[stmtType] = opaqueStmtInfo{
		typ:     opaqueType,
		buildFn: fn,
	}
}

type opaqueStmtInfo struct {
	typ     OpaqueType
	buildFn BuildOpaqueFn
}

var opaqueStatements = make(map[reflect.Type]opaqueStmtInfo)

func (b *Builder) tryBuildOpaque(stmt tree.Statement, inScope *scope) (outScope *scope) {
	info, ok := opaqueStatements[reflect.TypeOf(stmt)]
	if !ok {
		return nil
	}
	obj, err := info.buildFn(b.ctx, b.semaCtx, b.evalCtx, stmt)
	if err != nil {
		panic(err)
	}
	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, obj.Columns())
	private := &memo.OpaqueRelPrivate{
		Columns:  colsToColList(outScope.cols),
		Metadata: obj,
	}
	switch info.typ {
	case OpaqueReadOnly:
		outScope.expr = b.factory.ConstructOpaqueRel(private)
	case OpaqueMutation:
		outScope.expr = b.factory.ConstructOpaqueMutation(private)
	case OpaqueDDL:
		outScope.expr = b.factory.ConstructOpaqueDDL(private)
	default:
		panic(errors.AssertionFailedf("invalid opaque statement type %d", info.typ))
	}
	return outScope
}
