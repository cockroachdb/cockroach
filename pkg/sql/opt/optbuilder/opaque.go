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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// BuildOpaqueFn is a handler for building the metadata for an opaque statement.
type BuildOpaqueFn func(
	context.Context, *tree.SemaContext, *tree.EvalContext, tree.Statement,
) (opt.OpaqueMetadata, sqlbase.ResultColumns, error)

// RegisterOpaque registers an opaque handler for a specific statement type.
func RegisterOpaque(stmtType reflect.Type, fn BuildOpaqueFn) {
	opaqueStatements[stmtType] = fn
}

var opaqueStatements = make(map[reflect.Type]BuildOpaqueFn)

func (b *Builder) tryBuildOpaque(stmt tree.Statement, inScope *scope) (outScope *scope) {
	fn, ok := opaqueStatements[reflect.TypeOf(stmt)]
	if !ok {
		return nil
	}
	obj, cols, err := fn(b.ctx, b.semaCtx, b.evalCtx, stmt)
	if err != nil {
		panic(err)
	}
	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, cols)
	outScope.expr = b.factory.ConstructOpaqueRel(&memo.OpaqueRelPrivate{
		Columns:  colsToColList(outScope.cols),
		Metadata: obj,
	})
	return outScope
}
