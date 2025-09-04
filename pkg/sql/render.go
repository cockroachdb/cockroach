// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// renderNode encapsulates the render logic of a select statement:
// expressing new values using expressions over source values.
type renderNode struct {
	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	_ util.NoCopy

	singleInputPlanNode
	nonReusablePlanNode

	// Rendering expressions for rows and corresponding output columns.
	render []tree.TypedExpr

	// columns is the set of result columns.
	columns colinfo.ResultColumns

	// if set, join any distributed streams before applying the rendering; used to
	// "materialize" an ordering before removing the ordering columns (at the root
	// of a query or subquery).
	serialize bool

	reqOrdering ReqOrdering
}

var _ tree.IndexedVarContainer = &renderNode{}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (r *renderNode) IndexedVarResolvedType(idx int) *types.T {
	return r.columns[idx].Typ
}

func (r *renderNode) startExec(runParams) error {
	panic("renderNode can't be run in local mode")
}

func (r *renderNode) Next(params runParams) (bool, error) {
	panic("renderNode can't be run in local mode")
}

func (r *renderNode) Values() tree.Datums {
	panic("renderNode can't be run in local mode")
}

func (r *renderNode) Close(ctx context.Context) { r.input.Close(ctx) }
