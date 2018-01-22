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

package opt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
)

// ExecNode represents a node in the execution tree (currently maps to a
// sql.planNode).
type ExecNode interface {
	// Run() executes the plan and returns the results as a Datum table.
	Run() ([]tree.Datums, error)

	// Explain() executes EXPLAIN (VERBOSE) on the given plan and returns the
	// results as a Datum table.
	Explain() ([]tree.Datums, error)
}

// ExecFactory is an interface used by the opt package to build
// an execution plan (currently a sql.planNode tree).
type ExecFactory interface {
	// ConstructScan returns an ExecNode that represents a scan of the given
	// table.
	// TODO(radu): support list of columns, index, index constraints
	ConstructScan(table optbase.Table) (ExecNode, error)

	// ConstructFilter returns an ExecNode that applies a filter on the results
	// of the given ExecNode.
	ConstructFilter(n ExecNode, filter tree.TypedExpr) (ExecNode, error)

	// Close cleans up any state associated with the factory. Any
	// created ExecNodes must not be used after calling this function.
	Close()
}

// execNodeSchema describes the schema for the results of
// a plan (ExecNode).
//
// The reason we need to keep track of this (instead of using just the
// relational properties) is that the relational properties don't force a single
// "schema": any ordering of the columns in relationalProps.outputCols is
// possible. We choose the schema that is most convenient: for scans, we use the
// table's column ordering. Consider:
//   SELECT a, b FROM t WHERE a = b
// and the following two cases:
//   1. The table is defined as (k INT PRIMARY KEY, a INT, b INT). The scan will
//      return (k, a, b).
//   2. The table is defined as (k INT PRIMARY KEY, b INT, a INT). The scan will
//      return (k, b, a).
// In these two cases, the relational properties are effectively the same.
//
// An alternative to this would be to always use a "canonical" schema, for
// example the columns in relationalProps.outputCols in increasing index order.
// This would require a lot of otherwise unnecessary projections.
type execNodeSchema struct {
	// outputCols maps columns in the output set of the relational expression to
	// indices in the result columns of the ExecNode.
	outputCols  columnMap
	outputTypes []types.T
}

func getSchema(props *relationalProps) execNodeSchema {
	s := execNodeSchema{
		outputTypes: make([]types.T, 0, props.outputCols.Len()),
	}

	for i := range props.columns {
		if props.outputCols.Contains(props.columns[i].index) {
			s.outputCols.Set(props.columns[i].index, i)
			s.outputTypes = append(s.outputTypes, props.columns[i].typ)
		}
	}
	return s
}

// execPlan is an execution plan associated with a relational expression.
type execPlan struct {
	// root node of the plan tree
	root ExecNode

	schema execNodeSchema
}

// makeExec uses an ExecFactory to build an execution tree.
func makeExec(e *Expr, bld ExecFactory) (execPlan, error) {
	var plan execPlan
	var err error
	switch e.op {
	case scanOp:
		plan.schema = getSchema(e.relProps)
		plan.root, err = bld.ConstructScan(e.private.(optbase.Table))

	case selectOp:
		var underlying execPlan
		underlying, err = makeExec(e.inputs()[0], bld)
		if err != nil {
			break
		}
		plan.schema = underlying.schema
		filter := makeFilter(e, underlying.schema)
		plan.root, err = bld.ConstructFilter(underlying.root, filter)

	default:
		err = errors.Errorf("unsupported op %s", e.op)
	}
	if err != nil {
		return execPlan{}, err
	}
	return plan, nil
}

// makeFilter creates a TypedExpr that corresponds to the filters of a selectOp.
func makeFilter(e *Expr, schema execNodeSchema) tree.TypedExpr {
	if e.op != selectOp {
		panic(fmt.Sprintf("invalid op %s", e.op))
	}
	ivh := tree.MakeTypesOnlyIndexedVarHelper(schema.outputTypes)
	c := typedExprConvCtx{
		ivh:             &ivh,
		varToIndexedVar: schema.outputCols,
	}

	var res tree.TypedExpr
	for _, f := range e.filters() {
		expr := scalarToTypedExpr(&c, f)
		if res == nil {
			res = expr
		} else {
			res = tree.NewTypedAndExpr(res, expr)
		}
	}
	return res
}
