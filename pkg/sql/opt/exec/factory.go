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

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Node represents a node in the execution tree (currently maps to a
// sql.planNode).
type Node interface{}

// Factory defines the interface for building an execution plan, which consists
// of a tree of execution nodes (currently a sql.planNode tree).
//
// The tree is always built bottom-up. The Construct methods either construct
// leaf nodes, or they take other nodes previously constructed by this same
// factory as children.
//
// The TypedExprs passed to these functions refer to columns of the input node
// via IndexedVars.
type Factory interface {
	// ConstructValues returns a node that outputs the given rows as results.
	ConstructValues(rows [][]tree.TypedExpr, cols sqlbase.ResultColumns) (Node, error)

	// ConstructScan returns a node that represents a scan of the given index on
	// the given table.
	//   - Only the given set of needed columns are part of the result.
	//   - If indexConstraint is not nil, the scan is restricted to the spans in
	//     in the constraint.
	//   - If hardLimit > 0, then only up to hardLimit rows can be returned from
	//     the scan.
	ConstructScan(
		table opt.Table,
		index opt.Index,
		needed ColumnOrdinalSet,
		indexConstraint *constraint.Constraint,
		hardLimit int64,
	) (Node, error)

	// ConstructFilter returns a node that applies a filter on the results of
	// the given input node.
	ConstructFilter(n Node, filter tree.TypedExpr) (Node, error)

	// ConstructSimpleProject returns a node that applies a "simple" projection on the
	// results of the given input node. A simple projection is one that does not
	// involve new expressions; it's just a reshuffling of columns. This is a
	// more efficient version of ConstructRender.
	// The colNames argument is optional; if it is nil, the names of the
	// corresponding input columns are kept.
	ConstructSimpleProject(n Node, cols []ColumnOrdinal, colNames []string) (Node, error)

	// ConstructRender returns a node that applies a projection on the results of
	// the given input node. The projection can contain new expressions.
	ConstructRender(n Node, exprs tree.TypedExprs, colNames []string) (Node, error)

	// ConstructJoin returns a node that runs a hash-join between the results
	// of two input nodes. The expression can refer to columns from both inputs
	// using IndexedVars (first the left columns, then the right columns).
	ConstructJoin(joinType sqlbase.JoinType, left, right Node, onCond tree.TypedExpr) (Node, error)

	// ConstructGroupBy returns a node that runs an aggregation. If group columns
	// are specified, a set of aggregations is performed for each group of values
	// on those columns (otherwise there is a single group).
	ConstructGroupBy(input Node, groupCols []ColumnOrdinal, aggregations []AggInfo) (Node, error)

	// ConstructSetOp returns a node that performs a UNION / INTERSECT / EXCEPT
	// operation (either the ALL or the DISTINCT version). The left and right
	// nodes must have the same number of columns.
	ConstructSetOp(typ tree.UnionType, all bool, left, right Node) (Node, error)

	// ConstructSort returns a node that performs a resorting of the rows produced
	// by the input node.
	ConstructSort(input Node, ordering sqlbase.ColumnOrdering) (Node, error)

	// ConstructLimit returns a node that implements LIMIT and/or OFFSET on the
	// results of the given node. If only an offset is desired, limit should be
	// math.MaxInt64.
	ConstructLimit(input Node, limit int64, offset int64) (Node, error)

	// RenameColumns modifies the column names of a node.
	RenameColumns(input Node, colNames []string) (Node, error)
}

// ColumnOrdinal is the 0-based ordinal index of a column produced by a Node.
type ColumnOrdinal int32

// ColumnOrdinalSet contains a set of ColumnOrdinal values as ints.
type ColumnOrdinalSet = util.FastIntSet

// AggInfo represents an aggregation (see ConstructGroupBy).
type AggInfo struct {
	FuncName   string
	Builtin    *tree.Builtin
	ResultType types.T
	ArgCols    []ColumnOrdinal
}
