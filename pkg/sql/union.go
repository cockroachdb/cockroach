// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// unionNode is a planNode whose rows are the result of one of three set
// operations (UNION, INTERSECT, or EXCEPT) on left and right. There are two
// variations of each set operation: distinct, which always returns unique
// results, and all, which does no uniqueing.
//
// Ordering of rows is expected to be handled externally to unionNode.
// TODO(dan): In the long run, this is insufficient. If we know both left and
// right are ordered the same way, we can do the set logic without the map
// state. Additionally, if the unionNode has an ordering then we can hint it
// down to left and right and force the condition for this first optimization.
//
// All six of the operations can be completed without cacheing rows by
// iterating one side then the other and keeping counts of unique rows
// in a map. The logic is common for all six. However, because EXCEPT
// needs to iterate the right side first, the common code always reads
// the right operand first. Meanwhile, we invert the operands for the
// non-EXCEPT cases in order to preserve the appearance of the
// original specified order.
//
// The emit logic for each op is represented by implementors of the
// unionNodeEmit interface. The emitRight method is called for each row output
// by the right side and passed a hashable representation of the row. If it
// returns true, the row is emitted. After all right rows are examined, then
// each left row is passed to emitLeft in the same way.
//
// An example: intersectNodeEmitAll
// VALUES (1), (1), (1), (2), (2) INTERSECT ALL VALUES (1), (3), (1)
// ----
// 1
// 1
// There are three 1s on the left and two 1s on the right, so we emit 1, 1.
// Nothing else is in both.
//  emitRight: For each row, increment the map entry.
//  emitLeft: For each row, if the row is not present in the map, it was not in
//    both, don't emit. Otherwise, if the count for the row was > 0, emit and
//    decrement the entry. Otherwise, the row was on the right, but we've
//    already emitted as many as were on the right, don't emit.
type unionNode struct {
	// right and left are the data source operands.
	// right is read first, to populate the `emit` field.
	right, left planNode

	// columns contains the metadata for the results of this node.
	columns colinfo.ResultColumns
	// inverted, when true, indicates that the right plan corresponds to
	// the left operand in the input SQL syntax, and vice-versa.
	inverted bool
	// emitAll is a performance optimization for UNION ALL. When set
	// the union logic avoids the `emit` logic entirely.
	emitAll bool

	// unionType is the type of operation (UNION, INTERSECT, EXCEPT)
	unionType tree.UnionType
	// all indicates if the operation is the ALL or DISTINCT version
	all bool

	// streamingOrdering specifies the ordering on both inputs. If not empty, all
	// columns must be included in this ordering.
	streamingOrdering colinfo.ColumnOrdering

	// reqOrdering specifies the required output ordering. If not empty, both
	// inputs are already ordered according to streamingOrdering, and reqOrdering
	// is a prefix of streamingOrdering.
	reqOrdering ReqOrdering

	// hardLimit can only be set for UNION ALL operations. It is used to implement
	// locality optimized search, and instructs the execution engine that it
	// should execute the left node to completion and possibly short-circuit if
	// the limit is reached before executing the right node. The limit is
	// guaranteed but the short-circuit behavior is not.
	hardLimit uint64
}

func (p *planner) newUnionNode(
	typ tree.UnionType,
	all bool,
	left, right planNode,
	streamingOrdering colinfo.ColumnOrdering,
	reqOrdering ReqOrdering,
	hardLimit uint64,
) (planNode, error) {
	emitAll := false
	switch typ {
	case tree.UnionOp:
		if all {
			emitAll = true
		}
	case tree.IntersectOp:
	case tree.ExceptOp:
	default:
		return nil, errors.Errorf("%v is not supported", typ)
	}

	leftColumns := planColumns(left)
	rightColumns := planColumns(right)
	if len(leftColumns) != len(rightColumns) {
		return nil, pgerror.Newf(
			pgcode.Syntax,
			"each %v query must have the same number of columns: %d vs %d",
			typ, len(leftColumns), len(rightColumns),
		)
	}
	unionColumns := append(colinfo.ResultColumns(nil), leftColumns...)
	for i := 0; i < len(unionColumns); i++ {
		l := leftColumns[i]
		r := rightColumns[i]
		// TODO(dan): This currently checks whether the types are exactly the same,
		// but Postgres is more lenient:
		// http://www.postgresql.org/docs/9.5/static/typeconv-union-case.html.
		if !(l.Typ.Equivalent(r.Typ) || l.Typ.Family() == types.UnknownFamily || r.Typ.Family() == types.UnknownFamily) {
			return nil, pgerror.Newf(pgcode.DatatypeMismatch,
				"%v types %s and %s cannot be matched", typ, l.Typ, r.Typ)
		}
		if l.Typ.Family() == types.UnknownFamily {
			unionColumns[i].Typ = r.Typ
		}
	}

	inverted := false
	if typ != tree.ExceptOp {
		// The logic below reads the rows from the right operand first,
		// because for EXCEPT in particular this is what we need to match.
		// However for the other operators (UNION, INTERSECT) it is
		// actually confusing to see the right values come up first in the
		// results. So invert this here, to reduce surprise by users.
		left, right = right, left
		inverted = true
	}

	node := &unionNode{
		right:             right,
		left:              left,
		columns:           unionColumns,
		inverted:          inverted,
		emitAll:           emitAll,
		unionType:         typ,
		all:               all,
		streamingOrdering: streamingOrdering,
		reqOrdering:       reqOrdering,
		hardLimit:         hardLimit,
	}
	return node, nil
}

func (n *unionNode) startExec(params runParams) error {
	panic("unionNode cannot be run in local mode")
}

func (n *unionNode) Next(params runParams) (bool, error) {
	panic("unionNode cannot be run in local mode")
}

func (n *unionNode) Values() tree.Datums {
	panic("unionNode cannot be run in local mode")
}

func (n *unionNode) Close(ctx context.Context) {
	n.right.Close(ctx)
	n.left.Close(ctx)
}
