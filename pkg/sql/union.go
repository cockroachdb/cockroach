// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// unionNode is a planNode whose rows are the result of one of three set
// operations (UNION, INTERSECT, or EXCEPT) on left and right. There are two
// variations of each set operation: distinct, which always returns unique
// results, and all, which does no de-duplication.
type unionNode struct {
	setOpPlanningInfo

	// right and left are the data source operands.
	right, left planNode

	// columns contains the metadata for the results of this node.
	columns colinfo.ResultColumns
}

type setOpPlanningInfo struct {
	// unionType is the type of operation (UNION, INTERSECT, EXCEPT)
	unionType tree.UnionType

	// all indicates if the operation is the ALL or DISTINCT version
	all bool

	// enforceHomeRegion is true if this UNION ALL is a locality-optimized search
	// and the session setting `enforce_home_region` is true, indicating the
	// operation should error out if the query cannot be satisfied by accessing
	// only rows in the local region. The left branch of the UNION ALL is set up
	// to only read rows in the local region when this flag is true, and the
	// right branch reads rows in remote regions.
	enforceHomeRegion bool

	// leftOrdering and rightOrdering are the orderings of the left and right
	// inputs, respectively.
	leftOrdering, rightOrdering colinfo.ColumnOrdering

	// streamingOrdering specifies the ordering on both inputs in terms of the
	// output columns they map to. If not empty, all columns must be included in
	// this ordering.
	streamingOrdering colinfo.ColumnOrdering

	// reqOrdering specifies the required output ordering. If not empty, both
	// inputs are already ordered according to streamingOrdering, and reqOrdering
	// is a prefix of streamingOrdering except for optional columns, which do not
	// affect the ordering.
	reqOrdering ReqOrdering

	// hardLimit can only be set for UNION ALL operations. It is used to implement
	// locality optimized search, and instructs the execution engine that it
	// should execute the left node to completion and possibly short-circuit if
	// the limit is reached before executing the right node. The limit is
	// guaranteed but the short-circuit behavior is not.
	hardLimit uint64

	// finalizeLastStageCb will be nil in the spec factory.
	finalizeLastStageCb func(*physicalplan.PhysicalPlan)
}

func (p *planner) newUnionNode(
	typ tree.UnionType,
	all bool,
	left, right planNode,
	leftOrdering, rightOrdering, streamingOrdering colinfo.ColumnOrdering,
	reqOrdering ReqOrdering,
	hardLimit uint64,
	enforceHomeRegion bool,
) (planNode, error) {
	switch typ {
	case tree.UnionOp, tree.IntersectOp, tree.ExceptOp:
	default:
		return nil, errors.Errorf("%v is not supported", typ)
	}
	resultCols, err := getSetOpResultColumns(typ, planColumns(left), planColumns(right))
	if err != nil {
		return nil, err
	}

	node := &unionNode{
		right:   right,
		left:    left,
		columns: resultCols,
		setOpPlanningInfo: setOpPlanningInfo{
			unionType:         typ,
			all:               all,
			leftOrdering:      leftOrdering,
			rightOrdering:     rightOrdering,
			streamingOrdering: streamingOrdering,
			reqOrdering:       reqOrdering,
			hardLimit:         hardLimit,
			enforceHomeRegion: enforceHomeRegion,
		},
	}
	return node, nil
}

func getSetOpResultColumns(
	typ tree.UnionType, leftColumns, rightColumns colinfo.ResultColumns,
) (colinfo.ResultColumns, error) {
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
	return unionColumns, nil
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

func (n *unionNode) InputCount() int {
	return 2
}

func (n *unionNode) Input(i int) (planNode, error) {
	switch i {
	case 0:
		return n.right, nil
	case 1:
		return n.left, nil
	default:
		return nil, errors.AssertionFailedf("input index %d is out of range", i)
	}
}
