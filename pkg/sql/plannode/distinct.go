// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// distinctNode de-duplicates rows returned by a wrapped planNode.
type DistinctNode struct {
	singleInputPlanNode

	// DistinctOnColIdxs are the column indices of the child planNode and
	// is what defines the distinct key.
	// For a normal DISTINCT (without the ON clause), DistinctOnColIdxs
	// contains all the column indices of the child planNode.
	// Otherwise, DistinctOnColIdxs is a strict subset of the child
	// planNode's column indices indicating which columns are specified in
	// the DISTINCT ON (<exprs>) clause.
	DistinctOnColIdxs intsets.Fast

	// Subset of DistinctOnColIdxs on which the input guarantees an ordering.
	// All rows that are equal on these columns appear contiguously in the input.
	ColumnsInOrder intsets.Fast

	ReqOrdering ReqOrdering

	// NullsAreDistinct, if true, causes the distinct operation to treat NULL
	// values as not equal to one another. Each NULL value will cause a new row
	// group to be created. For example:
	//
	//   c
	//   ----
	//   NULL
	//   NULL
	//
	// A distinct operation on column "c" will result in one output row if
	// NullsAreDistinct is false, or two output rows if true. This is set to true
	// for UPSERT and INSERT..ON CONFLICT statements, since they must treat NULL
	// values as distinct.
	NullsAreDistinct bool

	// ErrorOnDup, if non-empty, is the text of the error that will be raised if
	// the distinct operation finds two rows with duplicate grouping column
	// values. This is used to implement the UPSERT and INSERT..ON CONFLICT
	// statements, both of which prohibit the same row from being changed twice.
	ErrorOnDup string
}

func (n *distinctNode) StartExec(params runParams) error {
	panic("distinctNode can't be called in local mode")
}

func (n *distinctNode) Next(params runParams) (bool, error) {
	panic("distinctNode can't be called in local mode")
}

func (n *distinctNode) Values() tree.Datums {
	panic("distinctNode can't be called in local mode")
}

func (n *distinctNode) Close(ctx context.Context) {
	n.Source.Close(ctx)
}



// Lowercase alias for backward compatibility
type distinctNode = DistinctNode
