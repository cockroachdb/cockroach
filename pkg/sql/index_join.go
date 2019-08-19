// Copyright 2015 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// indexJoinNode implements joining of results from an index with the rows
// of a table.
//
// There are three parameters to an index join:
// - which index is being used;
// - which table is providing the row values;
// - which filter is applied on the result.
//   From this filter, we can further distinguish:
//   - the index-specific part of the filter, which uses only columns
//     provided by the index; and
//   - the rest of the filter, which uses (at least) non-indexed columns.
//
// The basic operation is as follows:
//
// - at instantiation:
//
//   - the original table scan is replaced by two scanNodes, one for the
//     index and one for the table.
//   - the filter expression is split in a filter-specific part and
//     table-specific part, and propagated to the respective scanNodes.
//
// - during execution:
//
//   - rows from the index scanNode are fetched; this contains
//     both the indexed columns (as pk of the index itself)
//     and the PK of the indexed table.
//   - using the PK of the indexed table, rows from the indexed
//     table are fetched using the table scanNode.
//
//   The work is batched: we pull joinBatchSize rows from the index
//   and use the primary key to construct spans that are looked up in
//   the table.
//
// In addition to this basic operation, we need to support the
// optimization implemented by setNeededColumns() (needed_columns.go)
// which aims to reduce I/O by avoiding the decoding of column data
// when it is not required downstream. This optimization needs to know
// which columns are needed from the index scanNode and which are
// needed from the table scanNode. This is determined as follows:
//
// - from the index scanNode: we need at least the indexed table's PK
//   (for otherwise the table lookup would not be possible), and
//   the columns needed by the index-specific filter.
// - from the table scanNode: we need at least the columns needed by
//   the table-specific filter.
//
// Here the question remains of where to obtain the additional columns
// needed by the downstream consumer node. For any non-indexed
// columns, the table scanNode naturally provides the values. For
// indexed columns, currently the table scanNode also provides the
// values, but really this could be optimized further to re-use the
// column data from the index scanNode instead. See the comment
// for valNeededForCol in scanNode.
type indexJoinNode struct {
	index *scanNode
	table *scanNode

	// primaryKeyColumns is the set of PK columns for which the
	// indexJoinNode requires a value from the index scanNode, to use as
	// lookup keys in the table scanNode. Note that the index scanNode
	// may produce more values than this, e.g. when its filter expression
	// uses more columns than the PK.
	primaryKeyColumns []bool

	// The columns returned by this node. While these are not ever different from
	// the table scanNode in the heuristic planner, the optimizer plans them to
	// be different in some cases.
	cols []sqlbase.ColumnDescriptor
	// There is a 1-1 correspondence between cols and resultColumns.
	resultColumns sqlbase.ResultColumns

	// props contains the physical properties provided by this node.
	// These are pre-computed at construction time.
	props physicalProps

	run indexJoinRun
}

// processIndexJoinColumns returns
// * a slice denoting the columns being returned which are members of the primary key and
// * a map from column IDs to their index in the output.
func processIndexJoinColumns(
	table *scanNode, indexScan *scanNode,
) (primaryKeyColumns []bool, colIDtoRowIndex map[sqlbase.ColumnID]int) {
	colIDtoRowIndex = map[sqlbase.ColumnID]int{}

	// primaryKeyColumns defined here will serve both as the primaryKeyColumns
	// field in the indexJoinNode, and to determine which columns are
	// provided by this index for the purpose of splitting the WHERE
	// filter into an index-specific part and a "rest" part.
	// TODO(justin): make this a FastIntSet.
	primaryKeyColumns = make([]bool, len(indexScan.cols))
	for _, colID := range table.desc.PrimaryIndex.ColumnIDs {
		// All the PK columns from the table scanNode must
		// be fetched in the index scanNode.
		idx, ok := indexScan.colIdxMap[colID]
		if !ok {
			panic(fmt.Sprintf("Unknown column %d in PrimaryIndex!", colID))
		}
		primaryKeyColumns[idx] = true
		colIDtoRowIndex[colID] = idx
	}

	return primaryKeyColumns, colIDtoRowIndex
}

// indexJoinRun stores the run-time state of indexJoinNode during local execution.
type indexJoinRun struct {
	// primaryKeyPrefix is the KV key prefix of the rows
	// retrieved from the table scanNode.
	primaryKeyPrefix roachpb.Key

	// colIDtoRowIndex maps column IDs in the table scanNode into column
	// IDs in the index scanNode's results. The presence of a column ID
	// in this mapping is not sufficient to cause a column's values to
	// be produced; which columns are effectively loaded are decided by
	// the scanNodes' own valNeededForCol, which is updated by
	// setNeededColumns(). So there may be more columns in
	// colIDtoRowIndex than effectively accessed.
	colIDtoRowIndex map[sqlbase.ColumnID]int
}

func (n *indexJoinNode) startExec(params runParams) error {
	panic("indexJoinNode cannot be run in local mode")
}

func (n *indexJoinNode) Next(params runParams) (bool, error) {
	panic("indexJoinNode cannot be run in local mode")
}

func (n *indexJoinNode) Values() tree.Datums {
	panic("indexJoinNode cannot be run in local mode")
}

func (n *indexJoinNode) Close(ctx context.Context) {
	n.index.Close(ctx)
	n.table.Close(ctx)
}
