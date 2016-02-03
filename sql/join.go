// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

const joinBatchSize = 100

// An indexJoinNode implements joining of results from an index with the rows
// of a table. The index side of the join is pulled first and the resulting
// rows are used to lookup rows in the table. The work is batched: we pull
// joinBatchSize rows from the index and use the primary key to construct spans
// that are looked up in the table.
type indexJoinNode struct {
	index            *scanNode
	table            *scanNode
	primaryKeyPrefix roachpb.Key
	colIDtoRowIndex  map[ColumnID]int
	pErr             *roachpb.Error
}

func makeIndexJoin(indexScan *scanNode, exactPrefix int) (*indexJoinNode, *roachpb.Error) {
	// Create a new table scan node with the primary index.
	table := &scanNode{planner: indexScan.planner, txn: indexScan.txn}
	table.desc = indexScan.desc
	table.initDescDefaults()
	table.initOrdering(0)

	// TODO(radu): the join node should be aware of any filtering expressions that refer only to
	// columns in the index to filter out rows during the join.

	colIDtoRowIndex := map[ColumnID]int{}
	for _, colID := range table.desc.PrimaryIndex.ColumnIDs {
		idx, ok := indexScan.colIdxMap[colID]
		if !ok {
			panic(fmt.Sprintf("Unknown column %d in PrimaryIndex!", colID))
		}
		colIDtoRowIndex[colID] = idx
	}
	for _, colID := range indexScan.index.ColumnIDs {
		idx, ok := indexScan.colIdxMap[colID]
		if !ok {
			panic(fmt.Sprintf("Unknown column %d in index!", colID))
		}
		colIDtoRowIndex[colID] = idx
	}

	for i := range indexScan.valNeededForCol {
		// We transfer valNeededForCol to the table node
		table.valNeededForCol[i] = indexScan.valNeededForCol[i]

		// For the index node, we set valNeededForCol for columns that are part of the index.
		id := indexScan.visibleCols[i].ID
		_, found := colIDtoRowIndex[id]
		indexScan.valNeededForCol[i] = found
	}

	indexScan.initOrdering(exactPrefix)

	primaryKeyPrefix := roachpb.Key(MakeIndexKeyPrefix(table.desc.ID, table.index.ID))

	return &indexJoinNode{
		index:            indexScan,
		table:            table,
		primaryKeyPrefix: primaryKeyPrefix,
		colIDtoRowIndex:  colIDtoRowIndex,
	}, nil
}

func (n *indexJoinNode) Columns() []ResultColumn {
	return n.table.Columns()
}

func (n *indexJoinNode) Ordering() orderingInfo {
	return n.index.Ordering()
}

func (n *indexJoinNode) Values() parser.DTuple {
	return n.table.Values()
}

func (*indexJoinNode) DebugValues() debugValues {
	// TODO(radu)
	panic("debug mode not implemented in indexJoinNode")
}

func (n *indexJoinNode) Next() bool {
	// Loop looking up the next row. We either are going to pull a row from the
	// table or a batch of rows from the index. If we pull a batch of rows from
	// the index we perform another iteration of the loop looking for rows in the
	// table. This outer loop is necessary because a batch of rows from the index
	// might all be filtered when the resulting rows are read from the table.
	for tableLookup := (n.table.kvs != nil); true; tableLookup = true {
		// First, try to pull a row from the table.
		if tableLookup && n.table.Next() {
			return true
		}
		if n.pErr = n.table.PErr(); n.pErr != nil {
			return false
		}

		// The table is out of rows. Pull primary keys from the index.
		n.table.kvs = nil
		n.table.kvIndex = 0
		n.table.spans = n.table.spans[:0]

		for len(n.table.spans) < joinBatchSize {
			if !n.index.Next() {
				// The index is out of rows or an error occurred.
				if n.pErr = n.index.PErr(); n.pErr != nil {
					return false
				}
				if len(n.table.spans) == 0 {
					// The index is out of rows.
					return false
				}
				break
			}

			vals := n.index.Values()
			var primaryIndexKey []byte
			primaryIndexKey, _, n.pErr = encodeIndexKey(
				n.table.index, n.colIDtoRowIndex, vals, n.primaryKeyPrefix)
			if n.pErr != nil {
				return false
			}
			key := roachpb.Key(primaryIndexKey)
			n.table.spans = append(n.table.spans, span{
				start: key,
				end:   key.PrefixEnd(),
			})
		}

		if log.V(3) {
			log.Infof("table scan: %s", prettySpans(n.table.spans, 0))
		}
	}
	return false
}

func (n *indexJoinNode) PErr() *roachpb.Error {
	return n.pErr
}

func (n *indexJoinNode) ExplainPlan() (name, description string, children []planNode) {
	return "index-join", "", []planNode{n.index, n.table}
}
