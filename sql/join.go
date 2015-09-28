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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/proto"
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
	primaryKeyPrefix proto.Key
	colIDtoRowIndex  map[ColumnID]int
	err              error
}

func makeIndexJoin(indexScan *scanNode, exactPrefix int) (*indexJoinNode, error) {
	// Copy the index scan node into a new table scan node and reset the fields
	// that were set up for the index scan.
	table := &scanNode{}
	*table = *indexScan
	table.index = &table.desc.PrimaryIndex
	table.spans = nil
	table.reverse = false
	table.isSecondaryIndex = false
	table.initOrdering(0)

	// We want to the index scan to keep the same render target indexes for
	// columns which are part of the primary key or part of the index. This
	// allows sorting (which was calculated based on the output columns) to be
	// avoided if possible.
	colIDtoRowIndex := map[ColumnID]int{}
	for _, colID := range table.desc.PrimaryIndex.ColumnIDs {
		colIDtoRowIndex[colID] = -1
	}
	for _, colID := range indexScan.index.ColumnIDs {
		colIDtoRowIndex[colID] = -1
	}

	// Rebuild the render targets for the index scan by looping over the render
	// targets for the table. Any referenced column that is part of the index or
	// the primary key is kept in its current location. Any other render target
	// is replaced with "1".
	indexScan.render = nil
	for _, render := range table.render {
		switch t := render.(type) {
		case *qvalue:
			if _, ok := colIDtoRowIndex[t.col.ID]; ok {
				colIDtoRowIndex[t.col.ID] = len(indexScan.render)
				indexScan.render = append(indexScan.render, t)
				continue
			}
		}
		indexScan.render = append(indexScan.render, parser.DInt(1))
	}
	for colID, index := range colIDtoRowIndex {
		if index == -1 {
			col, err := table.desc.FindColumnByID(colID)
			if err != nil {
				return nil, err
			}
			colIDtoRowIndex[col.ID] = len(indexScan.render)
			indexScan.render = append(indexScan.render, indexScan.getQVal(*col))
		}
	}

	indexScan.initOrdering(exactPrefix)

	primaryKeyPrefix := proto.Key(MakeIndexKeyPrefix(table.desc.ID, table.index.ID))

	return &indexJoinNode{
		index:            indexScan,
		table:            table,
		primaryKeyPrefix: primaryKeyPrefix,
		colIDtoRowIndex:  colIDtoRowIndex,
	}, nil
}

func (n *indexJoinNode) Columns() []string {
	return n.table.Columns()
}

func (n *indexJoinNode) Ordering() ([]int, int) {
	return n.index.Ordering()
}

func (n *indexJoinNode) Values() parser.DTuple {
	return n.table.Values()
}

func (n *indexJoinNode) Next() bool {
	// First, try to pull rows from the table.
	if n.table.kvs != nil && n.table.Next() {
		return true
	}
	if n.err = n.table.Err(); n.err != nil {
		return false
	}

	// The table is out of rows. Pull primary keys from the index.
	n.table.kvs = nil
	n.table.kvIndex = 0
	n.table.spans = n.table.spans[0:0]

	for len(n.table.spans) < joinBatchSize {
		if !n.index.Next() {
			// The index is out of rows or an error occurred.
			if n.err = n.index.Err(); n.err != nil {
				return false
			}
			if len(n.table.spans) == 0 {
				// The index is out of rows.
				return false
			}
			break
		}
		vals := n.index.Values()
		if log.V(3) {
			log.Infof("primary key vals: %v", vals)
		}

		var primaryIndexKey []byte
		primaryIndexKey, _, n.err = encodeIndexKey(
			n.table.index.ColumnIDs, n.colIDtoRowIndex, vals, n.primaryKeyPrefix)
		if n.err != nil {
			return false
		}
		key := proto.Key(primaryIndexKey)
		n.table.spans = append(n.table.spans, span{
			start: key,
			end:   key.PrefixEnd(),
		})
	}

	if n.table.Next() {
		return true
	}
	n.err = n.table.Err()
	return false
}

func (n *indexJoinNode) Err() error {
	return n.err
}

func (n *indexJoinNode) ExplainPlan() (name, description string, children []planNode) {
	return "index-join", "", []planNode{n.index, n.table}
}
