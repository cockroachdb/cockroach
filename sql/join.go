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
	"github.com/cockroachdb/cockroach/sql/sqlbase"
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
	colIDtoRowIndex  map[sqlbase.ColumnID]int
	err              error
	explain          explainMode
	debugVals        debugValues
}

// makeIndexJoin build an index join node.
// This destroys the original table scan node argument and reuses its
// storage to construct a new index scan node. A new table scan node
// is created separately as a member of the resulting index join node.
// The new index scan node is also returned alongside the new index join
// node.
func (p *planner) makeIndexJoin(origScan *scanNode, exactPrefix int) (resultPlan *indexJoinNode, indexScan *scanNode) {
	// Reuse the input argument's scanNode and its initialized parameters
	// at a starting point to build the new indexScan node.
	indexScan = origScan

	// Create a new table scan node with the primary index.
	table := p.Scan()
	table.desc = origScan.desc
	table.initDescDefaults()
	table.initOrdering(0)

	colIDtoRowIndex := map[sqlbase.ColumnID]int{}
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

	for i := range origScan.valNeededForCol {
		// We transfer valNeededForCol to the table node.
		table.valNeededForCol[i] = origScan.valNeededForCol[i]

		// For the index node, we set valNeededForCol for columns that are part of the index.
		id := indexScan.desc.Columns[i].ID
		_, found := colIDtoRowIndex[id]
		indexScan.valNeededForCol[i] = found
	}

	if origScan.filter != nil {
		// Transfer the filter to the table node. We must first convert the
		// IndexedVars associated with indexNode.
		convFunc := func(expr parser.VariableExpr) (ok bool, newExpr parser.VariableExpr) {
			iv := expr.(*parser.IndexedVar)
			return true, table.filterVars.IndexedVar(iv.Idx)
		}
		table.filter = exprConvertVars(origScan.filter, convFunc)

		// Now we split the filter by extracting the part that can be evaluated using just the index
		// columns.
		splitFunc := func(expr parser.VariableExpr) (ok bool, newExpr parser.VariableExpr) {
			colIdx := expr.(*parser.IndexedVar).Idx
			if !indexScan.valNeededForCol[colIdx] {
				return false, nil
			}
			return true, indexScan.filterVars.IndexedVar(colIdx)
		}
		indexScan.filter, table.filter = splitFilter(table.filter, splitFunc)
	}

	indexScan.initOrdering(exactPrefix)

	primaryKeyPrefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(table.desc.ID, table.index.ID))

	return &indexJoinNode{
		index:            indexScan,
		table:            table,
		primaryKeyPrefix: primaryKeyPrefix,
		colIDtoRowIndex:  colIDtoRowIndex,
	}, indexScan
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

func (n *indexJoinNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	// Mark both the index and the table scan nodes as debug.
	n.index.MarkDebug(mode)
	n.table.MarkDebug(mode)
}

func (n *indexJoinNode) DebugValues() debugValues {
	if n.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", n.explain))
	}
	return n.debugVals
}

func (n *indexJoinNode) expandPlan() error {
	// TODO(knz): Some code from makeIndexJoin() above really belongs here.
	if err := n.table.expandPlan(); err != nil {
		return err
	}
	return n.index.expandPlan()
}

func (n *indexJoinNode) Start() error {
	if err := n.table.Start(); err != nil {
		return err
	}
	return n.index.Start()
}

func (n *indexJoinNode) Next() bool {
	// Loop looking up the next row. We either are going to pull a row from the
	// table or a batch of rows from the index. If we pull a batch of rows from
	// the index we perform another iteration of the loop looking for rows in the
	// table. This outer loop is necessary because a batch of rows from the index
	// might all be filtered when the resulting rows are read from the table.
	for tableLookup := (len(n.table.spans) > 0); true; tableLookup = true {
		// First, try to pull a row from the table.
		if tableLookup && n.table.Next() {
			if n.explain == explainDebug {
				n.debugVals = n.table.DebugValues()
			}
			return true
		}
		if n.err = n.table.Err(); n.err != nil {
			return false
		}

		// The table is out of rows. Pull primary keys from the index.
		n.table.scanInitialized = false
		n.table.spans = n.table.spans[:0]

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

			if n.explain == explainDebug {
				n.debugVals = n.index.DebugValues()
				if n.debugVals.output != debugValueRow {
					return true
				}
			}

			vals := n.index.Values()
			primaryIndexKey, _, err := sqlbase.EncodeIndexKey(
				n.table.index, n.colIDtoRowIndex, vals, n.primaryKeyPrefix)
			n.err = err
			if n.err != nil {
				return false
			}
			key := roachpb.Key(primaryIndexKey)
			n.table.spans = append(n.table.spans, sqlbase.Span{
				Start: key,
				End:   key.PrefixEnd(),
			})

			if n.explain == explainDebug {
				// In debug mode, return the index information as a "partial" row.
				n.debugVals.output = debugValuePartial
				return true
			}
		}

		if log.V(3) {
			log.Infof("table scan: %s", sqlbase.PrettySpans(n.table.spans, 0))
		}
	}
	return false
}

func (n *indexJoinNode) Err() error {
	return n.err
}

func (n *indexJoinNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	return "index-join", "", []planNode{n.index, n.table}
}

func (n *indexJoinNode) ExplainTypes(_ func(string, string)) {}

func (n *indexJoinNode) SetLimitHint(numRows int64, soft bool) {
	if numRows < joinBatchSize {
		numRows = joinBatchSize
	}
	n.index.SetLimitHint(numRows, soft)
}
