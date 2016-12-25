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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const indexJoinBatchSize = 100

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
	valNeededIndex   []bool
	explain          explainMode
	debugVals        debugValues
}

// makeIndexJoin build an index join node.
// This destroys the original table scan node argument and reuses its
// storage to construct a new index scan node. A new table scan node
// is created separately as a member of the resulting index join node.
// The new index scan node is also returned alongside the new index join
// node.
func (p *planner) makeIndexJoin(
	origScan *scanNode, exactPrefix int,
) (resultPlan *indexJoinNode, indexScan *scanNode) {
	// Reuse the input argument's scanNode and its initialized parameters
	// at a starting point to build the new indexScan node.
	indexScan = origScan

	// Create a new scanNode that will be used with the primary index.
	table := p.Scan()
	table.desc = origScan.desc
	table.initDescDefaults(publicColumns)
	table.initOrdering(0)
	table.disableBatchLimit()

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

	// For the index node, we need values for columns that are part of the index.
	// TODO(radu): we could reduce this further - we only need the PK columns plus
	// whatever filters may be used by the filter below.
	valNeededIndex := make([]bool, len(origScan.valNeededForCol))
	for _, idx := range colIDtoRowIndex {
		valNeededIndex[idx] = true
	}

	if origScan.filter != nil {
		// Now we split the filter by extracting the part that can be
		// evaluated using just the index columns.

		// Since we are re-populating the IndexedVars, reset the helper
		// first so that the new vars are properly accounted for.
		indexScan.filterVars.Reset()
		splitFunc := func(expr parser.VariableExpr) (ok bool, newExpr parser.VariableExpr) {
			colIdx := expr.(*parser.IndexedVar).Idx
			if !valNeededIndex[colIdx] {
				return false, nil
			}
			return true, indexScan.filterVars.IndexedVar(colIdx)
		}
		indexScan.filter, table.filter = splitFilter(origScan.filter, splitFunc)
	}

	// Ensure that the indexed vars are transferred to the scanNodes fully.
	table.filterVars.Reset()
	table.filter = table.filterVars.Rebind(table.filter)

	indexScan.initOrdering(exactPrefix)

	primaryKeyPrefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(&table.desc, table.index.ID))

	node := &indexJoinNode{
		index:            indexScan,
		table:            table,
		primaryKeyPrefix: primaryKeyPrefix,
		colIDtoRowIndex:  colIDtoRowIndex,
		valNeededIndex:   valNeededIndex,
	}

	return node, indexScan
}

func (n *indexJoinNode) Columns() ResultColumns {
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
	// If we arrive here, selectNode's expandPlan has run already and
	// created the indexJoinNode by means of makeIndexJoin() above.
	// We thus now need to expand the sub-nodes.
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

func (n *indexJoinNode) Next() (bool, error) {
	// Loop looking up the next row. We either are going to pull a row from the
	// table or a batch of rows from the index. If we pull a batch of rows from
	// the index we perform another iteration of the loop looking for rows in the
	// table. This outer loop is necessary because a batch of rows from the index
	// might all be filtered when the resulting rows are read from the table.
	for tableLookup := (len(n.table.spans) > 0); true; tableLookup = true {
		// First, try to pull a row from the table.
		if tableLookup {
			next, err := n.table.Next()
			if err != nil {
				return false, err
			}
			if next {
				if n.explain == explainDebug {
					n.debugVals = n.table.DebugValues()
				}
				return true, nil
			}
		}

		// The table is out of rows. Pull primary keys from the index.
		n.table.scanInitialized = false
		n.table.spans = n.table.spans[:0]

		for len(n.table.spans) < indexJoinBatchSize {
			if next, err := n.index.Next(); !next {
				// The index is out of rows or an error occurred.
				if err != nil {
					return false, err
				}
				if len(n.table.spans) == 0 {
					// The index is out of rows.
					return false, nil
				}
				break
			}

			if n.explain == explainDebug {
				n.debugVals = n.index.DebugValues()
				if n.debugVals.output != debugValueRow {
					return true, nil
				}
			}

			vals := n.index.Values()
			primaryIndexKey, _, err := sqlbase.EncodeIndexKey(
				&n.table.desc, n.table.index, n.colIDtoRowIndex, vals, n.primaryKeyPrefix)
			if err != nil {
				return false, err
			}
			key := roachpb.Key(primaryIndexKey)
			n.table.spans = append(n.table.spans, roachpb.Span{
				Key:    key,
				EndKey: key.PrefixEnd(),
			})

			if n.explain == explainDebug {
				// In debug mode, return the index information as a "partial" row.
				n.debugVals.output = debugValuePartial
				return true, nil
			}
		}

		if log.V(3) {
			log.Infof(n.index.p.ctx(), "table scan: %s", sqlbase.PrettySpans(n.table.spans, 0))
		}
	}
	return false, nil
}

func (n *indexJoinNode) SetLimitHint(numRows int64, soft bool) {
	n.index.SetLimitHint(numRows, soft)
}

func (n *indexJoinNode) Close() {
	n.index.Close()
	n.table.Close()
}
