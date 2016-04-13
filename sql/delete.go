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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
)

type deleteNode struct {
	// The following fields are populated during makePlan.
	p               *planner
	n               *parser.Delete
	rh              *returningHelper
	tableDesc       *TableDescriptor
	colIDtoRowIndex map[ColumnID]int
	autoCommit      bool
	// The following fields are populated during Start()
	rows                  planNode
	primaryIndex          IndexDescriptor
	primaryIndexKeyPrefix []byte
	indexes               []IndexDescriptor
	pErr                  *roachpb.Error
	b                     *client.Batch
	resultRow             parser.DTuple
	fastPath              bool
	done                  bool
}

// Delete removes rows from a table.
// Privileges: DELETE and SELECT on table. We currently always use a SELECT statement.
//   Notes: postgres requires DELETE. Also requires SELECT for "USING" and "WHERE" with tables.
//          mysql requires DELETE. Also requires SELECT if a table is used in the "WHERE" clause.
func (p *planner) Delete(n *parser.Delete, autoCommit bool) (planNode, *roachpb.Error) {
	tableDesc, pErr := p.getAliasedTableLease(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	if err := p.checkPrivilege(tableDesc, privilege.DELETE); err != nil {
		return nil, roachpb.NewError(err)
	}

	rh, err := makeReturningHelper(p, n.Returning, tableDesc.Name, tableDesc.Columns)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if pErr := rh.TypeCheck(); pErr != nil {
		return nil, pErr
	}

	// TODO(knz): Until we split the creation of the node from Start()
	// for the SelectClause too, we cannot cache this. This is because
	// this node's initSelect() method both does type checking and also
	// performs index selection. We cannot perform index selection
	// properly until the placeholder values are known.
	if _, pErr := p.SelectClause(&parser.SelectClause{
		Exprs: tableDesc.allColumnsSelector(),
		From:  []parser.TableExpr{n.Table},
		Where: n.Where,
	}); pErr != nil {
		return nil, pErr
	}

	res := &deleteNode{p: p, n: n, rh: rh, tableDesc: tableDesc, autoCommit: autoCommit}
	return res, nil
}

func (d *deleteNode) Start() *roachpb.Error {
	// TODO(knz): See the comment above in Delete().
	rows, pErr := d.p.SelectClause(&parser.SelectClause{
		Exprs: d.tableDesc.allColumnsSelector(),
		From:  []parser.TableExpr{d.n.Table},
		Where: d.n.Where,
	})
	if pErr != nil {
		return pErr
	}

	if pErr := rows.Start(); pErr != nil {
		return pErr
	}

	d.rows = rows

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex, err := makeColIDtoRowIndex(rows, d.tableDesc)
	if err != nil {
		return roachpb.NewError(err)
	}
	d.colIDtoRowIndex = colIDtoRowIndex

	d.primaryIndex = d.tableDesc.PrimaryIndex
	d.primaryIndexKeyPrefix = MakeIndexKeyPrefix(d.tableDesc.ID, d.primaryIndex.ID)

	// Determine the secondary indexes that need to be updated as well.
	indexes := d.tableDesc.Indexes
	// Also include all the indexes under mutation; mutation state is
	// irrelevant for deletions.
	for _, m := range d.tableDesc.Mutations {
		if index := m.GetIndex(); index != nil {
			indexes = append(indexes, *index)
		}
	}
	d.indexes = indexes

	if isSystemConfigID(d.tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		d.p.txn.SetSystemConfigTrigger()
	}

	d.b = d.p.txn.NewBatch()

	// Check if we can avoid doing a round-trip to read the values and just
	// "fast-path" skip to deleting the key ranges without reading them first.
	// TODO(dt): We could probably be smarter when presented with an index-join,
	// but this goes away anyway once we push-down more of SQL.
	sel := rows.(*selectNode)
	if scan, ok := sel.table.node.(*scanNode); ok && canDeleteWithoutScan(d.n, scan, len(indexes)) {
		d.fastPath = true
		d.pErr = d.fastDelete()
		d.done = true
		return d.pErr
	}

	return nil
}

func (d *deleteNode) FastPathResults() (int, bool) {
	if d.fastPath {
		return d.rh.rowCount, true
	}
	return 0, false
}

func (d *deleteNode) Next() bool {
	if d.done || d.pErr != nil {
		return false
	}

	next := d.rows.Next()
	if next {
		rowVals := d.rows.Values()

		primaryIndexKey, _, err := encodeIndexKey(
			&d.primaryIndex, d.colIDtoRowIndex, rowVals, d.primaryIndexKeyPrefix)
		if err != nil {
			d.pErr = roachpb.NewError(err)
			return false
		}

		secondaryIndexEntries, err := encodeSecondaryIndexes(
			d.tableDesc.ID, d.indexes, d.colIDtoRowIndex, rowVals)
		if err != nil {
			d.pErr = roachpb.NewError(err)
			return false
		}

		for _, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("Del %s", secondaryIndexEntry.key)
			}
			d.b.Del(secondaryIndexEntry.key)
		}

		// Delete the row.
		rowStartKey := roachpb.Key(primaryIndexKey)
		rowEndKey := rowStartKey.PrefixEnd()
		if log.V(2) {
			log.Infof("DelRange %s - %s", rowStartKey, rowEndKey)
		}
		d.b.DelRange(rowStartKey, rowEndKey, false)

		resultRow, err := d.rh.cookResultRow(rowVals)
		if err != nil {
			d.pErr = roachpb.NewError(err)
			return false
		}
		d.resultRow = resultRow
	} else {
		// We're done. Finish the batch.
		if d.autoCommit {
			// An auto-txn can commit the transaction with the batch. This is an
			// optimization to avoid an extra round-trip to the transaction
			// coordinator.
			d.pErr = d.p.txn.CommitInBatch(d.b)
		} else {
			d.pErr = d.p.txn.Run(d.b)
		}
		d.done = true
		d.b = nil
	}
	return next
}

// Determine if the deletion of `rows` can be done without actually scanning them,
// i.e. if we do not need to know their values for filtering expressions or a
// RETURNING clause or for updating secondary indexes.
func canDeleteWithoutScan(n *parser.Delete, scan *scanNode, indexCount int) bool {
	if indexCount != 0 {
		if log.V(2) {
			log.Infof("delete forced to scan: values required to update %d secondary indexes", indexCount)
		}
		return false
	}
	if n.Returning != nil {
		if log.V(2) {
			log.Infof("delete forced to scan: values required for RETURNING")
		}
		return false
	}
	if scan.filter != nil {
		if log.V(2) {
			log.Infof("delete forced to scan: values required for filter (%s)", scan.filter)
		}
		return false
	}
	return true
}

// `fastDelete` skips the scan of rows and just deletes the ranges that
// `rows` would scan. Should only be used if `canDeleteWithoutScan` indicates
// that it is safe to do so.
func (d *deleteNode) fastDelete() *roachpb.Error {
	scan := d.rows.(*selectNode).table.node.(*scanNode)
	if !scan.initScan() {
		return scan.pErr
	}

	for _, span := range scan.spans {
		if log.V(2) {
			log.Infof("Skipping scan and just deleting %s - %s", span.start, span.end)
		}
		d.b.DelRange(span.start, span.end, true)
	}

	if d.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		if pErr := d.p.txn.CommitInBatch(d.b); pErr != nil {
			return pErr
		}
	} else {
		if pErr := d.p.txn.Run(d.b); pErr != nil {
			return pErr
		}
	}

	for _, r := range d.b.Results {
		var prev []byte
		for _, i := range r.Keys {
			// If prefix is same, don't bother decoding key.
			if len(prev) > 0 && bytes.HasPrefix(i, prev) {
				continue
			}

			after, err := scan.readIndexKey(i)
			if err != nil {
				return roachpb.NewError(err)
			}
			k := i[:len(i)-len(after)]
			if !bytes.Equal(k, prev) {
				prev = k
				d.rh.rowCount++
			}
		}
	}
	return nil
}

func (d *deleteNode) Columns() []ResultColumn {
	return d.rh.columns
}

func (d *deleteNode) Values() parser.DTuple {
	return d.resultRow
}

func (d *deleteNode) MarkDebug(mode explainMode) {
	d.rows.MarkDebug(mode)
}

func (d *deleteNode) DebugValues() debugValues {
	return d.rows.DebugValues()
}

func (d *deleteNode) Ordering() orderingInfo {
	return d.rows.Ordering()
}

func (d *deleteNode) PErr() *roachpb.Error {
	return d.pErr
}

func (d *deleteNode) ExplainPlan() (name, description string, children []planNode) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "from %s returning (", d.tableDesc.Name)
	for i, col := range d.rh.columns {
		if i > 0 {
			fmt.Fprintf(&buf, ", ")
		}
		fmt.Fprintf(&buf, "%s", col.Name)
	}
	fmt.Fprintf(&buf, ")")
	return "delete", buf.String(), []planNode{d.rows}
}

func (d *deleteNode) SetLimitHint(numRows int64, soft bool) {}
