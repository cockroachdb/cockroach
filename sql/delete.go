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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
)

// Delete deletes rows from a table.
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

	// TODO(tamird,pmattis): avoid going through Select to avoid encoding
	// and decoding keys.
	rows, pErr := p.SelectClause(&parser.SelectClause{
		Exprs: tableDesc.allColumnsSelector(),
		From:  []parser.TableExpr{n.Table},
		Where: n.Where,
	})
	if pErr != nil {
		return nil, pErr
	}
	sel := rows.(*selectNode)

	rh, err := makeReturningHelper(p, n.Returning, tableDesc.Name, tableDesc.Columns)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if p.evalCtx.PrepareOnly {
		// Return the result column types.
		return rh.getResults()
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex, err := makeColIDtoRowIndex(rows, tableDesc)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	primaryIndex := tableDesc.PrimaryIndex
	primaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	// Determine the secondary indexes that need to be updated as well.
	indexes := tableDesc.Indexes
	// Also include all the indexes under mutation; mutation state is
	// irrelevant for deletions.
	for _, m := range tableDesc.Mutations {
		if index := m.GetIndex(); index != nil {
			indexes = append(indexes, *index)
		}
	}

	if isSystemConfigID(tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		p.txn.SetSystemConfigTrigger()
	}

	// Check if we can avoid doing a round-trip to read the values and just
	// "fast-path" skip to deleting the key ranges without reading them first.
	// TODO(dt): We could probably be smarter when presented with an index-join,
	// but this goes away anyway once we push-down more of SQL.
	if scan, ok := sel.table.node.(*scanNode); ok && canDeleteWithoutScan(n, scan, len(indexes)) {
		cols, err := rh.getResults()
		if err != nil {
			return nil, err
		}
		return p.fastDelete(scan, cols, autoCommit)
	}

	b := p.txn.NewBatch()

	for rows.Next() {
		rowVals := rows.Values()

		primaryIndexKey, _, err := encodeIndexKey(
			&primaryIndex, colIDtoRowIndex, rowVals, primaryIndexKeyPrefix)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		secondaryIndexEntries, err := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		for _, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("Del %s", secondaryIndexEntry.key)
			}
			b.Del(secondaryIndexEntry.key)
		}

		// Delete the row.
		rowStartKey := roachpb.Key(primaryIndexKey)
		rowEndKey := rowStartKey.PrefixEnd()
		if log.V(2) {
			log.Infof("DelRange %s - %s", rowStartKey, rowEndKey)
		}
		b.DelRange(rowStartKey, rowEndKey, false)

		if err := rh.append(rowVals); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	if pErr := rows.PErr(); pErr != nil {
		return nil, pErr
	}

	if autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		pErr = p.txn.CommitInBatch(b)
	} else {
		pErr = p.txn.Run(b)
	}
	if pErr != nil {
		return nil, pErr
	}

	return rh.getResults()
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
func (p *planner) fastDelete(scan *scanNode, result *returningNode, autoCommit bool) (planNode, *roachpb.Error) {
	b := p.txn.NewBatch()

	if !scan.initScan() {
		return nil, scan.pErr
	}

	for _, span := range scan.spans {
		if log.V(2) {
			log.Infof("Skipping scan and just deleting %s - %s", span.start, span.end)
		}
		b.DelRange(span.start, span.end, true)
	}

	if autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		if pErr := p.txn.CommitInBatch(b); pErr != nil {
			return nil, pErr
		}
	} else {
		if pErr := p.txn.Run(b); pErr != nil {
			return nil, pErr
		}
	}

	for _, r := range b.Results {
		var prev []byte
		for _, i := range r.Keys {
			// If prefix is same, don't bother decoding key.
			if len(prev) > 0 && bytes.HasPrefix(i, prev) {
				continue
			}

			after, err := scan.readIndexKey(i)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			k := i[:len(i)-len(after)]
			if !bytes.Equal(k, prev) {
				prev = k
				result.rowCount++
			}
		}
	}
	return result, nil
}
