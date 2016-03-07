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

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
// TODO(guanqun): need to support CHECK in UPDATE
func (p *planner) Update(n *parser.Update, autoCommit bool) (planNode, *roachpb.Error) {
	tracing.AnnotateTrace()
	tableDesc, pErr := p.getAliasedTableLease(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	if err := p.checkPrivilege(tableDesc, privilege.UPDATE); err != nil {
		return nil, roachpb.NewError(err)
	}

	// TODO(dan): Consider caching this on the TableDescriptor.
	primaryKeyCols := map[ColumnID]struct{}{}
	for _, id := range tableDesc.PrimaryIndex.ColumnIDs {
		primaryKeyCols[id] = struct{}{}
	}

	exprs := make([]parser.UpdateExpr, len(n.Exprs))
	for i, expr := range n.Exprs {
		exprs[i] = *expr
	}

	// Determine which columns we're inserting into.
	var names parser.QualifiedNames
	for i, expr := range exprs {
		newExpr, epErr := p.expandSubqueries(expr.Expr, len(expr.Names))
		if epErr != nil {
			return nil, epErr
		}
		exprs[i].Expr = newExpr

		if expr.Tuple {
			// TODO(pmattis): The distinction between Tuple and DTuple here is
			// irritating. We'll see a DTuple if the expression was a subquery that
			// has been evaluated. We'll see a Tuple in other cases.
			n := 0
			switch t := newExpr.(type) {
			case *parser.Tuple:
				n = len(t.Exprs)
			case parser.DTuple:
				n = len(t)
			default:
				return nil, roachpb.NewErrorf("unsupported tuple assignment: %T", newExpr)
			}
			if len(expr.Names) != n {
				return nil, roachpb.NewUErrorf("number of columns (%d) does not match number of values (%d)",
					len(expr.Names), n)
			}
		}
		names = append(names, expr.Names...)
	}
	cols, err := p.processColumns(tableDesc, names)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Set of columns being updated
	var primaryKeyColChange bool
	colIDSet := map[ColumnID]struct{}{}
	for _, c := range cols {
		colIDSet[c.ID] = struct{}{}
		if _, ok := primaryKeyCols[c.ID]; ok {
			primaryKeyColChange = true
		}
	}

	defaultExprs, err := makeDefaultExprs(cols, &p.parser, p.evalCtx)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Generate the list of select targets. We need to select all of the columns
	// plus we select all of the update expressions in case those expressions
	// reference columns (e.g. "UPDATE t SET v = v + 1"). Note that we flatten
	// expressions for tuple assignments just as we flattened the column names
	// above. So "UPDATE t SET (a, b) = (1, 2)" translates into select targets of
	// "*, 1, 2", not "*, (1, 2)".
	// TODO(radu): we only need to select columns necessary to generate primary and
	// secondary indexes keys, and columns needed by returningHelper.
	targets := tableDesc.allColumnsSelector()
	i := 0
	// Remember the index where the targets for exprs start.
	exprTargetIdx := len(targets)
	for _, expr := range exprs {
		if expr.Tuple {
			switch t := expr.Expr.(type) {
			case *parser.Tuple:
				for _, e := range t.Exprs {
					e = fillDefault(e, i, defaultExprs)
					targets = append(targets, parser.SelectExpr{Expr: e})
					i++
				}
			case parser.DTuple:
				for _, e := range t {
					targets = append(targets, parser.SelectExpr{Expr: e})
					i++
				}
			}
		} else {
			e := fillDefault(expr.Expr, i, defaultExprs)
			targets = append(targets, parser.SelectExpr{Expr: e})
			i++
		}
	}

	tracing.AnnotateTrace()

	// Query the rows that need updating.
	rows, pErr := p.SelectClause(&parser.SelectClause{
		Exprs: targets,
		From:  []parser.TableExpr{n.Table},
		Where: n.Where,
	})
	if pErr != nil {
		return nil, pErr
	}

	rh, err := makeReturningHelper(p, n.Returning, tableDesc.Name, tableDesc.Columns)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// ValArgs have their types populated in the above Select if they are part
	// of an expression ("SET a = 2 + $1") in the type check step where those
	// types are inferred. For the simpler case ("SET a = $1"), populate them
	// using marshalColumnValue. This step also verifies that the expression
	// types match the column types.
	if p.evalCtx.PrepareOnly {
		for i, target := range rows.(*selectNode).render[exprTargetIdx:] {
			// DefaultVal doesn't implement TypeCheck
			if _, ok := target.(parser.DefaultVal); ok {
				continue
			}
			d, err := target.TypeCheck(p.evalCtx.Args)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			if _, err := marshalColumnValue(cols[i], d, p.evalCtx.Args); err != nil {
				return nil, roachpb.NewError(err)
			}
		}
		// Return the result column types.
		return rh.getResults()
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, col := range tableDesc.Columns {
		colIDtoRowIndex[col.ID] = i
	}

	primaryIndex := tableDesc.PrimaryIndex
	primaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc.ID, primaryIndex.ID)

	// Secondary indexes needing updating.
	needsUpdate := func(index IndexDescriptor) bool {
		// If the primary key changed, we need to update all of them.
		if primaryKeyColChange {
			return true
		}
		for _, id := range index.ColumnIDs {
			if _, ok := colIDSet[id]; ok {
				return true
			}
		}
		return false
	}

	indexes := make([]IndexDescriptor, 0, len(tableDesc.Indexes)+len(tableDesc.Mutations))
	var deleteOnlyIndex map[int]struct{}

	for _, index := range tableDesc.Indexes {
		if needsUpdate(index) {
			indexes = append(indexes, index)
		}
	}
	for _, m := range tableDesc.Mutations {
		if index := m.GetIndex(); index != nil {
			if needsUpdate(*index) {
				indexes = append(indexes, *index)

				switch m.State {
				case DescriptorMutation_DELETE_ONLY:
					if deleteOnlyIndex == nil {
						// Allocate at most once.
						deleteOnlyIndex = make(map[int]struct{}, len(tableDesc.Mutations))
					}
					deleteOnlyIndex[len(indexes)-1] = struct{}{}

				case DescriptorMutation_WRITE_ONLY:
				}
			}
		}
	}

	marshalled := make([]interface{}, len(cols))

	b := p.txn.NewBatch()
	tracing.AnnotateTrace()
	for rows.Next() {
		tracing.AnnotateTrace()

		rowVals := rows.Values()

		primaryIndexKey, _, err := encodeIndexKey(
			&primaryIndex, colIDtoRowIndex, rowVals, primaryIndexKeyPrefix)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		// Compute the current secondary index key:value pairs for this row.
		secondaryIndexEntries, err := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		// Our updated value expressions occur immediately after the plain
		// columns in the output.
		newVals := rowVals[len(tableDesc.Columns):]

		// Ensure that the values honor the specified column widths.
		for i := range newVals {
			if err := checkValueWidth(cols[i], newVals[i]); err != nil {
				return nil, roachpb.NewError(err)
			}
		}

		// Update the row values.
		for i, col := range cols {
			val := newVals[i]
			if !col.Nullable && val == parser.DNull {
				return nil, roachpb.NewUErrorf("null value in column %q violates not-null constraint", col.Name)
			}
			rowVals[colIDtoRowIndex[col.ID]] = val
		}

		// Check that the new value types match the column types. This needs to
		// happen before index encoding because certain datum types (i.e. tuple)
		// cannot be used as index values.
		for i, val := range newVals {
			var mErr error
			if marshalled[i], mErr = marshalColumnValue(cols[i], val, p.evalCtx.Args); mErr != nil {
				return nil, roachpb.NewError(mErr)
			}
		}

		// Compute the new primary index key for this row.
		newPrimaryIndexKey := primaryIndexKey
		var rowPrimaryKeyChanged bool
		if primaryKeyColChange {
			newPrimaryIndexKey, _, err = encodeIndexKey(
				&primaryIndex, colIDtoRowIndex, rowVals, primaryIndexKeyPrefix)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			// Note that even if primaryIndexColChange is true, it's possible that
			// primary key fields in this particular row didn't change.
			rowPrimaryKeyChanged = !bytes.Equal(primaryIndexKey, newPrimaryIndexKey)
		}

		// Compute the new secondary index key:value pairs for this row.
		newSecondaryIndexEntries, eErr := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if eErr != nil {
			return nil, roachpb.NewError(eErr)
		}

		if rowPrimaryKeyChanged {
			// Delete all the data stored under the old primary key.
			rowStartKey := roachpb.Key(primaryIndexKey)
			rowEndKey := rowStartKey.PrefixEnd()
			if log.V(2) {
				log.Infof("DelRange %s - %s", rowStartKey, rowEndKey)
			}
			b.DelRange(rowStartKey, rowEndKey, false)

			// Delete all the old secondary indexes.
			for _, secondaryIndexEntry := range secondaryIndexEntries {
				if log.V(2) {
					log.Infof("Del %s", secondaryIndexEntry.key)
				}
				b.Del(secondaryIndexEntry.key)
			}

			// Write the new row sentinel. We want to write the sentinel first in case
			// we are trying to insert a duplicate primary key: if we write the
			// secondary indexes first, we may get an error that looks like a
			// uniqueness violation on a non-unique index.
			sentinelKey := keys.MakeNonColumnKey(newPrimaryIndexKey)
			if log.V(2) {
				log.Infof("CPut %s -> NULL", roachpb.Key(sentinelKey))
			}
			// This is subtle: An interface{}(nil) deletes the value, so we pass in
			// []byte{} as a non-nil value.
			b.CPut(sentinelKey, []byte{}, nil)

			// Write any fields from the old row that were not modified by the UPDATE.
			for i, col := range tableDesc.Columns {
				if _, ok := colIDSet[col.ID]; ok {
					continue
				}
				if _, ok := primaryKeyCols[col.ID]; ok {
					continue
				}
				key := keys.MakeColumnKey(newPrimaryIndexKey, uint32(col.ID))
				val := rowVals[i]
				marshalledVal, mErr := marshalColumnValue(col, val, p.evalCtx.Args)
				if mErr != nil {
					return nil, roachpb.NewError(mErr)
				}

				if log.V(2) {
					log.Infof("Put %s -> %v", roachpb.Key(key), val)
				}
				b.Put(key, marshalledVal)
			}
			// At this point, we've deleted the old row and associated index data and
			// written the sentinel keys and column keys for non-updated columns. Fall
			// through to below where the index keys and updated column keys will be
			// written.
		}

		// Update secondary indexes.
		for i, newSecondaryIndexEntry := range newSecondaryIndexEntries {
			secondaryIndexEntry := secondaryIndexEntries[i]
			secondaryKeyChanged := !bytes.Equal(newSecondaryIndexEntry.key, secondaryIndexEntry.key)
			if secondaryKeyChanged {
				if log.V(2) {
					log.Infof("Del %s", secondaryIndexEntry.key)
				}
				b.Del(secondaryIndexEntry.key)
			}
			if rowPrimaryKeyChanged || secondaryKeyChanged {
				// Do not update Indexes in the DELETE_ONLY state.
				if _, ok := deleteOnlyIndex[i]; !ok {
					if log.V(2) {
						log.Infof("CPut %s -> %v", newSecondaryIndexEntry.key,
							newSecondaryIndexEntry.value)
					}
					b.CPut(newSecondaryIndexEntry.key, newSecondaryIndexEntry.value, nil)
				}
			}
		}

		// Add the new values.
		for i, val := range newVals {
			col := cols[i]

			if _, ok := primaryKeyCols[col.ID]; ok {
				// Skip primary key columns as their values are encoded in the row
				// sentinel key which is guaranteed to exist for as long as the row
				// exists.
				continue
			}

			key := keys.MakeColumnKey(newPrimaryIndexKey, uint32(col.ID))
			if marshalled[i] != nil {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.
				if log.V(2) {
					log.Infof("Put %s -> %v", roachpb.Key(key), val)
				}

				b.Put(key, marshalled[i])
			} else {
				// The column might have already existed but is being set to NULL, so
				// delete it.
				if log.V(2) {
					log.Infof("Del %s", key)
				}

				b.Del(key)
			}
		}

		// rowVals[:len(tableDesc.Columns)] have been updated with the new values above.
		if err := rh.append(rowVals[:len(tableDesc.Columns)]); err != nil {
			return nil, roachpb.NewError(err)
		}
	}
	tracing.AnnotateTrace()

	if pErr := rows.PErr(); pErr != nil {
		return nil, pErr
	}

	if isSystemConfigID(tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		p.txn.SetSystemConfigTrigger()
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
		return nil, convertBatchError(tableDesc, *b, pErr)
	}

	tracing.AnnotateTrace()
	return rh.getResults()
}

func fillDefault(expr parser.Expr, index int, defaultExprs []parser.Expr) parser.Expr {
	switch expr.(type) {
	case parser.DefaultVal:
		return defaultExprs[index]
	}
	return expr
}
