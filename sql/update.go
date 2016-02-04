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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
)

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
func (p *planner) Update(n *parser.Update) (planNode, *roachpb.Error) {
	tableDesc, pErr := p.getAliasedTableLease(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	if pErr := p.checkPrivilege(tableDesc, privilege.UPDATE); pErr != nil {
		return nil, pErr
	}

	// Determine which columns we're inserting into.
	var names parser.QualifiedNames
	for _, expr := range n.Exprs {
		var epErr *roachpb.Error
		expr.Expr, epErr = p.expandSubqueries(expr.Expr, len(expr.Names))
		if epErr != nil {
			return nil, epErr
		}

		if expr.Tuple {
			// TODO(pmattis): The distinction between Tuple and DTuple here is
			// irritating. We'll see a DTuple if the expression was a subquery that
			// has been evaluated. We'll see a Tuple in other cases.
			n := 0
			switch t := expr.Expr.(type) {
			case parser.Tuple:
				n = len(t)
			case parser.DTuple:
				n = len(t)
			default:
				return nil, roachpb.NewErrorf("unsupported tuple assignment: %T", expr.Expr)
			}
			if len(expr.Names) != n {
				return nil, roachpb.NewUErrorf("number of columns (%d) does not match number of values (%d)",
					len(expr.Names), n)
			}
		}
		names = append(names, expr.Names...)
	}
	cols, pErr := p.processColumns(tableDesc, names)
	if pErr != nil {
		return nil, pErr
	}

	// Set of columns being updated
	colIDSet := map[ColumnID]struct{}{}
	for _, c := range cols {
		colIDSet[c.ID] = struct{}{}
	}
	// Don't allow updating any column that is part of the primary key.
	for i, id := range tableDesc.PrimaryIndex.ColumnIDs {
		if _, ok := colIDSet[id]; ok {
			return nil, roachpb.NewUErrorf("primary key column %q cannot be updated", tableDesc.PrimaryIndex.ColumnNames[i])
		}
	}

	defaultExprs, pErr := p.makeDefaultExprs(cols)
	if pErr != nil {
		return nil, pErr
	}

	// Generate the list of select targets. We need to select all of the columns
	// plus we select all of the update expressions in case those expressions
	// reference columns (e.g. "UPDATE t SET v = v + 1"). Note that we flatten
	// expressions for tuple assignments just as we flattened the column names
	// above. So "UPDATE t SET (a, b) = (1, 2)" translates into select targets of
	// "*, 1, 2", not "*, (1, 2)".
	targets := tableDesc.allColumnsSelector()
	i := 0
	for _, expr := range n.Exprs {
		if expr.Tuple {
			switch t := expr.Expr.(type) {
			case parser.Tuple:
				for _, e := range t {
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

	// Query the rows that need updating.
	rows, pErr := p.Select(&parser.Select{
		Exprs: targets,
		From:  parser.TableExprs{n.Table},
		Where: n.Where,
	})
	if pErr != nil {
		return nil, pErr
	}

	// ValArgs have their types populated in the above Select if they are part
	// of an expression ("SET a = 2 + $1") in the type check step where those
	// types are inferred. For the simpler case ("SET a = $1"), populate them
	// using marshalColumnValue. This step also verifies that the expression
	// types match the column types.
	if p.prepareOnly {
		i := 0
		f := func(expr parser.Expr) *roachpb.Error {
			idx := i
			i++
			// DefaultVal doesn't implement TypeCheck
			if _, ok := expr.(parser.DefaultVal); ok {
				return nil
			}
			d, err := expr.TypeCheck(p.evalCtx.Args)
			if err != nil {
				return roachpb.NewError(err)
			}
			if _, err := marshalColumnValue(cols[idx], d, p.evalCtx.Args); err != nil {
				return err
			}
			return nil
		}
		for _, expr := range n.Exprs {
			if expr.Tuple {
				switch t := expr.Expr.(type) {
				case parser.Tuple:
					for _, e := range t {
						if err := f(e); err != nil {
							return nil, err
						}
					}
				case parser.DTuple:
					for _, e := range t {
						if err := f(e); err != nil {
							return nil, err
						}
					}
				}
			} else {
				if err := f(expr.Expr); err != nil {
					return nil, err
				}
			}
		}

		return nil, nil
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

	b := client.Batch{}
	result := &valuesNode{}
	for rows.Next() {
		rowVals := rows.Values()
		result.rows = append(result.rows, parser.DTuple(nil))

		primaryIndexKey, _, pErr := encodeIndexKey(
			&primaryIndex, colIDtoRowIndex, rowVals, primaryIndexKeyPrefix)
		if pErr != nil {
			return nil, pErr
		}
		// Compute the current secondary index key:value pairs for this row.
		secondaryIndexEntries, pErr := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if pErr != nil {
			return nil, pErr
		}

		// Our updated value expressions occur immediately after the plain
		// columns in the output.
		newVals := rowVals[len(tableDesc.Columns):]
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
			var mpErr *roachpb.Error
			if marshalled[i], mpErr = marshalColumnValue(cols[i], val, p.evalCtx.Args); mpErr != nil {
				return nil, mpErr
			}
		}

		// Compute the new secondary index key:value pairs for this row.
		newSecondaryIndexEntries, epErr := encodeSecondaryIndexes(
			tableDesc.ID, indexes, colIDtoRowIndex, rowVals)
		if epErr != nil {
			return nil, epErr
		}

		// Update secondary indexes.
		for i, newSecondaryIndexEntry := range newSecondaryIndexEntries {
			secondaryIndexEntry := secondaryIndexEntries[i]
			if !bytes.Equal(newSecondaryIndexEntry.key, secondaryIndexEntry.key) {
				// Do not update Indexes in the DELETE_ONLY state.
				if _, ok := deleteOnlyIndex[i]; !ok {
					if log.V(2) {
						log.Infof("CPut %s -> %v", newSecondaryIndexEntry.key,
							newSecondaryIndexEntry.value)
					}
					b.CPut(newSecondaryIndexEntry.key, newSecondaryIndexEntry.value, nil)
				}
				if log.V(2) {
					log.Infof("Del %s", secondaryIndexEntry.key)
				}
				b.Del(secondaryIndexEntry.key)
			}
		}

		// Add the new values.
		for i, val := range newVals {
			col := cols[i]

			key := keys.MakeColumnKey(primaryIndexKey, uint32(col.ID))
			if marshalled[i] != nil {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.
				if log.V(2) {
					log.Infof("Put %s -> %v", key, val)
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
	}

	if pErr := rows.PErr(); pErr != nil {
		return nil, pErr
	}
	if pErr := p.txn.Run(&b); pErr != nil {
		return nil, convertBatchError(tableDesc, b, pErr)
	}

	return result, nil
}

func fillDefault(expr parser.Expr, index int, defaultExprs []parser.Expr) parser.Expr {
	switch expr.(type) {
	case parser.DefaultVal:
		return defaultExprs[index]
	}
	return expr
}
