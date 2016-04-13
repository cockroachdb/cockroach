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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/tracing"
)

type updateNode struct {
	// The following fields are populated during makePlan.
	p            *planner
	n            *parser.Update
	rh           *returningHelper
	tableDesc    *TableDescriptor
	defaultExprs []parser.Expr
	cols         []ColumnDescriptor
	colIDSet     map[ColumnID]struct{}
	autoCommit   bool

	// The following fields are populated during Start().
	rows                  planNode
	colIDtoRowIndex       map[ColumnID]int
	primaryIndex          IndexDescriptor
	primaryIndexKeyPrefix []byte
	deleteOnlyIndex       map[int]struct{}
	indexes               []IndexDescriptor
	pErr                  *roachpb.Error
	b                     *client.Batch
	resultRow             parser.DTuple
	done                  bool
}

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
func (p *planner) Update(n *parser.Update, autoCommit bool) (planNode, *roachpb.Error) {

	tracing.AnnotateTrace()

	tableDesc, pErr := p.getAliasedTableLease(n.Table)
	if pErr != nil {
		return nil, pErr
	}

	if err := p.checkPrivilege(tableDesc, privilege.UPDATE); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Determine which columns we're inserting into.
	var names parser.QualifiedNames
	for _, expr := range n.Exprs {
		// FIXME(knz): We need to (attempt to) expand subqueries here already
		// so that it retrieves the column names. But then we need to do
		// it again when the placeholder values are known below.
		newExpr, epErr := p.expandSubqueries(expr.Expr, len(expr.Names))
		if epErr != nil {
			return nil, epErr
		}

		if expr.Tuple {
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

	defaultExprs, err := makeDefaultExprs(cols, &p.parser, p.evalCtx)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	rh, err := makeReturningHelper(p, n.Returning, tableDesc.Name, tableDesc.Columns)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if pErr := rh.TypeCheck(); pErr != nil {
		return nil, pErr
	}

	tracing.AnnotateTrace()

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
	for _, expr := range n.Exprs {
		if expr.Tuple {
			if t, ok := expr.Expr.(*parser.Tuple); ok {
				for _, e := range t.Exprs {
					e = fillDefault(e, i, defaultExprs)
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

	// FIXME(knz): We need to initialize the SelectClause once here
	// (during prepare) as this is needed to initialize the placeholder
	// types in p.evalCtx.Args. This will be cleaned up once
	// we split prepare and makeplan for the SelectClause.
	rows, pErr := p.SelectClause(&parser.SelectClause{
		Exprs: targets,
		From:  []parser.TableExpr{n.Table},
		Where: n.Where,
	})
	if pErr != nil {
		return nil, pErr
	}

	// ValArgs have their types populated in the above Select if they are part
	// of an expression ("SET a = 2 + $1") in the type check step where those
	// types are inferred. For the simpler case ("SET a = $1"), populate them
	// using assignArgType. This step also verifies that the expression
	// types match the column types.
	for i, target := range rows.(*selectNode).render[exprTargetIdx:] {
		// DefaultVal doesn't implement TypeCheck
		if _, ok := target.(parser.DefaultVal); ok {
			continue
		}
		d, err := target.TypeCheck(p.evalCtx.Args)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if err := assignArgType(cols[i], d, p.evalCtx.Args); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	res := &updateNode{
		p:            p,
		n:            n,
		rh:           rh,
		tableDesc:    tableDesc,
		autoCommit:   autoCommit,
		defaultExprs: defaultExprs,
		cols:         cols,
		colIDSet:     colIDSet,
	}
	return res, nil
}

func (u *updateNode) Start() *roachpb.Error {
	exprs := make([]parser.UpdateExpr, len(u.n.Exprs))
	for i, expr := range u.n.Exprs {
		exprs[i] = *expr
	}

	// Expand the sub-queries and construct the real list of targets.
	for i, expr := range exprs {
		newExpr, epErr := u.p.expandSubqueries(expr.Expr, len(expr.Names))
		if epErr != nil {
			return epErr
		}
		exprs[i].Expr = newExpr
	}

	// Really generate the list of select targets.
	// TODO(radu): we only need to select columns necessary to generate primary and
	// secondary indexes keys, and columns needed by returningHelper.
	targets := u.tableDesc.allColumnsSelector()
	i := 0
	for _, expr := range exprs {
		if expr.Tuple {
			switch t := expr.Expr.(type) {
			case *parser.Tuple:
				//panic("unreachable xz")
				for _, e := range t.Exprs {
					e = fillDefault(e, i, u.defaultExprs)
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
			e := fillDefault(expr.Expr, i, u.defaultExprs)
			targets = append(targets, parser.SelectExpr{Expr: e})
			i++
		}
	}

	// Create the workhorse select clause for rows that need updating.
	rows, pErr := u.p.SelectClause(&parser.SelectClause{
		Exprs: targets,
		From:  []parser.TableExpr{u.n.Table},
		Where: u.n.Where,
	})
	if pErr != nil {
		return pErr
	}

	if pErr := rows.Start(); pErr != nil {
		return pErr
	}

	u.rows = rows

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, col := range u.tableDesc.Columns {
		colIDtoRowIndex[col.ID] = i
	}
	u.colIDtoRowIndex = colIDtoRowIndex

	u.primaryIndex = u.tableDesc.PrimaryIndex
	u.primaryIndexKeyPrefix = MakeIndexKeyPrefix(u.tableDesc.ID, u.primaryIndex.ID)

	// Secondary indexes needing updating.
	needsUpdate := func(index IndexDescriptor) bool {
		for _, id := range index.ColumnIDs {
			if _, ok := u.colIDSet[id]; ok {
				return true
			}
		}
		return false
	}

	indexes := make([]IndexDescriptor, 0, len(u.tableDesc.Indexes)+len(u.tableDesc.Mutations))
	var deleteOnlyIndex map[int]struct{}

	for _, index := range u.tableDesc.Indexes {
		if needsUpdate(index) {
			indexes = append(indexes, index)
		}
	}
	for _, m := range u.tableDesc.Mutations {
		if index := m.GetIndex(); index != nil {
			if needsUpdate(*index) {
				indexes = append(indexes, *index)

				switch m.State {
				case DescriptorMutation_DELETE_ONLY:
					if deleteOnlyIndex == nil {
						// Allocate at most once.
						deleteOnlyIndex = make(map[int]struct{}, len(u.tableDesc.Mutations))
					}
					deleteOnlyIndex[len(indexes)-1] = struct{}{}

				case DescriptorMutation_WRITE_ONLY:
				}
			}
		}
	}
	u.deleteOnlyIndex = deleteOnlyIndex
	u.indexes = indexes

	if isSystemConfigID(u.tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		u.p.txn.SetSystemConfigTrigger()
	}

	u.b = u.p.txn.NewBatch()

	return nil
}

func (u *updateNode) Next() bool {
	if u.done == true || u.pErr != nil {
		return false
	}

	next := u.rows.Next()
	if next {
		tracing.AnnotateTrace()

		rowVals := u.rows.Values()

		primaryIndexKey, _, err := encodeIndexKey(
			&u.primaryIndex, u.colIDtoRowIndex, rowVals, u.primaryIndexKeyPrefix)
		if err != nil {
			u.pErr = roachpb.NewError(err)
			return false
		}
		// Compute the current secondary index key:value pairs for this row.
		secondaryIndexEntries, err := encodeSecondaryIndexes(
			u.tableDesc.ID, u.indexes, u.colIDtoRowIndex, rowVals)
		if err != nil {
			u.pErr = roachpb.NewError(err)
			return false
		}

		// Our updated value expressions occur immediately after the plain
		// columns in the output.
		newVals := rowVals[len(u.tableDesc.Columns):]

		// Ensure that the values honor the specified column widths.
		for i := range newVals {
			if err := checkValueWidth(u.cols[i], newVals[i]); err != nil {
				u.pErr = roachpb.NewError(err)
				return false
			}
		}

		// Update the row values.
		for i, col := range u.cols {
			val := newVals[i]
			if !col.Nullable && val == parser.DNull {
				u.pErr = roachpb.NewUErrorf("null value in column %q violates not-null constraint", col.Name)
				return false
			}
			rowVals[u.colIDtoRowIndex[col.ID]] = val
		}

		// Check that the new value types match the column types. This needs to
		// happen before index encoding because certain datum types (i.e. tuple)
		// cannot be used as index values.
		marshalled := make([]interface{}, len(u.cols))

		for i, val := range newVals {
			var mErr error
			if marshalled[i], mErr = marshalColumnValue(u.cols[i], val); mErr != nil {
				u.pErr = roachpb.NewError(mErr)
				return false
			}
		}

		// Compute the new secondary index key:value pairs for this row.
		newSecondaryIndexEntries, eErr := encodeSecondaryIndexes(
			u.tableDesc.ID, u.indexes, u.colIDtoRowIndex, rowVals)
		if eErr != nil {
			u.pErr = roachpb.NewError(eErr)
			return false
		}

		// Update secondary indexes.
		for i, newSecondaryIndexEntry := range newSecondaryIndexEntries {
			secondaryIndexEntry := secondaryIndexEntries[i]
			if !bytes.Equal(newSecondaryIndexEntry.key, secondaryIndexEntry.key) {
				// Do not update Indexes in the DELETE_ONLY state.
				if _, ok := u.deleteOnlyIndex[i]; !ok {
					if log.V(2) {
						log.Infof("CPut %s -> %v", newSecondaryIndexEntry.key,
							newSecondaryIndexEntry.value)
					}
					u.b.CPut(newSecondaryIndexEntry.key, newSecondaryIndexEntry.value, nil)
				}
				if log.V(2) {
					log.Infof("Del %s", secondaryIndexEntry.key)
				}
				u.b.Del(secondaryIndexEntry.key)
			}
		}

		// Add the new values.
		for i, val := range newVals {
			col := u.cols[i]

			key := keys.MakeColumnKey(primaryIndexKey, uint32(col.ID))
			if marshalled[i] != nil {
				// We only output non-NULL values. Non-existent column keys are
				// considered NULL during scanning and the row sentinel ensures we know
				// the row exists.
				if log.V(2) {
					log.Infof("Put %s -> %v", key, val)
				}

				u.b.Put(key, marshalled[i])
			} else {
				// The column might have already existed but is being set to NULL, so
				// delete it.
				if log.V(2) {
					log.Infof("Del %s", key)
				}

				u.b.Del(key)
			}

		}

		// rowVals[:len(tableDesc.Columns)] have been updated with the new values above.
		resultRow, err := u.rh.cookResultRow(rowVals[:len(u.tableDesc.Columns)])
		if err != nil {
			u.pErr = roachpb.NewError(err)
			return false
		}
		u.resultRow = resultRow
	} else {
		// We're done. Finish the batch.
		if u.autoCommit {
			// An auto-txn can commit the transaction with the batch. This is an
			// optimization to avoid an extra round-trip to the transaction
			// coordinator.
			u.pErr = u.p.txn.CommitInBatch(u.b)
		} else {
			u.pErr = u.p.txn.Run(u.b)
		}
		// FIXME: insert had the following conversion, but delete didn't.
		// I am adding it here for symmetry. Is it really needed?
		if u.pErr != nil {
			u.pErr = convertBatchError(u.tableDesc, *u.b, u.pErr)
		}
		u.done = true
		u.b = nil
	}
	return next
}

func fillDefault(expr parser.Expr, index int, defaultExprs []parser.Expr) parser.Expr {
	switch expr.(type) {
	case parser.DefaultVal:
		return defaultExprs[index]
	}
	return expr
}

func (u *updateNode) Columns() []ResultColumn {
	return u.rh.columns
}

func (u *updateNode) Values() parser.DTuple {
	return u.resultRow
}

func (u *updateNode) MarkDebug(mode explainMode) {
	u.rows.MarkDebug(mode)
}

func (u *updateNode) DebugValues() debugValues {
	return u.rows.DebugValues()
}

func (u *updateNode) Ordering() orderingInfo {
	return u.rows.Ordering()
}

func (u *updateNode) PErr() *roachpb.Error {
	return u.pErr
}

func (u *updateNode) ExplainPlan() (name, description string, children []planNode) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "set %s (", u.tableDesc.Name)
	for i, col := range u.cols {
		if i > 0 {
			fmt.Fprintf(&buf, ", ")
		}
		fmt.Fprintf(&buf, "%s", col.Name)
	}
	fmt.Fprintf(&buf, ") returning (")
	for i, col := range u.rh.columns {
		if i > 0 {
			fmt.Fprintf(&buf, ", ")
		}
		fmt.Fprintf(&buf, "%s", col.Name)
	}
	fmt.Fprintf(&buf, ")")
	return "update", buf.String(), []planNode{u.rows}
}

func (u *updateNode) SetLimitHint(numRows int64, soft bool) {}
