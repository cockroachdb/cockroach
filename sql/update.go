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

// editNode (Base, Run) is shared between all row updating
// statements (DELETE, UPDATE, INSERT).

// editNodeBase holds the common (prepare+execute) state needed to run
// row-modifying statements.
type editNodeBase struct {
	p          *planner
	rh         returningHelper
	tableDesc  *TableDescriptor
	autoCommit bool
}

func (p *planner) makeEditNode(t parser.TableExpr, r parser.ReturningExprs, autoCommit bool, priv privilege.Kind) (editNodeBase, *roachpb.Error) {
	tableDesc, pErr := p.getAliasedTableLease(t)
	if pErr != nil {
		return editNodeBase{}, pErr
	}

	if err := p.checkPrivilege(tableDesc, priv); err != nil {
		return editNodeBase{}, roachpb.NewError(err)
	}

	rh, err := p.makeReturningHelper(r, tableDesc.Name, tableDesc.Columns)
	if err != nil {
		return editNodeBase{}, roachpb.NewError(err)
	}

	return editNodeBase{
		p:          p,
		rh:         rh,
		tableDesc:  tableDesc,
		autoCommit: autoCommit,
	}, nil
}

// editNodeRun holds the runtime (execute) state needed to run
// row-modifying statements.
type editNodeRun struct {
	rows                  planNode
	primaryIndex          IndexDescriptor
	primaryIndexKeyPrefix []byte
	indexes               []IndexDescriptor
	pErr                  *roachpb.Error
	b                     *client.Batch
	resultRow             parser.DTuple
	done                  bool
}

func (r *editNodeRun) startEditNode(en *editNodeBase, rows planNode, indexes []IndexDescriptor) {
	if isSystemConfigID(en.tableDesc.GetID()) {
		// Mark transaction as operating on the system DB.
		en.p.txn.SetSystemConfigTrigger()
	}

	r.rows = rows
	r.primaryIndex = en.tableDesc.PrimaryIndex
	r.primaryIndexKeyPrefix = MakeIndexKeyPrefix(en.tableDesc.ID, r.primaryIndex.ID)
	r.b = en.p.txn.NewBatch()
	r.indexes = indexes
}

func (r *editNodeRun) finalize(en *editNodeBase, convertError bool) {
	if en.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		r.pErr = en.p.txn.CommitInBatch(r.b)
	} else {
		r.pErr = en.p.txn.Run(r.b)
	}
	if r.pErr != nil && convertError {
		r.pErr = convertBatchError(en.tableDesc, *r.b, r.pErr)
	}

	r.done = true
	r.b = nil
}

// recordCreatorNode (Base, Run) is shared by row creating statements
// (UPDATE, INSERT).

// rowCreatorNodeBase holds the common (prepare+execute) state needed
// to run statements that create row values.
type rowCreatorNodeBase struct {
	editNodeBase
	defaultExprs    []parser.Expr
	cols            []ColumnDescriptor
	primaryKeyCols  map[ColumnID]struct{}
	colIDtoRowIndex map[ColumnID]int
}

func (p *planner) makeRowCreatorNode(en editNodeBase, cols []ColumnDescriptor, colIDtoRowIndex map[ColumnID]int, forInsert bool) (rowCreatorNodeBase, *roachpb.Error) {
	defaultExprs, err := makeDefaultExprs(cols, &p.parser, p.evalCtx)
	if err != nil {
		return rowCreatorNodeBase{}, roachpb.NewError(err)
	}

	primaryKeyCols := map[ColumnID]struct{}{}
	for i, id := range en.tableDesc.PrimaryIndex.ColumnIDs {
		if forInsert {
			// Verify we have at least the columns that are part of the primary key.
			if _, ok := colIDtoRowIndex[id]; !ok {
				return rowCreatorNodeBase{}, roachpb.NewUErrorf("missing %q primary key column", en.tableDesc.PrimaryIndex.ColumnNames[i])
			}
		}
		primaryKeyCols[id] = struct{}{}
	}

	return rowCreatorNodeBase{
		editNodeBase:    en,
		defaultExprs:    defaultExprs,
		cols:            cols,
		primaryKeyCols:  primaryKeyCols,
		colIDtoRowIndex: colIDtoRowIndex,
	}, nil
}

// rowCreatorNodeBase holds the runtime (execute) state needed to run
// statements that create row values.
type rowCreatorNodeRun struct {
	editNodeRun
	marshalled []interface{}
}

func (r *rowCreatorNodeRun) startRowCreatorNode(en *rowCreatorNodeBase, rows planNode, indexes []IndexDescriptor) {
	r.startEditNode(&en.editNodeBase, rows, indexes)
	r.marshalled = make([]interface{}, len(en.cols))
}

type updateNode struct {
	// The following fields are populated during makePlan.
	rowCreatorNodeBase
	n                   *parser.Update
	colIDSet            map[ColumnID]struct{}
	primaryKeyColChange bool

	run struct {
		// The following fields are populated during Start().
		rowCreatorNodeRun
		deleteOnlyIndex map[int]struct{}
	}
}

// Update updates columns for a selection of rows from a table.
// Privileges: UPDATE and SELECT on table. We currently always use a select statement.
//   Notes: postgres requires UPDATE. Requires SELECT with WHERE clause with table.
//          mysql requires UPDATE. Also requires SELECT with WHERE clause with table.
// TODO(guanqun): need to support CHECK in UPDATE
func (p *planner) Update(n *parser.Update, autoCommit bool) (planNode, *roachpb.Error) {
	tracing.AnnotateTrace()

	en, pErr := p.makeEditNode(n.Table, n.Returning, autoCommit, privilege.UPDATE)
	if pErr != nil {
		return nil, pErr
	}

	exprs := make([]parser.UpdateExpr, len(n.Exprs))
	for i, expr := range n.Exprs {
		exprs[i] = *expr
	}

	// Determine which columns we're inserting into.
	var names parser.QualifiedNames
	for _, expr := range n.Exprs {
		// TODO(knz): We need to (attempt to) expand subqueries here already
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
			case *parser.DTuple:
				n = len(*t)
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

	cols, err := p.processColumns(en.tableDesc, names)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	rc, rcErr := p.makeRowCreatorNode(en, cols, nil, false)
	if rcErr != nil {
		return nil, rcErr
	}

	// Set of columns being updated
	var primaryKeyColChange bool
	colIDSet := map[ColumnID]struct{}{}
	for _, c := range cols {
		colIDSet[c.ID] = struct{}{}
		if _, ok := rc.primaryKeyCols[c.ID]; ok {
			primaryKeyColChange = true
		}
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
	targets := en.tableDesc.allColumnsSelector()
	i := 0
	// Remember the index where the targets for exprs start.
	exprTargetIdx := len(targets)
	for _, expr := range n.Exprs {
		if expr.Tuple {
			if t, ok := expr.Expr.(*parser.Tuple); ok {
				for _, e := range t.Exprs {
					e = fillDefault(e, i, rc.defaultExprs)
					targets = append(targets, parser.SelectExpr{Expr: e})
					i++
				}
			}
		} else {
			e := fillDefault(expr.Expr, i, rc.defaultExprs)
			targets = append(targets, parser.SelectExpr{Expr: e})
			i++
		}
	}

	// TODO(knz): Until we split the creation of the node from Start()
	// for the SelectClause too, we cannot cache this. This is because
	// this node's initSelect() method both does type checking and also
	// performs index selection. We cannot perform index selection
	// properly until the placeholder values are known.
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
	// using checkColumnType. This step also verifies that the expression
	// types match the column types.
	for i, target := range rows.(*selectNode).render[exprTargetIdx:] {
		// DefaultVal doesn't implement TypeCheck
		if _, ok := target.(parser.DefaultVal); ok {
			continue
		}
		d, err := parser.PerformTypeChecking(target, p.evalCtx.Args)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if err := checkColumnType(cols[i], d, p.evalCtx.Args); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	if pErr := en.rh.TypeCheck(); pErr != nil {
		return nil, pErr
	}

	return &updateNode{
		n:                   n,
		rowCreatorNodeBase:  rc,
		primaryKeyColChange: primaryKeyColChange,
		colIDSet:            colIDSet,
	}, nil
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
			case *parser.DTuple:
				for _, e := range *t {
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
	// TODO(knz): See comment above in Update().
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

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, col := range u.tableDesc.Columns {
		colIDtoRowIndex[col.ID] = i
	}
	u.colIDtoRowIndex = colIDtoRowIndex

	// Secondary indexes needing updating.
	needsUpdate := func(index IndexDescriptor) bool {
		// If the primary key changed, we need to update all of them.
		if u.primaryKeyColChange {
			return true
		}
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

	u.run.startRowCreatorNode(&u.rowCreatorNodeBase, rows, indexes)

	u.run.deleteOnlyIndex = deleteOnlyIndex

	return nil
}

func (u *updateNode) Next() bool {
	if u.run.done || u.run.pErr != nil {
		return false
	}

	if !u.run.rows.Next() {
		u.run.finalize(&u.editNodeBase, true)
		return false
	}

	tracing.AnnotateTrace()

	rowVals := u.run.rows.Values()

	primaryIndexKey, _, err := encodeIndexKey(
		&u.run.primaryIndex, u.colIDtoRowIndex, rowVals, u.run.primaryIndexKeyPrefix)
	if err != nil {
		u.run.pErr = roachpb.NewError(err)
		return false
	}
	// Compute the current secondary index key:value pairs for this row.
	secondaryIndexEntries, err := encodeSecondaryIndexes(
		u.tableDesc.ID, u.run.indexes, u.colIDtoRowIndex, rowVals)
	if err != nil {
		u.run.pErr = roachpb.NewError(err)
		return false
	}

	// Our updated value expressions occur immediately after the plain
	// columns in the output.
	newVals := rowVals[len(u.tableDesc.Columns):]

	// Ensure that the values honor the specified column widths.
	for i := range newVals {
		if err := checkValueWidth(u.cols[i], newVals[i]); err != nil {
			u.run.pErr = roachpb.NewError(err)
			return false
		}
	}

	// Update the row values.
	for i, col := range u.cols {
		val := newVals[i]
		if !col.Nullable && val == parser.DNull {
			u.run.pErr = roachpb.NewUErrorf("null value in column %q violates not-null constraint", col.Name)
			return false
		}
		rowVals[u.colIDtoRowIndex[col.ID]] = val
	}

	// Check that the new value types match the column types. This needs to
	// happen before index encoding because certain datum types (i.e. tuple)
	// cannot be used as index values.
	for i, val := range newVals {
		var mErr error
		if u.run.marshalled[i], mErr = marshalColumnValue(u.cols[i], val); mErr != nil {
			u.run.pErr = roachpb.NewError(mErr)
			return false
		}
	}

	// Compute the new primary index key for this row.
	newPrimaryIndexKey := primaryIndexKey
	var rowPrimaryKeyChanged bool
	if u.primaryKeyColChange {
		newPrimaryIndexKey, _, err = encodeIndexKey(
			&u.run.primaryIndex, u.colIDtoRowIndex, rowVals, u.run.primaryIndexKeyPrefix)
		if err != nil {
			u.run.pErr = roachpb.NewError(err)
			return false
		}
		// Note that even if primaryIndexColChange is true, it's possible that
		// primary key fields in this particular row didn't change.
		rowPrimaryKeyChanged = !bytes.Equal(primaryIndexKey, newPrimaryIndexKey)
	}

	// Compute the new secondary index key:value pairs for this row.
	newSecondaryIndexEntries, eErr := encodeSecondaryIndexes(
		u.tableDesc.ID, u.run.indexes, u.colIDtoRowIndex, rowVals)
	if eErr != nil {
		u.run.pErr = roachpb.NewError(eErr)
		return false
	}

	if rowPrimaryKeyChanged {
		// Delete all the data stored under the old primary key.
		rowStartKey := roachpb.Key(primaryIndexKey)
		rowEndKey := rowStartKey.PrefixEnd()
		if log.V(2) {
			log.Infof("DelRange %s - %s", rowStartKey, rowEndKey)
		}
		u.run.b.DelRange(rowStartKey, rowEndKey, false)

		// Delete all the old secondary indexes.
		for _, secondaryIndexEntry := range secondaryIndexEntries {
			if log.V(2) {
				log.Infof("Del %s", secondaryIndexEntry.key)
			}
			u.run.b.Del(secondaryIndexEntry.key)
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
		u.run.b.CPut(sentinelKey, []byte{}, nil)

		// Write any fields from the old row that were not modified by the UPDATE.
		for i, col := range u.tableDesc.Columns {
			if _, ok := u.colIDSet[col.ID]; ok {
				continue
			}
			if _, ok := u.primaryKeyCols[col.ID]; ok {
				continue
			}
			key := keys.MakeColumnKey(newPrimaryIndexKey, uint32(col.ID))
			val := rowVals[i]
			marshalledVal, mErr := marshalColumnValue(col, val)
			if mErr != nil {
				u.run.pErr = roachpb.NewError(mErr)
				return false
			}

			if log.V(2) {
				log.Infof("Put %s -> %v", roachpb.Key(key), val)
			}
			u.run.b.Put(key, marshalledVal)
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
			u.run.b.Del(secondaryIndexEntry.key)
		}
		if rowPrimaryKeyChanged || secondaryKeyChanged {
			// Do not update Indexes in the DELETE_ONLY state.
			if _, ok := u.run.deleteOnlyIndex[i]; !ok {
				if log.V(2) {
					log.Infof("CPut %s -> %v", newSecondaryIndexEntry.key,
						newSecondaryIndexEntry.value)
				}
				u.run.b.CPut(newSecondaryIndexEntry.key, newSecondaryIndexEntry.value, nil)
			}
		}
	}

	// Add the new values.
	for i, val := range newVals {
		col := u.cols[i]

		if _, ok := u.primaryKeyCols[col.ID]; ok {
			// Skip primary key columns as their values are encoded in the row
			// sentinel key which is guaranteed to exist for as long as the row
			// exists.
			continue
		}

		key := keys.MakeColumnKey(newPrimaryIndexKey, uint32(col.ID))
		if u.run.marshalled[i] != nil {
			// We only output non-NULL values. Non-existent column keys are
			// considered NULL during scanning and the row sentinel ensures we know
			// the row exists.
			if log.V(2) {
				log.Infof("Put %s -> %v", roachpb.Key(key), val)
			}

			u.run.b.Put(key, u.run.marshalled[i])
		} else {
			// The column might have already existed but is being set to NULL, so
			// delete it.
			if log.V(2) {
				log.Infof("Del %s", key)
			}

			u.run.b.Del(key)
		}

	}

	// rowVals[:len(tableDesc.Columns)] have been updated with the new values above.
	resultRow, err := u.rh.cookResultRow(rowVals[:len(u.tableDesc.Columns)])
	if err != nil {
		u.run.pErr = roachpb.NewError(err)
		return false
	}
	u.run.resultRow = resultRow

	return true
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
	return u.run.resultRow
}

func (u *updateNode) MarkDebug(mode explainMode) {
	u.run.rows.MarkDebug(mode)
}

func (u *updateNode) DebugValues() debugValues {
	return u.run.rows.DebugValues()
}

func (u *updateNode) Ordering() orderingInfo {
	return u.run.rows.Ordering()
}

func (u *updateNode) PErr() *roachpb.Error {
	return u.run.pErr
}

func (u *updateNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	var buf bytes.Buffer
	if v {
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
	}
	return "update", buf.String(), []planNode{u.run.rows}
}

func (u *updateNode) SetLimitHint(numRows int64, soft bool) {}
