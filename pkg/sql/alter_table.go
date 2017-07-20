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
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type alterTableNode struct {
	n         *parser.AlterTable
	tableDesc *sqlbase.TableDescriptor
}

// AlterTable applies a schema change on a table.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) AlterTable(ctx context.Context, n *parser.AlterTable) (planNode, error) {
	tn, err := n.Table.NormalizeWithDatabaseName(p.session.Database)
	if err != nil {
		return nil, err
	}

	tableDesc, err := getTableDesc(ctx, p.txn, p.getVirtualTabler(), tn)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		if n.IfExists {
			return &emptyNode{}, nil
		}
		return nil, sqlbase.NewUndefinedRelationError(tn)
	}

	if err := p.CheckPrivilege(tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	return &alterTableNode{n: n, tableDesc: tableDesc}, nil
}

func (n *alterTableNode) Start(params runParams) error {
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)
	var droppedViews []string

	for _, cmd := range n.n.Cmds {
		switch t := cmd.(type) {
		case *parser.AlterTableAddColumn:
			d := t.ColumnDef
			if len(d.CheckExprs) > 0 {
				return pgerror.Unimplemented(
					"alter add check", "adding a CHECK constraint via ALTER not supported")
			}
			if d.HasFKConstraint() {
				return pgerror.Unimplemented(
					"alter add fk", "adding a REFERENCES constraint via ALTER not supported")
			}
			col, idx, err := sqlbase.MakeColumnDefDescs(d, params.p.session.SearchPath, &params.p.evalCtx)
			if err != nil {
				return err
			}
			_, dropped, err := n.tableDesc.FindColumnByName(d.Name)
			if err == nil {
				if dropped {
					return fmt.Errorf("column %q being dropped, try again later", col.Name)
				}
				if t.IfNotExists {
					continue
				}
			}

			n.tableDesc.AddColumnMutation(*col, sqlbase.DescriptorMutation_ADD)
			if idx != nil {
				n.tableDesc.AddIndexMutation(*idx, sqlbase.DescriptorMutation_ADD)
			}
			if d.HasColumnFamily() {
				err := n.tableDesc.AddColumnToFamilyMaybeCreate(
					col.Name, string(d.Family.Name), d.Family.Create,
					d.Family.IfNotExists)
				if err != nil {
					return err
				}
			}

		case *parser.AlterTableAddConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			inuseNames := make(map[string]struct{}, len(info))
			for k := range info {
				inuseNames[k] = struct{}{}
			}
			switch d := t.ConstraintDef.(type) {
			case *parser.UniqueConstraintTableDef:
				if d.PrimaryKey {
					return fmt.Errorf("multiple primary keys for table %q are not allowed", n.tableDesc.Name)
				}
				idx := sqlbase.IndexDescriptor{
					Name:             string(d.Name),
					Unique:           true,
					StoreColumnNames: d.Storing.ToStrings(),
				}
				if err := idx.FillColumns(d.Columns); err != nil {
					return err
				}
				_, dropped, err := n.tableDesc.FindIndexByName(string(d.Name))
				if err == nil {
					if dropped {
						return fmt.Errorf("index %q being dropped, try again later", d.Name)
					}
				}
				n.tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_ADD)

			case *parser.CheckConstraintTableDef:
				ck, err := makeCheckConstraint(*n.tableDesc, d, inuseNames, params.p.session.SearchPath)
				if err != nil {
					return err
				}
				ck.Validity = sqlbase.ConstraintValidity_Unvalidated
				n.tableDesc.Checks = append(n.tableDesc.Checks, ck)
				descriptorChanged = true

			case *parser.ForeignKeyConstraintTableDef:
				if _, err := d.Table.NormalizeWithDatabaseName(params.p.session.Database); err != nil {
					return err
				}
				affected := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
				err := params.p.resolveFK(params.ctx, n.tableDesc, d, affected, sqlbase.ConstraintValidity_Unvalidated)
				if err != nil {
					return err
				}
				descriptorChanged = true
				for _, updated := range affected {
					if err := params.p.saveNonmutationAndNotify(params.ctx, updated); err != nil {
						return err
					}
				}

			default:
				return fmt.Errorf("unsupported constraint: %T", t.ConstraintDef)
			}

		case *parser.AlterTableDropColumn:
			col, dropped, err := n.tableDesc.FindColumnByName(t.Column)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return err
			}
			if dropped {
				continue
			}
			// You can't drop a column depended on by a view unless CASCADE was
			// specified.
			for _, ref := range n.tableDesc.DependedOnBy {
				found := false
				for _, colID := range ref.ColumnIDs {
					if colID == col.ID {
						found = true
						break
					}
				}
				if !found {
					continue
				}
				err := params.p.canRemoveDependentViewGeneric(
					params.ctx, "column", string(t.Column), n.tableDesc.ParentID, ref, t.DropBehavior,
				)
				if err != nil {
					return err
				}
				viewDesc, err := params.p.getViewDescForCascade(
					params.ctx, "column", string(t.Column), n.tableDesc.ParentID, ref.ID, t.DropBehavior,
				)
				if err != nil {
					return err
				}
				droppedViews, err = params.p.removeDependentView(params.ctx, n.tableDesc, viewDesc)
				if err != nil {
					return err
				}
			}

			if n.tableDesc.PrimaryIndex.ContainsColumnID(col.ID) {
				return fmt.Errorf("column %q is referenced by the primary key", col.Name)
			}
			for _, idx := range n.tableDesc.AllNonDropIndexes() {
				// We automatically drop indexes on that column that only
				// index that column (and no other columns). If CASCADE is
				// specified, we also drop other indices that refer to this
				// column.  The criteria to determine whether an index "only
				// indexes that column":
				//
				// Assume a table created with CREATE TABLE foo (a INT, b INT).
				// Then assume the user issues ALTER TABLE foo DROP COLUMN a.
				//
				// INDEX i1 ON foo(a) -> i1 deleted
				// INDEX i2 ON foo(a) STORING(b) -> i2 deleted
				// INDEX i3 ON foo(a, b) -> i3 not deleted unless CASCADE is specified.
				// INDEX i4 ON foo(b) STORING(a) -> i4 not deleted unless CASCADE is specified.

				// containsThisColumn becomes true if the index is defined
				// over the column being dropped.
				containsThisColumn := false
				// containsOnlyThisColumn becomes false if the index also
				// includes non-PK columns other than the one being dropped.
				containsOnlyThisColumn := true

				// Analyze the index.
				for _, id := range idx.ColumnIDs {
					if id == col.ID {
						containsThisColumn = true
					} else {
						containsOnlyThisColumn = false
					}
				}
				for _, id := range idx.ExtraColumnIDs {
					if n.tableDesc.PrimaryIndex.ContainsColumnID(id) {
						// All secondary indices necessary contain the PK
						// columns, too. (See the comments on the definition of
						// IndexDescriptor). The presence of a PK column in the
						// secondary index should thus not be seen as a
						// sufficient reason to reject the DROP.
						continue
					}
					if id == col.ID {
						containsThisColumn = true
					}
				}
				// The loop above this comment is for the old STORING encoding. The
				// loop below is for the new encoding (where the STORING columns are
				// always in the value part of a KV).
				for _, id := range idx.StoreColumnIDs {
					if id == col.ID {
						containsThisColumn = true
					}
				}

				// Perform the DROP.
				if containsThisColumn {
					if containsOnlyThisColumn || t.DropBehavior == parser.DropCascade {
						if err := params.p.dropIndexByName(
							params.ctx, parser.Name(idx.Name), n.tableDesc, false, t.DropBehavior, n.n.String(),
						); err != nil {
							return err
						}
					} else {
						return fmt.Errorf("column %q is referenced by existing index %q", col.Name, idx.Name)
					}
				}
			}
			found := false
			for i := range n.tableDesc.Columns {
				if n.tableDesc.Columns[i].ID == col.ID {
					n.tableDesc.AddColumnMutation(col, sqlbase.DescriptorMutation_DROP)
					n.tableDesc.Columns = append(n.tableDesc.Columns[:i], n.tableDesc.Columns[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("column %q in the middle of being added, try again later", t.Column)
			}

		case *parser.AlterTableDropConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			details, ok := info[name]
			if !ok {
				if t.IfExists {
					continue
				}
				return fmt.Errorf("constraint %q does not exist", t.Constraint)
			}
			switch details.Kind {
			case sqlbase.ConstraintTypePK:
				return fmt.Errorf("cannot drop primary key")
			case sqlbase.ConstraintTypeUnique:
				return fmt.Errorf("UNIQUE constraint depends on index %q, use DROP INDEX if you really want to drop it", t.Constraint)
			case sqlbase.ConstraintTypeCheck:
				for i := range n.tableDesc.Checks {
					if n.tableDesc.Checks[i].Name == name {
						n.tableDesc.Checks = append(n.tableDesc.Checks[:i], n.tableDesc.Checks[i+1:]...)
						descriptorChanged = true
						break
					}
				}
			case sqlbase.ConstraintTypeFK:
				for i, idx := range n.tableDesc.Indexes {
					if idx.ForeignKey.Name == name {
						if err := params.p.removeFKBackReference(params.ctx, n.tableDesc, idx); err != nil {
							return err
						}
						n.tableDesc.Indexes[i].ForeignKey = sqlbase.ForeignKeyReference{}
						descriptorChanged = true
						break
					}
				}
			default:
				return errors.Errorf("dropping %s constraint %q unsupported", details.Kind, t.Constraint)
			}

		case *parser.AlterTableValidateConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			constraint, ok := info[name]
			if !ok {
				return fmt.Errorf("constraint %q does not exist", t.Constraint)
			}
			if !constraint.Unvalidated {
				continue
			}
			switch constraint.Kind {
			case sqlbase.ConstraintTypeCheck:
				found := false
				var idx int
				for idx = range n.tableDesc.Checks {
					if n.tableDesc.Checks[idx].Name == name {
						found = true
						break
					}
				}
				if !found {
					panic("constraint returned by GetConstraintInfo not found")
				}
				ck := n.tableDesc.Checks[idx]
				if err := params.p.validateCheckExpr(
					params.ctx, ck.Expr, &n.n.Table, n.tableDesc,
				); err != nil {
					return err
				}
				n.tableDesc.Checks[idx].Validity = sqlbase.ConstraintValidity_Validated
				descriptorChanged = true

			case sqlbase.ConstraintTypeFK:
				found := false
				var id sqlbase.IndexID
				for _, idx := range n.tableDesc.AllNonDropIndexes() {
					if idx.ForeignKey.IsSet() && idx.ForeignKey.Name == name {
						found = true
						id = idx.ID
						break
					}
				}
				if !found {
					panic("constraint returned by GetConstraintInfo not found")
				}
				idx, err := n.tableDesc.FindIndexByID(id)
				if err != nil {
					panic(err)
				}
				if err := params.p.validateForeignKey(params.ctx, n.tableDesc, idx); err != nil {
					return err
				}
				idx.ForeignKey.Validity = sqlbase.ConstraintValidity_Validated
				descriptorChanged = true

			default:
				return errors.Errorf("validating %s constraint %q unsupported", constraint.Kind, t.Constraint)
			}

		case parser.ColumnMutationCmd:
			// Column mutations
			col, dropped, err := n.tableDesc.FindColumnByName(t.GetColumn())
			if err != nil {
				return err
			}
			if dropped {
				return fmt.Errorf("column %q in the middle of being dropped", t.GetColumn())
			}
			if err := applyColumnMutation(
				&col, t, params.p.session.SearchPath,
			); err != nil {
				return err
			}
			n.tableDesc.UpdateColumnDescriptor(col)
			descriptorChanged = true

		default:
			return fmt.Errorf("unsupported alter cmd: %T", cmd)
		}
	}
	// Were some changes made?
	//
	// This is only really needed for the unittests that add dummy mutations
	// before calling ALTER TABLE commands. We do not want to apply those
	// dummy mutations. Most tests trigger errors above
	// this line, but tests that run redundant operations like dropping
	// a column when it's already dropped will hit this condition and exit.
	addedMutations := len(n.tableDesc.Mutations) > origNumMutations
	if !addedMutations && !descriptorChanged {
		return nil
	}

	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	mutationID := sqlbase.InvalidMutationID
	var err error
	if addedMutations {
		mutationID, err = params.p.createSchemaChangeJob(params.ctx, n.tableDesc, parser.AsString(n.n))
	} else {
		err = n.tableDesc.SetUpVersion()
	}
	if err != nil {
		return err
	}

	if err := params.p.writeTableDesc(params.ctx, n.tableDesc); err != nil {
		return err
	}

	// Record this table alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	if err := MakeEventLogger(params.p.LeaseMgr()).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogAlterTable,
		int32(n.tableDesc.ID),
		int32(params.p.evalCtx.NodeID),
		struct {
			TableName           string
			Statement           string
			User                string
			MutationID          uint32
			CascadeDroppedViews []string
		}{n.tableDesc.Name, n.n.String(), params.p.session.User, uint32(mutationID), droppedViews},
	); err != nil {
		return err
	}

	params.p.notifySchemaChange(n.tableDesc, mutationID)

	return nil
}

func (n *alterTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableNode) Close(context.Context)        {}
func (n *alterTableNode) Values() parser.Datums        { return parser.Datums{} }

func applyColumnMutation(
	col *sqlbase.ColumnDescriptor, mut parser.ColumnMutationCmd, searchPath parser.SearchPath,
) error {
	switch t := mut.(type) {
	case *parser.AlterTableSetDefault:
		if t.Default == nil {
			col.DefaultExpr = nil
		} else {
			colDatumType := col.Type.ToDatumType()
			if _, err := sqlbase.SanitizeVarFreeExpr(
				t.Default, colDatumType, "DEFAULT", searchPath,
			); err != nil {
				return err
			}
			s := parser.Serialize(t.Default)
			col.DefaultExpr = &s
		}

	case *parser.AlterTableDropNotNull:
		col.Nullable = true
	}
	return nil
}

func labeledRowValues(cols []sqlbase.ColumnDescriptor, values parser.Datums) string {
	var s bytes.Buffer
	for i := range cols {
		if i != 0 {
			s.WriteString(`, `)
		}
		s.WriteString(cols[i].Name)
		s.WriteString(`=`)
		s.WriteString(values[i].String())
	}
	return s.String()
}
