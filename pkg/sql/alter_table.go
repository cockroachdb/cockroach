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

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type alterTableNode struct {
	n         *tree.AlterTable
	tableDesc *sqlbase.TableDescriptor
}

// AlterTable applies a schema change on a table.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) AlterTable(ctx context.Context, n *tree.AlterTable) (planNode, error) {
	tn, err := n.Table.Normalize()
	if err != nil {
		return nil, err
	}

	var tableDesc *TableDescriptor
	// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
	// TODO(vivek): check if the cache can be used.
	p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
		tableDesc, err = ResolveExistingObject(ctx, p, tn, !n.IfExists, requireTableDesc)
	})
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	return &alterTableNode{n: n, tableDesc: tableDesc}, nil
}

func (n *alterTableNode) startExec(params runParams) error {
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)
	var droppedViews []string
	tn := n.n.Table.TableName()

	for _, cmd := range n.n.Cmds {
		switch t := cmd.(type) {
		case *tree.AlterTableAddColumn:
			d := t.ColumnDef
			if len(d.CheckExprs) > 0 {
				return pgerror.Unimplemented(
					"alter add check", "adding a CHECK constraint via ALTER not supported")
			}
			if d.HasFKConstraint() {
				return pgerror.Unimplemented(
					"alter add fk", "adding a REFERENCES constraint via ALTER not supported")
			}
			if d.Computed.Computed {
				return pgerror.Unimplemented(
					"alter add computed", "adding a computed column via ALTER not supported")
			}
			col, idx, expr, err := sqlbase.MakeColumnDefDescs(d, &params.p.semaCtx, params.EvalContext())
			if err != nil {
				return err
			}
			// If the new column has a DEFAULT expression that uses a sequence, add references between
			// its descriptor and this column descriptor.
			if d.HasDefaultExpr() {
				var changedSeqDescs []*TableDescriptor
				// DDL statements use uncached descriptors, and can view newly added things.
				// TODO(vivek): check if the cache can be used.
				params.p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
					changedSeqDescs, err = maybeAddSequenceDependencies(params.p, n.tableDesc, col, expr, params.EvalContext())
				})
				if err != nil {
					return err
				}
				for _, changedSeqDesc := range changedSeqDescs {
					if err := params.p.writeTableDesc(params.ctx, changedSeqDesc); err != nil {
						return err
					}
					params.p.notifySchemaChange(changedSeqDesc, sqlbase.InvalidMutationID)
				}
			}

			// We're checking to see if a user is trying add a non-nullable column without a default to a
			// non empty table by scanning the primary index span with a limit of 1 to see if any key exists.
			if !col.Nullable && col.DefaultExpr == nil {
				kvs, err := params.p.txn.Scan(params.ctx, n.tableDesc.PrimaryIndexSpan().Key, n.tableDesc.PrimaryIndexSpan().EndKey, 1)
				if err != nil {
					return err
				}
				if len(kvs) > 0 {
					return sqlbase.NewNonNullViolationError(col.Name)
				}
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
				if err := n.tableDesc.AddIndexMutation(*idx, sqlbase.DescriptorMutation_ADD); err != nil {
					return err
				}
			}
			if d.HasColumnFamily() {
				err := n.tableDesc.AddColumnToFamilyMaybeCreate(
					col.Name, string(d.Family.Name), d.Family.Create,
					d.Family.IfNotExists)
				if err != nil {
					return err
				}
			}

		case *tree.AlterTableAddConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			inuseNames := make(map[string]struct{}, len(info))
			for k := range info {
				inuseNames[k] = struct{}{}
			}
			switch d := t.ConstraintDef.(type) {
			case *tree.UniqueConstraintTableDef:
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
				if d.PartitionBy != nil {
					partitioning, err := CreatePartitioning(
						params.ctx, params.p.ExecCfg().Settings,
						params.EvalContext(), n.tableDesc, &idx, d.PartitionBy)
					if err != nil {
						return err
					}
					idx.Partitioning = partitioning
				}
				_, dropped, err := n.tableDesc.FindIndexByName(string(d.Name))
				if err == nil {
					if dropped {
						return fmt.Errorf("index %q being dropped, try again later", d.Name)
					}
				}
				if err := n.tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_ADD); err != nil {
					return err
				}

			case *tree.CheckConstraintTableDef:
				tableName, err := n.n.Table.Normalize()
				if err != nil {
					return err
				}
				ck, err := makeCheckConstraint(params.ctx,
					*n.tableDesc, d, inuseNames, &params.p.semaCtx, params.EvalContext(), *tableName)
				if err != nil {
					return err
				}
				ck.Validity = sqlbase.ConstraintValidity_Unvalidated
				n.tableDesc.Checks = append(n.tableDesc.Checks, ck)
				descriptorChanged = true

			case *tree.ForeignKeyConstraintTableDef:
				if _, err := d.Table.Normalize(); err != nil {
					return err
				}
				for _, colName := range d.FromCols {
					col, _, err := n.tableDesc.FindColumnByName(colName)
					if err != nil {
						return err
					}
					if err := col.CheckCanBeFKRef(); err != nil {
						return err
					}
				}
				affected := make(map[sqlbase.ID]*sqlbase.TableDescriptor)

				// If there are any FKs, we will need to update the table descriptor of the
				// depended-on table (to register this table against its DependedOnBy field).
				// This descriptor must be looked up uncached, and we'll allow FK dependencies
				// on tables that were just added. See the comment at the start of
				// the global-scope resolveFK().
				// TODO(vivek): check if the cache can be used.
				params.p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
					err = params.p.resolveFK(params.ctx, n.tableDesc, d, affected, sqlbase.ConstraintValidity_Unvalidated)
				})
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

		case *tree.AlterTableDropColumn:
			if params.SessionData().SafeUpdates {
				return pgerror.NewDangerousStatementErrorf("ALTER TABLE DROP COLUMN will remove all data in that column")
			}

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

			// If the dropped column uses a sequence, remove references to it from that sequence.
			if len(col.UsesSequenceIds) > 0 {
				if err := removeSequenceDependencies(n.tableDesc, &col, params); err != nil {
					return err
				}
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
					if containsOnlyThisColumn || t.DropBehavior == tree.DropCascade {
						if err := params.p.dropIndexByName(
							params.ctx, tn, tree.UnrestrictedName(idx.Name), n.tableDesc, false,
							t.DropBehavior, ignoreIdxConstraint,
							tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames),
						); err != nil {
							return err
						}
					} else {
						return fmt.Errorf("column %q is referenced by existing index %q", col.Name, idx.Name)
					}
				}
			}

			// Drop check constraints which reference the column.
			validChecks := n.tableDesc.Checks[:0]
			for _, check := range n.tableDesc.Checks {
				if used, err := check.UsesColumn(n.tableDesc, col.ID); err != nil {
					return err
				} else if !used {
					validChecks = append(validChecks, check)
				}
			}

			if len(validChecks) != len(n.tableDesc.Checks) {
				n.tableDesc.Checks = validChecks
				descriptorChanged = true
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

		case *tree.AlterTableDropConstraint:
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
				return fmt.Errorf("UNIQUE constraint depends on index %q, use DROP INDEX with CASCADE if you really want to drop it", t.Constraint)
			case sqlbase.ConstraintTypeCheck:
				for i := range n.tableDesc.Checks {
					if n.tableDesc.Checks[i].Name == name {
						n.tableDesc.Checks = append(n.tableDesc.Checks[:i], n.tableDesc.Checks[i+1:]...)
						descriptorChanged = true
						break
					}
				}
			case sqlbase.ConstraintTypeFK:
				idx, err := n.tableDesc.FindIndexByID(details.Index.ID)
				if err != nil {
					return err
				}
				if err := params.p.removeFKBackReference(params.ctx, n.tableDesc, *idx); err != nil {
					return err
				}
				idx.ForeignKey = sqlbase.ForeignKeyReference{}
				descriptorChanged = true
			default:
				return errors.Errorf("dropping %s constraint %q unsupported", details.Kind, t.Constraint)
			}

		case *tree.AlterTableValidateConstraint:
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

		case tree.ColumnMutationCmd:
			// Column mutations
			col, dropped, err := n.tableDesc.FindColumnByName(t.GetColumn())
			if err != nil {
				return err
			}
			if dropped {
				return fmt.Errorf("column %q in the middle of being dropped", t.GetColumn())
			}
			if err := applyColumnMutation(n.tableDesc, &col, t, params); err != nil {
				return err
			}
			n.tableDesc.UpdateColumnDescriptor(col)
			descriptorChanged = true

		case *tree.AlterTablePartitionBy:
			partitioning, err := CreatePartitioning(
				params.ctx, params.p.ExecCfg().Settings,
				params.EvalContext(),
				n.tableDesc, &n.tableDesc.PrimaryIndex, t.PartitionBy)
			if err != nil {
				return err
			}
			descriptorChanged = !proto.Equal(
				&n.tableDesc.PrimaryIndex.Partitioning,
				&partitioning,
			)
			err = deleteRemovedPartitionZoneConfigs(
				params.ctx, params.p.txn,
				n.tableDesc, &n.tableDesc.PrimaryIndex, &n.tableDesc.PrimaryIndex.Partitioning,
				&partitioning, params.extendedEvalCtx.ExecCfg,
			)
			if err != nil {
				return err
			}
			n.tableDesc.PrimaryIndex.Partitioning = partitioning

		case *tree.AlterTableSetAudit:
			var err error
			descriptorChanged, err = params.p.setAuditMode(params.ctx, n.tableDesc, t.Mode)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unsupported alter command: %T", cmd)
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
		mutationID, err = params.p.createSchemaChangeJob(params.ctx, n.tableDesc,
			tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames))
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
	if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogAlterTable,
		int32(n.tableDesc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			TableName           string
			Statement           string
			User                string
			MutationID          uint32
			CascadeDroppedViews []string
		}{n.n.Table.TableName().FQString(), n.n.String(),
			params.SessionData().User, uint32(mutationID), droppedViews},
	); err != nil {
		return err
	}

	params.p.notifySchemaChange(n.tableDesc, mutationID)

	return nil
}

func (p *planner) setAuditMode(
	ctx context.Context, desc *sqlbase.TableDescriptor, auditMode tree.AuditMode,
) (bool, error) {
	// An auditing config change is itself auditable!
	// We record the event even if the permission check below fails:
	// auditing wants to know who tried to change the settings.
	p.curPlan.auditEvents = append(p.curPlan.auditEvents,
		auditEvent{desc: desc, writing: true})

	// We require root for now. Later maybe use a different permission?
	if err := p.RequireSuperUser(ctx, "change auditing settings on a table"); err != nil {
		return false, err
	}

	return desc.SetAuditMode(auditMode)
}

func (n *alterTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableNode) Close(context.Context)        {}

// applyColumnMutation applies the mutation specified in `mut` to the given
// columnDescriptor, and saves the containing table descriptor. If the column's
// dependencies on sequences change, it updates them as well.
func applyColumnMutation(
	tableDesc *sqlbase.TableDescriptor,
	col *sqlbase.ColumnDescriptor,
	mut tree.ColumnMutationCmd,
	params runParams,
) error {
	switch t := mut.(type) {
	case *tree.AlterTableSetDefault:
		if len(col.UsesSequenceIds) > 0 {
			if err := removeSequenceDependencies(tableDesc, col, params); err != nil {
				return err
			}
		}
		if t.Default == nil {
			col.DefaultExpr = nil
		} else {
			colDatumType := col.Type.ToDatumType()
			expr, err := sqlbase.SanitizeVarFreeExpr(
				t.Default, colDatumType, "DEFAULT", &params.p.semaCtx, params.EvalContext(),
			)
			if err != nil {
				return err
			}
			s := tree.Serialize(t.Default)
			col.DefaultExpr = &s

			// Add references to the sequence descriptors this column is now using.

			// DDL statements avoid the cache to avoid leases, and can view non-public descriptors.
			// TODO(vivek): check if the cache can be used.
			var changedSeqDescs []*TableDescriptor
			params.p.runWithOptions(resolveFlags{allowAdding: true, skipCache: true}, func() {
				changedSeqDescs, err = maybeAddSequenceDependencies(params.p, tableDesc, col, expr, params.EvalContext())
			})
			if err != nil {
				return err
			}
			for _, changedSeqDesc := range changedSeqDescs {
				if err := params.p.writeTableDesc(params.ctx, changedSeqDesc); err != nil {
					return err
				}
				params.p.notifySchemaChange(changedSeqDesc, sqlbase.InvalidMutationID)
			}
		}

	case *tree.AlterTableDropNotNull:
		col.Nullable = true
	}
	return nil
}

func labeledRowValues(cols []sqlbase.ColumnDescriptor, values tree.Datums) string {
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
