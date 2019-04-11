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
	gojson "encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachange"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/proto"
	"golang.org/x/text/language"
)

type alterTableNode struct {
	n         *tree.AlterTable
	tableDesc *MutableTableDescriptor
	// statsData is populated with data for "alter table inject statistics"
	// commands - the JSON stats expressions.
	// It is parallel with n.Cmds (for the inject stats commands).
	statsData map[int]tree.TypedExpr
}

// AlterTable applies a schema change on a table.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) AlterTable(ctx context.Context, n *tree.AlterTable) (planNode, error) {
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, !n.IfExists, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	n.HoistAddColumnConstraints()

	// See if there's any "inject statistics" in the query and type check the
	// expressions.
	statsData := make(map[int]tree.TypedExpr)
	for i, cmd := range n.Cmds {
		injectStats, ok := cmd.(*tree.AlterTableInjectStats)
		if !ok {
			continue
		}
		typedExpr, err := p.analyzeExpr(
			ctx, injectStats.Stats,
			nil, /* sources - no name resolution */
			tree.IndexedVarHelper{},
			types.Jsonb, true, /* requireType */
			"INJECT STATISTICS" /* typingContext */)
		if err != nil {
			return nil, err
		}
		statsData[i] = typedExpr
	}

	return &alterTableNode{n: n, tableDesc: tableDesc, statsData: statsData}, nil
}

func (n *alterTableNode) startExec(params runParams) error {
	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)
	var droppedViews []string
	tn := &n.n.Table

	for i, cmd := range n.n.Cmds {
		switch t := cmd.(type) {
		case *tree.AlterTableAddColumn:
			d := t.ColumnDef
			if d.HasFKConstraint() {
				return pgerror.UnimplementedWithIssueError(32917,
					"adding a REFERENCES constraint while also adding a column via ALTER not supported")
			}

			newDef, seqDbDesc, seqName, seqOpts, err := params.p.processSerialInColumnDef(params.ctx, d, tn)
			if err != nil {
				return err
			}
			if seqName != nil {
				if err := doCreateSequence(params, n.n.String(), seqDbDesc, seqName, seqOpts); err != nil {
					return err
				}
			}
			d = newDef

			col, idx, expr, err := sqlbase.MakeColumnDefDescs(d, &params.p.semaCtx)
			if err != nil {
				return err
			}
			// If the new column has a DEFAULT expression that uses a sequence, add references between
			// its descriptor and this column descriptor.
			if d.HasDefaultExpr() {
				changedSeqDescs, err := maybeAddSequenceDependencies(params.ctx, params.p, n.tableDesc, col, expr)
				if err != nil {
					return err
				}
				for _, changedSeqDesc := range changedSeqDescs {
					if err := params.p.writeSchemaChange(params.ctx, changedSeqDesc, sqlbase.InvalidMutationID); err != nil {
						return err
					}
				}
			}

			// We're checking to see if a user is trying add a non-nullable column without a default to a
			// non empty table by scanning the primary index span with a limit of 1 to see if any key exists.
			if !col.Nullable && (col.DefaultExpr == nil && !col.IsComputed()) {
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
					return pgerror.NewErrorf(pgerror.CodeObjectNotInPrerequisiteStateError,
						"column %q being dropped, try again later", col.Name)
				}
				if t.IfNotExists {
					continue
				}
			}

			n.tableDesc.AddColumnMutation(col, sqlbase.DescriptorMutation_ADD)
			if idx != nil {
				if err := n.tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_ADD); err != nil {
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

			if d.IsComputed() {
				if err := validateComputedColumn(n.tableDesc, d, &params.p.semaCtx); err != nil {
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
					return pgerror.NewErrorf(pgerror.CodeSyntaxError,
						"multiple primary keys for table %q are not allowed", n.tableDesc.Name)
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
						return pgerror.NewErrorf(pgerror.CodeObjectNotInPrerequisiteStateError,
							"index %q being dropped, try again later", d.Name)
					}
				}
				if err := n.tableDesc.AddIndexMutation(&idx, sqlbase.DescriptorMutation_ADD); err != nil {
					return err
				}

			case *tree.CheckConstraintTableDef:
				ck, err := MakeCheckConstraint(params.ctx,
					n.tableDesc, d, inuseNames, &params.p.semaCtx, n.n.Table)
				if err != nil {
					return err
				}
				ck.Validity = sqlbase.ConstraintValidity_Validating
				n.tableDesc.AddCheckValidationMutation(ck)

			case *tree.ForeignKeyConstraintTableDef:
				for _, colName := range d.FromCols {
					col, err := n.tableDesc.FindActiveColumnByName(string(colName))
					if err != nil {
						if _, dropped, inactiveErr := n.tableDesc.FindColumnByName(colName); inactiveErr == nil && !dropped {
							return pgerror.UnimplementedWithIssueError(32917,
								"adding a REFERENCES constraint while the column is being added not supported")
						}
						return err
					}

					if err := col.CheckCanBeFKRef(); err != nil {
						return err
					}
				}
				affected := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)

				// If there are any FKs, we will need to update the table descriptor of the
				// depended-on table (to register this table against its DependedOnBy field).
				// This descriptor must be looked up uncached, and we'll allow FK dependencies
				// on tables that were just added. See the comment at the start of
				// the global-scope resolveFK().
				// TODO(vivek): check if the cache can be used.
				params.p.runWithOptions(resolveFlags{skipCache: true}, func() {
					// Check whether the table is empty, and pass the result to resolveFK(). If
					// the table is empty, then resolveFK will automatically add the necessary
					// index for a fk constraint if the index does not exist.
					kvs, scanErr := params.p.txn.Scan(params.ctx, n.tableDesc.PrimaryIndexSpan().Key, n.tableDesc.PrimaryIndexSpan().EndKey, 1)
					if scanErr != nil {
						err = scanErr
						return
					}
					var tableState FKTableState
					if len(kvs) == 0 {
						tableState = EmptyTable
					} else {
						tableState = NonEmptyTable
					}
					err = params.p.resolveFK(params.ctx, n.tableDesc, d, affected, tableState)
				})
				if err != nil {
					return err
				}
				descriptorChanged = true
				for _, updated := range affected {
					if err := params.p.writeSchemaChange(params.ctx, updated, sqlbase.InvalidMutationID); err != nil {
						return err
					}
				}

			default:
				return pgerror.NewAssertionErrorf(
					"unsupported constraint: %T", t.ConstraintDef)
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
				if err := removeSequenceDependencies(n.tableDesc, col, params); err != nil {
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
				return pgerror.NewErrorf(pgerror.CodeInvalidColumnReferenceError,
					"column %q is referenced by the primary key", col.Name)
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
						return pgerror.NewErrorf(pgerror.CodeInvalidColumnReferenceError,
							"column %q is referenced by existing index %q", col.Name, idx.Name)
					}
				}
			}

			// Drop check constraints which reference the column.
			validChecks := n.tableDesc.Checks[:0]
			for _, check := range n.tableDesc.AllActiveAndInactiveChecks() {
				if used, err := check.UsesColumn(n.tableDesc.TableDesc(), col.ID); err != nil {
					return err
				} else if used {
					if check.Validity == sqlbase.ConstraintValidity_Validating {
						return pgerror.NewErrorf(pgerror.CodeObjectNotInPrerequisiteStateError,
							"referencing constraint %q in the middle of being added, try again later", check.Name)
					}
				} else {
					validChecks = append(validChecks, check)
				}
			}

			if len(validChecks) != len(n.tableDesc.Checks) {
				n.tableDesc.Checks = validChecks
				descriptorChanged = true
			}

			if err != nil {
				return err
			}
			if err := params.p.removeColumnComment(params.ctx, n.tableDesc.ID, col.ID); err != nil {
				return err
			}

			found := false
			for i := range n.tableDesc.Columns {
				if n.tableDesc.Columns[i].ID == col.ID {
					n.tableDesc.AddColumnMutation(col, sqlbase.DescriptorMutation_DROP)
					// Use [:i:i] to prevent reuse of existing slice, or outstanding refs
					// to ColumnDescriptors may unexpectedly change.
					n.tableDesc.Columns = append(n.tableDesc.Columns[:i:i], n.tableDesc.Columns[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				return pgerror.NewErrorf(pgerror.CodeObjectNotInPrerequisiteStateError,
					"column %q in the middle of being added, try again later", t.Column)
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
				return pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
					"constraint %q does not exist", t.Constraint)
			}
			if err := n.tableDesc.DropConstraint(
				name, details,
				func(desc *sqlbase.MutableTableDescriptor, idx *sqlbase.IndexDescriptor) error {
					return params.p.removeFKBackReference(params.ctx, desc, idx)
				}); err != nil {
				return err
			}
			descriptorChanged = true

		case *tree.AlterTableValidateConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			constraint, ok := info[name]
			if !ok {
				return pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
					"constraint %q does not exist", t.Constraint)
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
					return pgerror.NewErrorf(pgerror.CodeObjectNotInPrerequisiteStateError,
						"constraint %q in the middle of being added, try again later", t.Constraint)
				}

				ck := n.tableDesc.Checks[idx]
				if err := validateCheckExpr(
					params.ctx, ck.Expr, n.tableDesc.TableDesc(), params.EvalContext().InternalExecutor, params.EvalContext().Txn,
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
					return pgerror.NewAssertionErrorf(
						"constraint returned by GetConstraintInfo not found")
				}
				idx, err := n.tableDesc.FindIndexByID(id)
				if err != nil {
					return pgerror.NewAssertionErrorWithWrappedErrf(err, "")
				}
				if err := params.p.validateForeignKey(params.ctx, n.tableDesc.TableDesc(), idx); err != nil {
					return err
				}
				idx.ForeignKey.Validity = sqlbase.ConstraintValidity_Validated
				descriptorChanged = true

			default:
				return pgerror.NewErrorf(pgerror.CodeWrongObjectTypeError,
					"constraint %q of relation %q is not a foreign key or check constraint",
					tree.ErrString(&t.Constraint), tree.ErrString(&n.n.Table))
			}

		case tree.ColumnMutationCmd:
			// Column mutations
			col, dropped, err := n.tableDesc.FindColumnByName(t.GetColumn())
			if err != nil {
				return err
			}
			if dropped {
				return pgerror.NewErrorf(pgerror.CodeObjectNotInPrerequisiteStateError,
					"column %q in the middle of being dropped", t.GetColumn())
			}
			// Apply mutations to copy of column descriptor.
			if err := applyColumnMutation(n.tableDesc, col, t, params); err != nil {
				return err
			}
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
				n.tableDesc.TableDesc(), &n.tableDesc.PrimaryIndex, &n.tableDesc.PrimaryIndex.Partitioning,
				&partitioning, params.extendedEvalCtx.ExecCfg,
			)
			if err != nil {
				return err
			}
			n.tableDesc.PrimaryIndex.Partitioning = partitioning

		case *tree.AlterTableSetAudit:
			var err error
			descriptorChanged, err = params.p.setAuditMode(params.ctx, n.tableDesc.TableDesc(), t.Mode)
			if err != nil {
				return err
			}

		case *tree.AlterTableInjectStats:
			sd, ok := n.statsData[i]
			if !ok {
				return pgerror.NewAssertionErrorf("missing stats data")
			}
			if err := injectTableStats(params, n.tableDesc.TableDesc(), sd); err != nil {
				return err
			}

		case *tree.AlterTableRenameColumn:
			descChanged, err := params.p.renameColumn(params.ctx, n.tableDesc, &t.Column, &t.NewName)
			if err != nil {
				return err
			}
			descriptorChanged = descChanged

		case *tree.AlterTableRenameConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil)
			if err != nil {
				return err
			}
			details, ok := info[string(t.Constraint)]
			if !ok {
				return pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
					"constraint %q does not exist", tree.ErrString(&t.Constraint))
			}
			if t.Constraint == t.NewName {
				// Nothing to do.
				break
			}

			if _, ok := info[string(t.NewName)]; ok {
				return pgerror.NewErrorf(pgerror.CodeDuplicateObjectError,
					"duplicate constraint name: %q", tree.ErrString(&t.NewName))
			}

			if err := params.p.CheckPrivilege(params.ctx, n.tableDesc, privilege.CREATE); err != nil {
				return err
			}

			depViewRenameError := func(objType string, refTableID sqlbase.ID) error {
				return params.p.dependentViewRenameError(params.ctx,
					objType, tree.ErrString(&t.NewName), n.tableDesc.ParentID, refTableID)
			}

			if err := n.tableDesc.RenameConstraint(
				details, string(t.Constraint), string(t.NewName), depViewRenameError); err != nil {
				return err
			}
			descriptorChanged = true

		default:
			return pgerror.NewAssertionErrorf("unsupported alter command: %T", cmd)
		}

		// Allocate IDs now, so new IDs are available to subsequent commands
		if err := n.tableDesc.AllocateIDs(); err != nil {
			return err
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

	mutationID := sqlbase.InvalidMutationID
	if addedMutations {
		var err error
		mutationID, err = params.p.createOrUpdateSchemaChangeJob(params.ctx, n.tableDesc,
			tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames))
		if err != nil {
			return err
		}
	}

	if err := params.p.writeSchemaChange(params.ctx, n.tableDesc, mutationID); err != nil {
		return err
	}

	// Record this table alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
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
		}{n.n.Table.FQString(), n.n.String(),
			params.SessionData().User, uint32(mutationID), droppedViews},
	)
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
	tableDesc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	mut tree.ColumnMutationCmd,
	params runParams,
) error {
	switch t := mut.(type) {
	case *tree.AlterTableAlterColumnType:
		// Convert the parsed type into one of the basic datum types.
		datum := coltypes.CastTargetToDatumType(t.ToType)

		// Special handling for STRING COLLATE xy to verify that we recognize the language.
		if t.Collation != "" {
			if types.IsStringType(datum) {
				if _, err := language.Parse(t.Collation); err != nil {
					return pgerror.NewErrorf(pgerror.CodeSyntaxError, `invalid locale %s`, t.Collation)
				}
				datum = types.TCollatedString{Locale: t.Collation}
			} else {
				return pgerror.NewError(pgerror.CodeSyntaxError, "COLLATE can only be used with string types")
			}
		}

		// First pass at converting the datum type to the SQL column type.
		nextType, err := sqlbase.DatumTypeToColumnType(datum)
		if err != nil {
			return err
		}

		// Finish populating width, precision, etc. from parsed data.
		nextType, err = sqlbase.PopulateTypeAttrs(nextType, t.ToType)
		if err != nil {
			return err
		}

		// No-op if the types are Equal.  We don't use Equivalent here
		// because the user may want to change the visible type of the
		// column without changing the underlying semantic type.
		if col.Type.Identical(&nextType) {
			return nil
		}

		kind, err := schemachange.ClassifyConversion(&col.Type, &nextType)
		if err != nil {
			return err
		}

		switch kind {
		case schemachange.ColumnConversionDangerous, schemachange.ColumnConversionImpossible:
			// We're not going to make it impossible for the user to perform
			// this conversion, but we do want them to explicit about
			// what they're going for.
			return pgerror.NewErrorf(pgerror.CodeCannotCoerceError,
				"the requested type conversion (%s -> %s) requires an explicit USING expression",
				col.Type.SQLString(), nextType.SQLString())
		case schemachange.ColumnConversionTrivial:
			col.Type = nextType
		default:
			return pgerror.UnimplementedWithIssueDetailError(9851,
				fmt.Sprintf("%s->%s", col.Type.SQLString(), nextType.SQLString()),
				"type conversion not yet implemented")
		}

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
				t.Default, colDatumType, "DEFAULT", &params.p.semaCtx, true, /* allowImpure */
			)
			if err != nil {
				return err
			}
			s := tree.Serialize(t.Default)
			col.DefaultExpr = &s

			// Add references to the sequence descriptors this column is now using.
			changedSeqDescs, err := maybeAddSequenceDependencies(params.ctx, params.p, tableDesc, col, expr)
			if err != nil {
				return err
			}
			for _, changedSeqDesc := range changedSeqDescs {
				if err := params.p.writeSchemaChange(params.ctx, changedSeqDesc, sqlbase.InvalidMutationID); err != nil {
					return err
				}
			}
		}

	case *tree.AlterTableDropNotNull:
		col.Nullable = true

	case *tree.AlterTableDropStored:
		if !col.IsComputed() {
			return pgerror.NewErrorf(pgerror.CodeInvalidColumnDefinitionError,
				"column %q is not a computed column", col.Name)
		}
		col.ComputeExpr = nil
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

// injectTableStats implements the INJECT STATISTICS command, which deletes any
// existing statistics on the table and replaces them with the statistics in the
// given json object (in the same format as the result of SHOW STATISTICS USING
// JSON). This is useful for reproducing planning issues without importing the
// data.
func injectTableStats(
	params runParams, desc *sqlbase.TableDescriptor, statsExpr tree.TypedExpr,
) error {
	val, err := statsExpr.Eval(params.EvalContext())
	if err != nil {
		return err
	}
	if val == tree.DNull {
		return pgerror.NewError(pgerror.CodeSyntaxError,
			"statistics cannot be NULL")
	}
	jsonStr := val.(*tree.DJSON).JSON.String()
	var jsonStats []stats.JSONStatistic
	if err := gojson.Unmarshal([]byte(jsonStr), &jsonStats); err != nil {
		return err
	}

	// First, delete all statistics for the table.
	if _ /* rows */, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
		params.ctx,
		"delete-stats",
		params.EvalContext().Txn,
		`DELETE FROM system.table_statistics WHERE "tableID" = $1`, desc.ID,
	); err != nil {
		return pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
			"failed to delete old stats")
	}

	// Insert each statistic.
	for i := range jsonStats {
		s := &jsonStats[i]
		h, err := s.GetHistogram(params.EvalContext())
		if err != nil {
			return err
		}
		// histogram will be passed to the INSERT statement; we want it to be a
		// nil interface{} if we don't generate a histogram.
		var histogram interface{}
		if h != nil {
			histogram, err = protoutil.Marshal(h)
			if err != nil {
				return err
			}
		}

		columnIDs := tree.NewDArray(types.Int)
		for _, colName := range s.Columns {
			colDesc, _, err := desc.FindColumnByName(tree.Name(colName))
			if err != nil {
				return err
			}
			if err := columnIDs.Append(tree.NewDInt(tree.DInt(colDesc.ID))); err != nil {
				return err
			}
		}
		var name interface{}
		if s.Name != "" {
			name = s.Name
		}
		if _ /* rows */, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"insert-stats",
			params.EvalContext().Txn,
			`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"createdAt",
					"rowCount",
					"distinctCount",
					"nullCount",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			desc.ID,
			name,
			columnIDs,
			s.CreatedAt,
			s.RowCount,
			s.DistinctCount,
			s.NullCount,
			histogram,
		); err != nil {
			return pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
				"failed to insert stats")
		}
	}

	// Invalidate the local cache synchronously; this guarantees that the next
	// statement in the same session won't use a stale cache (whereas the gossip
	// update is handled asynchronously).
	params.extendedEvalCtx.ExecCfg.TableStatsCache.InvalidateTableStats(params.ctx, desc.ID)

	return stats.GossipTableStatAdded(params.extendedEvalCtx.ExecCfg.Gossip, desc.ID)
}

func (p *planner) removeColumnComment(
	ctx context.Context, tableID sqlbase.ID, columnID sqlbase.ColumnID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx,
		"delete-column-comment",
		p.txn,
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.ColumnCommentType,
		tableID,
		columnID)

	return err
}
