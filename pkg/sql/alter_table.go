// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
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
	tableDesc, err := p.ResolveMutableTableDescriptorEx(
		ctx, n.Table, !n.IfExists, resolver.ResolveRequireTableDesc,
	)
	if errors.Is(err, resolver.ErrNoPrimaryKey) {
		if len(n.Cmds) > 0 && isAlterCmdValidWithoutPrimaryKey(n.Cmds[0]) {
			tableDesc, err = p.ResolveMutableTableDescriptorExAllowNoPrimaryKey(
				ctx, n.Table, !n.IfExists, resolver.ResolveRequireTableDesc,
			)
		}
	}
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

	return &alterTableNode{
		n:         n,
		tableDesc: tableDesc,
		statsData: statsData,
	}, nil
}

func isAlterCmdValidWithoutPrimaryKey(cmd tree.AlterTableCmd) bool {
	switch t := cmd.(type) {
	case *tree.AlterTableAlterPrimaryKey:
		return true
	case *tree.AlterTableAddConstraint:
		cs, ok := t.ConstraintDef.(*tree.UniqueConstraintTableDef)
		if ok && cs.PrimaryKey {
			return true
		}
	default:
		return false
	}
	return false
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because ALTER TABLE performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *alterTableNode) ReadingOwnWrites() {}

func (n *alterTableNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounter("table"))

	// Commands can either change the descriptor directly (for
	// alterations that don't require a backfill) or add a mutation to
	// the list.
	descriptorChanged := false
	origNumMutations := len(n.tableDesc.Mutations)
	var droppedViews []string
	resolved := params.p.ResolvedName(n.n.Table)
	tn, ok := resolved.(*tree.TableName)
	if !ok {
		return errors.AssertionFailedf(
			"%q was not resolved as a table but is %T", resolved, resolved)
	}

	for i, cmd := range n.n.Cmds {
		telemetry.Inc(cmd.TelemetryCounter())

		if !n.tableDesc.HasPrimaryKey() && !isAlterCmdValidWithoutPrimaryKey(cmd) {
			return errors.Newf("table %q does not have a primary key, cannot perform%s", n.tableDesc.Name, tree.AsString(cmd))
		}

		switch t := cmd.(type) {
		case *tree.AlterTableAddColumn:
			var err error
			params.p.runWithOptions(resolveFlags{contextDatabaseID: n.tableDesc.ParentID}, func() {
				err = params.p.addColumnImpl(params, n, tn, n.tableDesc, t)
			})
			if err != nil {
				return err
			}
		case *tree.AlterTableAddConstraint:
			switch d := t.ConstraintDef.(type) {
			case *tree.UniqueConstraintTableDef:
				if d.PrimaryKey {
					// We only support "adding" a primary key when we are using the
					// default rowid primary index or if a DROP PRIMARY KEY statement
					// was processed before this statement. If a DROP PRIMARY KEY
					// statement was processed, then n.tableDesc.HasPrimaryKey() = false.
					if n.tableDesc.HasPrimaryKey() && !n.tableDesc.IsPrimaryIndexDefaultRowID() {
						return pgerror.Newf(pgcode.InvalidTableDefinition,
							"multiple primary keys for table %q are not allowed", n.tableDesc.Name)
					}

					// Translate this operation into an ALTER PRIMARY KEY command.
					alterPK := &tree.AlterTableAlterPrimaryKey{
						Columns:    d.Columns,
						Sharded:    d.Sharded,
						Interleave: d.Interleave,
					}
					if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, alterPK); err != nil {
						return err
					}
					continue
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
						return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"index %q being dropped, try again later", d.Name)
					}
				}
				if err := n.tableDesc.AddIndexMutation(&idx, sqlbase.DescriptorMutation_ADD); err != nil {
					return err
				}

			case *tree.CheckConstraintTableDef:
				var err error
				params.p.runWithOptions(resolveFlags{contextDatabaseID: n.tableDesc.ParentID}, func() {
					info, infoErr := n.tableDesc.GetConstraintInfo(params.ctx, nil, params.ExecCfg().Codec)
					if err != nil {
						err = infoErr
						return
					}
					ckBuilder := schemaexpr.NewCheckConstraintBuilder(params.ctx, *tn, n.tableDesc, &params.p.semaCtx)
					for k := range info {
						ckBuilder.MarkNameInUse(k)
					}
					ck, buildErr := ckBuilder.Build(d)
					if buildErr != nil {
						err = buildErr
						return
					}
					if t.ValidationBehavior == tree.ValidationDefault {
						ck.Validity = sqlbase.ConstraintValidity_Validating
					} else {
						ck.Validity = sqlbase.ConstraintValidity_Unvalidated
					}
					n.tableDesc.AddCheckMutation(ck, sqlbase.DescriptorMutation_ADD)
				})
				if err != nil {
					return err
				}

			case *tree.ForeignKeyConstraintTableDef:
				for _, colName := range d.FromCols {
					col, err := n.tableDesc.FindActiveOrNewColumnByName(colName)
					if err != nil {
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
				var err error
				params.p.runWithOptions(resolveFlags{skipCache: true}, func() {
					// Check whether the table is empty, and pass the result to resolveFK(). If
					// the table is empty, then resolveFK will automatically add the necessary
					// index for a fk constraint if the index does not exist.
					span := n.tableDesc.PrimaryIndexSpan(params.ExecCfg().Codec)
					kvs, scanErr := params.p.txn.Scan(params.ctx, span.Key, span.EndKey, 1)
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
					err = params.p.resolveFK(params.ctx, n.tableDesc, d, affected, tableState, t.ValidationBehavior)
				})
				if err != nil {
					return err
				}
				descriptorChanged = true
				for _, updated := range affected {
					if err := params.p.writeSchemaChange(
						params.ctx, updated, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
					); err != nil {
						return err
					}
				}
				// TODO(lucy): Validate() can't be called here because it reads the
				// referenced table descs, which may have to be upgraded to the new FK
				// representation. That requires reading the original table descriptor
				// (which the backreference points to) from KV, but we haven't written
				// the updated table desc yet. We can restore the call to Validate()
				// after running a migration of all table descriptors, making it
				// unnecessary to read the original table desc from KV.
				// if err := n.tableDesc.Validate(params.ctx, params.p.txn); err != nil {
				// 	return err
				// }

			default:
				return errors.AssertionFailedf(
					"unsupported constraint: %T", t.ConstraintDef)
			}

		case *tree.AlterTableAlterPrimaryKey:
			if err := params.p.AlterPrimaryKey(params.ctx, n.tableDesc, t); err != nil {
				return err
			}
			// Mark descriptorChanged so that a mutation job is scheduled at the end of startExec.
			descriptorChanged = true

		case *tree.AlterTableDropColumn:
			if params.SessionData().SafeUpdates {
				return pgerror.DangerousStatementf("ALTER TABLE DROP COLUMN will remove all data in that column")
			}

			colToDrop, dropped, err := n.tableDesc.FindColumnByName(t.Column)
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
			if len(colToDrop.UsesSequenceIds) > 0 {
				if err := params.p.removeSequenceDependencies(params.ctx, n.tableDesc, colToDrop); err != nil {
					return err
				}
			}

			// You can't remove a column that owns a sequence that is depended on
			// by another column
			if err := params.p.canRemoveAllColumnOwnedSequences(params.ctx, n.tableDesc, colToDrop, t.DropBehavior); err != nil {
				return err
			}

			if err := params.p.dropSequencesOwnedByCol(params.ctx, colToDrop); err != nil {
				return err
			}

			// You can't drop a column depended on by a view unless CASCADE was
			// specified.
			for _, ref := range n.tableDesc.DependedOnBy {
				found := false
				for _, colID := range ref.ColumnIDs {
					if colID == colToDrop.ID {
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
				jobDesc := fmt.Sprintf("removing view %q dependent on column %q which is being dropped",
					viewDesc.Name, colToDrop.ColName())
				droppedViews, err = params.p.removeDependentView(params.ctx, n.tableDesc, viewDesc, jobDesc)
				if err != nil {
					return err
				}
			}

			// We cannot remove this column if there are computed columns that use it.
			computedColValidator := schemaexpr.NewComputedColumnValidator(
				params.ctx,
				n.tableDesc,
				&params.p.semaCtx,
				tn,
			)
			if err := computedColValidator.ValidateNoDependents(colToDrop); err != nil {
				return err
			}

			if n.tableDesc.PrimaryIndex.ContainsColumnID(colToDrop.ID) {
				return pgerror.Newf(pgcode.InvalidColumnReference,
					"column %q is referenced by the primary key", colToDrop.Name)
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
					if id == colToDrop.ID {
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
					if id == colToDrop.ID {
						containsThisColumn = true
					}
				}
				// The loop above this comment is for the old STORING encoding. The
				// loop below is for the new encoding (where the STORING columns are
				// always in the value part of a KV).
				for _, id := range idx.StoreColumnIDs {
					if id == colToDrop.ID {
						containsThisColumn = true
					}
				}

				// Perform the DROP.
				if containsThisColumn {
					if containsOnlyThisColumn || t.DropBehavior == tree.DropCascade {
						jobDesc := fmt.Sprintf("removing index %q dependent on column %q which is being"+
							" dropped; full details: %s", idx.Name, colToDrop.ColName(),
							tree.AsStringWithFQNames(n.n, params.Ann()))
						if err := params.p.dropIndexByName(
							params.ctx, tn, tree.UnrestrictedName(idx.Name), n.tableDesc, false,
							t.DropBehavior, ignoreIdxConstraint, jobDesc,
						); err != nil {
							return err
						}
					} else {
						return pgerror.Newf(pgcode.InvalidColumnReference,
							"column %q is referenced by existing index %q", colToDrop.Name, idx.Name)
					}
				}
			}

			// Drop check constraints which reference the column.
			validChecks := n.tableDesc.Checks[:0]
			for _, check := range n.tableDesc.AllActiveAndInactiveChecks() {
				if used, err := check.UsesColumn(n.tableDesc.TableDesc(), colToDrop.ID); err != nil {
					return err
				} else if used {
					if check.Validity == sqlbase.ConstraintValidity_Validating {
						return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
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
			if err := params.p.removeColumnComment(params.ctx, n.tableDesc.ID, colToDrop.ID); err != nil {
				return err
			}

			// Since we are able to drop indexes used by foreign keys on the origin side,
			// the drop index codepaths aren't going to remove dependent FKs, so we
			// need to do that here.
			if params.p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.VersionNoOriginFKIndexes) {
				// We update the FK's slice in place here.
				sliceIdx := 0
				for i := range n.tableDesc.OutboundFKs {
					n.tableDesc.OutboundFKs[sliceIdx] = n.tableDesc.OutboundFKs[i]
					sliceIdx++
					fk := &n.tableDesc.OutboundFKs[i]
					if sqlbase.ColumnIDs(fk.OriginColumnIDs).Contains(colToDrop.ID) {
						sliceIdx--
						if err := params.p.removeFKBackReference(params.ctx, n.tableDesc, fk); err != nil {
							return err
						}
					}
				}
				n.tableDesc.OutboundFKs = n.tableDesc.OutboundFKs[:sliceIdx]
			}

			found := false
			for i := range n.tableDesc.Columns {
				if n.tableDesc.Columns[i].ID == colToDrop.ID {
					n.tableDesc.AddColumnMutation(colToDrop, sqlbase.DescriptorMutation_DROP)
					// Use [:i:i] to prevent reuse of existing slice, or outstanding refs
					// to ColumnDescriptors may unexpectedly change.
					n.tableDesc.Columns = append(n.tableDesc.Columns[:i:i], n.tableDesc.Columns[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q in the middle of being added, try again later", t.Column)
			}
			if err := n.tableDesc.Validate(params.ctx, params.p.txn, params.ExecCfg().Codec); err != nil {
				return err
			}

		case *tree.AlterTableDropConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil, params.ExecCfg().Codec)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			details, ok := info[name]
			if !ok {
				if t.IfExists {
					continue
				}
				return pgerror.Newf(pgcode.UndefinedObject,
					"constraint %q does not exist", t.Constraint)
			}
			if err := n.tableDesc.DropConstraint(
				params.ctx,
				name, details,
				func(desc *sqlbase.MutableTableDescriptor, ref *sqlbase.ForeignKeyConstraint) error {
					return params.p.removeFKBackReference(params.ctx, desc, ref)
				}, params.ExecCfg().Settings); err != nil {
				return err
			}
			descriptorChanged = true
			if err := n.tableDesc.Validate(params.ctx, params.p.txn, params.ExecCfg().Codec); err != nil {
				return err
			}

		case *tree.AlterTableValidateConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil, params.ExecCfg().Codec)
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			constraint, ok := info[name]
			if !ok {
				return pgerror.Newf(pgcode.UndefinedObject,
					"constraint %q does not exist", t.Constraint)
			}
			if !constraint.Unvalidated {
				continue
			}
			switch constraint.Kind {
			case sqlbase.ConstraintTypeCheck:
				found := false
				var ck *sqlbase.TableDescriptor_CheckConstraint
				for _, c := range n.tableDesc.Checks {
					// If the constraint is still being validated, don't allow VALIDATE CONSTRAINT to run
					if c.Name == name && c.Validity != sqlbase.ConstraintValidity_Validating {
						found = true
						ck = c
						break
					}
				}
				if !found {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint %q in the middle of being added, try again later", t.Constraint)
				}
				if err := validateCheckInTxn(
					params.ctx, params.p.LeaseMgr(), &params.p.semaCtx, params.EvalContext(), n.tableDesc, params.EvalContext().Txn, name,
				); err != nil {
					return err
				}
				ck.Validity = sqlbase.ConstraintValidity_Validated

			case sqlbase.ConstraintTypeFK:
				var foundFk *sqlbase.ForeignKeyConstraint
				for i := range n.tableDesc.OutboundFKs {
					fk := &n.tableDesc.OutboundFKs[i]
					// If the constraint is still being validated, don't allow VALIDATE CONSTRAINT to run
					if fk.Name == name && fk.Validity != sqlbase.ConstraintValidity_Validating {
						foundFk = fk
						break
					}
				}
				if foundFk == nil {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint %q in the middle of being added, try again later", t.Constraint)
				}
				if err := validateFkInTxn(
					params.ctx, params.p.LeaseMgr(), params.EvalContext(), n.tableDesc, params.EvalContext().Txn, name,
				); err != nil {
					return err
				}
				foundFk.Validity = sqlbase.ConstraintValidity_Validated

			default:
				return pgerror.Newf(pgcode.WrongObjectType,
					"constraint %q of relation %q is not a foreign key or check constraint",
					tree.ErrString(&t.Constraint), tree.ErrString(n.n.Table))
			}
			descriptorChanged = true

		case tree.ColumnMutationCmd:
			// Column mutations
			col, dropped, err := n.tableDesc.FindColumnByName(t.GetColumn())
			if err != nil {
				return err
			}
			if dropped {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q in the middle of being dropped", t.GetColumn())
			}
			// Apply mutations to copy of column descriptor.
			if err := applyColumnMutation(params.ctx, n.tableDesc, col, t, params, n.n.Cmds, tn); err != nil {
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
			descriptorChanged, err = params.p.setAuditMode(params.ctx, n.tableDesc, t.Mode)
			if err != nil {
				return err
			}

		case *tree.AlterTableInjectStats:
			sd, ok := n.statsData[i]
			if !ok {
				return errors.AssertionFailedf("missing stats data")
			}
			if !params.p.EvalContext().TxnImplicit {
				return errors.New("cannot inject statistics in an explicit transaction")
			}
			if err := injectTableStats(params, n.tableDesc.TableDesc(), sd); err != nil {
				return err
			}

		case *tree.AlterTableRenameColumn:
			const allowRenameOfShardColumn = false
			descChanged, err := params.p.renameColumn(params.ctx, n.tableDesc,
				&t.Column, &t.NewName, allowRenameOfShardColumn)
			if err != nil {
				return err
			}
			descriptorChanged = descChanged

		case *tree.AlterTableRenameConstraint:
			info, err := n.tableDesc.GetConstraintInfo(params.ctx, nil, params.ExecCfg().Codec)
			if err != nil {
				return err
			}
			details, ok := info[string(t.Constraint)]
			if !ok {
				return pgerror.Newf(pgcode.UndefinedObject,
					"constraint %q does not exist", tree.ErrString(&t.Constraint))
			}
			if t.Constraint == t.NewName {
				// Nothing to do.
				break
			}

			if _, ok := info[string(t.NewName)]; ok {
				return pgerror.Newf(pgcode.DuplicateObject,
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
				details, string(t.Constraint), string(t.NewName), depViewRenameError, func(desc *MutableTableDescriptor, ref *sqlbase.ForeignKeyConstraint, newName string) error {
					return params.p.updateFKBackReferenceName(params.ctx, desc, ref, newName)
				}); err != nil {
				return err
			}
			descriptorChanged = true

		default:
			return errors.AssertionFailedf("unsupported alter command: %T", cmd)
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
		mutationID = n.tableDesc.ClusterVersion.NextMutationID
	}
	if err := params.p.writeSchemaChange(
		params.ctx, n.tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
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
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			TableName           string
			Statement           string
			User                string
			MutationID          uint32
			CascadeDroppedViews []string
		}{params.p.ResolvedName(n.n.Table).FQString(), n.n.String(),
			params.SessionData().User, uint32(mutationID), droppedViews},
	)
}

func (p *planner) setAuditMode(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor, auditMode tree.AuditMode,
) (bool, error) {
	// An auditing config change is itself auditable!
	// We record the event even if the permission check below fails:
	// auditing wants to know who tried to change the settings.
	p.curPlan.auditEvents = append(p.curPlan.auditEvents,
		auditEvent{desc: desc, writing: true})

	// We require root for now. Later maybe use a different permission?
	if err := p.RequireAdminRole(ctx, "change auditing settings on a table"); err != nil {
		return false, err
	}

	telemetry.Inc(sqltelemetry.SchemaSetAuditModeCounter(auditMode.TelemetryName()))

	return desc.SetAuditMode(auditMode)
}

func (n *alterTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableNode) Close(context.Context)        {}

// addIndexMutationWithSpecificPrimaryKey adds an index mutation into the given table descriptor, but sets up
// the index with ExtraColumnIDs from the given index, rather than the table's primary key.
func addIndexMutationWithSpecificPrimaryKey(
	table *sqlbase.MutableTableDescriptor,
	toAdd *sqlbase.IndexDescriptor,
	primary *sqlbase.IndexDescriptor,
) error {
	// Reset the ID so that a call to AllocateIDs will set up the index.
	toAdd.ID = 0
	if err := table.AddIndexMutation(toAdd, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := table.AllocateIDs(); err != nil {
		return err
	}
	// Use the columns in the given primary index to construct this indexes ExtraColumnIDs list.
	toAdd.ExtraColumnIDs = nil
	for _, colID := range primary.ColumnIDs {
		if !toAdd.ContainsColumnID(colID) {
			toAdd.ExtraColumnIDs = append(toAdd.ExtraColumnIDs, colID)
		}
	}
	return nil
}

// applyColumnMutation applies the mutation specified in `mut` to the given
// columnDescriptor, and saves the containing table descriptor. If the column's
// dependencies on sequences change, it updates them as well.
func applyColumnMutation(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	mut tree.ColumnMutationCmd,
	params runParams,
	cmds tree.AlterTableCmds,
	tn *tree.TableName,
) error {
	switch t := mut.(type) {
	case *tree.AlterTableAlterColumnType:
		return AlterColumnType(ctx, tableDesc, col, t, params, cmds, tn)

	case *tree.AlterTableSetDefault:
		if len(col.UsesSequenceIds) > 0 {
			if err := params.p.removeSequenceDependencies(params.ctx, tableDesc, col); err != nil {
				return err
			}
		}
		if t.Default == nil {
			col.DefaultExpr = nil
		} else {
			colDatumType := col.Type
			expr, err := sqlbase.SanitizeVarFreeExpr(
				params.ctx, t.Default, colDatumType, "DEFAULT", &params.p.semaCtx, tree.VolatilityVolatile,
			)
			if err != nil {
				return err
			}
			s := tree.Serialize(expr)
			col.DefaultExpr = &s

			// Add references to the sequence descriptors this column is now using.
			changedSeqDescs, err := maybeAddSequenceDependencies(
				params.ctx, params.p, tableDesc, col, expr, nil, /* backrefs */
			)
			if err != nil {
				return err
			}
			for _, changedSeqDesc := range changedSeqDescs {
				if err := params.p.writeSchemaChange(
					params.ctx, changedSeqDesc, sqlbase.InvalidMutationID,
					fmt.Sprintf("updating dependent sequence %s(%d) for table %s(%d)",
						changedSeqDesc.Name, changedSeqDesc.ID, tableDesc.Name, tableDesc.ID,
					)); err != nil {
					return err
				}
			}
		}

	case *tree.AlterTableSetNotNull:
		if !col.Nullable {
			return nil
		}
		// See if there's already a mutation to add a not null constraint
		for i := range tableDesc.Mutations {
			if constraint := tableDesc.Mutations[i].GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_NOT_NULL &&
				constraint.NotNullColumn == col.ID {
				if tableDesc.Mutations[i].Direction == sqlbase.DescriptorMutation_ADD {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint in the middle of being added")
				}
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint in the middle of being dropped, try again later")
			}
		}

		info, err := tableDesc.GetConstraintInfo(params.ctx, nil, params.ExecCfg().Codec)
		if err != nil {
			return err
		}
		inuseNames := make(map[string]struct{}, len(info))
		for k := range info {
			inuseNames[k] = struct{}{}
		}
		check := sqlbase.MakeNotNullCheckConstraint(col.Name, col.ID, inuseNames, sqlbase.ConstraintValidity_Validating)
		tableDesc.AddNotNullMutation(check, sqlbase.DescriptorMutation_ADD)

	case *tree.AlterTableDropNotNull:
		if col.Nullable {
			return nil
		}

		// Prevent a column in a primary key from becoming non-null.
		if tableDesc.PrimaryIndex.ContainsColumnID(col.ID) {
			return pgerror.Newf(pgcode.InvalidTableDefinition,
				`column "%s" is in a primary index`, col.Name)
		}

		// See if there's already a mutation to add/drop a not null constraint.
		for i := range tableDesc.Mutations {
			if constraint := tableDesc.Mutations[i].GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_NOT_NULL &&
				constraint.NotNullColumn == col.ID {
				if tableDesc.Mutations[i].Direction == sqlbase.DescriptorMutation_ADD {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint in the middle of being added, try again later")
				}
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint in the middle of being dropped")
			}
		}
		info, err := tableDesc.GetConstraintInfo(params.ctx, nil, params.ExecCfg().Codec)
		if err != nil {
			return err
		}
		inuseNames := make(map[string]struct{}, len(info))
		for k := range info {
			inuseNames[k] = struct{}{}
		}
		col.Nullable = true

		// Add a check constraint equivalent to the non-null constraint and drop
		// it in the schema changer.
		check := sqlbase.MakeNotNullCheckConstraint(col.Name, col.ID, inuseNames, sqlbase.ConstraintValidity_Dropping)
		tableDesc.Checks = append(tableDesc.Checks, check)
		tableDesc.AddNotNullMutation(check, sqlbase.DescriptorMutation_DROP)

	case *tree.AlterTableDropStored:
		if !col.IsComputed() {
			return pgerror.Newf(pgcode.InvalidColumnDefinition,
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
		return pgerror.New(pgcode.Syntax,
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
		return errors.Wrapf(err, "failed to delete old stats")
	}

	// Insert each statistic.
	for i := range jsonStats {
		s := &jsonStats[i]
		h, err := s.GetHistogram(&params.p.semaCtx, params.EvalContext())
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
			return errors.Wrapf(err, "failed to insert stats")
		}
	}

	// Invalidate the local cache synchronously; this guarantees that the next
	// statement in the same session won't use a stale cache (whereas the gossip
	// update is handled asynchronously).
	params.extendedEvalCtx.ExecCfg.TableStatsCache.InvalidateTableStats(params.ctx, desc.ID)

	if g, ok := params.extendedEvalCtx.ExecCfg.Gossip.Optional(47925); ok {
		return stats.GossipTableStatAdded(g, desc.ID)
	}
	return nil
}

func (p *planner) removeColumnComment(
	ctx context.Context, tableID sqlbase.ID, columnID sqlbase.ColumnID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-column-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.ColumnCommentType,
		tableID,
		columnID)

	return err
}

// updateFKBackReferenceName updates the name of a foreign key reference on
// the referenced table descriptor.
// TODO (lucy): This method is meant to be analogous to removeFKBackReference,
// in that it only updates the backreference, but we should refactor/unify all
// the places where we update both FKs and their backreferences, so that callers
// don't have to manually take care of updating both table descriptors.
func (p *planner) updateFKBackReferenceName(
	ctx context.Context,
	tableDesc *sqlbase.MutableTableDescriptor,
	ref *sqlbase.ForeignKeyConstraint,
	newName string,
) error {
	var referencedTableDesc *sqlbase.MutableTableDescriptor
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == ref.ReferencedTableID {
		referencedTableDesc = tableDesc
	} else {
		lookup, err := p.Tables().GetMutableTableVersionByID(ctx, ref.ReferencedTableID, p.txn)
		if err != nil {
			return errors.Errorf("error resolving referenced table ID %d: %v", ref.ReferencedTableID, err)
		}
		referencedTableDesc = lookup
	}
	if referencedTableDesc.Dropped() {
		// The referenced table is being dropped. No need to modify it further.
		return nil
	}
	for i := range referencedTableDesc.InboundFKs {
		backref := &referencedTableDesc.InboundFKs[i]
		if backref.Name == ref.Name && backref.OriginTableID == tableDesc.ID {
			backref.Name = newName
			return p.writeSchemaChange(
				ctx, referencedTableDesc, sqlbase.InvalidMutationID,
				fmt.Sprintf("updating referenced FK table %s(%d) for table %s(%d)",
					referencedTableDesc.Name, referencedTableDesc.ID, tableDesc.Name, tableDesc.ID),
			)
		}
	}
	return errors.Errorf("missing backreference for foreign key %s", ref.Name)
}
