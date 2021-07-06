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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type alterTableNode struct {
	n         *tree.AlterTable
	prefix    catalog.ResolvedObjectPrefix
	tableDesc *tabledesc.Mutable
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
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER TABLE",
	); err != nil {
		return nil, err
	}

	prefix, tableDesc, err := p.ResolveMutableTableDescriptorEx(
		ctx, n.Table, !n.IfExists, tree.ResolveRequireTableDesc,
	)
	if errors.Is(err, resolver.ErrNoPrimaryKey) {
		if len(n.Cmds) > 0 && isAlterCmdValidWithoutPrimaryKey(n.Cmds[0]) {
			prefix, tableDesc, err = p.ResolveMutableTableDescriptorExAllowNoPrimaryKey(
				ctx, n.Table, !n.IfExists, tree.ResolveRequireTableDesc,
			)
		}
	}
	if err != nil {
		return nil, err
	}

	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	// This check for CREATE privilege is kept for backwards compatibility.
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of table %s or have CREATE privilege on table %s",
			tree.Name(tableDesc.GetName()), tree.Name(tableDesc.GetName()))
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
		prefix:    prefix,
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
			if t.ColumnDef.Unique.WithoutIndex {
				// TODO(rytaft): add support for this in the future if we want to expose
				// UNIQUE WITHOUT INDEX to users.
				return errors.WithHint(
					pgerror.New(
						pgcode.FeatureNotSupported,
						"adding a column marked as UNIQUE WITHOUT INDEX is unsupported",
					),
					"add the column first, then run ALTER TABLE ... ADD CONSTRAINT to add a "+
						"UNIQUE WITHOUT INDEX constraint on the column",
				)
			}
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
				if d.WithoutIndex {
					if err := addUniqueWithoutIndexTableDef(
						params.ctx,
						params.EvalContext(),
						params.SessionData(),
						d,
						n.tableDesc,
						*tn,
						NonEmptyTable,
						t.ValidationBehavior,
						params.p.SemaCtx(),
					); err != nil {
						return err
					}
					continue
				}

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
						Name:       d.Name,
					}
					if err := params.p.AlterPrimaryKey(
						params.ctx,
						n.tableDesc,
						*alterPK,
						nil, /* localityConfigSwap */
					); err != nil {
						return err
					}
					continue
				}

				// Check if the columns exist on the table.
				for _, column := range d.Columns {
					col, err := n.tableDesc.FindColumnWithName(column.Column)
					if err != nil {
						return err
					}
					if col.IsInaccessible() {
						return pgerror.Newf(
							pgcode.UndefinedColumn,
							"column %q is inaccessible and cannot be referenced by a unique constraint",
							column.Column,
						)
					}
				}
				// If the index is named, ensure that the name is unique.
				// Unnamed indexes will be given a unique auto-generated name later on.
				if d.Name != "" && n.tableDesc.ValidateIndexNameIsUnique(d.Name.String()) != nil {
					return pgerror.Newf(pgcode.DuplicateRelation, "duplicate index name: %q", d.Name)
				}
				idx := descpb.IndexDescriptor{
					Name:             string(d.Name),
					Unique:           true,
					StoreColumnNames: d.Storing.ToStrings(),
				}
				if err := idx.FillColumns(d.Columns); err != nil {
					return err
				}

				var err error
				idx, err = params.p.configureIndexDescForNewIndexPartitioning(
					params.ctx,
					n.tableDesc,
					idx,
					d.PartitionByIndex,
				)
				if err != nil {
					return err
				}
				foundIndex, err := n.tableDesc.FindIndexWithName(string(d.Name))
				if err == nil {
					if foundIndex.Dropped() {
						return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"index %q being dropped, try again later", d.Name)
					}
				}
				if err := n.tableDesc.AddIndexMutation(&idx, descpb.DescriptorMutation_ADD); err != nil {
					return err
				}

				// We need to allocate IDs upfront in the event we need to update the zone config
				// in the same transaction.
				if err := n.tableDesc.AllocateIDs(params.ctx); err != nil {
					return err
				}
				if err := params.p.configureZoneConfigForNewIndexPartitioning(
					params.ctx,
					n.tableDesc,
					idx,
				); err != nil {
					return err
				}

				if n.tableDesc.IsLocalityRegionalByRow() {
					if err := params.p.checkNoRegionChangeUnderway(
						params.ctx,
						n.tableDesc.GetParentID(),
						"create an UNIQUE CONSTRAINT on a REGIONAL BY ROW table",
					); err != nil {
						return err
					}
				}
			case *tree.CheckConstraintTableDef:
				var err error
				params.p.runWithOptions(resolveFlags{contextDatabaseID: n.tableDesc.ParentID}, func() {
					info, infoErr := n.tableDesc.GetConstraintInfo()
					if infoErr != nil {
						err = infoErr
						return
					}
					ckBuilder := schemaexpr.MakeCheckConstraintBuilder(params.ctx, *tn, n.tableDesc, &params.p.semaCtx)
					for k := range info {
						ckBuilder.MarkNameInUse(k)
					}
					ck, buildErr := ckBuilder.Build(d)
					if buildErr != nil {
						err = buildErr
						return
					}
					if t.ValidationBehavior == tree.ValidationDefault {
						ck.Validity = descpb.ConstraintValidity_Validating
					} else {
						ck.Validity = descpb.ConstraintValidity_Unvalidated
					}
					n.tableDesc.AddCheckMutation(ck, descpb.DescriptorMutation_ADD)
				})
				if err != nil {
					return err
				}

			case *tree.ForeignKeyConstraintTableDef:
				affected := make(map[descpb.ID]*tabledesc.Mutable)

				// If there are any FKs, we will need to update the table descriptor of the
				// depended-on table (to register this table against its DependedOnBy field).
				// This descriptor must be looked up uncached, and we'll allow FK dependencies
				// on tables that were just added. See the comment at the start of ResolveFK().
				// TODO(vivek): check if the cache can be used.
				var err error
				params.p.runWithOptions(resolveFlags{skipCache: true}, func() {
					// Check whether the table is empty, and pass the result to ResolveFK(). If
					// the table is empty, then resolveFK will automatically add the necessary
					// index for a fk constraint if the index does not exist.
					span := n.tableDesc.PrimaryIndexSpan(params.ExecCfg().Codec)
					kvs, scanErr := params.p.txn.Scan(params.ctx, span.Key, span.EndKey, 1)
					if scanErr != nil {
						err = scanErr
						return
					}
					var tableState TableState
					if len(kvs) == 0 {
						tableState = EmptyTable
					} else {
						tableState = NonEmptyTable
					}
					err = ResolveFK(
						params.ctx,
						params.p.txn,
						params.p,
						n.prefix.Database,
						n.prefix.Schema,
						n.tableDesc,
						d,
						affected,
						tableState,
						t.ValidationBehavior,
						params.p.EvalContext(),
					)
				})
				if err != nil {
					return err
				}
				descriptorChanged = true
				for _, updated := range affected {
					if err := params.p.writeSchemaChange(
						params.ctx, updated, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
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
			if err := params.p.AlterPrimaryKey(
				params.ctx,
				n.tableDesc,
				*t,
				nil, /* localityConfigSwap */
			); err != nil {
				return err
			}
			// Mark descriptorChanged so that a mutation job is scheduled at the end of startExec.
			descriptorChanged = true

		case *tree.AlterTableDropColumn:
			if params.SessionData().SafeUpdates {
				err := pgerror.DangerousStatementf("ALTER TABLE DROP COLUMN will " +
					"remove all data in that column")
				if !params.extendedEvalCtx.TxnImplicit {
					err = errors.WithIssueLink(err, errors.IssueLink{
						IssueURL: "https://github.com/cockroachdb/cockroach/issues/46541",
						Detail: "when used in an explicit transaction combined with other " +
							"schema changes to the same table, DROP COLUMN can result in data " +
							"loss if one of the other schema change fails or is canceled",
					})
				}
				return err
			}

			if n.tableDesc.IsLocalityRegionalByRow() {
				rbrColName, err := n.tableDesc.GetRegionalByRowTableRegionColumnName()
				if err != nil {
					return err
				}
				if rbrColName == t.Column {
					return errors.WithHintf(
						pgerror.Newf(
							pgcode.InvalidColumnReference,
							"cannot drop column %s as it is used to store the region in a REGIONAL BY ROW table",
							t.Column,
						),
						"You must change the table locality before dropping this table or alter the table to use a different column to use for the region.",
					)
				}
			}

			colToDrop, err := n.tableDesc.FindColumnWithName(t.Column)
			if err != nil {
				if t.IfExists {
					// Noop.
					continue
				}
				return err
			}
			if colToDrop.Dropped() {
				continue
			}

			// If the dropped column uses a sequence, remove references to it from that sequence.
			if colToDrop.NumUsesSequences() > 0 {
				if err := params.p.removeSequenceDependencies(params.ctx, n.tableDesc, colToDrop); err != nil {
					return err
				}
			}

			// You can't remove a column that owns a sequence that is depended on
			// by another column
			if err := params.p.canRemoveAllColumnOwnedSequences(params.ctx, n.tableDesc, colToDrop, t.DropBehavior); err != nil {
				return err
			}

			if err := params.p.dropSequencesOwnedByCol(params.ctx, colToDrop, true /* queueJob */, t.DropBehavior); err != nil {
				return err
			}

			// You can't drop a column depended on by a view unless CASCADE was
			// specified.
			for _, ref := range n.tableDesc.DependedOnBy {
				found := false
				for _, colID := range ref.ColumnIDs {
					if colID == colToDrop.GetID() {
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
				cascadedViews, err := params.p.removeDependentView(params.ctx, n.tableDesc, viewDesc, jobDesc)
				if err != nil {
					return err
				}
				qualifiedView, err := params.p.getQualifiedTableName(params.ctx, viewDesc)
				if err != nil {
					return err
				}

				droppedViews = append(droppedViews, cascadedViews...)
				droppedViews = append(droppedViews, qualifiedView.FQString())
			}

			// We cannot remove this column if there are computed columns that use it.
			if err := schemaexpr.ValidateColumnHasNoDependents(n.tableDesc, colToDrop); err != nil {
				return err
			}

			if n.tableDesc.GetPrimaryIndex().CollectKeyColumnIDs().Contains(colToDrop.GetID()) {
				return pgerror.Newf(pgcode.InvalidColumnReference,
					"column %q is referenced by the primary key", colToDrop.GetName())
			}
			var idxNamesToDelete []string
			for _, idx := range n.tableDesc.NonDropIndexes() {
				// We automatically drop indexes that reference the column
				// being dropped.

				// containsThisColumn becomes true if the index is defined
				// over the column being dropped.
				containsThisColumn := false

				// Analyze the index.
				for j := 0; j < idx.NumKeyColumns(); j++ {
					if idx.GetKeyColumnID(j) == colToDrop.GetID() {
						containsThisColumn = true
						break
					}
				}
				if !containsThisColumn {
					for j := 0; j < idx.NumKeySuffixColumns(); j++ {
						id := idx.GetKeySuffixColumnID(j)
						if n.tableDesc.GetPrimaryIndex().CollectKeyColumnIDs().Contains(id) {
							// All secondary indices necessary contain the PK
							// columns, too. (See the comments on the definition of
							// IndexDescriptor). The presence of a PK column in the
							// secondary index should thus not be seen as a
							// sufficient reason to reject the DROP.
							continue
						}
						if id == colToDrop.GetID() {
							containsThisColumn = true
							break
						}
					}
				}
				if !containsThisColumn {
					// The loop above this comment is for the old STORING encoding. The
					// loop below is for the new encoding (where the STORING columns are
					// always in the value part of a KV).
					for j := 0; j < idx.NumSecondaryStoredColumns(); j++ {
						if idx.GetStoredColumnID(j) == colToDrop.GetID() {
							containsThisColumn = true
							break
						}
					}
				}

				// If the column being dropped is referenced in the partial
				// index predicate, then the index should be dropped.
				if !containsThisColumn && idx.IsPartial() {
					expr, err := parser.ParseExpr(idx.GetPredicate())
					if err != nil {
						return err
					}

					colIDs, err := schemaexpr.ExtractColumnIDs(n.tableDesc, expr)
					if err != nil {
						return err
					}

					if colIDs.Contains(colToDrop.GetID()) {
						containsThisColumn = true
					}
				}

				// Perform the DROP.
				if containsThisColumn {
					idxNamesToDelete = append(idxNamesToDelete, idx.GetName())
				}
			}

			for _, idxName := range idxNamesToDelete {
				jobDesc := fmt.Sprintf("removing index %q dependent on column %q which is being"+
					" dropped; full details: %s", idxName, colToDrop.ColName(),
					tree.AsStringWithFQNames(n.n, params.Ann()))
				if err := params.p.dropIndexByName(
					params.ctx, tn, tree.UnrestrictedName(idxName), n.tableDesc, false,
					t.DropBehavior, ignoreIdxConstraint, jobDesc,
				); err != nil {
					return err
				}
			}

			// Drop unique constraints that reference the column.
			sliceIdx := 0
			for i := range n.tableDesc.UniqueWithoutIndexConstraints {
				constraint := &n.tableDesc.UniqueWithoutIndexConstraints[i]
				n.tableDesc.UniqueWithoutIndexConstraints[sliceIdx] = *constraint
				sliceIdx++
				if descpb.ColumnIDs(constraint.ColumnIDs).Contains(colToDrop.GetID()) {
					sliceIdx--

					// If this unique constraint is used on the referencing side of any FK
					// constraints, try to remove the references. Don't bother trying to find
					// an alternate index or constraint, since all possible matches will
					// be dropped when the column is dropped.
					if err := params.p.tryRemoveFKBackReferences(
						params.ctx, n.tableDesc, constraint, t.DropBehavior, nil,
					); err != nil {
						return err
					}
				}
			}
			n.tableDesc.UniqueWithoutIndexConstraints = n.tableDesc.UniqueWithoutIndexConstraints[:sliceIdx]

			// Drop check constraints which reference the column.
			constraintsToDrop := make([]string, 0, len(n.tableDesc.Checks))
			constraintInfo, err := n.tableDesc.GetConstraintInfo()
			if err != nil {
				return err
			}

			for _, check := range n.tableDesc.AllActiveAndInactiveChecks() {
				if used, err := n.tableDesc.CheckConstraintUsesColumn(check, colToDrop.GetID()); err != nil {
					return err
				} else if used {
					if check.Validity == descpb.ConstraintValidity_Dropping {
						// We don't need to drop this constraint, its already
						// in the process.
						continue
					}
					constraintsToDrop = append(constraintsToDrop, check.Name)
				}
			}

			for _, constraintName := range constraintsToDrop {
				err := n.tableDesc.DropConstraint(params.ctx, constraintName, constraintInfo[constraintName],
					func(*tabledesc.Mutable, *descpb.ForeignKeyConstraint) error {
						return nil
					},
					params.extendedEvalCtx.Settings,
				)
				if err != nil {
					return err
				}
			}

			if err := params.p.removeColumnComment(params.ctx, n.tableDesc.ID, colToDrop.GetID()); err != nil {
				return err
			}

			// Since we are able to drop indexes used by foreign keys on the origin side,
			// the drop index codepaths aren't going to remove dependent FKs, so we
			// need to do that here.
			if params.p.ExecCfg().Settings.Version.IsActive(params.ctx, clusterversion.NoOriginFKIndexes) {
				// We update the FK's slice in place here.
				sliceIdx := 0
				for i := range n.tableDesc.OutboundFKs {
					n.tableDesc.OutboundFKs[sliceIdx] = n.tableDesc.OutboundFKs[i]
					sliceIdx++
					fk := &n.tableDesc.OutboundFKs[i]
					if descpb.ColumnIDs(fk.OriginColumnIDs).Contains(colToDrop.GetID()) {
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
				if n.tableDesc.Columns[i].ID == colToDrop.GetID() {
					n.tableDesc.AddColumnMutation(colToDrop.ColumnDesc(), descpb.DescriptorMutation_DROP)
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

			if err := validateDescriptor(params.ctx, params.p, n.tableDesc); err != nil {
				return err
			}

		case *tree.AlterTableDropConstraint:
			info, err := n.tableDesc.GetConstraintInfo()
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
					"constraint %q of relation %q does not exist", t.Constraint, n.tableDesc.Name)
			}
			if err := n.tableDesc.DropConstraint(
				params.ctx,
				name, details,
				func(desc *tabledesc.Mutable, ref *descpb.ForeignKeyConstraint) error {
					return params.p.removeFKBackReference(params.ctx, desc, ref)
				}, params.ExecCfg().Settings); err != nil {
				return err
			}
			descriptorChanged = true
			if err := validateDescriptor(params.ctx, params.p, n.tableDesc); err != nil {
				return err
			}

		case *tree.AlterTableValidateConstraint:
			info, err := n.tableDesc.GetConstraintInfo()
			if err != nil {
				return err
			}
			name := string(t.Constraint)
			constraint, ok := info[name]
			if !ok {
				return pgerror.Newf(pgcode.UndefinedObject,
					"constraint %q of relation %q does not exist", t.Constraint, n.tableDesc.Name)
			}
			if !constraint.Unvalidated {
				continue
			}
			switch constraint.Kind {
			case descpb.ConstraintTypeCheck:
				found := false
				var ck *descpb.TableDescriptor_CheckConstraint
				for _, c := range n.tableDesc.Checks {
					// If the constraint is still being validated, don't allow
					// VALIDATE CONSTRAINT to run.
					if c.Name == name && c.Validity != descpb.ConstraintValidity_Validating {
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
					params.ctx, params.p.LeaseMgr(), &params.p.semaCtx, params.EvalContext(), n.tableDesc, params.EvalContext().Txn, ck.Expr,
				); err != nil {
					return err
				}
				ck.Validity = descpb.ConstraintValidity_Validated

			case descpb.ConstraintTypeFK:
				var foundFk *descpb.ForeignKeyConstraint
				for i := range n.tableDesc.OutboundFKs {
					fk := &n.tableDesc.OutboundFKs[i]
					// If the constraint is still being validated, don't allow
					// VALIDATE CONSTRAINT to run.
					if fk.Name == name && fk.Validity != descpb.ConstraintValidity_Validating {
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
				foundFk.Validity = descpb.ConstraintValidity_Validated

			case descpb.ConstraintTypeUnique:
				if constraint.Index == nil {
					var foundUnique *descpb.UniqueWithoutIndexConstraint
					for i := range n.tableDesc.UniqueWithoutIndexConstraints {
						uc := &n.tableDesc.UniqueWithoutIndexConstraints[i]
						// If the constraint is still being validated, don't allow
						// VALIDATE CONSTRAINT to run.
						if uc.Name == name && uc.Validity != descpb.ConstraintValidity_Validating {
							foundUnique = uc
							break
						}
					}
					if foundUnique == nil {
						return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"constraint %q in the middle of being added, try again later", t.Constraint)
					}
					if err := validateUniqueWithoutIndexConstraintInTxn(
						params.ctx, params.EvalContext(), n.tableDesc, params.EvalContext().Txn, name,
					); err != nil {
						return err
					}
					foundUnique.Validity = descpb.ConstraintValidity_Validated
					break
				}

				// This unique constraint is enforced by an index, so fall through to
				// the error below.
				fallthrough

			default:
				return pgerror.Newf(pgcode.WrongObjectType,
					"constraint %q of relation %q is not a foreign key, check, or unique without index"+
						" constraint", tree.ErrString(&t.Constraint), tree.ErrString(n.n.Table))
			}
			descriptorChanged = true

		case tree.ColumnMutationCmd:
			// Column mutations
			col, err := n.tableDesc.FindColumnWithName(t.GetColumn())
			if err != nil {
				return err
			}
			if col.Dropped() {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q in the middle of being dropped", t.GetColumn())
			}
			// Apply mutations to copy of column descriptor.
			if err := applyColumnMutation(params.ctx, n.tableDesc, col, t, params, n.n.Cmds, tn); err != nil {
				return err
			}
			descriptorChanged = true

		case *tree.AlterTablePartitionByTable:
			if t.All {
				return unimplemented.NewWithIssue(58736, "PARTITION ALL BY not yet implemented")
			}
			if n.tableDesc.GetLocalityConfig() != nil {
				return pgerror.Newf(
					pgcode.FeatureNotSupported,
					"cannot set PARTITION BY on a table in a multi-region enabled database",
				)
			}
			if n.tableDesc.IsPartitionAllBy() {
				return unimplemented.NewWithIssue(58736, "changing partition of table with PARTITION ALL BY not yet implemented")
			}
			oldPartitioning := n.tableDesc.GetPrimaryIndex().GetPartitioning().DeepCopy()
			if oldPartitioning.NumImplicitColumns() > 0 {
				return unimplemented.NewWithIssue(
					58731,
					"cannot ALTER TABLE PARTITION BY on a table which already has implicit column partitioning",
				)
			}
			newPrimaryIndexDesc := n.tableDesc.GetPrimaryIndex().IndexDescDeepCopy()
			newImplicitCols, newPartitioning, err := CreatePartitioning(
				params.ctx, params.p.ExecCfg().Settings,
				params.EvalContext(),
				n.tableDesc,
				newPrimaryIndexDesc,
				t.PartitionBy,
				nil, /* allowedNewColumnNames */
				params.p.EvalContext().SessionData.ImplicitColumnPartitioningEnabled ||
					n.tableDesc.IsLocalityRegionalByRow(),
			)
			if err != nil {
				return err
			}
			if newPartitioning.NumImplicitColumns > 0 {
				return unimplemented.NewWithIssue(
					58731,
					"cannot ALTER TABLE and change the partitioning to contain implicit columns",
				)
			}
			isIndexAltered := tabledesc.UpdateIndexPartitioning(&newPrimaryIndexDesc, true /* isIndexPrimary */, newImplicitCols, newPartitioning)
			if isIndexAltered {
				n.tableDesc.SetPrimaryIndex(newPrimaryIndexDesc)
				descriptorChanged = true
				if err := deleteRemovedPartitionZoneConfigs(
					params.ctx,
					params.p.txn,
					n.tableDesc,
					n.tableDesc.GetPrimaryIndexID(),
					oldPartitioning,
					n.tableDesc.GetPrimaryIndex().GetPartitioning(),
					params.extendedEvalCtx.ExecCfg,
				); err != nil {
					return err
				}
			}

		case *tree.AlterTableSetAudit:
			changed, err := params.p.setAuditMode(params.ctx, n.tableDesc, t.Mode)
			if err != nil {
				return err
			}
			descriptorChanged = descriptorChanged || changed

		case *tree.AlterTableInjectStats:
			sd, ok := n.statsData[i]
			if !ok {
				return errors.AssertionFailedf("missing stats data")
			}
			if !params.p.EvalContext().TxnImplicit {
				return errors.New("cannot inject statistics in an explicit transaction")
			}
			if err := injectTableStats(params, n.tableDesc, sd); err != nil {
				return err
			}

		case *tree.AlterTableRenameColumn:
			const allowRenameOfShardColumn = false
			descChanged, err := params.p.renameColumn(params.ctx, n.tableDesc,
				&t.Column, &t.NewName, allowRenameOfShardColumn)
			if err != nil {
				return err
			}
			descriptorChanged = descriptorChanged || descChanged

		case *tree.AlterTableRenameConstraint:
			info, err := n.tableDesc.GetConstraintInfo()
			if err != nil {
				return err
			}
			details, ok := info[string(t.Constraint)]
			if !ok {
				return pgerror.Newf(pgcode.UndefinedObject,
					"constraint %q of relation %q does not exist", tree.ErrString(&t.Constraint), n.tableDesc.Name)
			}
			if t.Constraint == t.NewName {
				// Nothing to do.
				break
			}

			if _, ok := info[string(t.NewName)]; ok {
				return pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", tree.ErrString(&t.NewName))
			}
			// If this is a unique or primary constraint, renames of the constraint
			// lead to renames of the underlying index. Ensure that no index with this
			// new name exists. This is what postgres does.
			switch details.Kind {
			case descpb.ConstraintTypeUnique, descpb.ConstraintTypePK:
				if catalog.FindNonDropIndex(n.tableDesc, func(idx catalog.Index) bool {
					return idx.GetName() == string(t.NewName)
				}) != nil {
					return pgerror.Newf(pgcode.DuplicateRelation,
						"relation %v already exists", t.NewName)
				}
			}

			if err := params.p.CheckPrivilege(params.ctx, n.tableDesc, privilege.CREATE); err != nil {
				return err
			}

			depViewRenameError := func(objType string, refTableID descpb.ID) error {
				return params.p.dependentViewError(params.ctx,
					objType, tree.ErrString(&t.NewName), n.tableDesc.ParentID, refTableID, "rename",
				)
			}

			if err := n.tableDesc.RenameConstraint(
				details, string(t.Constraint), string(t.NewName), depViewRenameError,
				func(desc *tabledesc.Mutable, ref *descpb.ForeignKeyConstraint, newName string) error {
					return params.p.updateFKBackReferenceName(params.ctx, desc, ref, newName)
				}); err != nil {
				return err
			}
			descriptorChanged = true
		default:
			return errors.AssertionFailedf("unsupported alter command: %T", cmd)
		}

		// Allocate IDs now, so new IDs are available to subsequent commands
		if err := n.tableDesc.AllocateIDs(params.ctx); err != nil {
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

	mutationID := descpb.InvalidMutationID
	if addedMutations {
		mutationID = n.tableDesc.ClusterVersion.NextMutationID
	}
	if err := params.p.writeSchemaChange(
		params.ctx, n.tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Add all newly created type back references.
	if err := params.p.addBackRefsFromAllTypesInTable(params.ctx, n.tableDesc); err != nil {
		return err
	}

	// Record this table alteration in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	return params.p.logEvent(params.ctx,
		n.tableDesc.ID,
		&eventpb.AlterTable{
			TableName:           params.p.ResolvedName(n.n.Table).FQString(),
			MutationID:          uint32(mutationID),
			CascadeDroppedViews: droppedViews,
		})
}

func (p *planner) setAuditMode(
	ctx context.Context, desc *tabledesc.Mutable, auditMode tree.AuditMode,
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

// addIndexMutationWithSpecificPrimaryKey adds an index mutation into the given
// table descriptor, but sets up the index with KeySuffixColumnIDs from the
// given index, rather than the table's primary key.
func addIndexMutationWithSpecificPrimaryKey(
	ctx context.Context,
	table *tabledesc.Mutable,
	toAdd *descpb.IndexDescriptor,
	primary *descpb.IndexDescriptor,
) error {
	// Reset the ID so that a call to AllocateIDs will set up the index.
	toAdd.ID = 0
	if err := table.AddIndexMutation(toAdd, descpb.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := table.AllocateIDs(ctx); err != nil {
		return err
	}
	// Use the columns in the given primary index to construct this indexes
	// KeySuffixColumnIDs list.
	presentColIDs := catalog.MakeTableColSet(toAdd.KeyColumnIDs...)
	presentColIDs.UnionWith(catalog.MakeTableColSet(toAdd.StoreColumnIDs...))
	toAdd.KeySuffixColumnIDs = nil
	for _, colID := range primary.KeyColumnIDs {
		if !presentColIDs.Contains(colID) {
			toAdd.KeySuffixColumnIDs = append(toAdd.KeySuffixColumnIDs, colID)
		}
	}
	return nil
}

// applyColumnMutation applies the mutation specified in `mut` to the given
// columnDescriptor, and saves the containing table descriptor. If the column's
// dependencies on sequences change, it updates them as well.
func applyColumnMutation(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	col catalog.Column,
	mut tree.ColumnMutationCmd,
	params runParams,
	cmds tree.AlterTableCmds,
	tn *tree.TableName,
) error {
	switch t := mut.(type) {
	case *tree.AlterTableAlterColumnType:
		return AlterColumnType(ctx, tableDesc, col, t, params, cmds, tn)

	case *tree.AlterTableSetDefault:
		if col.NumUsesSequences() > 0 {
			if err := params.p.removeSequenceDependencies(params.ctx, tableDesc, col); err != nil {
				return err
			}
		}
		if t.Default == nil {
			col.ColumnDesc().DefaultExpr = nil
		} else {
			colDatumType := col.GetType()
			expr, err := schemaexpr.SanitizeVarFreeExpr(
				params.ctx, t.Default, colDatumType, "DEFAULT", &params.p.semaCtx, tree.VolatilityVolatile,
			)
			if err != nil {
				return pgerror.WithCandidateCode(err, pgcode.DatatypeMismatch)
			}
			s := tree.Serialize(expr)
			col.ColumnDesc().DefaultExpr = &s

			// Add references to the sequence descriptors this column is now using.
			changedSeqDescs, err := maybeAddSequenceDependencies(
				params.ctx, params.p.ExecCfg().Settings, params.p, tableDesc, col.ColumnDesc(), expr, nil, /* backrefs */
			)
			if err != nil {
				return err
			}
			for _, changedSeqDesc := range changedSeqDescs {
				if err := params.p.writeSchemaChange(
					params.ctx, changedSeqDesc, descpb.InvalidMutationID,
					fmt.Sprintf("updating dependent sequence %s(%d) for table %s(%d)",
						changedSeqDesc.Name, changedSeqDesc.ID, tableDesc.Name, tableDesc.ID,
					)); err != nil {
					return err
				}
			}
		}

	case *tree.AlterTableSetVisible:
		column, err := tableDesc.FindActiveOrNewColumnByName(col.ColName())
		if err != nil {
			return err
		}
		column.ColumnDesc().Hidden = !t.Visible

	case *tree.AlterTableSetNotNull:
		if !col.IsNullable() {
			return nil
		}
		// See if there's already a mutation to add a not null constraint
		for i := range tableDesc.Mutations {
			if constraint := tableDesc.Mutations[i].GetConstraint(); constraint != nil &&
				constraint.ConstraintType == descpb.ConstraintToUpdate_NOT_NULL &&
				constraint.NotNullColumn == col.GetID() {
				if tableDesc.Mutations[i].Direction == descpb.DescriptorMutation_ADD {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint in the middle of being added")
				}
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint in the middle of being dropped, try again later")
			}
		}

		info, err := tableDesc.GetConstraintInfo()
		if err != nil {
			return err
		}
		inuseNames := make(map[string]struct{}, len(info))
		for k := range info {
			inuseNames[k] = struct{}{}
		}
		check := tabledesc.MakeNotNullCheckConstraint(col.GetName(), col.GetID(), inuseNames, descpb.ConstraintValidity_Validating)
		tableDesc.AddNotNullMutation(check, descpb.DescriptorMutation_ADD)

	case *tree.AlterTableDropNotNull:
		if col.IsNullable() {
			return nil
		}

		// Prevent a column in a primary key from becoming non-null.
		if tableDesc.GetPrimaryIndex().CollectKeyColumnIDs().Contains(col.GetID()) {
			return pgerror.Newf(pgcode.InvalidTableDefinition,
				`column "%s" is in a primary index`, col.GetName())
		}

		// See if there's already a mutation to add/drop a not null constraint.
		for i := range tableDesc.Mutations {
			if constraint := tableDesc.Mutations[i].GetConstraint(); constraint != nil &&
				constraint.ConstraintType == descpb.ConstraintToUpdate_NOT_NULL &&
				constraint.NotNullColumn == col.GetID() {
				if tableDesc.Mutations[i].Direction == descpb.DescriptorMutation_ADD {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"constraint in the middle of being added, try again later")
				}
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint in the middle of being dropped")
			}
		}
		info, err := tableDesc.GetConstraintInfo()
		if err != nil {
			return err
		}
		inuseNames := make(map[string]struct{}, len(info))
		for k := range info {
			inuseNames[k] = struct{}{}
		}
		col.ColumnDesc().Nullable = true

		// Add a check constraint equivalent to the non-null constraint and drop
		// it in the schema changer.
		check := tabledesc.MakeNotNullCheckConstraint(col.GetName(), col.GetID(), inuseNames, descpb.ConstraintValidity_Dropping)
		tableDesc.Checks = append(tableDesc.Checks, check)
		tableDesc.AddNotNullMutation(check, descpb.DescriptorMutation_DROP)

	case *tree.AlterTableDropStored:
		if !col.IsComputed() {
			return pgerror.Newf(pgcode.InvalidColumnDefinition,
				"column %q is not a computed column", col.GetName())
		}
		if col.IsVirtual() {
			return pgerror.Newf(pgcode.InvalidColumnDefinition,
				"column %q is not a stored computed column", col.GetName())
		}
		col.ColumnDesc().ComputeExpr = nil
	}
	return nil
}

func labeledRowValues(cols []catalog.Column, values tree.Datums) string {
	var s bytes.Buffer
	for i := range cols {
		if i != 0 {
			s.WriteString(`, `)
		}
		s.WriteString(cols[i].GetName())
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
	params runParams, desc catalog.TableDescriptor, statsExpr tree.TypedExpr,
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
		`DELETE FROM system.table_statistics WHERE "tableID" = $1`, desc.GetID(),
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
			col, err := desc.FindColumnWithName(tree.Name(colName))
			if err != nil {
				return err
			}
			if err := columnIDs.Append(tree.NewDInt(tree.DInt(col.GetID()))); err != nil {
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
			desc.GetID(),
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
	params.extendedEvalCtx.ExecCfg.TableStatsCache.InvalidateTableStats(params.ctx, desc.GetID())

	// Use Gossip to refresh the caches on other nodes.
	if g, ok := params.extendedEvalCtx.ExecCfg.Gossip.Optional(47925); ok {
		return stats.GossipTableStatAdded(g, desc.GetID())
	}
	return nil
}

func (p *planner) removeColumnComment(
	ctx context.Context, tableID descpb.ID, columnID descpb.ColumnID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-column-comment",
		p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
	tableDesc *tabledesc.Mutable,
	ref *descpb.ForeignKeyConstraint,
	newName string,
) error {
	var referencedTableDesc *tabledesc.Mutable
	// We don't want to lookup/edit a second copy of the same table.
	if tableDesc.ID == ref.ReferencedTableID {
		referencedTableDesc = tableDesc
	} else {
		lookup, err := p.Descriptors().GetMutableTableVersionByID(ctx, ref.ReferencedTableID, p.txn)
		if err != nil {
			return errors.Wrapf(err, "error resolving referenced table ID %d", ref.ReferencedTableID)
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
				ctx, referencedTableDesc, descpb.InvalidMutationID,
				fmt.Sprintf("updating referenced FK table %s(%d) for table %s(%d)",
					referencedTableDesc.Name, referencedTableDesc.ID, tableDesc.Name, tableDesc.ID),
			)
		}
	}
	return errors.Errorf("missing backreference for foreign key %s", ref.Name)
}

// tryRemoveFKBackReferences determines whether the provided unique constraint
// is used on the referencing side of a FK constraint. If so, it tries to remove
// the references or find an alternate unique constraint that will suffice.
func (p *planner) tryRemoveFKBackReferences(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	constraint descpb.UniqueConstraint,
	behavior tree.DropBehavior,
	candidateConstraints []descpb.UniqueConstraint,
) error {
	// uniqueConstraintHasReplacementCandidate runs
	// IsValidReferencedUniqueConstraint on the candidateConstraints. Returns true
	// if at least one constraint satisfies IsValidReferencedUniqueConstraint.
	uniqueConstraintHasReplacementCandidate := func(
		referencedColumnIDs []descpb.ColumnID,
	) bool {
		for _, uc := range candidateConstraints {
			if uc.IsValidReferencedUniqueConstraint(referencedColumnIDs) {
				return true
			}
		}
		return false
	}

	// Index for updating the FK slices in place when removing FKs.
	sliceIdx := 0
	for i := range tableDesc.InboundFKs {
		tableDesc.InboundFKs[sliceIdx] = tableDesc.InboundFKs[i]
		sliceIdx++
		fk := &tableDesc.InboundFKs[i]
		// The constraint being deleted could potentially be the referenced unique
		// constraint for this fk.
		if constraint.IsValidReferencedUniqueConstraint(fk.ReferencedColumnIDs) &&
			!uniqueConstraintHasReplacementCandidate(fk.ReferencedColumnIDs) {
			// If we found haven't found a replacement, then we check that the drop
			// behavior is cascade.
			if err := p.canRemoveFKBackreference(ctx, constraint.GetName(), fk, behavior); err != nil {
				return err
			}
			sliceIdx--
			if err := p.removeFKForBackReference(ctx, tableDesc, fk); err != nil {
				return err
			}
		}
	}
	tableDesc.InboundFKs = tableDesc.InboundFKs[:sliceIdx]
	return nil
}
