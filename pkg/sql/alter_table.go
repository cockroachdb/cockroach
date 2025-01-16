// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging/auditevents"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam/tablestorageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type alterTableNode struct {
	zeroInputPlanNode
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
//
//	notes: postgres requires CREATE on the table.
//	       mysql requires ALTER, CREATE, INSERT on the table.
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
		return nil, pgerror.Wrapf(err, pgcode.InsufficientPrivilege,
			"must be owner of table %s or have CREATE privilege on table %s",
			tree.Name(tableDesc.GetName()), tree.Name(tableDesc.GetName()))
	}

	// Disallow schema changes if this table's schema is locked, unless it is to
	// set/reset the "schema_locked" storage parameter.
	if err = checkSchemaChangeIsAllowed(tableDesc, n); err != nil {
		return nil, err
	}

	n.HoistAddColumnConstraints(func() {
		telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra("table", "add_column.references"))
	})

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
		telemetry.Inc(sqltelemetry.SchemaChangeAlterCounterWithExtra("table", cmd.TelemetryName()))

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
			if t.ColumnDef.PrimaryKey.IsPrimaryKey {
				return pgerror.Newf(pgcode.InvalidColumnDefinition,
					"multiple primary keys for table %q are not allowed", tn.Object())
			}
			var err error
			params.p.runWithOptions(resolveFlags{contextDatabaseID: n.tableDesc.ParentID}, func() {
				err = params.p.addColumnImpl(params, n, tn, n.tableDesc, t)
			})
			if err != nil {
				return err
			}
		case *tree.AlterTableAddConstraint:
			if skip, err := validateConstraintNameIsNotUsed(n.tableDesc, t); err != nil {
				return err
			} else if skip {
				continue
			}
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
					if t.ValidationBehavior == tree.ValidationSkip {
						return sqlerrors.NewUnsupportedUnvalidatedConstraintError(catconstants.ConstraintTypePK)
					}
					// Translate this operation into an ALTER PRIMARY KEY command.
					alterPK := &tree.AlterTableAlterPrimaryKey{
						Columns:       d.Columns,
						Sharded:       d.Sharded,
						Name:          d.Name,
						StorageParams: d.StorageParams,
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

				if t.ValidationBehavior == tree.ValidationSkip {
					return sqlerrors.NewUnsupportedUnvalidatedConstraintError(catconstants.ConstraintTypeUnique)
				}

				if err := validateColumnsAreAccessible(n.tableDesc, d.Columns); err != nil {
					return err
				}

				tableName, err := params.p.getQualifiedTableName(params.ctx, n.tableDesc)
				if err != nil {
					return err
				}

				// We are going to modify the AST to replace any index expressions with
				// virtual columns. If the txn ends up retrying, then this change is not
				// syntactically valid, since the virtual column is only added in the descriptor
				// and not in the AST.
				columns := make(tree.IndexElemList, len(d.Columns))
				copy(columns, d.Columns)
				if err := replaceExpressionElemsWithVirtualCols(
					params.ctx,
					n.tableDesc,
					tableName,
					columns,
					false, /* isInverted */
					false, /* isNewTable */
					params.p.SemaCtx(),
					params.ExecCfg().Settings.Version.ActiveVersion(params.ctx),
				); err != nil {
					return err
				}

				// Check if the columns exist on the table.
				for _, column := range columns {
					if column.Expr != nil {
						return pgerror.New(
							pgcode.InvalidTableDefinition,
							"cannot create a unique constraint on an expression, use UNIQUE INDEX instead",
						)
					}
					_, err := catalog.MustFindColumnByTreeName(n.tableDesc, column.Column)
					if err != nil {
						return err
					}
				}

				idx := descpb.IndexDescriptor{
					Name:             string(d.Name),
					Unique:           true,
					NotVisible:       d.Invisibility.Value != 0.0,
					Invisibility:     d.Invisibility.Value,
					StoreColumnNames: d.Storing.ToStrings(),
					CreatedAtNanos:   params.EvalContext().GetTxnTimestamp(time.Microsecond).UnixNano(),
				}
				if err := idx.FillColumns(columns); err != nil {
					return err
				}

				if d.Predicate != nil {
					expr, err := schemaexpr.ValidatePartialIndexPredicate(
						params.ctx, n.tableDesc, d.Predicate, tableName, params.p.SemaCtx(),
						params.ExecCfg().Settings.Version.ActiveVersion(params.ctx),
					)
					if err != nil {
						return err
					}
					idx.Predicate = expr
				}

				idx, err = params.p.configureIndexDescForNewIndexPartitioning(
					params.ctx,
					n.tableDesc,
					idx,
					d.PartitionByIndex,
				)
				if err != nil {
					return err
				}
				foundIndex := catalog.FindIndexByName(n.tableDesc, string(d.Name))
				if foundIndex != nil && foundIndex.Dropped() {
					return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
						"index %q being dropped, try again later", d.Name)
				}
				if err := n.tableDesc.AddIndexMutationMaybeWithTempIndex(
					&idx, descpb.DescriptorMutation_ADD,
				); err != nil {
					return err
				}

				// We need to allocate IDs upfront in the event we need to update the zone config
				// in the same transaction.
				version := params.ExecCfg().Settings.Version.ActiveVersion(params.ctx)
				if err := n.tableDesc.AllocateIDs(params.ctx, version); err != nil {
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
					ckBuilder := schemaexpr.MakeCheckConstraintBuilder(params.ctx, *tn, n.tableDesc, &params.p.semaCtx)
					for _, c := range n.tableDesc.AllConstraints() {
						ckBuilder.MarkNameInUse(c.GetName())
					}
					ck, buildErr := ckBuilder.Build(d, params.ExecCfg().Settings.Version.ActiveVersion(params.ctx))
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
				// There are two cases that we want to reject FKs related to
				// ON UPDATE/DELETE clauses:
				// - a FK ON UPDATE action and there is already an ON UPDATE
				//   expression for the column
				// - a FK over a computed column, and we have a ON UPDATE or ON DELETE
				//   that modifies the FK column value in some manner. We block these
				//   because the column value cannot change since it is computed. We do
				//   allow ON DELETE CASCADE though since that removes the entire row.
				hasUpdateAction := d.Actions.HasUpdateAction()
				if hasUpdateAction || d.Actions.HasDisallowedActionForComputedFKCol() {
					for _, fromCol := range d.FromCols {
						for _, toCheck := range n.tableDesc.Columns {
							if fromCol != toCheck.ColName() {
								continue
							}
							if hasUpdateAction && toCheck.HasOnUpdate() {
								return pgerror.Newf(
									pgcode.InvalidTableDefinition,
									"cannot specify a foreign key update action and an ON UPDATE"+
										" expression on the same column",
								)
							} else if toCheck.IsComputed() {
								return sqlerrors.NewInvalidActionOnComputedFKColumnError(hasUpdateAction)
							}
						}
					}
				}

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
					// Disallow schema change if the FK references a table whose schema is
					// locked.
					if err := checkSchemaChangeIsAllowed(updated, n.n); err != nil {
						return err
					}
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
			// For `ALTER PRIMARY KEY`, carry over the primary index name, like how we
			// carried over comments associated with the old primary index.
			t.Name = tree.Name(n.tableDesc.PrimaryIndex.Name)
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
					"remove all data in that column and drop any indexes that reference that column")
				if !params.extendedEvalCtx.TxnIsSingleStmt {
					err = errors.WithIssueLink(err, errors.IssueLink{
						IssueURL: build.MakeIssueURL(46541),
						Detail: "when used in an explicit transaction combined with other " +
							"schema changes to the same table, DROP COLUMN can result in data " +
							"loss if one of the other schema change fails or is canceled",
					})
				}
				return err
			}

			tableDesc := n.tableDesc
			if t.Column == catpb.TTLDefaultExpirationColumnName &&
				tableDesc.HasRowLevelTTL() &&
				tableDesc.GetRowLevelTTL().HasDurationExpr() {
				return errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidTableDefinition,
						`cannot drop column %s while ttl_expire_after is set`,
						t.Column,
					),
					"use ALTER TABLE %[1]s RESET (ttl) or ALTER TABLE %[1]s SET (ttl_expiration_expression = ...) instead",
					tree.Name(tableDesc.GetName()),
				)
			}

			colDroppedViews, err := dropColumnImpl(params, tn, tableDesc, tableDesc.GetRowLevelTTL(), t)
			if err != nil {
				return err
			}
			droppedViews = append(droppedViews, colDroppedViews...)
		case *tree.AlterTableDropConstraint:
			name := string(t.Constraint)
			c := catalog.FindConstraintByName(n.tableDesc, name)
			if c == nil {
				if t.IfExists {
					continue
				}
				return sqlerrors.NewUndefinedConstraintError(string(t.Constraint), n.tableDesc.Name)
			}
			if uwoi := c.AsUniqueWithoutIndex(); uwoi != nil {
				if err := params.p.tryRemoveFKBackReferences(
					params.ctx, n.tableDesc, uwoi, t.DropBehavior, true,
				); err != nil {
					return err
				}
			}
			if err := n.tableDesc.DropConstraint(
				c,
				func(backRef catalog.ForeignKeyConstraint) error {
					return params.p.removeFKBackReference(params.ctx, n.tableDesc, backRef.ForeignKeyDesc())
				},
				func(ck *descpb.TableDescriptor_CheckConstraint) error {
					return params.p.removeCheckBackReferenceInFunctions(params.ctx, n.tableDesc, ck)
				},
			); err != nil {
				return err
			}
			descriptorChanged = true
			if err := validateDescriptor(params.ctx, params.p, n.tableDesc); err != nil {
				return err
			}

		case *tree.AlterTableValidateConstraint:
			name := string(t.Constraint)
			c := catalog.FindConstraintByName(n.tableDesc, name)
			if c == nil {
				return sqlerrors.NewUndefinedConstraintError(string(t.Constraint), n.tableDesc.Name)
			}
			switch c.GetConstraintValidity() {
			case descpb.ConstraintValidity_Validated:
				// Nothing to do.
				continue
			case descpb.ConstraintValidity_Validating:
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint %q in the middle of being added, try again later", t.Constraint)
			case descpb.ConstraintValidity_Dropping:
				return sqlerrors.NewUndefinedConstraintError(string(t.Constraint), n.tableDesc.Name)
			}
			if ck := c.AsCheck(); ck != nil {
				if err := validateCheckInTxn(
					params.ctx, params.p.InternalSQLTxn(), params.p.EvalContext(),
					&params.p.semaCtx, params.p.SessionData(), n.tableDesc, ck,
				); err != nil {
					return err
				}
				ck.CheckDesc().Validity = descpb.ConstraintValidity_Validated
			} else if fk := c.AsForeignKey(); fk != nil {
				if err := validateFkInTxn(
					params.ctx, params.p.InternalSQLTxn(), n.tableDesc, name,
				); err != nil {
					return err
				}
				fk.ForeignKeyDesc().Validity = descpb.ConstraintValidity_Validated
			} else if uwoi := c.AsUniqueWithoutIndex(); uwoi != nil {
				if err := validateUniqueWithoutIndexConstraintInTxn(
					params.ctx,
					params.p.InternalSQLTxn(),
					n.tableDesc,
					params.p.User(),
					name,
				); err != nil {
					return err
				}
				uwoi.UniqueWithoutIndexDesc().Validity = descpb.ConstraintValidity_Validated
			} else {
				return pgerror.Newf(pgcode.WrongObjectType,
					"constraint %q of relation %q is not a foreign key, check, or unique without index"+
						" constraint", tree.ErrString(&t.Constraint), tree.ErrString(n.n.Table))
			}
			descriptorChanged = true

		case tree.ColumnMutationCmd:
			// Column mutations
			tableDesc := n.tableDesc
			col, err := catalog.MustFindColumnByTreeName(tableDesc, t.GetColumn())
			if err != nil {
				return err
			}
			if col.Dropped() {
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"column %q in the middle of being dropped", t.GetColumn())
			}
			// Block modification on system columns.
			if col.IsSystemColumn() {
				return pgerror.Newf(
					pgcode.FeatureNotSupported,
					"cannot alter system column %q", col.GetName())
			}
			columnName := col.GetName()
			if columnName == catpb.TTLDefaultExpirationColumnName &&
				tableDesc.HasRowLevelTTL() &&
				tableDesc.GetRowLevelTTL().HasDurationExpr() {
				return sqlerrors.NewAlterDependsOnDurationExprError("alter", "column", columnName, tn.Object())
			}
			// Apply mutations to copy of column descriptor.
			if err := applyColumnMutation(params.ctx, tableDesc, col, t, params, n.n.Cmds, tn); err != nil {
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
			if n.tableDesc.GetPrimaryIndex().IsSharded() {
				return pgerror.New(
					pgcode.FeatureNotSupported,
					"cannot set explicit partitioning with PARTITION BY on hash sharded primary key",
				)
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
				params.p.EvalContext().SessionData().ImplicitColumnPartitioningEnabled ||
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
					params.p.InternalSQLTxn(),
					n.tableDesc,
					n.tableDesc.GetPrimaryIndexID(),
					oldPartitioning,
					n.tableDesc.GetPrimaryIndex().GetPartitioning(),
					params.extendedEvalCtx.ExecCfg,
					params.extendedEvalCtx.Tracing.KVTracingEnabled(),
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
			if !params.extendedEvalCtx.TxnIsSingleStmt {
				return errors.New("cannot inject statistics in an explicit transaction")
			}
			if err := injectTableStats(params, n.tableDesc, sd); err != nil {
				return err
			}

		case *tree.AlterTableSetStorageParams:
			setter := tablestorageparam.NewSetter(n.tableDesc)
			if err := storageparam.Set(
				params.ctx,
				params.p.SemaCtx(),
				params.EvalContext(),
				t.StorageParams,
				setter,
			); err != nil {
				return err
			}

			var err error
			descriptorChanged, err = handleTTLStorageParamChange(
				params,
				tn,
				setter.TableDesc,
				setter.UpdatedRowLevelTTL,
			)
			if err != nil {
				return err
			}

		case *tree.AlterTableResetStorageParams:
			setter := tablestorageparam.NewSetter(n.tableDesc)
			if err := storageparam.Reset(
				params.ctx,
				params.EvalContext(),
				t.Params,
				setter,
			); err != nil {
				return err
			}

			var err error
			descriptorChanged, err = handleTTLStorageParamChange(
				params,
				tn,
				setter.TableDesc,
				setter.UpdatedRowLevelTTL,
			)
			if err != nil {
				return err
			}

		case *tree.AlterTableRenameColumn:
			tableDesc := n.tableDesc
			columnName := t.Column
			if columnName == catpb.TTLDefaultExpirationColumnName &&
				tableDesc.HasRowLevelTTL() &&
				tableDesc.GetRowLevelTTL().HasDurationExpr() {
				return pgerror.Newf(
					pgcode.InvalidTableDefinition,
					`cannot rename column %s while ttl_expire_after is set`,
					columnName,
				)
			}
			descChanged, err := params.p.renameColumn(params.ctx, tableDesc, columnName, t.NewName)
			if err != nil {
				return err
			}
			descriptorChanged = descriptorChanged || descChanged

		case *tree.AlterTableRenameConstraint:
			constraint := catalog.FindConstraintByName(n.tableDesc, string(t.Constraint))
			if constraint == nil {
				return sqlerrors.NewUndefinedConstraintError(tree.ErrString(&t.Constraint), n.tableDesc.Name)
			}
			if t.Constraint == t.NewName {
				// Nothing to do.
				break
			}
			switch constraint.GetConstraintValidity() {
			case descpb.ConstraintValidity_Validating:
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint %q in the middle of being added, try again later", t.Constraint)
			case descpb.ConstraintValidity_Dropping:
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"constraint %q in the middle of being dropped", t.Constraint)
			}
			if other := catalog.FindConstraintByName(n.tableDesc, string(t.NewName)); other != nil {
				return pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", tree.ErrString(&t.NewName))
			}
			// If this is a unique or primary constraint, renames of the constraint
			// lead to renames of the underlying index. Ensure that no index with this
			// new name exists. This is what postgres does.
			if constraint.AsUniqueWithIndex() != nil {
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
				return params.p.dependentError(params.ctx,
					objType, tree.ErrString(&t.NewName), n.tableDesc.ParentID, refTableID, "rename",
				)
			}

			if err := n.tableDesc.RenameConstraint(constraint, string(t.NewName), depViewRenameError,
				func(desc *tabledesc.Mutable, ref catalog.ForeignKeyConstraint, newName string) error {
					return params.p.updateFKBackReferenceName(params.ctx, desc, ref.ForeignKeyDesc(), newName)
				}); err != nil {
				return err
			}
			descriptorChanged = true
		case *tree.AlterTableSetRLSMode:
			return unimplemented.NewWithIssuef(
				136700,
				"row-level security mode alteration is not supported")
		default:
			return errors.AssertionFailedf("unsupported alter command: %T", cmd)
		}

		// Allocate IDs now, so new IDs are available to subsequent commands
		version := params.ExecCfg().Settings.Version.ActiveVersion(params.ctx)
		if err := n.tableDesc.AllocateIDs(params.ctx, version); err != nil {
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
		mutationID = n.tableDesc.ClusterVersion().NextMutationID
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

	// Replace all UDF names with OIDs in check constraints and update back
	// references in functions used.
	for _, ck := range n.tableDesc.CheckConstraints() {
		if err := params.p.updateFunctionReferencesForCheck(params.ctx, n.tableDesc, ck.CheckDesc()); err != nil {
			return err
		}
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
	event := &auditevents.SensitiveTableAccessEvent{TableDesc: desc, Writing: true}
	p.curPlan.auditEventBuilders = append(p.curPlan.auditEventBuilders, event)

	// Requires MODIFYCLUSTERSETTING as of 22.2.
	// Check for system privilege first, otherwise fall back to role options.
	hasModify, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.MODIFYCLUSTERSETTING)
	if err != nil {
		return false, err
	}
	if !hasModify {
		return false, pgerror.Newf(pgcode.InsufficientPrivilege,
			"only users with admin or %s system privilege are allowed to change audit settings on a table ", privilege.MODIFYCLUSTERSETTING)
	}

	telemetry.Inc(sqltelemetry.SchemaSetAuditModeCounter(auditMode.TelemetryName()))

	return desc.SetAuditMode(auditMode)
}

func (n *alterTableNode) Next(runParams) (bool, error) { return false, nil }
func (n *alterTableNode) Values() tree.Datums          { return tree.Datums{} }
func (n *alterTableNode) Close(context.Context)        {}

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
		// If our column is computed, block mixing defaults in entirely.
		// This check exists here instead of later on during validation because
		// adding a null default to a computed column should also be blocked, but
		// is undetectable later on since SET DEFAULT NUL means a nil default
		// expression.
		if col.IsComputed() {
			// Block dropping a computed column "default" as well.
			if t.Default == nil {
				return pgerror.Newf(
					pgcode.Syntax,
					"column %q of relation %q is a computed column",
					col.GetName(),
					tn.ObjectName)
			}
			return pgerror.Newf(
				pgcode.Syntax,
				"computed column %q cannot also have a DEFAULT expression",
				col.GetName())
		}
		if err := updateNonComputedColExpr(
			params,
			tableDesc,
			col,
			t.Default,
			&col.ColumnDesc().DefaultExpr,
			tree.ColumnDefaultExprInSetDefault,
		); err != nil {
			return err
		}
		if col.HasNullDefault() {
			// `SET DEFAULT NULL` means a nil default expression.
			col.ColumnDesc().DefaultExpr = nil
		}

	case *tree.AlterTableSetOnUpdate:
		// We want to reject uses of ON UPDATE where there is also a foreign key ON
		// UPDATE.
		for _, fk := range tableDesc.OutboundFKs {
			for _, colID := range fk.OriginColumnIDs {
				if colID == col.GetID() &&
					fk.OnUpdate != semenumpb.ForeignKeyAction_NO_ACTION &&
					fk.OnUpdate != semenumpb.ForeignKeyAction_RESTRICT {
					return pgerror.Newf(
						pgcode.InvalidColumnDefinition,
						"column %s(%d) cannot have both an ON UPDATE expression and a foreign"+
							" key ON UPDATE action",
						col.GetName(),
						col.GetID(),
					)
				}
			}
		}

		if err := updateNonComputedColExpr(
			params,
			tableDesc,
			col,
			t.Expr,
			&col.ColumnDesc().OnUpdateExpr,
			tree.ColumnOnUpdateExpr,
		); err != nil {
			return err
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

		if err := addNotNullConstraintMutationForCol(tableDesc, col); err != nil {
			return err
		}

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
		col.ColumnDesc().Nullable = true

		// Add a check constraint equivalent to the non-null constraint and drop
		// it in the schema changer.
		check := tabledesc.MakeNotNullCheckConstraint(tableDesc, col,
			descpb.ConstraintValidity_Dropping, tableDesc.GetNextConstraintID())
		if tableDesc.Adding() {
			tableDesc.Checks = append(tableDesc.Checks, check)
		}
		tableDesc.NextConstraintID++
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

	case *tree.AlterTableAddIdentity:
		if typ := col.GetType(); typ == nil || typ.InternalType.Family != types.IntFamily {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"column %q of relation %q type must be an integer type", col.GetName(), tableDesc.GetName())
		}
		if col.IsGeneratedAsIdentity() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q of relation %q is already an identity column",
				col.GetName(), tableDesc.GetName())
		}
		if col.HasDefault() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q of relation %q already has a default value", col.GetName(), tableDesc.GetName())
		}
		if col.IsComputed() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q of relation %q already has a computed value", col.GetName(), tableDesc.GetName())
		}
		if col.IsNullable() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q of relation %q must be declared NOT NULL before identity can be added", col.GetName(), tableDesc.GetName())
		}
		if col.HasOnUpdate() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q of relation %q already has an update expression", col.GetName(), tableDesc.GetName())
		}

		// Create column definition for identity column
		q := []tree.NamedColumnQualification{{Qualification: t.Qualification}}
		colDef, err := tree.NewColumnTableDef(tree.Name(col.GetName()), col.GetType(), false /* isSerial */, q)
		if err != nil {
			return err
		}
		newDef, prefix, seqName, seqOpts, err := params.p.processSerialLikeInColumnDef(params.ctx, colDef, tn)
		if err != nil {
			return err
		}
		if seqName == nil {
			return errors.Newf("failed to create sequence %q for new identity column %q in %q", seqName, col.ColName(), tn)
		}
		colDef = newDef

		colOwnedSeqDesc, err := doCreateSequence(
			ctx,
			params.p,
			params.SessionData(),
			prefix.Database,
			prefix.Schema,
			seqName,
			tree.PersistencePermanent,
			seqOpts,
			fmt.Sprintf("creating sequence %q for new identity column %q in %q", seqName, col.ColName(), tn),
		)
		if err != nil {
			return err
		}

		typedExpr, _, err := sanitizeColumnExpression(params, colDef.DefaultExpr.Expr, col, tree.ColumnDefaultExprInSetDefault)
		if err != nil {
			return err
		}

		changedSeqDescs, err := maybeAddSequenceDependencies(
			params.ctx,
			params.p.ExecCfg().Settings,
			params.p,
			tableDesc,
			col.ColumnDesc(),
			typedExpr,
			nil, /* backrefs */
			tabledesc.DefaultExpr,
		)
		if err != nil {
			return err
		}
		for _, changedSeqDesc := range changedSeqDescs {
			// `colOwnedSeqDesc` and `changedSeqDesc` should refer to a same instance.
			// But we still want to use the right copy to write a schema change for by
			// using `changedSeqDesc` just in case the assumption became false in the
			// future.
			if colOwnedSeqDesc != nil && colOwnedSeqDesc.ID == changedSeqDesc.ID {
				if err := setSequenceOwner(changedSeqDesc, col.ColName(), tableDesc); err != nil {
					return err
				}
			}
			if err := params.p.writeSchemaChange(
				params.ctx, changedSeqDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(t, params.Ann()),
			); err != nil {
				return err
			}
		}

		// Set column description to identity
		switch (t.Qualification).(type) {
		case *tree.GeneratedAlwaysAsIdentity:
			col.ColumnDesc().GeneratedAsIdentityType = catpb.GeneratedAsIdentityType_GENERATED_ALWAYS
		case *tree.GeneratedByDefAsIdentity:
			col.ColumnDesc().GeneratedAsIdentityType = catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT
		}
		seqOptsStr := tree.Serialize(&seqOpts)
		col.ColumnDesc().GeneratedAsIdentitySequenceOption = &seqOptsStr

	case *tree.AlterTableSetIdentity:
		if !col.IsGeneratedAsIdentity() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q of relation %q is not an identity column",
				col.GetName(), tableDesc.GetName())
		}

		switch t.GeneratedAsIdentityType {
		case tree.GeneratedAlways:
			if col.IsGeneratedAlwaysAsIdentity() {
				return nil
			}
			col.ColumnDesc().GeneratedAsIdentityType = catpb.GeneratedAsIdentityType_GENERATED_ALWAYS
		case tree.GeneratedByDefault:
			if col.IsGeneratedByDefaultAsIdentity() {
				return nil
			}
			col.ColumnDesc().GeneratedAsIdentityType = catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT
		}

	case *tree.AlterTableIdentity:
		if !col.IsGeneratedAsIdentity() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q of relation %q is not an identity column",
				col.GetName(), tableDesc.GetName())
		}

		// It is assumed that an identify column owns only one sequence.
		if col.NumUsesSequences() != 1 {
			return errors.AssertionFailedf(
				"identity column %q of relation %q has %d sequences instead of 1",
				col.GetName(), tableDesc.GetName(), col.NumUsesSequences())
		}

		seqDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Table(ctx, col.GetUsesSequenceID(0))
		if err != nil {
			return err
		}

		// Alter referenced sequence for identity with sepcified option.
		// Does not override existing values if not specified.
		if err := alterSequenceImpl(params, seqDesc, t.SeqOptions, t); err != nil {
			return err
		}

		opts := seqDesc.GetSequenceOpts()
		optsNode := tree.SequenceOptions{}
		if opts.CacheSize > 1 {
			optsNode = append(optsNode, tree.SequenceOption{Name: tree.SeqOptCache, IntVal: &opts.CacheSize})
		}
		optsNode = append(optsNode, tree.SequenceOption{Name: tree.SeqOptMinValue, IntVal: &opts.MinValue})
		optsNode = append(optsNode, tree.SequenceOption{Name: tree.SeqOptMaxValue, IntVal: &opts.MaxValue})
		optsNode = append(optsNode, tree.SequenceOption{Name: tree.SeqOptIncrement, IntVal: &opts.Increment})
		optsNode = append(optsNode, tree.SequenceOption{Name: tree.SeqOptStart, IntVal: &opts.Start})
		if opts.Virtual {
			optsNode = append(optsNode, tree.SequenceOption{Name: tree.SeqOptVirtual})
		}
		s := tree.Serialize(&optsNode)
		col.ColumnDesc().GeneratedAsIdentitySequenceOption = &s

	case *tree.AlterTableDropIdentity:
		if !col.IsGeneratedAsIdentity() {
			if t.IfExists {
				params.p.BufferClientNotice(
					params.ctx,
					pgnotice.Newf("column %q of relation %q is not an identity column, skipping", col.GetName(), tableDesc.GetName()),
				)
				return nil
			}
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q of relation %q is not an identity column",
				col.GetName(), tableDesc.GetName())
		}

		// It is assumed that an identify column owns only one sequence.
		if col.NumUsesSequences() != 1 {
			return errors.AssertionFailedf(
				"identity column %q of relation %q has %d sequences instead of 1",
				col.GetName(), tableDesc.GetName(), col.NumUsesSequences())
		}

		// Verify sequence is not depended on by another column.
		// Use tree.DropDefault behavior to verify without the need to alter other dependencies via tree.DropCascade.
		if err := params.p.canRemoveAllColumnOwnedSequences(params.ctx, tableDesc, col, tree.DropDefault); err != nil {
			return err
		}
		// Drop the identity flag first, so that it is treated like a normal column.
		// Otherwise, we will run into the assertion saying that uses sequences should
		// exist.
		col.ColumnDesc().GeneratedAsIdentityType = catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN

		// Drop the identity sequence and remove it from the column OwnsSequenceIds and DefaultExpr.
		// Use tree.DropCascade behavior to remove dependencies on the column.
		if err := params.p.dropSequencesOwnedByCol(params.ctx, col, true /* queueJob */, tree.DropCascade); err != nil {
			return err
		}

		// Remove column identity descriptors
		col.ColumnDesc().GeneratedAsIdentitySequenceOption = nil

	}
	return nil
}

func addNotNullConstraintMutationForCol(tableDesc *tabledesc.Mutable, col catalog.Column) error {
	check := tabledesc.MakeNotNullCheckConstraint(tableDesc, col,
		descpb.ConstraintValidity_Validating, tableDesc.GetNextConstraintID())
	tableDesc.AddNotNullMutation(check, descpb.DescriptorMutation_ADD)
	tableDesc.NextConstraintID++
	return nil
}

func labeledRowValues(cols []catalog.Column, values tree.Datums) string {
	var s bytes.Buffer
	for i := range cols {
		if i != 0 {
			s.WriteString(`, `)
		}
		colName := cols[i].ColName()
		s.WriteString(colName.String())
		s.WriteString(`=`)
		s.WriteString(values[i].String())
	}
	return s.String()
}

// updateNonComputedColExpr updates an ON UPDATE or DEFAULT column expression
// and recalculates sequence dependencies for the column. `exprField1 is a
// pointer to the column descriptor field that should be updated with the
// serialized `newExpr`. For example, for DEFAULT expressions, this is
// `&column.ColumnDesc().OnUpdateExpr`
func updateNonComputedColExpr(
	params runParams,
	tab *tabledesc.Mutable,
	col catalog.Column,
	newExpr tree.Expr,
	exprField **string,
	op tree.SchemaExprContext,
) error {
	if col.IsGeneratedAsIdentity() {
		return sqlerrors.NewSyntaxErrorf("column %q is an identity column", col.GetName())
	}

	// If a DEFAULT or ON UPDATE expression starts using a sequence and is then
	// modified to not use that sequence, we need to drop the dependency from
	// the sequence to the column. The way this is done is by wiping all
	// sequence dependencies on the column and then recalculating the
	// dependencies after the new expression has been parsed.
	if col.NumUsesSequences() > 0 {
		if err := params.p.removeSequenceDependencies(params.ctx, tab, col); err != nil {
			return err
		}
	}

	if newExpr == nil {
		*exprField = nil
	} else {
		_, s, err := sanitizeColumnExpression(params, newExpr, col, op)
		if err != nil {
			return err
		}

		*exprField = &s
	}

	if err := updateSequenceDependencies(params, tab, col, op); err != nil {
		return err
	}

	if err := params.p.maybeUpdateFunctionReferencesForColumn(params.ctx, tab, col.ColumnDesc()); err != nil {
		return err
	}

	return nil
}

func sanitizeColumnExpression(
	p runParams, expr tree.Expr, col catalog.Column, context tree.SchemaExprContext,
) (tree.TypedExpr, string, error) {
	colDatumType := col.GetType()
	typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
		p.ctx, expr, colDatumType, context, &p.p.semaCtx, volatility.Volatile, false, /*allowAssignmentCast*/
	)
	if err != nil {
		return nil, "", pgerror.WithCandidateCode(err, pgcode.DatatypeMismatch)
	}

	if err := funcdesc.MaybeFailOnUDFUsage(typedExpr, context, p.EvalContext().Settings.Version.ActiveVersionOrEmpty(p.ctx)); err != nil {
		return nil, "", err
	}

	typedExpr, err = schemaexpr.MaybeReplaceUDFNameWithOIDReferenceInTypedExpr(typedExpr)
	if err != nil {
		return nil, "", err
	}

	s := tree.Serialize(typedExpr)
	return typedExpr, s, nil
}

// updateSequenceDependencies checks for sequence dependencies on the provided
// DEFAULT and ON UPDATE expressions and adds any dependencies to the tableDesc.
func updateSequenceDependencies(
	params runParams,
	tableDesc *tabledesc.Mutable,
	colDesc catalog.Column,
	defaultExprCtx tree.SchemaExprContext,
) error {
	var seqDescsToUpdate []*tabledesc.Mutable
	mergeNewSeqDescs := func(toAdd []*tabledesc.Mutable) {
		seqDescsToUpdate = append(seqDescsToUpdate, toAdd...)
		sort.Slice(seqDescsToUpdate,
			func(i, j int) bool {
				return seqDescsToUpdate[i].GetID() < seqDescsToUpdate[j].GetID()
			})
		truncated := make([]*tabledesc.Mutable, 0, len(seqDescsToUpdate))
		for i, v := range seqDescsToUpdate {
			if i == 0 || seqDescsToUpdate[i-1].GetID() != v.GetID() {
				truncated = append(truncated, v)
			}
		}
		seqDescsToUpdate = truncated
	}
	for _, colExpr := range []struct {
		colExprKind    tabledesc.ColExprKind
		colExprContext tree.SchemaExprContext
		exists         func() bool
		get            func() string
	}{
		{
			colExprKind:    tabledesc.DefaultExpr,
			colExprContext: defaultExprCtx,
			exists:         colDesc.HasDefault,
			get:            colDesc.GetDefaultExpr,
		},
		{
			colExprKind:    tabledesc.OnUpdateExpr,
			colExprContext: tree.ColumnOnUpdateExpr,
			exists:         colDesc.HasOnUpdate,
			get:            colDesc.GetOnUpdateExpr,
		},
	} {
		if !colExpr.exists() {
			continue
		}
		untypedExpr, err := parser.ParseExpr(colExpr.get())
		if err != nil {
			panic(err)
		}

		typedExpr, _, err := sanitizeColumnExpression(
			params,
			untypedExpr,
			colDesc,
			colExpr.colExprContext,
		)
		if err != nil {
			return err
		}

		newSeqDescs, err := maybeAddSequenceDependencies(
			params.ctx,
			params.p.ExecCfg().Settings,
			params.p,
			tableDesc,
			colDesc.ColumnDesc(),
			typedExpr,
			nil, /* backrefs */
			colExpr.colExprKind,
		)
		if err != nil {
			return err
		}

		mergeNewSeqDescs(newSeqDescs)
	}

	for _, changedSeqDesc := range seqDescsToUpdate {
		if err := params.p.writeSchemaChange(
			params.ctx, changedSeqDesc, descpb.InvalidMutationID,
			fmt.Sprintf("updating dependent sequence %s(%d) for table %s(%d)",
				changedSeqDesc.Name, changedSeqDesc.ID, tableDesc.Name, tableDesc.ID,
			)); err != nil {
			return err
		}
	}

	return nil
}

// injectTableStats implements the INJECT STATISTICS command, which deletes any
// existing statistics on the table and replaces them with the statistics in the
// given json object (in the same format as the result of SHOW STATISTICS USING
// JSON). This is useful for reproducing planning issues without importing the
// data.
func injectTableStats(
	params runParams, desc catalog.TableDescriptor, statsExpr tree.TypedExpr,
) error {
	val, err := eval.Expr(params.ctx, params.EvalContext(), statsExpr)
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

	// Check that we're not injecting any forecasted stats.
	for i := range jsonStats {
		if jsonStats[i].Name == jobspb.ForecastStatsName {
			return errors.WithHintf(
				pgerror.New(pgcode.InvalidName, "cannot inject forecasted statistics"),
				"either remove forecasts from the statement, or rename them from %q to something else",
				jobspb.ForecastStatsName,
			)
		}
	}

	// First, delete all statistics for the table. (We use the current transaction
	// so that this will rollback on any error.)
	if _ /* rows */, err := params.p.InternalSQLTxn().Exec(
		params.ctx,
		"delete-stats",
		params.p.Txn(),
		`DELETE FROM system.table_statistics WHERE "tableID" = $1`, desc.GetID(),
	); err != nil {
		return errors.Wrapf(err, "failed to delete old stats")
	}

	// Insert each statistic.
StatsLoop:
	for i := range jsonStats {
		s := &jsonStats[i]
		h, err := s.GetHistogram(params.ctx, &params.p.semaCtx, params.EvalContext())
		if err != nil {
			return err
		}

		// Check that the type matches.
		// TODO(49698): When we support multi-column histograms this check will need
		// adjustment.
		if len(s.Columns) == 1 {
			col := catalog.FindColumnByName(desc, s.Columns[0])
			// Ignore dropped columns (they are handled below).
			if col != nil {
				if err := h.TypeCheck(
					col.GetType(), desc.GetName(), s.Columns[0], stats.TSFromString(s.CreatedAt),
				); err != nil {
					return pgerror.WithCandidateCode(err, pgcode.DatatypeMismatch)
				}
			}
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
			col := catalog.FindColumnByName(desc, colName)
			if col == nil {
				params.p.BufferClientNotice(
					params.ctx,
					pgnotice.Newf("column %q does not exist", colName),
				)
				continue StatsLoop
			}
			if err := columnIDs.Append(tree.NewDInt(tree.DInt(col.GetID()))); err != nil {
				return err
			}
		}

		if err := insertJSONStatistic(params, desc.GetID(), columnIDs, s, histogram); err != nil {
			return errors.Wrap(err, "failed to insert stats")
		}
	}

	// Invalidate the local cache synchronously; this guarantees that the next
	// statement in the same session won't use a stale cache (the cache would
	// normally be updated asynchronously).
	params.extendedEvalCtx.ExecCfg.TableStatsCache.InvalidateTableStats(params.ctx, desc.GetID())

	return nil
}

func insertJSONStatistic(
	params runParams,
	tableID descpb.ID,
	columnIDs *tree.DArray,
	s *stats.JSONStatistic,
	histogram interface{},
) error {
	var (
		ctx = params.ctx
		txn = params.p.InternalSQLTxn()
	)

	var name interface{}
	if s.Name != "" {
		name = s.Name
	}

	var predicateValue interface{}
	if s.PartialPredicate != "" {
		predicateValue = s.PartialPredicate
	}

	var fullStatisticIDValue interface{}
	if s.FullStatisticID != 0 {
		fullStatisticIDValue = s.FullStatisticID
	}

	if s.ID != 0 {
		_ /* rows */, err := txn.Exec(
			ctx,
			"insert-stats",
			txn.KV(),
			`INSERT INTO system.table_statistics (
					"statisticID",
					"tableID",
					"name",
					"columnIDs",
					"createdAt",
					"rowCount",
					"distinctCount",
					"nullCount",
					"avgSize",
					histogram,
					"partialPredicate",
					"fullStatisticID"
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			s.ID,
			tableID,
			name,
			columnIDs,
			s.CreatedAt,
			s.RowCount,
			s.DistinctCount,
			s.NullCount,
			s.AvgSize,
			histogram,
			predicateValue,
			fullStatisticIDValue,
		)
		return err
	} else {
		_ /* rows */, err := txn.Exec(
			ctx,
			"insert-stats",
			txn.KV(),
			`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"createdAt",
					"rowCount",
					"distinctCount",
					"nullCount",
					"avgSize",
					histogram,
					"partialPredicate",
					"fullStatisticID"
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			tableID,
			name,
			columnIDs,
			s.CreatedAt,
			s.RowCount,
			s.DistinctCount,
			s.NullCount,
			s.AvgSize,
			histogram,
			predicateValue,
			fullStatisticIDValue,
		)
		return err
	}
}

// validateConstraintNameIsNotUsed checks that the name of the constraint we're
// trying to add isn't already used, and, if it is, whether the constraint
// addition should be skipped:
// - if the name is free to use, it returns false;
// - if it's already used but IF NOT EXISTS was specified, it returns true;
// - otherwise, it returns an error.
func validateConstraintNameIsNotUsed(
	tableDesc *tabledesc.Mutable, cmd *tree.AlterTableAddConstraint,
) (skipAddConstraint bool, _ error) {
	var name tree.Name
	var hasIfNotExists bool
	switch d := cmd.ConstraintDef.(type) {
	case *tree.CheckConstraintTableDef:
		name = d.Name
		hasIfNotExists = d.IfNotExists
	case *tree.ForeignKeyConstraintTableDef:
		name = d.Name
		hasIfNotExists = d.IfNotExists
	case *tree.UniqueConstraintTableDef:
		name = d.Name
		hasIfNotExists = d.IfNotExists
		if d.WithoutIndex {
			break
		}
		// Handle edge cases specific to unique constraints with indexes.
		if d.PrimaryKey {
			// We only support "adding" a primary key when we are using the
			// default rowid primary index or if a DROP PRIMARY KEY statement
			// was processed before this statement. If a DROP PRIMARY KEY
			// statement was processed, then n.tableDesc.HasPrimaryKey() = false.
			if tableDesc.HasPrimaryKey() && !tableDesc.IsPrimaryIndexDefaultRowID() {
				if d.IfNotExists {
					return true, nil
				}
				return false, pgerror.Newf(pgcode.InvalidTableDefinition,
					"multiple primary keys for table %q are not allowed", tableDesc.Name)
			}

			// Allow the PRIMARY KEY to have the same name as the existing PRIMARY KEY
			// if the existing PRIMARY KEY is the implicit rowid column.
			// This allows CREATE TABLE without a PRIMARY KEY, then adding a
			// PRIMARY KEY with the same autogenerated name as postgres does
			// without erroring if the rowid PRIMARY KEY name conflicts.
			// The implicit rowid PRIMARY KEY index will be deleted anyway, so we're
			// ok with the conflict in this case.
			defaultPKName := tabledesc.PrimaryKeyIndexName(tableDesc.GetName())
			if tableDesc.HasPrimaryKey() && tableDesc.IsPrimaryIndexDefaultRowID() &&
				tableDesc.PrimaryIndex.Name == defaultPKName &&
				name == tree.Name(defaultPKName) {
				return false, nil
			}
			// If there is no active primary key, then adding one with the exact
			// same name is allowed.
			if !tableDesc.HasPrimaryKey() &&
				tableDesc.PrimaryIndex.Name == string(name) {
				return false, nil
			}
		}
		if name == "" {
			return false, nil
		}
		idx := catalog.FindIndexByName(tableDesc, string(name))
		// If an index is found and its disabled, then we know it will be dropped
		// later on.
		if idx == nil {
			return false, nil
		}
		if d.IfNotExists {
			return true, nil
		}
		if idx.Dropped() {
			return false, pgerror.Newf(pgcode.DuplicateRelation, "constraint with name %q already exists and is being dropped, try again later", name)
		}
		return false, pgerror.Newf(pgcode.DuplicateRelation, "constraint with name %q already exists", name)

	default:
		return false, errors.AssertionFailedf(
			"unsupported constraint: %T", cmd.ConstraintDef)
	}

	if name == "" {
		return false, nil
	}
	constraint := catalog.FindConstraintByName(tableDesc, string(name))
	if constraint == nil {
		return false, nil
	}
	// If the primary index is being replaced, then the name can be reused for
	// another constraint.
	if u := constraint.AsUniqueWithIndex(); u != nil && u.GetID() == tableDesc.GetPrimaryIndexID() {
		for _, mut := range tableDesc.GetMutations() {
			if primaryKeySwap := mut.GetPrimaryKeySwap(); primaryKeySwap != nil &&
				primaryKeySwap.OldPrimaryIndexId == u.GetID() &&
				primaryKeySwap.NewPrimaryIndexName != name.String() {
				return false, nil
			}
		}
	}
	if hasIfNotExists {
		return true, nil
	}
	return false, pgerror.Newf(pgcode.DuplicateObject,
		"duplicate constraint name: %q", name)
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
		lookup, err := p.Descriptors().MutableByID(p.txn).Table(ctx, ref.ReferencedTableID)
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

func dropColumnImpl(
	params runParams,
	tn *tree.TableName,
	tableDesc *tabledesc.Mutable,
	rowLevelTTL *catpb.RowLevelTTL,
	t *tree.AlterTableDropColumn,
) (droppedViews []string, err error) {
	if tableDesc.IsLocalityRegionalByRow() {
		rbrColName, err := tableDesc.GetRegionalByRowTableRegionColumnName()
		if err != nil {
			return nil, err
		}
		if rbrColName == t.Column {
			return nil, errors.WithHintf(
				pgerror.Newf(
					pgcode.InvalidColumnReference,
					"cannot drop column %s as it is used to store the region in a REGIONAL BY ROW table",
					t.Column,
				),
				"You must change the table locality before dropping this table or alter the table to use a different column to use for the region.",
			)
		}
	}

	colToDrop, err := catalog.MustFindColumnByTreeName(tableDesc, t.Column)
	if err != nil {
		if t.IfExists {
			// Noop.
			return nil, nil
		}
		return nil, err
	}
	if colToDrop.Dropped() {
		return nil, nil
	}

	// Block modification on system columns.
	if colToDrop.IsSystemColumn() {
		return nil, pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot alter system column %q", colToDrop.GetName())
	}

	if colToDrop.IsInaccessible() {
		return nil, pgerror.Newf(
			pgcode.InvalidColumnReference,
			"cannot drop inaccessible column %q",
			t.Column,
		)
	}

	if err := params.p.disallowDroppingPrimaryIndexReferencedInUDFOrView(params.ctx, tableDesc); err != nil {
		return nil, err
	}

	// If the dropped column uses a sequence, remove references to it from that sequence.
	if colToDrop.NumUsesSequences() > 0 {
		if err := params.p.removeSequenceDependencies(params.ctx, tableDesc, colToDrop); err != nil {
			return nil, err
		}
	}

	if colToDrop.NumUsesFunctions() > 0 {
		if err := params.p.removeColumnBackReferenceInFunctions(params.ctx, tableDesc, colToDrop.ColumnDesc()); err != nil {
			return nil, err
		}
	}

	// You can't remove a column that owns a sequence that is depended on
	// by another column
	if err := params.p.canRemoveAllColumnOwnedSequences(params.ctx, tableDesc, colToDrop, t.DropBehavior); err != nil {
		return nil, err
	}

	if err := params.p.dropSequencesOwnedByCol(params.ctx, colToDrop, true /* queueJob */, t.DropBehavior); err != nil {
		return nil, err
	}

	// You can't drop a column depended on by a view unless CASCADE was
	// specified.
	var depsToDrop catalog.DescriptorIDSet
	for _, ref := range tableDesc.DependedOnBy {
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
		err := params.p.canRemoveDependent(
			params.ctx, "column", string(t.Column), tableDesc.ParentID, ref, t.DropBehavior,
		)
		if err != nil {
			return nil, err
		}
		depsToDrop.Add(ref.ID)
	}

	droppedViews, err = params.p.removeDependents(
		params.ctx, tableDesc, depsToDrop, "column", colToDrop.GetName(), t.DropBehavior,
	)
	if err != nil {
		return nil, err
	}

	// We cannot remove this column if there are computed columns, TTL expiration
	// expression, or policy expressions that use it.
	if err := schemaexpr.ValidateColumnHasNoDependents(tableDesc, colToDrop); err != nil {
		return nil, err
	}
	if err := schemaexpr.ValidateTTLExpression(tableDesc, rowLevelTTL, colToDrop, tn, "drop"); err != nil {
		return nil, err
	}
	if err := schemaexpr.ValidatePolicyExpressionsDoNotDependOnColumn(tableDesc, colToDrop, "column", "drop"); err != nil {
		return nil, err
	}

	if tableDesc.GetPrimaryIndex().CollectKeyColumnIDs().Contains(colToDrop.GetID()) {
		return nil, sqlerrors.NewColumnReferencedByPrimaryKeyError(colToDrop.GetName())
	}
	var idxNamesToDelete []string
	for _, idx := range tableDesc.NonDropIndexes() {
		// We automatically drop indexes that reference the column
		// being dropped.
		containsThisColumn := idx.CollectKeyColumnIDs().Contains(colToDrop.GetID()) ||
			idx.CollectKeySuffixColumnIDs().Contains(colToDrop.GetID()) ||
			idx.CollectSecondaryStoredColumnIDs().Contains(colToDrop.GetID())
		if idx.IsPartial() {
			expr, err := parser.ParseExpr(idx.GetPredicate())
			if err != nil {
				return nil, err
			}

			colIDs, err := schemaexpr.ExtractColumnIDs(tableDesc, expr)
			if err != nil {
				return nil, err
			}

			if colIDs.Contains(colToDrop.GetID()) {
				containsThisColumn = true
				return nil, sqlerrors.ColumnReferencedByPartialIndex(
					"drop", "column", string(colToDrop.ColName()), idx.GetName())
			}
		}
		// Perform the DROP.
		if containsThisColumn {
			idxNamesToDelete = append(idxNamesToDelete, idx.GetName())
		}
	}

	for _, idxName := range idxNamesToDelete {
		params.EvalContext().ClientNoticeSender.BufferClientNotice(params.ctx, pgnotice.Newf(
			"dropping index %q which depends on column %q",
			idxName,
			colToDrop.ColName(),
		))
		jobDesc := fmt.Sprintf(
			"removing index %q dependent on column %q which is being dropped; full details: %s",
			idxName,
			colToDrop.ColName(),
			tree.AsStringWithFQNames(tn, params.Ann()),
		)
		if err := params.p.dropIndexByName(
			params.ctx, tn, tree.UnrestrictedName(idxName), tableDesc, false,
			t.DropBehavior, ignoreIdxConstraint, jobDesc,
		); err != nil {
			return nil, err
		}
	}

	// Drop non-index-backed unique constraints which reference the column.
	for _, uwoi := range tableDesc.EnforcedUniqueConstraintsWithoutIndex() {
		if uwoi.IsPartial() {
			expr, err := parser.ParseExpr(uwoi.GetPredicate())
			if err != nil {
				return nil, err
			}

			colIDs, err := schemaexpr.ExtractColumnIDs(tableDesc, expr)
			if err != nil {
				return nil, err
			}

			if colIDs.Contains(colToDrop.GetID()) {
				return nil, sqlerrors.ColumnReferencedByPartialUniqueWithoutIndexConstraint(
					"drop", "column", string(colToDrop.ColName()), uwoi.GetName())
			}
		}
		if uwoi.Dropped() || !uwoi.CollectKeyColumnIDs().Contains(colToDrop.GetID()) {
			continue
		}
		// If this unique constraint is used on the referencing side of any FK
		// constraints, try to remove the references. Don't bother trying to find
		// an alternate index or constraint, since all possible matches will
		// be dropped when the column is dropped.
		const withSearchForReplacement = false
		if err := params.p.tryRemoveFKBackReferences(
			params.ctx, tableDesc, uwoi, t.DropBehavior, withSearchForReplacement,
		); err != nil {
			return nil, err
		}
		if err := tableDesc.DropConstraint(uwoi, nil /* removeFKBackRef */, nil /* removeFnBackRef */); err != nil {
			return nil, err
		}
	}

	// Drop check constraints which reference the column.
	for _, check := range tableDesc.CheckConstraints() {
		if check.Dropped() {
			continue
		}
		if !check.CollectReferencedColumnIDs().Contains(colToDrop.GetID()) {
			continue
		}
		if err := tableDesc.DropConstraint(
			check,
			nil, /* removeFKBackRef */
			func(ck *descpb.TableDescriptor_CheckConstraint) error {
				return params.p.removeCheckBackReferenceInFunctions(params.ctx, tableDesc, ck)
			},
		); err != nil {
			return nil, err
		}
	}

	if err := params.p.deleteComment(
		params.ctx, tableDesc.ID, uint32(colToDrop.GetPGAttributeNum()), catalogkeys.ColumnCommentType,
	); err != nil {
		return nil, err
	}

	// Since we are able to drop indexes used by foreign keys on the origin side,
	// the drop index codepaths aren't going to remove dependent FKs, so we
	// need to do that here.
	// We update the FK's slice in place here.
	sliceIdx := 0
	for i := range tableDesc.OutboundFKs {
		tableDesc.OutboundFKs[sliceIdx] = tableDesc.OutboundFKs[i]
		sliceIdx++
		fk := &tableDesc.OutboundFKs[i]
		if descpb.ColumnIDs(fk.OriginColumnIDs).Contains(colToDrop.GetID()) {
			sliceIdx--
			if err := params.p.removeFKBackReference(params.ctx, tableDesc, fk); err != nil {
				return nil, err
			}
		}
	}
	tableDesc.OutboundFKs = tableDesc.OutboundFKs[:sliceIdx]

	found := false
	for i := range tableDesc.Columns {
		if tableDesc.Columns[i].ID == colToDrop.GetID() {
			tableDesc.AddColumnMutation(colToDrop.ColumnDesc(), descpb.DescriptorMutation_DROP)
			// Use [:i:i] to prevent reuse of existing slice, or outstanding refs
			// to ColumnDescriptors may unexpectedly change.
			tableDesc.Columns = append(tableDesc.Columns[:i:i], tableDesc.Columns[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"column %q in the middle of being added, try again later", t.Column)
	}

	return droppedViews, validateDescriptor(params.ctx, params.p, tableDesc)
}

// handleTTLStorageParamChange changes TTL storage parameters. descriptorChanged
// must be true if the descriptor was modified directly. The caller
// (alterTableNode), has a separate check to see if any mutations were
// enqueued.
func handleTTLStorageParamChange(
	params runParams, tn *tree.TableName, tableDesc *tabledesc.Mutable, after *catpb.RowLevelTTL,
) (descriptorChanged bool, err error) {

	before := tableDesc.GetRowLevelTTL()

	// Update existing config.
	if before != nil && after != nil {

		// Update cron schedule if required.
		if before.DeletionCron != after.DeletionCron {
			env := JobSchedulerEnv(params.ExecCfg().JobsKnobs())
			schedules := jobs.ScheduledJobTxn(params.p.InternalSQLTxn())
			s, err := schedules.Load(
				params.ctx,
				env,
				after.ScheduleID,
			)
			if err != nil {
				return false, err
			}
			if err := s.SetScheduleAndNextRun(after.DeletionCronOrDefault()); err != nil {
				return false, err
			}
			if err := schedules.Update(params.ctx, s); err != nil {
				return false, err
			}
		}

		// Update default expression on automated column if required.
		if before.HasDurationExpr() && after.HasDurationExpr() && before.DurationExpr != after.DurationExpr {
			col, err := catalog.MustFindColumnByName(tableDesc, catpb.TTLDefaultExpirationColumnName)
			if err != nil {
				return false, err
			}
			intervalExpr, err := parser.ParseExpr(string(after.DurationExpr))
			if err != nil {
				return false, errors.Wrapf(err, "unexpected expression for TTL duration")
			}
			newExpr := rowLevelTTLAutomaticColumnExpr(intervalExpr)

			if err := updateNonComputedColExpr(
				params,
				tableDesc,
				col,
				newExpr,
				&col.ColumnDesc().DefaultExpr,
				tree.TTLDefaultExpr,
			); err != nil {
				return false, err
			}

			if err := updateNonComputedColExpr(
				params,
				tableDesc,
				col,
				newExpr,
				&col.ColumnDesc().OnUpdateExpr,
				tree.TTLUpdateExpr,
			); err != nil {
				return false, err
			}
		}
	}

	// Create new column.
	if (before == nil || !before.HasDurationExpr()) && (after != nil && after.HasDurationExpr()) {
		if catalog.FindColumnByName(tableDesc, catpb.TTLDefaultExpirationColumnName) != nil {
			return false, pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"cannot add TTL to table with the %s column already defined",
				catpb.TTLDefaultExpirationColumnName,
			)
		}
		col, err := rowLevelTTLAutomaticColumnDef(after)
		if err != nil {
			return false, err
		}
		addCol := &tree.AlterTableAddColumn{
			ColumnDef: col,
		}
		if err := params.p.addColumnImpl(
			params,
			&alterTableNode{
				tableDesc: tableDesc,
				n: &tree.AlterTable{
					Cmds: []tree.AlterTableCmd{addCol},
				},
			},
			tn,
			tableDesc,
			addCol,
		); err != nil {
			return false, err
		}
		version := params.ExecCfg().Settings.Version.ActiveVersion(params.ctx)
		if err := tableDesc.AllocateIDs(params.ctx, version); err != nil {
			return false, err
		}
	}

	// Remove existing column.
	if (before != nil && before.HasDurationExpr()) && (after == nil || !after.HasDurationExpr()) {
		telemetry.Inc(sqltelemetry.RowLevelTTLDropped)
		droppedViews, err := dropColumnImpl(params, tn, tableDesc, after, &tree.AlterTableDropColumn{
			Column: catpb.TTLDefaultExpirationColumnName,
		})
		if err != nil {
			return false, err
		}
		// This should never happen as we do not CASCADE, but error again just in case.
		if len(droppedViews) > 0 {
			return false, pgerror.Newf(pgcode.InvalidParameterValue, "cannot drop TTL automatic column if it is depended on by a view")
		}
	}

	// Adding TTL requires adding the TTL job before adding the TTL fields.
	// Removing TTL requires removing the TTL job before removing the TTL fields.
	var direction descpb.DescriptorMutation_Direction
	directlyModifiesDescriptor := false
	switch {
	case before == nil && after != nil:
		direction = descpb.DescriptorMutation_ADD
	case before != nil && after == nil:
		direction = descpb.DescriptorMutation_DROP
	default:
		directlyModifiesDescriptor = true
	}
	if !directlyModifiesDescriptor {
		// Add TTL mutation so that job is scheduled in SchemaChanger.
		tableDesc.AddModifyRowLevelTTLMutation(
			&descpb.ModifyRowLevelTTL{RowLevelTTL: after},
			direction,
		)
		// Also, check if the table has inbound foreign keys (i.e. this table is being
		// referenced  by other tables). In such a case, flag a notice to the user
		// advising them to update the ttl_delete_batch_size to avoid generating
		// TTL deletion jobs with a high cardinality of rows being deleted.
		// See https://github.com/cockroachdb/cockroach/issues/125103 for more details.
		for _, fk := range tableDesc.InboundFKs {
			// Use foreign key actions to determine upstream impact and flag a notice if the
			// actions for delete involve cascading deletes for any one of the inbound foreign keys.
			if fk.OnDelete != semenumpb.ForeignKeyAction_NO_ACTION && fk.OnDelete != semenumpb.ForeignKeyAction_RESTRICT {
				params.p.BufferClientNotice(
					params.ctx,
					pgnotice.Newf("Columns within table %s are referenced as foreign keys."+
						" This will make TTL deletion jobs more expensive as dependent rows"+
						" in other tables will need to be updated as well. To improve performance"+
						" of the TTL job, consider reducing the value of ttl_delete_batch_size.", tableDesc.GetName()))
			}
		}
	}
	// Validate the type and volatility of ttl_expiration_expression.
	if after != nil {
		if err := schemaexpr.ValidateTTLExpirationExpression(
			params.ctx, tableDesc, params.p.SemaCtx(), tn, after,
			params.ExecCfg().Settings.Version.ActiveVersion(params.ctx),
		); err != nil {
			return false, err
		}
	}

	// Modify the TTL fields here because it will not be done in a mutation.
	if directlyModifiesDescriptor {
		tableDesc.RowLevelTTL = after
	}

	return directlyModifiesDescriptor, nil
}

// tryRemoveFKBackReferences determines whether the provided unique constraint
// is used on the referencing side of a FK constraint. If so, it tries to remove
// the references or find an alternate unique constraint that will suffice.
func (p *planner) tryRemoveFKBackReferences(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	uniqueConstraint catalog.UniqueConstraint,
	behavior tree.DropBehavior,
	withSearchForReplacement bool,
) error {
	isSuitable := func(fk catalog.ForeignKeyConstraint, u catalog.UniqueConstraint) bool {
		return u.GetConstraintID() != uniqueConstraint.GetConstraintID() && !u.Dropped() &&
			u.IsValidReferencedUniqueConstraint(fk)
	}
	uwis := tableDesc.UniqueConstraintsWithIndex()
	uwois := tableDesc.UniqueConstraintsWithoutIndex()
	uniqueConstraintHasReplacementCandidate := func(
		fk catalog.ForeignKeyConstraint,
	) bool {
		if !withSearchForReplacement {
			return false
		}
		for _, uwi := range uwis {
			if isSuitable(fk, uwi) {
				return true
			}
		}
		for _, uwoi := range uwois {
			if isSuitable(fk, uwoi) {
				return true
			}
		}
		return false
	}

	// Index for updating the FK slices in place when removing FKs.
	sliceIdx := 0
	for i, fk := range tableDesc.InboundForeignKeys() {
		tableDesc.InboundFKs[sliceIdx] = tableDesc.InboundFKs[i]
		sliceIdx++
		// The constraint being deleted could potentially be required by a
		// referencing foreign key. Find alternatives if that's the case,
		// otherwise remove the foreign key.
		if uniqueConstraint.IsValidReferencedUniqueConstraint(fk) &&
			!uniqueConstraintHasReplacementCandidate(fk) {
			// If we haven't found a replacement, then we check that the drop
			// behavior is cascade.
			if err := p.canRemoveFKBackreference(ctx, uniqueConstraint.GetName(), fk, behavior); err != nil {
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

// checkSchemaChangeIsAllowed checks if a schema change is allowed on
// this table. A schema change is disallowed if one of the following is true:
//   - The schema_locked table storage parameter is true, and this statement is
//     not modifying the value of schema_locked.
//   - The table is referenced by logical data replication jobs, and the statement
//     is not in the allow list of LDR schema changes.
func checkSchemaChangeIsAllowed(desc catalog.TableDescriptor, n tree.Statement) (ret error) {
	if desc == nil {
		return nil
	}
	if desc.IsSchemaLocked() && !tree.IsSetOrResetSchemaLocked(n) {
		return sqlerrors.NewSchemaChangeOnLockedTableErr(desc.GetName())
	}
	if len(desc.TableDesc().LDRJobIDs) > 0 {
		var virtualColNames []string
		for _, col := range desc.NonDropColumns() {
			if col.IsVirtual() {
				virtualColNames = append(virtualColNames, col.GetName())
			}
		}
		if !tree.IsAllowedLDRSchemaChange(n, virtualColNames) {
			return sqlerrors.NewDisallowedSchemaChangeOnLDRTableErr(desc.GetName(), desc.TableDesc().LDRJobIDs)

		}
	}
	return nil
}
