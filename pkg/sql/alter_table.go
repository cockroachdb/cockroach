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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
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

				if err := validateColumnsAreAccessible(n.tableDesc, d.Columns); err != nil {
					return err
				}

				tableName, err := params.p.getQualifiedTableName(params.ctx, n.tableDesc)
				if err != nil {
					return err
				}

				if err := replaceExpressionElemsWithVirtualCols(
					params.ctx,
					n.tableDesc,
					tableName,
					d.Columns,
					false, /* isInverted */
					false, /* isNewTable */
					params.p.SemaCtx(),
				); err != nil {
					return err
				}

				// Check if the columns exist on the table.
				for _, column := range d.Columns {
					if column.Expr != nil {
						return pgerror.New(
							pgcode.InvalidTableDefinition,
							"cannot create a unique constraint on an expression, use UNIQUE INDEX instead",
						)
					}
					_, err := n.tableDesc.FindColumnWithName(column.Column)
					if err != nil {
						return err
					}
				}
				idx := descpb.IndexDescriptor{
					Name:             string(d.Name),
					Unique:           true,
					StoreColumnNames: d.Storing.ToStrings(),
					CreatedAtNanos:   params.EvalContext().GetTxnTimestamp(time.Microsecond).UnixNano(),
				}
				if err := idx.FillColumns(d.Columns); err != nil {
					return err
				}

				if d.Predicate != nil {
					expr, err := schemaexpr.ValidatePartialIndexPredicate(
						params.ctx, n.tableDesc, d.Predicate, tableName, params.p.SemaCtx(),
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
				foundIndex, err := n.tableDesc.FindIndexWithName(string(d.Name))
				if err == nil {
					if foundIndex.Dropped() {
						return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
							"index %q being dropped, try again later", d.Name)
					}
				}
				if err := n.tableDesc.AddIndexMutation(params.ctx, &idx, descpb.DescriptorMutation_ADD, params.p.ExecCfg().Settings); err != nil {
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
				// We want to reject uses of FK ON UPDATE actions where there is already
				// an ON UPDATE expression for the column.
				if d.Actions.Update != tree.NoAction && d.Actions.Update != tree.Restrict {
					for _, fromCol := range d.FromCols {
						for _, toCheck := range n.tableDesc.Columns {
							if fromCol == toCheck.ColName() && toCheck.HasOnUpdate() {
								return pgerror.Newf(
									pgcode.InvalidTableDefinition,
									"cannot specify a foreign key update action and an ON UPDATE"+
										" expression on the same column",
								)
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

			if t.Column == colinfo.TTLDefaultExpirationColumnName && n.tableDesc.HasRowLevelTTL() {
				return errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidTableDefinition,
						`cannot drop column %s while row-level TTL is active`,
						t.Column,
					),
					"use ALTER TABLE %s RESET (ttl) instead",
					tree.Name(n.tableDesc.GetName()),
				)
			}

			colDroppedViews, err := dropColumnImpl(params, tn, n.tableDesc, t)
			if err != nil {
				return err
			}
			droppedViews = append(droppedViews, colDroppedViews...)
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
					params.ctx, &params.p.semaCtx, params.ExecCfg().InternalExecutorFactory,
					params.SessionData(), n.tableDesc, params.EvalContext().Txn, ck.Expr,
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
					params.ctx,
					params.ExecCfg().InternalExecutorFactory,
					params.p.SessionData(),
					n.tableDesc,
					params.EvalContext().Txn,
					params.p.Descriptors(),
					name,
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
						params.ctx, params.ExecCfg().InternalExecutorFactory(
							params.ctx, params.SessionData(),
						), n.tableDesc, params.EvalContext().Txn, name,
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

		case *tree.AlterTableSetStorageParams:
			var ttlBefore *catpb.RowLevelTTL
			if ttl := n.tableDesc.GetRowLevelTTL(); ttl != nil {
				ttlBefore = protoutil.Clone(ttl).(*catpb.RowLevelTTL)
			}
			if err := paramparse.SetStorageParameters(
				params.ctx,
				params.p.SemaCtx(),
				params.EvalContext(),
				t.StorageParams,
				paramparse.NewTableStorageParamObserver(n.tableDesc),
			); err != nil {
				return err
			}
			descriptorChanged = true

			if err := handleTTLStorageParamChange(
				params,
				tn,
				n.tableDesc,
				ttlBefore,
				n.tableDesc.GetRowLevelTTL(),
			); err != nil {
				return err
			}

		case *tree.AlterTableResetStorageParams:
			var ttlBefore *catpb.RowLevelTTL
			if ttl := n.tableDesc.GetRowLevelTTL(); ttl != nil {
				ttlBefore = protoutil.Clone(ttl).(*catpb.RowLevelTTL)
			}
			if err := paramparse.ResetStorageParameters(
				params.ctx,
				params.EvalContext(),
				t.Params,
				paramparse.NewTableStorageParamObserver(n.tableDesc),
			); err != nil {
				return err
			}
			descriptorChanged = true

			if err := handleTTLStorageParamChange(
				params,
				tn,
				n.tableDesc,
				ttlBefore,
				n.tableDesc.GetRowLevelTTL(),
			); err != nil {
				return err
			}

		case *tree.AlterTableRenameColumn:
			descChanged, err := params.p.renameColumn(params.ctx, n.tableDesc, t.Column, t.NewName)
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
		if err := updateNonComputedColExpr(
			params,
			tableDesc,
			col,
			t.Default,
			&col.ColumnDesc().DefaultExpr,
			"DEFAULT",
		); err != nil {
			return err
		}

	case *tree.AlterTableSetOnUpdate:
		// We want to reject uses of ON UPDATE where there is also a foreign key ON
		// UPDATE.
		for _, fk := range tableDesc.OutboundFKs {
			for _, colID := range fk.OriginColumnIDs {
				if colID == col.GetID() &&
					fk.OnUpdate != catpb.ForeignKeyAction_NO_ACTION &&
					fk.OnUpdate != catpb.ForeignKeyAction_RESTRICT {
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
			"ON UPDATE",
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

		info, err := tableDesc.GetConstraintInfo()
		if err != nil {
			return err
		}
		inuseNames := make(map[string]struct{}, len(info))
		for k := range info {
			inuseNames[k] = struct{}{}
		}
		check := tabledesc.MakeNotNullCheckConstraint(col.GetName(), col.GetID(), tableDesc.GetNextConstraintID(), inuseNames, descpb.ConstraintValidity_Validating)
		tableDesc.AddNotNullMutation(check, descpb.DescriptorMutation_ADD)
		tableDesc.NextConstraintID++

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
		check := tabledesc.MakeNotNullCheckConstraint(col.GetName(), col.GetID(), tableDesc.GetNextConstraintID(), inuseNames, descpb.ConstraintValidity_Dropping)
		tableDesc.Checks = append(tableDesc.Checks, check)
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
	op string,
) error {
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

	if col.IsGeneratedAsIdentity() {
		return sqlerrors.NewSyntaxErrorf("column %q is an identity column", col.GetName())
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

	if err := updateSequenceDependencies(params, tab, col); err != nil {
		return err
	}

	return nil
}

func sanitizeColumnExpression(
	p runParams, expr tree.Expr, col catalog.Column, opName string,
) (tree.TypedExpr, string, error) {
	colDatumType := col.GetType()
	typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
		p.ctx, expr, colDatumType, opName, &p.p.semaCtx, tree.VolatilityVolatile,
	)
	if err != nil {
		return nil, "", pgerror.WithCandidateCode(err, pgcode.DatatypeMismatch)
	}

	s := tree.Serialize(typedExpr)
	return typedExpr, s, nil
}

// updateSequenceDependencies checks for sequence dependencies on the provided
// DEFAULT and ON UPDATE expressions and adds any dependencies to the tableDesc.
func updateSequenceDependencies(
	params runParams, tableDesc *tabledesc.Mutable, colDesc catalog.Column,
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
		name   string
		exists func() bool
		get    func() string
	}{
		{
			name:   "DEFAULT",
			exists: colDesc.HasDefault,
			get:    colDesc.GetDefaultExpr,
		},
		{
			name:   "ON UPDATE",
			exists: colDesc.HasOnUpdate,
			get:    colDesc.GetOnUpdateExpr,
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
			"DEFAULT",
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
		ctx      = params.ctx
		ie       = params.ExecCfg().InternalExecutor
		txn      = params.EvalContext().Txn
		settings = params.ExecCfg().Settings
	)

	var name interface{}
	if s.Name != "" {
		name = s.Name
	}

	if !settings.Version.IsActive(params.ctx, clusterversion.AlterSystemTableStatisticsAddAvgSizeCol) {
		_ /* rows */, err := ie.Exec(
			ctx,
			"insert-stats",
			txn,
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
			tableID,
			name,
			columnIDs,
			s.CreatedAt,
			s.RowCount,
			s.DistinctCount,
			s.NullCount,
			histogram)
		return err
	}
	_ /* rows */, err := ie.Exec(
		ctx,
		"insert-stats",
		txn,
		`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"createdAt",
					"rowCount",
					"distinctCount",
					"nullCount",
					"avgSize",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		tableID,
		name,
		columnIDs,
		s.CreatedAt,
		s.RowCount,
		s.DistinctCount,
		s.NullCount,
		s.AvgSize,
		histogram)
	return err
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
				tableDesc.PrimaryIndex.GetName() == defaultPKName &&
				name == tree.Name(defaultPKName) {
				return false, nil
			}
			// If there is no active primary key, then adding one with the exact
			// same name is allowed.
			if !tableDesc.HasPrimaryKey() &&
				tableDesc.PrimaryIndex.Name == name.String() {
				return false, nil
			}
		}
		if name == "" {
			return false, nil
		}
		idx, _ := tableDesc.FindIndexWithName(string(name))
		// If an index is found and its disabled, then we know it will be dropped
		// later on.
		if idx == nil {
			return false, nil
		}
		if d.IfNotExists {
			return true, nil
		}
		if idx.Dropped() {
			return false, pgerror.Newf(pgcode.DuplicateObject, "constraint with name %q already exists and is being dropped, try again later", name)
		}
		return false, pgerror.Newf(pgcode.DuplicateObject, "constraint with name %q already exists", name)

	default:
		return false, errors.AssertionFailedf(
			"unsupported constraint: %T", cmd.ConstraintDef)
	}

	if name == "" {
		return false, nil
	}
	info, err := tableDesc.GetConstraintInfo()
	if err != nil {
		// Unexpected error: table descriptor should be valid at this point.
		return false, errors.WithAssertionFailure(err)
	}
	constraintInfo, isInUse := info[name.String()]
	if !isInUse {
		return false, nil
	}
	// If the primary index is being replaced, then the name can be reused for
	// another constraint.
	if isInUse &&
		constraintInfo.Index != nil &&
		constraintInfo.Index.ID == tableDesc.PrimaryIndex.ID {
		for _, mut := range tableDesc.GetMutations() {
			if primaryKeySwap := mut.GetPrimaryKeySwap(); primaryKeySwap != nil &&
				primaryKeySwap.OldPrimaryIndexId == tableDesc.PrimaryIndex.ID &&
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

func dropColumnImpl(
	params runParams, tn *tree.TableName, tableDesc *tabledesc.Mutable, t *tree.AlterTableDropColumn,
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

	colToDrop, err := tableDesc.FindColumnWithName(t.Column)
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

	if colToDrop.IsInaccessible() {
		return nil, pgerror.Newf(
			pgcode.InvalidColumnReference,
			"cannot drop inaccessible column %q",
			t.Column,
		)
	}

	// If the dropped column uses a sequence, remove references to it from that sequence.
	if colToDrop.NumUsesSequences() > 0 {
		if err := params.p.removeSequenceDependencies(params.ctx, tableDesc, colToDrop); err != nil {
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
		err := params.p.canRemoveDependentViewGeneric(
			params.ctx, "column", string(t.Column), tableDesc.ParentID, ref, t.DropBehavior,
		)
		if err != nil {
			return nil, err
		}
		viewDesc, err := params.p.getViewDescForCascade(
			params.ctx, "column", string(t.Column), tableDesc.ParentID, ref.ID, t.DropBehavior,
		)
		if err != nil {
			return nil, err
		}
		jobDesc := fmt.Sprintf("removing view %q dependent on column %q which is being dropped",
			viewDesc.Name, colToDrop.ColName())
		cascadedViews, err := params.p.removeDependentView(params.ctx, tableDesc, viewDesc, jobDesc)
		if err != nil {
			return nil, err
		}
		qualifiedView, err := params.p.getQualifiedTableName(params.ctx, viewDesc)
		if err != nil {
			return nil, err
		}

		droppedViews = append(droppedViews, cascadedViews...)
		droppedViews = append(droppedViews, qualifiedView.FQString())
	}

	// We cannot remove this column if there are computed columns that use it.
	if err := schemaexpr.ValidateColumnHasNoDependents(tableDesc, colToDrop); err != nil {
		return nil, err
	}

	if tableDesc.GetPrimaryIndex().CollectKeyColumnIDs().Contains(colToDrop.GetID()) {
		return nil, pgerror.Newf(pgcode.InvalidColumnReference,
			"column %q is referenced by the primary key", colToDrop.GetName())
	}
	var idxNamesToDelete []string
	for _, idx := range tableDesc.NonDropIndexes() {
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
				if tableDesc.GetPrimaryIndex().CollectKeyColumnIDs().Contains(id) {
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
				return nil, err
			}

			colIDs, err := schemaexpr.ExtractColumnIDs(tableDesc, expr)
			if err != nil {
				return nil, err
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

	// Drop unique constraints that reference the column.
	sliceIdx := 0
	for i := range tableDesc.UniqueWithoutIndexConstraints {
		constraint := &tableDesc.UniqueWithoutIndexConstraints[i]
		tableDesc.UniqueWithoutIndexConstraints[sliceIdx] = *constraint
		sliceIdx++
		if descpb.ColumnIDs(constraint.ColumnIDs).Contains(colToDrop.GetID()) {
			sliceIdx--

			// If this unique constraint is used on the referencing side of any FK
			// constraints, try to remove the references. Don't bother trying to find
			// an alternate index or constraint, since all possible matches will
			// be dropped when the column is dropped.
			if err := params.p.tryRemoveFKBackReferences(
				params.ctx, tableDesc, constraint, t.DropBehavior, nil,
			); err != nil {
				return nil, err
			}
		}
	}
	tableDesc.UniqueWithoutIndexConstraints = tableDesc.UniqueWithoutIndexConstraints[:sliceIdx]

	// Drop check constraints which reference the column.
	constraintsToDrop := make([]string, 0, len(tableDesc.Checks))
	constraintInfo, err := tableDesc.GetConstraintInfo()
	if err != nil {
		return nil, err
	}

	for _, check := range tableDesc.AllActiveAndInactiveChecks() {
		if used, err := tableDesc.CheckConstraintUsesColumn(check, colToDrop.GetID()); err != nil {
			return nil, err
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
		err := tableDesc.DropConstraint(params.ctx, constraintName, constraintInfo[constraintName],
			func(*tabledesc.Mutable, *descpb.ForeignKeyConstraint) error {
				return nil
			},
			params.extendedEvalCtx.Settings,
		)
		if err != nil {
			return nil, err
		}
	}

	if err := params.p.removeColumnComment(params.ctx, tableDesc.ID, colToDrop.GetID()); err != nil {
		return nil, err
	}

	// Since we are able to drop indexes used by foreign keys on the origin side,
	// the drop index codepaths aren't going to remove dependent FKs, so we
	// need to do that here.
	// We update the FK's slice in place here.
	sliceIdx = 0
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
		return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"column %q in the middle of being added, try again later", t.Column)
	}

	return droppedViews, validateDescriptor(params.ctx, params.p, tableDesc)
}

func handleTTLStorageParamChange(
	params runParams,
	tn *tree.TableName,
	tableDesc *tabledesc.Mutable,
	before, after *catpb.RowLevelTTL,
) error {
	switch {
	case before == nil && after == nil:
		// Do not have to do anything here.
	case before != nil && after != nil:
		// Update cron schedule if required.
		if before.DeletionCron != after.DeletionCron {
			env := JobSchedulerEnv(params.ExecCfg())
			s, err := jobs.LoadScheduledJob(
				params.ctx,
				env,
				after.ScheduleID,
				params.ExecCfg().InternalExecutor,
				params.p.txn,
			)
			if err != nil {
				return err
			}
			if err := s.SetSchedule(rowLevelTTLSchedule(after)); err != nil {
				return err
			}
			if err := s.Update(params.ctx, params.ExecCfg().InternalExecutor, params.p.txn); err != nil {
				return err
			}
		}
		// Update default expression on automated column if required.
		if before.DurationExpr != after.DurationExpr {
			col, err := tableDesc.FindColumnWithName(colinfo.TTLDefaultExpirationColumnName)
			if err != nil {
				return err
			}
			intervalExpr, err := parser.ParseExpr(string(after.DurationExpr))
			if err != nil {
				return errors.Wrapf(err, "unexpected expression for TTL duration")
			}
			newExpr := rowLevelTTLAutomaticColumnExpr(intervalExpr)

			if err := updateNonComputedColExpr(
				params,
				tableDesc,
				col,
				newExpr,
				&col.ColumnDesc().DefaultExpr,
				"TTL DEFAULT",
			); err != nil {
				return err
			}

			if err := updateNonComputedColExpr(
				params,
				tableDesc,
				col,
				newExpr,
				&col.ColumnDesc().OnUpdateExpr,
				"TTL UPDATE",
			); err != nil {
				return err
			}
		}
	case before == nil && after != nil:
		if err := checkTTLEnabledForCluster(params.ctx, params.p.ExecCfg().Settings); err != nil {
			return err
		}

		// Adding a TTL requires adding the automatic column and deferring the TTL
		// addition to after the column is successfully added.
		tableDesc.RowLevelTTL = nil
		if _, err := tableDesc.FindColumnWithName(colinfo.TTLDefaultExpirationColumnName); err == nil {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"cannot add TTL to table with the %s column already defined",
				colinfo.TTLDefaultExpirationColumnName,
			)
		}
		col, err := rowLevelTTLAutomaticColumnDef(after)
		if err != nil {
			return err
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
			return err
		}
		tableDesc.AddModifyRowLevelTTLMutation(
			&descpb.ModifyRowLevelTTL{RowLevelTTL: after},
			descpb.DescriptorMutation_ADD,
		)
		version := params.ExecCfg().Settings.Version.ActiveVersion(params.ctx)
		if err := tableDesc.AllocateIDs(params.ctx, version); err != nil {
			return err
		}
	case before != nil && after == nil:
		telemetry.Inc(sqltelemetry.RowLevelTTLDropped)

		// Keep the TTL from beforehand, but create the DROP COLUMN job and the
		// associated mutation.
		tableDesc.RowLevelTTL = before

		droppedViews, err := dropColumnImpl(params, tn, tableDesc, &tree.AlterTableDropColumn{
			Column: colinfo.TTLDefaultExpirationColumnName,
		})
		if err != nil {
			return err
		}
		// This should never happen as we do not CASCADE, but error again just in case.
		if len(droppedViews) > 0 {
			return pgerror.Newf(pgcode.InvalidParameterValue, "cannot drop TTL automatic column if it is depended on by a view")
		}

		tableDesc.AddModifyRowLevelTTLMutation(
			&descpb.ModifyRowLevelTTL{RowLevelTTL: before},
			descpb.DescriptorMutation_DROP,
		)
	}

	return nil
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
