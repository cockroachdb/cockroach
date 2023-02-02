// Copyright 2019 The Cockroach Authors.
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
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type opaqueMetadata struct {
	info    string
	plan    planNode
	columns colinfo.ResultColumns
}

var _ opt.OpaqueMetadata = &opaqueMetadata{}

func (o *opaqueMetadata) ImplementsOpaqueMetadata()      {}
func (o *opaqueMetadata) String() string                 { return o.info }
func (o *opaqueMetadata) Columns() colinfo.ResultColumns { return o.columns }

func buildOpaque(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, stmt tree.Statement,
) (opt.OpaqueMetadata, error) {
	p := evalCtx.Planner.(*planner)

	// Opaque statements handle their own scalar arguments, with no help from the
	// optimizer. As such, they cannot contain subqueries.
	scalarProps := &semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	scalarProps.Require(stmt.StatementTag(), tree.RejectSubqueries)

	var plan planNode
	if tree.CanModifySchema(stmt) {
		if err := p.checkNoConflictingCursors(stmt); err != nil {
			return nil, err
		}
		var err error
		plan, err = p.SchemaChange(ctx, stmt)
		if err != nil {
			return nil, err
		}
	}
	if plan == nil {
		var err error
		plan, err = planOpaque(ctx, p, stmt)
		if err != nil {
			return nil, err
		}
	}
	if plan == nil {
		return nil, errors.AssertionFailedf("planNode cannot be nil for %T", stmt)
	}
	res := &opaqueMetadata{
		info:    stmt.StatementTag(),
		plan:    plan,
		columns: planColumns(plan),
	}
	return res, nil
}

func planOpaque(ctx context.Context, p *planner, stmt tree.Statement) (planNode, error) {
	switch n := stmt.(type) {
	case *tree.AlterDatabaseOwner:
		return p.AlterDatabaseOwner(ctx, n)
	case *tree.AlterDatabaseAddRegion:
		return p.AlterDatabaseAddRegion(ctx, n)
	case *tree.AlterDatabaseDropRegion:
		return p.AlterDatabaseDropRegion(ctx, n)
	case *tree.AlterDatabasePrimaryRegion:
		return p.AlterDatabasePrimaryRegion(ctx, n)
	case *tree.AlterDatabasePlacement:
		return p.AlterDatabasePlacement(ctx, n)
	case *tree.AlterDatabaseSurvivalGoal:
		return p.AlterDatabaseSurvivalGoal(ctx, n)
	case *tree.AlterDatabaseAddSuperRegion:
		return p.AlterDatabaseAddSuperRegion(ctx, n)
	case *tree.AlterDatabaseDropSuperRegion:
		return p.AlterDatabaseDropSuperRegion(ctx, n)
	case *tree.AlterDatabaseAlterSuperRegion:
		return p.AlterDatabaseAlterSuperRegion(ctx, n)
	case *tree.AlterDatabaseSecondaryRegion:
		return p.AlterDatabaseSecondaryRegion(ctx, n)
	case *tree.AlterDatabaseDropSecondaryRegion:
		return p.AlterDatabaseDropSecondaryRegion(ctx, n)
	case *tree.AlterDatabaseSetZoneConfigExtension:
		return p.AlterDatabaseSetZoneConfigExtension(ctx, n)
	case *tree.AlterDefaultPrivileges:
		return p.alterDefaultPrivileges(ctx, n)
	case *tree.AlterFunctionOptions:
		return p.AlterFunctionOptions(ctx, n)
	case *tree.AlterFunctionRename:
		return p.AlterFunctionRename(ctx, n)
	case *tree.AlterFunctionSetOwner:
		return p.AlterFunctionSetOwner(ctx, n)
	case *tree.AlterFunctionSetSchema:
		return p.AlterFunctionSetSchema(ctx, n)
	case *tree.AlterFunctionDepExtension:
		return p.AlterFunctionDepExtension(ctx, n)
	case *tree.AlterIndex:
		return p.AlterIndex(ctx, n)
	case *tree.AlterIndexVisible:
		return p.AlterIndexVisible(ctx, n)
	case *tree.AlterSchema:
		return p.AlterSchema(ctx, n)
	case *tree.AlterTable:
		return p.AlterTable(ctx, n)
	case *tree.AlterTableLocality:
		return p.AlterTableLocality(ctx, n)
	case *tree.AlterTableOwner:
		return p.AlterTableOwner(ctx, n)
	case *tree.AlterTableSetSchema:
		return p.AlterTableSetSchema(ctx, n)
	case *tree.AlterTenantCapability:
		return p.AlterTenantCapability(ctx, n)
	case *tree.AlterTenantSetClusterSetting:
		return p.AlterTenantSetClusterSetting(ctx, n)
	case *tree.AlterTenantRename:
		return p.alterRenameTenant(ctx, n)
	case *tree.AlterTenantService:
		return p.alterTenantService(ctx, n)
	case *tree.AlterType:
		return p.AlterType(ctx, n)
	case *tree.AlterRole:
		return p.AlterRole(ctx, n)
	case *tree.AlterRoleSet:
		return p.AlterRoleSet(ctx, n)
	case *tree.AlterSequence:
		return p.AlterSequence(ctx, n)
	case *tree.CloseCursor:
		return p.CloseCursor(ctx, n)
	case *tree.CommentOnColumn:
		return p.CommentOnColumn(ctx, n)
	case *tree.CommentOnConstraint:
		return p.CommentOnConstraint(ctx, n)
	case *tree.CommentOnDatabase:
		return p.CommentOnDatabase(ctx, n)
	case *tree.CommentOnSchema:
		return p.CommentOnSchema(ctx, n)
	case *tree.CommentOnIndex:
		return p.CommentOnIndex(ctx, n)
	case *tree.CommentOnTable:
		return p.CommentOnTable(ctx, n)
	case *tree.CreateDatabase:
		return p.CreateDatabase(ctx, n)
	case *tree.CreateIndex:
		return p.CreateIndex(ctx, n)
	case *tree.CreateSchema:
		return p.CreateSchema(ctx, n)
	case *tree.CreateType:
		return p.CreateType(ctx, n)
	case *tree.CreateRole:
		return p.CreateRole(ctx, n)
	case *tree.CreateSequence:
		return p.CreateSequence(ctx, n)
	case *tree.CreateExtension:
		return p.CreateExtension(ctx, n)
	case *tree.CreateExternalConnection:
		return p.CreateExternalConnection(ctx, n)
	case *tree.CreateTenant:
		return p.CreateTenantNode(ctx, n)
	case *tree.DropExternalConnection:
		return p.DropExternalConnection(ctx, n)
	case *tree.Deallocate:
		return p.Deallocate(ctx, n)
	case *tree.DeclareCursor:
		return p.DeclareCursor(ctx, n)
	case *tree.Discard:
		return p.Discard(ctx, n)
	case *tree.DropDatabase:
		return p.DropDatabase(ctx, n)
	case *tree.DropFunction:
		return p.DropFunction(ctx, n)
	case *tree.DropIndex:
		return p.DropIndex(ctx, n)
	case *tree.DropOwnedBy:
		return p.DropOwnedBy(ctx)
	case *tree.DropRole:
		return p.DropRole(ctx, n)
	case *tree.DropSchema:
		return p.DropSchema(ctx, n)
	case *tree.DropSequence:
		return p.DropSequence(ctx, n)
	case *tree.DropTable:
		return p.DropTable(ctx, n)
	case *tree.DropTenant:
		return p.DropTenant(ctx, n)
	case *tree.DropType:
		return p.DropType(ctx, n)
	case *tree.DropView:
		return p.DropView(ctx, n)
	case *tree.FetchCursor:
		return p.FetchCursor(ctx, &n.CursorStmt, false /* isMove */)
	case *tree.Grant:
		return p.Grant(ctx, n)
	case *tree.GrantRole:
		return p.GrantRole(ctx, n)
	case *tree.MoveCursor:
		return p.FetchCursor(ctx, &n.CursorStmt, true /* isMove */)
	case *tree.ReassignOwnedBy:
		return p.ReassignOwnedBy(ctx, n)
	case *tree.RefreshMaterializedView:
		return p.RefreshMaterializedView(ctx, n)
	case *tree.RenameColumn:
		return p.RenameColumn(ctx, n)
	case *tree.RenameDatabase:
		return p.RenameDatabase(ctx, n)
	case *tree.ReparentDatabase:
		return p.ReparentDatabase(ctx, n)
	case *tree.RenameIndex:
		return p.RenameIndex(ctx, n)
	case *tree.RenameTable:
		return p.RenameTable(ctx, n)
	case *tree.Revoke:
		return p.Revoke(ctx, n)
	case *tree.RevokeRole:
		return p.RevokeRole(ctx, n)
	case *tree.Scatter:
		return p.Scatter(ctx, n)
	case *tree.Scrub:
		return p.Scrub(ctx, n)
	case *tree.SetClusterSetting:
		return p.SetClusterSetting(ctx, n)
	case *tree.SetZoneConfig:
		return p.SetZoneConfig(ctx, n)
	case *tree.SetVar:
		return p.SetVar(ctx, n)
	case *tree.SetTransaction:
		return p.SetTransaction(ctx, n)
	case *tree.SetSessionAuthorizationDefault:
		return p.SetSessionAuthorizationDefault()
	case *tree.SetSessionCharacteristics:
		return p.SetSessionCharacteristics(ctx, n)
	case *tree.ShowClusterSetting:
		return p.ShowClusterSetting(ctx, n)
	case *tree.ShowTenantClusterSetting:
		return p.ShowTenantClusterSetting(ctx, n)
	case *tree.ShowCreateSchedules:
		return p.ShowCreateSchedule(ctx, n)
	case *tree.ShowCreateExternalConnections:
		return p.ShowCreateExternalConnection(ctx, n)
	case *tree.ShowHistogram:
		return p.ShowHistogram(ctx, n)
	case *tree.ShowTableStats:
		return p.ShowTableStats(ctx, n)
	case *tree.ShowTenant:
		return p.ShowTenant(ctx, n)
	case *tree.ShowTraceForSession:
		return p.ShowTrace(ctx, n)
	case *tree.ShowVar:
		return p.ShowVar(ctx, n)
	case *tree.ShowZoneConfig:
		return p.ShowZoneConfig(ctx, n)
	case *tree.ShowFingerprints:
		return p.ShowFingerprints(ctx, n)
	case *tree.ShowTransactionStatus:
		return p.ShowVar(ctx, &tree.ShowVar{Name: "transaction_status"})
	case *tree.Truncate:
		return p.Truncate(ctx, n)
	case *tree.Unlisten:
		return p.Unlisten(ctx, n)
	case tree.CCLOnlyStatement:
		plan, err := p.maybePlanHook(ctx, stmt)
		if plan == nil && err == nil {
			return nil, pgerror.Newf(pgcode.CCLRequired,
				"a CCL binary is required to use this statement type: %T", stmt)
		}
		return plan, err
	default:
		return nil, errors.AssertionFailedf("unknown opaque statement %T", stmt)
	}
}

func init() {
	for _, stmt := range []tree.Statement{
		&tree.AlterChangefeed{},
		&tree.AlterDatabaseAddRegion{},
		&tree.AlterDatabaseDropRegion{},
		&tree.AlterDatabaseOwner{},
		&tree.AlterDatabasePrimaryRegion{},
		&tree.AlterDatabasePlacement{},
		&tree.AlterDatabaseSurvivalGoal{},
		&tree.AlterDatabaseAddSuperRegion{},
		&tree.AlterDatabaseDropSuperRegion{},
		&tree.AlterDatabaseAlterSuperRegion{},
		&tree.AlterDatabaseSecondaryRegion{},
		&tree.AlterDatabaseDropSecondaryRegion{},
		&tree.AlterDatabaseSetZoneConfigExtension{},
		&tree.AlterDefaultPrivileges{},
		&tree.AlterFunctionOptions{},
		&tree.AlterFunctionRename{},
		&tree.AlterFunctionSetOwner{},
		&tree.AlterFunctionSetSchema{},
		&tree.AlterFunctionDepExtension{},
		&tree.AlterIndex{},
		&tree.AlterIndexVisible{},
		&tree.AlterSchema{},
		&tree.AlterTable{},
		&tree.AlterTableLocality{},
		&tree.AlterTableOwner{},
		&tree.AlterTableSetSchema{},
		&tree.AlterTenantCapability{},
		&tree.AlterTenantRename{},
		&tree.AlterTenantSetClusterSetting{},
		&tree.AlterTenantService{},
		&tree.AlterType{},
		&tree.AlterSequence{},
		&tree.AlterRole{},
		&tree.AlterRoleSet{},
		&tree.CloseCursor{},
		&tree.CommentOnColumn{},
		&tree.CommentOnDatabase{},
		&tree.CommentOnSchema{},
		&tree.CommentOnIndex{},
		&tree.CommentOnConstraint{},
		&tree.CommentOnTable{},
		&tree.CreateDatabase{},
		&tree.CreateExtension{},
		&tree.CreateExternalConnection{},
		&tree.CreateTenant{},
		&tree.CreateIndex{},
		&tree.CreateSchema{},
		&tree.CreateSequence{},
		&tree.CreateType{},
		&tree.CreateRole{},
		&tree.Deallocate{},
		&tree.DeclareCursor{},
		&tree.Discard{},
		&tree.DropDatabase{},
		&tree.DropExternalConnection{},
		&tree.DropFunction{},
		&tree.DropIndex{},
		&tree.DropOwnedBy{},
		&tree.DropRole{},
		&tree.DropSchema{},
		&tree.DropSequence{},
		&tree.DropTable{},
		&tree.DropTenant{},
		&tree.DropType{},
		&tree.DropView{},
		&tree.FetchCursor{},
		&tree.Grant{},
		&tree.GrantRole{},
		&tree.MoveCursor{},
		&tree.ReassignOwnedBy{},
		&tree.RefreshMaterializedView{},
		&tree.RenameColumn{},
		&tree.RenameDatabase{},
		&tree.RenameIndex{},
		&tree.RenameTable{},
		&tree.ReparentDatabase{},
		&tree.Revoke{},
		&tree.RevokeRole{},
		&tree.Scatter{},
		&tree.Scrub{},
		&tree.SetClusterSetting{},
		&tree.SetZoneConfig{},
		&tree.SetVar{},
		&tree.SetTransaction{},
		&tree.SetSessionAuthorizationDefault{},
		&tree.SetSessionCharacteristics{},
		&tree.ShowClusterSetting{},
		&tree.ShowTenantClusterSetting{},
		&tree.ShowCreateSchedules{},
		&tree.ShowCreateExternalConnections{},
		&tree.ShowHistogram{},
		&tree.ShowTableStats{},
		&tree.ShowTenant{},
		&tree.ShowTraceForSession{},
		&tree.ShowZoneConfig{},
		&tree.ShowFingerprints{},
		&tree.ShowVar{},
		&tree.ShowTransactionStatus{},
		&tree.Truncate{},
		&tree.Unlisten{},

		// CCL statements (without Export which has an optimizer operator).
		&tree.AlterBackup{},
		&tree.AlterBackupSchedule{},
		&tree.AlterTenantReplication{},
		&tree.Backup{},
		&tree.ShowBackup{},
		&tree.Restore{},
		&tree.CreateChangefeed{},
		&tree.ScheduledChangefeed{},
		&tree.Import{},
		&tree.ScheduledBackup{},
		&tree.CreateTenantFromReplication{},
	} {
		typ := optbuilder.OpaqueReadOnly
		if tree.CanModifySchema(stmt) {
			typ = optbuilder.OpaqueDDL
		} else if tree.CanWriteData(stmt) {
			typ = optbuilder.OpaqueMutation
		}
		optbuilder.RegisterOpaque(reflect.TypeOf(stmt), typ, buildOpaque)
	}
}
