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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

type opaqueMetadata struct {
	info string
	plan planNode
}

var _ opt.OpaqueMetadata = &opaqueMetadata{}

func (o *opaqueMetadata) ImplementsOpaqueMetadata() {}
func (o *opaqueMetadata) String() string            { return o.info }

func buildOpaque(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, stmt tree.Statement,
) (opt.OpaqueMetadata, sqlbase.ResultColumns, error) {
	p := evalCtx.Planner.(*planner)

	// Opaque statements handle their own scalar arguments, with no help from the
	// optimizer. As such, they cannot contain subqueries.
	scalarProps := &semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	scalarProps.Require(stmt.StatementTag(), tree.RejectSubqueries)

	var plan planNode
	var err error
	switch n := stmt.(type) {
	case *tree.AlterIndex:
		plan, err = p.AlterIndex(ctx, n)
	case *tree.AlterTable:
		plan, err = p.AlterTable(ctx, n)
	case *tree.AlterType:
		plan, err = p.AlterType(ctx, n)
	case *tree.AlterRole:
		plan, err = p.AlterRole(ctx, n)
	case *tree.AlterSequence:
		plan, err = p.AlterSequence(ctx, n)
	case *tree.Analyze:
		plan, err = p.Analyze(ctx, n)
	case *tree.CommentOnColumn:
		plan, err = p.CommentOnColumn(ctx, n)
	case *tree.CommentOnDatabase:
		plan, err = p.CommentOnDatabase(ctx, n)
	case *tree.CommentOnIndex:
		plan, err = p.CommentOnIndex(ctx, n)
	case *tree.CommentOnTable:
		plan, err = p.CommentOnTable(ctx, n)
	case *tree.CreateDatabase:
		plan, err = p.CreateDatabase(ctx, n)
	case *tree.CreateIndex:
		plan, err = p.CreateIndex(ctx, n)
	case *tree.CreateSchema:
		plan, err = p.CreateSchema(ctx, n)
	case *tree.CreateType:
		plan, err = p.CreateType(ctx, n)
	case *tree.CreateRole:
		plan, err = p.CreateRole(ctx, n)
	case *tree.CreateSequence:
		plan, err = p.CreateSequence(ctx, n)
	case *tree.CreateStats:
		plan, err = p.CreateStatistics(ctx, n)
	case *tree.Deallocate:
		plan, err = p.Deallocate(ctx, n)
	case *tree.Discard:
		plan, err = p.Discard(ctx, n)
	case *tree.DropDatabase:
		plan, err = p.DropDatabase(ctx, n)
	case *tree.DropIndex:
		plan, err = p.DropIndex(ctx, n)
	case *tree.DropRole:
		plan, err = p.DropRole(ctx, n)
	case *tree.DropTable:
		plan, err = p.DropTable(ctx, n)
	case *tree.DropType:
		plan, err = p.DropType(ctx, n)
	case *tree.DropView:
		plan, err = p.DropView(ctx, n)
	case *tree.DropSequence:
		plan, err = p.DropSequence(ctx, n)
	case *tree.Grant:
		plan, err = p.Grant(ctx, n)
	case *tree.GrantRole:
		plan, err = p.GrantRole(ctx, n)
	case *tree.RenameColumn:
		plan, err = p.RenameColumn(ctx, n)
	case *tree.RenameDatabase:
		plan, err = p.RenameDatabase(ctx, n)
	case *tree.RenameIndex:
		plan, err = p.RenameIndex(ctx, n)
	case *tree.RenameTable:
		plan, err = p.RenameTable(ctx, n)
	case *tree.Revoke:
		plan, err = p.Revoke(ctx, n)
	case *tree.RevokeRole:
		plan, err = p.RevokeRole(ctx, n)
	case *tree.Scatter:
		plan, err = p.Scatter(ctx, n)
	case *tree.Scrub:
		plan, err = p.Scrub(ctx, n)
	case *tree.SetClusterSetting:
		plan, err = p.SetClusterSetting(ctx, n)
	case *tree.SetZoneConfig:
		plan, err = p.SetZoneConfig(ctx, n)
	case *tree.SetVar:
		plan, err = p.SetVar(ctx, n)
	case *tree.SetTransaction:
		plan, err = p.SetTransaction(ctx, n)
	case *tree.SetSessionAuthorizationDefault:
		plan, err = p.SetSessionAuthorizationDefault()
	case *tree.SetSessionCharacteristics:
		plan, err = p.SetSessionCharacteristics(n)
	case *tree.ShowClusterSetting:
		plan, err = p.ShowClusterSetting(ctx, n)
	case *tree.ShowHistogram:
		plan, err = p.ShowHistogram(ctx, n)
	case *tree.ShowTableStats:
		plan, err = p.ShowTableStats(ctx, n)
	case *tree.ShowTraceForSession:
		plan, err = p.ShowTrace(ctx, n)
	case *tree.ShowZoneConfig:
		plan, err = p.ShowZoneConfig(ctx, n)
	case *tree.ShowFingerprints:
		plan, err = p.ShowFingerprints(ctx, n)
	case *tree.Truncate:
		plan, err = p.Truncate(ctx, n)
	case tree.CCLOnlyStatement:
		plan, err = p.maybePlanHook(ctx, stmt)
		if plan == nil && err == nil {
			return nil, nil, pgerror.Newf(pgcode.CCLRequired,
				"a CCL binary is required to use this statement type: %T", stmt)
		}
	default:
		return nil, nil, errors.AssertionFailedf("unknown opaque statement %T", stmt)
	}
	if err != nil {
		return nil, nil, err
	}
	res := &opaqueMetadata{
		info: stmt.StatementTag(),
		plan: plan,
	}
	return res, planColumns(plan), nil
}

func init() {
	for _, stmt := range []tree.Statement{
		&tree.AlterIndex{},
		&tree.AlterTable{},
		&tree.AlterType{},
		&tree.AlterSequence{},
		&tree.AlterRole{},
		&tree.Analyze{},
		&tree.CommentOnColumn{},
		&tree.CommentOnDatabase{},
		&tree.CommentOnIndex{},
		&tree.CommentOnTable{},
		&tree.CreateDatabase{},
		&tree.CreateIndex{},
		&tree.CreateSchema{},
		&tree.CreateSequence{},
		&tree.CreateStats{},
		&tree.CreateType{},
		&tree.CreateRole{},
		&tree.Deallocate{},
		&tree.Discard{},
		&tree.DropDatabase{},
		&tree.DropIndex{},
		&tree.DropTable{},
		&tree.DropType{},
		&tree.DropView{},
		&tree.DropRole{},
		&tree.DropSequence{},
		&tree.Grant{},
		&tree.GrantRole{},
		&tree.RenameColumn{},
		&tree.RenameDatabase{},
		&tree.RenameIndex{},
		&tree.RenameTable{},
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
		&tree.ShowHistogram{},
		&tree.ShowTableStats{},
		&tree.ShowTraceForSession{},
		&tree.ShowZoneConfig{},
		&tree.ShowFingerprints{},
		&tree.Truncate{},

		// CCL statements (without Export which has an optimizer operator).
		&tree.Backup{},
		&tree.ShowBackup{},
		&tree.Restore{},
		&tree.CreateChangefeed{},
		&tree.Import{},
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
