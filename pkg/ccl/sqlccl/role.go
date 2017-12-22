// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlccl

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func createRolePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	createRole, ok := stmt.(*tree.CreateRole)
	if !ok {
		return nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "CREATE ROLE",
	); err != nil {
		return nil, err
	}

	// Call directly into the OSS code.
	return p.CreateUserNode(ctx, createRole.Name, nil /* password */, createRole.IfNotExists, true /* isRole */, "CREATE ROLE")
}

func dropRolePlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanNode, error) {
	dropRole, ok := stmt.(*tree.DropRole)
	if !ok {
		return nil, nil
	}

	if err := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "DROP ROLE",
	); err != nil {
		return nil, err
	}

	// Call directly into the OSS code.
	return p.DropUserNode(ctx, dropRole.Names, dropRole.IfExists, true /* isRole */, "DROP ROLE")
}

func init() {
	sql.AddWrappedPlanHook(createRolePlanHook)
	sql.AddWrappedPlanHook(dropRolePlanHook)
}
