// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/issuelink"
)

func init() {
	sql.AddPlanHook("create logical replication stream", createLogicalReplicationStreamPlanHook, createLogicalReplicationStreamTypeCheck)
}

func createLogicalReplicationStreamPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	if _, ok := stmt.(*tree.CreateLogicalReplicationStream); !ok {
		return nil, nil, nil, false, nil
	}
	return nil, nil, nil, false, errors.UnimplementedError(issuelink.IssueLink{Detail: "plan hook to be implemented"}, "plan hook to be implemented")
}

func createLogicalReplicationStreamTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	return false, nil, nil
}
