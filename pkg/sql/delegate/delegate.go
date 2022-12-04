// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// Certain statements (most SHOW variants) are just syntactic sugar for a more
// complicated underlying query.
//
// This package contains the logic to convert the AST of such a statement to the
// AST of the equivalent query to be planned.

// TryDelegate takes a statement and checks if it is one of the statements that
// can be rewritten as a lower level query. If it can, returns a new AST which
// is equivalent to the original statement. Otherwise, returns nil.
func TryDelegate(
	ctx context.Context, catalog cat.Catalog, evalCtx *eval.Context, stmt tree.Statement,
) (tree.Statement, error) {
	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}
	switch t := stmt.(type) {
	case *tree.ShowClusterSettingList:
		return d.delegateShowClusterSettingList(t)

	case *tree.ShowTenantClusterSettingList:
		return d.delegateShowTenantClusterSettingList(t)

	case *tree.ShowDatabases:
		return d.delegateShowDatabases(t)

	case *tree.ShowEnums:
		return d.delegateShowEnums(t)

	case *tree.ShowTypes:
		return d.delegateShowTypes()

	case *tree.ShowCreate:
		return d.delegateShowCreate(t)

	case *tree.ShowCreateFunction:
		return d.delegateShowCreateFunction(t)

	case *tree.ShowCreateAllSchemas:
		return d.delegateShowCreateAllSchemas()

	case *tree.ShowCreateAllTables:
		return d.delegateShowCreateAllTables()

	case *tree.ShowCreateAllTypes:
		return d.delegateShowCreateAllTypes()

	case *tree.ShowDatabaseIndexes:
		return d.delegateShowDatabaseIndexes(t)

	case *tree.ShowIndexes:
		return d.delegateShowIndexes(t)

	case *tree.ShowColumns:
		return d.delegateShowColumns(t)

	case *tree.ShowConstraints:
		return d.delegateShowConstraints(t)

	case *tree.ShowPartitions:
		return d.delegateShowPartitions(t)

	case *tree.ShowGrants:
		return d.delegateShowGrants(t)

	case *tree.ShowJobs:
		return d.delegateShowJobs(t)

	case *tree.ShowChangefeedJobs:
		return d.delegateShowChangefeedJobs(t)

	case *tree.ShowQueries:
		return d.delegateShowQueries(t)

	case *tree.ShowRanges:
		return d.delegateShowRanges(t)

	case *tree.ShowRangeForRow:
		return d.delegateShowRangeForRow(t)

	case *tree.ShowSurvivalGoal:
		return d.delegateShowSurvivalGoal(t)

	case *tree.ShowRegions:
		return d.delegateShowRegions(t)

	case *tree.ShowRoleGrants:
		return d.delegateShowRoleGrants(t)

	case *tree.ShowRoles:
		return d.delegateShowRoles()

	case *tree.ShowSchemas:
		return d.delegateShowSchemas(t)

	case *tree.ShowSequences:
		return d.delegateShowSequences(t)

	case *tree.ShowSessions:
		return d.delegateShowSessions(t)

	case *tree.ShowSyntax:
		return d.delegateShowSyntax(t)

	case *tree.ShowFunctions:
		return d.delegateShowFunctions(t)

	case *tree.ShowTables:
		return d.delegateShowTables(t)

	case *tree.ShowTransactions:
		return d.delegateShowTransactions(t)

	case *tree.ShowUsers:
		return d.delegateShowRoles()

	case *tree.ShowVar:
		return d.delegateShowVar(t)

	case *tree.ShowZoneConfig:
		return d.delegateShowZoneConfig(t)

	case *tree.ShowSchedules:
		return d.delegateShowSchedules(t)

	case *tree.ControlJobsForSchedules:
		return d.delegateJobControl(ControlJobsDelegate{
			Schedules: t.Schedules,
			Command:   t.Command,
		})

	case *tree.ControlJobsOfType:
		return d.delegateJobControl(ControlJobsDelegate{
			Type:    t.Type,
			Command: t.Command,
		})

	case *tree.ShowFullTableScans:
		return d.delegateShowFullTableScans()

	case *tree.ShowDefaultPrivileges:
		return d.delegateShowDefaultPrivileges(t)

	case *tree.ShowLastQueryStatistics:
		return nil, unimplemented.New(
			"show last query statistics",
			"cannot use SHOW LAST QUERY STATISTICS as a statement source",
		)

	case *tree.ShowSavepointStatus:
		return nil, unimplemented.NewWithIssue(
			47333, "cannot use SHOW SAVEPOINT STATUS as a statement source")

	// SHOW TRANSFER STATE cannot be rewritten as a low-level query due to
	// the format of its output (e.g. transfer key echoed back, and errors are
	// returned in the form of a SQL value).
	case *tree.ShowTransferState:
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot use SHOW TRANSFER STATE as a statement source")

	default:
		return nil, nil
	}
}

type delegator struct {
	ctx     context.Context
	catalog cat.Catalog
	evalCtx *eval.Context
}

func parse(sql string) (tree.Statement, error) {
	s, err := parser.ParseOne(sql)
	return s.AST, err
}
