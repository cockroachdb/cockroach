// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func inspectTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	_, ok := stmt.(*tree.Inspect)
	if !ok {
		return false, nil, nil
	}

	return true, jobs.InspectJobExecutionResultHeader, nil
}

// inspectRun represents the runtime state of an execution of INSPECT.
type inspectRun struct {
	table  catalog.TableDescriptor
	schema catalog.SchemaDescriptor
	db     catalog.DatabaseDescriptor

	checks        []*jobspb.InspectDetails_Check
	asOfTimestamp hlc.Timestamp
}

func newInspectRun(
	ctx context.Context, stmt *tree.Inspect, p sql.PlanHookState,
) (inspectRun, error) {
	var run inspectRun

	switch stmt.Typ {
	case tree.InspectTable:
		if table, err := p.ResolveExistingObjectEx(ctx, stmt.Table.ToUnresolvedObjectName(), true /* required */, tree.ResolveRequireTableDesc); err != nil {
			return inspectRun{}, err
		} else {
			run.table = table
		}

		if schema, err := p.Descriptors().ByIDWithLeased(p.Txn()).Get().Schema(ctx, run.table.GetParentSchemaID()); err != nil {
			return inspectRun{}, err
		} else {
			run.schema = schema
		}

		if db, err := p.Descriptors().ByIDWithLeased(p.Txn()).Get().Database(ctx, run.table.GetParentID()); err != nil {
			return inspectRun{}, err
		} else {
			run.db = db
		}
	case tree.InspectDatabase:
		if db, err := p.Descriptors().ByNameWithLeased(p.Txn()).Get().Database(ctx, stmt.Database.ToUnresolvedName().String()); err != nil {
			return inspectRun{}, err
		} else {
			run.db = db
		}
	default:
		return inspectRun{}, errors.AssertionFailedf("unexpected INSPECT type received, got: %v", stmt.Typ)
	}

	if len(stmt.Options) == 0 || stmt.Options.HasIndexAll() {
		// No options or INDEX ALL specified - inspect all indexes.
		switch stmt.Typ {
		case tree.InspectTable:
			checks, err := sql.InspectChecksForTable(ctx, p, run.table)
			if err != nil {
				return inspectRun{}, err
			}
			run.checks = checks
		case tree.InspectDatabase:
			if checks, err := sql.InspectChecksForDatabase(ctx, p, run.db); err != nil {
				return inspectRun{}, err
			} else {
				run.checks = checks
			}
		default:
			return inspectRun{}, errors.AssertionFailedf("unexpected INSPECT type received, got: %v", stmt.Typ)
		}
	} else {
		// Named indexes specified.

		// Use the resolved database. Schema isn't changed.
		prevDatabase := p.SessionData().Database
		p.SessionDataMutatorIterator().SetDatabase(run.db.GetName())
		defer func() {
			p.SessionDataMutatorIterator().SetDatabase(prevDatabase)
		}()

		var tdn *sql.TableDescriptorName
		switch stmt.Typ {
		case tree.InspectTable:
			tdn = &sql.TableDescriptorName{
				TableDescriptor: run.table,
				TableName: tree.MakeTableNameWithSchema(
					tree.Name(run.db.GetName()), tree.Name(run.schema.GetName()), tree.Name(run.table.GetName()),
				),
			}
		}

		if checks, err := sql.InspectChecksByIndexNames(ctx, p, stmt.Options.NamedIndexes(), tdn); err != nil {
			return inspectRun{}, err
		} else {
			run.checks = checks
		}
	}

	if stmt.AsOf.Expr != nil {
		asOf, err := p.EvalAsOfTimestamp(ctx, stmt.AsOf)
		if err != nil {
			return inspectRun{}, err
		}
		run.asOfTimestamp = asOf.Timestamp
	}

	return run, nil
}

// inspectPlanHook implements sql.PlanHookFn.
func inspectPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	inspectStmt, ok := stmt.(*tree.Inspect)
	if !ok {
		return nil, nil, false, nil
	}

	if !p.ExtendedEvalContext().Settings.Version.IsActive(ctx, clusterversion.V25_4) {
		return nil, nil, false, pgerror.Newf(pgcode.FeatureNotSupported, "INSPECT requires the cluster to be upgraded to v25.4")
	}

	switch inspectStmt.Typ {
	case tree.InspectTable, tree.InspectDatabase:
		if !p.ExtendedEvalContext().SessionData().EnableInspectCommand {
			return nil, nil, false, errors.WithHint(
				pgerror.Newf(pgcode.ExperimentalFeature, "INSPECT is an experimental feature and is disabled by default"),
				"To enable, run SET enable_inspect_command = true;",
			)
		}
	default:
		return nil, nil, false, errors.AssertionFailedf("unexpected INSPECT type received, got: %v", inspectStmt.Typ)
	}

	if err := p.CheckGlobalPrivilegeOrRoleOption(ctx, privilege.INSPECT); err != nil {
		return nil, nil, false, err
	}

	if err := inspectStmt.Validate(); err != nil {
		return nil, nil, false, err
	}

	if !p.ExtendedEvalContext().TxnIsSingleStmt {
		return nil, nil, false, pgerror.Newf(pgcode.InvalidTransactionState,
			"cannot run within a multi-statement transaction")
	}

	run, err := newInspectRun(ctx, inspectStmt, p)
	if err != nil {
		return nil, nil, false, err
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		// We create the job record in the planner's transaction to ensure that
		// the job record creation happens transactionally.
		plannerTxn := p.InternalSQLTxn()

		sj, err := sql.TriggerInspectJob(ctx, tree.AsString(stmt), p.ExecCfg(), plannerTxn, run.checks, run.asOfTimestamp)
		if err != nil {
			return err
		}

		if err := sj.AwaitCompletion(ctx); err != nil {
			return err
		}
		return sj.ReportExecutionResults(ctx, resultsCh)
	}

	return fn, jobs.InspectJobExecutionResultHeader, false, nil
}

func init() {
	sql.AddPlanHook("inspect", inspectPlanHook, inspectTypeCheck)
}
