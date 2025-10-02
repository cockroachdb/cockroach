// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
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
	indexes       []sql.IndexTableDesc
	asOfTimestamp hlc.Timestamp
}

func newInspectRun(
	ctx context.Context, stmt *tree.Inspect, p sql.PlanHookState,
) (inspectRun, error) {
	var tableDescs []catalog.TableDescriptor
	switch stmt.Typ {
	case tree.InspectTable:
		tableName := stmt.Table.ToTableName()
		_, desc, err := p.ResolveMutableTableDescriptor(ctx, &tableName, true /* required */, tree.ResolveRequireTableDesc)
		if err != nil {
			return inspectRun{}, err
		}

		tableDescs = []catalog.TableDescriptor{desc}
	case tree.InspectDatabase:
		dbDesc, err := p.Descriptors().ByName(p.Txn()).Get().Database(ctx, stmt.Database.ToUnresolvedName().String())
		if err != nil {
			return inspectRun{}, err
		}

		tables, err := p.Descriptors().ByName(p.Txn()).Get().GetAllTablesInDatabase(ctx, p.Txn(), dbDesc)
		if err != nil {
			return inspectRun{}, err
		}

		if err := tables.ForEachDescriptor(func(desc catalog.Descriptor) error {
			tableDescs = append(tableDescs, desc.(catalog.TableDescriptor))
			return nil
		}); err != nil {
			return inspectRun{}, err
		}

	default:
		return inspectRun{}, errors.AssertionFailedf("unexpected INSPECT type received, got: %v", stmt.Typ)
	}

	var run inspectRun

	allIndexes := func() []sql.IndexTableDesc {
		var indexDescs []sql.IndexTableDesc

		for _, tableDesc := range tableDescs {
			indexes := tableDesc.PublicNonPrimaryIndexes()
			for _, index := range indexes {
				if skip, reason := sql.IsUnsupportedIndexForInspect(index, tableDesc); skip {
					p.BufferClientNotice(ctx, pgnotice.Newf(
						"skipping index %q on table %q: %s", index.GetName(), tableDesc.GetName(), reason))
					continue
				}
				indexDescs = append(indexDescs, sql.IndexTableDesc{Index: index, TableDescriptor: tableDesc})
			}
		}

		return indexDescs
	}

	findIndexesByName := func(names tree.TableIndexNames) ([]sql.IndexTableDesc, error) {
		var indexDescs []sql.IndexTableDesc

		for _, indexName := range names {
			_, mut, idx, err := p.GetTableAndIndex(ctx, indexName, privilege.INSPECT, false /* skipCache */)
			if err != nil {
				return nil, err
			}

			if unsupported, reason := sql.IsUnsupportedIndexForInspect(idx, mut); unsupported {
				return nil, pgerror.Newf(pgcode.FeatureNotSupported, "inspect of index %q on table %q is unsupported: %s", idx.GetName(), mut.GetName(), reason)
			}

			indexDescs = append(indexDescs, sql.IndexTableDesc{Index: idx, TableDescriptor: mut})
		}

		return indexDescs, nil
	}

	// Process any options into the run.
	if len(stmt.Options) == 0 {
		// The bare command (no options) is treated as a default INDEX ALL.
		run.indexes = allIndexes()
	} else {
		indexDescs, err := findIndexesByName(stmt.Options.NamedIndexes())
		if err != nil {
			return inspectRun{}, err
		}
		run.indexes = indexDescs

		for _, option := range stmt.Options {
			switch option.(type) {
			case *tree.InspectOptionIndex:
				break
			case *tree.InspectOptionIndexAll:
				run.indexes = allIndexes()
			default:
				return inspectRun{}, errors.AssertionFailedf("unexpected INSPECT option received, got: %T", option)
			}
		}
	}
	if len(run.indexes) == 0 {
		return inspectRun{}, pgerror.Newf(pgcode.InvalidParameterValue, "no indexes found to inspect")
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
	case tree.InspectTable:
		if !p.ExtendedEvalContext().SessionData().EnableInspectCommand {
			return nil, nil, false, pgerror.Newf(pgcode.ExperimentalFeature, "INSPECT TABLE requires the enable_inspect_command setting to be enabled")
		}
	case tree.InspectDatabase:
		if !p.ExtendedEvalContext().SessionData().EnableInspectCommand {
			return nil, nil, false, pgerror.Newf(pgcode.ExperimentalFeature, "INSPECT DATABASE requires the enable_inspect_command setting to be enabled")
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

		sj, err := sql.TriggerInspectJob(ctx, tree.AsString(stmt), p.ExecCfg(), plannerTxn, run.indexes, run.asOfTimestamp)
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
