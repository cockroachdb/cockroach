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
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
	inspectStmt, ok := stmt.(*tree.Inspect)
	if !ok {
		return false, nil, nil
	}

	// Determine header based on DETACHED option to ensure consistency in
	// local-prepared mode where plans are cached.
	detached := inspectStmt.Options.IsDetached()
	if detached {
		header = jobs.DetachedJobExecutionResultHeader
	} else {
		header = jobs.InspectJobExecutionResultHeader
	}

	return true, header, nil
}

// inspectRun represents the runtime state of an execution of INSPECT.
type inspectRun struct {
	table catalog.TableDescriptor
	db    catalog.DatabaseDescriptor

	namedIndexes tree.TableIndexNames

	checks        []*jobspb.InspectDetails_Check
	asOfTimestamp hlc.Timestamp
}

func newInspectRun(
	ctx context.Context, stmt *tree.Inspect, p sql.PlanHookState,
) (inspectRun, error) {
	var run inspectRun

	avoidLeased := false
	if aost := p.ExtendedEvalContext().AsOfSystemTime; aost != nil {
		avoidLeased = true
	}

	if stmt.AsOf.Expr != nil {
		asOf, err := p.EvalAsOfTimestamp(ctx, stmt.AsOf)
		if err != nil {
			return inspectRun{}, err
		}
		run.asOfTimestamp = asOf.Timestamp
		avoidLeased = true
	}

	switch stmt.Typ {
	case tree.InspectTable:
		if table, err := p.ResolveExistingObjectEx(ctx, stmt.Table, true /* required */, tree.ResolveRequireTableDesc); err != nil {
			return inspectRun{}, err
		} else {
			run.table = table
		}

		dbGetter := p.Descriptors().ByIDWithLeased(p.Txn())
		if avoidLeased {
			dbGetter = p.Descriptors().ByIDWithoutLeased(p.Txn())
		}
		if db, err := dbGetter.Get().Database(ctx, run.table.GetParentID()); err != nil {
			return inspectRun{}, err
		} else {
			run.db = db
		}
	case tree.InspectDatabase:
		dbGetter := p.Descriptors().ByNameWithLeased(p.Txn())
		if avoidLeased {
			dbGetter = p.Descriptors().ByName(p.Txn())
		}
		if db, err := dbGetter.Get().Database(ctx, stmt.Database.ToUnresolvedName().String()); err != nil {
			return inspectRun{}, err
		} else {
			run.db = db
		}
	default:
		return inspectRun{}, errors.AssertionFailedf("unexpected INSPECT type received, got: %v", stmt.Typ)
	}

	if !stmt.Options.HasIndexOption() || stmt.Options.HasIndexAll() {
		// No INDEX options or INDEX ALL specified - inspect all indexes.
		switch stmt.Typ {
		case tree.InspectTable:
			checks, err := ChecksForTable(ctx, p, run.table)
			if err != nil {
				return inspectRun{}, err
			}
			run.checks = checks
		case tree.InspectDatabase:
			if checks, err := checksForDatabase(ctx, p, run.db); err != nil {
				return inspectRun{}, err
			} else {
				run.checks = checks
			}
		default:
			return inspectRun{}, errors.AssertionFailedf("unexpected INSPECT type received, got: %v", stmt.Typ)
		}
	} else {
		// Named indexes specified.
		switch stmt.Typ {
		case tree.InspectTable:
			schemaGetter := p.Descriptors().ByIDWithLeased(p.Txn())
			if avoidLeased {
				schemaGetter = p.Descriptors().ByIDWithoutLeased(p.Txn())
			}
			schema, err := schemaGetter.Get().Schema(ctx, run.table.GetParentSchemaID())
			if err != nil {
				return inspectRun{}, err
			}

			tableName := tree.MakeTableNameWithSchema(
				tree.Name(run.db.GetName()), tree.Name(schema.GetName()), tree.Name(run.table.GetName()),
			)
			if namedIndexes, err := stmt.Options.WithNamedIndexesOnTable(&tableName); err != nil {
				return inspectRun{}, err
			} else {
				run.namedIndexes = namedIndexes
			}
		case tree.InspectDatabase:
			if namedIndexes, err := stmt.Options.WithNamedIndexesInDatabase(run.db.GetName()); err != nil {
				return inspectRun{}, err
			} else {
				run.namedIndexes = namedIndexes
			}
		}

		if checks, err := checksByIndexNames(ctx, p, run.namedIndexes); err != nil {
			return inspectRun{}, err
		} else {
			run.checks = checks
		}
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
		// valid types.
	default:
		return nil, nil, false, errors.AssertionFailedf("unexpected INSPECT type received, got: %v", inspectStmt.Typ)
	}

	if err := p.CheckGlobalPrivilegeOrRoleOption(ctx, privilege.INSPECT); err != nil {
		return nil, nil, false, err
	}

	if err := inspectStmt.Validate(); err != nil {
		return nil, nil, false, err
	}

	detached := inspectStmt.Options.IsDetached()

	if !detached && !p.ExtendedEvalContext().TxnIsSingleStmt {
		return nil, nil, false, pgerror.Newf(pgcode.InvalidTransactionState,
			"cannot run within a multi-statement transaction")
	}

	run, err := newInspectRun(ctx, inspectStmt, p)
	if err != nil {
		return nil, nil, false, err
	}

	if detached && !p.ExtendedEvalContext().TxnImplicit {
		fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
			jobID := p.ExecCfg().JobRegistry.MakeJobID()
			jr := makeInspectJobRecord(tree.AsString(stmt), jobID, run.checks, run.asOfTimestamp)
			if _, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
				ctx, jr, jobID, p.InternalSQLTxn(),
			); err != nil {
				return err
			}
			notice := pgnotice.Newf("INSPECT job %d queued; will start after the current transaction commits", jobID)
			if err := p.SendClientNotice(ctx, notice, true /* immediateFlush */); err != nil {
				return err
			}
			resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(jobID))}
			return nil
		}
		return fn, jobs.DetachedJobExecutionResultHeader, false, nil
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		sj, err := TriggerJob(ctx, tree.AsString(stmt), p.ExecCfg(), run.checks, run.asOfTimestamp)
		if err != nil {
			return err
		}

		var notice pgnotice.Notice
		if detached {
			notice = pgnotice.Newf("INSPECT job %d running in the background", sj.ID())
		} else {
			notice = pgnotice.Newf("waiting for INSPECT job to complete: %s\nIf the statement is canceled, the job will continue in the background.", sj.ID())
		}
		if err = p.SendClientNotice(ctx, notice, true /* immediateFlush */); err != nil {
			return err
		}

		if detached {
			resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(sj.ID()))}
			return nil
		}

		if err = sj.AwaitCompletion(ctx); err != nil {
			return err
		}
		return sj.ReportExecutionResults(ctx, resultsCh)
	}

	header := jobs.InspectJobExecutionResultHeader
	if detached {
		header = jobs.DetachedJobExecutionResultHeader
	}
	return fn, header, false, nil
}

func makeInspectJobRecord(
	description string, jobID jobspb.JobID, checks []*jobspb.InspectDetails_Check, asOf hlc.Timestamp,
) jobs.Record {
	descIDs := catalog.DescriptorIDSet{}
	for _, check := range checks {
		descIDs.Add(check.TableID)
	}
	return jobs.Record{
		JobID:       jobID,
		Description: description,
		Details: jobspb.InspectDetails{
			Checks: checks,
			AsOf:   asOf,
		},
		Progress:      jobspb.InspectProgress{},
		CreatedBy:     nil,
		Username:      username.NodeUserName(),
		DescriptorIDs: descIDs.Ordered(),
	}
}

func init() {
	sql.AddPlanHook("inspect", inspectPlanHook, inspectTypeCheck)
}
