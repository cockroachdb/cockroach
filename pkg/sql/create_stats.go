// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// createStatsPlanHook implements PlanHookFn.
func createStatsPlanHook(
	_ context.Context, stmt tree.Statement, ps PlanHookState,
) (fn PlanHookRowFn, header sqlbase.ResultColumns, subplans []planNode, err error) {
	n, ok := stmt.(*tree.CreateStats)
	if !ok {
		return nil, nil, nil, nil
	}

	p := ps.(*planner)
	fn = func(ctx context.Context, _ []PlanNode, resultsCh chan<- tree.Datums) error {
		var tableDesc *ImmutableTableDescriptor
		switch t := n.Table.(type) {
		case *tree.TableName:
			// TODO(anyone): if CREATE STATISTICS is meant to be able to operate
			// within a transaction, then the following should probably run with
			// caching disabled, like other DDL statements.
			tableDesc, err = ResolveExistingObject(ctx, p, t, true /*required*/, requireTableDesc)
			if err != nil {
				return err
			}

		case *tree.TableRef:
			flags := ObjectLookupFlags{CommonLookupFlags: CommonLookupFlags{
				avoidCached: p.avoidCachedDescriptors,
			}}
			tableDesc, err = p.Tables().getTableVersionByID(ctx, p.txn, sqlbase.ID(t.TableID), flags)
			if err != nil {
				return err
			}
		}

		if tableDesc.IsVirtualTable() {
			return errors.Errorf("cannot create statistics on virtual tables")
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.SELECT); err != nil {
			return err
		}

		// Identify which columns we should create statistics for.
		var createStatsColumns []jobspb.CreateStatsDetails_Columns
		if len(n.ColumnNames) == 0 {
			if createStatsColumns, err = createStatsDefaultColumns(tableDesc); err != nil {
				return err
			}
		} else {
			columns, err := tableDesc.FindActiveColumnsByNames(n.ColumnNames)
			if err != nil {
				return err
			}

			columnIDs := make([]sqlbase.ColumnID, len(columns))
			for i := range columns {
				columnIDs[i] = columns[i].ID
			}
			createStatsColumns = []jobspb.CreateStatsDetails_Columns{{IDs: columnIDs}}
		}

		// Create a job to run statistics creation.
		_, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, jobs.Record{
			Description: tree.AsStringWithFlags(n, tree.FmtAlwaysQualifyTableNames),
			Username:    p.User(),
			Details: jobspb.CreateStatsDetails{
				Name:      n.Name,
				Table:     tableDesc.TableDescriptor,
				Columns:   createStatsColumns,
				Statement: n.String(),
			},
			Progress: jobspb.CreateStatsProgress{},
		})
		if err != nil {
			return err
		}
		return <-errCh
	}
	return fn, noColumns, nil, nil
}

// createStatsDefaultColumns creates column statistics on a default set of
// columns when no columns were specified by the caller.
//
// To determine a useful set of default column statistics, we rely on
// information provided by the schema. In particular, the presence of an index
// on a particular set of columns indicates that the workload likely contains
// queries that involve those columns (e.g., for filters), and it would be
// useful to have statistics on prefixes of those columns. For example, if a
// table abc contains indexes on (a ASC, b ASC) and (b ASC, c ASC), we will
// collect statistics on a, {a, b}, b, and {b, c}.
// TODO(rytaft): This currently only generates one single-column stat per
// index. Add code to collect multi-column stats once they are supported.
func createStatsDefaultColumns(
	desc *ImmutableTableDescriptor,
) ([]jobspb.CreateStatsDetails_Columns, error) {
	columns := make([]jobspb.CreateStatsDetails_Columns, 0, len(desc.Indexes)+1)

	var requestedCols util.FastIntSet

	// If the primary key is not the hidden rowid column, collect stats on it.
	pkCol := desc.PrimaryIndex.ColumnIDs[0]
	if !isHidden(desc, pkCol) {
		columns = append(columns, jobspb.CreateStatsDetails_Columns{IDs: []sqlbase.ColumnID{pkCol}})
		requestedCols.Add(int(pkCol))
	}

	// Add columns for each secondary index.
	for i := range desc.Indexes {
		idxCol := desc.Indexes[i].ColumnIDs[0]
		if !requestedCols.Contains(int(idxCol)) {
			columns = append(
				columns, jobspb.CreateStatsDetails_Columns{IDs: []sqlbase.ColumnID{idxCol}},
			)
			requestedCols.Add(int(idxCol))
		}
	}

	// If there are no non-hidden index columns, collect stats on the first
	// non-hidden column in the table.
	if len(columns) == 0 {
		for i := range desc.Columns {
			if !desc.Columns[i].IsHidden() {
				columns = append(
					columns, jobspb.CreateStatsDetails_Columns{IDs: []sqlbase.ColumnID{desc.Columns[i].ID}},
				)
				break
			}
		}
	}

	// If there are still no columns, return an error.
	if len(columns) == 0 {
		return nil, errors.New("CREATE STATISTICS called on a table with no visible columns")
	}

	return columns, nil
}

func isHidden(desc *ImmutableTableDescriptor, columnID sqlbase.ColumnID) bool {
	for i := range desc.Columns {
		if desc.Columns[i].ID == columnID {
			return desc.Columns[i].IsHidden()
		}
	}
	panic("column not found in table")
}

type createStatsResumer struct {
	evalCtx *extendedEvalContext
}

var _ jobs.Resumer = &createStatsResumer{}

// Resume is part of the jobs.Resumer interface.
func (r *createStatsResumer) Resume(
	ctx context.Context, job *jobs.Job, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	// TODO(rytaft): If this job is for automatic stats creation, use the
	// Job ID to lock automatic stats creation with a lock manager. If the
	// lock succeeds, proceed to the next step (but first check the stats
	// cache to make sure a new statistic was not just added for this table).
	// If the lock fails, cancel this job and return an error.

	p := phs.(*planner)
	r.evalCtx = p.ExtendedEvalContext()

	ci := sqlbase.ColTypeInfoFromColTypes([]sqlbase.ColumnType{})
	rows := sqlbase.NewRowContainer(r.evalCtx.Mon.MakeBoundAccount(), ci, 0)
	defer func() {
		if rows != nil {
			rows.Close(ctx)
		}
	}()

	dsp := p.DistSQLPlanner()
	err := r.evalCtx.ExecCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if err := dsp.planAndRunCreateStats(ctx, r.evalCtx, txn, job, NewRowResultWriter(rows)); err != nil {
			// Check if this was a context canceled error and restart if it was.
			if s, ok := status.FromError(errors.Cause(err)); ok {
				if s.Code() == codes.Canceled && s.Message() == context.Canceled.Error() {
					return jobs.NewRetryJobError("node failure")
				}
			}

			// If the job was canceled, any of the distsql processors could have been
			// the first to encounter the .Progress error. This error's string is sent
			// through distsql back here, so we can't examine the err type in this case
			// to see if it's a jobs.InvalidStatusError. Instead, attempt to update the
			// job progress to coerce out the correct error type. If the update succeeds
			// then return the original error, otherwise return this error instead so
			// it can be cleaned up at a higher level.
			if err := job.FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
				d := details.(*jobspb.Progress_CreateStats).CreateStats
				return d.Completed()
			}); err != nil {
				return err
			}
			return err
		}

		return nil
	})

	return err
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r *createStatsResumer) OnFailOrCancel(
	ctx context.Context, txn *client.Txn, job *jobs.Job,
) error {
	return nil
}

// OnSuccess is part of the jobs.Resumer interface.
func (r *createStatsResumer) OnSuccess(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
	details := job.Details().(jobspb.CreateStatsDetails)
	// Record this statistics creation in the event log.
	// TODO(rytaft): This creates a new transaction for the CREATE STATISTICS
	// event. It must be different from the CREATE STATISTICS transaction,
	// because that transaction must be read-only. In the future we may want
	// to use the transaction that inserted the new stats into the
	// system.table_statistics table, but that would require calling
	// MakeEventLogger from the distsqlrun package.
	return r.evalCtx.ExecCfg.DB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		return MakeEventLogger(r.evalCtx.ExecCfg).InsertEventRecord(
			ctx,
			txn,
			EventLogCreateStatistics,
			int32(details.Table.ID),
			int32(r.evalCtx.NodeID),
			struct {
				StatisticName string
				Statement     string
			}{details.Name.String(), details.Statement},
		)
	})
}

// OnTerminal is part of the jobs.Resumer interface.
func (r *createStatsResumer) OnTerminal(
	ctx context.Context, job *jobs.Job, status jobs.Status, resultsCh chan<- tree.Datums,
) {
}

func createStatsResumeHook(typ jobspb.Type, settings *cluster.Settings) jobs.Resumer {
	if typ != jobspb.TypeCreateStats {
		return nil
	}

	return &createStatsResumer{}
}

func init() {
	AddPlanHook(createStatsPlanHook)
	jobs.AddResumeHook(createStatsResumeHook)
}
