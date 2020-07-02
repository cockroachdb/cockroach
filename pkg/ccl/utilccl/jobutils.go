// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package utilccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// BulkJobExecutionResultHeader is the header for various job commands
// (BACKUP, RESTORE, IMPORT, etc) stmt results.
var BulkJobExecutionResultHeader = sqlbase.ResultColumns{
	{Name: "job_id", Typ: types.Int},
	{Name: "status", Typ: types.String},
	{Name: "fraction_completed", Typ: types.Float},
	{Name: "rows", Typ: types.Int},
	{Name: "index_entries", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}

// DetachedJobExecutionResultHeader is a the header for various job commands when
// job executes in detached mode (i.e. the caller doesn't wait for job completion).
var DetachedJobExecutionResultHeader = sqlbase.ResultColumns{
	{Name: "job_id", Typ: types.Int},
}

// StartAsyncJob starts running the job without blocking and waiting for job completion.
func StartAsyncJob(
	ctx context.Context, p sql.PlanHookState, jr *jobs.Record, resultsCh chan<- tree.Datums,
) error {
	// When running inside explicit transaction, we simply create the job record.
	// We do not wait for the job to finish.
	j, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
		ctx, *jr, p.ExtendedEvalContext().Txn)
	if err != nil {
		return err
	}
	resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(*j.ID()))}
	return nil
}
