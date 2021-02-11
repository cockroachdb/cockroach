// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingutils

import (
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func init() {
	streaming.CompleteIngestionHook = doCompleteIngestion
}

func doCompleteIngestion(evalCtx *tree.EvalContext, txn *kv.Txn, jobID int) error {
	// Get the job payload for job_id.
	const jobsQuery = `SELECT progress FROM system.jobs WHERE id=$1 FOR UPDATE`
	row, err := evalCtx.InternalExecutor.QueryRow(evalCtx.Context,
		"get-stream-ingestion-job-metadata", txn, jobsQuery, jobID)
	if err != nil {
		return err
	}
	// If an entry does not exist for the provided job_id we return an
	// error.
	if row == nil {
		return errors.Newf("job %d: not found in system.jobs table", jobID)
	}

	progress, err := jobs.UnmarshalProgress(row[0])
	if err != nil {
		return err
	}
	var sp *jobspb.Progress_StreamIngest
	var ok bool
	if sp, ok = progress.GetDetails().(*jobspb.Progress_StreamIngest); !ok {
		return errors.Newf("job %d: not of expected type StreamIngest", jobID)
	}

	// Update the sentinel being polled by the stream ingestion job to
	// check if a complete has been signaled.
	sp.StreamIngest.MarkedForCompletion = true
	progress.ModifiedMicros = timeutil.ToUnixMicros(txn.ReadTimestamp().GoTime())
	progressBytes, err := protoutil.Marshal(progress)
	if err != nil {
		return err
	}
	updateJobQuery := `UPDATE system.jobs SET progress=$1 WHERE id=$2`
	_, err = evalCtx.InternalExecutor.QueryRow(evalCtx.Context,
		"set-stream-ingestion-job-metadata", txn, updateJobQuery, progressBytes, jobID)
	return err
}
