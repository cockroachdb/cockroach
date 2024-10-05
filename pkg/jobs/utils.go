// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// RunningJobExists checks that whether there are any job of the given types
// in the pending, running, or paused status, optionally ignoring the job with
// the ID specified by ignoreJobID as well as any jobs created after it, if
// the passed ID is not InvalidJobID.
func RunningJobExists(
	ctx context.Context, ignoreJobID jobspb.JobID, txn isql.Txn, jobTypes ...jobspb.Type,
) (exists bool, retErr error) {
	var typeStrs string
	switch len(jobTypes) {
	case 0:
		return false, errors.AssertionFailedf("must specify job types")
	case 1:
		typeStrs = fmt.Sprintf("('%s')", jobTypes[0].String())
	case 2:
		typeStrs = fmt.Sprintf("('%s', '%s')", jobTypes[0].String(), jobTypes[1].String())
	default:
		var s strings.Builder
		fmt.Fprintf(&s, "('%s'", jobTypes[0].String())
		for _, typ := range jobTypes[1:] {
			fmt.Fprintf(&s, ", '%s'", typ.String())
		}
		s.WriteByte(')')
		typeStrs = s.String()
	}

	orderBy := " ORDER BY created"
	if ignoreJobID == jobspb.InvalidJobID {
		// There is no need to order by the created column if there is no job to
		// ignore.
		orderBy = ""
	}

	stmt := `
SELECT
  id
FROM
  system.jobs@jobs_status_created_idx
WHERE
	job_type IN ` + typeStrs + ` AND
  status IN ` + NonTerminalStatusTupleString + orderBy + `
LIMIT 1`
	it, err := txn.QueryIterator(
		ctx,
		"find-running-jobs-of-type",
		txn.KV(),
		stmt,
	)
	if err != nil {
		return false, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	ok, err := it.Next(ctx)
	if err != nil {
		return false, err
	}
	// The query is ordered by `created` so if the first is the ignored ID, then
	// any additional rows that would match the passed types must be created after
	// the ignored ID and are also supposed to be ignored, meaning we only return
	// true when the there are non-zero results and the first does not match.
	return ok && jobspb.JobID(*it.Cur()[0].(*tree.DInt)) != ignoreJobID, nil
}

// JobExists returns true if there is a row corresponding to jobID in the
// system.jobs table.
func JobExists(
	ctx context.Context, jobID jobspb.JobID, txn *kv.Txn, ex isql.Executor,
) (bool, error) {
	row, err := ex.QueryRow(ctx, "check-for-job", txn, `SELECT id FROM system.jobs WHERE id = $1`, jobID)
	if err != nil {
		return false, err
	}
	return row != nil, nil
}

// JobCoordinatorID returns the coordinator node ID of the job.
func JobCoordinatorID(
	ctx context.Context, jobID jobspb.JobID, txn *kv.Txn, ex isql.Executor,
) (int32, error) {
	row, err := ex.QueryRow(ctx, "fetch-job-coordinator", txn, `SELECT claim_instance_id FROM system.jobs WHERE id = $1`, jobID)
	if err != nil {
		return 0, err
	}
	if row == nil {
		return 0, errors.Errorf("coordinator not found for job %d", jobID)
	}
	coordinatorID, ok := tree.AsDInt(row[0])
	if !ok {
		return 0, errors.AssertionFailedf("expected coordinator ID to be an int, got %T", row[0])
	}
	return int32(coordinatorID), nil
}
