// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// RunningJobExists checks that whether there are any job of the given types
// in the pending, running, or paused status, optionally ignoring the job with
// the ID specified by ignoreJobID as well as any jobs created after it, if
// the passed ID is not InvalidJobID.
func RunningJobExists(
	ctx context.Context,
	ignoreJobID jobspb.JobID,
	txn isql.Txn,
	cv clusterversion.Handle,
	jobTypes ...jobspb.Type,
) (exists bool, retErr error) {
	if !cv.IsActive(ctx, clusterversion.V23_1BackfillTypeColumnInJobsTable) {
		return legacyRunningJobExists(ctx, ignoreJobID, txn, jobTypes...)
	}

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

func legacyRunningJobExists(
	ctx context.Context, jobID jobspb.JobID, txn isql.Txn, jobTypes ...jobspb.Type,
) (exists bool, retErr error) {
	const stmt = `
SELECT
  id, payload
FROM
  crdb_internal.system_jobs
WHERE
  status IN ` + NonTerminalStatusTupleString + `
ORDER BY created`

	it, err := txn.QueryIterator(
		ctx,
		"get-jobs",
		txn.KV(),
		stmt,
	)
	if err != nil {
		return false /* exists */, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		payload, err := UnmarshalPayload(row[1])
		if err != nil {
			return false /* exists */, err
		}

		isTyp := false
		for _, typ := range jobTypes {
			if payload.Type() == typ {
				isTyp = true
				break
			}
		}
		if isTyp {
			id := jobspb.JobID(*row[0].(*tree.DInt))
			if id == jobID {
				break
			}

			return true /* exists */, nil /* retErr */
		}
	}
	return false /* exists */, err
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

// isJobTypeColumnDoesNotExistError returns true if the error is of the form
// `column "job_type" does not exist`.
func isJobTypeColumnDoesNotExistError(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.UndefinedColumn &&
		strings.Contains(err.Error(), "column \"job_type\" does not exist")
}

// isJobInfoTableDoesNotExistError returns true if the error is of the form
// `related "job_info" does not exist`.
func isJobInfoTableDoesNotExistError(err error) bool {
	return pgerror.GetPGCode(err) == pgcode.UndefinedTable &&
		strings.Contains(err.Error(), "relation \"system.job_info\" does not exist")
}

// MaybeGenerateForcedRetryableError returns a
// TransactionRetryWithProtoRefreshError that will cause the txn to be retried
// if the error is because of an undefined job_type column or missing job_info
// table.
//
// In https://github.com/cockroachdb/cockroach/issues/106762 we noticed that if
// a query is executed with an AS OF SYSTEM TIME clause that picks a transaction
// timestamp before the job_type migration, then parts of the jobs
// infrastructure will attempt to query the job_type column even though it
// doesn't exist at the transaction's timestamp.
//
// As a short term fix, when we encounter an `UndefinedTable` or
// `UndefinedColumn` error we generate a synthetic retryable error so that the
// txn is pushed to a higher timestamp at which the upgrade will have completed
// and the table/column will be visible. The longer term fix is being tracked in
// https://github.com/cockroachdb/cockroach/issues/106764.
func MaybeGenerateForcedRetryableError(ctx context.Context, txn *kv.Txn, err error) error {
	if err != nil && isJobTypeColumnDoesNotExistError(err) {
		return txn.GenerateForcedRetryableErr(ctx, "synthetic error "+
			"to push timestamp to after the `job_type` upgrade has run")
	}
	if err != nil && isJobInfoTableDoesNotExistError(err) {
		return txn.GenerateForcedRetryableErr(ctx, "synthetic error "+
			"to push timestamp to after the `job_info` upgrade has run")
	}
	return err
}
