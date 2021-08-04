// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

// RunningJobExists checks that:
// * If job argument is not nil, whether there are any other jobs (matched by
//   payloadPredicate callback) in the pending, running, or paused status that
//   started earlier than this one.
// * If job is nil, RunningJobExists just checks if there are any pending,
//   running, or paused jobs that matches payloadPredicate.
func RunningJobExists(
	ctx context.Context,
	job *jobs.Job,
	ie sqlutil.InternalExecutor,
	txn *kv.Txn,
	payloadPredicate func(payload *jobspb.Payload) bool,
) (exists bool, retErr error) {
	var jobID jobspb.JobID
	if job != nil {
		jobID = job.ID()
	}
	const stmt = `
SELECT
  id, payload
FROM
  system.jobs
WHERE
  status IN ($1, $2, $3)
ORDER BY created`

	it, err := ie.QueryIterator(
		ctx,
		"get-jobs",
		txn,
		stmt,
		jobs.StatusPending,
		jobs.StatusRunning,
		jobs.StatusPaused,
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
		payload, err := jobs.UnmarshalPayload(row[1])
		if err != nil {
			return false /* exists */, err
		}

		if payloadPredicate(payload) {
			id := jobspb.JobID(*row[0].(*tree.DInt))
			if id == jobID {
				break
			}

			return true /* exists */, nil /* retErr */
		}
	}
	return false /* exists */, err
}
