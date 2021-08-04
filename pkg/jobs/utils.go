// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/errors"
)

// RunningJobExists checks that whether there are any other jobs (matched by
// payloadPredicate callback) in the pending, running, or paused status that
// started earlier than the job with provided jobID.
func RunningJobExists(
	ctx context.Context,
	jobID jobspb.JobID,
	ie sqlutil.InternalExecutor,
	txn *kv.Txn,
	payloadPredicate func(payload *jobspb.Payload) bool,
) (exists bool, retErr error) {
	const stmt = `
SELECT
  id, payload
FROM
  system.jobs
WHERE
  status IN ($1, $2, $3, $4, $5, $6)
ORDER BY created`

	it, err := ie.QueryIterator(
		ctx,
		"get-jobs",
		txn,
		stmt,
		StatusPending,
		StatusRunning,
		StatusPaused,
		StatusCancelRequested,
		StatusPauseRequested,
		StatusReverting,
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
