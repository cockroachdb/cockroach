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

const jobExistsOp = "get-jobs"

// JobExists checks that whether there are any other jobs (matched by
// payloadPredicate callback) that started earlier than the job with provided jobID.
// If the running parameter is true, this function will only search jobs that have
// pending, running, or paused status.
// If the provided jobID is a jobspb.InvalidJobID, this function checks if
// exists any jobs that matches the payloadPredicate.
func JobExists(
	ctx context.Context,
	jobID jobspb.JobID,
	ie sqlutil.InternalExecutor,
	txn *kv.Txn,
	running bool,
	payloadPredicate func(payload *jobspb.Payload) bool,
) (exists bool, retErr error) {

	var statusPredicate string
	if running {
		statusPredicate = `WHERE status IN ` + NonTerminalStatusTupleString
	}
	stmt := `
	SELECT
		id, payload
	FROM
		system.jobs ` +
		statusPredicate +
		`ORDER BY created`

	it, err := ie.QueryIterator(
		ctx,
		jobExistsOp,
		txn,
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
