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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
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

	stmt := `
SELECT
  id
FROM
  system.jobs
WHERE
	job_type IN ` + typeStrs + ` AND
  status IN ` + NonTerminalStatusTupleString + `
ORDER BY created
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
