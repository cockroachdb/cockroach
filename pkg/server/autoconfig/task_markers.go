// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package autoconfig

import (
	"bytes"
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// InfoKeyCompletionPrefix is the prefix of the key inserted in job_info
// when a task has completed.
var InfoKeyCompletionPrefix = roachpb.Key("completed-")

// InfoKeyStartPrefix is the prefix of the key inserted in job_info
// when a task has started.
var InfoKeyStartPrefix = roachpb.Key("started-")

// DecodeTaskID returns the task ID for the given infoKey.
func DecodeTaskID(prefix roachpb.Key, infoKey []byte) (uint64, error) {
	if !bytes.HasPrefix(infoKey, []byte(prefix)) {
		return 0, errors.AssertionFailedf("programming error: prefix %q missing: %q", string(prefix), string(infoKey))
	}
	infoKey = infoKey[len(prefix):]
	_, v, err := encoding.DecodeUvarintAscending(infoKey)
	return v, err
}

// MakeTaskInfoKey creates a job_info infoKey for the given task.
func MakeTaskInfoKey(prefix roachpb.Key, taskID uint64) roachpb.Key {
	infoKey := make([]byte, 0, len(prefix)+10)
	infoKey = append(infoKey, []byte(prefix)...)
	infoKey = encoding.EncodeUvarintAscending(infoKey, taskID)
	return infoKey
}

// writeStartMarker writes a start marker for the given task ID and
// also writes its job ID into the value part.
func writeStartMarker(
	ctx context.Context, txn isql.Txn, execCfg *sql.ExecutorConfig, jobID jobspb.JobID, taskID uint64,
) error {
	startMarkerInfoKey := MakeTaskInfoKey(InfoKeyStartPrefix, taskID)
	// We cannot use the InfoStorage directly here because this function
	// is called from two different jobs (the runner and the task) but
	// must always write with the job ID of the runner to job_info.
	_, err := txn.ExecEx(ctx, "write-start-marker", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
		jobs.AutoConfigRunnerJobID,
		startMarkerInfoKey,
		[]byte(strconv.FormatUint(uint64(jobID), 10)))
	return err
}

// getCurrentlyStartedTaskID retrieves the ID of the last task which
// has a start marker in job_info.
func getCurrentlyStartedTaskID(
	ctx context.Context, txn isql.Txn, execCfg *sql.ExecutorConfig,
) (prevTaskID uint64, prevJobID jobspb.JobID, err error) {
	row, err := txn.QueryRowEx(ctx,
		"get-last-task-start-marker", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		// Note: we need the condition "AND info_key < prefixend" so as
		// to avoid observing other non-marker job info entries.
		`SELECT info_key, value FROM system.job_info
     WHERE job_id = $1 AND info_key >= $2 AND info_key < $3
     ORDER BY info_key DESC, written ASC LIMIT 1`,
		jobs.AutoConfigRunnerJobID,
		[]byte(InfoKeyStartPrefix), []byte(InfoKeyStartPrefix.PrefixEnd()))
	if err != nil {
		return 0, 0, errors.Wrap(err, "finding last task start marker")
	}
	if row == nil {
		// No start marker - no task running.
		return 0, 0, nil
	}

	// There's a started task. What is its task ID? Decode it from the info key.
	infoKey := []byte(tree.MustBeDBytes(row[0]))
	prevTaskID, err = DecodeTaskID(InfoKeyStartPrefix, infoKey)
	if err != nil {
		return 0, 0, errors.Wrap(err, "while decoding last start marker")
	}

	// Also retrieve is job ID from the value bytes.
	valueBytes := tree.MustBeDBytes(row[1])
	jid, err := strconv.ParseInt(string(valueBytes), 10, 64)
	if err != nil {
		return prevTaskID, 0, errors.Wrapf(err,
			"while decoding value (%q) for start marker for task %d",
			string(valueBytes), prevTaskID)
	}

	prevJobID = jobspb.JobID(jid)
	return prevTaskID, prevJobID, nil
}

// getLastCompletedTaskID retrieves the task ID of the last task which
// has a completion marker in job_info.
func getLastCompletedTaskID(
	ctx context.Context, txn isql.Txn, execCfg *sql.ExecutorConfig,
) (lastTaskID uint64, err error) {
	row, err := txn.QueryRowEx(ctx,
		"get-last-task-completion-info", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		// Note: we need the condition "AND info_key < prefixend" so as
		// to avoid observing other non-marker job info entries.
		`SELECT info_key FROM system.job_info
     WHERE job_id = $1 AND info_key >= $2 AND info_key < $3
     ORDER BY info_key DESC, written ASC LIMIT 1`,
		jobs.AutoConfigRunnerJobID,
		[]byte(InfoKeyCompletionPrefix), []byte(InfoKeyCompletionPrefix.PrefixEnd()))
	if err != nil {
		return 0, errors.Wrap(err, "finding latest task completion marker")
	}
	if row == nil {
		// No completed task marker -- probably because we're going to execute
		// the very first task.
		return 0, nil
	}

	// There's a task.
	infoKey := []byte(tree.MustBeDBytes(row[0]))
	lastTaskID, err = DecodeTaskID(InfoKeyCompletionPrefix, infoKey)
	if err != nil {
		return 0, errors.Wrapf(err, "decoding info key (%q) for last completion marker", string(infoKey))
	}
	return lastTaskID, nil
}

// markTaskCompletes transactionally removes the task's start marker
// and creates a completion marker.
func markTaskComplete(
	ctx context.Context,
	txn isql.Txn,
	execCfg *sql.ExecutorConfig,
	taskID uint64,
	completionValue []byte,
) error {
	startInfoKey := MakeTaskInfoKey(InfoKeyStartPrefix, taskID)
	completionInfoKey := MakeTaskInfoKey(InfoKeyCompletionPrefix, taskID)

	// Remove the start marker.
	_, err := txn.ExecEx(ctx, "delete-task-start-info",
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.job_info WHERE job_id = $1 AND info_key = $2`,
		jobs.AutoConfigRunnerJobID, startInfoKey)
	if err != nil {
		return err
	}

	// Remove any previous completion marker. This avoids the
	// accumulation of past completion markers over time.
	_, err = txn.ExecEx(ctx, "delete-previous-completion-markers",
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.job_info
      WHERE job_id = $1 AND info_key >= $2 AND info_key < $3`,
		jobs.AutoConfigRunnerJobID,
		[]byte(InfoKeyCompletionPrefix), completionInfoKey)
	if err != nil {
		return err
	}

	// Add our completion marker.
	// We cannot use the InfoStorage directly here because this function
	// is called from two different jobs (the runner and the task) but
	// must always write with the job ID of the runner to job_info.
	_, err = txn.ExecEx(ctx, "write-start-marker", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`INSERT INTO system.job_info (job_id, info_key, value) VALUES ($1, $2, $3)`,
		jobs.AutoConfigRunnerJobID, completionInfoKey, completionValue)
	return err
}
