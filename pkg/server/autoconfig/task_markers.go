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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// InfoKeyCompletionPrefix is the prefix of the key inserted in job_info
// when a task has completed.
const InfoKeyCompletionPrefix = "completed-"

// InfoKeyStartPrefix is the prefix of the key inserted in job_info
// when a task has started.
const InfoKeyStartPrefix = "started-"

// DecodeTaskID returns the task ID for the given infoKey.
func DecodeTaskID(prefix []byte, infoKey []byte) (uint64, error) {
	if !bytes.HasPrefix(infoKey, prefix) {
		return 0, errors.AssertionFailedf("programming error: prefix %q missing: %q", string(prefix), string(infoKey))
	}
	infoKey = infoKey[len(prefix):]
	_, v, err := encoding.DecodeUvarintAscending(infoKey)
	return v, err
}

// MakeTaskInfoKey creates a job_info infoKey for the given task.
func MakeTaskInfoKey(prefix []byte, taskID uint64) []byte {
	infoKey := make([]byte, 0, len(prefix)+10)
	infoKey = append(infoKey, prefix...)
	infoKey = encoding.EncodeUvarintAscending(infoKey, taskID)
	return infoKey
}

// writeStartMarker writes a start marker for the given task ID and
// also writes its job ID into the value part.
func writeStartMarker(
	ctx context.Context, txn isql.Txn, execCfg *sql.ExecutorConfig, jobID jobspb.JobID, taskID uint64,
) error {
	startMarkerInfoKey := MakeTaskInfoKey([]byte(InfoKeyStartPrefix), taskID)
	return execCfg.JobRegistry.WriteJobInfo(ctx,
		jobs.AutoConfigRunnerJobID,                    /* jobID */
		startMarkerInfoKey,                            /* infoKey */
		[]byte(strconv.FormatUint(uint64(jobID), 10)), /* value */
		txn,
	)
}

// getCurrentlyStartedTaskID retrieves the ID of the last task which
// has a start marker in job_info.
func getCurrentlyStartedTaskID(
	ctx context.Context, txn isql.Txn, execCfg *sql.ExecutorConfig,
) (prevTaskID uint64, prevJobID jobspb.JobID, err error) {
	row, err := txn.QueryRowEx(ctx,
		"get-last-task-start-marker", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`SELECT info_key, value FROM system.job_info
     WHERE job_id = $1 AND info_key >= $2
     ORDER BY info_key DESC, written ASC LIMIT 1`,
		jobs.AutoConfigRunnerJobID, []byte(InfoKeyStartPrefix))
	if err != nil {
		return 0, 0, errors.Wrap(err, "finding last task start marker")
	}
	if row == nil {
		// No start marker - no task running.
		return 0, 0, nil
	}

	// There's a started task. What is its task ID? Decode it from the info key.
	infoKey := []byte(tree.MustBeDBytes(row[0]))
	prevTaskID, err = DecodeTaskID([]byte(InfoKeyStartPrefix), infoKey)
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
		// Note: we need the condition "AND info_key < 'started-'" so as
		// to avoid observer the first start marker when there are no
		// completion markers yet.
		`SELECT info_key FROM system.job_info
     WHERE job_id = $1 AND info_key >= $2 AND info_key < $3
     ORDER BY info_key DESC, written ASC LIMIT 1`,
		jobs.AutoConfigRunnerJobID,
		[]byte(InfoKeyCompletionPrefix), []byte(InfoKeyStartPrefix))
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
	lastTaskID, err = DecodeTaskID([]byte(InfoKeyCompletionPrefix), infoKey)
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
	startInfoKey := MakeTaskInfoKey([]byte(InfoKeyStartPrefix), taskID)
	completionInfoKey := MakeTaskInfoKey([]byte(InfoKeyCompletionPrefix), taskID)

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
		`DELETE FROM system.job_info WHERE job_id = $1 AND info_key < $2`,
		jobs.AutoConfigRunnerJobID, completionInfoKey)
	if err != nil {
		return err
	}

	// Add our completion marker.
	return execCfg.JobRegistry.WriteJobInfo(ctx,
		jobs.AutoConfigRunnerJobID,
		completionInfoKey, completionValue,
		txn)
}
