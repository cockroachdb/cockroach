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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// infoKeyCompletionPrefix is the prefix of the key inserted in job_info
// when a task has completed.
var infoKeyCompletionPrefix = []byte("completed-")

// infoKeyStartPrefix is the prefix of the key inserted in job_info
// when a task has started.
var infoKeyStartPrefix = []byte("started-")

// InfoKeyStartPrefix returns the info_key scan start key for
// all task start markers for the given environment.
func InfoKeyStartPrefix(env EnvironmentID) []byte {
	infoKey := make([]byte, 0, len(infoKeyStartPrefix)+len(env)+10)
	infoKey = append(infoKey, infoKeyStartPrefix...)
	infoKey = encoding.EncodeStringAscending(infoKey, string(env))
	return infoKey
}

// InfoKeyCompletionPrefix returns the info_key scan start key for
// all task completion markers for the given environment.
func InfoKeyCompletionPrefix(env EnvironmentID) []byte {
	infoKey := make([]byte, 0, len(infoKeyCompletionPrefix)+len(env)+10)
	infoKey = append(infoKey, infoKeyCompletionPrefix...)
	infoKey = encoding.EncodeStringAscending(infoKey, string(env))
	return infoKey
}

// InfoKeyTaskRef represents the reference to a task stored in
// job_info task markers.
type InfoKeyTaskRef struct {
	Environment EnvironmentID
	Task        TaskID
}

// EncodeStartMarker creates a job_info info key that identifies
// a start marker for this task.
func (tr *InfoKeyTaskRef) EncodeStartMarkerKey() []byte {
	return tr.encodeInternal(infoKeyStartPrefix)
}

// DecodeStartMarker decodes a job_info info key that identifies a
// start marker for this task.
func (tr *InfoKeyTaskRef) DecodeStartMarkerKey(infoKey []byte) error {
	return tr.decodeInternal(infoKeyStartPrefix, infoKey)
}

// EncodeCompletionMarker creates a job_info info key that identifies
// a completion marker for this task.
func (tr *InfoKeyTaskRef) EncodeCompletionMarkerKey() []byte {
	return tr.encodeInternal(infoKeyCompletionPrefix)
}

// DecodeCompletionMarker decodes a job_info info key that identifies
// a completion marker for this task.
func (tr *InfoKeyTaskRef) DecodeCompletionMarkerKey(infoKey []byte) error {
	return tr.decodeInternal(infoKeyCompletionPrefix, infoKey)
}

func (tr *InfoKeyTaskRef) encodeInternal(prefix []byte) []byte {
	infoKey := make([]byte, 0, len(prefix)+10)
	infoKey = append(infoKey, prefix...)
	infoKey = encoding.EncodeStringAscending(infoKey, string(tr.Environment))
	infoKey = encoding.EncodeUvarintAscending(infoKey, uint64(tr.Task))
	return infoKey
}

func (tr *InfoKeyTaskRef) decodeInternal(prefix, infoKey []byte) error {
	if !bytes.HasPrefix(infoKey, prefix) {
		return errors.AssertionFailedf("programming error: prefix %q missing: %q", string(prefix), string(infoKey))
	}
	infoKey = infoKey[len(prefix):]
	rest, s, err := encoding.DecodeUnsafeStringAscendingDeepCopy(infoKey, nil)
	if err != nil {
		return errors.Wrap(err, "decoding environment from task info key")
	}
	_, v, err := encoding.DecodeUvarintAscending(rest)
	if err != nil {
		return errors.Wrap(err, "decoding task ID from task info key")
	}
	tr.Environment = EnvironmentID(s)
	tr.Task = TaskID(v)
	return nil
}

// writeStartMarker writes a start marker for the given task ID and
// also writes its job ID into the value part.
func writeStartMarker(
	ctx context.Context, txn isql.Txn, taskRef InfoKeyTaskRef, jobID jobspb.JobID,
) error {
	startMarkerInfoKey := taskRef.EncodeStartMarkerKey()
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
	ctx context.Context, txn isql.Txn, env EnvironmentID,
) (prevTaskID TaskID, prevJobID jobspb.JobID, err error) {
	startPrefix := InfoKeyStartPrefix(env)
	row, err := txn.QueryRowEx(ctx,
		"get-last-task-start-marker", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		// Note: we need the condition "AND info_key < prefixend" so as
		// to avoid observing other non-marker job info entries.
		`SELECT info_key, value FROM system.job_info
     WHERE job_id = $1 AND info_key >= $2 AND info_key < $3
     ORDER BY info_key DESC, written ASC LIMIT 1`,
		jobs.AutoConfigRunnerJobID,
		startPrefix, roachpb.Key(startPrefix).PrefixEnd())
	if err != nil {
		return 0, 0, errors.Wrap(err, "finding last task start marker")
	}
	if row == nil {
		// No start marker - no task running.
		return 0, 0, nil
	}

	// There's a started task. What is its task ID? Decode it from the info key.
	infoKey := []byte(tree.MustBeDBytes(row[0]))
	var taskRef InfoKeyTaskRef
	if err := taskRef.DecodeStartMarkerKey(infoKey); err != nil {
		return 0, 0, errors.Wrap(err, "while decoding last start marker")
	}
	prevTaskID = taskRef.Task

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
	ctx context.Context, txn isql.Txn, env EnvironmentID,
) (lastTaskID TaskID, err error) {
	startPrefix := InfoKeyCompletionPrefix(env)
	row, err := txn.QueryRowEx(ctx,
		"get-last-task-completion-info", txn.KV(), sessiondata.NodeUserSessionDataOverride,
		// Note: we need the condition "AND info_key < prefixend" so as
		// to avoid observing other non-marker job info entries.
		`SELECT info_key FROM system.job_info
     WHERE job_id = $1 AND info_key >= $2 AND info_key < $3
     ORDER BY info_key DESC, written ASC LIMIT 1`,
		jobs.AutoConfigRunnerJobID,
		startPrefix, roachpb.Key(startPrefix).PrefixEnd())
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
	var taskRef InfoKeyTaskRef
	if err := taskRef.DecodeCompletionMarkerKey(infoKey); err != nil {
		return 0, errors.Wrapf(err, "decoding info key (%q) for last completion marker", string(infoKey))
	}
	return taskRef.Task, nil
}

// markTaskCompletes transactionally removes the task's start marker
// and creates a completion marker.
func markTaskComplete(
	ctx context.Context, txn isql.Txn, taskRef InfoKeyTaskRef, completionValue []byte,
) error {
	// Remove the start marker.
	_, err := txn.ExecEx(ctx, "delete-task-start-info",
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.job_info WHERE job_id = $1 AND info_key = $2`,
		jobs.AutoConfigRunnerJobID, taskRef.EncodeStartMarkerKey())
	if err != nil {
		return err
	}

	// Remove any previous completion marker. This avoids the
	// accumulation of past completion markers over time.
	completionKeyPrefix := InfoKeyCompletionPrefix(taskRef.Environment)
	completionInfoKey := taskRef.EncodeCompletionMarkerKey()
	_, err = txn.ExecEx(ctx, "delete-previous-completion-markers",
		txn.KV(), sessiondata.NodeUserSessionDataOverride,
		`DELETE FROM system.job_info
      WHERE job_id = $1 AND info_key >= $2 AND info_key < $3`,
		jobs.AutoConfigRunnerJobID,
		completionKeyPrefix, completionInfoKey)
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
