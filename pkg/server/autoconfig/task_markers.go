// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package autoconfig

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// infoKeyCompletionPrefix is the prefix of the key inserted in job_info
// when a task has completed.
const infoKeyCompletionPrefix = "completed-"

// infoKeyStartPrefix is the prefix of the key inserted in job_info
// when a task has started.
const infoKeyStartPrefix = "started-"

// InfoKeyStartPrefix returns the info_key scan start key for
// all task start markers for the given environment.
func InfoKeyStartPrefix(env EnvironmentID) string {
	orderedKeyBytes := make([]byte, 0, len(env)+10)
	orderedKeyBytes = encoding.EncodeStringAscending(orderedKeyBytes, string(env))

	var buf strings.Builder
	buf.Grow(len(infoKeyStartPrefix) + hex.EncodedLen(len(orderedKeyBytes)))
	buf.WriteString(infoKeyStartPrefix)
	hexEncode := hex.NewEncoder(&buf)
	_, err := hexEncode.Write(orderedKeyBytes)
	if err != nil {
		// This can't happen because strings.Builder always auto-grows.
		panic(errors.HandleAsAssertionFailure(err))
	}
	return buf.String()
}

// InfoKeyCompletionPrefix returns the info_key scan start key for
// all task completion markers for the given environment.
func InfoKeyCompletionPrefix(env EnvironmentID) string {
	orderedKeyBytes := make([]byte, 0, len(env)+10)
	orderedKeyBytes = encoding.EncodeStringAscending(orderedKeyBytes, string(env))

	var buf strings.Builder
	buf.Grow(len(infoKeyStartPrefix) + hex.EncodedLen(len(orderedKeyBytes)))
	buf.WriteString(infoKeyCompletionPrefix)
	hexEncode := hex.NewEncoder(&buf)
	_, err := hexEncode.Write(orderedKeyBytes)
	if err != nil {
		// This can't happen because strings.Builder always auto-grows.
		panic(errors.HandleAsAssertionFailure(err))
	}
	return buf.String()
}

// InfoKeyTaskRef represents the reference to a task stored in
// job_info task markers.
type InfoKeyTaskRef struct {
	Environment EnvironmentID
	Task        TaskID
}

// EncodeStartMarker creates a job_info info key that identifies
// a start marker for this task.
func (tr *InfoKeyTaskRef) EncodeStartMarkerKey() string {
	return tr.encodeInternal(infoKeyStartPrefix)
}

// DecodeStartMarker decodes a job_info info key that identifies a
// start marker for this task.
func (tr *InfoKeyTaskRef) DecodeStartMarkerKey(infoKey string) error {
	return tr.decodeInternal(infoKeyStartPrefix, infoKey)
}

// EncodeCompletionMarker creates a job_info info key that identifies
// a completion marker for this task.
func (tr *InfoKeyTaskRef) EncodeCompletionMarkerKey() string {
	return tr.encodeInternal(infoKeyCompletionPrefix)
}

// DecodeCompletionMarker decodes a job_info info key that identifies
// a completion marker for this task.
func (tr *InfoKeyTaskRef) DecodeCompletionMarkerKey(infoKey string) error {
	return tr.decodeInternal(infoKeyCompletionPrefix, infoKey)
}

func (tr *InfoKeyTaskRef) encodeInternal(prefix string) string {
	orderedKeyBytes := make([]byte, 0, len(tr.Environment)+10)
	orderedKeyBytes = encoding.EncodeStringAscending(orderedKeyBytes, string(tr.Environment))
	orderedKeyBytes = encoding.EncodeUvarintAscending(orderedKeyBytes, uint64(tr.Task))

	var buf strings.Builder
	buf.Grow(len(prefix) + hex.EncodedLen(len(orderedKeyBytes)))
	buf.WriteString(prefix)
	hexEncode := hex.NewEncoder(&buf)
	_, err := hexEncode.Write(orderedKeyBytes)
	if err != nil {
		// This can't happen because strings.Builder always auto-grows.
		panic(errors.HandleAsAssertionFailure(err))
	}

	return buf.String()
}

func (tr *InfoKeyTaskRef) decodeInternal(prefix, infoKey string) error {
	if !strings.HasPrefix(infoKey, prefix) {
		return errors.AssertionFailedf("programming error: prefix %q missing: %q", prefix, infoKey)
	}
	infoKey = infoKey[len(prefix):]
	bytes, err := hex.DecodeString(infoKey)
	if err != nil {
		return errors.Wrapf(err, "decoding hex-encoded info key (%q)", infoKey)
	}
	rest, s, err := encoding.DecodeUnsafeStringAscendingDeepCopy(bytes, nil)
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
	infoStorage := jobs.InfoStorageForJob(txn, jobs.AutoConfigRunnerJobID)
	return infoStorage.Write(ctx,
		taskRef.EncodeStartMarkerKey(),
		[]byte(strconv.FormatUint(uint64(jobID), 10)))
}

// getCurrentlyStartedTaskID retrieves the ID of the last task which
// has a start marker in job_info.
func getCurrentlyStartedTaskID(
	ctx context.Context, txn isql.Txn, env EnvironmentID,
) (prevTaskID TaskID, prevJobID jobspb.JobID, err error) {
	infoStorage := jobs.InfoStorageForJob(txn, jobs.AutoConfigRunnerJobID)

	if err := infoStorage.GetLast(ctx,
		InfoKeyStartPrefix(env),
		func(infoKey string, value []byte) error {
			var taskRef InfoKeyTaskRef
			if err := taskRef.DecodeStartMarkerKey(infoKey); err != nil {
				return errors.Wrapf(err, "decoding info key (%q)", infoKey)
			}
			prevTaskID = taskRef.Task

			// Also retrieve is job ID from the value bytes.
			jid, err := strconv.ParseInt(string(value), 10, 64)
			if err != nil {
				return errors.Wrapf(err,
					"while decoding value (%q) for start marker for task %d",
					string(value), prevTaskID)
			}
			prevJobID = jobspb.JobID(jid)
			return nil
		}); err != nil {
		return 0, 0, errors.Wrap(err, "finding last task start marker")
	}

	return prevTaskID, prevJobID, nil
}

// getLastCompletedTaskID retrieves the task ID of the last task which
// has a completion marker in job_info.
func getLastCompletedTaskID(
	ctx context.Context, txn isql.Txn, env EnvironmentID,
) (lastTaskID TaskID, err error) {
	infoStorage := jobs.InfoStorageForJob(txn, jobs.AutoConfigRunnerJobID)

	if err := infoStorage.GetLast(ctx,
		InfoKeyCompletionPrefix(env),
		func(infoKey string, value []byte) error {
			// There's a task.
			var taskRef InfoKeyTaskRef
			if err := taskRef.DecodeCompletionMarkerKey(infoKey); err != nil {
				return errors.Wrapf(err, "decoding info key (%q)", infoKey)
			}
			lastTaskID = taskRef.Task
			return nil
		}); err != nil {
		return 0, errors.Wrap(err, "finding last task completion marker")
	}

	return lastTaskID, nil
}

// markTaskCompletes transactionally removes the task's start marker
// and creates a completion marker.
func markTaskComplete(
	ctx context.Context, txn isql.Txn, taskRef InfoKeyTaskRef, completionValue []byte,
) error {
	infoStorage := jobs.InfoStorageForJob(txn, jobs.AutoConfigRunnerJobID)

	// Remove the start marker.
	if err := infoStorage.Delete(ctx, taskRef.EncodeStartMarkerKey()); err != nil {
		return err
	}

	// Remove any previous completion marker. This avoids the
	// accumulation of past completion markers over time.
	completionKeyPrefix := InfoKeyCompletionPrefix(taskRef.Environment)
	completionInfoKey := taskRef.EncodeCompletionMarkerKey()
	if err := infoStorage.DeleteRange(ctx, completionKeyPrefix, completionInfoKey); err != nil {
		return err
	}

	// Add our completion marker.
	return infoStorage.Write(ctx, completionInfoKey, completionValue)
}
