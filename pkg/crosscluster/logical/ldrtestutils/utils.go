// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrtestutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// WaitUntilReplicatedTime waits for a logical replication job to reach the target replicated time.
// The test will fail immediately if the job enters a paused or failed state.
func WaitUntilReplicatedTime(
	t *testing.T, targetTime hlc.Timestamp, db *sqlutils.SQLRunner, jobID jobspb.JobID,
) {
	t.Logf("waiting for logical replication job %d to reach replicated time of %s", jobID, targetTime)
	testutils.SucceedsSoon(t, func() error {
		// Check job status first to fail fast if the job is paused or failed
		var status string
		db.QueryRow(t, "SELECT status FROM system.jobs WHERE id = $1", jobID).Scan(&status)
		if jobs.State(status) == jobs.StatePaused {
			payload := jobutils.GetJobPayload(t, db, jobID)
			t.Fatalf("logical replication job %d is paused: %s", jobID, payload.Error)
		}
		if jobs.State(status) == jobs.StateFailed {
			payload := jobutils.GetJobPayload(t, db, jobID)
			t.Fatalf("logical replication job %d failed: %s", jobID, payload.Error)
		}

		progress := jobutils.GetJobProgress(t, db, jobID)
		replicatedTime := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime
		if replicatedTime.IsEmpty() {
			return errors.Newf("logical replication has not recorded any progress yet, waiting to advance past %s", targetTime)
		}
		if replicatedTime.Less(targetTime) {
			return errors.Newf("logical replication job %d at %s, waiting to advance past %s", jobID, replicatedTime, targetTime)
		}
		return nil
	})
}
