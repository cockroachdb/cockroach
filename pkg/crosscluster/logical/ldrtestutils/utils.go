// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrtestutils

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// WaitUntilReplicatedTime waits for a logical replication job to reach the target replicated time.
func WaitUntilReplicatedTime(
	t *testing.T, targetTime hlc.Timestamp, db *sqlutils.SQLRunner, jobID jobspb.JobID,
) {
	t.Logf("waiting for logical replication job %d to reach replicated time of %s", jobID, targetTime)
	testutils.SucceedsSoon(t, func() error {
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
