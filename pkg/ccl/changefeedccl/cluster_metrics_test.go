// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestClusterMetrics_SetAndDelete verifies that the per-job
// SetCheckpointLag and DeleteJob helpers correctly route through the
// underlying cmmetrics.GaugeVec: SetCheckpointLag adds (or updates) a
// child keyed by job_id and scope, DeleteJob removes the matching child, and
// entries for distinct jobs or scopes are isolated from each other.
func TestClusterMetricsCheckpointLag_SetAndDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Cluster metric constructors panic in test builds when called
	// outside an init() function
	defer cmmetrics.TestingAllowNonInitConstruction()()

	cm := &ClusterMetrics{
		CheckpointLag: cmmetrics.NewWriteStopwatchVec(
			metaCheckpointLag, timeutil.DefaultTimeSource{}, "job_id", "scope",
		),
	}

	// childValue returns the stored value for the given job_id and scope, or -1.
	childValue := func(jobID, scope string) int64 {
		got := int64(-1)
		cm.CheckpointLag.Each(func(s cmmetrics.WritableMetric) {
			labels := s.GetLabels()
			if labels["job_id"] == jobID && labels["scope"] == scope {
				got = s.Value()
			}
		})
		return got
	}

	const (
		jobA = jobspb.JobID(42)
		jobB = jobspb.JobID(43)
	)

	require.Equal(t, int64(-1), childValue("42", defaultSLIScope))
	require.Equal(t, int64(-1), childValue("43", "reporting"))

	cm.SetCheckpointLag(jobA, "", 10)
	require.Equal(t, int64(10), childValue("42", defaultSLIScope))
	require.Equal(t, int64(-1), childValue("43", "reporting"))

	cm.SetCheckpointLag(jobB, " Reporting ", 20)
	require.Equal(t, int64(10), childValue("42", defaultSLIScope))
	require.Equal(t, int64(20), childValue("43", "reporting"))

	cm.SetCheckpointLag(jobA, "", 50)
	require.Equal(t, int64(50), childValue("42", defaultSLIScope))
	require.Equal(t, int64(20), childValue("43", "reporting"))

	cm.SetCheckpointLag(jobA, "moved", 70)
	require.Equal(t, int64(50), childValue("42", defaultSLIScope))
	require.Equal(t, int64(70), childValue("42", "moved"))

	cm.DeleteJob(jobA, "")
	require.Equal(t, int64(-1), childValue("42", defaultSLIScope))
	require.Equal(t, int64(70), childValue("42", "moved"))
	require.Equal(t, int64(20), childValue("43", "reporting"))

	cm.DeleteJob(jobA, "moved")
	require.Equal(t, int64(-1), childValue("42", "moved"))

	cm.DeleteJob(jobB, "REPORTING")
	require.Equal(t, int64(-1), childValue("43", "reporting"))
}
