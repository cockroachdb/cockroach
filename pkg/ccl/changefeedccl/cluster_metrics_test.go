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
// child keyed by job_id, DeleteJob removes it, and entries for distinct
// jobs are isolated from each other.
func TestClusterMetricsCheckpointLag_SetAndDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Cluster metric constructors panic in test builds when called
	// outside an init() function
	defer cmmetrics.TestingAllowNonInitConstruction()()

	cm := &ClusterMetrics{
		CheckpointLag: cmmetrics.NewWriteStopwatchVec(metaCheckpointLag, timeutil.DefaultTimeSource{}, "job_id"),
	}

	// childValue returns the stored value for the given job_id, or -1
	childValue := func(jobID string) int64 {
		got := int64(-1)
		cm.CheckpointLag.Each(func(s cmmetrics.WritableMetric) {
			if s.GetLabels()["job_id"] == jobID {
				got = s.Value()
			}
		})
		return got
	}

	const (
		jobA = jobspb.JobID(42)
		jobB = jobspb.JobID(43)
	)

	require.Equal(t, int64(-1), childValue("42"))
	require.Equal(t, int64(-1), childValue("43"))

	cm.SetCheckpointLag(jobA, 10)
	require.Equal(t, int64(10), childValue("42"))
	require.Equal(t, int64(-1), childValue("43"))

	cm.SetCheckpointLag(jobB, 20)
	require.Equal(t, int64(10), childValue("42"))
	require.Equal(t, int64(20), childValue("43"))

	cm.SetCheckpointLag(jobA, 50)
	require.Equal(t, int64(50), childValue("42"))
	require.Equal(t, int64(20), childValue("43"))

	cm.DeleteJob(jobA)
	require.Equal(t, int64(-1), childValue("42"))
	require.Equal(t, int64(20), childValue("43"))

	cm.DeleteJob(jobB)
	require.Equal(t, int64(-1), childValue("42"))
	require.Equal(t, int64(-1), childValue("43"))
}
