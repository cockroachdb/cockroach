// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

// TestChangefeedCheckpointLagClusterMetricsPersistence checks that
// changefeed.checkpoint_lag rows in system.cluster_metrics match running
// changefeeds (job_id and metrics scope), survive PAUSE, follow ALTER
// CHANGEFEED metrics_label changes, and are removed when jobs terminate
// (canceled or successful completion such as initial_scan = 'only').
func TestChangefeedCheckpointLagClusterMetricsPersistence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, stopServer := makeServer(t, feedTestNoTenants)
	defer stopServer()

	ctx := context.Background()
	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	clusterMetricsWriter := s.Server.SQLServerInternal().(*server.SQLServer).ClusterMetricsWriter()

	type checkpointLagMetric struct {
		jobID string
		scope string
	}
	checkpointLagMetrics := func() []checkpointLagMetric {
		rows := sqlDB.QueryStr(t, `SELECT labels->>'job_id', labels->>'scope' FROM system.cluster_metrics WHERE name = 'changefeed.checkpoint_lag' ORDER BY 1, 2`)
		metrics := make([]checkpointLagMetric, 0, len(rows))
		for _, row := range rows {
			metrics = append(metrics, checkpointLagMetric{jobID: row[0], scope: row[1]})
		}
		return metrics
	}
	expectCheckpointLagMetrics := func(want ...checkpointLagMetric) {
		t.Helper()
		sort.Slice(want, func(i, j int) bool {
			return want[i].jobID < want[j].jobID ||
				(want[i].jobID == want[j].jobID && want[i].scope < want[j].scope)
		})

		testutils.SucceedsSoon(t, func() error {
			clusterMetricsWriter.Flush(ctx)
			got := checkpointLagMetrics()
			if slices.Equal(got, want) {
				return nil
			}
			return errors.Errorf("expected checkpoint-lag metrics %v, got %v", want, got)
		})
	}
	checkpointLag := func(jobID jobspb.JobID, scope string) checkpointLagMetric {
		return checkpointLagMetric{
			jobID: strconv.FormatInt(int64(jobID), 10),
			scope: scope,
		}
	}

	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

	var feedA, feedB jobspb.JobID
	sqlDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'null://' WITH no_initial_scan, resolved = '10ms', min_checkpoint_frequency = '1ns'`).Scan(&feedA)
	sqlDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'null://' WITH no_initial_scan, resolved = '10ms', min_checkpoint_frequency = '1ns', metrics_label = 'Scope_B'`).Scan(&feedB)
	expectCheckpointLagMetrics(
		checkpointLag(feedA, defaultSLIScope),
		checkpointLag(feedB, "scope_b"),
	)

	sqlDB.Exec(t, `PAUSE JOB $1`, feedA)
	waitForJobState(sqlDB, t, feedA, jobs.StatePaused)
	expectCheckpointLagMetrics(
		checkpointLag(feedA, defaultSLIScope),
		checkpointLag(feedB, "scope_b"),
	)

	sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET metrics_label = 'Altered_Scope'`, feedA))
	expectCheckpointLagMetrics(checkpointLag(feedB, "scope_b"))

	sqlDB.Exec(t, `RESUME JOB $1`, feedA)
	waitForJobState(sqlDB, t, feedA, jobs.StateRunning)
	expectCheckpointLagMetrics(
		checkpointLag(feedA, "altered_scope"),
		checkpointLag(feedB, "scope_b"),
	)

	sqlDB.Exec(t, `CANCEL JOB $1`, feedA)
	sqlDB.Exec(t, `CANCEL JOB $1`, feedB)
	waitForJobState(sqlDB, t, feedA, jobs.StateCanceled)
	waitForJobState(sqlDB, t, feedB, jobs.StateCanceled)
	expectCheckpointLagMetrics()

	var scanOnlyFeed jobspb.JobID
	sqlDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'null://' WITH initial_scan = 'only'`).Scan(&scanOnlyFeed)
	waitForJobState(sqlDB, t, scanOnlyFeed, jobs.StateSucceeded)
	expectCheckpointLagMetrics()

}
