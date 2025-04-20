// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// resumeJob implements OperationCleanup and resumes a paused job by job ID.
type resumeJob struct {
	jobId string
}

func (r *resumeJob) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("resuming job %s", r.jobId))
	resumeJobStmt := fmt.Sprintf("RESUME JOB %s", r.jobId)
	_, err := conn.ExecContext(ctx, resumeJobStmt)
	if err != nil {
		o.Fatal(err)
	}
}

// runPauseJob returns a registry.OperationCleanup function that pauses a running job of the given type.
func runPauseJob(
	jobType string,
) func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
		defer conn.Close()

		// Query for running jobs of the specified type
		query := `
			WITH jobs AS (SHOW JOBS)
			SELECT job_id FROM jobs
			WHERE job_type = $1 AND status = 'running'
		`
		rows, err := conn.QueryContext(ctx, query, jobType)
		if err != nil {
			o.Fatal(err)
		}

		var jobIds []string
		for rows.Next() {
			var jobId string
			if err := rows.Scan(&jobId); err != nil {
				o.Fatal(err)
			}
			jobIds = append(jobIds, jobId)
		}

		if len(jobIds) == 0 {
			o.Fatal(fmt.Sprintf("no running %s jobs found", jobType))
		}

		// Randomly pick a job to pause
		rng, _ := randutil.NewPseudoRand()
		jobId := jobIds[rng.Intn(len(jobIds))]

		o.Status(fmt.Sprintf("pausing %s job %s", jobType, jobId))
		pauseStmt := fmt.Sprintf("PAUSE JOB %s WITH REASON = 'roachtest operation'", jobId)
		if _, err := conn.ExecContext(ctx, pauseStmt); err != nil {
			o.Fatal(err)
		}

		o.Status(fmt.Sprintf("paused %s job %s", jobType, jobId))

		return &resumeJob{jobId: jobId}
	}
}

// registerPauseJob registers pause-job operations for various job types (e.g., BACKUP, RESTORE, LOGICAL REPLICATION).
func registerPauseJob(r registry.Registry) {
	jobTypes := []struct {
		JobType     string
		OpName      string
		Dependency  registry.OperationDependency
		Owner       registry.Owner
		Description string
	}{
		{
			JobType:    "LOGICAL REPLICATION",
			OpName:     "pause-job/logical-replication",
			Dependency: registry.OperationRequiresLDRJobRunning,
			Owner:      registry.OwnerDisasterRecovery,
		},
		{
			JobType:    "BACKUP",
			OpName:     "pause-job/backup",
			Dependency: registry.OperationRequiresRunningBackupJob,
			Owner:      registry.OwnerDisasterRecovery,
		},
		{
			JobType:    "RESTORE",
			OpName:     "pause-job/restore",
			Dependency: registry.OperationRequiresRunningRestoreJob,
			Owner:      registry.OwnerDisasterRecovery,
		},
	}

	for _, jt := range jobTypes {
		r.AddOperation(registry.OperationSpec{
			Name:               jt.OpName,
			Owner:              jt.Owner,
			Timeout:            15 * time.Minute,
			CompatibleClouds:   registry.AllClouds,
			Dependencies:       []registry.OperationDependency{jt.Dependency},
			Run:                runPauseJob(jt.JobType),
			CanRunConcurrently: registry.OperationCanRunConcurrently,
		})
	}
}
