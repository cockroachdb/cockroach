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

func runCancelJob(
	jobType string,
) func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
		defer conn.Close()

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

		rng, _ := randutil.NewPseudoRand()
		jobId := jobIds[rng.Intn(len(jobIds))]

		o.Status(fmt.Sprintf("cancelling %s job %s", jobType, jobId))
		stmt := fmt.Sprintf("CANCEL JOB %s", jobId)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			o.Fatal(err)
		}

		o.Status(fmt.Sprintf("cancelled %s job %s", jobType, jobId))
		return nil // No cleanup needed after cancel
	}
}

func registerCancelJob(r registry.Registry) {
	jobs := []struct {
		JobType    string
		OpName     string
		Owner      registry.Owner
		Dependency registry.OperationDependency
	}{
		{"LOGICAL REPLICATION", "cancel-job/logical-replication", registry.OwnerDisasterRecovery, registry.OperationRequiresLDRJobRunning},
		{"BACKUP", "cancel-job/backup", registry.OwnerDisasterRecovery, registry.OperationRequiresRunningBackupJob},
		{"RESTORE", "cancel-job/restore", registry.OwnerDisasterRecovery, registry.OperationRequiresRunningRestoreJob},
	}

	for _, j := range jobs {
		r.AddOperation(registry.OperationSpec{
			Name:               j.OpName,
			Owner:              j.Owner,
			Timeout:            15 * time.Minute,
			CompatibleClouds:   registry.AllClouds,
			Dependencies:       []registry.OperationDependency{j.Dependency},
			Run:                runCancelJob(j.JobType),
			CanRunConcurrently: registry.OperationCanRunConcurrently,
		})
	}
}
