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

type resumePausedJob struct {
	jobId string
}

func (r *resumePausedJob) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	resumeJobStmt := fmt.Sprintf("RESUME JOB %s", r.jobId)
	_, err := conn.ExecContext(ctx, resumeJobStmt)
	if err != nil {
		o.Fatal(err)
	}
}

func pauseLDRJob(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	//fetch running ldr jobs
	jobs, err := conn.QueryContext(ctx, "(WITH x AS (SHOW JOBS) SELECT job_id FROM x WHERE job_type = 'LOGICAL REPLICATION' AND status = 'running')")
	if err != nil {
		o.Fatal(err)
	}

	var jobIds []string
	for jobs.Next() {
		var jobId string
		if err := jobs.Scan(&jobId); err != nil {
			o.Fatal(err)
		}
		jobIds = append(jobIds, jobId)
	}

	//pick a random ldr job
	rng, _ := randutil.NewPseudoRand()
	jobId := jobIds[rng.Intn(len(jobIds))]

	o.Status(fmt.Sprintf("pausing LDR job %s", jobId))
	pauseJobStmt := fmt.Sprintf("PAUSE JOB %s WITH REASON = 'roachtest operation'", jobId)
	_, err = conn.ExecContext(ctx, pauseJobStmt)
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("paused LDR job %s", jobId))
	return &resumePausedJob{
		jobId: jobId,
	}
}

func registerPauseLDRJob(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:             "pause-ldr",
		Owner:            registry.OwnerDisasterRecovery,
		Timeout:          15 * time.Minute,
		CompatibleClouds: registry.AllClouds,
		Dependencies:     []registry.OperationDependency{registry.OperationRequiresLDRJobRunning},
		Run:              pauseLDRJob,
	})
}
