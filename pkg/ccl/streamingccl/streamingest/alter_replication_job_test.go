// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestAlterTenantPauseResume(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs

	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(t, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilHighWatermark(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	// Pause the replication job.
	c.DestSysSQL.Exec(t, `ALTER TENANT $1 PAUSE REPLICATION`, args.DestTenantName)
	jobutils.WaitForJobToPause(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	// Unpause the replication job.
	c.DestSysSQL.Exec(t, `ALTER TENANT $1 RESUME REPLICATION`, args.DestTenantName)
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	c.WaitUntilHighWatermark(c.SrcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))
	var cutoverTime time.Time
	c.DestSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)

	var cutoverStr string
	c.DestSysSQL.QueryRow(c.T, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`,
		args.DestTenantName, cutoverTime).Scan(&cutoverStr)
	cutoverOutput := replicationtestutils.DecimalTimeToHLC(t, cutoverStr)
	require.Equal(t, cutoverTime, cutoverOutput.GoTime())
	jobutils.WaitForJobToSucceed(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	cleanupTenant := c.CreateDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanupTenant())
	}()

	t.Run("pause-nonexistant-tenant", func(t *testing.T) {
		c.DestSysSQL.ExpectErr(t, "tenant \"nonexistent\" does not exist", `ALTER TENANT $1 PAUSE REPLICATION`, "nonexistent")
	})

	t.Run("pause-resume-tenant-with-no-replication", func(t *testing.T) {
		c.DestSysSQL.Exec(t, `CREATE TENANT noreplication`)
		c.DestSysSQL.ExpectErr(t, `tenant "noreplication" \(3\) does not have an active replication job`,
			`ALTER TENANT $1 PAUSE REPLICATION`, "noreplication")
		c.DestSysSQL.ExpectErr(t, `tenant "noreplication" \(3\) does not have an active replication job`,
			`ALTER TENANT $1 RESUME REPLICATION`, "noreplication")
	})

	t.Run("pause-resume-in-readonly-txn", func(t *testing.T) {
		c.DestSysSQL.Exec(t, `set default_transaction_read_only = on;`)
		c.DestSysSQL.ExpectErr(t, "cannot execute ALTER TENANT REPLICATION in a read-only transaction", `ALTER TENANT $1 PAUSE REPLICATION`, "foo")
		c.DestSysSQL.ExpectErr(t, "cannot execute ALTER TENANT REPLICATION in a read-only transaction", `ALTER TENANT $1 RESUME REPLICATION`, "foo")
		c.DestSysSQL.Exec(t, `set default_transaction_read_only = off;`)
	})

	t.Run("pause-resume-as-non-system-tenant", func(t *testing.T) {
		c.DestTenantSQL.ExpectErr(t, "only the system tenant can alter tenant", `ALTER TENANT $1 PAUSE REPLICATION`, "foo")
		c.DestTenantSQL.ExpectErr(t, "only the system tenant can alter tenant", `ALTER TENANT $1 RESUME REPLICATION`, "foo")
	})
}
