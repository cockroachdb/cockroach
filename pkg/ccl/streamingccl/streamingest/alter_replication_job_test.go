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
	args := defaultTenantStreamingClustersArgs

	c, cleanup := createTenantStreamingClusters(ctx, t, args)
	defer cleanup()
	producerJobID, ingestionJobID := c.startStreamReplication()

	jobutils.WaitForJobToRun(c.t, c.srcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	c.waitUntilHighWatermark(c.srcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))

	// Pause the replication job.
	c.destSysSQL.Exec(t, `ALTER TENANT $1 PAUSE REPLICATION`, args.destTenantName)
	jobutils.WaitForJobToPause(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	// Unpause the replication job.
	c.destSysSQL.Exec(t, `ALTER TENANT $1 RESUME REPLICATION`, args.destTenantName)
	jobutils.WaitForJobToRun(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))

	c.waitUntilHighWatermark(c.srcCluster.Server(0).Clock().Now(), jobspb.JobID(ingestionJobID))
	var cutoverTime time.Time
	c.destSysSQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&cutoverTime)

	c.destSysSQL.Exec(c.t, `ALTER TENANT $1 COMPLETE REPLICATION TO SYSTEM TIME $2::string`, args.destTenantName, cutoverTime)
	jobutils.WaitForJobToSucceed(c.t, c.destSysSQL, jobspb.JobID(ingestionJobID))
	cleanupTenant := c.createDestTenantSQL(ctx)
	defer func() {
		require.NoError(t, cleanupTenant())
	}()

	t.Run("pause-nonexistant-tenant", func(t *testing.T) {
		c.destSysSQL.ExpectErr(t, "tenant \"nonexistent\" does not exist", `ALTER TENANT $1 PAUSE REPLICATION`, "nonexistent")
	})

	t.Run("pause-resume-tenant-with-no-replication", func(t *testing.T) {
		c.destSysSQL.Exec(t, `CREATE TENANT noreplication`)
		c.destSysSQL.ExpectErr(t, "tenant \"noreplication\" does not have an active replication job",
			`ALTER TENANT $1 PAUSE REPLICATION`, "noreplication")
		c.destSysSQL.ExpectErr(t, "tenant \"noreplication\" does not have an active replication job",
			`ALTER TENANT $1 RESUME REPLICATION`, "noreplication")
	})

	t.Run("pause-resume-in-readonly-txn", func(t *testing.T) {
		c.destSysSQL.Exec(t, `set default_transaction_read_only = on;`)
		c.destSysSQL.ExpectErr(t, "cannot execute ALTER TENANT REPLICATION in a read-only transaction", `ALTER TENANT $1 PAUSE REPLICATION`, "foo")
		c.destSysSQL.ExpectErr(t, "cannot execute ALTER TENANT REPLICATION in a read-only transaction", `ALTER TENANT $1 RESUME REPLICATION`, "foo")
		c.destSysSQL.Exec(t, `set default_transaction_read_only = off;`)
	})

	t.Run("pause-resume-as-non-system-tenant", func(t *testing.T) {
		c.destTenantSQL.ExpectErr(t, "only the system tenant can alter tenant", `ALTER TENANT $1 PAUSE REPLICATION`, "foo")
		c.destTenantSQL.ExpectErr(t, "only the system tenant can alter tenant", `ALTER TENANT $1 RESUME REPLICATION`, "foo")
	})
}
