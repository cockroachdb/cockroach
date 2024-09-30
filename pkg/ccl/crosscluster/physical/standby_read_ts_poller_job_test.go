// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package physical

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStandbyReadTSPollerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 131611)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.EnableReaderTenant = true
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
	readerTenantID := stats.IngestionDetails.ReadTenantID
	require.NotNil(t, readerTenantID)

	readerTenantName := fmt.Sprintf("%s-readonly", args.DestTenantName)
	c.ConnectToReaderTenant(ctx, readerTenantID, readerTenantName, 0)

	pollInterval := time.Microsecond
	serverutils.SetClusterSetting(t, c.DestCluster, "bulkio.stream_ingestion.standby_read_ts_poll_interval", pollInterval)

	var jobID jobspb.JobID
	c.ReaderTenantSQL.QueryRow(t, `
SELECT job_id 
FROM crdb_internal.jobs 
WHERE job_type = 'STANDBY READ TS POLLER'
`).Scan(&jobID)
	jobutils.WaitForJobToRun(t, c.ReaderTenantSQL, jobID)

	c.SrcTenantSQL.Exec(t, `
USE defaultdb;
CREATE TABLE a (i INT PRIMARY KEY);
INSERT INTO a VALUES (1);
`)

	// Verify that updates have been replicated to reader tenant
	testutils.SucceedsWithin(t, func() error {
		var numTables int
		c.ReaderTenantSQL.QueryRow(t, `SELECT count(*) FROM [SHOW TABLES]`).Scan(&numTables)

		if numTables != 1 {
			return errors.Errorf("expected 1 table to be present in reader tenant, but got %d instead", numTables)
		}

		var actualQueryResult int
		c.ReaderTenantSQL.QueryRow(t, `SELECT * FROM a`).Scan(&actualQueryResult)
		if actualQueryResult != 1 {
			return errors.Newf("expected %d to be replicated to table {a} in reader tenant, received %d instead",
				1, actualQueryResult)
		}
		return nil
	}, time.Minute)
}
