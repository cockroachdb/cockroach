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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStandbyReadTSPollerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.EnableReaderTenant = true
	args.TestingKnobs = &sql.StreamingTestingKnobs{
		StandbyPollInterval: time.Second,
	}
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	registry := c.DestSysServer.JobRegistry().(*jobs.Registry)
	jobRecord := makeStandbyReadTSPollerJobRecord(registry)
	sqlDB := c.DestSysServer.InternalDB().(isql.DB)

	stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
	readerTenantID := stats.IngestionDetails.ReadTenantID
	require.NotNil(t, readerTenantID)

	err := sqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := registry.CreateJobWithTxn(ctx, jobRecord, jobRecord.JobID, txn)
		return err
	})

	require.NoError(t, err)
	jobutils.WaitForJobToRun(t, c.DestSysSQL, jobRecord.JobID)

	c.SrcSysSQL.Exec(t, `
USE defaultdb;
CREATE TABLE a (i INT PRIMARY KEY);
INSERT INTO a VALUES (1);
`)

	// Open reader tenant SQL connection
	readerTenantName := fmt.Sprintf("%s-readonly", args.DestTenantName)
	readerTenantConn := c.DestCluster.Server(0).SystemLayer().SQLConn(c.T,
		serverutils.DBName("cluster:"+readerTenantName+"/defaultdb"))
	readerTenantSQL := sqlutils.MakeSQLRunner(readerTenantConn)
	testutils.SucceedsSoon(c.T, func() error { return readerTenantConn.Ping() })

	// Verify that updates have been replicated to reader tenant
	waitForReaderTenantUpdate(t, readerTenantSQL, 1)
}

func waitForReaderTenantUpdate(t *testing.T, readerTenantSQL *sqlutils.SQLRunner, expectedRow int) {
	testutils.SucceedsSoon(t, func() error {
		var actualRow int
		readerTenantSQL.QueryRow(t, `SELECT * FROM a`).Scan(&actualRow)
		if actualRow != expectedRow {
			return errors.Newf("expected %d in reader tenant, received %d instead", expectedRow, actualRow)
		}
		return nil
	})
}

func makeStandbyReadTSPollerJobRecord(registry *jobs.Registry) jobs.Record {
	return jobs.Record{
		JobID:         registry.MakeJobID(),
		Description:   "standby read-only timestamp poller job",
		Username:      username.MakeSQLUsernameFromPreNormalizedString("user"),
		Details:       jobspb.StandbyReadTSPollerDetails{},
		Progress:      jobspb.StandbyReadTSPollerProgress{},
		NonCancelable: true,
	}
}
