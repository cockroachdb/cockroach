// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestStandbyRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "slow test")

	testcases := []struct {
		standby  bool
		stmt     string
		expected [][]string
	}{
		{stmt: `CREATE TABLE abc (a INT PRIMARY KEY, b INT, c JSONB)`},
		{stmt: `ALTER TABLE abc CONFIGURE ZONE USING range_max_bytes = 64 << 20, range_min_bytes = 1 << 20`},
		// Insert 2m rows, which should be split into two ranges.
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0000000, 0099999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0100000, 0199999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0200000, 0299999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0300000, 0399999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0400000, 0499999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0500000, 0599999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0600000, 0699999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0700000, 0799999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0800000, 0899999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(0900000, 0999999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1000000, 1099999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1100000, 1199999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1200000, 1299999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1300000, 1399999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1400000, 1499999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1500000, 1599999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1600000, 1699999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1700000, 1799999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1800000, 1899999) AS s (i)`},
		{stmt: `INSERT INTO abc SELECT i, 1, to_json(i) FROM generate_series(1900000, 1999999) AS s (i)`},
		{stmt: `SELECT count(*) FROM [SHOW TABLES]`, expected: [][]string{{"1"}}},
		{stmt: `SELECT count(*) FROM abc`, expected: [][]string{{"2000000"}}},
		{standby: true, stmt: `SELECT count(*) FROM [SHOW TABLES]`, expected: [][]string{{"1"}}},
		{standby: true, stmt: `SELECT count(*) FROM abc`, expected: [][]string{{"2000000"}}},
	}

	beginTS := timeutil.Now()
	ctx := context.Background()
	args := replicationtestutils.DefaultTenantStreamingClustersArgs
	args.EnableReaderTenant = true
	c, cleanup := replicationtestutils.CreateTenantStreamingClusters(ctx, t, args)
	defer cleanup()

	producerJobID, ingestionJobID := c.StartStreamReplication(ctx)

	jobutils.WaitForJobToRun(c.T, c.SrcSysSQL, jobspb.JobID(producerJobID))
	jobutils.WaitForJobToRun(c.T, c.DestSysSQL, jobspb.JobID(ingestionJobID))
	t.Logf("test setup took %s", timeutil.Since(beginTS))

	srcTime := c.SrcCluster.Server(0).Clock().Now()
	c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))

	stats := replicationutils.TestingGetStreamIngestionStatsFromReplicationJob(t, ctx, c.DestSysSQL, ingestionJobID)
	readerTenantID := stats.IngestionDetails.ReadTenantID
	require.NotNil(t, readerTenantID)

	readerTenantName := fmt.Sprintf("%s-readonly", args.DestTenantName)
	c.ConnectToReaderTenant(ctx, readerTenantID, readerTenantName, 0)

	waitForPollerJobToStart(t, c, ingestionJobID)

	for _, tc := range testcases {
		var runner *sqlutils.SQLRunner
		if tc.standby {
			srcTime = c.SrcCluster.Server(0).Clock().Now()
			c.WaitUntilReplicatedTime(srcTime, jobspb.JobID(ingestionJobID))
			runner = c.ReaderTenantSQL
		} else {
			runner = c.SrcTenantSQL
		}
		if tc.expected == nil {
			runner.Exec(t, tc.stmt)
		} else {
			runner.CheckQueryResultsRetry(t, tc.stmt, tc.expected)
		}
	}
}
