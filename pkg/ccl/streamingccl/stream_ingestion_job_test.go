// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestStreamIngestionJobRollBack tests that the job rollsback the data to the
// start time if there are no progress updates. This test should be expanded
// after the job's progress field is updated as the job runs.
func TestStreamIngestionJobRollBack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Load some initial data in the table. We're going to rollback to this time.
	sqlDB.Exec(t, `CREATE TABLE foo AS SELECT * FROM generate_series(0, 100);`)
	var tableID uint32
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'foo'`).Scan(&tableID)

	// Create the stream ingestion job.
	descTableKey := keys.SystemSQLCodec.TablePrefix(tableID)
	ingestionSpan := roachpb.Span{
		Key:    descTableKey,
		EndKey: descTableKey.PrefixEnd(),
	}
	startTimestamp := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	streamIngestJobRecord := jobs.Record{
		Description: "test stream ingestion",
		Username:    security.RootUserName(),
		Details: jobspb.StreamIngestionDetails{
			StreamAddress: "some://address/here",
			Span:          ingestionSpan,
			StartTime:     startTimestamp,
		},
		Progress: jobspb.StreamIngestionProgress{},
	}
	j, _, err := registry.CreateAndStartJob(ctx, nil, streamIngestJobRecord)
	require.NoError(t, err)

	// Insert more data in the table. These changes should be rollback during job
	// cancellation.
	sqlDB.CheckQueryResults(t, "SELECT count(*) FROM foo", [][]string{{"101"}})
	sqlDB.Exec(t, `INSERT INTO foo SELECT * FROM generate_series(100, 200);`)
	sqlDB.CheckQueryResults(t, "SELECT count(*) FROM foo", [][]string{{"202"}})

	// Cancel the job and expect the table to have the same number of rows as it
	// did at the start.
	sqlDB.Exec(t, "CANCEL JOB $1", j.ID())
	sqlDB.CheckQueryResultsRetry(t, "SELECT count(*) FROM foo", [][]string{{"101"}})
}
