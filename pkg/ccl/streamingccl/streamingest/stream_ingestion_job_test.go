// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingtest"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamproducer"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestStreamIngestionJobRollBack tests that the job rolls back the data to the
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
	j, err := jobs.TestingCreateAndStartJob(ctx, registry, tc.Server(0).DB(), streamIngestJobRecord)
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

// TestTenantStreaming tests that tenants can stream changes end-to-end.
func TestTenantStreaming(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Start server
	source, sourceDB, _ := serverutils.StartServer(t, base.TestServerArgs{})

	// Make changefeeds run faster.
	resetFreq := changefeedbase.TestingSetDefaultFlushFrequency(50 * time.Millisecond)

	// Set required cluster settings.
	_, err := sourceDB.Exec(`
SET CLUSTER SETTING kv.rangefeed.enabled = true;
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';
SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'
`)
	require.NoError(t, err)

	defer func() {
		resetFreq()
		source.Stopper().Stop(ctx)
	}()

	hDest, cleanupDest := streamingtest.NewReplicationHelper(t)
	defer cleanupDest()

	// Prevent a logging assertion that the server ID is initialized multiple times.
	log.TestingClearServerIdentifiers()

	// Sink to read data from.
	pgURL, cleanupSink := sqlutils.PGUrl(t, source.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupSink()

	// Start tenant server
	tenantID := roachpb.MakeTenantID(10)
	_, tenantConn := serverutils.StartTenant(t, source, base.TestTenantArgs{TenantID: tenantID})
	defer tenantConn.Close()

	var jobID int
	hDest.SysDB.Exec(t, `SET enable_experimental_stream_replication = true`)
	hDest.SysDB.QueryRow(t, fmt.Sprintf(`RESTORE TENANT 10 FROM REPLICATION STREAM FROM '%s'`,
		pgURL.String())).Scan(&jobID)
	defer hDest.SysDB.Exec(t, fmt.Sprintf(`CANCEL JOB %d`, jobID))
	// TODO: Get rid of these sleeps, after rebasing on the AOST branch.
	time.Sleep(5 * time.Second)

	sourceSQL := sqlutils.MakeSQLRunner(tenantConn)
	sourceSQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
CREATE TABLE d.t2(i int primary key);
INSERT INTO d.t1 (i) VALUES (42);
INSERT INTO d.t2 VALUES (2);
`)

	// TODO: Remove this time.Sleep when we can cutover in the future.
	time.Sleep(5 * time.Second)
	// TODO(pbardea): Cutover the job here and wait for it to succeed when
	// rebased on cutover changes.

	query := "SELECT * FROM d.t1"
	sourceData := sourceSQL.QueryStr(t, query)
	destData := hDest.Tenant.SQL.QueryStr(t, query)
	require.Equal(t, sourceData, destData)
}
