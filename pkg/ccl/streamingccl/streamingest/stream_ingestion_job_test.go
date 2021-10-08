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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTenantStreaming tests that tenants can stream changes end-to-end.
func TestTenantStreaming(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "slow under race")

	ctx := context.Background()

	args := base.TestServerArgs{Knobs: base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	}

	// Start the source server.
	source, sourceDB, _ := serverutils.StartServer(t, args)
	defer source.Stopper().Stop(ctx)

	// Start tenant server in the srouce cluster.
	tenantID := serverutils.TestTenantID()
	_, tenantConn := serverutils.StartTenant(t, source, base.TestTenantArgs{TenantID: tenantID})
	defer tenantConn.Close()
	// sourceSQL refers to the tenant generating the data.
	sourceSQL := sqlutils.MakeSQLRunner(tenantConn)

	// Make changefeeds run faster.
	resetFreq := changefeedbase.TestingSetDefaultFlushFrequency(50 * time.Millisecond)
	defer resetFreq()
	// Set required cluster settings.
	_, err := sourceDB.Exec(`
SET CLUSTER SETTING kv.rangefeed.enabled = true;
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';
SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'
`)
	require.NoError(t, err)

	// Prevent a logging assertion that the server ID is initialized multiple times.
	log.TestingClearServerIdentifiers()

	// Start the destination server.
	hDest, cleanupDest := streamingtest.NewReplicationHelper(t)
	defer cleanupDest()
	// destSQL refers to the system tenant as that's the one that's running the
	// job.
	destSQL := hDest.SysDB
	destSQL.Exec(t, `
SET CLUSTER SETTING bulkio.stream_ingestion.minimum_flush_interval = '5us';
SET CLUSTER SETTING bulkio.stream_ingestion.cutover_signal_poll_interval = '100ms';
SET enable_experimental_stream_replication = true;
`)

	// Sink to read data from.
	pgURL, cleanupSink := sqlutils.PGUrl(t, source.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanupSink()

	var ingestionJobID int
	var startTime string
	sourceSQL.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&startTime)
	destSQL.QueryRow(t,
		`RESTORE TENANT 10 FROM REPLICATION STREAM FROM $1 AS OF SYSTEM TIME `+startTime,
		pgURL.String(),
	).Scan(&ingestionJobID)

	sourceSQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
CREATE TABLE d.t2(i int primary key);
INSERT INTO d.t1 (i) VALUES (42);
INSERT INTO d.t2 VALUES (2);
`)

	// Pick a cutover time, then wait for the job to reach that time.
	cutoverTime := timeutil.Now().Round(time.Microsecond)
	testutils.SucceedsSoon(t, func() error {
		progress := jobutils.GetJobProgress(t, destSQL, jobspb.JobID(ingestionJobID))
		if progress.GetHighWater() == nil {
			return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
				cutoverTime.String())
		}
		highwater := timeutil.Unix(0, progress.GetHighWater().WallTime)
		if highwater.Before(cutoverTime) {
			return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
				highwater.String(), cutoverTime.String())
		}
		return nil
	})

	destSQL.Exec(
		t,
		`SELECT crdb_internal.complete_stream_ingestion_job($1, $2)`,
		ingestionJobID, cutoverTime)

	jobutils.WaitForJob(t, destSQL, jobspb.JobID(ingestionJobID))

	query := "SELECT * FROM d.t1"
	sourceData := sourceSQL.QueryStr(t, query)
	destData := hDest.Tenant.SQL.QueryStr(t, query)
	require.Equal(t, sourceData, destData)
}
