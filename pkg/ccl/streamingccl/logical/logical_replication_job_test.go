// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var (
	testClusterSettings = []string{
		"SET CLUSTER SETTING kv.rangefeed.enabled = true",
		"SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '200ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'",

		// TODO(ssd): Duplicate these over to logical_replication as well.
		"SET CLUSTER SETTING physical_replication.producer.min_checkpoint_frequency='100ms'",
		"SET CLUSTER SETTING physical_replication.consumer.heartbeat_frequency = '1s'",

		"SET CLUSTER SETTING logical_replication.consumer.job_checkpoint_frequency = '100ms'",
		"SET CLUSTER SETTING logical_replication.consumer.minimum_flush_interval = '10ms'",
		"SET CLUSTER SETTING logical_replication.consumer.timestamp_granularity = '100ms'",
	}
	lwwColumnAdd = "ALTER TABLE tab ADD COLUMN crdb_internal_origin_timestamp DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL"
)

func TestLogicalStreamIngestionJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	serverA := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer serverA.Stopper().Stop(ctx)

	serverB := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer serverB.Stopper().Stop(ctx)

	serverASQL := sqlutils.MakeSQLRunner(serverA.Server(0).ApplicationLayer().SQLConn(t))
	serverBSQL := sqlutils.MakeSQLRunner(serverB.Server(0).ApplicationLayer().SQLConn(t))

	for _, s := range testClusterSettings {
		serverASQL.Exec(t, s)
		serverBSQL.Exec(t, s)
	}

	createStmt := "CREATE TABLE tab (pk int primary key, payload string)"
	serverASQL.Exec(t, createStmt)
	serverBSQL.Exec(t, createStmt)
	serverASQL.Exec(t, lwwColumnAdd)
	serverBSQL.Exec(t, lwwColumnAdd)

	serverASQL.Exec(t, "INSERT INTO tab VALUES (1, 'hello')")
	serverBSQL.Exec(t, "INSERT INTO tab VALUES (1, 'goodbye')")

	serverAURL, cleanup := sqlutils.PGUrl(t, serverA.Server(0).ApplicationLayer().SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	serverBURL, cleanupB := sqlutils.PGUrl(t, serverB.Server(0).ApplicationLayer().SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanupB()

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)
	serverASQL.QueryRow(t, fmt.Sprintf("SELECT crdb_internal.start_logical_replication_job('%s', %s)", serverBURL.String(), `ARRAY['tab']`)).Scan(&jobAID)
	serverBSQL.QueryRow(t, fmt.Sprintf("SELECT crdb_internal.start_logical_replication_job('%s', %s)", serverAURL.String(), `ARRAY['tab']`)).Scan(&jobBID)

	now := serverA.Server(0).Clock().Now()
	t.Logf("waiting for replication job %d", jobAID)
	WaitUntilReplicatedTime(t, now, serverASQL, jobAID)
	t.Logf("waiting for replication job %d", jobBID)
	WaitUntilReplicatedTime(t, now, serverBSQL, jobBID)

	serverASQL.Exec(t, "INSERT INTO tab VALUES (2, 'potato')")
	serverBSQL.Exec(t, "INSERT INTO tab VALUES (3, 'celeriac')")
	serverASQL.Exec(t, "UPSERT INTO tab VALUES (1, 'hello, again')")
	serverBSQL.Exec(t, "UPSERT INTO tab VALUES (1, 'goodbye, again')")

	now = serverA.Server(0).Clock().Now()
	WaitUntilReplicatedTime(t, now, serverASQL, jobAID)
	WaitUntilReplicatedTime(t, now, serverBSQL, jobBID)

	expectedRows := [][]string{
		{"1", "goodbye, again"},
		{"2", "potato"},
		{"3", "celeriac"},
	}
	serverBSQL.CheckQueryResults(t, "SELECT * from tab", expectedRows)
	serverASQL.CheckQueryResults(t, "SELECT * from tab", expectedRows)
}

func TestLogicalStreamIngestionJobWithColumnFamilies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	serverA := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer serverA.Stopper().Stop(ctx)

	serverB := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer serverB.Stopper().Stop(ctx)

	serverASQL := sqlutils.MakeSQLRunner(serverA.Server(0).ApplicationLayer().SQLConn(t))
	serverBSQL := sqlutils.MakeSQLRunner(serverB.Server(0).ApplicationLayer().SQLConn(t))

	for _, s := range testClusterSettings {
		serverASQL.Exec(t, s)
		serverBSQL.Exec(t, s)
	}

	createStmt := `CREATE TABLE tab (
pk int primary key,
payload string,
v1 int as (pk + 9000) virtual,
v2 int as (pk + 42) stored,
other_payload string,
family f1(pk, payload),
family f2(other_payload, v2))
`
	serverASQL.Exec(t, createStmt)
	serverBSQL.Exec(t, createStmt)
	serverASQL.Exec(t, lwwColumnAdd)
	serverBSQL.Exec(t, lwwColumnAdd)

	serverASQL.Exec(t, "INSERT INTO tab(pk, payload, other_payload) VALUES (1, 'hello', 'ruroh1')")

	serverAURL, cleanup := sqlutils.PGUrl(t, serverA.Server(0).ApplicationLayer().SQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()

	var jobBID jobspb.JobID
	serverBSQL.QueryRow(t, fmt.Sprintf("SELECT crdb_internal.start_logical_replication_job('%s', %s)", serverAURL.String(), `ARRAY['tab']`)).Scan(&jobBID)

	WaitUntilReplicatedTime(t, serverA.Server(0).Clock().Now(), serverBSQL, jobBID)
	serverASQL.Exec(t, "INSERT INTO tab(pk, payload, other_payload) VALUES (2, 'potato', 'ruroh2')")
	serverASQL.Exec(t, "UPSERT INTO tab(pk, payload, other_payload) VALUES (1, 'hello, again', 'ruroh3')")

	WaitUntilReplicatedTime(t, serverA.Server(0).Clock().Now(), serverBSQL, jobBID)

	expectedRows := [][]string{
		{"1", "hello, again", "9001", "43", "ruroh3"},
		{"2", "potato", "9002", "44", "ruroh2"},
	}
	serverBSQL.CheckQueryResults(t, "SELECT * from tab", expectedRows)
	serverASQL.CheckQueryResults(t, "SELECT * from tab", expectedRows)
}

func WaitUntilReplicatedTime(
	t *testing.T, targetTime hlc.Timestamp, db *sqlutils.SQLRunner, ingestionJobID jobspb.JobID,
) {
	testutils.SucceedsSoon(t, func() error {
		progress := jobutils.GetJobProgress(t, db, ingestionJobID)
		replicatedTime := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime
		if replicatedTime.IsEmpty() {
			return errors.Newf("stream ingestion has not recorded any progress yet, waiting to advance pos %s",
				targetTime)
		}
		if replicatedTime.Less(targetTime) {
			return errors.Newf("waiting for stream ingestion job progress %s to advance beyond %s",
				replicatedTime, targetTime)
		}
		return nil
	})
}
