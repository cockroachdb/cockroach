// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/streamclient/randclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

var (
	testClusterSystemSettings = []string{
		"SET CLUSTER SETTING kv.rangefeed.enabled = true",
		"SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '200ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'",
	}
	testClusterSettings = []string{
		"SET CLUSTER SETTING physical_replication.producer.timestamp_granularity = '0s'",
		"SET CLUSTER SETTING physical_replication.producer.min_checkpoint_frequency='100ms'",
		"SET CLUSTER SETTING logical_replication.consumer.heartbeat_frequency = '1s'",
		"SET CLUSTER SETTING logical_replication.consumer.job_checkpoint_frequency = '100ms'",
	}

	testClusterBaseClusterArgs = base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(127241),
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	lwwColumnAdd = "ADD COLUMN crdb_replication_origin_timestamp DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL"
)

func TestLogicalStreamIngestionJobNameResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	type testCase struct {
		name                string
		setup               func(*testing.T, *sqlutils.SQLRunner, *sqlutils.SQLRunner)
		localStmtTableName  string
		remoteStmtTableName string
		expectedErr         string
	}
	cases := []testCase{
		{
			name: "table in schema",
			setup: func(t *testing.T, dbA *sqlutils.SQLRunner, dbB *sqlutils.SQLRunner) {
				dbA.Exec(t, `CREATE SCHEMA foo`)
				createBasicTable(t, dbA, "foo.bar")
				dbB.Exec(t, `CREATE SCHEMA foo`)
				createBasicTable(t, dbB, "foo.bar")
			},
			localStmtTableName:  "foo.bar",
			remoteStmtTableName: "foo.bar",
		},
		{
			name: "table in schema with special characters",
			setup: func(t *testing.T, dbA *sqlutils.SQLRunner, dbB *sqlutils.SQLRunner) {
				dbA.Exec(t, `CREATE SCHEMA "foo-bar"`)
				createBasicTable(t, dbA, `"foo-bar".bar`)
				dbB.Exec(t, `CREATE SCHEMA "foo-bar"`)
				createBasicTable(t, dbB, `"foo-bar".bar`)
			},
			localStmtTableName:  `"foo-bar".bar`,
			remoteStmtTableName: `"foo-bar".bar`,
		},
		{
			name: "table with special characters in schema with special characters",
			setup: func(t *testing.T, dbA *sqlutils.SQLRunner, dbB *sqlutils.SQLRunner) {
				dbA.Exec(t, `CREATE SCHEMA "foo-bar2"`)
				createBasicTable(t, dbA, `"foo-bar2"."baz-bat"`)
				dbB.Exec(t, `CREATE SCHEMA "foo-bar2"`)
				createBasicTable(t, dbB, `"foo-bar2"."baz-bat"`)
			},
			localStmtTableName:  `"foo-bar2"."baz-bat"`,
			remoteStmtTableName: `"foo-bar2"."baz-bat"`,
		},
		{
			name: "table with periods in schema with periods",
			setup: func(t *testing.T, dbA *sqlutils.SQLRunner, dbB *sqlutils.SQLRunner) {
				dbA.Exec(t, `CREATE SCHEMA "foo.bar"`)
				createBasicTable(t, dbA, `"foo.bar"."baz.bat"`)
				dbB.Exec(t, `CREATE SCHEMA "foo.bar"`)
				createBasicTable(t, dbB, `"foo.bar"."baz.bat"`)
			},
			localStmtTableName:  `"foo.bar"."baz.bat"`,
			remoteStmtTableName: `"foo.bar"."baz.bat"`,
		},
		{
			name: "table in public schema",
			setup: func(t *testing.T, dbA *sqlutils.SQLRunner, dbB *sqlutils.SQLRunner) {
				createBasicTable(t, dbA, "bar")
				createBasicTable(t, dbB, "bar")
			},
			localStmtTableName:  "public.bar",
			remoteStmtTableName: "public.bar",
		},
	}

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer server.Stopper().Stop(ctx)
	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.setup(t, dbA, dbB)
			query := fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLE %s ON $1 INTO TABLE %s",
				c.remoteStmtTableName, c.localStmtTableName)
			if c.expectedErr != "" {
				dbA.ExpectErr(t, c.expectedErr, query, dbBURL.String())
			} else {
				var unusedID int
				dbA.QueryRow(t, query, dbBURL.String()).Scan(&unusedID)
			}
		})
	}
}

type fatalDLQ struct{ *testing.T }

func (fatalDLQ) Create(ctx context.Context) error { return nil }

func (t fatalDLQ) Log(
	_ context.Context,
	_ int64,
	_ streampb.StreamEvent_KV,
	cdcEventRow cdcevent.Row,
	reason error,
	_ retryEligibility,
) error {
	t.Fatal(errors.Wrapf(reason, "failed to apply row update: %s", cdcEventRow.DebugString()))
	return nil
}

func TestLogicalStreamIngestionJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer TestingSetDLQ(fatalDLQ{t})()

	ctx := context.Background()
	// keyPrefix will be set later, but before countPuts is set.
	var keyPrefix []byte
	var countPuts atomic.Bool
	var numPuts, numCPuts atomic.Int64
	// seenPuts and seenCPuts track which transactions have already been counted
	// in the number of Puts and CPuts, respectively (we want to ignore any txn
	// retries).
	seenPuts, seenCPuts := make(map[uuid.UUID]struct{}), make(map[uuid.UUID]struct{})
	var muSeenTxns syncutil.Mutex
	// seenTxn returns whether we've already seen this txn and includes it into
	// the map if not.
	seenTxn := func(seenTxns map[uuid.UUID]struct{}, txnID uuid.UUID) bool {
		muSeenTxns.Lock()
		defer muSeenTxns.Unlock()
		_, seen := seenTxns[txnID]
		seenTxns[txnID] = struct{}{}
		return seen
	}
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
						if !countPuts.Load() || !ba.IsWrite() || len(ba.Requests) > 2 {
							return nil
						}
						switch req := ba.Requests[0].GetInner().(type) {
						case *kvpb.PutRequest:
							if bytes.HasPrefix(req.Key, keyPrefix) && !seenTxn(seenPuts, ba.Txn.ID) {
								numPuts.Add(1)
							}
							return nil
						case *kvpb.ConditionalPutRequest:
							if bytes.HasPrefix(req.Key, keyPrefix) && !seenTxn(seenCPuts, ba.Txn.ID) {
								numCPuts.Add(1)
							}
							return nil
						default:
							return nil
						}
					},
				},
			},
		},
	}

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, clusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	retryQueueSizeLimit.Override(ctx, &s.ClusterSettings().SV, 0)

	desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "a", "tab")
	keyPrefix = rowenc.MakeIndexKeyPrefix(s.Codec(), desc.GetID(), desc.GetPrimaryIndexID())
	countPuts.Store(true)

	dbA.Exec(t, "INSERT INTO tab VALUES (1, 'hello')")
	dbB.Exec(t, "INSERT INTO tab VALUES (1, 'goodbye')")

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()
	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	// Swap one of the URLs to external:// to verify this indirection works.
	// TODO(dt): this create should support placeholder for URI.
	dbB.Exec(t, "CREATE EXTERNAL CONNECTION a AS '"+dbAURL.String()+"'")
	dbAURL = url.URL{
		Scheme: "external",
		Host:   "a",
	}

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	dbA.Exec(t, "INSERT INTO tab VALUES (2, 'potato')")
	dbB.Exec(t, "INSERT INTO tab VALUES (3, 'celeriac')")
	dbA.Exec(t, "UPSERT INTO tab VALUES (1, 'hello, again')")
	dbB.Exec(t, "UPSERT INTO tab VALUES (1, 'goodbye, again')")

	now = s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	expectedRows := [][]string{
		{"1", "goodbye, again"},
		{"2", "potato"},
		{"3", "celeriac"},
	}
	dbA.CheckQueryResults(t, "SELECT * from a.tab", expectedRows)
	dbB.CheckQueryResults(t, "SELECT * from b.tab", expectedRows)

	// Verify that we didn't have the data looping problem. We expect 3 CPuts
	// when inserting new rows and 3 Puts when updating existing rows.
	expPuts, expCPuts := 3, 4
	if tryOptimisticInsertEnabled.Get(&s.ClusterSettings().SV) {
		// When performing 1 update, we don't have the prevValue set, so if
		// we're using the optimistic insert strategy, it would result in an
		// additional CPut (that ultimately fails). The cluster setting is
		// randomized in tests, so we need to handle both cases.
		expCPuts++
	}
	require.Equal(t, int64(expPuts), numPuts.Load())
	require.Equal(t, int64(expCPuts), numCPuts.Load())
}

func TestLogicalStreamIngestionJobWithCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbA.Exec(t, "INSERT INTO tab VALUES (1, 'hello')")
	dbB.Exec(t, "INSERT INTO tab VALUES (1, 'goodbye')")

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()
	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	// Swap one of the URLs to external:// to verify this indirection works.
	// TODO(dt): this create should support placeholder for URI.
	dbB.Exec(t, "CREATE EXTERNAL CONNECTION a AS '"+dbAURL.String()+"'")
	dbAURL = url.URL{
		Scheme: "external",
		Host:   "a",
	}

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)

	// Perform inserts that should not be replicated since
	// they will be before the cursor time.
	dbA.Exec(t, "INSERT INTO tab VALUES (7, 'do not replicate')")
	dbB.Exec(t, "INSERT INTO tab VALUES (8, 'do not replicate')")
	// Perform the inserts first before starting the LDR stream.
	now := s.Clock().Now()
	dbA.Exec(t, "INSERT INTO tab VALUES (2, 'potato')")
	dbB.Exec(t, "INSERT INTO tab VALUES (3, 'celeriac')")
	dbA.Exec(t, "UPSERT INTO tab VALUES (1, 'hello, again')")
	dbB.Exec(t, "UPSERT INTO tab VALUES (1, 'goodbye, again')")
	// We should expect starting at the provided now() to replicate all the data from that time.
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH CURSOR=$2", dbBURL.String(), now.AsOfSystemTime()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH CURSOR=$2", dbAURL.String(), now.AsOfSystemTime()).Scan(&jobBID)

	now = s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	// The rows added before the now time should remain only
	// on their respective side and not replicate.
	expectedRowsA := [][]string{
		{"1", "goodbye, again"},
		{"2", "potato"},
		{"3", "celeriac"},
		{"7", "do not replicate"},
	}
	expectedRowsB := [][]string{
		{"1", "goodbye, again"},
		{"2", "potato"},
		{"3", "celeriac"},
		{"8", "do not replicate"},
	}
	dbA.CheckQueryResults(t, "SELECT * from a.tab", expectedRowsA)
	dbB.CheckQueryResults(t, "SELECT * from b.tab", expectedRowsB)
}

// TestLogicalStreamIngestionAdvancePTS tests that the producer side pts advances
// as the destination side frontier advances.
func TestLogicalStreamIngestionAdvancePTS(t *testing.T) {
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

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, clusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbA.Exec(t, "INSERT INTO tab VALUES (1, 'hello')")
	dbB.Exec(t, "INSERT INTO tab VALUES (1, 'goodbye')")

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()
	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	// Swap one of the URLs to external:// to verify this indirection works.
	// TODO(dt): this create should support placeholder for URI.
	dbB.Exec(t, "CREATE EXTERNAL CONNECTION a AS '"+dbAURL.String()+"'")
	dbAURL = url.URL{
		Scheme: "external",
		Host:   "a",
	}

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)

	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	// The ingestion job on cluster A has a pts on cluster B.
	producerJobIDB := replicationutils.GetProducerJobIDFromLDRJob(t, dbA, jobAID)
	replicationutils.WaitForPTSProtection(t, ctx, dbB, s, producerJobIDB, now)

	WaitUntilReplicatedTime(t, now, dbB, jobBID)
	producerJobIDA := replicationutils.GetProducerJobIDFromLDRJob(t, dbB, jobBID)
	replicationutils.WaitForPTSProtection(t, ctx, dbA, s, producerJobIDA, now)
}

// TestLogicalStreamIngestionCancelUpdatesProducerJob tests whether
// the producer job's OnFailOrCancel updates the the related producer
// job, resulting in the PTS record being removed.
func TestLogicalStreamIngestionCancelUpdatesProducerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbA.Exec(t, "SET CLUSTER SETTING physical_replication.producer.stream_liveness_track_frequency='50ms'")

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()

	var jobBID jobspb.JobID
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	WaitUntilReplicatedTime(t, s.Clock().Now(), dbB, jobBID)

	producerJobID := replicationutils.GetProducerJobIDFromLDRJob(t, dbB, jobBID)
	jobutils.WaitForJobToRun(t, dbA, producerJobID)

	dbB.Exec(t, "CANCEL JOB $1", jobBID)
	jobutils.WaitForJobToCancel(t, dbB, jobBID)
	jobutils.WaitForJobToFail(t, dbA, producerJobID)
	replicationutils.WaitForPTSProtectionToNotExist(t, ctx, dbA, s, producerJobID)
}

func TestLogicalStreamIngestionErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	server := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer server.Stopper().Stop(ctx)
	s := server.Server(0).ApplicationLayer()
	url, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()
	urlA := url.String()

	_, err := server.Conns[0].Exec("CREATE DATABASE a")
	require.NoError(t, err)
	_, err = server.Conns[0].Exec("CREATE DATABASE B")
	require.NoError(t, err)

	dbA := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("a")))
	dbB := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("b")))

	createStmt := "CREATE TABLE tab (pk int primary key, payload string)"
	dbA.Exec(t, createStmt)
	dbB.Exec(t, createStmt)

	createQ := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab"

	dbB.ExpectErrWithHint(t, "currently require a .* DECIMAL column", "ADD COLUMN", createQ, urlA)

	dbB.Exec(t, "ALTER TABLE tab ADD COLUMN crdb_replication_origin_timestamp STRING")
	dbB.ExpectErr(t, ".*column must be type DECIMAL for use by logical replication", createQ, urlA)

	dbB.Exec(t, fmt.Sprintf("ALTER TABLE tab RENAME COLUMN %[1]s TO str_col, ADD COLUMN %[1]s DECIMAL", originTimestampColumnName))

	if s.Codec().IsSystem() {
		dbB.ExpectErr(t, "kv.rangefeed.enabled must be enabled on the source cluster for logical replication", createQ, urlA)
		kvserver.RangefeedEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	}

	dbB.Exec(t, createQ, urlA)
}

func TestLogicalStreamIngestionJobWithColumnFamilies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, s, serverASQL, serverBSQL := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer tc.Stopper().Stop(ctx)

	createStmt := `CREATE TABLE tab_with_cf (
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
	serverASQL.Exec(t, "ALTER TABLE tab_with_cf "+lwwColumnAdd)
	serverBSQL.Exec(t, "ALTER TABLE tab_with_cf "+lwwColumnAdd)

	serverASQL.Exec(t, "INSERT INTO tab_with_cf(pk, payload, other_payload) VALUES (1, 'hello', 'ruroh1')")

	serverAURL, cleanup := s.PGUrl(t)
	serverAURL.Path = "a"
	defer cleanup()

	var jobBID jobspb.JobID
	serverBSQL.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab_with_cf ON $1 INTO TABLE tab_with_cf", serverAURL.String()).Scan(&jobBID)

	WaitUntilReplicatedTime(t, s.Clock().Now(), serverBSQL, jobBID)
	serverASQL.Exec(t, "INSERT INTO tab_with_cf(pk, payload, other_payload) VALUES (2, 'potato', 'ruroh2')")
	serverASQL.Exec(t, "INSERT INTO tab_with_cf(pk, payload, other_payload) VALUES (4, 'spud', 'shrub')")
	serverASQL.Exec(t, "UPSERT INTO tab_with_cf(pk, payload, other_payload) VALUES (1, 'hello, again', 'ruroh3')")
	serverASQL.Exec(t, "DELETE FROM tab_with_cf WHERE pk = 4")

	WaitUntilReplicatedTime(t, s.Clock().Now(), serverBSQL, jobBID)

	expectedRows := [][]string{
		{"1", "hello, again", "9001", "43", "ruroh3"},
		{"2", "potato", "9002", "44", "ruroh2"},
	}
	serverBSQL.CheckQueryResults(t, "SELECT * from tab_with_cf", expectedRows)
	serverASQL.CheckQueryResults(t, "SELECT * from tab_with_cf", expectedRows)
}

func TestLogicalReplicationWithPhantomDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, s, serverASQL, serverBSQL := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer tc.Stopper().Stop(ctx)

	serverAURL, cleanup := s.PGUrl(t)
	serverAURL.Path = "a"
	defer cleanup()

	for _, mode := range []string{"validated", "immediate"} {
		t.Run(mode, func(t *testing.T) {
			serverASQL.Exec(t, "TRUNCATE tab")
			serverBSQL.Exec(t, "TRUNCATE tab")
			var jobBID jobspb.JobID
			serverBSQL.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = $2", serverAURL.String(), mode).Scan(&jobBID)
			serverASQL.Exec(t, "DELETE FROM tab WHERE pk = 4")
			WaitUntilReplicatedTime(t, s.Clock().Now(), serverBSQL, jobBID)
			expectedRows := [][]string{}
			serverASQL.CheckQueryResults(t, "SELECT * from tab", expectedRows)
			serverBSQL.CheckQueryResults(t, "SELECT * from tab", expectedRows)
		})
	}
}

func TestFilterRangefeedInReplicationStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "multi cluster/node config exhausts hardware")

	ctx := context.Background()

	filterVal := []bool{}
	var filterValLock syncutil.Mutex

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				DistSQL: &execinfra.TestingKnobs{
					StreamingTestingKnobs: &sql.StreamingTestingKnobs{
						BeforeClientSubscribe: func(_ string, _ string, _ span.Frontier, filterRangefeed bool) {
							filterValLock.Lock()
							defer filterValLock.Unlock()
							filterVal = append(filterVal, filterRangefeed)
						},
					},
				},
			},
		},
	}

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, clusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()
	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)

	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH IGNORE_CDC_IGNORED_TTL_DELETES", dbAURL.String()).Scan(&jobBID)

	now := server.Server(0).Clock().Now()
	t.Logf("waiting for replication job %d", jobAID)
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	t.Logf("waiting for replication job %d", jobBID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	// Verify that Job contains FilterRangeFeed
	details := jobutils.GetJobPayload(t, dbA, jobAID).GetLogicalReplicationDetails()
	require.False(t, details.IgnoreCDCIgnoredTTLDeletes)

	details = jobutils.GetJobPayload(t, dbB, jobBID).GetLogicalReplicationDetails()
	require.True(t, details.IgnoreCDCIgnoredTTLDeletes)

	require.Equal(t, len(filterVal), 2)

	// Only one should be true
	require.True(t, filterVal[0] != filterVal[1])
}

func TestRandomTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, s, runnerA, runnerB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer tc.Stopper().Stop(ctx)

	sqlA := s.SQLConn(t, serverutils.DBName("a"))

	tableName := "rand_table"
	rng, _ := randutil.NewPseudoRand()
	createStmt := randgen.RandCreateTableWithName(
		ctx,
		rng,
		tableName,
		1,
		false, /* isMultiregion */
		// We do not have full support for column families.
		randgen.SkipColumnFamilyMutation())
	stmt := tree.SerializeForDisplay(createStmt)
	t.Log(stmt)
	runnerA.Exec(t, stmt)
	runnerB.Exec(t, stmt)

	// TODO(ssd): We have to turn off randomized_anchor_key
	// because this, in combination of optimizer difference that
	// might prevent CommitInBatch, could result in the replicated
	// transaction being too large to commit.
	runnerA.Exec(t, "SET CLUSTER SETTING kv.transaction.randomized_anchor_key.enabled=false")

	// Workaround for the behaviour described in #127321. This
	// ensures that we are generating rows using similar
	// optimization decisions to our replication process.
	runnerA.Exec(t, "SET plan_cache_mode=force_generic_plan")

	numInserts := 20
	_, err := randgen.PopulateTableWithRandData(rng,
		sqlA, tableName, numInserts, nil)
	require.NoError(t, err)

	addCol := fmt.Sprintf(`ALTER TABLE %s `+lwwColumnAdd, tableName)
	runnerA.Exec(t, addCol)
	runnerB.Exec(t, addCol)

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()

	streamStartStmt := fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLE %[1]s ON $1 INTO TABLE %[1]s", tableName)
	var jobBID jobspb.JobID
	runnerB.QueryRow(t, streamStartStmt, dbAURL.String()).Scan(&jobBID)

	WaitUntilReplicatedTime(t, s.Clock().Now(), runnerB, jobBID)

	compareReplicatedTables(t, s, "a", "b", tableName, runnerA, runnerB)
}

func TestRandomStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	eventCount := 1000
	testState := struct {
		syncutil.Mutex
		count int
		done  chan struct{}
	}{
		done: make(chan struct{}),
	}

	streamingKnobs := &sql.StreamingTestingKnobs{
		DistSQLRetryPolicy: &retry.Options{
			InitialBackoff: time.Microsecond,
			MaxBackoff:     2 * time.Microsecond,
		},
		RunAfterReceivingEvent: func(_ context.Context) error {
			testState.Lock()
			defer testState.Unlock()
			testState.count++
			if testState.count == eventCount {
				close(testState.done)
			}
			return nil
		},
	}
	clusterArgs := testClusterBaseClusterArgs
	clusterArgs.ServerArgs.Knobs.DistSQL = &execinfra.TestingKnobs{
		StreamingTestingKnobs: streamingKnobs,
	}
	clusterArgs.ServerArgs.Knobs.Streaming = streamingKnobs

	ctx := context.Background()
	tc, s, runnerA, _ := setupLogicalTestServer(t, ctx, clusterArgs, 1)
	defer tc.Stopper().Stop(ctx)

	desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "a", "tab")
	// We use a low events per checkpoint since we want to
	// generate errors at many different code locations.
	uri := fmt.Sprintf("randomgen://?TABLE_ID=%d&SST_PROBABILITY=0&ERROR_PROBABILITY=0.10&EVENTS_PER_CHECKPOINT=5", desc.GetID())

	streamStartStmt := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab"
	runnerA.Exec(t, streamStartStmt, uri)
	t.Logf("waiting for %d events", eventCount)
	select {
	case <-testState.done:
	case <-time.After(testutils.SucceedsSoonDuration()):
		t.Logf("%s", string(allstacks.Get()))
		t.Fatal("timed out waiting for events")
	}

}

// TestPreviouslyInterestingTables tests some schemas from previous failed runs of TestRandomTables.
func TestPreviouslyInterestingTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, s, runnerA, runnerB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer tc.Stopper().Stop(ctx)

	sqlA := s.SQLConn(t, serverutils.DBName("a"))

	type testCase struct {
		name   string
		schema string
		useUDF bool
		delete bool
	}

	testCases := []testCase{
		{
			// This caught a problem with the comparison we were
			// using rather than the replication process itself. We
			// leave it here as an example of how to add new
			// schemas.
			name:   "comparison-invariant-to-different-covering-indexes",
			schema: `CREATE TABLE rand_table (col1_0 DECIMAL, INDEX (col1_0) VISIBILITY 0.17, UNIQUE (col1_0 DESC), UNIQUE (col1_0 ASC), INDEX (col1_0 ASC), UNIQUE (col1_0 ASC))`,
		},
		{

			// Single column tables previously had a bug
			// with tuple parsing the UDF apply query.
			name:   "single column table with udf",
			schema: `CREATE TABLE rand_table (pk INT PRIMARY KEY)`,
			useUDF: true,
		},
	}

	baseTableName := "rand_table"
	rng, _ := randutil.NewPseudoRand()
	numInserts := 20
	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableName := fmt.Sprintf("%s%d", baseTableName, i)
			schemaStmt := strings.ReplaceAll(tc.schema, baseTableName, tableName)
			addCol := fmt.Sprintf(`ALTER TABLE %s `+lwwColumnAdd, tableName)
			runnerA.Exec(t, schemaStmt)
			runnerB.Exec(t, schemaStmt)
			runnerA.Exec(t, addCol)
			runnerB.Exec(t, addCol)
			if tc.useUDF {
				runnerB.Exec(t, fmt.Sprintf(testingUDFAcceptProposedBase, tableName))
			}
			_, err := randgen.PopulateTableWithRandData(rng,
				sqlA, tableName, numInserts, nil)
			require.NoError(t, err)

			var streamStartStmt string
			if tc.useUDF {
				streamStartStmt = fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLE %[1]s ON $1 INTO TABLE %[1]s WITH FUNCTION repl_apply FOR TABLE %[1]s", tableName)
			} else {
				streamStartStmt = fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLE %[1]s ON $1 INTO TABLE %[1]s", tableName)
			}
			var jobBID jobspb.JobID
			runnerB.QueryRow(t, streamStartStmt, dbAURL.String()).Scan(&jobBID)

			WaitUntilReplicatedTime(t, s.Clock().Now(), runnerB, jobBID)

			if tc.delete {
				runnerA.Exec(t, fmt.Sprintf("DELETE FROM %s LIMIT 5", tableName))
				WaitUntilReplicatedTime(t, s.Clock().Now(), runnerB, jobBID)
			}

			compareReplicatedTables(t, s, "a", "b", tableName, runnerA, runnerB)
		})
	}
}

// TestLogicalAutoReplan asserts that if a new node can participate in the
// logical replication job, it will trigger distSQL replanning.
func TestLogicalAutoReplan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "multi cluster/node config exhausts hardware")

	ctx := context.Background()

	// Double the number of nodes
	retryErrorChan := make(chan error, 4)
	turnOffReplanning := make(chan struct{})
	var alreadyReplanned atomic.Bool

	// Track the number of unique addresses that we're connected to.
	clientAddresses := make(map[string]struct{})
	var addressesMu syncutil.Mutex

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				DistSQL: &execinfra.TestingKnobs{
					StreamingTestingKnobs: &sql.StreamingTestingKnobs{
						BeforeClientSubscribe: func(addr string, token string, _ span.Frontier, _ bool) {
							addressesMu.Lock()
							defer addressesMu.Unlock()
							clientAddresses[addr] = struct{}{}
						},
					},
				},
				Streaming: &sql.StreamingTestingKnobs{
					AfterRetryIteration: func(err error) {
						if err != nil && !alreadyReplanned.Load() {
							retryErrorChan <- err
							<-turnOffReplanning
							alreadyReplanned.Swap(true)
						}
					},
				},
			},
		},
	}

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, clusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	// Don't allow for replanning until the new nodes and scattered table have been created.
	serverutils.SetClusterSetting(t, server, "logical_replication.replan_flow_threshold", 0)
	serverutils.SetClusterSetting(t, server, "logical_replication.replan_flow_frequency", time.Millisecond*500)

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()
	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	server.AddAndStartServer(t, clusterArgs.ServerArgs)
	server.AddAndStartServer(t, clusterArgs.ServerArgs)
	t.Logf("New nodes added")

	// Only need at least two nodes as leaseholders for test.
	CreateScatteredTable(t, dbA, 2, "A")

	// Configure the ingestion job to replan eagerly.
	serverutils.SetClusterSetting(t, server, "logical_replication.replan_flow_threshold", 0.1)

	// The ingestion job should eventually retry because it detects new nodes to add to the plan.
	require.ErrorContains(t, <-retryErrorChan, sql.ErrPlanChanged.Error())

	// Prevent continuous replanning to reduce test runtime. dsp.PartitionSpans()
	// on the src cluster may return a different set of src nodes that can
	// participate in the replication job (especially under stress), so if we
	// repeatedly replan the job, we will repeatedly restart the job, preventing
	// job progress.
	serverutils.SetClusterSetting(t, server, "logical_replication.replan_flow_threshold", 0)
	serverutils.SetClusterSetting(t, server, "logical_replication.replan_flow_frequency", time.Minute*10)
	close(turnOffReplanning)

	require.Greater(t, len(clientAddresses), 1)
}

// TestLogicalJobResiliency tests that the stream addresses from
// the initial job plan are persisted in system.job_info. In the
// case that the coordinator node is unavailable, a new node should
// pick up the job and resume
func TestLogicalJobResiliency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "multi cluster/node config exhausts hardware")
	skip.UnderDeadlock(t, "Scattering prior to creating LDR job slows down ingestion")

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	ctx := context.Background()

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, clusterArgs, 3)
	defer server.Stopper().Stop(ctx)

	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	CreateScatteredTable(t, dbB, 2, "B")

	var jobAID jobspb.JobID
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)

	progress := jobutils.GetJobProgress(t, dbA, jobAID)
	addresses := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.StreamAddresses

	require.Greaterf(t, len(addresses), 1, "Less than 2 addresses were persisted in system.job_info")
}

func TestHeartbeatCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "multi cluster/node config exhausts hardware")

	ctx := context.Background()

	// Make size of channel double the number of nodes
	retryErrorChan := make(chan error, 4)
	var alreadyCancelled atomic.Bool

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Streaming: &sql.StreamingTestingKnobs{
					AfterRetryIteration: func(err error) {
						if err != nil && !alreadyCancelled.Load() {
							retryErrorChan <- err
							alreadyCancelled.Store(true)
						}
					},
				},
			},
		},
	}

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, clusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	serverutils.SetClusterSetting(t, server, "logical_replication.consumer.heartbeat_frequency", time.Second*1)

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()
	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	prodAID := replicationutils.GetProducerJobIDFromLDRJob(t, dbB, jobBID)

	// Cancel the producer job and wait for the hearbeat to pick up that the stream is inactive
	t.Logf("canceling replication producer %s", prodAID)
	dbA.QueryRow(t, "CANCEL JOB $1", prodAID)

	// The ingestion job should eventually retry because it detects 2 nodes are dead
	require.ErrorContains(t, <-retryErrorChan, fmt.Sprintf("replication stream %s is not running, status is STREAM_INACTIVE", prodAID))
}

// TestMultipleSourcesIntoSingleDest tests if one destination table can handle
// conflicts streaming from multiple source tables
func TestMultipleSourcesIntoSingleDest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				DistSQL:          &execinfra.TestingKnobs{},
			},
		},
	}

	server, s, runners, dbNames := setupServerWithNumDBs(t, ctx, clusterArgs, 1, 3)
	defer server.Stopper().Stop(ctx)

	PGURLs, cleanup := GetPGURLs(t, s, dbNames)
	defer cleanup()

	dbA, dbB, dbC := runners[0], runners[1], runners[2]

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)
	dbC.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", PGURLs[0].String()).Scan(&jobAID)
	dbC.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", PGURLs[1].String()).Scan(&jobBID)

	// Insert into dest, then check source2 -> dest wins
	dbC.Exec(t, "UPSERT INTO tab VALUES (1, 'hello')")
	dbB.Exec(t, "UPSERT INTO tab VALUES (1, 'goodbye')")
	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbC, jobBID)
	expectedRows := [][]string{
		{"1", "goodbye"},
	}
	dbC.CheckQueryResults(t, "SELECT * from tab", expectedRows)

	// Write to source1 and source2, which should keep their respective rows but dest should resolve a conflict
	dbA.Exec(t, "UPSERT INTO tab VALUES (1, 'insertA')")
	dbB.Exec(t, "UPSERT INTO tab VALUES (1, 'insertB')")

	expectedRowsS1 := [][]string{
		{"1", "insertA"},
	}
	expectedRowsDest := [][]string{
		{"1", "insertB"},
	}
	now = s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbC, jobAID)
	WaitUntilReplicatedTime(t, now, dbC, jobBID)
	dbA.CheckQueryResults(t, "SELECT * from tab", expectedRowsS1)
	dbB.CheckQueryResults(t, "SELECT * from tab", expectedRowsDest)
	dbC.CheckQueryResults(t, "SELECT * from tab", expectedRowsDest)
}

// TestFullyConnectedReplication tests 4 tables that are all streaming
// from each other and how they handle conflicts
func TestFullyConnectedReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "Replication doesn't complete in time")

	ctx := context.Background()

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	verifyExpectedRowAllServers := func(
		t *testing.T, runners []*sqlutils.SQLRunner, expectedRows [][]string, dbNames []string,
	) {
		for i, name := range dbNames {
			runners[i].CheckQueryResults(t, fmt.Sprintf("SELECT * from %s.tab", name), expectedRows)
		}
	}

	waitUntilReplicatedTimeAllServers := func(
		t *testing.T,
		targetTime hlc.Timestamp,
		runners []*sqlutils.SQLRunner,
		jobIDs [][]jobspb.JobID,
	) {
		for destIdx := range jobIDs {
			for srcIdx := range jobIDs[destIdx] {
				if destIdx == srcIdx {
					continue
				}
				WaitUntilReplicatedTime(t, targetTime, runners[destIdx], jobIDs[destIdx][srcIdx])
			}
		}
	}

	numDBs := 4
	server, s, runners, dbNames := setupServerWithNumDBs(t, ctx, clusterArgs, 1, numDBs)
	defer server.Stopper().Stop(ctx)

	PGURLs, cleanup := GetPGURLs(t, s, dbNames)
	defer cleanup()

	// Each row is a DB, each column is a jobID from another DB to that target DB
	jobIDs := make([][]jobspb.JobID, numDBs)
	for dstIdx := range numDBs {
		jobIDs[dstIdx] = make([]jobspb.JobID, numDBs)
		for srcIdx := range numDBs {
			if dstIdx == srcIdx {
				jobIDs[dstIdx][srcIdx] = jobspb.InvalidJobID
				continue
			}
			runners[dstIdx].QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", PGURLs[srcIdx].String()).Scan(&jobIDs[dstIdx][srcIdx])
		}
	}

	runners[0].Exec(t, "UPSERT INTO tab VALUES (1, 'celery')")
	now := s.Clock().Now()
	waitUntilReplicatedTimeAllServers(t, now, runners, jobIDs)

	expectedRows := [][]string{
		{"1", "celery"},
	}
	verifyExpectedRowAllServers(t, runners, expectedRows, dbNames)

	for i := range numDBs {
		runners[i].Exec(t, fmt.Sprintf("UPSERT INTO tab VALUES (2, 'row%v')", i))
	}
	now = s.Clock().Now()
	waitUntilReplicatedTimeAllServers(t, now, runners, jobIDs)

	expectedRows = [][]string{
		{"1", "celery"},
		{"2", "row3"},
	}
	verifyExpectedRowAllServers(t, runners, expectedRows, dbNames)
}

func TestForeignKeyConstraints(t *testing.T) {
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

	server, s, dbA, _ := setupLogicalTestServer(t, ctx, clusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	dbA.Exec(t, "CREATE TABLE test(a int primary key, b int)")

	testutils.RunTrueAndFalse(t, "immediate-mode", func(t *testing.T, immediateMode bool) {
		testutils.RunTrueAndFalse(t, "valid-foreign-key", func(t *testing.T, validForeignKey bool) {
			fkStmt := "ALTER TABLE test ADD CONSTRAINT fkc FOREIGN KEY (b) REFERENCES tab(pk)"
			if !validForeignKey {
				fkStmt = fkStmt + " NOT VALID"
			}
			dbA.Exec(t, fkStmt)

			var mode string
			if immediateMode {
				mode = "IMMEDIATE"
			} else {
				mode = "VALIDATED"
			}

			var jobID jobspb.JobID
			stmt := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = " + mode
			if immediateMode && validForeignKey {
				dbA.ExpectErr(t, "only 'NOT VALID' foreign keys are only supported with MODE = 'validated'", stmt, dbBURL.String())
			} else {
				dbA.QueryRow(t, stmt, dbBURL.String()).Scan(&jobID)
			}

			dbA.Exec(t, "ALTER TABLE test DROP CONSTRAINT fkc")
		})
	})
}

func setupServerWithNumDBs(
	t *testing.T, ctx context.Context, clusterArgs base.TestClusterArgs, numNodes int, numDBs int,
) (
	*testcluster.TestCluster,
	serverutils.ApplicationLayerInterface,
	[]*sqlutils.SQLRunner,
	[]string,
) {
	server := testcluster.StartTestCluster(t, numNodes, clusterArgs)
	s := server.Server(0).ApplicationLayer()

	_, err := server.Conns[0].Exec("SET CLUSTER SETTING physical_replication.producer.timestamp_granularity = '0s'")
	require.NoError(t, err)

	runners := []*sqlutils.SQLRunner{}
	dbNames := []string{}

	for i := range numDBs {
		dbName := string(rune('a' + i))
		_, err = server.Conns[0].Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
		require.NoError(t, err)
		runners = append(runners, sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName(dbName))))
		dbNames = append(dbNames, dbName)
	}

	sysDB := sqlutils.MakeSQLRunner(server.SystemLayer(0).SQLConn(t))
	for _, s := range testClusterSystemSettings {
		sysDB.Exec(t, s)
	}

	for _, s := range testClusterSettings {
		runners[0].Exec(t, s)
	}

	for i := range numDBs {
		createBasicTable(t, runners[i], "tab")
	}
	return server, s, runners, dbNames
}

func setupLogicalTestServer(
	t *testing.T, ctx context.Context, clusterArgs base.TestClusterArgs, numNodes int,
) (
	*testcluster.TestCluster,
	serverutils.ApplicationLayerInterface,
	*sqlutils.SQLRunner,
	*sqlutils.SQLRunner,
) {
	server, s, runners, _ := setupServerWithNumDBs(t, ctx, clusterArgs, numNodes, 2)
	return server, s, runners[0], runners[1]
}

func createBasicTable(t *testing.T, db *sqlutils.SQLRunner, tableName string) {
	createStmt := fmt.Sprintf("CREATE TABLE %s (pk int primary key, payload string)", tableName)
	db.Exec(t, createStmt)
	db.Exec(t, fmt.Sprintf("ALTER TABLE %s %s", tableName, lwwColumnAdd))
}

func compareReplicatedTables(
	t *testing.T,
	s serverutils.ApplicationLayerInterface,
	dbA, dbB, tableName string,
	runnerA, runnerB *sqlutils.SQLRunner,
) {
	descA := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), dbA, tableName)
	descB := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), dbB, tableName)

	for _, indexA := range descA.AllIndexes() {
		if indexA.GetType() == descpb.IndexDescriptor_INVERTED {
			t.Logf("skipping fingerprinting of inverted index %s", indexA.GetName())
			continue
		}

		indexB, err := catalog.MustFindIndexByName(descB, indexA.GetName())
		require.NoError(t, err)

		aFingerprintQuery, err := sql.BuildFingerprintQueryForIndex(descA, indexA, []string{originTimestampColumnName})
		require.NoError(t, err)
		bFingerprintQuery, err := sql.BuildFingerprintQueryForIndex(descB, indexB, []string{originTimestampColumnName})
		require.NoError(t, err)
		t.Logf("fingerprinting index %s", indexA.GetName())
		runnerB.CheckQueryResults(t, bFingerprintQuery, runnerA.QueryStr(t, aFingerprintQuery))
	}
}

func CreateScatteredTable(t *testing.T, db *sqlutils.SQLRunner, numNodes int, dbName string) {
	// Create a source table with multiple ranges spread across multiple nodes. We
	// need around 50 or more ranges because there are already over 50 system
	// ranges, so if we write just a few ranges those might all be on a single
	// server, which will cause the test to flake.
	numRanges := 50
	rowsPerRange := 20
	db.Exec(t, "INSERT INTO tab (pk) SELECT * FROM generate_series(1, $1)",
		numRanges*rowsPerRange)
	db.Exec(t, "ALTER TABLE tab SPLIT AT (SELECT * FROM generate_series($1::INT, $2::INT, $3::INT))",
		rowsPerRange, (numRanges-1)*rowsPerRange, rowsPerRange)
	db.Exec(t, "ALTER TABLE tab SCATTER")
	timeout := 45 * time.Second
	if skip.Duress() {
		timeout *= 5
	}

	testutils.SucceedsWithin(t, func() error {
		var leaseHolderCount int
		query := fmt.Sprintf("SELECT count(DISTINCT lease_holder) FROM [SHOW RANGES FROM DATABASE %s WITH DETAILS]", dbName)
		db.QueryRow(t,
			query).
			Scan(&leaseHolderCount)
		require.Greater(t, leaseHolderCount, 0)
		if leaseHolderCount < numNodes {
			return errors.New("leaseholders not scattered yet")
		}
		return nil
	}, timeout)
}

func GetPGURLs(
	t *testing.T, s serverutils.ApplicationLayerInterface, dbNames []string,
) ([]url.URL, func()) {
	result := []url.URL{}
	cleanups := []func(){}
	for _, name := range dbNames {
		resultURL, cleanup := s.PGUrl(t, serverutils.DBName(name))
		result = append(result, resultURL)
		cleanups = append(cleanups, cleanup)
	}

	return result, func() {
		for _, f := range cleanups {
			f()
		}
	}
}

func WaitUntilReplicatedTime(
	t *testing.T, targetTime hlc.Timestamp, db *sqlutils.SQLRunner, ingestionJobID jobspb.JobID,
) {
	t.Logf("waiting for logical replication job %d to reach replicated time of %s", ingestionJobID, targetTime)
	testutils.SucceedsSoon(t, func() error {
		progress := jobutils.GetJobProgress(t, db, ingestionJobID)
		replicatedTime := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.ReplicatedTime
		if replicatedTime.IsEmpty() {
			return errors.Newf("logical replication has not recorded any progress yet, waiting to advance pos %s",
				targetTime)
		}
		if replicatedTime.Less(targetTime) {
			return errors.Newf("waiting for logical replication job replicated time %s to advance beyond %s",
				replicatedTime, targetTime)
		}
		return nil
	})
}

type mockBatchHandler bool

var _ BatchHandler = mockBatchHandler(true)

func (m mockBatchHandler) HandleBatch(
	_ context.Context, _ []streampb.StreamEvent_KV,
) (batchStats, error) {
	if m {
		return batchStats{}, errors.New("batch processing failure")
	}
	return batchStats{}, nil
}
func (m mockBatchHandler) GetLastRow() cdcevent.Row            { return cdcevent.Row{} }
func (m mockBatchHandler) SetSyntheticFailurePercent(_ uint32) {}
func (m mockBatchHandler) Close(context.Context)               {}

type mockDLQ int

func (m *mockDLQ) Create(_ context.Context) error {
	return nil
}

func (m *mockDLQ) Log(
	_ context.Context,
	_ int64,
	_ streampb.StreamEvent_KV,
	_ cdcevent.Row,
	_ error,
	_ retryEligibility,
) error {
	*m++
	return nil
}

// TestFlushErrorHandling exercises the flush path in cases where writes fail.
func TestFlushErrorHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dlq := mockDLQ(0)
	lrw := &logicalReplicationWriterProcessor{
		metrics:      MakeMetrics(0).(*Metrics),
		getBatchSize: func() int { return 1 },
		dlqClient:    &dlq,
	}
	lrw.purgatory.flush = lrw.flushBuffer
	lrw.purgatory.bytesGauge = lrw.metrics.RetryQueueBytes
	lrw.purgatory.eventsGauge = lrw.metrics.RetryQueueEvents

	lrw.bh = []BatchHandler{(mockBatchHandler(true))}

	lrw.purgatory.byteLimit = func() int64 { return 1 }
	// One failure immediately means a 1-byte purgatory is full.
	require.NoError(t, lrw.handleStreamBuffer(ctx, []streampb.StreamEvent_KV{skv("a")}))
	require.Equal(t, int64(1), lrw.metrics.RetryQueueEvents.Value())
	require.True(t, lrw.purgatory.full())
	require.Equal(t, 0, int(dlq))

	// Another failure causes a forced drain of purgatory, incrementing DLQ count.
	require.NoError(t, lrw.handleStreamBuffer(ctx, []streampb.StreamEvent_KV{skv("b")}))
	require.Equal(t, int64(1), lrw.metrics.RetryQueueEvents.Value())
	require.Equal(t, 1, int(dlq))

	// Bump up the purgatory size limit and observe no more DLQ'ed items.
	lrw.purgatory.byteLimit = func() int64 { return 1 << 20 }
	require.False(t, lrw.purgatory.full())
	require.NoError(t, lrw.handleStreamBuffer(ctx, []streampb.StreamEvent_KV{skv("c")}))
	require.NoError(t, lrw.handleStreamBuffer(ctx, []streampb.StreamEvent_KV{skv("d")}))
	require.Equal(t, 1, int(dlq))
	require.Equal(t, int64(3), lrw.metrics.RetryQueueEvents.Value())
}

func TestLogicalStreamIngestionJobWithFallbackUDF(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 129569, "flakey test")

	ctx := context.Background()
	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}, 1)
	defer server.Stopper().Stop(ctx)

	lwwFunc := `CREATE OR REPLACE FUNCTION repl_apply(action STRING, proposed tab, existing tab, prev tab, existing_mvcc_timestamp DECIMAL, existing_origin_timestamp DECIMAL,proposed_mvcc_timestamp DECIMAL, proposed_previous_mvcc_timestamp DECIMAL)
	RETURNS string
	AS $$
	BEGIN
	SELECT crdb_internal.log((proposed).payload);
        IF existing IS NULL THEN
            RETURN 'accept_proposed';
        END IF;

	IF existing_origin_timestamp IS NULL THEN
	    IF existing_mvcc_timestamp < proposed_mvcc_timestamp THEN
			SELECT crdb_internal.log('case 1');
			RETURN 'accept_proposed';
		ELSE
			SELECT crdb_internal.log('case 2');
			RETURN 'ignore_proposed';
		END IF;
	ELSE
		IF existing_origin_timestamp < proposed_mvcc_timestamp THEN
			SELECT crdb_internal.log('case 3');
			RETURN 'accept_proposed';
		ELSE
			SELECT crdb_internal.log('case 4');
			RETURN 'ignore_proposed';
		END IF;
	END IF;
	END
	$$ LANGUAGE plpgsql`
	dbB.Exec(t, lwwFunc)
	dbA.Exec(t, lwwFunc)

	dbAURL, cleanup := s.PGUrl(t, serverutils.DBName("a"))
	defer cleanup()
	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	// Swap one of the URLs to external:// to verify this indirection works.
	// TODO(dt): this create should support placeholder for URI.
	dbB.Exec(t, "CREATE EXTERNAL CONNECTION a AS '"+dbAURL.String()+"'")
	dbAURL = url.URL{
		Scheme: "external",
		Host:   "a",
	}

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH FUNCTION repl_apply FOR TABLE tab", dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH FUNCTION repl_apply FOR TABLE tab", dbAURL.String()).Scan(&jobBID)

	now := s.Clock().Now()

	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	dbA.Exec(t, "INSERT INTO tab VALUES (2, 'potato')")
	dbB.Exec(t, "INSERT INTO tab VALUES (3, 'celeriac')")
	dbA.Exec(t, "UPSERT INTO tab VALUES (1, 'hello, again')")
	dbB.Exec(t, "UPSERT INTO tab VALUES (1, 'goodbye, again')")

	now = s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	expectedRows := [][]string{
		{"1", "goodbye, again"},
		{"2", "potato"},
		{"3", "celeriac"},
	}
	dbA.CheckQueryResults(t, "SELECT * from a.tab", expectedRows)
	dbB.CheckQueryResults(t, "SELECT * from b.tab", expectedRows)
}

func TestLogicalReplicationPlanner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	registry := s.JobRegistry().(*jobs.Registry)

	jobExecCtx, cleanup := sql.MakeJobExecContext(ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer cleanup()

	replicationStartTime := hlc.Timestamp{WallTime: 42}

	var sj *jobs.StartableJob
	id := registry.MakeJobID()
	require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) (err error) {
		return registry.CreateStartableJobWithTxn(ctx, &sj, id, txn, jobs.Record{
			Username: username.RootUserName(),
			Details: jobspb.LogicalReplicationDetails{
				ReplicationStartTime: replicationStartTime,
			},
			Progress: jobspb.LogicalReplicationProgress{},
		})
	}))
	asOfChan := make(chan hlc.Timestamp, 1)
	client := &streamclient.MockStreamClient{
		OnPlanLogicalReplication: func(req streampb.LogicalReplicationPlanRequest) (streamclient.LogicalReplicationPlan, error) {
			asOfChan <- req.PlanAsOf
			return streamclient.LogicalReplicationPlan{
				Topology: streamclient.Topology{
					Partitions: []streamclient.PartitionInfo{
						{
							ID:                "1",
							SubscriptionToken: streamclient.SubscriptionToken("1"),
							Spans:             []roachpb.Span{s.Codec().TenantSpan()},
						},
					},
				},
			}, nil
		},
	}
	requireAsOf := func(expected hlc.Timestamp) {
		select {
		case actual := <-asOfChan:
			require.Equal(t, expected, actual)
		case <-time.After(testutils.SucceedsSoonDuration()):
		}
	}
	planner := logicalReplicationPlanner{
		job:        sj.Job,
		jobExecCtx: jobExecCtx,
		client:     client,
	}
	t.Run("generatePlan uses the replicationStartTime for planning if replication is unset", func(t *testing.T) {
		_, _, _ = planner.generatePlan(ctx, jobExecCtx.DistSQLPlanner())
		requireAsOf(replicationStartTime)
	})
	t.Run("generatePlan uses the latest replicated time for planning", func(t *testing.T) {
		replicatedTime := hlc.Timestamp{WallTime: 142}
		require.NoError(t, sj.Job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			prog := md.Progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
			prog.ReplicatedTime = replicatedTime
			ju.UpdateProgress(md.Progress)
			return nil
		}))
		_, _, _ = planner.generatePlan(ctx, jobExecCtx.DistSQLPlanner())
		requireAsOf(replicatedTime)
	})
}

func TestShowLogicalReplicationJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbAURL, cleanup := s.PGUrl(t,
		serverutils.DBName("a"),
		serverutils.UserPassword(username.RootUser, "password"))
	defer cleanup()

	dbBURL, cleanupB := s.PGUrl(t,
		serverutils.DBName("b"),
		serverutils.UserPassword(username.RootUser, "password"))
	defer cleanupB()

	redactedDbAURL := strings.Replace(dbAURL.String(), "password", `redacted`, 1)
	redactedDbBURL := strings.Replace(dbBURL.String(), "password", `redacted`, 1)

	redactedJobADescription := fmt.Sprintf("LOGICAL REPLICATION STREAM into a.public.tab from %s", redactedDbBURL)
	redactedJobBDescription := fmt.Sprintf("LOGICAL REPLICATION STREAM into b.public.tab from %s", redactedDbAURL)

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)
	dbA.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab on $1 INTO TABLE tab",
		dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab on $1 INTO TABLE tab WITH DEFAULT FUNCTION = 'dlq'",
		dbAURL.String()).Scan(&jobBID)

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	// Sort job IDs to match rows ordered with ORDER BY clause
	jobIDs := []jobspb.JobID{jobAID, jobBID}
	slices.Sort(jobIDs)

	var expectedReplicatedTimes []time.Time
	for _, jobID := range jobIDs {
		progress := jobutils.GetJobProgress(t, dbA, jobID)
		replicatedTime := progress.GetLogicalReplication().ReplicatedTime.GoTime().Round(time.Microsecond)
		expectedReplicatedTimes = append(expectedReplicatedTimes, replicatedTime)
	}

	var (
		jobID                  jobspb.JobID
		status                 string
		targets                pq.StringArray
		replicatedTime         time.Time
		replicationStartTime   time.Time
		conflictResolutionType string
		description            string
	)

	showRows := dbA.Query(t, "SELECT * FROM [SHOW LOGICAL REPLICATION JOBS] ORDER BY job_id")
	defer showRows.Close()

	rowIdx := 0
	for showRows.Next() {
		err := showRows.Scan(&jobID, &status, &targets, &replicatedTime)
		require.NoError(t, err)

		expectedJobID := jobIDs[rowIdx]
		require.Equal(t, expectedJobID, jobID)
		require.Equal(t, jobs.StatusRunning, jobs.Status(status))

		if expectedJobID == jobAID {
			require.Equal(t, pq.StringArray{"a.public.tab"}, targets)
		} else if expectedJobID == jobBID {
			require.Equal(t, pq.StringArray{"b.public.tab"}, targets)
		}

		// `SHOW LOGICAL REPLICATION JOBS` query runs after the job query in `jobutils.GetJobProgress()`,
		// `LogicalReplicationProgress.ReplicatedTime` could have advanced by the time we run
		// `SHOW LOGICAL REPLICATION JOBS`, therefore expectedReplicatedTime should be less than or equal to
		// replicatedTime.
		require.LessOrEqual(t, expectedReplicatedTimes[rowIdx], replicatedTime)

		rowIdx++
	}
	require.Equal(t, 2, rowIdx)

	showWithDetailsRows := dbA.Query(t, "SELECT * FROM [SHOW LOGICAL REPLICATION JOBS WITH DETAILS] ORDER BY job_id")
	defer showWithDetailsRows.Close()

	rowIdx = 0
	for showWithDetailsRows.Next() {
		err := showWithDetailsRows.Scan(
			&jobID,
			&status,
			&targets,
			&replicatedTime,
			&replicationStartTime,
			&conflictResolutionType,
			&description)
		require.NoError(t, err)

		expectedJobID := jobIDs[rowIdx]
		payload := jobutils.GetJobPayload(t, dbA, expectedJobID)
		expectedReplicationStartTime := payload.GetLogicalReplicationDetails().ReplicationStartTime.GoTime().Round(time.Microsecond)
		require.Equal(t, expectedReplicationStartTime, replicationStartTime)

		expectedConflictResolutionType := payload.GetLogicalReplicationDetails().DefaultConflictResolution.ConflictResolutionType.String()
		require.Equal(t, expectedConflictResolutionType, conflictResolutionType)

		expectedJobDescription := payload.Description

		// Verify that URL is redacted in job descriptions
		if jobID == jobAID {
			require.Equal(t, redactedJobADescription, expectedJobDescription)
		} else if jobID == jobBID {
			require.Equal(t, redactedJobBDescription, expectedJobDescription)
		}

		require.Equal(t, expectedJobDescription, description)

		rowIdx++
	}
	require.Equal(t, 2, rowIdx)

	dbA.Exec(t, "CANCEL JOB $1", jobAID.String())
	dbA.Exec(t, "CANCEL JOB $1", jobBID.String())

	jobutils.WaitForJobToCancel(t, dbA, jobAID)
	jobutils.WaitForJobToCancel(t, dbA, jobBID)
}

// TestUserPrivileges verifies the grants and role permissions
// needed to start and administer LDR
func TestUserPrivileges(t *testing.T) {
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

	server, s, dbA, _ := setupLogicalTestServer(t, ctx, clusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbBURL, cleanupB := s.PGUrl(t, serverutils.DBName("b"))
	defer cleanupB()

	var jobAID jobspb.JobID
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)

	// Create user with no privileges
	dbA.Exec(t, fmt.Sprintf("CREATE USER %s", username.TestUser))
	testuser := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.User(username.TestUser), serverutils.DBName("a")))

	t.Run("view-job", func(t *testing.T) {
		showJobStmt := "select job_id from [SHOW JOBS] where job_id=$1"
		showLDRJobStmt := "select job_id from [SHOW LOGICAL REPLICATION JOBS] where job_id=$1"
		// NEED VIEWJOB system grant to view admin LDR jobs
		result := testuser.QueryStr(t, showJobStmt, jobAID)
		require.Empty(t, result, "The user should see no rows without the VIEWJOB grant when running [SHOW JOBS]")

		result = testuser.QueryStr(t, showLDRJobStmt, jobAID)
		require.Empty(t, result, "The user should see no rows without the VIEWJOB grant when running [SHOW LOGICAL REPLICATION JOBS]")

		var returnedJobID jobspb.JobID
		dbA.Exec(t, fmt.Sprintf("GRANT SYSTEM VIEWJOB to %s", username.TestUser))
		testuser.QueryRow(t, showJobStmt, jobAID).Scan(&returnedJobID)
		require.Equal(t, returnedJobID, jobAID, "The user should see the LDR job with the VIEWJOB grant when running [SHOW JOBS]")

		testuser.QueryRow(t, showLDRJobStmt, jobAID).Scan(&returnedJobID)
		require.Equal(t, returnedJobID, jobAID, "The user should see the LDR job with the VIEWJOB grant when running [SHOW LOGICAL REPLICATION JOBS]")
	})

	// Kill replication job so we can create one with the testuser for the following test
	dbA.Exec(t, "CANCEL JOB $1", jobAID)
	jobutils.WaitForJobToCancel(t, dbA, jobAID)

	t.Run("create-on-schema", func(t *testing.T) {
		dbA.Exec(t, "CREATE SCHEMA testschema")

		testuser.ExpectErr(t, "user testuser does not have CREATE privilege on schema testschema", fmt.Sprintf(testingUDFAcceptProposedBaseWithSchema, "testschema", "tab"))
		dbA.Exec(t, "GRANT CREATE ON SCHEMA testschema TO testuser")
		testuser.Exec(t, fmt.Sprintf(testingUDFAcceptProposedBaseWithSchema, "testschema", "tab"))
	})

	t.Run("replication", func(t *testing.T) {
		createWithUDFStmt := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH DEFAULT FUNCTION = 'testschema.repl_apply'"
		testuser.ExpectErr(t, "user testuser does not have REPLICATION system privilege", createWithUDFStmt, dbBURL.String())
		dbA.Exec(t, fmt.Sprintf("GRANT SYSTEM REPLICATION TO %s", username.TestUser))
		testuser.QueryRow(t, createWithUDFStmt, dbBURL.String()).Scan(&jobAID)
	})

	t.Run("control-job", func(t *testing.T) {
		pauseJobStmt := "PAUSE JOB $1"
		testuser.ExpectErr(t, fmt.Sprintf("user testuser does not have privileges for job %s", jobAID), pauseJobStmt, jobAID)

		dbA.Exec(t, fmt.Sprintf("GRANT SYSTEM CONTROLJOB to %s", username.TestUser))
		testuser.Exec(t, pauseJobStmt, jobAID)
		jobutils.WaitForJobToPause(t, dbA, jobAID)
	})
}
