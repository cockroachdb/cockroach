// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	_ "github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient/randclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
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
)

func TestLogicalStreamIngestionJobNameResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
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

	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

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
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	defer TestingSetDLQ(fatalDLQ{t})()

	// keyPrefix will be set later, but before countPuts is set.
	for _, mode := range []string{"validated", "immediate"} {
		t.Run(mode, func(t *testing.T) {
			testLogicalStreamIngestionJobBasic(t, mode)
		})
	}
}

func testLogicalStreamIngestionJobBasic(t *testing.T, mode string) {
	ctx := context.Background()
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

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = $2", dbBURL.String(), mode).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = $2", dbAURL.String(), mode).Scan(&jobBID)

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

func TestLogicalStreamIngestionJobWithCursor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbA.Exec(t, "INSERT INTO tab VALUES (1, 'hello')")
	dbB.Exec(t, "INSERT INTO tab VALUES (1, 'goodbye')")

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

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

func TestCreateTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	skip.UnderRace(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, srv, sqlDBs, _ := setupServerWithNumDBs(t, ctx, testClusterBaseClusterArgs, 1, 1)
	defer tc.Stopper().Stop(ctx)

	sqlA := sqlDBs[0]
	aURL := replicationtestutils.GetExternalConnectionURI(t, srv, srv, serverutils.DBName("a"))

	t.Run("basic", func(t *testing.T) {
		// Ensure the offline scan replicates index spans.
		sqlA.Exec(t, "CREATE INDEX idx ON tab(payload)")

		// Insert a row that should replicate via the initial scan.
		sqlA.Exec(t, "INSERT INTO tab VALUES (1, 'hello')")

		sqlA.Exec(t, "CREATE DATABASE b")
		sqlB := sqlutils.MakeSQLRunner(srv.SQLConn(t, serverutils.DBName("b")))

		var jobID jobspb.JobID
		sqlB.QueryRow(t, "CREATE LOGICALLY REPLICATED TABLE b.tab FROM TABLE tab ON $1 WITH UNIDIRECTIONAL", aURL.String()).Scan(&jobID)

		// Check LWW on initial scan data.
		sqlA.Exec(t, "UPSERT INTO tab VALUES (1, 'howdy')")

		// Insert a row that should replicate during steady state.
		sqlA.Exec(t, "INSERT INTO tab VALUES (2, 'bye')")

		WaitUntilReplicatedTime(t, srv.Clock().Now(), sqlB, jobID)
		sqlB.CheckQueryResults(t, "SELECT * FROM tab", [][]string{{"1", "howdy"}, {"2", "bye"}})
		// Ensure secondary index was replicated as well.
		compareReplicatedTables(t, srv, "a", "b", "tab", sqlA, sqlB)
	})

	t.Run("pause initial scan", func(t *testing.T) {
		sqlA.Exec(t, "CREATE DATABASE c")
		sqlA.Exec(t, "CREATE TABLE tab2 (pk int primary key, payload string)")
		sqlc := sqlutils.MakeSQLRunner(srv.SQLConn(t, serverutils.DBName("c")))
		sqlc.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'logical_replication.after.retryable_error'")
		defer func() {
			sqlc.Exec(t, "RESET CLUSTER SETTING jobs.debug.pausepoints")
		}()

		var jobID jobspb.JobID
		sqlc.QueryRow(t, "CREATE LOGICALLY REPLICATED TABLE tab2 FROM TABLE tab2 ON $1 WITH UNIDIRECTIONAL", aURL.String()).Scan(&jobID)
		jobutils.WaitForJobToPause(t, sqlc, jobID)

		// Verify created tables are not visible as we paused before publishing
		// tables.
		var res int
		sqlc.QueryRow(t, "SELECT count(*) FROM [SHOW TABLES]").Scan(&res)
		require.Zero(t, res)

		sqlc.QueryRow(t, "SELECT count(*) FROM system.namespace WHERE name = 'tab2'").Scan(&res)
		require.Equal(t, 2, res)

		// First, cancel the initial scan to verify the tables have been dropped.
		sqlc.Exec(t, "CANCEL JOB $1", jobID)
		jobutils.WaitForJobToCancel(t, sqlc, jobID)
		sqlc.QueryRow(t, "SELECT count(*) FROM [SHOW TABLES]").Scan(&res)
		require.Zero(t, res)
		var dropTable string
		sqlc.QueryRow(t, "SELECT name FROM crdb_internal.tables WHERE database_name = 'c' AND state = 'DROP'").Scan(&dropTable)
		require.Equal(t, "tab2", dropTable)

		// Next, setup the ldr job again and wait for it to pause.
		//
		// TODO(msbutler): use the recently cancelled tab2 to ensure we can quickly
		// rerun LDR again. As you can see in the
		// restore-on-fail-or-cancel-fast-drop test, setting this up is a pain, so I
		// will address this in an upcoming pr.
		sqlc.QueryRow(t, "CREATE LOGICALLY REPLICATED TABLE tab FROM TABLE tab ON $1 WITH UNIDIRECTIONAL", aURL.String()).Scan(&jobID)
		jobutils.WaitForJobToPause(t, sqlc, jobID)

		// Next, resume it and wait for the table and its dlq table to come online.
		sqlc.Exec(t, "RESUME JOB $1", jobID)
		sqlc.CheckQueryResultsRetry(t, "SELECT count(*) FROM [SHOW TABLES]", [][]string{{"2"}})
	})
	t.Run("bidi", func(t *testing.T) {
		sqlA.Exec(t, "CREATE TABLE tab3 (pk int primary key, payload string)")
		sqlA.Exec(t, "INSERT INTO tab3 VALUES (1, 'hello')")

		sqlA.Exec(t, "CREATE DATABASE d")
		dURL := replicationtestutils.GetExternalConnectionURI(t, srv, srv, serverutils.DBName("d"))
		sqlD := sqlutils.MakeSQLRunner(srv.SQLConn(t, serverutils.DBName("d")))

		var jobID jobspb.JobID
		sqlD.QueryRow(t, "CREATE LOGICALLY REPLICATED TABLE tab3 FROM TABLE tab3 ON $1 WITH BIDIRECTIONAL ON $2", aURL.String(), dURL.String()).Scan(&jobID)
		WaitUntilReplicatedTime(t, srv.Clock().Now(), sqlD, jobID)
		sqlD.CheckQueryResultsRetry(t, `SELECT count(*) FROM [SHOW LOGICAL REPLICATION JOBS] WHERE tables = '{a.public.tab3}'`, [][]string{{"1"}})
		var reverseJobID jobspb.JobID
		sqlD.QueryRow(t, "SELECT job_id FROM [SHOW LOGICAL REPLICATION JOBS] WHERE tables = '{a.public.tab3}'").Scan(&reverseJobID)
		sqlD.Exec(t, "INSERT INTO tab3 VALUES (2, 'goodbye')")
		sqlA.Exec(t, "INSERT INTO tab3 VALUES (3, 'brb')")
		WaitUntilReplicatedTime(t, srv.Clock().Now(), sqlD, jobID)
		WaitUntilReplicatedTime(t, srv.Clock().Now(), sqlD, reverseJobID)
		compareReplicatedTables(t, srv, "a", "d", "tab3", sqlA, sqlD)
	})
	t.Run("create command errors", func(t *testing.T) {
		sqlA.Exec(t, "CREATE TABLE tab4 (pk int primary key, payload string)")
		sqlA.Exec(t, "INSERT INTO tab4 VALUES (1, 'hello')")

		sqlA.Exec(t, "CREATE DATABASE e")
		sqlE := sqlutils.MakeSQLRunner(srv.SQLConn(t, serverutils.DBName("e")))
		eURL := replicationtestutils.GetExternalConnectionURI(t, srv, srv, serverutils.DBName("e"))
		sqlE.ExpectErr(t, "either BIDIRECTIONAL or UNIDRECTIONAL must be specified", "CREATE LOGICALLY REPLICATED TABLE b.tab4 FROM TABLE tab4 ON $1", eURL.String())
		sqlE.ExpectErr(t, "UNIDIRECTIONAL and BIDIRECTIONAL cannot be specified together", "CREATE LOGICALLY REPLICATED TABLE tab4 FROM TABLE tab4 ON $1 WITH BIDIRECTIONAL ON $2, UNIDIRECTIONAL", aURL.String(), eURL.String())

		// Reverse stream uri not an external connection
		eURLRaw, cleanup := srv.PGUrl(t, serverutils.DBName("e"))
		defer cleanup()
		sqlE.ExpectErr(t, "reverse stream uri failed validation", "CREATE LOGICALLY REPLICATED TABLE tab4 FROM TABLE tab4 ON $1 WITH BIDIRECTIONAL ON $2", aURL.String(), eURLRaw.String())

	})
	t.Run("parent id check", func(t *testing.T) {
		sqlA.Exec(t, "CREATE DATABASE f")
		sqlF := sqlutils.MakeSQLRunner(srv.SQLConn(t, serverutils.DBName("f")))
		fURL := replicationtestutils.GetExternalConnectionURI(t, srv, srv, serverutils.DBName("f"))

		sqlF.Exec(t, "CREATE TABLE tab (pk int primary key, payload string)")

		var jobID, jobIDDupe, jobIDDiff jobspb.JobID
		repeatcmd := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH PARENT = '123'"
		sqlF.QueryRow(t, repeatcmd, fURL.String()).Scan(&jobID)
		sqlF.QueryRow(t, repeatcmd, fURL.String()).Scan(&jobIDDupe)
		require.Equal(t, jobID, jobIDDupe)
		sqlF.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH PARENT = '124'", fURL.String()).Scan(&jobIDDiff)
		require.NotEqual(t, jobID, jobIDDiff)
	})
}

// TestLogicalStreamIngestionAdvancePTS tests that the producer side pts advances
// as the destination side frontier advances.
func TestLogicalStreamIngestionAdvancePTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
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

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)

	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	// The ingestion job on cluster A has a pts on cluster B.
	producerJobIDB := replicationtestutils.GetProducerJobIDFromLDRJob(t, dbA, jobAID)
	replicationtestutils.WaitForPTSProtection(t, ctx, dbB, s, producerJobIDB, now)

	WaitUntilReplicatedTime(t, now, dbB, jobBID)
	producerJobIDA := replicationtestutils.GetProducerJobIDFromLDRJob(t, dbB, jobBID)
	replicationtestutils.WaitForPTSProtection(t, ctx, dbA, s, producerJobIDA, now)
}

// TestLogicalStreamIngestionCancelUpdatesProducerJob tests whether
// the producer job's OnFailOrCancel updates the the related producer
// job, resulting in the PTS record being removed.
func TestLogicalStreamIngestionCancelUpdatesProducerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbA.Exec(t, "SET CLUSTER SETTING physical_replication.producer.stream_liveness_track_frequency='50ms'")

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))

	var jobBID jobspb.JobID
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	WaitUntilReplicatedTime(t, s.Clock().Now(), dbB, jobBID)

	producerJobID := replicationtestutils.GetProducerJobIDFromLDRJob(t, dbB, jobBID)
	jobutils.WaitForJobToRun(t, dbA, producerJobID)

	dbB.Exec(t, "CANCEL JOB $1", jobBID)
	jobutils.WaitForJobToCancel(t, dbB, jobBID)
	jobutils.WaitForJobToFail(t, dbA, producerJobID)
	replicationtestutils.WaitForPTSProtectionToNotExist(t, ctx, dbA, s, producerJobID)
}

func TestRestoreFromLDR(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dataDir, dirCleanupFunc := testutils.TempDir(t)
	defer dirCleanupFunc()
	args := testClusterBaseClusterArgs
	args.ServerArgs.ExternalIODir = dataDir
	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, args, 1)
	defer server.Stopper().Stop(ctx)

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))

	var jobBID jobspb.JobID
	dbA.Exec(t, "INSERT INTO tab VALUES (1, 'hello')")
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	WaitUntilReplicatedTime(t, s.Clock().Now(), dbB, jobBID)

	dbB.Exec(t, "BACKUP DATABASE b INTO 'nodelocal://1/backup'")
	dbB.Exec(t, "RESTORE DATABASE b FROM LATEST IN 'nodelocal://1/backup' with new_db_name = 'c'")

	// Verify that the index backfill schema changes can run on the restored table.
	dbC := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("c")))
	dbC.Exec(t, "ALTER TABLE tab ADD COLUMN new_col INT DEFAULT 2")
	dbC.CheckQueryResults(t, "SELECT * FROM tab", [][]string{{"1", "hello", "2"}})
}

func TestImportIntoLDR(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dataDir, dirCleanupFunc := testutils.TempDir(t)
	defer dirCleanupFunc()
	args := testClusterBaseClusterArgs
	args.ServerArgs.ExternalIODir = dataDir
	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, args, 1)
	defer server.Stopper().Stop(ctx)

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))

	var jobBID jobspb.JobID
	dbA.Exec(t, "INSERT INTO tab VALUES (1, 'hello')")
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	WaitUntilReplicatedTime(t, s.Clock().Now(), dbB, jobBID)

	dbA.Exec(t, "EXPORT INTO CSV 'nodelocal://1/export1/' FROM SELECT * FROM tab;")
	dbA.ExpectErr(t, "which is apart of a Logical Data Replication stream", "IMPORT INTO tab CSV DATA ('nodelocal://1/export1/export*-n*.0.csv')")

	dbB.ExpectErr(t, "which is apart of a Logical Data Replication stream", "IMPORT INTO tab CSV DATA ('nodelocal://1/export1/export*-n*.0.csv')")
}

func TestLogicalStreamIngestionErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	server := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer server.Stopper().Stop(ctx)
	s := server.Server(0).ApplicationLayer()
	url := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))
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

	t.Run("rangefeed disabled", func(t *testing.T) {
		createQ := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab"
		if s.Codec().IsSystem() {
			dbB.ExpectErr(t, "kv.rangefeed.enabled must be enabled on the source cluster for logical replication", createQ, urlA)
			kvserver.RangefeedEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		}

		dbB.Exec(t, createQ, urlA)
	})

	t.Run("multi stmt creation", func(t *testing.T) {
		db := dbB.DB.(*gosql.DB)
		var jobID jobspb.JobID
		err := crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
			return tx.QueryRow("CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", urlA).Scan(&jobID)
		})
		require.True(t, testutils.IsError(err,
			"cannot CREATE LOGICAL REPLICATION STREAM in a multi-statement transaction"))
	})

	t.Run("not external conn", func(t *testing.T) {
		sourceURI, cleanup := s.PGUrl(t, serverutils.DBName("a"))
		defer cleanup()
		dbB.ExpectErr(t, "uri must be an external connection", "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", sourceURI.String())
	})

}

func TestLogicalStreamIngestionJobWithColumnFamilies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	skip.IgnoreLint(t, "column families are not supported yet by LDR")

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

	serverASQL.Exec(t, "INSERT INTO tab_with_cf(pk, payload, other_payload) VALUES (1, 'hello', 'ruroh1')")

	serverAURL := replicationtestutils.GetExternalConnectionURI(t, s, s)
	serverAURL.Path = "a"

	var jobBID jobspb.JobID
	serverBSQL.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab_with_cf ON $1 INTO TABLE tab_with_cf WITH MODE = validated", serverAURL.String()).Scan(&jobBID)

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
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, s, serverASQL, serverBSQL := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer tc.Stopper().Stop(ctx)

	serverAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))

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
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "multi cluster/node config exhausts hardware")

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	server, s, dbs, _ := setupServerWithNumDBs(t, ctx, clusterArgs, 1, 3)
	defer server.Stopper().Stop(ctx)

	dbA, dbB, dbC := dbs[0], dbs[1], dbs[2]

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	var jobAID, jobBID, jobCID jobspb.JobID

	// Add a TTL; it won't kick in the short-lived test and won't delete anything,
	// but to enable filtering it has to be present since otherwise we think the
	// user forgot it. We'll get a delete to ignore manually later so this is only
	// for the creation check.
	dbA.Exec(t, "ALTER TABLE a.tab SET (ttl_disable_changefeed_replication=true,ttl_expiration_expression='now()')")
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE b.public.tab ON $1 INTO TABLE a.tab", dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE a.tab ON $1 INTO TABLE b.tab WITH DISCARD = 'ttl-deletes'", dbAURL.String()).Scan(&jobBID)
	dbC.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE a.tab ON $1 INTO TABLE c.tab WITH DISCARD = 'all-deletes'", dbAURL.String()).Scan(&jobCID)

	dbA.Exec(t, "INSERT INTO a.tab VALUES (0, 'a'), (1, 'b'), (2, 'c')")
	dbA.Exec(t, "UPDATE a.tab SET payload = 'x' WHERE pk = 0")

	dbA.Exec(t, "DELETE FROM a.tab WHERE pk = 2")

	// Delete a row with omit in rangefeeds true, the same way a TTL job with it
	// set would.
	dbA.Exec(t, "SET disable_changefeed_replication = true")
	dbA.Exec(t, "DELETE FROM a.tab WHERE pk = 1")
	dbA.Exec(t, "SET disable_changefeed_replication = false")

	now := server.Server(0).Clock().Now()
	t.Logf("waiting for replication job %d", jobAID)
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	t.Logf("waiting for replication job %d", jobBID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)
	t.Logf("waiting for replication job %d", jobCID)
	WaitUntilReplicatedTime(t, now, dbB, jobCID)

	// Verify that Job contains FilterRangeFeed
	require.Equal(t, jobspb.LogicalReplicationDetails_DiscardNothing,
		jobutils.GetJobPayload(t, dbA, jobAID).GetLogicalReplicationDetails().Discard)
	require.Equal(t, jobspb.LogicalReplicationDetails_DiscardCDCIgnoredTTLDeletes,
		jobutils.GetJobPayload(t, dbB, jobBID).GetLogicalReplicationDetails().Discard)
	require.Equal(t, jobspb.LogicalReplicationDetails_DiscardAllDeletes,
		jobutils.GetJobPayload(t, dbC, jobCID).GetLogicalReplicationDetails().Discard)

	// A had both rows deleted, and zero updated.
	dbA.CheckQueryResults(t, "SELECT * from a.tab", [][]string{{"0", "x"}})

	// B ignored the delete of 'b' done in a session that had the omit bit similar
	// to a TTL job, but did replicate the (normal) delete of 'c'.
	dbB.CheckQueryResults(t, "SELECT * from b.tab", [][]string{{"0", "x"}, {"1", "b"}})

	// C ignored all deletes and still has all three rows.
	dbC.CheckQueryResults(t, "SELECT * from c.tab", [][]string{{"0", "x"}, {"1", "b"}, {"2", "c"}})
}

func TestRandomTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, s, runnerA, runnerB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer tc.Stopper().Stop(ctx)

	sqlA := s.SQLConn(t, serverutils.DBName("a"))

	var tableName, streamStartStmt string
	rng, _ := randutil.NewPseudoRand()

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))

	// Keep retrying until the random table satisfies all the static checks
	// we make when creating the replication stream.
	var i int
	for {
		tableName = fmt.Sprintf("rand_table_%d", i)
		streamStartStmt = fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLE %[1]s ON $1 INTO TABLE %[1]s", tableName)
		i++
		createStmt := randgen.RandCreateTableWithName(
			ctx,
			rng,
			tableName,
			1,
			randgen.TableOptSkipColumnFamilyMutations)
		stmt := tree.SerializeForDisplay(createStmt)
		t.Log(stmt)
		runnerA.Exec(t, stmt)
		runnerB.Exec(t, stmt)

		var jobBID jobspb.JobID
		err := runnerB.DB.QueryRowContext(ctx, streamStartStmt, dbAURL.String()).Scan(&jobBID)
		if err != nil {
			t.Log(err)
			continue
		}

		// Kill replication job. The one we want to test with is created further
		// below.
		runnerB.Exec(t, "CANCEL JOB $1", jobBID)
		jobutils.WaitForJobToCancel(t, runnerB, jobBID)
		break
	}

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

	var jobBID jobspb.JobID
	runnerB.QueryRow(t, streamStartStmt, dbAURL.String()).Scan(&jobBID)

	WaitUntilReplicatedTime(t, s.Clock().Now(), runnerB, jobBID)
	require.NoError(t, replicationtestutils.CheckEmptyDLQs(ctx, runnerB.DB, "b"))
	compareReplicatedTables(t, s, "a", "b", tableName, runnerA, runnerB)
}

func TestRandomStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	eventCount := 100
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
	skip.UnderDeadlock(t)
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
	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableName := fmt.Sprintf("%s%d", baseTableName, i)
			schemaStmt := strings.ReplaceAll(tc.schema, baseTableName, tableName)
			runnerA.Exec(t, schemaStmt)
			runnerB.Exec(t, schemaStmt)

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
			require.NoError(t, replicationtestutils.CheckEmptyDLQs(ctx, runnerB.DB, "b"))
			compareReplicatedTables(t, s, "a", "b", tableName, runnerA, runnerB)
		})
	}
}

// TestLogicalAutoReplan asserts that if a new node can participate in the
// logical replication job, it will trigger distSQL replanning.
func TestLogicalAutoReplan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 131184)

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

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

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
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 131184)

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

	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	CreateScatteredTable(t, dbB, 2, "B")

	var jobAID jobspb.JobID
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)

	progress := jobutils.GetJobProgress(t, dbA, jobAID)
	addresses := progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.PartitionConnUris

	require.Greaterf(t, len(addresses), 1, "Less than 2 addresses were persisted in system.job_info")
}

func TestHeartbeatCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
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

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, dbA, jobAID)
	WaitUntilReplicatedTime(t, now, dbB, jobBID)

	prodAID := replicationtestutils.GetProducerJobIDFromLDRJob(t, dbB, jobBID)

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
	skip.UnderDeadlock(t)
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

	PGURLs := GetPGURLs(t, s, dbNames)

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

// TestThreeWayReplication ensures LWW works on a fully connected graph of
// streams across 3 databases.
func TestThreeWayReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "running 6 LDR jobs on one server is too much")

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

	numDBs := 3
	server, s, runners, dbNames := setupServerWithNumDBs(t, ctx, clusterArgs, 1, numDBs)
	defer server.Stopper().Stop(ctx)

	PGURLs := GetPGURLs(t, s, dbNames)

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

	// Assert Row 2's update wins LWW.
	for i := range numDBs {
		runners[i].Exec(t, fmt.Sprintf("UPSERT INTO tab VALUES (2, 'row%v')", i))
	}
	now = s.Clock().Now()
	waitUntilReplicatedTimeAllServers(t, now, runners, jobIDs)

	expectedRows = [][]string{
		{"1", "celery"},
		{"2", "row2"},
	}
	verifyExpectedRowAllServers(t, runners, expectedRows, dbNames)
}

func TestForeignKeyConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
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

	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	dbA.Exec(t, "CREATE TABLE test(a int primary key, b int)")

	testutils.RunTrueAndFalse(t, "immediate-mode", func(t *testing.T, immediateMode bool) {
		fkStmt := "ALTER TABLE test ADD CONSTRAINT fkc FOREIGN KEY (b) REFERENCES tab(pk)"
		dbA.Exec(t, fkStmt)

		var mode string
		if immediateMode {
			mode = "IMMEDIATE"
		} else {
			mode = "VALIDATED"
		}

		var jobID jobspb.JobID
		stmt := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH MODE = " + mode
		if immediateMode {
			dbA.ExpectErr(t, "foreign keys are only supported with MODE = 'validated'", stmt, dbBURL.String())
		} else {
			dbA.QueryRow(t, stmt, dbBURL.String()).Scan(&jobID)
			dbA.Exec(t, "CANCEL JOB $1", jobID)
			jobutils.WaitForJobToCancel(t, dbA, jobID)
		}

		dbA.Exec(t, "ALTER TABLE test DROP CONSTRAINT fkc")
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

	_, err = server.Conns[0].Exec("SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '1s'")
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
		if indexA.GetType() == idxtype.INVERTED {
			t.Logf("skipping fingerprinting of inverted index %s", indexA.GetName())
			continue
		}

		indexB, err := catalog.MustFindIndexByName(descB, indexA.GetName())
		require.NoError(t, err)

		aFingerprintQuery, err := sql.BuildFingerprintQueryForIndex(descA, indexA, []string{})
		require.NoError(t, err)
		bFingerprintQuery, err := sql.BuildFingerprintQueryForIndex(descB, indexB, []string{})
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

func GetPGURLs(t *testing.T, s serverutils.ApplicationLayerInterface, dbNames []string) []url.URL {
	result := []url.URL{}
	for _, name := range dbNames {
		resultURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName(name))
		result = append(result, resultURL)
	}
	return result
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

type mockBatchHandler struct {
	err error
}

var _ BatchHandler = &mockBatchHandler{}

func (m mockBatchHandler) HandleBatch(
	_ context.Context, _ []streampb.StreamEvent_KV,
) (batchStats, error) {
	if m.err != nil {
		return batchStats{}, m.err
	}
	return batchStats{}, nil
}
func (m mockBatchHandler) GetLastRow() cdcevent.Row            { return cdcevent.Row{} }
func (m mockBatchHandler) SetSyntheticFailurePercent(_ uint32) {}
func (m mockBatchHandler) Close(context.Context)               {}
func (m mockBatchHandler) ReportMutations(_ *stats.Refresher)  {}
func (m mockBatchHandler) ReleaseLeases(_ context.Context)     {}

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
	skip.UnderDeadlock(t)

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
	lrw.purgatory.debug = &streampb.DebugLogicalConsumerStatus{}

	lrw.bh = []BatchHandler{(mockBatchHandler{pgerror.New(pgcode.UniqueViolation, "index write conflict")})}
	lrw.bhStats = make([]flushStats, 1)

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
	skip.UnderDeadlock(t)
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

	lwwFunc := `CREATE OR REPLACE FUNCTION repl_apply(action STRING, proposed tab, existing tab, prev tab, existing_mvcc_timestamp DECIMAL, existing_origin_timestamp DECIMAL, proposed_mvcc_timestamp DECIMAL)
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

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

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
	skip.UnderDeadlock(t)
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
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, testClusterBaseClusterArgs, 1)
	defer server.Stopper().Stop(ctx)

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"), serverutils.UserPassword(username.RootUser, "password"))
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"), serverutils.UserPassword(username.RootUser, "password"))

	var (
		jobAID jobspb.JobID
		jobBID jobspb.JobID
	)

	cmdA := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab"
	cmdB := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH OPTIONS (DEFAULT FUNCTION = 'dlq')"
	dbA.QueryRow(t,
		cmdA,
		dbBURL.String()).Scan(&jobAID)
	dbB.QueryRow(t,
		cmdB,
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
		command                string
	)

	showRows := dbA.Query(t, "SELECT * FROM [SHOW LOGICAL REPLICATION JOBS] ORDER BY job_id")
	defer showRows.Close()

	rowIdx := 0
	for showRows.Next() {
		err := showRows.Scan(&jobID, &status, &targets, &replicatedTime)
		require.NoError(t, err)

		expectedJobID := jobIDs[rowIdx]
		require.Equal(t, expectedJobID, jobID)
		require.Equal(t, jobs.StateRunning, jobs.State(status))

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
			&command)
		require.NoError(t, err)

		expectedJobID := jobIDs[rowIdx]
		payload := jobutils.GetJobPayload(t, dbA, expectedJobID)
		expectedReplicationStartTime := payload.GetLogicalReplicationDetails().ReplicationStartTime.GoTime().Round(time.Microsecond)
		require.Equal(t, expectedReplicationStartTime, replicationStartTime)

		expectedConflictResolutionType := payload.GetLogicalReplicationDetails().DefaultConflictResolution.ConflictResolutionType.String()
		require.Equal(t, expectedConflictResolutionType, conflictResolutionType)

		expectedJobDescription := payload.Description

		// Verify that URL is redacted in job descriptions. Note these do not appear
		// in SHOW LDR JOBS, but do in the db console description.
		redactedDbAURL := strings.Replace(dbAURL.String(), "password", `redacted`, 1)
		redactedDbBURL := strings.Replace(dbBURL.String(), "password", `redacted`, 1)
		redactedJobADescription := fmt.Sprintf("LOGICAL REPLICATION STREAM into a.public.tab from %s", redactedDbBURL)
		redactedJobBDescription := fmt.Sprintf("LOGICAL REPLICATION STREAM into b.public.tab from %s", redactedDbAURL)
		if jobID == jobAID {
			require.Equal(t, redactedJobADescription, expectedJobDescription)
			require.Equal(t, cmdA, command)

		} else if jobID == jobBID {
			require.Equal(t, redactedJobBDescription, expectedJobDescription)
			require.Equal(t, cmdB, command)
		}

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
	skip.UnderDeadlock(t)
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

	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	// Create user with no privileges
	dbA.Exec(t, fmt.Sprintf("CREATE USER %s", username.TestUser))
	dbA.Exec(t, fmt.Sprintf("CREATE USER %s", username.TestUser+"2"))
	dbA.Exec(t, fmt.Sprintf("GRANT SYSTEM REPLICATION TO %s", username.TestUser+"2"))
	testuser := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.User(username.TestUser), serverutils.DBName("a")))
	testuser2 := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.User(username.TestUser+"2"), serverutils.DBName("a")))

	var jobAID jobspb.JobID
	createStmt := "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab"
	testuser2.QueryRow(t, createStmt, dbBURL.String()).Scan(&jobAID)

	t.Run("view-control-job", func(t *testing.T) {
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

		pauseJobStmt := "PAUSE JOB $1"
		testuser.ExpectErr(t, fmt.Sprintf("user testuser does not have privileges for job %s", jobAID), pauseJobStmt, jobAID)

		dbA.Exec(t, fmt.Sprintf("GRANT SYSTEM CONTROLJOB to %s", username.TestUser))
		testuser.Exec(t, pauseJobStmt, jobAID)
		jobutils.WaitForJobToPause(t, dbA, jobAID)
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

	t.Run("replication-dest", func(t *testing.T) {
		testuser.ExpectErr(t, "user testuser does not have REPLICATION system privilege", createStmt, dbBURL.String())
		dbA.Exec(t, fmt.Sprintf("GRANT SYSTEM REPLICATION TO %s", username.TestUser))
		testuser.QueryRow(t, createStmt, dbBURL.String()).Scan(&jobAID)
	})
	t.Run("replication-src", func(t *testing.T) {
		dbB.Exec(t, "CREATE USER testuser3")
		dbBURL2 := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"), serverutils.User(username.TestUser+"3"))
		testuser.ExpectErr(t, "user testuser3 does not have REPLICATION system privilege", createStmt, dbBURL2.String())
		dbB.Exec(t, fmt.Sprintf("GRANT SYSTEM REPLICATION TO %s", username.TestUser+"3"))
		testuser.QueryRow(t, createStmt, dbBURL2.String()).Scan(&jobAID)
	})
}

// TestLogicalReplicationSchemaChanges verifies that only certain schema changes
// are allowed on tables participating in logical replication.
//
// NOTE: for trivial schema allow list testing, add a test to
// TestIsAllowedLDRSchemaChange instead.
func TestLogicalReplicationSchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
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

	// Add some stuff to tables in prep for schema change testing.
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN virtual_col INT NOT NULL AS (pk + 1) VIRTUAL")
	dbB.Exec(t, "ALTER TABLE b.tab ADD COLUMN virtual_col INT NOT NULL AS (pk + 1) VIRTUAL")
	dbA.Exec(t, "CREATE OR REPLACE FUNCTION my_trigger() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END $$ LANGUAGE PLPGSQL")
	dbB.Exec(t, "CREATE OR REPLACE FUNCTION my_trigger() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END $$ LANGUAGE PLPGSQL")
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN composite_col DECIMAL NOT NULL")
	dbB.Exec(t, "ALTER TABLE b.tab ADD COLUMN composite_col DECIMAL NOT NULL")

	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	var jobAID jobspb.JobID
	dbA.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String()).Scan(&jobAID)

	// Changing safe table storage parameters is allowed.
	// table driven test schema changes
	testCases := []struct {
		name    string
		cmd     string
		allowed bool
	}{

		// add and drop index allowed on composite column
		{"add index", "CREATE INDEX idx ON tab(composite_col)", true},
		{"drop index", "DROP INDEX idx", true},

		// adding unsuppored indexes disallowed
		{"add virtual column index", "CREATE INDEX virtual_col_idx ON tab(virtual_col)", false},
		{"add hash index", "CREATE INDEX hash_idx ON tab(pk) USING HASH WITH (bucket_count = 4)", false},
		{"add partial index", "CREATE INDEX partial_idx ON tab(composite_col) WHERE pk > 0", false},
		{"add unique index", "CREATE UNIQUE INDEX unique_idx ON tab(composite_col)", false},

		// Drop table is blocked
		{"drop table", "DROP TABLE tab", false},

		// Dissalow storage param updates if is not the only change.
		{"storage param update", "ALTER TABLE tab ADD COLUMN C INT, SET (fillfactor = 70)", false},
		{"storage param update", "ALTER TABLE tab SET (fillfactor = 70)", true},

		// Allow ttl schema changes that do not conduct a backfill.
		{"reset ttl", "ALTER TABLE tab RESET (ttl)", false},
		{"ttl expression", "ALTER TABLE tab SET (ttl_expiration_expression = $$ '2024-01-01 12:00:00'::TIMESTAMPTZ $$)", true},
		{"ttl on", "ALTER TABLE tab SET (ttl = 'on', ttl_expire_after = '5m')", false},

		{"trigger", "CREATE TRIGGER my_trigger BEFORE INSERT ON tab FOR EACH ROW EXECUTE FUNCTION my_trigger()", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.allowed {
				dbA.Exec(t, tc.cmd)
				dbB.Exec(t, tc.cmd)
			} else {
				expectedErr := "this schema change is disallowed on table tab because it is referenced by one or more logical replication jobs"
				dbA.ExpectErr(t, expectedErr, tc.cmd)
				dbB.ExpectErr(t, expectedErr, tc.cmd)
			}
		})
	}

	// Kill replication job and verify that one of the schema changes work now to
	// verify the schema lock has been lifted from the target tables.
	dbA.Exec(t, "CANCEL JOB $1", jobAID)
	jobutils.WaitForJobToCancel(t, dbA, jobAID)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)
	dbA.Exec(t, testCases[0].cmd)
	dbB.Exec(t, testCases[0].cmd)
}

// TestUserDefinedTypes verifies that user-defined types are correctly
// replicated if the type is defined identically on both sides.
func TestUserDefinedTypes(t *testing.T) {
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

	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	// Create the same user-defined type both tables.
	dbA.Exec(t, "CREATE TYPE my_enum AS ENUM ('one', 'two', 'three')")
	dbB.Exec(t, "CREATE TYPE my_enum AS ENUM ('one', 'two', 'three')")
	dbA.Exec(t, "CREATE TYPE my_composite AS (a INT, b TEXT)")
	dbB.Exec(t, "CREATE TYPE my_composite AS (a INT, b TEXT)")

	for _, mode := range []string{"validated", "immediate"} {
		t.Run(mode, func(t *testing.T) {
			dbA.Exec(t, "CREATE TABLE data (pk INT PRIMARY KEY, val1 my_enum DEFAULT 'two', val2 my_composite)")
			dbA.Exec(t, "CREATE TABLE data2 (pk INT PRIMARY KEY, val1 my_enum DEFAULT 'two', val2 my_composite)")
			dbB.Exec(t, "CREATE TABLE data (pk INT PRIMARY KEY, val1 my_enum DEFAULT 'two', val2 my_composite)")
			dbB.Exec(t, "CREATE TABLE data2 (pk INT PRIMARY KEY, val1 my_enum DEFAULT 'two', val2 my_composite)")

			dbB.Exec(t, "INSERT INTO data VALUES (1, 'one', (3, 'cat'))")
			// Force default expression evaluation.
			dbB.Exec(t, "INSERT INTO data (pk, val2) VALUES (2, (4, 'dog'))")

			var jobAID jobspb.JobID
			dbA.QueryRow(t,
				fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLES (data, data2) ON $1 INTO TABLES (data, data2) WITH mode = %s", mode),
				dbBURL.String(),
			).Scan(&jobAID)
			WaitUntilReplicatedTime(t, s.Clock().Now(), dbA, jobAID)
			require.NoError(t, replicationtestutils.CheckEmptyDLQs(ctx, dbA.DB, "A"))
			dbB.CheckQueryResults(t, "SELECT * FROM data", [][]string{{"1", "one", "(3,cat)"}, {"2", "two", "(4,dog)"}})
			dbA.CheckQueryResults(t, "SELECT * FROM data", [][]string{{"1", "one", "(3,cat)"}, {"2", "two", "(4,dog)"}})

			var jobBID jobspb.JobID
			dbB.QueryRow(t,
				"SELECT job_id FROM [SHOW JOBS]"+
					"WHERE job_type = 'REPLICATION STREAM PRODUCER' AND status = 'running'",
			).Scan(&jobBID)

			dbA.Exec(t, "CANCEL JOB $1", jobAID)
			jobutils.WaitForJobToCancel(t, dbA, jobAID)
			jobutils.WaitForJobToFail(t, dbB, jobBID)

			dbA.Exec(t, "DROP TABLE data")
			dbA.Exec(t, "DROP TABLE data2")
			dbB.Exec(t, "DROP TABLE data")
			dbB.Exec(t, "DROP TABLE data2")
		})
	}
}

func TestLogicalReplicationGatewayRoute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a blackhole so we can claim a port and black hole any connections
	// routed there.
	blackhole, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, blackhole.Close())
	}()

	t.Log("blackhole listening on", blackhole.Addr())
	// Set the SQL advertise addr to something unroutable so that we know the
	// config connection url was used for all streams.
	args := testClusterBaseClusterArgs
	args.ServerArgs.Knobs.Streaming = &sql.StreamingTestingKnobs{
		OnGetSQLInstanceInfo: func(node *roachpb.NodeDescriptor) *roachpb.NodeDescriptor {
			copy := *node
			copy.SQLAddress = util.UnresolvedAddr{
				NetworkField: "tcp",
				AddressField: blackhole.Addr().String(),
			}
			return &copy
		},
	}
	ts, s, runners, dbs := setupServerWithNumDBs(t, context.Background(), args, 1, 2)
	defer ts.Stopper().Stop(context.Background())

	sourceUrl, cleanup := s.PGUrl(t, serverutils.DBName(dbs[1]))
	defer cleanup()

	q := sourceUrl.Query()
	q.Set(streamclient.RoutingModeKey, string(streamclient.RoutingModeGateway))
	sourceUrl.RawQuery = q.Encode()

	externalUri := url.URL{Scheme: "external", Host: "replication-uri"}
	runners[1].Exec(t, fmt.Sprintf("CREATE EXTERNAL CONNECTION '%s' AS '%s'", externalUri.Host, sourceUrl.String()))

	var jobID jobspb.JobID
	runners[0].QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", externalUri.String()).Scan(&jobID)
	runners[1].Exec(t, "INSERT INTO tab VALUES (1, 'hello')")

	now := s.Clock().Now()
	WaitUntilReplicatedTime(t, now, runners[0], jobID)

	progress := jobutils.GetJobProgress(t, runners[0], jobID)
	require.Empty(t, progress.Details.(*jobspb.Progress_LogicalReplication).LogicalReplication.PartitionConnUris)
}

// TestLogicalReplicationCreationChecks verifies that we check that the table
// schemas are compatible when creating the replication stream.
func TestLogicalReplicationCreationChecks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
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

	dbBURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("b"))

	// Column families are not allowed.
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN new_col INT NOT NULL CREATE FAMILY f1")
	dbB.Exec(t, "ALTER TABLE b.tab ADD COLUMN new_col INT NOT NULL")
	dbA.ExpectErr(t,
		"cannot create logical replication stream: table tab has more than one column family",
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// UniqueWithoutIndex constraints are not allowed.
	for _, db := range []*sqlutils.SQLRunner{dbA, dbB} {
		db.Exec(t, "SET experimental_enable_unique_without_index_constraints = true")
		db.Exec(t, "CREATE TABLE tab_with_uwi (pk INT PRIMARY KEY, v INT UNIQUE WITHOUT INDEX)")
	}
	dbA.ExpectErr(t,
		"cannot create logical replication stream: table tab_with_uwi has UNIQUE WITHOUT INDEX constraints: unique_v",
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab_with_uwi ON $1 INTO TABLE tab_with_uwi", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check for mismatched numbers of columns.
	dbA.Exec(t, "ALTER TABLE tab DROP COLUMN new_col")
	dbA.ExpectErr(t,
		"cannot create logical replication stream: destination table tab has 2 columns, but the source table tab has 3 columns",
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check for mismatched column types.
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN new_col TEXT NOT NULL")
	dbA.ExpectErr(t,
		"cannot create logical replication stream: destination table tab column new_col has type STRING, but the source table tab has type INT8",
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check for composite type in primary key.
	dbA.Exec(t, "ALTER TABLE tab DROP COLUMN new_col")
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN new_col INT NOT NULL")
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN composite_col DECIMAL NOT NULL")
	dbB.Exec(t, "ALTER TABLE b.tab ADD COLUMN composite_col DECIMAL NOT NULL")
	dbA.Exec(t, "ALTER TABLE tab ALTER PRIMARY KEY USING COLUMNS (pk, composite_col)")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: table tab has a primary key column \(composite_col\) with composite encoding`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check for partial indexes.
	dbA.Exec(t, "ALTER TABLE tab ALTER PRIMARY KEY USING COLUMNS (pk)")
	dbA.Exec(t, "CREATE INDEX partial_idx ON tab(composite_col) WHERE pk > 0")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: table tab has a partial index partial_idx`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check for virtual computed columns that are a key of a secondary index.
	dbA.Exec(t, "DROP INDEX partial_idx")
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN virtual_col INT NOT NULL AS (pk + 1) VIRTUAL")
	dbB.Exec(t, "ALTER TABLE b.tab ADD COLUMN virtual_col INT NOT NULL AS (pk + 1) VIRTUAL")
	dbA.Exec(t, "CREATE INDEX virtual_col_idx ON tab(virtual_col)")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: table tab has a virtual computed column virtual_col that is a key of index virtual_col_idx`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check for virtual columns that are in the primary index.
	dbA.Exec(t, "DROP INDEX virtual_col_idx")
	dbA.Exec(t, "ALTER TABLE tab ALTER PRIMARY KEY USING COLUMNS (pk, virtual_col)")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: table tab has a virtual computed column virtual_col that appears in the primary key`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Change the primary key back, and remove the indexes that are left over from
	// changing the PK.
	dbA.Exec(t, "ALTER TABLE tab ALTER PRIMARY KEY USING COLUMNS (pk)")
	dbA.Exec(t, "DROP INDEX tab_pk_virtual_col_key")
	dbA.Exec(t, "DROP INDEX tab_pk_key")
	dbA.Exec(t, "DROP INDEX tab_pk_composite_col_key")

	// Check that CHECK constraints match.
	dbA.Exec(t, "ALTER TABLE tab ADD CONSTRAINT check_constraint_1 CHECK (pk > 0)")
	dbB.Exec(t, "ALTER TABLE b.tab ADD CONSTRAINT check_constraint_1 CHECK (length(payload) > 1)")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: destination table tab CHECK constraints do not match source table tab`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Allow user to create LDR stream with mismatched CHECK via SKIP SCHEMA CHECK.
	var jobIDSkipSchemaCheck jobspb.JobID
	dbA.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH SKIP SCHEMA CHECK",
		dbBURL.String(),
	).Scan(&jobIDSkipSchemaCheck)
	dbA.Exec(t, "CANCEL JOB $1", jobIDSkipSchemaCheck)
	jobutils.WaitForJobToCancel(t, dbA, jobIDSkipSchemaCheck)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Add missing CHECK constraints, and verify that the stream can be created.
	dbA.Exec(t, "ALTER TABLE tab ADD CONSTRAINT check_constraint_2 CHECK (length(payload) > 1)")
	dbB.Exec(t, "ALTER TABLE b.tab ADD CONSTRAINT check_constraint_2 CHECK (pk > 0)")
	var jobAID jobspb.JobID
	dbA.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab",
		dbBURL.String(),
	).Scan(&jobAID)

	// Kill replication job.
	dbA.Exec(t, "CANCEL JOB $1", jobAID)
	jobutils.WaitForJobToCancel(t, dbA, jobAID)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check if the table references a UDF.
	dbA.Exec(t, "CREATE OR REPLACE FUNCTION my_udf() RETURNS INT AS $$ SELECT 1 $$ LANGUAGE SQL")
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN udf_col INT NOT NULL")
	dbA.Exec(t, "ALTER TABLE tab ALTER COLUMN udf_col SET DEFAULT my_udf()")
	dbB.Exec(t, "ALTER TABLE tab ADD COLUMN udf_col INT NOT NULL DEFAULT 1")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: table tab references functions with IDs \[[0-9]+\]`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check if the table references a sequence.
	dbA.Exec(t, "ALTER TABLE tab DROP COLUMN udf_col")
	dbB.Exec(t, "ALTER TABLE tab DROP COLUMN udf_col")
	dbA.Exec(t, "CREATE SEQUENCE my_seq")
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN seq_col INT NOT NULL DEFAULT nextval('my_seq')")
	dbB.Exec(t, "ALTER TABLE tab ADD COLUMN seq_col INT NOT NULL DEFAULT 1")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: table tab references sequences with IDs \[[0-9]+\]`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check if table has a trigger.
	dbA.Exec(t, "ALTER TABLE tab DROP COLUMN seq_col")
	dbB.Exec(t, "ALTER TABLE tab DROP COLUMN seq_col")
	dbA.Exec(t, "CREATE OR REPLACE FUNCTION my_trigger() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END $$ LANGUAGE PLPGSQL")
	dbA.Exec(t, "CREATE TRIGGER my_trigger BEFORE INSERT ON tab FOR EACH ROW EXECUTE FUNCTION my_trigger()")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: table tab references triggers \[my_trigger\]`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Verify that the stream cannot be created with mismatched enum types.
	dbA.Exec(t, "DROP TRIGGER my_trigger ON tab")
	dbA.Exec(t, "CREATE TYPE mytype AS ENUM ('a', 'b', 'c')")
	dbB.Exec(t, "CREATE TYPE b.mytype AS ENUM ('a', 'b')")
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN enum_col mytype NOT NULL")
	dbB.Exec(t, "ALTER TABLE b.tab ADD COLUMN enum_col b.mytype NOT NULL")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: .* destination type USER DEFINED ENUM: public.mytype has logical representations \[a b c\], but the source type USER DEFINED ENUM: mytype has \[a b\]`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Allows user to create LDR stream with UDT via SKIP SCHEMA CHECK.
	dbA.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab WITH SKIP SCHEMA CHECK",
		dbBURL.String(),
	).Scan(&jobIDSkipSchemaCheck)
	dbA.Exec(t, "CANCEL JOB $1", jobIDSkipSchemaCheck)
	jobutils.WaitForJobToCancel(t, dbA, jobIDSkipSchemaCheck)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Verify that the stream cannot be created with mismatched composite types.
	dbA.Exec(t, "ALTER TABLE tab DROP COLUMN enum_col")
	dbB.Exec(t, "ALTER TABLE b.tab DROP COLUMN enum_col")
	dbA.Exec(t, "CREATE TYPE composite_typ AS (a INT, b TEXT)")
	dbB.Exec(t, "CREATE TYPE b.composite_typ AS (a TEXT, b INT)")
	dbA.Exec(t, "ALTER TABLE tab ADD COLUMN composite_udt_col composite_typ NOT NULL")
	dbB.Exec(t, "ALTER TABLE b.tab ADD COLUMN composite_udt_col b.composite_typ NOT NULL")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: .* destination type USER DEFINED RECORD: public.composite_typ tuple element 0 does not match source type USER DEFINED RECORD: composite_typ tuple element 0: destination type INT8 does not match source type STRING`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Check that UNIQUE indexes match.
	dbA.Exec(t, "ALTER TABLE tab DROP COLUMN composite_udt_col")
	dbB.Exec(t, "ALTER TABLE b.tab DROP COLUMN composite_udt_col")
	dbA.Exec(t, "CREATE UNIQUE INDEX payload_idx ON tab(payload)")
	dbB.Exec(t, "CREATE UNIQUE INDEX multi_idx ON b.tab(composite_col, pk)")
	dbA.ExpectErr(t,
		`cannot create logical replication stream: destination table tab UNIQUE indexes do not match source table tab`,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbBURL.String(),
	)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Create the missing indexes on each side and verify the stream can be
	// created. Note that the indexes don't need to be created in the same order
	// for the check to pass.
	dbA.Exec(t, "CREATE UNIQUE INDEX multi_idx ON tab(composite_col, pk)")
	dbB.Exec(t, "CREATE UNIQUE INDEX payload_idx ON b.tab(payload)")
	dbA.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab",
		dbBURL.String(),
	).Scan(&jobAID)

	// Kill replication job.
	dbA.Exec(t, "CANCEL JOB $1", jobAID)
	jobutils.WaitForJobToCancel(t, dbA, jobAID)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)

	// Add different default values to to the source and dest, verify the stream
	// can be created, and that the default value is sent over the wire.
	dbA.Exec(t, "CREATE TABLE tab2 (pk INT PRIMARY KEY, payload STRING DEFAULT 'cat')")
	dbB.Exec(t, "CREATE TABLE b.tab2 (pk INT PRIMARY KEY, payload STRING DEFAULT 'dog')")
	dbB.Exec(t, "Insert into tab2 values (1)")
	dbA.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE tab2 ON $1 INTO TABLE tab2",
		dbBURL.String(),
	).Scan(&jobAID)
	WaitUntilReplicatedTime(t, s.Clock().Now(), dbA, jobAID)
	dbA.CheckQueryResults(t, "SELECT * FROM tab2", [][]string{{"1", "dog"}})

	// Kill replication job.
	dbA.Exec(t, "CANCEL JOB $1", jobAID)
	jobutils.WaitForJobToCancel(t, dbA, jobAID)
	replicationtestutils.WaitForAllProducerJobsToFail(t, dbB)
}

func TestFailDestAfterSourceFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	args := testClusterBaseClusterArgs
	server, s, dbA, dbB := setupLogicalTestServer(t, ctx, args, 1)
	defer server.Stopper().Stop(ctx)

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, s, s, serverutils.DBName("a"))

	var jobBID jobspb.JobID
	dbB.QueryRow(t, "CREATE LOGICAL REPLICATION STREAM FROM TABLE tab ON $1 INTO TABLE tab", dbAURL.String()).Scan(&jobBID)

	WaitUntilReplicatedTime(t, s.Clock().Now(), dbB, jobBID)
	dbB.Exec(t, "PAUSE JOB $1", jobBID)

	producerJobID := replicationtestutils.GetProducerJobIDFromLDRJob(t, dbA, jobBID)
	dbA.Exec(t, "CANCEL JOB $1", producerJobID)
	jobutils.WaitForJobToCancel(t, dbA, producerJobID)

	dbB.Exec(t, "RESUME JOB $1", jobBID)
	jobutils.WaitForJobToFail(t, dbB, jobBID)
}
