// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReplicationManagerRequiresReplicationPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tDB := sqlutils.MakeSQLRunner(sqlDB)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	var m sessiondatapb.MigratableSession
	var sessionSerialized []byte
	tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
	require.NoError(t, protoutil.Unmarshal(sessionSerialized, &m))
	sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
	require.NoError(t, err)
	sd.SessionData = m.SessionData
	sd.LocalOnlySessionData = m.LocalOnlySessionData

	getManagerForUser := func(u string) (eval.ReplicationStreamManager, error) {
		sqlUser, err := username.MakeSQLUsernameFromUserInput(u, username.PurposeValidation)
		require.NoError(t, err)
		txn := kvDB.NewTxn(ctx, "test")
		p, cleanup := sql.NewInternalPlanner("test", txn, sqlUser, &sql.MemoryMetrics{}, &execCfg, sd)

		// Extract
		pi := p.(interface {
			EvalContext() *eval.Context
			InternalSQLTxn() descs.Txn
		})
		defer cleanup()
		ec := pi.EvalContext()
		mgr, err := newReplicationStreamManager(ctx, ec, p.(resolver.SchemaResolver), pi.InternalSQLTxn(), clusterunique.ID{})
		require.NoError(t, err)
		if err := mgr.AuthorizeViaReplicationPriv(context.Background()); err != nil {
			return nil, err
		}
		return mgr, nil
	}

	tDB.Exec(t, "CREATE ROLE somebody")
	tDB.Exec(t, "GRANT SYSTEM REPLICATIONSOURCE TO somebody")
	tDB.Exec(t, "CREATE ROLE anybody")

	for _, tc := range []struct {
		user   string
		expErr string
	}{
		{user: "admin", expErr: ""},
		{user: "root", expErr: ""},
		{user: "somebody", expErr: ""},
		{user: "anybody", expErr: "user anybody does not have REPLICATIONSOURCE system privilege"},
		{user: "nobody", expErr: `role/user "nobody" does not exist`},
	} {
		t.Run(tc.user, func(t *testing.T) {
			m, err := getManagerForUser(tc.user)
			if tc.expErr == "" {
				require.NoError(t, err)
				require.NotNil(t, m)
			} else {
				require.Regexp(t, tc.expErr, err)
				require.Nil(t, m)
			}
		})
	}

}

func TestPlanLogicalReplicationScopesToJobTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tDB := sqlutils.MakeSQLRunner(sqlDB)
	tDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
	tDB.Exec(t, "CREATE TABLE tab_a (pk INT PRIMARY KEY)")
	tDB.Exec(t, "CREATE TABLE tab_b (pk INT PRIMARY KEY)")

	var tabAID, tabBID int32
	tDB.QueryRow(t, "SELECT 'tab_a'::regclass::oid::int").Scan(&tabAID)
	tDB.QueryRow(t, "SELECT 'tab_b'::regclass::oid::int").Scan(&tabBID)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	var m sessiondatapb.MigratableSession
	var sessionSerialized []byte
	tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
	require.NoError(t, protoutil.Unmarshal(sessionSerialized, &m))
	sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
	require.NoError(t, err)
	sd.SessionData = m.SessionData
	sd.LocalOnlySessionData = m.LocalOnlySessionData

	startStream := func() streampb.StreamID {
		txn := kvDB.NewTxn(ctx, "start-stream")
		p, cleanup := sql.NewInternalPlanner(
			"start-stream", txn, username.RootUserName(), &sql.MemoryMetrics{}, &execCfg, sd)
		defer cleanup()
		pi := p.(interface {
			EvalContext() *eval.Context
			InternalSQLTxn() descs.Txn
		})
		mgr, err := newReplicationStreamManager(
			ctx, pi.EvalContext(), p.(resolver.SchemaResolver), pi.InternalSQLTxn(), clusterunique.ID{})
		require.NoError(t, err)
		require.NoError(t, mgr.AuthorizeViaReplicationPriv(ctx))
		spec, err := mgr.StartReplicationStreamForTables(
			ctx, streampb.ReplicationProducerRequest{TableNames: []string{"tab_a"}})
		require.NoError(t, err)
		require.NoError(t, txn.Commit(ctx))
		return spec.StreamID
	}
	streamID := startStream()

	plan := func(t *testing.T, tableIDs []int32) error {
		reqBytes, err := protoutil.Marshal(&streampb.LogicalReplicationPlanRequest{
			StreamID: streamID,
			TableIDs: tableIDs,
		})
		require.NoError(t, err)
		var respBytes []byte
		row := sqlDB.QueryRow("SELECT crdb_internal.plan_logical_replication($1)", reqBytes)
		return row.Scan(&respBytes)
	}

	tests := []struct {
		name        string
		tableIDs    []int32
		expectedErr string
	}{
		{
			name:     "in-scope table",
			tableIDs: []int32{tabAID},
		},
		{
			name:        "out-of-scope table",
			tableIDs:    []int32{tabBID},
			expectedErr: "not contained within the keyspace authorized",
		},
		{
			name:        "mixed in-scope and out-of-scope",
			tableIDs:    []int32{tabAID, tabBID},
			expectedErr: "not contained within the keyspace authorized",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := plan(t, tc.tableIDs)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAuthorizeViaJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	tDB.Exec(t, "CREATE USER alice")
	tDB.Exec(t, "CREATE USER bob")
	tDB.Exec(t, "CREATE USER bob_source")
	tDB.Exec(t, "GRANT SYSTEM REPLICATIONSOURCE TO bob_source")
	tDB.Exec(t, "CREATE USER bob_controljob")
	tDB.Exec(t, "GRANT SYSTEM CONTROLJOB TO bob_controljob")

	aliceUsername := username.MakeSQLUsernameFromPreNormalizedString("alice")
	registry := s.JobRegistry().(*jobs.Registry)
	ptp := s.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider

	createProducerJob := func(desc string, tableIDs []uint32) streampb.StreamID {
		t.Helper()
		ptsID := uuid.MakeV4()
		jr := makeProducerJobRecordForLogicalReplication(
			registry, time.Hour, aliceUsername, ptsID,
			nil /* spans */, tableIDs, desc)
		require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			record := jobsprotectedts.MakeRecord(
				ptsID, int64(jr.JobID),
				hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
				jobsprotectedts.Jobs, ptpb.MakeClusterTarget())
			if err := ptp.WithTxn(txn).Protect(ctx, record); err != nil {
				return err
			}
			_, err := registry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, txn)
			return err
		}))
		return streampb.StreamID(jr.JobID)
	}

	aliceStreamID := createProducerJob("test", []uint32{100})

	canceledStreamID := createProducerJob("canceled", nil /* tableIDs */)
	tDB.Exec(t, fmt.Sprintf("CANCEL JOB %d", canceledStreamID))
	jobutils.WaitForJobToCancel(t, tDB, jobspb.JobID(canceledStreamID))

	// Force jobs into StateFailed and StateSucceeded via the registry's
	// internal helpers so we don't have to wait for real resumers to run.
	failedStreamID := createProducerJob("failed", nil /* tableIDs */)
	require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return registry.UnsafeFailed(ctx, txn, jobspb.JobID(failedStreamID), errors.New("forced for test"))
	}))
	succeededStreamID := createProducerJob("succeeded", nil /* tableIDs */)
	require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return registry.Succeeded(ctx, txn, jobspb.JobID(succeededStreamID))
	}))

	const nonexistentStreamID streampb.StreamID = 99999

	var m sessiondatapb.MigratableSession
	var sessionSerialized []byte
	tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
	require.NoError(t, protoutil.Unmarshal(sessionSerialized, &m))
	sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
	require.NoError(t, err)
	sd.SessionData = m.SessionData
	sd.LocalOnlySessionData = m.LocalOnlySessionData

	getManagerWithTxn := func(
		t *testing.T, u string,
	) (eval.ReplicationStreamManager, descs.Txn) {
		sqlUser, err := username.MakeSQLUsernameFromUserInput(u, username.PurposeValidation)
		require.NoError(t, err)
		txn := kvDB.NewTxn(ctx, "test")
		p, cleanup := sql.NewInternalPlanner("test", txn, sqlUser, &sql.MemoryMetrics{}, &execCfg, sd)
		t.Cleanup(cleanup)
		pi := p.(interface {
			EvalContext() *eval.Context
			InternalSQLTxn() descs.Txn
		})
		mgr, err := newReplicationStreamManager(ctx, pi.EvalContext(),
			p.(resolver.SchemaResolver), pi.InternalSQLTxn(), clusterunique.ID{})
		require.NoError(t, err)
		return mgr, pi.InternalSQLTxn()
	}
	getManager := func(t *testing.T, u string) eval.ReplicationStreamManager {
		mgr, _ := getManagerWithTxn(t, u)
		return mgr
	}

	t.Run("owner authorizes stream created in caller transaction", func(t *testing.T) {
		mgr, txn := getManagerWithTxn(t, "alice")
		jr := makeProducerJobRecordForLogicalReplication(
			registry, time.Hour, aliceUsername, uuid.MakeV4(),
			nil /* spans */, []uint32{100}, "uncommitted")
		_, err := registry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, txn)
		require.NoError(t, err)

		authCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, mgr.AuthorizeViaJob(authCtx, streampb.StreamID(jr.JobID)))
	})

	viaJobTests := []struct {
		name        string
		user        string
		streamID    streampb.StreamID
		expectedErr string
	}{
		{
			name:     "owner accesses own stream",
			user:     "alice",
			streamID: aliceStreamID,
		},
		{
			name:        "non-owner without privileges rejected",
			user:        "bob",
			streamID:    aliceStreamID,
			expectedErr: "does not own stream",
		},
		{
			name:        "REPLICATIONSOURCE does not grant control over another user's stream",
			user:        "bob_source",
			streamID:    aliceStreamID,
			expectedErr: "does not own stream",
		},
		{
			name:        "CONTROLJOB does not grant control over another user's stream",
			user:        "bob_controljob",
			streamID:    aliceStreamID,
			expectedErr: "does not own stream",
		},
		{
			name:        "admin without ownership rejected",
			user:        "root",
			streamID:    aliceStreamID,
			expectedErr: "does not own stream",
		},
		{
			name:        "nonexistent stream errors for anyone",
			user:        "alice",
			streamID:    nonexistentStreamID,
			expectedErr: "does not exist",
		},
		{
			name:        "owner rejected on canceled stream",
			user:        "alice",
			streamID:    canceledStreamID,
			expectedErr: "is not running",
		},
		{
			name:        "owner rejected on failed stream",
			user:        "alice",
			streamID:    failedStreamID,
			expectedErr: "is not running",
		},
		{
			name:        "owner rejected on succeeded stream",
			user:        "alice",
			streamID:    succeededStreamID,
			expectedErr: "is not running",
		},
	}
	for _, tc := range viaJobTests {
		t.Run(tc.name, func(t *testing.T) {
			mgr := getManager(t, tc.user)
			err := mgr.AuthorizeViaJob(ctx, tc.streamID)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}

	forStatusTests := []struct {
		name             string
		user             string
		streamID         streampb.StreamID
		expectedNotFound bool
		expectedErr      string
	}{
		{
			name:     "status: owner accesses own stream",
			user:     "alice",
			streamID: aliceStreamID,
		},
		{
			name:        "status: non-owner rejected",
			user:        "bob",
			streamID:    aliceStreamID,
			expectedErr: "does not own stream",
		},
		{
			name:     "status: owner tolerated on canceled stream",
			user:     "alice",
			streamID: canceledStreamID,
		},
		{
			name:             "status: missing stream reported as not found",
			user:             "root",
			streamID:         nonexistentStreamID,
			expectedNotFound: true,
		},
	}
	for _, tc := range forStatusTests {
		t.Run(tc.name, func(t *testing.T) {
			mgr := getManager(t, tc.user)
			notFound, err := mgr.AuthorizeViaJobAllowTerminal(ctx, tc.streamID)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedNotFound, notFound)
		})
	}

}

func TestReplicationBuiltinsRejectAOST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	tests := []struct {
		stmt           string
		expectRejected bool
	}{
		{stmt: "SELECT * FROM crdb_internal.start_replication_stream('t') AS OF SYSTEM TIME '-1us'", expectRejected: true},
		{stmt: "SELECT * FROM crdb_internal.start_replication_stream('t', ''::BYTES) AS OF SYSTEM TIME '-1us'", expectRejected: true},
		{stmt: "SELECT * FROM crdb_internal.replication_stream_progress(1, '0.0') AS OF SYSTEM TIME '-1us'", expectRejected: true},
		{stmt: "SELECT * FROM crdb_internal.stream_partition(1, ''::BYTES) AS OF SYSTEM TIME '-1us'", expectRejected: true},
		{stmt: "SELECT * FROM crdb_internal.replication_stream_spec(1) AS OF SYSTEM TIME '-1us'", expectRejected: true},
		{stmt: "SELECT * FROM crdb_internal.complete_replication_stream(1, true) AS OF SYSTEM TIME '-1us'", expectRejected: true},
		{stmt: "SELECT * FROM crdb_internal.setup_span_configs_stream('t') AS OF SYSTEM TIME '-1us'", expectRejected: true},
		{stmt: "SELECT * FROM crdb_internal.start_replication_stream_for_tables(''::BYTES) AS OF SYSTEM TIME '-1us'", expectRejected: true},
		{stmt: "SELECT * FROM crdb_internal.logical_replication_inject_failures(1, 1, 0) AS OF SYSTEM TIME '-1us'", expectRejected: true},
		{stmt: "SELECT * FROM crdb_internal.plan_logical_replication(''::BYTES) AS OF SYSTEM TIME '-1us'", expectRejected: false},
	}
	for _, tc := range tests {
		t.Run(tc.stmt, func(t *testing.T) {
			_, err := sqlDB.ExecContext(ctx, tc.stmt)
			if tc.expectRejected {
				require.ErrorContains(t, err, "cannot be used in an AS OF SYSTEM TIME query")
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "AS OF SYSTEM TIME")
			}
		})
	}
}
