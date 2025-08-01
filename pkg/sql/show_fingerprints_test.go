// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed/schematestutils"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// NB: Most of the SHOW EXPERIMENTAL_FINGERPRINTS tests are in the
// show_fingerprints logic test. This is just to test the AS OF SYSTEM TIME
// functionality.
func TestShowFingerprintsAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (a INT PRIMARY KEY, b INT, INDEX b_idx (b))`)
	sqlDB.Exec(t, `INSERT INTO d.t VALUES (1, 2)`)

	const fprintQuery = `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE d.t`
	fprint1 := sqlDB.QueryStr(t, fprintQuery)

	var ts string
	sqlDB.QueryRow(t, `SELECT now()`).Scan(&ts)

	sqlDB.Exec(t, `INSERT INTO d.t VALUES (3, 4)`)
	sqlDB.Exec(t, `DROP INDEX d.t@b_idx`)

	fprint2 := sqlDB.QueryStr(t, fprintQuery)
	if reflect.DeepEqual(fprint1, fprint2) {
		t.Errorf("expected different fingerprints: %v vs %v", fprint1, fprint2)
	}

	fprint3Query := fmt.Sprintf(`SELECT * FROM [%s] AS OF SYSTEM TIME '%s'`, fprintQuery, ts)
	sqlDB.CheckQueryResults(t, fprint3Query, fprint1)
}

func TestShowFingerprintsColumnNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t (
		lowercase INT PRIMARY KEY,
		"cApiTaLInT" INT,
		"cApiTaLByTEs" BYTES,
		INDEX capital_int_idx ("cApiTaLInT"),
		INDEX capital_bytes_idx ("cApiTaLByTEs")
	)`)

	sqlDB.Exec(t, `INSERT INTO d.t VALUES (1, 2, 'a')`)
	fprint1 := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE d.t`)

	sqlDB.Exec(t, `TRUNCATE TABLE d.t`)
	sqlDB.Exec(t, `INSERT INTO d.t VALUES (3, 4, 'b')`)
	fprint2 := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE d.t`)

	if reflect.DeepEqual(fprint1, fprint2) {
		t.Errorf("expected different fingerprints: %v vs %v", fprint1, fprint2)
	}
}

// TestShowFingerprintsDuringSchemaChange is a regression test that asserts that
// fingerprinting does not fail when done in the middle of a schema change using
// an AOST query. In the middle of a schema change such as `ADD COLUMN ...
// DEFAULT`, there may be non-public indexes in the descriptor. Prior to the
// change which introduced this test, fingerprinting would attempt to read these
// non-public indexes and fail.
func TestShowFingerprintsDuringSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `USE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (
		a INT PRIMARY KEY,
		b INT
	)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 0)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 1)`)

	// At version 5, there are non-public indexes.
	sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN c INT DEFAULT -1`)
	ts := schematestutils.FetchDescVersionModificationTime(
		t, s, "d", "public", "foo", 5)
	sqlDB.Exec(t, fmt.Sprintf(
		`SELECT * FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE foo] AS OF SYSTEM TIME %s`,
		ts.AsOfSystemTime()))
}

// TestShowTenantFingerprintsProtectsTimestamp tests that we actually
// protect the relevant keyspan by intercepting the relevant
// ExportRequests and enqueing ranges for GC right before evaluating
// the request.
func TestShowTenantFingerprintsProtectsTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Under deadlock there isn't enough time for the CREATE TENANT txn to commit
	// due to the intervals we lower to speed up this test. There is no difference
	// otherwise when running this test under deadlock
	skip.UnderDeadlock(t, 121445, "Deadlock makes txns take too long to commit")

	ctx := context.Background()

	var exportStartedClosed atomic.Bool
	exportsStarted := make(chan struct{})
	exportsResume := make(chan struct{})
	testingRequestFilter := func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		for _, req := range ba.Requests {
			if expReq := req.GetExport(); expReq != nil {
				if expReq.ExportFingerprint && exportStartedClosed.CompareAndSwap(false, true) {
					close(exportsStarted)
					<-exportsResume
				}
			}
		}
		return nil
	}

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: testingRequestFilter,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	systemSQL := sqlutils.MakeSQLRunner(db)
	systemSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	systemSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval ='100ms'")
	systemSQL.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval ='100ms'")

	tenantApp, tenantDB, err := s.TenantController().StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{
		TenantName: "testtenant",
	})
	require.NoError(t, err)

	tenantSQL := sqlutils.MakeSQLRunner(tenantDB)
	tenantSQL.Exec(t, "CREATE DATABASE test")
	tenantSQL.Exec(t, "CREATE TABLE test.foo (k PRIMARY KEY) AS SELECT generate_series(1, 1000)")
	tenantSQL.Exec(t, "ALTER TABLE test.foo CONFIGURE ZONE USING gc.ttlseconds=1")
	tenantSQL.Exec(t, "UPDATE test.foo SET k=k+2000")

	refreshPTSReaderCache := func(asOf hlc.Timestamp) {
		tableID, err := tenantApp.QueryTableID(ctx, username.RootUserName(), "test", "foo")
		require.NoError(t, err)
		tableKey := tenantApp.Codec().TablePrefix(uint32(tableID))
		store, err := s.StorageLayer().GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		require.NoError(t, err)
		var repl *kvserver.Replica
		testutils.SucceedsSoon(t, func() error {
			repl = store.LookupReplica(roachpb.RKey(tableKey))
			if repl == nil {
				return errors.New("could not find replica")
			}
			return nil
		})
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader
		t.Logf("udating PTS reader cache to %s", asOf)
		require.NoError(
			t,
			spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, asOf),
		)
		require.NoError(t, repl.ReadProtectedTimestampsForTesting(ctx))
	}
	gcTestTableRange := func() {
		row := tenantSQL.QueryRow(t, "SELECT range_id FROM [SHOW RANGES FROM TABLE test.foo]")
		var rangeID int64
		row.Scan(&rangeID)
		refreshPTSReaderCache(tenantApp.Clock().Now())
		t.Logf("enqueuing range %d for mvccGC", rangeID)
		systemSQL.Exec(t, `SELECT crdb_internal.kv_enqueue_replica($1, 'mvccGC', true)`, rangeID)
	}

	errCh := make(chan error)
	go func() {
		_, err := db.Exec("SHOW EXPERIMENTAL_FINGERPRINTS FROM VIRTUAL CLUSTER testtenant")
		errCh <- err
	}()
	<-exportsStarted
	// Unsure a better way to syncronize this. The gc.ttlseconds
	// zone configuration is 1 second, so to ensure that our GC
	// threshold moves beyond the transaction read timestamp we
	// need to wait.
	time.Sleep(2 * time.Second)
	gcTestTableRange()
	close(exportsResume)
	require.NoError(t, <-errCh)
}
