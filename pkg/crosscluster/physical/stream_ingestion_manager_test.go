// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRevertTenantToTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, systemDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer srv.Stopper().Stop(ctx)
	systemSQL := sqlutils.MakeSQLRunner(systemDB)

	systemSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	systemSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval ='100ms'")
	systemSQL.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval ='100ms'")
	systemSQL.Exec(t, "CREATE VIRTUAL CLUSTER target")

	t.Run("errors if tenant doesn't exist", func(t *testing.T) {
		_, err := systemDB.Exec("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('doesnotexist', cluster_logical_timestamp())")
		require.ErrorContains(t, err, "does not exist")
		require.Error(t, err)
	})
	t.Run("errors if tenant is the system tenant", func(t *testing.T) {
		_, err := systemDB.Exec("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('system', cluster_logical_timestamp())")
		require.ErrorContains(t, err, "cannot revert the system tenant")
		require.Error(t, err)
	})
	t.Run("requires the MANAGEVIRTUALCLUSTER permission", func(t *testing.T) {
		systemSQL.Exec(t, "CREATE ROLE otheruser LOGIN")
		systemSQL.Exec(t, "SET role=otheruser")
		defer func() { systemSQL.Exec(t, "SET role=admin") }()
		_, err := systemDB.Exec("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('target', cluster_logical_timestamp())")
		require.ErrorContains(t, err, "does not have MANAGEVIRTUALCLUSTER system privilege")
	})
	t.Run("requires sql_safe_updates=false", func(t *testing.T) {
		systemSQL.Exec(t, "SET sql_safe_updates=true")
		defer func() { systemSQL.Exec(t, "SET sql_safe_updates=false") }()
		_, err := systemDB.Exec("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('target', cluster_logical_timestamp())")
		require.ErrorContains(t, err, "rejected (via sql_safe_updates)")
	})
	t.Run("requires that tenant has a non-none service mode", func(t *testing.T) {
		systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target START SERVICE shared")
		_, err := systemDB.Exec("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('target', cluster_logical_timestamp())")
		require.ErrorContains(t, err, "service mode must be none")
		systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target STOP SERVICE")
		systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target START SERVICE EXTERNAL")

		_, err = systemDB.Exec("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('target', cluster_logical_timestamp())")
		require.ErrorContains(t, err, "service mode must be none")

		systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target STOP SERVICE")
	})

	newConn := func(t *testing.T) *gosql.DB {
		var conn *gosql.DB
		testutils.SucceedsSoon(t, func() error {
			db, err := srv.SystemLayer().SQLConnE(serverutils.DBName("cluster:target"))
			if err != nil {
				return err
			}
			if err := db.Ping(); err != nil {
				return err
			}
			conn = db
			return nil
		})
		return conn
	}

	t.Run("reverts changes inside a tenant", func(t *testing.T) {
		systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target START SERVICE SHARED")
		tenantDB := newConn(t)
		defer tenantDB.Close()
		tenantSQL := sqlutils.MakeSQLRunner(tenantDB)
		tenantSQL.Exec(t, "CREATE DATABASE test")
		tenantSQL.Exec(t, "CREATE TABLE test.revert1 (k PRIMARY KEY) AS SELECT generate_series(1, 100)")
		var ts string
		tenantSQL.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&ts)
		tenantSQL.Exec(t, "UPDATE test.revert1 SET k = k+1000")
		tenantSQL.Exec(t, "ALTER TABLE test.revert1 SPLIT AT VALUES (1000), (1010), (1020)")
		tenantSQL.CheckQueryResults(t, "SELECT max(k) FROM test.revert1", [][]string{{"1100"}})

		systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target STOP SERVICE")
		waitUntilTenantServerStopped(t, srv.SystemLayer(), "target")

		systemSQL.Exec(t, fmt.Sprintf("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('target', %s)", ts))
		systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target START SERVICE SHARED")

		tenantDB2 := newConn(t)
		defer tenantDB2.Close()
		tenantSQL = sqlutils.MakeSQLRunner(tenantDB2)
		tenantSQL.CheckQueryResults(t, "SELECT max(k) FROM test.revert1", [][]string{{"100"}})
	})
}

func TestRevertTenantToTimestampPTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// We are going to force a GC between installing our PTS and
	// the processing the first RevertRange request to ensure that
	// the request still works.
	var trapRevertRange atomic.Bool
	waitForRevertRangeStart := make(chan struct{})
	allowRevertRange := make(chan struct{})
	testingRequestFilter := func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		for _, req := range ba.Requests {
			if expReq := req.GetRevertRange(); expReq != nil {
				if trapRevertRange.Load() {
					trapRevertRange.Store(false)
					close(waitForRevertRangeStart)
					<-allowRevertRange
				}
			}
		}
		return nil
	}

	srv, systemDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: testingRequestFilter,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	systemSQL := sqlutils.MakeSQLRunner(systemDB)

	systemSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	systemSQL.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval ='100ms'")
	systemSQL.Exec(t, "SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval ='100ms'")
	systemSQL.Exec(t, "CREATE VIRTUAL CLUSTER target")
	systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target START SERVICE SHARED")
	var tid uint64
	systemSQL.QueryRow(t, "SELECT id FROM system.tenants WHERE name = 'target'").Scan(&tid)
	codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(tid))

	getFirstStoreAndReplica := func(t *testing.T, key roachpb.Key) (*kvserver.Store, *kvserver.Replica) {
		storage := srv.StorageLayer()
		store, err := storage.GetStores().(*kvserver.Stores).GetStore(storage.GetFirstStoreID())
		require.NoError(t, err)
		var repl *kvserver.Replica
		testutils.SucceedsSoon(t, func() error {
			repl = store.LookupReplica(roachpb.RKey(key))
			if repl == nil {
				return errors.New("could not find replica")
			}
			return nil
		})
		return store, repl
	}

	gcTenantTableRange := func(t *testing.T, rangeID int64, tablePrefix roachpb.Key) {
		// Update the PTSReader cache and then send the range
		// through the mvccGC queue.
		store, repl := getFirstStoreAndReplica(t, tablePrefix)
		asOf := s.Clock().Now()
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader
		t.Logf("udating PTS reader cache to %s", asOf)
		require.NoError(
			t,
			spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, asOf),
		)
		require.NoError(t, repl.ReadProtectedTimestampsForTesting(ctx))

		t.Logf("enqueuing range %d for mvccGC", rangeID)
		systemSQL.Exec(t, `SELECT crdb_internal.kv_enqueue_replica($1, 'mvccGC', true)`, rangeID)
	}

	waitForGCTTL := func(t *testing.T, tablePrefix roachpb.Key) {
		testutils.SucceedsSoon(t, func() error {
			_, r := getFirstStoreAndReplica(t, tablePrefix)
			conf, err := r.LoadSpanConfig(ctx)
			if err == nil {
				return err
			}
			if conf.GCPolicy.TTLSeconds != 1 {
				return fmt.Errorf("expected %d, got %d", 1, conf.GCPolicy.TTLSeconds)
			}
			return nil
		})
	}
	waitForGCProtection := func(t *testing.T, tablePrefix roachpb.Key) {
		testutils.SucceedsSoon(t, func() error {
			_, r := getFirstStoreAndReplica(t, tablePrefix)
			conf, err := r.LoadSpanConfig(ctx)
			if err == nil {
				return err
			}
			if len(conf.GCPolicy.ProtectionPolicies) == 0 {
				return fmt.Errorf("expected policies, got %d", len(conf.GCPolicy.ProtectionPolicies))
			}
			return nil
		})
	}

	newConn := func(t *testing.T) *gosql.DB {
		var conn *gosql.DB
		testutils.SucceedsSoon(t, func() error {
			db, err := srv.SystemLayer().SQLConnE(serverutils.DBName("cluster:target"))
			if err != nil {
				return err
			}
			if err := db.Ping(); err != nil {
				return err
			}
			conn = db
			return nil
		})
		return conn
	}

	tenantDB := newConn(t)
	defer tenantDB.Close()
	tenantSQL := sqlutils.MakeSQLRunner(tenantDB)
	tenantSQL.Exec(t, "CREATE DATABASE test")
	tenantSQL.Exec(t, "CREATE TABLE test.revert (k PRIMARY KEY) AS SELECT generate_series(1, 100)")
	tenantSQL.Exec(t, "ALTER TABLE test.revert CONFIGURE ZONE USING gc.ttlseconds=1")
	var ts string
	tenantSQL.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&ts)
	tenantSQL.Exec(t, "UPDATE test.revert SET k = k+1000")
	tenantSQL.CheckQueryResults(t, "SELECT max(k) FROM test.revert", [][]string{{"1100"}})

	var rangeID int64
	tenantSQL.QueryRow(t, "SELECT range_id FROM [SHOW RANGES FROM TABLE test.revert]").Scan(&rangeID)
	var tableID int64
	tenantSQL.QueryRow(t, "select 'test.revert'::regclass::oid").Scan(&tableID)
	tableStartKey := codec.TablePrefix(uint32(tableID))

	waitForGCTTL(t, tableStartKey)

	systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target STOP SERVICE")

	trapRevertRange.Store(true)
	errCh := make(chan error)
	go func() {
		t.Logf("reverting to %s", ts)
		_, err := systemDB.Exec(fmt.Sprintf("SELECT crdb_internal.unsafe_revert_tenant_to_timestamp('target', %s)", ts))
		errCh <- err
	}()
	t.Logf("waiting for revert range to start")
	<-waitForRevertRangeStart
	waitForGCProtection(t, tableStartKey)
	// Wait antoher couple of seconds to make sure that the data
	// is actually eligible for garbage collection.
	time.Sleep(2 * time.Second)
	gcTenantTableRange(t, rangeID, tableStartKey)
	close(allowRevertRange)
	require.NoError(t, <-errCh)

	systemSQL.Exec(t, "ALTER VIRTUAL CLUSTER target START SERVICE SHARED")

	tenantDB2 := newConn(t)
	defer tenantDB2.Close()
	tenantSQL = sqlutils.MakeSQLRunner(tenantDB2)
	tenantSQL.CheckQueryResults(t, "SELECT max(k) FROM test.revert", [][]string{{"100"}})
}
