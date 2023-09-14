// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestChangefeedUpdateProtectedTimestamp tests that a running changefeed
// continuously advances the timestamp of its PTS record as its highwater
// advances.
func TestChangefeedUpdateProtectedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		ptsInterval := 50 * time.Millisecond
		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, ptsInterval)

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t, ""))
		sysDB.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'") // speeds up the test
		sysDB.Exec(t, "ALTER TENANT ALL SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '20ms'`)
		defer closeFeed(t, foo)

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			s.SystemServer.DB(), s.Codec, "d", "foo")

		ptp := s.Server.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
		store, err := s.SystemServer.GetStores().(*kvserver.Stores).GetStore(s.SystemServer.GetFirstStoreID())
		require.NoError(t, err)
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader

		// Wait and return the next resolved timestamp after the wait time
		waitAndDrainResolved := func(ts time.Duration) hlc.Timestamp {
			targetTs := timeutil.Now().Add(ts)
			for {
				resolvedTs, _ := expectResolvedTimestamp(t, foo)
				if resolvedTs.GoTime().UnixNano() > targetTs.UnixNano() {
					return resolvedTs
				}
			}
		}

		mkGetProtections := func(t *testing.T, ptp protectedts.Provider,
			srv serverutils.ApplicationLayerInterface, ptsReader spanconfig.ProtectedTSReader,
			span roachpb.Span) func() []hlc.Timestamp {
			return func() (r []hlc.Timestamp) {
				require.NoError(t,
					spanconfigptsreader.TestingRefreshPTSState(ctx, t, ptsReader, srv.Clock().Now()))
				protections, _, err := ptsReader.GetProtectionTimestamps(ctx, span)
				require.NoError(t, err)
				return protections
			}
		}

		mkWaitForProtectionCond := func(t *testing.T, getProtection func() []hlc.Timestamp,
			check func(protection []hlc.Timestamp) error) func() {
			return func() {
				t.Helper()
				testutils.SucceedsSoon(t, func() error { return check(getProtection()) })
			}
		}

		// Setup helpers on the system.descriptors table.
		descriptorTableKey := s.Codec.TablePrefix(keys.DescriptorTableID)
		descriptorTableSpan := roachpb.Span{
			Key: descriptorTableKey, EndKey: descriptorTableKey.PrefixEnd(),
		}
		getDescriptorTableProtection := mkGetProtections(t, ptp, s.Server, ptsReader,
			descriptorTableSpan)

		// Setup helpers on the user table.
		tableKey := s.Codec.TablePrefix(uint32(fooDesc.GetID()))
		tableSpan := roachpb.Span{
			Key: tableKey, EndKey: tableKey.PrefixEnd(),
		}
		getTableProtection := mkGetProtections(t, ptp, s.Server, ptsReader, tableSpan)
		waitForProtectionAdvanced := func(ts hlc.Timestamp, getProtection func() []hlc.Timestamp) {
			check := func(protections []hlc.Timestamp) error {
				if len(protections) == 0 {
					return errors.New("expected protection but found none")
				}
				for _, p := range protections {
					if p.LessEq(ts) {
						return errors.Errorf("expected protected timestamp to exceed %v, found %v", ts, p)
					}
				}
				return nil
			}

			mkWaitForProtectionCond(t, getProtection, check)()
		}

		// Observe the protected timestamp advancing along with resolved timestamps
		for i := 0; i < 5; i++ {
			// Progress the changefeed and allow time for a pts record to be laid down
			nextResolved := waitAndDrainResolved(100 * time.Millisecond)
			waitForProtectionAdvanced(nextResolved, getTableProtection)
			waitForProtectionAdvanced(nextResolved, getDescriptorTableProtection)
		}
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks)
}

// TestChangefeedProtectedTimestamps asserts the state of changefeed PTS records
// in various scenarios
//   - There is a protection during the initial scan which is advanced once it
//     completes
//   - There is a protection during a schema change backfill which is advanced
//     once it completes
//   - When a changefeed is cancelled the protection is removed.
func TestChangefeedProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		ctx      = context.Background()
		userSpan = roachpb.Span{
			Key:    bootstrap.TestingUserTableDataMin(keys.SystemSQLCodec),
			EndKey: keys.TableDataMax,
		}
		done               = make(chan struct{})
		blockRequestCh     = make(chan chan chan struct{}, 1)
		requestBlockedScan = func() (waitForBlockedScan func() (unblockScan func())) {
			blockRequest := make(chan chan struct{})
			blockRequestCh <- blockRequest // test sends to filter to request a block
			return func() (unblockScan func()) {
				toClose := <-blockRequest // filter sends back to test to report blocked
				return func() {
					close(toClose) // test closes to unblock filter
				}
			}
		}
		requestFilter = kvserverbase.ReplicaRequestFilter(func(
			ctx context.Context, ba *kvpb.BatchRequest,
		) *kvpb.Error {
			if ba.Txn == nil || ba.Txn.Name != "changefeed backfill" {
				return nil
			}
			scanReq, ok := ba.GetArg(kvpb.Scan)
			if !ok {
				return nil
			}
			if !userSpan.Contains(scanReq.Header().Span()) {
				return nil
			}
			select {
			case notifyCh := <-blockRequestCh:
				waitUntilClosed := make(chan struct{})
				notifyCh <- waitUntilClosed
				select {
				case <-waitUntilClosed:
				case <-done:
				case <-ctx.Done():
				}
			default:
			}
			return nil
		})
		mkGetProtections = func(t *testing.T, ptp protectedts.Provider,
			srv serverutils.ApplicationLayerInterface, ptsReader spanconfig.ProtectedTSReader,
			span roachpb.Span) func() []hlc.Timestamp {
			return func() (r []hlc.Timestamp) {
				require.NoError(t,
					spanconfigptsreader.TestingRefreshPTSState(ctx, t, ptsReader, srv.Clock().Now()))
				protections, _, err := ptsReader.GetProtectionTimestamps(ctx, span)
				require.NoError(t, err)
				return protections
			}
		}
		checkProtection = func(protections []hlc.Timestamp) error {
			if len(protections) == 0 {
				return errors.New("expected protected timestamp to exist")
			}
			return nil
		}
		checkNoProtection = func(protections []hlc.Timestamp) error {
			if len(protections) != 0 {
				return errors.Errorf("expected protected timestamp to not exist, found %v", protections)
			}
			return nil
		}
		mkWaitForProtectionCond = func(t *testing.T, getProtection func() []hlc.Timestamp,
			check func(protection []hlc.Timestamp) error) func() {
			return func() {
				t.Helper()
				testutils.SucceedsSoon(t, func() error { return check(getProtection()) })
			}
		}
	)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t, ""))
		sysDB.Exec(t, `SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'`)
		sysDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
		sysDB.Exec(t, `ALTER TENANT ALL SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
		sqlDB.Exec(t, `ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 100`)
		sqlDB.Exec(t, `ALTER RANGE system CONFIGURE ZONE USING gc.ttlseconds = 100`)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

		var tableID int
		sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables `+
			`WHERE name = 'foo' AND database_name = current_database()`).
			Scan(&tableID)

		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 100*time.Millisecond)

		ptp := s.Server.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
		store, err := s.SystemServer.GetStores().(*kvserver.Stores).GetStore(s.SystemServer.GetFirstStoreID())
		require.NoError(t, err)
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader

		// Setup helpers on the system.descriptors table.
		descriptorTableKey := s.Codec.TablePrefix(keys.DescriptorTableID)
		descriptorTableSpan := roachpb.Span{
			Key: descriptorTableKey, EndKey: descriptorTableKey.PrefixEnd(),
		}
		getDescriptorTableProtection := mkGetProtections(t, ptp, s.Server, ptsReader,
			descriptorTableSpan)
		waitForDescriptorTableProtection := mkWaitForProtectionCond(t, getDescriptorTableProtection,
			checkProtection)
		waitForNoDescriptorTableProtection := mkWaitForProtectionCond(t, getDescriptorTableProtection,
			checkNoProtection)

		// Setup helpers on the user table.
		tableKey := s.Codec.TablePrefix(uint32(tableID))
		tableSpan := roachpb.Span{
			Key: tableKey, EndKey: tableKey.PrefixEnd(),
		}
		getTableProtection := mkGetProtections(t, ptp, s.Server, ptsReader, tableSpan)
		waitForTableProtection := mkWaitForProtectionCond(t, getTableProtection, checkProtection)
		waitForNoTableProtection := mkWaitForProtectionCond(t, getTableProtection, checkNoProtection)
		waitForBlocked := requestBlockedScan()
		waitForProtectionAdvanced := func(ts hlc.Timestamp, getProtection func() []hlc.Timestamp) {
			check := func(protections []hlc.Timestamp) error {
				if len(protections) != 0 {
					for _, p := range protections {
						if p.LessEq(ts) {
							return errors.Errorf("expected protected timestamp to exceed %v, found %v", ts, p)
						}
					}
				}
				return nil
			}

			mkWaitForProtectionCond(t, getProtection, check)()
		}

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved`)
		defer closeFeed(t, foo)
		{
			// Ensure that there's a protected timestamp on startup that goes
			// away after the initial scan.
			unblock := waitForBlocked()
			waitForTableProtection()
			unblock()
			assertPayloads(t, foo, []string{
				`foo: [1]->{"after": {"a": 1, "b": "a"}}`,
				`foo: [2]->{"after": {"a": 2, "b": "b"}}`,
				`foo: [4]->{"after": {"a": 4, "b": "c"}}`,
				`foo: [7]->{"after": {"a": 7, "b": "d"}}`,
				`foo: [8]->{"after": {"a": 8, "b": "e"}}`,
			})
			resolved, _ := expectResolvedTimestamp(t, foo)
			waitForProtectionAdvanced(resolved, getTableProtection)
		}

		{
			// Ensure that a protected timestamp is created for a backfill due
			// to a schema change and removed after.
			waitForBlocked = requestBlockedScan()
			sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN c INT NOT NULL DEFAULT 1`)
			unblock := waitForBlocked()
			waitForTableProtection()
			waitForDescriptorTableProtection()
			unblock()
			assertPayloads(t, foo, []string{
				`foo: [1]->{"after": {"a": 1, "b": "a", "c": 1}}`,
				`foo: [2]->{"after": {"a": 2, "b": "b", "c": 1}}`,
				`foo: [4]->{"after": {"a": 4, "b": "c", "c": 1}}`,
				`foo: [7]->{"after": {"a": 7, "b": "d", "c": 1}}`,
				`foo: [8]->{"after": {"a": 8, "b": "e", "c": 1}}`,
			})
			resolved, _ := expectResolvedTimestamp(t, foo)
			waitForProtectionAdvanced(resolved, getTableProtection)
			waitForProtectionAdvanced(resolved, getDescriptorTableProtection)
		}

		{
			// Ensure that the protected timestamp is removed when the job is
			// canceled.
			waitForBlocked = requestBlockedScan()
			sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN d INT NOT NULL DEFAULT 2`)
			_ = waitForBlocked()
			waitForTableProtection()
			waitForDescriptorTableProtection()
			sqlDB.Exec(t, `CANCEL JOB $1`, foo.(cdctest.EnterpriseTestFeed).JobID())
			waitForNoTableProtection()
			waitForNoDescriptorTableProtection()
		}
	}

	cdcTestWithSystem(t, testFn, feedTestNoTenants, feedTestEnterpriseSinks, withArgsFn(func(args *base.TestServerArgs) {
		storeKnobs := &kvserver.StoreTestingKnobs{}
		storeKnobs.TestingRequestFilter = requestFilter
		args.Knobs.Store = storeKnobs
	}))
}

// TestChangefeedAlterPTS is a regression test for (#103855).
// It verifies that we do not lose track of existing PTS records nor create
// extraneous PTS records when altering a changefeed by adding a table.
func TestChangefeedAlterPTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE foo2 (a INT PRIMARY KEY, b STRING)`)
		f2 := feed(t, f, `CREATE CHANGEFEED FOR table foo with protect_data_from_gc_on_pause,
			resolved='1s', min_checkpoint_frequency='1s'`)
		defer closeFeed(t, f2)

		getNumPTSRecords := func() int {
			rows := sqlDB.Query(t, "SELECT * FROM system.protected_ts_records")
			r, err := sqlutils.RowsToStrMatrix(rows)
			if err != nil {
				t.Fatalf("%v", err)
			}
			return len(r)
		}

		jobFeed := f2.(cdctest.EnterpriseTestFeed)

		_, _ = expectResolvedTimestamp(t, f2)

		require.Equal(t, 1, getNumPTSRecords())

		require.NoError(t, jobFeed.Pause())
		sqlDB.Exec(t, fmt.Sprintf("ALTER CHANGEFEED %d ADD TABLE foo2 with initial_scan='yes'", jobFeed.JobID()))
		require.NoError(t, jobFeed.Resume())

		_, _ = expectResolvedTimestamp(t, f2)

		require.Equal(t, 1, getNumPTSRecords())
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestChangefeedCanceledWhenPTSIsOld is a test for the setting
// `kv.closed_timestamp.target_duration` which ensures that a paused changefeed
// job holding a PTS record gets canceled if paused for too long.
func TestChangefeedCanceledWhenPTSIsOld(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t, ""))
		sysDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
		sysDB.Exec(t, `ALTER TENANT ALL SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
		sqlDB.Exec(t, `SET CLUSTER SETTING jobs.metrics.interval.poll = '100ms'`) // speed up metrics poller
		// Create the data table; it will only contain a
		// single row with multiple versions.
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b INT)`)

		feed, err := f.Feed("CREATE CHANGEFEED FOR TABLE foo WITH protect_data_from_gc_on_pause, gc_protect_expires_after='24h'")
		require.NoError(t, err)
		defer func() {
			closeFeed(t, feed)
		}()

		jobFeed := feed.(cdctest.EnterpriseTestFeed)
		require.NoError(t, jobFeed.Pause())

		// While the job is paused, take opportunity to test that alter changefeed
		// works when setting gc_protect_expires_after option.

		// Verify we can set it to 0 -- i.e. disable.
		sqlDB.Exec(t, fmt.Sprintf("ALTER CHANGEFEED %d SET gc_protect_expires_after = '0s'", jobFeed.JobID()))
		// Now, set it to something very small.
		sqlDB.Exec(t, fmt.Sprintf("ALTER CHANGEFEED %d SET gc_protect_expires_after = '250ms'", jobFeed.JobID()))

		// Stale PTS record should trigger job cancellation.
		require.NoError(t, jobFeed.WaitForStatus(func(s jobs.Status) bool {
			return s == jobs.StatusCanceled
		}))
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks)
}

// TestPTSRecordProtectsTargetsAndDescriptorTable tests that descriptors are not
// GC'd when they are protected by a PTS record.
func TestPTSRecordProtectsTargetsAndDescriptorTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, stopServer := startTestFullServer(t, feedTestOptions{})
	defer stopServer()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, "CREATE TABLE foo (a INT, b STRING)")
	ts := s.Clock().Now()
	ctx := context.Background()

	fooDescr := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "d", "foo")
	var targets changefeedbase.Targets
	targets.Add(changefeedbase.Target{
		TableID: fooDescr.GetID(),
	})

	// Lay protected timestamp record.
	ptr := createProtectedTimestampRecord(ctx, s.Codec(), 42, targets, ts)
	require.NoError(t, execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return execCfg.ProtectedTimestampProvider.WithTxn(txn).Protect(ctx, ptr)
	}))

	// Alter foo few times, then force GC at ts-1.
	sqlDB.Exec(t, "ALTER TABLE foo ADD COLUMN c STRING")
	sqlDB.Exec(t, "ALTER TABLE foo ADD COLUMN d STRING")
	require.NoError(t, s.ForceTableGC(ctx, "system", "descriptor", ts.Add(-1, 0)))

	// We can still fetch table descriptors because of protected timestamp record.
	asOf := ts
	_, err := fetchTableDescriptors(ctx, &execCfg, targets, asOf)
	require.NoError(t, err)
}
