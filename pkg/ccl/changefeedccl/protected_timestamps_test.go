// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigjob"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, ptsInterval)

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))
		sysDB.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'") // speeds up the test
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
					spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, srv.Clock().Now()))
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
					spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, srv.Clock().Now()))
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
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))
		sysDB.Exec(t, `SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'`)
		sysDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
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
		changefeedbase.ProtectTimestampLag.Override(
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
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))
		sysDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
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
		require.NoError(t, jobFeed.WaitForState(func(s jobs.State) bool {
			return s == jobs.StateCanceled
		}))
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks)
}

// TestPTSRecordProtectsTargetsAndSystemTables tests that descriptors and other
// required tables are not GC'd when they are protected by a PTS record.
func TestPTSRecordProtectsTargetsAndSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Useful for debugging.
	require.NoError(t, log.SetVModule("spanconfigstore=2,store=2,reconciler=3,mvcc_gc_queue=2,kvaccessor=2"))

	settings := cluster.MakeTestingClusterSettings()
	spanconfigjob.ReconciliationJobCheckpointInterval.Override(ctx, &settings.SV, 1*time.Second)

	// Keep track of where the spanconfig reconciler is up to.
	lastReconcilerCheckpoint := atomic.Value{}
	lastReconcilerCheckpoint.Store(hlc.Timestamp{})
	s, db, stopServer := startTestFullServer(t, feedTestOptions{
		knobsFn: func(knobs *base.TestingKnobs) {
			if knobs.SpanConfig == nil {
				knobs.SpanConfig = &spanconfig.TestingKnobs{}
			}
			scKnobs := knobs.SpanConfig.(*spanconfig.TestingKnobs)
			scKnobs.JobOnCheckpointInterceptor = func(lastCheckpoint hlc.Timestamp) error {
				now := hlc.Timestamp{WallTime: time.Now().UnixNano()}
				t.Logf("reconciler checkpoint %s (%s)", lastCheckpoint, now.GoTime().Sub(lastCheckpoint.GoTime()))
				lastReconcilerCheckpoint.Store(lastCheckpoint)
				return nil
			}
			scKnobs.SQLWatcherCheckpointNoopsEveryDurationOverride = 1 * time.Second
		},
		settings: settings,
	})

	defer stopServer()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE TABLE foo (a INT, b STRING)")
	sqlDB.Exec(t, `CREATE USER test`)
	sqlDB.Exec(t, `GRANT admin TO test`)
	ts := s.Clock().Now()

	fooDescr := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "d", "foo")
	var targets changefeedbase.Targets
	targets.Add(changefeedbase.Target{
		TableID: fooDescr.GetID(),
	})

	// We need to give our PTS record a legit job ID so the protected ts
	// reconciler doesn't delete it, so start up a dummy changefeed job and use its id.
	registry := s.JobRegistry().(*jobs.Registry)
	dummyJobDone := make(chan struct{})
	defer close(dummyJobDone)
	registry.TestingWrapResumerConstructor(jobspb.TypeChangefeed,
		func(raw jobs.Resumer) jobs.Resumer {
			return &fakeResumer{done: dummyJobDone}
		})
	var jobID jobspb.JobID
	sqlDB.QueryRow(t, `CREATE CHANGEFEED FOR TABLE foo INTO 'null://'`).Scan(&jobID)
	waitForJobState(sqlDB, t, jobID, `running`)

	// Lay protected timestamp record.
	ptr := createProtectedTimestampRecord(ctx, s.Codec(), jobID, targets, ts)
	require.NoError(t, execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return execCfg.ProtectedTimestampProvider.WithTxn(txn).Protect(ctx, ptr)
	}))

	// Set GC TTL to a small value to make the tables GC'd. We need to set this
	// *after* we set the PTS record so that we dont GC the tables before
	// the PTS is applied/picked up.
	sqlDB.Exec(t, `ALTER DATABASE system CONFIGURE ZONE USING gc.ttlseconds = 1`)

	// The following code was shameless stolen from
	// TestShowTenantFingerprintsProtectsTimestamp which almost
	// surely copied it from the 2-3 other tests that have
	// something similar.  We should put this in a helper. We have
	// ForceTableGC, but in ad-hoc testing that appeared to bypass
	// the PTS record making it useless for this test.
	//
	// TODO(ssd): Make a helper that does this.
	refreshPTSReaderCache := func(asOf hlc.Timestamp, tableName, databaseName string) {
		tableID, err := s.QueryTableID(ctx, username.RootUserName(), tableName, databaseName)
		require.NoError(t, err)
		tableKey := s.Codec().TablePrefix(uint32(tableID))
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
		t.Logf("updating PTS reader cache to %s", asOf)
		require.NoError(
			t,
			spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, asOf),
		)
		require.NoError(t, repl.ReadProtectedTimestampsForTesting(ctx))
	}
	gcTestTableRange := func(tableName, databaseName string) {
		row := sqlDB.QueryRow(t, fmt.Sprintf("SELECT range_id FROM [SHOW RANGES FROM TABLE %s.%s]", tableName, databaseName))
		var rangeID int64
		row.Scan(&rangeID)
		refreshPTSReaderCache(s.Clock().Now(), tableName, databaseName)
		t.Logf("enqueuing range %d (table %s.%s) for mvccGC", rangeID, tableName, databaseName)
		sqlDB.Exec(t, `SELECT crdb_internal.kv_enqueue_replica($1, 'mvccGC', true)`, rangeID)
	}

	// Alter foo few times, then force GC at ts-1.
	sqlDB.Exec(t, "ALTER TABLE foo ADD COLUMN c STRING")
	sqlDB.Exec(t, "ALTER TABLE foo ADD COLUMN d STRING")

	// Remove this entry from role_members.
	sqlDB.Exec(t, "REVOKE admin FROM test")

	// Change the user's password to update the users table.
	sqlDB.Exec(t, `ALTER USER test WITH PASSWORD 'testpass'`)

	// Sleep for enough time to pass the configured GC threshold (1 second).
	time.Sleep(2 * time.Second)

	// Wait for the spanconfigs to be reconciled.
	now := hlc.Timestamp{WallTime: time.Now().UnixNano()}
	t.Logf("waiting for spanconfigs to be reconciled")
	testutils.SucceedsWithin(t, func() error {
		lastCheckpoint := lastReconcilerCheckpoint.Load().(hlc.Timestamp)
		if lastCheckpoint.Less(now) {
			return errors.Errorf("last checkpoint %s is not less than now %s", lastCheckpoint, now)
		}
		t.Logf("last reconciler checkpoint ok at %s", lastCheckpoint)
		return nil
	}, 1*time.Minute)

	// If you want to GC all system tables:
	//
	// tabs := systemschema.MakeSystemTables()
	// for _, t := range tabs {
	// 	if t.IsPhysicalTable() && !t.IsSequence() {
	// 		gcTestTableRange("system", t.GetName())
	// 	}
	// }
	t.Logf("GC'ing system tables")
	gcTestTableRange("system", "descriptor")
	gcTestTableRange("system", "zones")
	gcTestTableRange("system", "comments")
	gcTestTableRange("system", "role_members")
	gcTestTableRange("system", "users")

	// We can still fetch table descriptors and role members because of protected timestamp record.
	asOf := ts
	_, err := fetchTableDescriptors(ctx, &execCfg, targets, asOf)
	require.NoError(t, err)
	// The role_members entry we removed is still visible at the asOf time because of the PTS record.
	rms, err := fetchRoleMembers(ctx, &execCfg, asOf)
	require.NoError(t, err)
	require.Contains(t, rms, []string{"admin", "test"})

	// The user password is still null.
	ups, err := fetchUsersAndPasswords(ctx, &execCfg, asOf)
	require.NoError(t, err)
	found := false
	for _, up := range ups {
		if up.username == "test" {
			require.Equal(t, tree.DNull, up.password)
			found = true
			break
		}
	}
	require.True(t, found)

}

// TestChangefeedUpdateProtectedTimestampTargets tests that changefeeds will
// remake their PTS records if they detect that they lack required targets.
func TestChangefeedMigratesProtectedTimestampTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		ctx := context.Background()

		dontMigrate := atomic.Bool{}
		dontMigrate.Store(true)
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.PreservePTSTargets = func() bool {
			return dontMigrate.Load()
		}

		ptsInterval := 50 * time.Millisecond
		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, ptsInterval)
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, ptsInterval)

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))

		sysDB.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'") // speeds up the test
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '20ms'`)
		defer closeFeed(t, foo)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		execCfg := s.Server.ExecutorConfig().(sql.ExecutorConfig)
		ptp := s.Server.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
		fooDesc := desctestutils.TestingGetPublicTableDescriptor(s.SystemServer.DB(), s.Codec, "d", "foo")
		fooID := fooDesc.GetID()

		jobFeed := foo.(cdctest.EnterpriseTestFeed)

		// removes table 3 from the target of the PTS record.
		removeOnePTSTarget := func(recordID uuid.UUID) error {
			return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				s := `select target from system.protected_ts_records where id = $1`
				datums, err := txn.QueryRowEx(ctx, "pts-test", txn.KV(), sessiondata.NodeUserSessionDataOverride, s, recordID)
				require.NoError(t, err)
				j := tree.MustBeDBytes(datums[0])

				target := &ptpb.Target{}
				require.NoError(t, protoutil.Unmarshal([]byte(j), target))

				// remove '3' (system.descriptor) to simulate a missing system table
				ids := target.GetSchemaObjects().IDs
				idx := slices.Index(ids, catid.DescID(3))
				target.GetSchemaObjects().IDs = slices.Delete(ids, idx, idx+1)

				bs, err := protoutil.Marshal(target)
				require.NoError(t, err)

				_, err = txn.ExecEx(ctx, "pts-test", txn.KV(), sessiondata.NodeUserSessionDataOverride,
					"UPDATE system.protected_ts_records SET target = $1 WHERE id = $2", bs, recordID,
				)
				require.NoError(t, err)
				return nil
			})
		}

		// Wipe out the targets from the changefeed PTS record, simulating an old-style PTS record.
		oldRecordID := getPTSRecordID(ctx, t, registry, jobFeed)
		require.NoError(t, removeOnePTSTarget(oldRecordID))

		// Sanity check: make sure that it worked
		oldRecord, err := readPTSRecord(ctx, t, execCfg, ptp, oldRecordID)
		require.NoError(t, err)
		targetIDs := oldRecord.Target.GetSchemaObjects().IDs
		require.Contains(t, targetIDs, fooID)
		require.NotSubset(t, targetIDs, systemTablesToProtect)

		// Flip the knob so the changefeed migrates the record
		dontMigrate.Store(false)

		getNewPTSRecord := func() *ptpb.Record {
			var recID uuid.UUID
			var record *ptpb.Record
			testutils.SucceedsSoon(t, func() error {
				recID = getPTSRecordID(ctx, t, registry, jobFeed)
				if recID.Equal(oldRecordID) {
					return errors.New("waiting for new PTS record")
				}
				return nil
			})
			record, err := readPTSRecord(ctx, t, execCfg, ptp, recID)
			require.NoError(t, err)
			return record
		}

		// Read the new PTS record.
		newRec := getNewPTSRecord()
		require.NotNil(t, newRec.Target)

		// Assert the new PTS record has the right targets.
		targetIDs = newRec.Target.GetSchemaObjects().IDs
		require.Contains(t, targetIDs, fooID)
		require.Subset(t, targetIDs, systemTablesToProtect)

		// Ensure the old pts record was deleted.
		_, err = readPTSRecord(ctx, t, execCfg, ptp, oldRecordID)
		require.ErrorContains(t, err, "does not exist")
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks)
}

// TestChangefeedUpdateProtectedTimestamp tests that changefeeds using the
// old style PTS records will migrate themselves to use the new style PTS
// records.
func TestChangefeedMigratesProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		ctx := context.Background()

		useOldStylePts := atomic.Bool{}
		useOldStylePts.Store(true)
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.PreserveDeprecatedPts = func() bool {
			return useOldStylePts.Load()
		}

		ptsInterval := 50 * time.Millisecond
		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, ptsInterval)
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, ptsInterval)

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sysDB := sqlutils.MakeSQLRunner(s.SystemServer.SQLConn(t))

		sysDB.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'")
		sysDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'") // speeds up the test
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '20ms'`)
		defer closeFeed(t, foo)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		execCfg := s.Server.ExecutorConfig().(sql.ExecutorConfig)
		ptp := s.Server.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
		fooDesc := desctestutils.TestingGetPublicTableDescriptor(s.SystemServer.DB(), s.Codec, "d", "foo")
		fooID := fooDesc.GetID()
		descID := descpb.ID(keys.DescriptorTableID)

		jobFeed := foo.(cdctest.EnterpriseTestFeed)

		removePTSTarget := func(recordID uuid.UUID) error {
			return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				if _, err := txn.ExecEx(ctx, "pts-test", txn.KV(), sessiondata.NodeUserSessionDataOverride,
					fmt.Sprintf(
						"UPDATE system.protected_ts_records SET target = NULL WHERE id = '%s'",
						recordID),
				); err != nil {
					return err
				}
				return nil
			})
		}

		// Wipe out the targets from the changefeed PTS record, simulating an old-style PTS record.
		oldRecordID := getPTSRecordID(ctx, t, registry, jobFeed)
		require.NoError(t, removePTSTarget(oldRecordID))
		rec, err := readPTSRecord(ctx, t, execCfg, ptp, oldRecordID)
		require.NoError(t, err)
		require.NotNil(t, rec)
		require.Nil(t, rec.Target)

		// Flip the knob so the changefeed migrates the old style PTS record to the new one.
		useOldStylePts.Store(false)

		getNewPTSRecord := func() *ptpb.Record {
			var recID uuid.UUID
			var record *ptpb.Record
			testutils.SucceedsSoon(t, func() error {
				recID = getPTSRecordID(ctx, t, registry, jobFeed)
				if recID.Equal(oldRecordID) {
					return errors.New("waiting for new PTS record")
				}

				return nil
			})
			record, err = readPTSRecord(ctx, t, execCfg, ptp, recID)
			if err != nil {
				t.Fatal(err)
			}
			return record
		}

		// Read the new PTS record.
		newRec := getNewPTSRecord()
		require.NotNil(t, newRec.Target)

		// Assert the new PTS record has the right targets.
		targetIDs := newRec.Target.GetSchemaObjects().IDs
		require.Contains(t, targetIDs, fooID)
		require.Contains(t, targetIDs, descID)

		// Ensure the old pts record was deleted.
		_, err = readPTSRecord(ctx, t, execCfg, ptp, oldRecordID)
		require.ErrorContains(t, err, "does not exist")
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks)
}

func fetchRoleMembers(
	ctx context.Context, execCfg *sql.ExecutorConfig, ts hlc.Timestamp,
) ([][]string, error) {
	var roleMembers [][]string
	err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		if err := txn.KV().SetFixedTimestamp(ctx, ts); err != nil {
			return err
		}
		it, err := txn.QueryIteratorEx(ctx, "test-get-role-members", txn.KV(), sessiondata.NoSessionDataOverride, "SELECT role, member FROM system.role_members")
		if err != nil {
			return err
		}
		defer func() { _ = it.Close() }()

		var ok bool
		for ok, err = it.Next(ctx); ok && err == nil; ok, err = it.Next(ctx) {
			role, member := string(tree.MustBeDString(it.Cur()[0])), string(tree.MustBeDString(it.Cur()[1]))
			roleMembers = append(roleMembers, []string{role, member})
		}
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return roleMembers, nil
}

type userPass struct {
	username string
	password tree.Datum
}

func fetchUsersAndPasswords(
	ctx context.Context, execCfg *sql.ExecutorConfig, ts hlc.Timestamp,
) ([]userPass, error) {
	var users []userPass
	err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		if err := txn.KV().SetFixedTimestamp(ctx, ts); err != nil {
			return err
		}
		it, err := txn.QueryIteratorEx(ctx, "test-get-users", txn.KV(),
			sessiondata.NoSessionDataOverride,
			`SELECT username, "hashedPassword" FROM system.users`,
		)
		if err != nil {
			return err
		}
		defer func() { _ = it.Close() }()

		var ok bool
		for ok, err = it.Next(ctx); ok && err == nil; ok, err = it.Next(ctx) {
			username := string(tree.MustBeDString(it.Cur()[0]))
			users = append(users, userPass{username: username, password: it.Cur()[1]})
		}
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return users, nil
}

func getPTSRecordID(
	ctx context.Context, t *testing.T, registry *jobs.Registry, jobFeed cdctest.EnterpriseTestFeed,
) uuid.UUID {
	var recordID uuid.UUID
	testutils.SucceedsSoon(t, func() error {
		progress, err := loadProgressErr(ctx, registry, jobFeed)
		if err != nil {
			return err
		}
		uid := progress.GetChangefeed().ProtectedTimestampRecord
		if uid == uuid.Nil {
			return errors.Newf("no pts record")
		}
		recordID = uid
		return nil
	})
	return recordID
}

func readPTSRecord(
	ctx context.Context,
	t *testing.T,
	execCfg sql.ExecutorConfig,
	ptp protectedts.Provider,
	recID uuid.UUID,
) (rec *ptpb.Record, err error) {
	err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		rec, err = ptp.WithTxn(txn).GetRecord(ctx, recID)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func loadProgressErr(
	ctx context.Context, registry *jobs.Registry, jobFeed cdctest.EnterpriseTestFeed,
) (jobspb.Progress, error) {
	job, err := registry.LoadJob(ctx, jobFeed.JobID())
	if err != nil {
		return jobspb.Progress{}, err
	}
	return job.Progress(), nil
}
