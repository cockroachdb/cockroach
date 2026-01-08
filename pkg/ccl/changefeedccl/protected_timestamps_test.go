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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcprogresspb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptutil"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
			resolved='1s', min_checkpoint_frequency='1s'`,
			optOutOfMetamorphicDBLevelChangefeed{
				reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
			})
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

		// TODO(#158779): Re-add per table protected timestamps setting and
		// fetch this value from that cluster setting.
		perTablePTSEnabled := false

		if perTablePTSEnabled {
			// We expect 2 PTS records: one for the per-table PTS record and
			// one for the system tables PTS record.
			require.Equal(t, 2, getNumPTSRecords())
		} else {
			require.Equal(t, 1, getNumPTSRecords())
		}

		require.NoError(t, jobFeed.Pause())
		sqlDB.Exec(t, fmt.Sprintf("ALTER CHANGEFEED %d ADD TABLE foo2 with initial_scan='yes'", jobFeed.JobID()))
		require.NoError(t, jobFeed.Resume())

		_, _ = expectResolvedTimestamp(t, f2)

		if perTablePTSEnabled {
			// We protect the new table the next time the highwater is advanced.
			// TODO(#153894): Newly added/dropped tables should be protected
			// at ALTER time.
			eFeed, ok := f2.(cdctest.EnterpriseTestFeed)
			require.True(t, ok)
			hwm, err := eFeed.HighWaterMark()
			require.NoError(t, err)
			require.NoError(t, eFeed.WaitForHighWaterMark(hwm))

			// We expect 3 PTS records: one per-table record for each of the two
			// tables and one for the system tables PTS record.
			require.Equal(t, 3, getNumPTSRecords())
		} else {
			require.Equal(t, 1, getNumPTSRecords())
		}
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

		t.Run("canceled due to gc_protect_expires_after option", func(t *testing.T) {
			testutils.RunValues(t, "initially-protected-with", []string{"none", "option", "setting"},
				func(t *testing.T, initialProtect string) {
					defer func() {
						sqlDB.Exec(t, `RESET CLUSTER SETTING changefeed.protect_timestamp.max_age`)
					}()

					if initialProtect == "option" {
						// We set the cluster setting to something small to make sure that
						// the option alone is able to protect the PTS record.
						sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.protect_timestamp.max_age = '1us'`)
					} else {
						sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.protect_timestamp.max_age = '24h'`)
					}

					feedStmt := `CREATE CHANGEFEED FOR TABLE foo`
					switch initialProtect {
					case "none":
						feedStmt += ` WITH gc_protect_expires_after='1us'`
					case "option":
						feedStmt += ` WITH gc_protect_expires_after='24h'`
					}

					feed, err := f.Feed(feedStmt)
					require.NoError(t, err)
					defer func() {
						closeFeed(t, feed)
					}()

					jobFeed := feed.(cdctest.EnterpriseTestFeed)

					if initialProtect != "none" {
						require.NoError(t, jobFeed.Pause())

						// Wait a little bit and make sure the job ISN'T canceled.
						require.ErrorContains(t, jobFeed.WaitDurationForState(10*time.Second, func(s jobs.State) bool {
							return s == jobs.StateCanceled
						}), `still waiting for job status; current status is "paused"`)

						if initialProtect == "option" {
							// Set the cluster setting back to something high to make sure the
							// option alone can cause the changefeed to be canceled.
							sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.protect_timestamp.max_age = '24h'`)
						}

						// Set option to something small so that job will be canceled.
						sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET gc_protect_expires_after = '1us'`, jobFeed.JobID()))
					}

					// Stale PTS record should trigger job cancellation.
					require.NoError(t, jobFeed.WaitForState(func(s jobs.State) bool {
						return s == jobs.StateCanceled
					}))
				})
		})

		t.Run("canceled due to changefeed.protect_timestamp.max_age setting", func(t *testing.T) {
			testutils.RunValues(t, "initially-protected-with", []string{"none", "option", "setting"},
				func(t *testing.T, initialProtect string) {
					defer func() {
						sqlDB.Exec(t, `RESET CLUSTER SETTING changefeed.protect_timestamp.max_age`)
					}()

					if initialProtect == "setting" {
						sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.protect_timestamp.max_age = '24h'`)
					} else {
						sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.protect_timestamp.max_age = '1us'`)
					}

					// Set the max age cluster setting to something small.
					feedStmt := `CREATE CHANGEFEED FOR TABLE foo`
					if initialProtect == "option" {
						feedStmt += ` WITH gc_protect_expires_after='24h'`
					}
					feed, err := f.Feed(feedStmt)
					require.NoError(t, err)
					defer func() {
						closeFeed(t, feed)
					}()

					jobFeed := feed.(cdctest.EnterpriseTestFeed)

					if initialProtect != "none" {
						require.NoError(t, jobFeed.Pause())

						// Wait a little bit and make sure the job ISN'T canceled.
						require.ErrorContains(t, jobFeed.WaitDurationForState(10*time.Second, func(s jobs.State) bool {
							return s == jobs.StateCanceled
						}), `still waiting for job status; current status is "paused"`)

						switch initialProtect {
						case "option":
							// Reset the option so that it defaults to the cluster setting.
							sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET gc_protect_expires_after = '0s'`, jobFeed.JobID()))
						case "setting":
							// Modify the cluster setting and do an ALTER CHANGEFEED so that
							// the new value is picked up.
							sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.protect_timestamp.max_age = '1us'`)
							sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET diff`, jobFeed.JobID()))
						}
					}

					// Stale PTS record should trigger job cancellation.
					require.NoError(t, jobFeed.WaitForState(func(s jobs.State) bool {
						return s == jobs.StateCanceled
					}))
				})
		})
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
	testutils.SetVModule(t, "spanconfigstore=2,store=2,reconciler=3,mvcc_gc_queue=2,kvaccessor=2")

	settings := cluster.MakeTestingClusterSettings()
	spanconfigjob.ReconciliationJobCheckpointInterval.Override(ctx, &settings.SV, 1*time.Second)

	// Keep track of where the spanconfig reconciler is up to.
	lastReconcilerCheckpoint := atomic.Value{}
	lastReconcilerCheckpoint.Store(hlc.Timestamp{})
	s, db, stopServer := startTestFullServer(t, makeOptions(t, withKnobsFn(
		func(knobs *base.TestingKnobs) {
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
		}),
		feedTestwithSettings(settings),
	))

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
		DescID: fooDescr.GetID(),
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
	ptr := createCombinedProtectedTimestampRecord(ctx, jobID, targets, ts)
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
		require.NoError(t, repl.TestingReadProtectedTimestamps(ctx))
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
		// TODO(#158779): This test assumes the non-per-table PTS behavior.
		// When we add back that setting, make sure this test turns it off.

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

		// TODO(#158779): When we re-enable per-table PTS, make sure this test
		// correctly computes if per-table PTS is enabled by checking that setting
		// and the per table tracking setting.
		perTablePTSEnabled := false

		// Gets the system tables specific PTS record ID which exist when
		// per-table protected timestamps are enabled.
		getSystemTablesRecordID := func() uuid.UUID {
			var systemTablesRecordID uuid.UUID
			require.NoError(t, execCfg.InternalDB.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
				var ptsEntries cdcprogresspb.ProtectedTimestampRecords
				if err := readChangefeedJobInfo(ctx, perTableProtectedTimestampsFilename, &ptsEntries, txn, jobFeed.JobID()); err != nil {
					return err
				}
				systemTablesRecordID = ptsEntries.SystemTables
				return nil
			}))
			return systemTablesRecordID
		}

		// Remove a PTS target from the changefeed PTS record. This simulates a
		// PTS record that is missing a system table target.
		oldRecordID := func() uuid.UUID {
			if perTablePTSEnabled {
				return getSystemTablesRecordID()
			}
			return getPTSRecordID(ctx, t, registry, jobFeed)
		}()
		require.NotEqual(t, oldRecordID, uuid.Nil)
		require.NoError(t, removeOnePTSTarget(oldRecordID))

		// Sanity check: make sure that it worked
		oldRecord, err := readPTSRecord(ctx, t, execCfg, ptp, oldRecordID)
		require.NoError(t, err)
		targetIDs := oldRecord.Target.GetSchemaObjects().IDs
		require.NotSubset(t, targetIDs, systemTablesToProtect)

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(s.SystemServer.DB(), s.Codec, "d", "foo")
		fooID := fooDesc.GetID()
		if !perTablePTSEnabled {
			require.Contains(t, targetIDs, fooID)
		}

		// Flip the knob so the changefeed migrates the record
		dontMigrate.Store(false)

		getNewPTSRecord := func() *ptpb.Record {
			var recID uuid.UUID
			var record *ptpb.Record
			testutils.SucceedsSoon(t, func() error {
				recID = func() uuid.UUID {
					if perTablePTSEnabled {
						return getSystemTablesRecordID()
					}
					return getPTSRecordID(ctx, t, registry, jobFeed)
				}()
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
		if !perTablePTSEnabled {
			require.Contains(t, targetIDs, fooID)
		}
		require.Subset(t, targetIDs, systemTablesToProtect)

		// Ensure the old pts record was deleted.
		_, err = readPTSRecord(ctx, t, execCfg, ptp, oldRecordID)
		require.ErrorContains(t, err, "does not exist")
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks)
}

// TestChangefeedUpdateProtectedTimestamp tests that changefeeds using the
// old style PTS records will migrate themselves to use the new style PTS
// records. The old style PTS records did not specify target tables.
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
		// TODO(#158779): This test assumes the non-per-table PTS behavior.
		// When we add back that setting, make sure this test turns it off.

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

// TestChangefeedProtectedTimestampUpdateForMultipleTables verifies that
// a changefeed with multiple tables will successfully create and update
// protected timestamp records when PerTableProtectedTimestamps is disabled,
// that it will NOT create per-table protected timestamp records, and that
// it will increment the relevant metrics when managing its protected timestamps.
func TestChangefeedProtectedTimestampUpdateForMultipleTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	verifyFunc := func() {}
	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		defer verifyFunc()
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		// Checkpoint and trigger potential protected timestamp updates frequently.
		// Make the protected timestamp lag long enough that it shouldn't be
		// immediately updated after a restart.
		changefeedbase.SpanCheckpointInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Millisecond)
		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Millisecond)
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 10*time.Hour)
		// TODO(#158779): This test assumes the non-per-table PTS behavior.
		// When we add back that setting, make sure this test turns it off.

		sqlDB.Exec(t, `CREATE TABLE foo (id INT)`)
		sqlDB.Exec(t, `CREATE TABLE bar (id INT)`)
		registry := s.Server.JobRegistry().(*jobs.Registry)
		metrics := registry.MetricsStruct().Changefeed.(*Metrics)
		createPTSCount, _ := metrics.AggMetrics.Timers.PTSCreate.WindowedSnapshot().Total()
		managePTSCount, _ := metrics.AggMetrics.Timers.PTSManage.WindowedSnapshot().Total()
		managePTSErrorCount, _ := metrics.AggMetrics.Timers.PTSManageError.WindowedSnapshot().Total()
		require.Equal(t, int64(0), createPTSCount)
		require.Equal(t, int64(0), managePTSCount)
		require.Equal(t, int64(0), managePTSErrorCount)

		createStmt := `CREATE CHANGEFEED FOR foo, bar
WITH resolved='10ms', min_checkpoint_frequency='100ms', initial_scan='no'`
		testFeed := feed(t, f, createStmt)
		defer closeFeed(t, testFeed)

		createPTSCount, _ = metrics.AggMetrics.Timers.PTSCreate.WindowedSnapshot().Total()
		managePTSCount, _ = metrics.AggMetrics.Timers.PTSManage.WindowedSnapshot().Total()
		require.Equal(t, int64(1), createPTSCount)
		require.Equal(t, int64(0), managePTSCount)

		eFeed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		// Wait for the changefeed to checkpoint and update PTS at least once.
		require.NoError(t, eFeed.WaitForHighWaterMark(hlc.Timestamp{}))

		// TODO(#151690): Ideally we could use the same pts record id
		// for all times we get the PTS, but that's not possible right now
		// because of the linked issue (pts records rewrite unnecessarily for
		// multi-table feeds).
		getPTS := func() hlc.Timestamp {
			p, err := eFeed.Progress()
			require.NoError(t, err)
			ptsQry := fmt.Sprintf(`SELECT ts FROM system.protected_ts_records WHERE id = '%s'`, p.ProtectedTimestampRecord)
			var tsStr string
			sqlDB.QueryRow(t, ptsQry).Scan(&tsStr)
			require.NoError(t, err)
			ts, err := hlc.ParseHLC(tsStr)
			require.NoError(t, err)
			return ts
		}
		ts := getPTS()

		// Force the changefeed to restart.
		require.NoError(t, eFeed.Pause())
		require.NoError(t, eFeed.Resume())

		// Wait for a new checkpoint.
		hwm, err := eFeed.HighWaterMark()
		require.NoError(t, err)
		require.NoError(t, eFeed.WaitForHighWaterMark(hwm))

		// TODO(#151690): Check that the PTS was not updated after the resume.
		// Right now we cannot do this without the test flaking because of the
		// linked issue (pts records rewrite unnecessarily for multi-table feeds).

		ptsLag := 10 * time.Millisecond
		// Lower the PTS lag and check that it has been updated.
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, ptsLag)

		hwm, err = eFeed.HighWaterMark()
		require.NoError(t, err)
		require.NoError(t, eFeed.WaitForHighWaterMark(hwm))

		ts2 := getPTS()
		require.True(t, ts.Less(ts2))

		managePTSCount, _ = metrics.AggMetrics.Timers.PTSManage.WindowedSnapshot().Total()
		managePTSErrorCount, _ = metrics.AggMetrics.Timers.PTSManageError.WindowedSnapshot().Total()
		require.GreaterOrEqual(t, managePTSCount, int64(1))
		require.Equal(t, int64(0), managePTSErrorCount)

		execCfg := s.Server.ExecutorConfig().(sql.ExecutorConfig)
		err = execCfg.InternalDB.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
			var ptsEntries cdcprogresspb.ProtectedTimestampRecords
			if err := readChangefeedJobInfo(ctx, perTableProtectedTimestampsFilename, &ptsEntries, txn, eFeed.JobID()); err != nil {
				return err
			}

			require.Equal(t, 0, len(ptsEntries.UserTables))
			require.Equal(t, uuid.Nil, ptsEntries.SystemTables)
			return nil
		})

		require.NoError(t, err)
	}

	withTxnRetries := withArgsFn(func(args *base.TestServerArgs) {
		requestFilter, vf := testutils.TestingRequestFilterRetryTxnWithPrefix(t, changefeedJobProgressTxnName, 1)
		args.Knobs.Store = &kvserver.StoreTestingKnobs{
			TestingRequestFilter: requestFilter,
		}
		verifyFunc = vf
	})

	cdcTest(t, testFn, feedTestForceSink("kafka"), withTxnRetries)
}

// TestChangefeedPerTableProtectedTimestampProgression tests that
// the changefeed's per-table protected timestamps progress as expected
// when table lag is introduced and removed.
func TestChangefeedPerTableProtectedTimestampProgression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 158779, "unreleased feature")

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// It's not possible to enable per-table protected timestamps
		// since it's disabled.
		// TODO(#158779): This is where we will enable the per-table pts setting.
		changefeedbase.TrackPerTableProgress.Override(
			context.Background(), &s.Server.ClusterSettings().SV, true)

		ptsLag := 100 * time.Millisecond

		// Configure frequent checkpointing and PTS updates for faster testing
		changefeedbase.SpanCheckpointInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 100*time.Millisecond)
		changefeedbase.ProtectTimestampInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, ptsLag)
		changefeedbase.ProtectTimestampLag.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 50*time.Millisecond)

		sqlDB.Exec(t, `CREATE TABLE table1 (id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE table2 (id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE table3 (id INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO table1 VALUES (1)`)
		sqlDB.Exec(t, `INSERT INTO table2 VALUES (1)`)
		sqlDB.Exec(t, `INSERT INTO table3 VALUES (1)`)

		var table1ID, table2ID, table3ID descpb.ID
		sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables WHERE name = 'table1' AND database_name = current_database()`).Scan(&table1ID)
		sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables WHERE name = 'table2' AND database_name = current_database()`).Scan(&table2ID)
		sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables WHERE name = 'table3' AND database_name = current_database()`).Scan(&table3ID)

		createStmt := `CREATE CHANGEFEED FOR table1, table2, table3
		WITH resolved='100ms', min_checkpoint_frequency='100ms'`
		testFeed := feed(t, f, createStmt)
		defer closeFeed(t, testFeed)

		assertPayloads(t, testFeed, []string{
			`table1: [1]->{"after": {"id": 1}}`,
			`table2: [1]->{"after": {"id": 1}}`,
			`table3: [1]->{"after": {"id": 1}}`,
		})

		eFeed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		execCfg := s.Server.ExecutorConfig().(sql.ExecutorConfig)

		// Assert that the feed-level PTS record does not exist because per-table
		// protected timestamps are enabled.
		progress, err := eFeed.Progress()
		require.NoError(t, err)
		require.Equal(t, progress.ProtectedTimestampRecord, uuid.UUID{})

		systemTablesPTS := hlc.Timestamp{}
		tablePTS := make(map[descpb.ID]hlc.Timestamp)
		expectedTables := map[descpb.ID]struct{}{
			table1ID: {},
			table2ID: {},
			table3ID: {},
		}
		testutils.SucceedsSoon(t, func() error {
			return execCfg.InternalDB.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
				var ptsEntries cdcprogresspb.ProtectedTimestampRecords
				if err := readChangefeedJobInfo(
					ctx, perTableProtectedTimestampsFilename, &ptsEntries, txn, eFeed.JobID(),
				); err != nil {
					return err
				}

				if len(ptsEntries.UserTables) != len(expectedTables) {
					return errors.Newf(
						"expected %d per-table PTS records, got %d",
						len(expectedTables), len(ptsEntries.UserTables),
					)
				}

				// We also collect all PTS record IDs to assert they are unique.
				ptsRecordIDs := make(map[uuid.UUID]struct{})
				for tableID := range expectedTables {
					// Assert that the per-table PTS record exists and is unique.
					if ptsEntries.UserTables[tableID] == uuid.Nil {
						return errors.Newf("expected PTS record for table %d", tableID)
					}
					ptsRecordID := ptsEntries.UserTables[tableID]
					if _, exists := ptsRecordIDs[ptsRecordID]; exists {
						return errors.Newf("duplicate PTS record ID %s found", ptsRecordID)
					}
					ptsRecordIDs[ptsRecordID] = struct{}{}

					// Assert that the per-table PTS record targets only the user table.
					tableTarget := ptutil.GetPTSTarget(t, sqlDB, &ptsRecordID)
					require.Equal(t, tableID, tableTarget.GetSchemaObjects().IDs[0])
					require.Equal(t, 1, len(tableTarget.GetSchemaObjects().IDs))

					// We save the protection timestamps for each table in tablePTS
					// so that we can assert that they progress as expected later.
					tablePTS[tableID] =
						ptutil.GetPTSTimestamp(t, sqlDB, ptsEntries.UserTables[tableID])
				}

				// Assert that the system tables PTS record exists.
				if ptsEntries.SystemTables == uuid.Nil {
					return errors.Newf("expected system tables PTS record")
				}

				// Assert that the system tables PTS record targets all system tables.
				systemTablesTarget := ptutil.GetPTSTarget(t, sqlDB, &ptsEntries.SystemTables)
				actualProtectedTables := systemTablesTarget.GetSchemaObjects().IDs

				require.Equal(t, len(systemTablesToProtect), len(actualProtectedTables))
				for _, id := range systemTablesToProtect {
					require.Contains(t, actualProtectedTables, id)
				}

				// Store its timestamp so that we can assert that it progresses later.
				systemTablesPTS = ptutil.GetPTSTimestamp(t, sqlDB, ptsEntries.SystemTables)
				require.NotEqual(t, systemTablesPTS, hlc.Timestamp{})
				return nil
			})
		})

		// Assert that each per table PTS record progresses as expected.
		testutils.SucceedsSoon(t, func() error {
			return execCfg.InternalDB.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
				var ptsEntries cdcprogresspb.ProtectedTimestampRecords
				if err := readChangefeedJobInfo(
					ctx, perTableProtectedTimestampsFilename, &ptsEntries, txn, eFeed.JobID(),
				); err != nil {
					return err
				}

				for tableID := range expectedTables {
					newTablePTS :=
						ptutil.GetPTSTimestamp(t, sqlDB, ptsEntries.UserTables[tableID])
					if !newTablePTS.After(tablePTS[tableID]) {
						return errors.Newf(
							"expected PTS record for table %d to progress since %s, got %s",
							tableID, tablePTS[tableID], newTablePTS,
						)
					}
				}

				newSystemTablesPTS :=
					ptutil.GetPTSTimestamp(t, sqlDB, ptsEntries.SystemTables)
				if !newSystemTablesPTS.After(systemTablesPTS) {
					return errors.Newf(
						"expected system tables PTS to progress since %s, got %s",
						systemTablesPTS, newSystemTablesPTS,
					)
				}
				return nil
			})
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestCachedEventDescriptorGivesUpdatedTimestamp is a regression test for
// #156091. It tests that when a changefeed with a cdc query receives events
// from the KVFeed out of order, even across table descriptor versions, the
// query will be replanned at a timestamp we know has not been garbage collected.
// Previously, we would get an old timestamp from the cached event descriptor
// and if the db descriptor version had changed and been GC'd, this replan would
// fail, failing the changefeed.
func TestCachedEventDescriptorGivesUpdatedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Making sure there's only one worker so that there is a single event
		// descriptor cache. This means that the later events will use the
		// cached event descriptor and its outdated timestamp, to properly
		// reproduce the issue.
		changefeedbase.EventConsumerWorkers.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 1)

		/*
			The situation from issue #156091 that we are trying to reproduce here
			happens when a changefeed is replanning a CDC query for a table descriptor
			version it has seen before. In that case, it replans the query at the
			timestamp (stored in the cache) of the first event it saw for that table
			descriptor version.

			That's problematic because that could be well before the highwater for
			the feed and therefore not protected by PTS. Even though our protected
			timestamp system ensures that the relevant table descriptor version has
			not been GC'd (since it is still used by the event we're processing),
			there is no such guarantee that the *DB* descriptor version from that
			time (which wasn't around at the time of that event) has not been GC'd.
			If we try to fetch the DB descriptor version (as we do when replanning
			a CDC query) and it has already been GC'd, the CDC query replan will
			fail, and the changefeed with it.

			So, in order to reproduce this we require that
			a) KV events must come out of order so that we are both doing a CDC
			query replan AND that the timestamp for that replan comes from the
			EventDescriptor cache.
			b) the DB descriptor version has changed between the event that seeded
			the cache and the later event that shares that table descriptor version
			and finally
			c) that the old DB descriptor version has been garbage collected.

			Ultimately the series of events will be
			1. We see event 1 with table descriptor version 1 and DB descriptor
			version 1. This is processed by the changefeeed seeding the event
			descriptor cache at T_0.
			2. We update the DB descriptor version to version 2.
			3. We garbage collect the descriptor table through time T_0 and with it
			DB descriptor version 1.
			4. We make an update causing event 2 with the same table descriptor version
			as event 1 (table descriptor version 1) but whose kv event will only
			come through after event 3's.
			5. We update the table descriptor version (to version 2) and make an
			update (event 3).
			6. The KV event for event 3 comes in first, causing the changefeed's
			current table version to be table version 2.
			7. The KV event for event 2 comes in out of order causing a replan of
			the CDC query to happen (back to table descriptor version 1).

			If we return the timestamp from the first event we saw on table descriptor
			version 1, this replan will try to fetch the DB descriptor at time T_0
			(when event 1 happened)	which has been garbage collected and would fail
			the feed.
		*/
		var dbDescTS atomic.Value
		dbDescTS.Store(hlc.Timestamp{})
		var hasGCdDBDesc atomic.Bool
		var kvEvents []kvevent.Event
		var hasProcessedAllEvents atomic.Bool
		beforeAddKnob := func(ctx context.Context, e kvevent.Event) (_ context.Context, _ kvevent.Event, shouldAdd bool) {
			// Since we are going to be ignoring some KV events, we don't send
			// resolved events to avoid violating changefeed guarantees.
			// Since this test also depends on specific GC behavior, we handle GC
			// ourselves.
			if e.Type() == kvevent.TypeResolved {
				resolvedTimestamp := e.Timestamp()

				// We need to wait for the resolved timestamp to move past the
				// first kv event so that we know it's safe to GC the first database
				// descriptor version.
				if !hasGCdDBDesc.Load() {
					dbDescTSVal := dbDescTS.Load().(hlc.Timestamp)
					if !dbDescTSVal.IsEmpty() && dbDescTSVal.Less(resolvedTimestamp) {
						t.Logf("GCing database descriptor table at timestamp: %s", dbDescTSVal)
						forceTableGCAtTimestamp(t, s.SystemServer, "system", "descriptor", dbDescTSVal)
						hasGCdDBDesc.Store(true)
					}
				}

				// We use the resolved events to know when we can stop the test.
				if len(kvEvents) > 2 && resolvedTimestamp.After(kvEvents[2].Timestamp()) {
					hasProcessedAllEvents.Store(true)
				}

				// Do not send any of the resolved events.
				return ctx, e, false
			}

			if e.Type() == kvevent.TypeKV {
				if len(kvEvents) > 0 && e.Timestamp() == kvEvents[0].Timestamp() {
					// Ignore duplicates of the first kv event which may come while
					// we're waiting to GC the first database descriptor version.
					return ctx, e, false
				}

				kvEvents = append(kvEvents, e)
				switch len(kvEvents) {
				case 1:
					// Event 1 is sent as normal to seed the event descriptor cache.
					t.Logf("Event 1 timestamp: %s", kvEvents[0].Timestamp())
					return ctx, kvEvents[0], true
				case 2:
					// Event 2 is stored in kvEvents and we will send it later.
					// Sending it after event 3, which has a different table
					// descriptor version, will cause CDC query replan.
					return ctx, e, false
				case 3:
					// Event 3 is sent as normal to replan the CDC query with the
					// new table descriptor version.
					t.Logf("Event 3 timestamp: %s", kvEvents[2].Timestamp())
					return ctx, kvEvents[2], true
				case 4:
					// Now we send event 2 *after* we've sent event 3. This should
					// cause a CDC query replan with a cached event descriptor,
					// since the table version is the same as event 1. If we use
					// the timestamp of event 1, that replan will fail to fetch
					// the GC'd DB descriptor version failing the changefeed.
					// This is what we saw in issue #156091.
					t.Logf("Event 2 timestamp: %s", kvEvents[1].Timestamp())
					return ctx, kvEvents[1], true
				default:
					// We do not need to send any more events after events
					// 1, 2 and 3 have been processed.
					return ctx, e, false
				}
			}

			return ctx, e, true
		}

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		knobs.MakeKVFeedToAggregatorBufferKnobs = func() kvevent.BlockingBufferTestingKnobs {
			return kvevent.BlockingBufferTestingKnobs{
				BeforeAdd: beforeAddKnob,
			}
		}

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		foo := feed(t, f, `CREATE CHANGEFEED WITH resolved = '10ms' AS SELECT * FROM foo`)
		defer closeFeed(t, foo)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		// Change the database descriptor version by granting permission to a user.
		sqlDB.Exec(t, `CREATE USER testuser`)
		sqlDB.Exec(t, `GRANT CREATE ON DATABASE d TO testuser`)

		// Fetch the cluster logical timestamp so that we can make sure the
		// resolved timestamp has moved past it and it's safe to GC the
		// descriptor table (specifically the first database descriptor version).
		var dbDescTSString string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&dbDescTSString)
		dbDescTSParsed, err := hlc.ParseHLC(dbDescTSString)
		require.NoError(t, err)
		dbDescTS.Store(dbDescTSParsed)
		t.Logf("Timestamp after DB descriptor version change: %s", dbDescTSParsed)

		testutils.SucceedsSoon(t, func() error {
			if !hasGCdDBDesc.Load() {
				return errors.New("database descriptor table not GCed")
			}
			return nil
		})

		// This event will be delayed by the KVFeed until after event 3 has been sent.
		// Instead of sending it out, we GC the descriptor table including the
		// old database descriptor version.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2)`)

		// Change the table descriptor version by granting permission to a user.
		sqlDB.Exec(t, `GRANT CREATE ON TABLE foo TO testuser`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (3)`)

		// Since we skip processing the KV event for event 2, we will replace
		// the KV event for event 4 the stored one for event 2. This event is
		// not itself relevant to the test, but helps us send the KV events out
		// of order.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (4)`)

		// Wait for changefeed events 1, 2 and 3 to be processed. If the feed
		// has failed, stop waiting and fail the test immediately.
		testutils.SucceedsSoon(t, func() error {
			var errorStr string
			sqlDB.QueryRow(t, `SELECT error FROM [SHOW CHANGEFEED JOBS] WHERE job_id = $1`, foo.(cdctest.EnterpriseTestFeed).JobID()).Scan(&errorStr)
			if errorStr != "" {
				t.Fatalf("changefeed error: %s", errorStr)
				return nil
			}
			if !hasProcessedAllEvents.Load() {
				return errors.New("events not processed")
			}
			return nil
		})
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
