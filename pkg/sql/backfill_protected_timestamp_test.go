// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestValidationWithProtectedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "test takes too long")
	skip.UnderStress(t, "test takes too long")
	skip.UnderRace(t, "test takes too long")

	ctx := context.Background()
	indexValidationQueryWait := make(chan struct{})
	indexValidationQueryResume := make(chan struct{})
	validationQuerySeen := sync.Once{}

	indexScanQuery := regexp.MustCompile(`SELECT count\(1\) FROM \[\d+ AS t\]@\[2\]`)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &eval.TestingKnobs{
				ForceProductionValues: true,
			},
			Store: &kvserver.StoreTestingKnobs{
				DisableSplitQueue: true,
			},
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeExecute: func(ctx context.Context, sql string, descriptors *descs.Collection) {
					if indexScanQuery.MatchString(sql) {
						validationQuerySeen.Do(func() {
							indexValidationQueryWait <- struct{}{}
							<-indexValidationQueryResume
						})

					}
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()
	tenantSettings := ts.ClusterSettings()
	protectedts.PollInterval.Override(ctx, &tenantSettings.SV, time.Millisecond)
	r := sqlutils.MakeSQLRunner(db)

	systemSqlDb := s.SystemLayer().SQLConn(t, serverutils.DBName("system"))
	rSys := sqlutils.MakeSQLRunner(systemSqlDb)

	// Refreshes the in-memory protected timestamp state to asOf.
	refreshTo := func(t *testing.T, tableKey roachpb.Key, asOf hlc.Timestamp) {
		store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		require.NoError(t, err)
		var repl *kvserver.Replica
		testutils.SucceedsSoon(t, func() error {
			repl = store.LookupReplica(roachpb.RKey(tableKey))
			if repl == nil {
				return errors.New(`could not find replica`)
			}
			return nil
		})
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader
		require.NoError(
			t,
			spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, asOf),
		)
		require.NoError(t, repl.ReadProtectedTimestampsForTesting(ctx))
	}
	// Refresh forces the PTS cache to update to at least asOf.
	refreshPTSCacheTo := func(t *testing.T, asOf hlc.Timestamp) {
		ptp := ts.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		require.NoError(t, ptp.Refresh(ctx, asOf))
	}

	for _, sql := range []string{
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval ='10ms'",
		"SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval ='10ms'",
	} {
		rSys.Exec(t, sql)
	}
	for _, sql := range []string{
		"SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
		"ALTER DATABASE defaultdb CONFIGURE ZONE USING gc.ttlseconds = 1",
		"CREATE TABLE t(n int)",
		"ALTER TABLE t CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 67108864, gc.ttlseconds = 1",
		"INSERT INTO t(n) SELECT * FROM generate_series(1, 250000)",
	} {
		r.Exec(t, sql)
	}

	getTableID := func() (tableID uint32) {
		r.QueryRow(t, `SELECT table_id FROM crdb_internal.tables`+
			` WHERE name = 't' AND database_name = current_database()`).Scan(&tableID)
		return tableID
	}
	tableID := getTableID()
	tableKey := ts.Codec().TablePrefix(tableID)

	grp := ctxgroup.WithContext(ctx)
	grp.Go(func() error {
		<-indexValidationQueryWait
		defer func() {
			// Always unblock the validation query, even if something
			// fails here.
			indexValidationQueryResume <- struct{}{}
		}()
		getTableRangeIDs := func(t *testing.T) ([]int64, error) {
			t.Helper()
			rows, err := db.QueryContext(ctx, "WITH r AS (SHOW RANGES FROM TABLE t) SELECT range_id FROM r ORDER BY start_key")
			if err != nil {
				return nil, errors.Wrap(err, "failed to query ranges")
			}
			var rangeIDs []int64
			for rows.Next() {
				var rangeID int64
				if err := rows.Scan(&rangeID); err != nil {
					return nil, errors.Wrapf(err, "failed to read row with range id")
				}
				rangeIDs = append(rangeIDs, rangeID)
			}
			if err := rows.Close(); err != nil {
				return nil, err
			}
			return rangeIDs, nil
		}
		ranges, err := getTableRangeIDs(t)
		if err != nil {
			return err
		}
		const retryTxnErrorSubstring = "restart transaction"
		const replicaGCError = "must be after replica GC threshold"
		for {
			if _, err := db.ExecContext(ctx, "BEGIN"); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, "SET sql_safe_updates=off"); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, "DELETE FROM t;"); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES('9999999')"); err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "COMMIT")
			if err != nil {
				if strings.Contains(err.Error(), retryTxnErrorSubstring) ||
					strings.Contains(err.Error(), replicaGCError) {
					err = nil
					continue
				}
				return err
			}
			break
		}
		refreshTo(t, tableKey, ts.Clock().Now())
		refreshPTSCacheTo(t, ts.Clock().Now())
		for _, id := range ranges {
			if _, err := systemSqlDb.ExecContext(ctx, `SELECT crdb_internal.kv_enqueue_replica($1, 'mvccGC', true)`, id); err != nil {
				return err
			}
		}
		return nil
	})
	grp.Go(func() error {
		_, err := db.ExecContext(ctx, `CREATE INDEX foo ON t (n)`)
		return err
	})

	require.NoError(t, grp.Wait())
	// Validate the rows were removed due to the delete.
	// Note: Because of the low GC timestamp we can hit "must be after replica GC
	// threshold" errors.
	var res [][]string
	testutils.SucceedsSoon(t, func() error {
		rows, err := r.DB.QueryContext(ctx, `SELECT n FROM t@foo`)
		if err != nil {
			return err
		}
		res, err = sqlutils.RowsToStrMatrix(rows)
		return err
	})
	if len(res) != 1 {
		t.Errorf("expected %d entries, got %d", 1, len(res))
	}
}

// TestBackfillWithProtectedTS runs operations that backfill into a table and
// confirms that a protected timestamp is setup. It also confirms that if the
// protected timestamp is not ready in time we do not infinitely retry.
func TestBackfillWithProtectedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "test takes too long")
	skip.UnderStress(t, "test takes too long")
	skip.UnderRace(t, "test takes too long")

	ctx := context.Background()
	backfillQueryWait := make(chan struct{})
	backfillQueryResume := make(chan struct{})
	blockBackFillsForPTSFailure := atomic.Bool{}
	blockBackFillsForPTSCheck := atomic.Bool{}
	usingDeclarativeSchemaChanger := atomic.Bool{}
	var s serverutils.TestServerInterface
	var db *gosql.DB
	var tableID uint32
	s, db, _ = serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &eval.TestingKnobs{
				ForceProductionValues: true,
			},
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				RunBeforeBackfill: func() error {
					// Cause the backfill to pause before adding the protected
					// timestamp. This knob is for testing schema changes that
					// are on the declarative schema changer.
					if blockBackFillsForPTSFailure.Load() && usingDeclarativeSchemaChanger.Load() {
						if !blockBackFillsForPTSFailure.Swap(false) {
							return nil
						}
						backfillQueryWait <- struct{}{}
						<-backfillQueryResume
					}
					return nil
				},
			},
			DistSQL: &execinfra.TestingKnobs{
				RunBeforeBackfillChunk: func(sp roachpb.Span) error {
					// Cause the backfill to pause after it already began running
					// and has installed a protected timestamp. This knob is for
					// testing schema changes that use the index backfiller.
					if blockBackFillsForPTSCheck.Load() && usingDeclarativeSchemaChanger.Load() {
						_, prefix, err := s.Codec().DecodeTablePrefix(sp.Key)
						if err != nil || prefix != tableID {
							//nolint:returnerrcheck
							return nil
						}
						if !blockBackFillsForPTSCheck.Swap(false) {
							return nil
						}
						backfillQueryWait <- struct{}{}
						<-backfillQueryResume
					}
					return nil
				},
			},
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeQueryBackfill: func() error {
					// Cause the backfill to pause before adding the protected
					// timestamp. This knob is for testing CREATE MATERIALIZED VIEW.
					if blockBackFillsForPTSFailure.Load() && !usingDeclarativeSchemaChanger.Load() {
						if !blockBackFillsForPTSFailure.Swap(false) {
							return nil
						}
						backfillQueryWait <- struct{}{}
						<-backfillQueryResume
					}
					return nil
				},
			},
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
					// Detect the first scan on table from the backfill, which is
					// after the PTS has been set up. This knob is for testing CREATE
					// MATERIALIZED VIEW.
					if blockBackFillsForPTSCheck.Load() && !usingDeclarativeSchemaChanger.Load() &&
						request.Txn != nil &&
						request.Txn.Name == "schemaChangerBackfill" &&
						request.Requests[0].GetInner().Method() == kvpb.Scan {
						scan := request.Requests[0].GetScan()
						_, prefix, err := s.Codec().DecodeTablePrefix(scan.Key)
						if err != nil || prefix != tableID {
							//nolint:returnerrcheck
							return nil
						}
						if !blockBackFillsForPTSCheck.Swap(false) {
							return nil
						}
						backfillQueryWait <- struct{}{}
						<-backfillQueryResume
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()
	tenantSettings := ts.ClusterSettings()
	protectedts.PollInterval.Override(ctx, &tenantSettings.SV, time.Millisecond)
	r := sqlutils.MakeSQLRunner(db)

	systemSqlDb := s.SystemLayer().SQLConn(t, serverutils.DBName("system"))
	rSys := sqlutils.MakeSQLRunner(systemSqlDb)

	// Refreshes the in-memory protected timestamp state to asOf.
	refreshTo := func(ctx context.Context, tableKey roachpb.Key, asOf hlc.Timestamp) error {
		store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		if err != nil {
			return err
		}
		var repl *kvserver.Replica
		startTime := timeutil.Now()
		for timeutil.Since(startTime) < time.Second*45 {
			repl = store.LookupReplica(roachpb.RKey(tableKey))
			if repl != nil {
				break
			}
		}
		if repl == nil {
			return errors.New(`could not find replica`)
		}
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader
		if err := spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, asOf); err != nil {
			return err
		}
		return repl.ReadProtectedTimestampsForTesting(ctx)
	}
	// Refresh forces the PTS cache to update to at least asOf.
	refreshPTSCacheTo := func(ctx context.Context, asOf hlc.Timestamp) error {
		ptp := ts.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		return ptp.Refresh(ctx, asOf)
	}

	for _, sql := range []string{
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval ='10ms'",
		"SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval ='10ms'",
	} {
		rSys.Exec(t, sql)
	}
	for _, sql := range []string{
		"SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
		"ALTER DATABASE defaultdb CONFIGURE ZONE USING gc.ttlseconds = 5",
	} {
		r.Exec(t, sql)
	}

	const initialRowCount = 500000
	const rowsDeletedPerIteration = 200000
	const rowsAddedPerIteration = 1

	for _, tc := range []struct {
		name                   string
		tableName              string
		backfillSchemaChange   string
		jobDescriptionPrefix   string
		postTestQuery          string
		expectedCount          int
		usingDeclSchemaChanger bool
	}{
		{
			name:                   "create materialized view",
			tableName:              "t_mat_view",
			backfillSchemaChange:   "CREATE MATERIALIZED VIEW test AS (SELECT n from t_mat_view)",
			jobDescriptionPrefix:   "CREATE MATERIALIZED VIEW",
			postTestQuery:          "SELECT count(*) FROM test",
			expectedCount:          initialRowCount - rowsDeletedPerIteration + rowsAddedPerIteration,
			usingDeclSchemaChanger: false,
		},
		{
			name:                   "create index",
			tableName:              "t_idx",
			backfillSchemaChange:   "CREATE INDEX idx ON t_idx(n)",
			jobDescriptionPrefix:   "CREATE INDEX idx",
			postTestQuery:          "SELECT count(*) FROM t_idx@idx",
			expectedCount:          initialRowCount - 2*rowsDeletedPerIteration + 2*rowsAddedPerIteration,
			usingDeclSchemaChanger: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usingDeclarativeSchemaChanger.Store(tc.usingDeclSchemaChanger)
			for _, sql := range []string{
				fmt.Sprintf("CREATE TABLE %s(n int primary key)", tc.tableName),
				fmt.Sprintf("ALTER TABLE %s CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 67108864, gc.ttlseconds = 5", tc.tableName),
				fmt.Sprintf("INSERT INTO %s(n) SELECT * FROM generate_series(1, %d)", tc.tableName, initialRowCount),
			} {
				r.Exec(t, sql)
			}

			getTableID := func() (tableID uint32) {
				r.QueryRow(t, fmt.Sprintf(`SELECT '%s'::regclass::oid`, tc.tableName)).Scan(&tableID)
				return tableID
			}
			tableID = getTableID()
			tableKey := ts.Codec().TablePrefix(tableID)

			grp := ctxgroup.WithContext(ctx)
			grp.GoCtx(func(ctx context.Context) error {
				// We are going to do this twice, first to cause a PTS related failure,
				// and a second time for the successful case. The first time we will cause
				// the GC to happen before the PTS is setup. The second time we will allow
				// the PTS to be installed and then cause the GC.
				for i := 0; i < 2; i++ {
					i := i
					if err := func() (retErr error) {
						<-backfillQueryWait
						t.Logf(
							"%s i=%d begin running operations in the middle of backfill",
							timeutil.Now().Format(time.RFC3339), i,
						)
						defer func() {
							t.Logf(
								"%s i=%d finished running operations in the middle of backfill. retErr: %v",
								timeutil.Now().Format(time.RFC3339), i, retErr)
							backfillQueryResume <- struct{}{}
						}()
						if _, err := db.ExecContext(ctx, "SET sql_safe_updates=off"); err != nil {
							return errors.Wrap(err, "failed to set sql_safe_updates")
						}
						deletedSoFar := 0
						for deletedSoFar < rowsDeletedPerIteration {
							const deleteBatchSize = 1000
							if _, err := db.ExecContext(ctx, fmt.Sprintf(
								"DELETE FROM %s LIMIT %d",
								tc.tableName, deleteBatchSize,
							)); err != nil {
								return errors.Wrapf(err, "failed to DELETE; deleted %d so far", deletedSoFar)
							}
							deletedSoFar += deleteBatchSize
						}
						if _, err := db.ExecContext(ctx, fmt.Sprintf(
							"INSERT INTO %s VALUES(9999999 + %d)",
							tc.tableName, i,
						)); err != nil {
							return errors.Wrap(err, "failed to INSERT")
						}
						if err := refreshTo(ctx, tableKey, ts.Clock().Now()); err != nil {
							return errors.Wrap(err, "failed to refresh in-memory PTS")
						}
						if err := refreshPTSCacheTo(ctx, ts.Clock().Now()); err != nil {
							return errors.Wrap(err, "failed to refresh PTS cache")
						}
						if _, err := db.ExecContext(ctx, fmt.Sprintf(`
SELECT crdb_internal.kv_enqueue_replica(range_id, 'mvccGC', true)
FROM (SELECT range_id FROM [SHOW RANGES FROM TABLE %s] ORDER BY start_key);`, tc.tableName)); err != nil {
							return errors.Wrap(err, "failed to enqueue replica for GC")
						}
						row := db.QueryRowContext(ctx, "SELECT count(*) FROM system.protected_ts_records WHERE meta_type='jobs'")
						var count int
						if err := row.Scan(&count); err != nil {
							return errors.Wrap(err, "failed to query protected_ts_records")
						}
						// First iteration is before the PTS is setup, so it will be 0. Second
						// iteration the PTS should be setup.
						expectedCount := i
						if count != expectedCount {
							return errors.AssertionFailedf("no protected timestamp was set up by the schema change job (expected %d, got : %d)", expectedCount, count)
						}
						return nil
					}(); err != nil {
						return err
					}
				}
				return nil
			})
			grp.GoCtx(func(ctx context.Context) (retErr error) {
				// Backfill with the PTS being not setup early enough, which will
				// lead to failure.
				defer func() {
					t.Logf("%s backfill goroutine finished. retErr: %v", timeutil.Now().Format(time.RFC3339), retErr)
				}()
				t.Logf("%s running backfill with PTS not setup early enough", timeutil.Now().Format(time.RFC3339))
				blockBackFillsForPTSFailure.Swap(true)
				_, err := db.ExecContext(ctx, tc.backfillSchemaChange)
				if err == nil || !testutils.IsError(err, "unable to retry backfill since fixed timestamp is before the GC timestamp") {
					if err == nil {
						return errors.AssertionFailedf("expected error was not hit")
					}
					return errors.NewAssertionErrorWithWrappedErrf(err, "expected error was not hit")
				}
				err = testutils.SucceedsSoonError(func() error {
					// Wait until schema change is fully rolled back.
					var status string
					err = db.QueryRowContext(ctx, fmt.Sprintf(
						"SELECT status FROM crdb_internal.jobs WHERE description LIKE '%s%%'",
						tc.jobDescriptionPrefix,
					)).Scan(&status)
					if err != nil {
						return errors.Wrap(err, "could not read jobs table")
					}
					if status != "failed" {
						return errors.Newf("schema change not rolled back yet; status=%s", status)
					}
					return nil
				})
				if err != nil {
					return err
				}
				// Next backfill with the PTS being setup on time, which should always
				// succeed.
				t.Logf("%s running backfill with PTS setup on time", timeutil.Now().Format(time.RFC3339))
				blockBackFillsForPTSCheck.Swap(true)
				_, err = db.ExecContext(ctx, tc.backfillSchemaChange)
				if err != nil {
					return errors.Wrap(err, "failed to run backfill")
				}
				return nil
			})

			require.NoError(t, grp.Wait())
			var rowCount int
			res := r.QueryRow(t, tc.postTestQuery)
			res.Scan(&rowCount)
			if rowCount != tc.expectedCount {
				t.Errorf("expected %d entries, got %d", tc.expectedCount, rowCount)
			}
			require.Falsef(t, blockBackFillsForPTSFailure.Load(), "no backfill txn was detected in testing knob.")
			require.Falsef(t, blockBackFillsForPTSCheck.Load(), "no backfill txn was detected in testing knob.")
		})
	}
}
