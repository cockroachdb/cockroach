// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestMaybeRefreshStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)
	AutomaticStatisticsMinStaleRows.Override(ctx, &st.SV, 5)

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	sqlRun.Exec(t,
		`CREATE DATABASE t;
		CREATE TABLE t.a (k INT PRIMARY KEY);
		INSERT INTO t.a VALUES (1);
		CREATE VIEW t.vw AS SELECT k, k+1 FROM t.a;`)

	executor := s.InternalExecutor().(sqlutil.InternalExecutor)
	descA := catalogkv.TestingGetTableDescriptor(s.DB(), keys.SystemSQLCodec, "t", "a")
	cache := NewTableStatisticsCache(
		ctx,
		10, /* cacheSize */
		kvDB,
		executor,
		keys.SystemSQLCodec,
		s.LeaseManager().(*lease.Manager),
		s.ClusterSettings(),
		s.RangeFeedFactory().(*rangefeed.Factory),
	)
	refresher := MakeRefresher(st, executor, cache, time.Microsecond /* asOfTime */)

	// There should not be any stats yet.
	if err := checkStatsCount(ctx, cache, descA.GetID(), 0 /* expected */); err != nil {
		t.Fatal(err)
	}

	// There are no stats yet, so this must refresh the statistics on table t
	// even though rowsAffected=0.
	refresher.maybeRefreshStats(
		ctx, s.Stopper(), descA.GetID(), 0 /* rowsAffected */, time.Microsecond, /* asOf */
	)
	if err := checkStatsCount(ctx, cache, descA.GetID(), 1 /* expected */); err != nil {
		t.Fatal(err)
	}

	// Try to refresh again. With rowsAffected=0, the probability of a refresh
	// is 0, so refreshing will not succeed.
	refresher.maybeRefreshStats(
		ctx, s.Stopper(), descA.GetID(), 0 /* rowsAffected */, time.Microsecond, /* asOf */
	)
	if err := checkStatsCount(ctx, cache, descA.GetID(), 1 /* expected */); err != nil {
		t.Fatal(err)
	}

	// With rowsAffected=10, refreshing should work. Since there are more rows
	// updated than exist in the table, the probability of a refresh is 100%.
	refresher.maybeRefreshStats(
		ctx, s.Stopper(), descA.GetID(), 10 /* rowsAffected */, time.Microsecond, /* asOf */
	)
	if err := checkStatsCount(ctx, cache, descA.GetID(), 2 /* expected */); err != nil {
		t.Fatal(err)
	}

	// Ensure that attempt to refresh stats on view does not result in re-
	// enqueuing the attempt.
	// TODO(rytaft): Should not enqueue views to begin with.
	descVW := catalogkv.TestingGetTableDescriptor(s.DB(), keys.SystemSQLCodec, "t", "vw")
	refresher.maybeRefreshStats(
		ctx, s.Stopper(), descVW.GetID(), 0 /* rowsAffected */, time.Microsecond, /* asOf */
	)
	select {
	case <-refresher.mutations:
		t.Fatal("refresher should not re-enqueue attempt to create stats over view")
	default:
	}
}

func TestAverageRefreshTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	sqlRun.Exec(t,
		`CREATE DATABASE t;
		CREATE TABLE t.a (k INT PRIMARY KEY);
		INSERT INTO t.a VALUES (1);`)

	executor := s.InternalExecutor().(sqlutil.InternalExecutor)
	tableID := catalogkv.TestingGetTableDescriptor(s.DB(), keys.SystemSQLCodec, "t", "a").GetID()
	cache := NewTableStatisticsCache(
		ctx,
		10, /* cacheSize */
		kvDB,
		executor,
		keys.SystemSQLCodec,
		s.LeaseManager().(*lease.Manager),
		s.ClusterSettings(),
		s.RangeFeedFactory().(*rangefeed.Factory),
	)
	refresher := MakeRefresher(st, executor, cache, time.Microsecond /* asOfTime */)

	// curTime is used as the current time throughout the test to ensure that the
	// calculated average refresh time is consistent even if there are delays due
	// to running the test under race.
	curTime := timeutil.Now()

	checkAverageRefreshTime := func(expected time.Duration) error {
		return testutils.SucceedsSoonError(func() error {
			stats, err := cache.GetTableStats(ctx, tableID)
			if err != nil {
				return err
			}
			if actual := avgRefreshTime(stats).Round(time.Minute); actual != expected {
				return fmt.Errorf("expected avgRefreshTime %s but found %s",
					expected.String(), actual.String())
			}
			return nil
		})
	}

	// Checks that the most recent statistic was created less than (greater than)
	// expectedAge time ago if lessThan is true (false).
	checkMostRecentStat := func(expectedAge time.Duration, lessThan bool) error {
		return testutils.SucceedsSoonError(func() error {
			stats, err := cache.GetTableStats(ctx, tableID)
			if err != nil {
				return err
			}
			stat := mostRecentAutomaticStat(stats)
			if stat == nil {
				return fmt.Errorf("no recent automatic statistic found")
			}
			if !lessThan && stat.CreatedAt.After(curTime.Add(-1*expectedAge)) {
				return fmt.Errorf("most recent stat is less than %s old. Created at: %s Current time: %s",
					expectedAge, stat.CreatedAt, curTime,
				)
			}
			if lessThan && stat.CreatedAt.Before(curTime.Add(-1*expectedAge)) {
				return fmt.Errorf("most recent stat is more than %s old. Created at: %s Current time: %s",
					expectedAge, stat.CreatedAt, curTime,
				)
			}
			return nil
		})
	}

	// Since there are no stats yet, avgRefreshTime should return the default
	// value.
	if err := checkAverageRefreshTime(defaultAverageTimeBetweenRefreshes); err != nil {
		t.Fatal(err)
	}

	insertStat := func(
		txn *kv.Txn, name string, columnIDs *tree.DArray, createdAt *tree.DTimestamp,
	) error {
		_, err := executor.Exec(
			ctx, "insert-statistic", txn,
			`INSERT INTO system.table_statistics (
					  "tableID",
					  "name",
					  "columnIDs",
					  "createdAt",
					  "rowCount",
					  "distinctCount",
					  "nullCount"
				  ) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			tableID,
			name,
			columnIDs,
			createdAt,
			1, /* rowCount */
			1, /* distinctCount */
			0, /* nullCount */
		)
		return err
	}

	// Add some stats on column k in table a with a name different from
	// AutoStatsName, separated by three hours each, starting 7 hours ago.
	if err := s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		for i := 0; i < 10; i++ {
			columnIDsVal := tree.NewDArray(types.Int)
			if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(1))); err != nil {
				return err
			}
			createdAt, err := tree.MakeDTimestamp(
				curTime.Add(time.Duration(-1*(i*3+7))*time.Hour), time.Hour,
			)
			if err != nil {
				return err
			}
			name := fmt.Sprintf("stat%d", i)
			if err := insertStat(txn, name, columnIDsVal, createdAt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := checkStatsCount(ctx, cache, tableID, 10 /* expected */); err != nil {
		t.Fatal(err)
	}

	// None of the stats have the name AutoStatsName, so avgRefreshTime
	// should still return the default value.
	if err := checkAverageRefreshTime(defaultAverageTimeBetweenRefreshes); err != nil {
		t.Fatal(err)
	}

	// Add some stats on column v in table a with name AutoStatsName, separated
	// by four hours each, starting 6 hours ago.
	if err := s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		for i := 0; i < 10; i++ {
			columnIDsVal := tree.NewDArray(types.Int)
			if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(2))); err != nil {
				return err
			}
			createdAt, err := tree.MakeDTimestamp(
				curTime.Add(time.Duration(-1*(i*4+6))*time.Hour), time.Hour,
			)
			if err != nil {
				return err
			}
			if err := insertStat(txn, jobspb.AutoStatsName, columnIDsVal, createdAt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := checkStatsCount(ctx, cache, tableID, 20 /* expected */); err != nil {
		t.Fatal(err)
	}

	// Check that the calculated average refresh time is four hours. Even though
	// the average time between all stats just added is less than four hours, we
	// should only calculate the average based on stats with the name __auto__,
	// and only on the automatic column statistic that was most recently updated
	// (in this case, column v, 6 hours ago).
	if err := checkAverageRefreshTime(4 * time.Hour); err != nil {
		t.Fatal(err)
	}

	// Check that the most recent stat is less than 8 hours old.
	if err := checkMostRecentStat(8*time.Hour, true /* lessThan */); err != nil {
		t.Fatal(err)
	}

	// The most recent stat is less than 8 hours old, which is less than 2x the
	// average time between refreshes, so this call is not required to refresh
	// the statistics on table t. With rowsAffected=0, the probability of refresh
	// is 0.
	refresher.maybeRefreshStats(
		ctx, s.Stopper(), tableID, 0 /* rowsAffected */, time.Microsecond, /* asOf */
	)
	if err := checkStatsCount(ctx, cache, tableID, 20 /* expected */); err != nil {
		t.Fatal(err)
	}

	// Add some stats on column k in table a with name AutoStatsName, separated
	// by 1.5 hours each, starting 5 hours ago.
	if err := s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		for i := 0; i < 10; i++ {
			columnIDsVal := tree.NewDArray(types.Int)
			if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(1))); err != nil {
				return err
			}
			createdAt, err := tree.MakeDTimestamp(
				curTime.Add(time.Duration(-1*(i*90+300))*time.Minute), time.Minute,
			)
			if err != nil {
				return err
			}
			if err := insertStat(txn, jobspb.AutoStatsName, columnIDsVal, createdAt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := checkStatsCount(ctx, cache, tableID, 30 /* expected */); err != nil {
		t.Fatal(err)
	}

	// Check that the calculated average refresh time is 1.5 hours, based on the
	// automatic column statistic that was most recently updated (in this case,
	// column k, 5 hours ago).
	if err := checkAverageRefreshTime(90 * time.Minute); err != nil {
		t.Fatal(err)
	}

	// Check that the most recent stat is over 4 hours old.
	if err := checkMostRecentStat(4*time.Hour, false /* lessThan */); err != nil {
		t.Fatal(err)
	}

	// The most recent stat is over 4 hours old, which is more than 2x the
	// average time between refreshes, so this call must refresh the statistics
	// on table t even though rowsAffected=0. After refresh, only 15 stats should
	// remain (5 from column k and 10 from column v), since the old stats on k
	// were deleted.
	refresher.maybeRefreshStats(
		ctx, s.Stopper(), tableID, 0 /* rowsAffected */, time.Microsecond, /* asOf */
	)
	if err := checkStatsCount(ctx, cache, tableID, 15 /* expected */); err != nil {
		t.Fatal(err)
	}
}

func TestAutoStatsReadOnlyTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	sqlRun.Exec(t,
		`CREATE DATABASE t;
		CREATE TABLE t.a (k INT PRIMARY KEY);`)

	// Test that stats for tables in user-defined schemas are also refreshed.
	sqlRun.Exec(t,
		`CREATE SCHEMA my_schema;
		CREATE TABLE my_schema.b (j INT PRIMARY KEY);`)

	executor := s.InternalExecutor().(sqlutil.InternalExecutor)
	cache := NewTableStatisticsCache(
		ctx,
		10, /* cacheSize */
		kvDB,
		executor,
		keys.SystemSQLCodec,
		s.LeaseManager().(*lease.Manager),
		s.ClusterSettings(),
		s.RangeFeedFactory().(*rangefeed.Factory),
	)
	refresher := MakeRefresher(st, executor, cache, time.Microsecond /* asOfTime */)

	AutomaticStatisticsClusterMode.Override(ctx, &st.SV, true)

	if err := refresher.Start(
		ctx, s.Stopper(), time.Millisecond, /* refreshInterval */
	); err != nil {
		t.Fatal(err)
	}

	// There should be one stat for table t.a.
	sqlRun.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE t.a]`,
		[][]string{
			{"__auto__", "{k}", "0"},
		})

	// There should be one stat for table my_schema.b.
	sqlRun.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count FROM [SHOW STATISTICS FOR TABLE my_schema.b]`,
		[][]string{
			{"__auto__", "{j}", "0"},
		})
}

func TestNoRetryOnFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	executor := s.InternalExecutor().(sqlutil.InternalExecutor)
	cache := NewTableStatisticsCache(
		ctx,
		10, /* cacheSize */
		kvDB,
		executor,
		keys.SystemSQLCodec,
		s.LeaseManager().(*lease.Manager),
		s.ClusterSettings(),
		s.RangeFeedFactory().(*rangefeed.Factory),
	)
	r := MakeRefresher(st, executor, cache, time.Microsecond /* asOfTime */)

	// Try to refresh stats on a table that doesn't exist.
	r.maybeRefreshStats(
		ctx, s.Stopper(), 100 /* tableID */, math.MaxInt32, time.Microsecond, /* asOfTime */
	)

	// Ensure that we will not try to refresh tableID 100 again.
	if expected, actual := 0, len(r.mutations); expected != actual {
		t.Fatalf("expected channel size %d but found %d", expected, actual)
	}
}

func TestMutationsChannel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	AutomaticStatisticsClusterMode.Override(ctx, &st.SV, true)
	r := Refresher{
		st:        st,
		mutations: make(chan mutation, refreshChanBufferLen),
	}

	// Test that the mutations channel doesn't block even when we add 10 more
	// items than can fit in the buffer.
	for i := 0; i < refreshChanBufferLen+10; i++ {
		r.NotifyMutation(descpb.ID(53), 5 /* rowsAffected */)
	}

	if expected, actual := refreshChanBufferLen, len(r.mutations); expected != actual {
		t.Fatalf("expected channel size %d but found %d", expected, actual)
	}
}

func TestDefaultColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	AutomaticStatisticsClusterMode.Override(ctx, &st.SV, false)
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	sqlRun.Exec(t,
		`CREATE DATABASE t;
		CREATE TABLE t.a (c0 INT PRIMARY KEY);`)

	for i := 1; i < 110; i++ {
		// Add more columns than we will collect stats on.
		sqlRun.Exec(t,
			fmt.Sprintf("ALTER TABLE t.a ADD COLUMN c%d INT", i))
	}

	sqlRun.Exec(t, `CREATE STATISTICS s FROM t.a`)

	// There should be 101 stats. One for the primary index, plus 100 other
	// columns.
	sqlRun.CheckQueryResults(t,
		`SELECT count(*) FROM [SHOW STATISTICS FOR TABLE t.a] WHERE statistics_name = 's'`,
		[][]string{
			{"101"},
		})
}

func checkStatsCount(
	ctx context.Context, cache *TableStatisticsCache, tableID descpb.ID, expected int,
) error {
	return testutils.SucceedsSoonError(func() error {
		stats, err := cache.GetTableStats(ctx, tableID)
		if err != nil {
			return err
		}
		if len(stats) != expected {
			return fmt.Errorf("expected %d stat(s) but found %d", expected, len(stats))
		}
		return nil
	})
}
