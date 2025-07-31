// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func insertTableStat(ctx context.Context, ex isql.Executor, stat *TableStatisticProto) error {
	insertStatStmt := `
INSERT INTO system.table_statistics ("tableID", "statisticID", name, "columnIDs", "createdAt",
	"rowCount", "distinctCount", "nullCount", "avgSize", histogram)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
`
	columnIDs := tree.NewDArray(types.Int)
	for _, id := range stat.ColumnIDs {
		if err := columnIDs.Append(tree.NewDInt(tree.DInt(int(id)))); err != nil {
			return err
		}
	}

	args := []interface{}{
		stat.TableID,
		stat.StatisticID,
		nil, // name
		columnIDs,
		stat.CreatedAt,
		stat.RowCount,
		stat.DistinctCount,
		stat.NullCount,
		stat.AvgSize,
		nil, // histogram
	}
	if len(stat.Name) != 0 {
		args[2] = stat.Name
	}
	if stat.HistogramData != nil {
		histogramBytes, err := protoutil.Marshal(stat.HistogramData)
		if err != nil {
			return err
		}
		args[9] = histogramBytes
	}

	var rows int
	rows, err := ex.Exec(ctx, "insert-stat", nil /* txn */, insertStatStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by stats insertion; expected exactly one row affected.", rows)
	}
	return nil

}

func lookupTableStats(
	ctx context.Context, sc *TableStatisticsCache, tableID descpb.ID,
) ([]*TableStatistic, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if e, ok := sc.mu.cache.Get(tableID); ok {
		return e.(*cacheEntry).stats, true
	}
	return nil, false
}

func checkStatsForTable(
	ctx context.Context,
	t *testing.T,
	sc *TableStatisticsCache,
	expected []*TableStatisticProto,
	tableID descpb.ID,
) {
	t.Helper()
	// Initially the stats won't be in the cache.
	if statsList, ok := lookupTableStats(ctx, sc, tableID); ok {
		t.Fatalf("lookup of missing key %d returned: %s", tableID, statsList)
	}

	// Perform the lookup and refresh, and confirm the
	// returned stats match the expected values.
	statsList, err := sc.getTableStatsFromCache(ctx, tableID, nil /* forecast */, nil /* udtCols */, nil /* typeResolver */)
	if err != nil {
		t.Fatalf("error retrieving stats: %s", err)
	}
	if !checkStats(statsList, expected) {
		t.Fatalf("for lookup of key %d, expected stats %s, got %s", tableID, expected, statsList)
	}

	// Now the stats should be in the cache.
	if _, ok := lookupTableStats(ctx, sc, tableID); !ok {
		t.Fatalf("for lookup of key %d, expected stats %s", tableID, expected)
	}
}

func checkStats(actual []*TableStatistic, expected []*TableStatisticProto) bool {
	if len(actual) == 0 && len(expected) == 0 {
		// DeepEqual differentiates between nil and empty slices, we don't.
		return true
	}
	var protoList []*TableStatisticProto
	for i := range actual {
		protoList = append(protoList, &actual[i].TableStatisticProto)
	}
	return reflect.DeepEqual(protoList, expected)
}

func initTestData(
	ctx context.Context, ex isql.Executor,
) (map[descpb.ID][]*TableStatisticProto, error) {
	// The expected stats must be ordered by TableID+, CreatedAt- so they can
	// later be compared with the returned stats using reflect.DeepEqual.
	expStatsList := []TableStatisticProto{
		{
			TableID:       descpb.ID(100),
			StatisticID:   0,
			Name:          "table0",
			ColumnIDs:     []descpb.ColumnID{1},
			CreatedAt:     time.Date(2010, 11, 20, 11, 35, 24, 0, time.UTC),
			RowCount:      32,
			DistinctCount: 30,
			NullCount:     0,
			AvgSize:       4,
			HistogramData: &HistogramData{ColumnType: types.Int, Buckets: []HistogramData_Bucket{
				{NumEq: 3, NumRange: 30, UpperBound: encoding.EncodeVarintAscending(nil, 3000)}},
			},
		},
		{
			TableID:       descpb.ID(100),
			StatisticID:   1,
			ColumnIDs:     []descpb.ColumnID{2, 3},
			CreatedAt:     time.Date(2010, 11, 20, 11, 35, 23, 0, time.UTC),
			RowCount:      32,
			DistinctCount: 5,
			NullCount:     5,
			AvgSize:       4,
		},
		{
			TableID:       descpb.ID(101),
			StatisticID:   0,
			ColumnIDs:     []descpb.ColumnID{0},
			CreatedAt:     time.Date(2017, 11, 20, 11, 35, 23, 0, time.UTC),
			RowCount:      320000,
			DistinctCount: 300000,
			NullCount:     100,
			AvgSize:       2,
		},
		{
			TableID:       descpb.ID(102),
			StatisticID:   34,
			Name:          "table2",
			ColumnIDs:     []descpb.ColumnID{1, 2, 3},
			CreatedAt:     time.Date(2001, 1, 10, 5, 25, 14, 0, time.UTC),
			RowCount:      0,
			DistinctCount: 0,
			NullCount:     0,
			AvgSize:       0,
		},
	}

	// Insert the stats into system.table_statistics
	// and store them in maps for fast retrieval.
	expectedStats := make(map[descpb.ID][]*TableStatisticProto)
	for i := range expStatsList {
		stat := &expStatsList[i]

		if err := insertTableStat(ctx, ex, stat); err != nil {
			return nil, err
		}

		expectedStats[stat.TableID] = append(expectedStats[stat.TableID], stat)
	}

	// Add another TableID for which we don't have stats.
	expectedStats[descpb.ID(103)] = nil

	return expectedStats, nil
}

func TestCacheBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	db := s.InternalDB().(descs.DB)
	expectedStats, err := initTestData(ctx, db.Executor())
	if err != nil {
		t.Fatal(err)
	}

	// Collect the tableIDs and sort them so we can iterate over them in a
	// consistent order (Go randomizes the order of iteration over maps).
	var tableIDs descpb.IDs
	for tableID := range expectedStats {
		tableIDs = append(tableIDs, tableID)
	}
	sort.Sort(tableIDs)

	// Create a cache and iteratively query the cache for each tableID. This
	// will result in the cache getting populated. When the stats cache size is
	// exceeded, entries should be evicted according to the LRU policy.
	sc := NewTableStatisticsCache(2 /* cacheSize */, s.ClusterSettings(), db, s.AppStopper())
	require.NoError(t, sc.Start(ctx, s.Codec(), s.RangeFeedFactory().(*rangefeed.Factory)))
	for _, tableID := range tableIDs {
		checkStatsForTable(ctx, t, sc, expectedStats[tableID], tableID)
	}

	tab0 := descpb.ID(100)
	tab1 := descpb.ID(101)
	tab2 := descpb.ID(102)
	tab3 := descpb.ID(103)

	// Table IDs 0 and 1 should have been evicted since the cache size is 2.
	tableIDs = []descpb.ID{tab0, tab1}
	for _, tableID := range tableIDs {
		if statsList, ok := lookupTableStats(ctx, sc, tableID); ok {
			t.Fatalf("lookup of evicted key %d returned: %s", tableID, statsList)
		}
	}

	// Table IDs 2 and 3 should still be in the cache.
	tableIDs = []descpb.ID{tab2, tab3}
	for _, tableID := range tableIDs {
		if _, ok := lookupTableStats(ctx, sc, tableID); !ok {
			t.Fatalf("for lookup of key %d, expected stats %s", tableID, expectedStats[tableID])
		}
	}

	// Insert a new stat for Table ID 2.
	stat := TableStatisticProto{
		TableID:       tab2,
		StatisticID:   35,
		Name:          "table2",
		ColumnIDs:     []descpb.ColumnID{1, 2, 3},
		CreatedAt:     time.Date(2001, 1, 10, 5, 26, 34, 0, time.UTC),
		RowCount:      10,
		DistinctCount: 10,
		NullCount:     0,
	}
	if err := insertTableStat(ctx, db.Executor(), &stat); err != nil {
		t.Fatal(err)
	}

	// Table ID 2 should be available immediately in the cache for querying, and
	// eventually should contain the updated stat.
	if _, ok := lookupTableStats(ctx, sc, tab2); !ok {
		t.Fatalf("expected lookup of refreshed key %d to succeed", tab2)
	}
	expected := append([]*TableStatisticProto{&stat}, expectedStats[tab2]...)
	testutils.SucceedsSoon(t, func() error {
		statsList, ok := lookupTableStats(ctx, sc, tab2)
		if !ok {
			return errors.Errorf("expected lookup of refreshed key %d to succeed", tab2)
		}
		if !checkStats(statsList, expected) {
			return errors.Errorf(
				"for lookup of key %d, expected stats %s but found %s", tab2, expected, statsList,
			)
		}
		return nil
	})

	// After invalidation Table ID 2 should be gone.
	sc.InvalidateTableStats(ctx, tab2)
	if statsList, ok := lookupTableStats(ctx, sc, tab2); ok {
		t.Fatalf("lookup of invalidated key %d returned: %s", tab2, statsList)
	}

	// Verify that Refresh doesn't count toward the "recently used" policy.
	checkStatsForTable(ctx, t, sc, expectedStats[tab0], tab0)
	checkStatsForTable(ctx, t, sc, expectedStats[tab1], tab1)

	// Sleep a bit to give the async refresh process a chance to do something.
	// Note that this is not flaky - the check below passes even if the refresh is
	// delayed.
	time.Sleep(time.Millisecond)

	checkStatsForTable(ctx, t, sc, expectedStats[tab3], tab3)
	// Verify that tab0 was evicted (despite the refreshes).
	if statsList, ok := lookupTableStats(ctx, sc, tab0); ok {
		t.Fatalf("lookup of evicted key %d returned: %s", tab0, statsList)
	}
}

func TestCacheUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	sqlRunner := sqlutils.MakeSQLRunner(sqlDB)

	sqlRunner.Exec(t, `CREATE DATABASE t;`)
	sqlRunner.Exec(t, `USE t;`)
	sqlRunner.Exec(t, `CREATE TYPE t AS ENUM ('hello');`)
	sqlRunner.Exec(t, `CREATE TABLE tt (x t PRIMARY KEY, y INT, INDEX(y));`)
	sqlRunner.Exec(t, `INSERT INTO tt VALUES ('hello');`)
	sqlRunner.Exec(t, `CREATE STATISTICS s FROM tt;`)

	insqlDB := s.InternalDB().(descs.DB)

	// Make a stats cache.
	sc := NewTableStatisticsCache(1, s.ClusterSettings(), insqlDB, s.AppStopper())
	require.NoError(t, sc.Start(ctx, s.Codec(), s.RangeFeedFactory().(*rangefeed.Factory)))
	tbl := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "t", "tt")
	// Get stats for our table. We are ensuring here that the access to the stats
	// for tt properly hydrates the user defined type t before access.
	stats, err := sc.GetTableStats(ctx, tbl, nil /* typeResolver */)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 {
		t.Errorf("expected two statistics (for x and y), got %d", len(stats))
	}

	// Drop the table and the type.
	sqlRunner.Exec(t, `DROP TABLE tt;`)
	sqlRunner.Exec(t, `DROP TYPE t;`)
	// Purge the cache.
	sc.InvalidateTableStats(ctx, tbl.GetID())
	// Verify that GetTableStats ignores the statistic on the now unknown type and
	// returns the rest.
	stats, err = sc.GetTableStats(ctx, tbl, nil /* typeResolver */)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 1 {
		t.Errorf("expected one statistic (for y), got %d", len(stats))
	}
}

// TestCacheWait verifies that when a table gets invalidated, we only retrieve
// the stats one time, even if there are multiple callers asking for them.
func TestCacheWait(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	db := s.InternalDB().(descs.DB)

	expectedStats, err := initTestData(ctx, db.Executor())
	if err != nil {
		t.Fatal(err)
	}

	// Collect the tableIDs and sort them so we can iterate over them in a
	// consistent order (Go randomizes the order of iteration over maps).
	var tableIDs descpb.IDs
	for tableID := range expectedStats {
		tableIDs = append(tableIDs, tableID)
	}
	sort.Sort(tableIDs)
	sc := NewTableStatisticsCache(len(tableIDs) /* cacheSize */, s.ClusterSettings(), db, s.AppStopper())
	require.NoError(t, sc.Start(ctx, s.Codec(), s.RangeFeedFactory().(*rangefeed.Factory)))
	for _, tableID := range tableIDs {
		checkStatsForTable(ctx, t, sc, expectedStats[tableID], tableID)
	}

	for run := 0; run < 10; run++ {
		before := sc.mu.numInternalQueries

		id := tableIDs[rand.Intn(len(tableIDs))]
		sc.InvalidateTableStats(ctx, id)
		// Run GetTableStats multiple times in parallel.
		var wg sync.WaitGroup
		for n := 0; n < 10; n++ {
			wg.Add(1)
			go func() {
				stats, err := sc.getTableStatsFromCache(ctx, id, nil /* forecast */, nil /* udtCols */, nil /* typeResolver */)
				if err != nil {
					t.Error(err)
				} else if !checkStats(stats, expectedStats[id]) {
					t.Errorf("for table %d, expected stats %s, got %s", id, expectedStats[id], stats)
				}
				wg.Done()
			}()
		}
		wg.Wait()

		if t.Failed() {
			return
		}

		// Verify that we only issued one read from the statistics table.
		if num := sc.mu.numInternalQueries - before; num != 1 {
			t.Fatalf("expected 1 query, got %d", num)
		}
	}
}

// TestCacheAutoRefresh verifies that the cache gets refreshed automatically
// when new statistics are added.
func TestCacheAutoRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	s := tc.ApplicationLayer(0)
	sc := NewTableStatisticsCache(
		10, /* cacheSize */
		s.ClusterSettings(),
		s.InternalDB().(descs.DB),
		s.AppStopper(),
	)
	require.NoError(t, sc.Start(ctx, s.Codec(), s.RangeFeedFactory().(*rangefeed.Factory)))

	sr0 := sqlutils.MakeSQLRunner(s.SQLConn(t))
	sr0.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
	sr0.Exec(t, "CREATE DATABASE test")
	sr0.Exec(t, "CREATE TABLE test.t (k INT PRIMARY KEY, v INT)")
	sr0.Exec(t, "INSERT INTO test.t VALUES (1, 1), (2, 2), (3, 3)")

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "test", "t")

	expectNStats := func(n int) error {
		stats, err := sc.GetTableStats(ctx, tableDesc, nil /* typeResolver */)
		if err != nil {
			t.Fatal(err)
		}
		if len(stats) != n {
			return fmt.Errorf("expected %d stats, got: %v", n, stats)
		}
		return nil
	}

	if err := expectNStats(0); err != nil {
		t.Fatal(err)
	}
	sr1 := sqlutils.MakeSQLRunner(tc.ApplicationLayer(1).SQLConn(t))
	sr1.Exec(t, "CREATE STATISTICS k ON k FROM test.t")

	testutils.SucceedsSoon(t, func() error {
		return expectNStats(1)
	})

	sr2 := sqlutils.MakeSQLRunner(tc.ApplicationLayer(2).SQLConn(t))
	sr2.Exec(t, "CREATE STATISTICS v ON v FROM test.t")

	testutils.SucceedsSoon(t, func() error {
		return expectNStats(2)
	})
}
