// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/assert"
)

type initFetcherArgs struct {
	tableDesc catalog.TableDescriptor
	indexIdx  int
	columns   []int
}

func makeIndexFetchSpec(t *testing.T, entry initFetcherArgs) descpb.IndexFetchSpec {
	index := entry.tableDesc.ActiveIndexes()[entry.indexIdx]
	colIDs := entry.tableDesc.PublicColumnIDs()
	if entry.columns != nil {
		allColIDs := colIDs
		colIDs = nil
		for _, ord := range entry.columns {
			colIDs = append(colIDs, allColIDs[ord])
		}
	}
	var spec descpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(&spec, keys.SystemSQLCodec, entry.tableDesc, index, colIDs); err != nil {
		t.Fatal(err)
	}
	return spec
}

func initFetcher(
	t *testing.T,
	entry initFetcherArgs,
	reverseScan bool,
	alloc *tree.DatumAlloc,
	memMon *mon.BytesMonitor,
) *Fetcher {
	fetcher := &Fetcher{}

	spec := makeIndexFetchSpec(t, entry)

	if err := fetcher.Init(
		context.Background(),
		reverseScan,
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		0, /* lockTimeout */
		alloc,
		memMon,
		&spec,
	); err != nil {
		t.Fatal(err)
	}

	return fetcher
}

type fetcherEntryArgs struct {
	modFactor int // Useful modulo to apply for value columns
	schema    string
	nRows     int
	nCols     int // Number of columns in the table
	nVals     int // Number of values requested from scan
	genValue  sqlutils.GenRowFn
}

func TestNextRowSingle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tables := map[string]fetcherEntryArgs{
		"t1": {
			modFactor: 42,
			nRows:     1337,
			nCols:     2,
		},
		"t2": {
			modFactor: 13,
			nRows:     2014,
			nCols:     2,
		},
		"norows": {
			modFactor: 10,
			nRows:     0,
			nCols:     2,
		},
		"onerow": {
			modFactor: 10,
			nRows:     1,
			nCols:     2,
		},
	}

	// Initialize tables first.
	for tableName, table := range tables {
		sqlutils.CreateTable(
			t, sqlDB, tableName,
			"k INT PRIMARY KEY, v INT",
			table.nRows,
			sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(table.modFactor)),
		)
	}

	alloc := &tree.DatumAlloc{}

	// We try to read rows from each table.
	for tableName, table := range tables {
		t.Run(tableName, func(t *testing.T) {
			tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

			args := initFetcherArgs{
				tableDesc: tableDesc,
				indexIdx:  0,
			}

			rf := initFetcher(t, args, false /*reverseScan*/, alloc, nil /* memMon */)

			if err := rf.StartScan(
				context.Background(),
				kv.NewTxn(ctx, kvDB, 0),
				roachpb.Spans{tableDesc.IndexSpan(keys.SystemSQLCodec, tableDesc.GetPrimaryIndexID())},
				rowinfra.NoBytesLimit,
				rowinfra.NoRowLimit,
				false, /*traceKV*/
				false, /*forceProductionKVBatchSize*/
			); err != nil {
				t.Fatal(err)
			}

			count := 0

			expectedVals := [2]int64{1, 1}
			for {
				datums, err := rf.NextRowDecoded(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if datums == nil {
					break
				}

				count++

				if table.nCols != len(datums) {
					t.Fatalf("expected %d columns, got %d columns", table.nCols, len(datums))
				}

				for i, expected := range expectedVals {
					actual := int64(*datums[i].(*tree.DInt))
					if expected != actual {
						t.Fatalf("unexpected value for row %d, col %d.\nexpected: %d\nactual: %d", count, i, expected, actual)
					}
				}

				expectedVals[0]++
				expectedVals[1]++
				// Value column is in terms of a modulo.
				expectedVals[1] %= int64(table.modFactor)
			}

			if table.nRows != count {
				t.Fatalf("expected %d rows, got %d rows", table.nRows, count)
			}
		})
	}
}

func TestNextRowBatchLimiting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tables := map[string]fetcherEntryArgs{
		"t1": {
			modFactor: 42,
			nRows:     1337,
			nCols:     2,
		},
		"t2": {
			modFactor: 13,
			nRows:     2014,
			nCols:     2,
		},
		"norows": {
			modFactor: 10,
			nRows:     0,
			nCols:     2,
		},
		"onerow": {
			modFactor: 10,
			nRows:     1,
			nCols:     2,
		},
	}

	// Initialize tables first.
	for tableName, table := range tables {
		sqlutils.CreateTable(
			t, sqlDB, tableName,
			"k INT PRIMARY KEY, v INT, FAMILY f1 (k), FAMILY f2(v)",
			table.nRows,
			sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(table.modFactor)),
		)
	}

	alloc := &tree.DatumAlloc{}

	// We try to read rows from each table.
	for tableName, table := range tables {
		t.Run(tableName, func(t *testing.T) {
			tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

			args := initFetcherArgs{
				tableDesc: tableDesc,
				indexIdx:  0,
			}

			rf := initFetcher(t, args, false /*reverseScan*/, alloc, nil /*memMon*/)

			if err := rf.StartScan(
				context.Background(),
				kv.NewTxn(ctx, kvDB, 0),
				roachpb.Spans{tableDesc.IndexSpan(keys.SystemSQLCodec, tableDesc.GetPrimaryIndexID())},
				rowinfra.DefaultBatchBytesLimit,
				10,    /*limitHint*/
				false, /*traceKV*/
				false, /*forceProductionKVBatchSize*/
			); err != nil {
				t.Fatal(err)
			}

			count := 0

			expectedVals := [2]int64{1, 1}
			for {
				datums, err := rf.NextRowDecoded(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if datums == nil {
					break
				}

				count++

				if table.nCols != len(datums) {
					t.Fatalf("expected %d columns, got %d columns", table.nCols, len(datums))
				}

				for i, expected := range expectedVals {
					actual := int64(*datums[i].(*tree.DInt))
					if expected != actual {
						t.Fatalf("unexpected value for row %d, col %d.\nexpected: %d\nactual: %d", count, i, expected, actual)
					}
				}

				expectedVals[0]++
				expectedVals[1]++
				// Value column is in terms of a modulo.
				expectedVals[1] %= int64(table.modFactor)
			}

			if table.nRows != count {
				t.Fatalf("expected %d rows, got %d rows", table.nRows, count)
			}
		})
	}
}

func TestRowFetcherMemoryLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	oneMegString := make([]byte, 1<<20)
	for i := range oneMegString {
		oneMegString[i] = '!'
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tableName := "wide_table"
	sqlutils.CreateTable(
		t, sqlDB, tableName,
		"k INT PRIMARY KEY, v STRING",
		10,
		func(i int) []tree.Datum {
			return []tree.Datum{
				tree.NewDInt(tree.DInt(i)),
				tree.NewDString(string(oneMegString)),
			}
		})

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

	args := initFetcherArgs{
		tableDesc: tableDesc,
		indexIdx:  0,
	}

	alloc := &tree.DatumAlloc{}

	settings := cluster.MakeTestingClusterSettings()

	// Give a 1 megabyte limit to the memory monitor, so that
	// we can test whether scans of wide tables are prevented if
	// we have insufficient memory to do them.
	memMon := mon.NewMonitor("test", mon.MemoryResource, nil, nil, -1, 1000, settings)
	memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1<<20))
	defer memMon.Stop(ctx)
	rf := initFetcher(t, args, false /*reverseScan*/, alloc, memMon)
	defer rf.Close(ctx)

	err := rf.StartScan(
		context.Background(),
		kv.NewTxn(ctx, kvDB, 0),
		roachpb.Spans{tableDesc.IndexSpan(keys.SystemSQLCodec, tableDesc.GetPrimaryIndexID())},
		rowinfra.NoBytesLimit,
		rowinfra.NoRowLimit,
		false, /*traceKV*/
		false, /*forceProductionKVBatchSize*/
	)
	assert.Error(t, err)
	assert.Equal(t, pgerror.GetPGCode(err), pgcode.OutOfMemory)
}

// Regression test for #29374. Ensure that RowFetcher can handle multi-span
// fetches where individual batches end in the middle of a multi-column family
// row with not-null columns.
func TestNextRowPartialColumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tableName := "t1"
	table := fetcherEntryArgs{
		modFactor: 42,
		nRows:     2,
		nCols:     4,
	}

	// Initialize a table with multiple column families with some not null ones.
	// We'll insert rows that contain nulls for the nullable column families, to
	// trick the rowfetcher heuristic that multiplies the input batch size by the
	// number of columns in the table.
	sqlutils.CreateTable(
		t, sqlDB, tableName,
		`
k INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, c INT NULL,
FAMILY f1 (k), FAMILY f2(a), FAMILY f3(b), FAMILY f4(c),
INDEX(c)
`,
		table.nRows,
		sqlutils.ToRowFn(sqlutils.RowIdxFn,
			sqlutils.RowModuloFn(table.modFactor),
			sqlutils.RowModuloFn(table.modFactor),
		),
	)

	alloc := &tree.DatumAlloc{}

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

	args := initFetcherArgs{
		tableDesc: tableDesc,
		indexIdx:  0,
	}

	rf := initFetcher(t, args, false /*reverseScan*/, alloc, nil /*memMon*/)

	// Start a scan that has multiple input spans, to tickle the codepath that
	// sees an "empty batch". When we have multiple input spans, the kv server
	// will always return one response per input span. Make sure that the
	// empty response that will be produced in the case where the first span
	// does not end before the limit is satisfied doesn't cause the rowfetcher
	// to think that a row has ended, and therefore have issues when it sees
	// the next kvs from that row in isolation in the next batch.

	// We'll make the first span go to some random key in the middle of the
	// key space (by appending a number to the index's start key) and the
	// second span go from that key to the end of the index.
	indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, tableDesc.GetPrimaryIndexID())
	endKey := indexSpan.EndKey
	midKey := encoding.EncodeUvarintAscending(indexSpan.Key, uint64(100))
	indexSpan.EndKey = midKey

	if err := rf.StartScan(
		context.Background(),
		kv.NewTxn(ctx, kvDB, 0),
		roachpb.Spans{indexSpan,
			roachpb.Span{Key: midKey, EndKey: endKey},
		},
		rowinfra.DefaultBatchBytesLimit,
		// Set a limitHint of 1 to more quickly end the first batch, causing a
		// batch that ends between rows.
		1,     /*limitHint*/
		false, /*traceKV*/
		false, /*forceProductionKVBatchSize*/
	); err != nil {
		t.Fatal(err)
	}

	var count int
	for {
		// Just try to grab the row - we don't need to validate the contents
		// in this test.
		datums, err := rf.NextRowDecoded(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if datums == nil {
			break
		}
		count++
	}

	if table.nRows != count {
		t.Fatalf("expected %d rows, got %d rows", table.nRows, count)
	}
}

// Secondary indexes contain extra values (the primary key of the primary index
// as well as STORING columns).
func TestNextRowSecondaryIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Modulo to use for s1, s2 storing columns.
	storingMods := [2]int{7, 13}
	// Number of NULL secondary index values.
	nNulls := 20

	tables := map[string]*fetcherEntryArgs{
		"nonunique": {
			modFactor: 20,
			schema:    "p INT PRIMARY KEY, idx INT, s1 INT, s2 INT, INDEX i1 (idx)",
			nRows:     422,
			nCols:     4,
			nVals:     2,
		},
		"unique": {
			// Must be > nRows since this value must be unique.
			modFactor: 1000,
			schema:    "p INT PRIMARY KEY, idx INT, s1 INT, s2 INT, UNIQUE INDEX i1 (idx)",
			nRows:     123,
			nCols:     4,
			nVals:     2,
		},
		"nonuniquestoring": {
			modFactor: 42,
			schema:    "p INT PRIMARY KEY, idx INT, s1 INT, s2 INT, INDEX i1 (idx) STORING (s1, s2)",
			nRows:     654,
			nCols:     4,
			nVals:     4,
		},
		"uniquestoring": {
			// Must be > nRows since this value must be unique.
			modFactor: 1000,
			nRows:     555,
			schema:    "p INT PRIMARY KEY, idx INT, s1 INT, s2 INT, UNIQUE INDEX i1 (idx) STORING (s1, s2)",
			nCols:     4,
			nVals:     4,
		},
	}

	// Initialize the generate value functions.
	tables["nonunique"].genValue = sqlutils.ToRowFn(
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tables["nonunique"].modFactor),
	)
	tables["unique"].genValue = sqlutils.ToRowFn(
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tables["unique"].modFactor),
	)
	tables["nonuniquestoring"].genValue = sqlutils.ToRowFn(
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tables["nonuniquestoring"].modFactor),
		sqlutils.RowModuloFn(storingMods[0]),
		sqlutils.RowModuloFn(storingMods[1]),
	)
	tables["uniquestoring"].genValue = sqlutils.ToRowFn(
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tables["uniquestoring"].modFactor),
		sqlutils.RowModuloFn(storingMods[0]),
		sqlutils.RowModuloFn(storingMods[1]),
	)

	// Add family definitions to each table.
	tablesWithFamilies := make(map[string]*fetcherEntryArgs)
	for tableName, table := range tables {
		argCopy := *table
		argCopy.schema = argCopy.schema + ", FAMILY (p), FAMILY (idx), FAMILY (s1), FAMILY (s2)"
		familyName := tableName + "_with_families"
		tablesWithFamilies[familyName] = &argCopy
	}
	for tableName, args := range tablesWithFamilies {
		tables[tableName] = args
	}

	r := sqlutils.MakeSQLRunner(sqlDB)
	// Initialize tables first.
	for tableName, table := range tables {
		sqlutils.CreateTable(
			t, sqlDB, tableName,
			table.schema,
			table.nRows,
			table.genValue,
		)

		// Insert nNulls NULL secondary index values (this tests if
		// we're properly decoding (UNIQUE) secondary index keys
		// properly).
		for i := 1; i <= nNulls; i++ {
			r.Exec(t, fmt.Sprintf(
				`INSERT INTO %s.%s VALUES (%d, NULL, %d, %d);`,
				sqlutils.TestDB,
				tableName,
				table.nRows+i,
				(table.nRows+i)%storingMods[0],
				(table.nRows+i)%storingMods[1],
			))
		}
		table.nRows += nNulls
	}

	alloc := &tree.DatumAlloc{}
	// We try to read rows from each index.
	for tableName, table := range tables {
		t.Run(tableName, func(t *testing.T) {
			tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

			args := initFetcherArgs{
				tableDesc: tableDesc,
				// We scan from the first secondary index.
				indexIdx: 1,
				columns:  []int{0, 1},
			}
			if table.nVals == 4 {
				args.columns = []int{0, 1, 2, 3}
			}

			rf := initFetcher(t, args, false /*reverseScan*/, alloc, nil /*memMon*/)

			if err := rf.StartScan(
				context.Background(),
				kv.NewTxn(ctx, kvDB, 0),
				roachpb.Spans{tableDesc.IndexSpan(keys.SystemSQLCodec, tableDesc.PublicNonPrimaryIndexes()[0].GetID())},
				rowinfra.NoBytesLimit,
				rowinfra.NoRowLimit,
				false, /*traceKV*/
				false, /*forceProductionKVBatchSize*/
			); err != nil {
				t.Fatal(err)
			}

			count := 0
			nullCount := 0
			var prevIdxVal int64
			for {
				datums, err := rf.NextRowDecoded(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if datums == nil {
					break
				}

				count++

				if len(args.columns) != len(datums) {
					t.Fatalf("expected %d columns, got %d columns", len(args.columns), len(datums))
				}

				// Verify that the correct # of values are returned.
				numVals := 0
				for _, datum := range datums {
					if datum != tree.DNull {
						numVals++
					}
				}

				// Some secondary index values can be NULL. We keep track
				// of how many we encounter.
				idxNull := datums[1] == tree.DNull
				if idxNull {
					nullCount++
					// It is okay to bump this up by one since we know
					// this index value is suppose to be NULL.
					numVals++
				}

				if table.nVals != numVals {
					t.Fatalf("expected %d non-NULL values, got %d", table.nVals, numVals)
				}

				id := int64(*datums[0].(*tree.DInt))
				// Verify the value in the value column is
				// correct (if it is not NULL).
				if !idxNull {
					idx := int64(*datums[1].(*tree.DInt))
					if id%int64(table.modFactor) != idx {
						t.Fatalf("for row id %d, expected %d value, got %d", id, id%int64(table.modFactor), idx)
					}

					// Index values must be fetched in
					// non-decreasing order.
					if prevIdxVal > idx {
						t.Fatalf("index value unexpectedly decreased from %d to %d", prevIdxVal, idx)
					}
					prevIdxVal = idx
				}

				// We verify that the storing values are
				// decoded correctly.
				if tableName == "nonuniquestoring" || tableName == "uniquestoring" {
					s1 := int64(*datums[2].(*tree.DInt))
					s2 := int64(*datums[3].(*tree.DInt))

					if id%int64(storingMods[0]) != s1 {
						t.Fatalf("for row id %d, expected %d for s1 value, got %d", id, id%int64(storingMods[0]), s1)
					}
					if id%int64(storingMods[1]) != s2 {
						t.Fatalf("for row id %d, expected %d for s2 value, got %d", id, id%int64(storingMods[1]), s2)
					}
				}
			}

			if table.nRows != count {
				t.Fatalf("expected %d rows, got %d rows", table.nRows, count)
			}
		})
	}
}

func TestRowFetcherReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlutils.CreateTable(
		t, sqlDB, "foo",
		"k INT PRIMARY KEY, v INT",
		0,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(1)),
	)

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "foo")

	args := initFetcherArgs{
		tableDesc: tableDesc,
		indexIdx:  0,
	}
	da := tree.DatumAlloc{}
	fetcher := initFetcher(t, args, false, &da, nil /*memMon*/)

	resetFetcher := initFetcher(t, args, false /*reverseScan*/, &da, nil /*memMon*/)

	resetFetcher.Reset()

	// Now re-init the reset fetcher and make sure its the same as the fetcher we
	// didn't reset.

	spec := makeIndexFetchSpec(t, args)
	if err := resetFetcher.Init(
		ctx,
		false, /*reverse*/
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		0, /* lockTimeout */
		&da,
		nil, /* memMonitor */
		&spec,
	); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(resetFetcher, fetcher) {
		t.Fatal("unequal before and after reset", resetFetcher, fetcher)
	}

}

func TestFetcherUninitialized(t *testing.T) {
	// Regression test for #39013: make sure it's okay to call GetBytesReader even
	// before the fetcher was fully initialized.
	var fetcher Fetcher
	assert.Zero(t, fetcher.GetBytesRead())
}
