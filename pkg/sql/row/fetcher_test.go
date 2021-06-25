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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/assert"
)

type initFetcherArgs struct {
	tableDesc       catalog.TableDescriptor
	indexIdx        int
	valNeededForCol util.FastIntSet
	spans           roachpb.Spans
}

func makeFetcherArgs(entries []initFetcherArgs) []FetcherTableArgs {
	fetcherArgs := make([]FetcherTableArgs, len(entries))

	for i, entry := range entries {
		index := entry.tableDesc.ActiveIndexes()[entry.indexIdx]
		fetcherArgs[i] = FetcherTableArgs{
			Spans:            entry.spans,
			Desc:             entry.tableDesc,
			Index:            index,
			ColIdxMap:        catalog.ColumnIDToOrdinalMap(entry.tableDesc.PublicColumns()),
			IsSecondaryIndex: !index.Primary(),
			Cols:             entry.tableDesc.PublicColumns(),
			ValNeededForCol:  entry.valNeededForCol,
		}
	}
	return fetcherArgs
}

func initFetcher(
	entries []initFetcherArgs, reverseScan bool, alloc *rowenc.DatumAlloc, memMon *mon.BytesMonitor,
) (fetcher *Fetcher, err error) {
	fetcher = &Fetcher{}

	fetcherCodec := keys.SystemSQLCodec
	fetcherArgs := makeFetcherArgs(entries)

	if err := fetcher.Init(
		context.Background(),
		fetcherCodec,
		reverseScan,
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /* isCheck */
		alloc,
		memMon,
		fetcherArgs...,
	); err != nil {
		return nil, err
	}

	return fetcher, nil
}

type fetcherEntryArgs struct {
	tableName        string
	indexName        string // Specify if this entry is an index
	indexIdx         int    // 0 for primary index (default)
	modFactor        int    // Useful modulo to apply for value columns
	schema           string
	interleaveSchema string // Specify if this entry is to be interleaved into another table
	indexSchema      string // Specify if this entry is to be created as an index
	nRows            int
	nCols            int // Number of columns in the table
	nVals            int // Number of values requested from scan
	valNeededForCol  util.FastIntSet
	genValue         sqlutils.GenRowFn
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

	alloc := &rowenc.DatumAlloc{}

	// We try to read rows from each table.
	for tableName, table := range tables {
		t.Run(tableName, func(t *testing.T) {
			tableDesc := catalogkv.TestingGetImmutableTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

			var valNeededForCol util.FastIntSet
			valNeededForCol.AddRange(0, table.nCols-1)

			args := []initFetcherArgs{
				{
					tableDesc:       tableDesc,
					indexIdx:        0,
					valNeededForCol: valNeededForCol,
				},
			}

			rf, err := initFetcher(args, false /*reverseScan*/, alloc, nil /* memMon */)
			if err != nil {
				t.Fatal(err)
			}

			if err := rf.StartScan(
				context.Background(),
				kv.NewTxn(ctx, kvDB, 0),
				roachpb.Spans{tableDesc.IndexSpan(keys.SystemSQLCodec, tableDesc.GetPrimaryIndexID())},
				false, /*limitBatches*/
				0,     /*limitHint*/
				false, /*traceKV*/
				false, /*forceProductionKVBatchSize*/
			); err != nil {
				t.Fatal(err)
			}

			count := 0

			expectedVals := [2]int64{1, 1}
			for {
				datums, desc, index, err := rf.NextRowDecoded(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if datums == nil {
					break
				}

				count++

				if desc.GetID() != tableDesc.GetID() || index.GetID() != tableDesc.GetPrimaryIndexID() {
					t.Fatalf(
						"unexpected row retrieved from fetcher.\nnexpected:  table %s - index %s\nactual: table %s - index %s",
						tableDesc.GetName(), tableDesc.GetPrimaryIndex().GetName(),
						desc.GetName(), index.GetName(),
					)
				}

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

	alloc := &rowenc.DatumAlloc{}

	// We try to read rows from each table.
	for tableName, table := range tables {
		t.Run(tableName, func(t *testing.T) {
			tableDesc := catalogkv.TestingGetImmutableTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

			var valNeededForCol util.FastIntSet
			valNeededForCol.AddRange(0, table.nCols-1)

			args := []initFetcherArgs{
				{
					tableDesc:       tableDesc,
					indexIdx:        0,
					valNeededForCol: valNeededForCol,
				},
			}

			rf, err := initFetcher(args, false /*reverseScan*/, alloc, nil /*memMon*/)
			if err != nil {
				t.Fatal(err)
			}

			if err := rf.StartScan(
				context.Background(),
				kv.NewTxn(ctx, kvDB, 0),
				roachpb.Spans{tableDesc.IndexSpan(keys.SystemSQLCodec, tableDesc.GetPrimaryIndexID())},
				true,  /*limitBatches*/
				10,    /*limitHint*/
				false, /*traceKV*/
				false, /*forceProductionKVBatchSize*/
			); err != nil {
				t.Fatal(err)
			}

			count := 0

			expectedVals := [2]int64{1, 1}
			for {
				datums, desc, index, err := rf.NextRowDecoded(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if datums == nil {
					break
				}

				count++

				if desc.GetID() != tableDesc.GetID() || index.GetID() != tableDesc.GetPrimaryIndexID() {
					t.Fatalf(
						"unexpected row retrieved from fetcher.\nnexpected:  table %s - index %s\nactual: table %s - index %s",
						tableDesc.GetName(), tableDesc.GetPrimaryIndex().GetName(),
						desc.GetName(), index.GetName(),
					)
				}

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

	tableDesc := catalogkv.TestingGetImmutableTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, 1)

	args := []initFetcherArgs{
		{
			tableDesc:       tableDesc,
			indexIdx:        0,
			valNeededForCol: valNeededForCol,
		},
	}

	alloc := &rowenc.DatumAlloc{}

	settings := cluster.MakeTestingClusterSettings()

	// Give a 1 megabyte limit to the memory monitor, so that
	// we can test whether scans of wide tables are prevented if
	// we have insufficient memory to do them.
	memMon := mon.NewMonitor("test", mon.MemoryResource, nil, nil, -1, 1000, settings)
	memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1<<20))
	defer memMon.Stop(ctx)
	rf, err := initFetcher(args, false /*reverseScan*/, alloc, memMon)
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close(ctx)

	err = rf.StartScan(
		context.Background(),
		kv.NewTxn(ctx, kvDB, 0),
		roachpb.Spans{tableDesc.IndexSpan(keys.SystemSQLCodec, tableDesc.GetPrimaryIndexID())},
		false, /*limitBatches*/
		0,     /*limitHint*/
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

	alloc := &rowenc.DatumAlloc{}

	tableDesc := catalogkv.TestingGetImmutableTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, table.nCols-1)

	args := []initFetcherArgs{
		{
			tableDesc:       tableDesc,
			indexIdx:        0,
			valNeededForCol: valNeededForCol,
		},
	}

	rf, err := initFetcher(args, false /*reverseScan*/, alloc, nil /*memMon*/)
	if err != nil {
		t.Fatal(err)
	}

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
		true, /*limitBatches*/
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
		datums, _, _, err := rf.NextRowDecoded(context.Background())
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

	alloc := &rowenc.DatumAlloc{}
	// We try to read rows from each index.
	for tableName, table := range tables {
		t.Run(tableName, func(t *testing.T) {
			tableDesc := catalogkv.TestingGetImmutableTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tableName)

			var valNeededForCol util.FastIntSet
			valNeededForCol.AddRange(0, table.nVals-1)

			args := []initFetcherArgs{
				{
					tableDesc: tableDesc,
					// We scan from the first secondary index.
					indexIdx:        1,
					valNeededForCol: valNeededForCol,
				},
			}

			rf, err := initFetcher(args, false /*reverseScan*/, alloc, nil /*memMon*/)
			if err != nil {
				t.Fatal(err)
			}

			if err := rf.StartScan(
				context.Background(),
				kv.NewTxn(ctx, kvDB, 0),
				roachpb.Spans{tableDesc.IndexSpan(keys.SystemSQLCodec, tableDesc.PublicNonPrimaryIndexes()[0].GetID())},
				false, /*limitBatches*/
				0,     /*limitHint*/
				false, /*traceKV*/
				false, /*forceProductionKVBatchSize*/
			); err != nil {
				t.Fatal(err)
			}

			count := 0
			nullCount := 0
			var prevIdxVal int64
			for {
				datums, desc, index, err := rf.NextRowDecoded(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if datums == nil {
					break
				}

				count++

				if desc.GetID() != tableDesc.GetID() || index.GetID() != tableDesc.PublicNonPrimaryIndexes()[0].GetID() {
					t.Fatalf(
						"unexpected row retrieved from fetcher.\nnexpected:  table %s - index %s\nactual: table %s - index %s",
						tableDesc.GetName(), tableDesc.PublicNonPrimaryIndexes()[0].GetName(),
						desc.GetName(), index.GetName(),
					)
				}

				if table.nCols != len(datums) {
					t.Fatalf("expected %d columns, got %d columns", table.nCols, len(datums))
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

// Appends all non-empty subsets of indices in [0, maxIdx).
func generateIdxSubsets(maxIdx int, subsets [][]int) [][]int {
	if maxIdx < 0 {
		return subsets
	}
	subsets = generateIdxSubsets(maxIdx-1, subsets)
	curLength := len(subsets)
	for i := 0; i < curLength; i++ {
		// Keep original subsets by duplicating them.
		dupe := make([]int, len(subsets[i]))
		copy(dupe, subsets[i])
		subsets = append(subsets, dupe)
		// Generate new subsets with the current index.
		subsets[i] = append(subsets[i], maxIdx)
	}
	return append(subsets, []int{maxIdx})
}

// We test reading rows from six tables in a database that contains two
// interleave hierarchies.
// The tables are structured as follows:
// parent1
// parent2
//   child1
//      grandchild1
//	      grandgrandchild1
//   child2
//   grandgrandchild1@ggc1_unique_idx
// parent3
// We test reading rows from every non-empty subset for completeness.
func TestNextRowInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tableArgs := map[string]*fetcherEntryArgs{
		"parent1": {
			tableName: "parent1",
			modFactor: 12,
			schema:    "p1 INT PRIMARY KEY, v INT",
			nRows:     100,
			nCols:     2,
			nVals:     2,
		},
		"parent2": {
			tableName: "parent2",
			modFactor: 3,
			schema:    "p2 INT PRIMARY KEY, v INT",
			nRows:     400,
			nCols:     2,
			nVals:     2,
		},
		"child1": {
			tableName:        "child1",
			modFactor:        5,
			schema:           "p2 INT, c1 INT, v INT, PRIMARY KEY (p2, c1)",
			interleaveSchema: "parent2 (p2)",
			// child1 has more rows than parent2, thus some parent2
			// rows will have multiple child1.
			nRows: 500,
			nCols: 3,
			nVals: 3,
		},
		"grandchild1": {
			tableName:        "grandchild1",
			modFactor:        7,
			schema:           "p2 INT, c1 INT, gc1 INT, v INT, PRIMARY KEY (p2, c1, gc1)",
			interleaveSchema: "child1 (p2, c1)",
			nRows:            2000,
			nCols:            4,
			nVals:            4,
		},
		"grandgrandchild1": {
			tableName:        "grandgrandchild1",
			modFactor:        12,
			schema:           "p2 INT, c1 INT, gc1 INT, ggc1 INT, v INT, PRIMARY KEY (p2, c1, gc1, ggc1)",
			interleaveSchema: "grandchild1 (p2, c1, gc1)",
			nRows:            350,
			nCols:            5,
			nVals:            5,
		},
		"child2": {
			tableName:        "child2",
			modFactor:        42,
			schema:           "p2 INT, c2 INT, v INT, PRIMARY KEY (p2, c2)",
			interleaveSchema: "parent2 (p2)",
			// child2 has less rows than parent2, thus not all
			// parent2 rows will have a nested child2 row.
			nRows: 100,
			nCols: 3,
			nVals: 3,
		},
		"parent3": {
			tableName: "parent3",
			modFactor: 42,
			schema:    "p3 INT PRIMARY KEY, v INT",
			nRows:     1,
			nCols:     2,
			nVals:     2,
		},
	}

	for _, table := range tableArgs {
		table.valNeededForCol.AddRange(0, table.nVals-1)
	}

	// Initialize generating value functions for each table.
	tableArgs["parent1"].genValue = sqlutils.ToRowFn(
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tableArgs["parent1"].modFactor),
	)

	tableArgs["parent2"].genValue = sqlutils.ToRowFn(
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tableArgs["parent2"].modFactor),
	)

	tableArgs["child1"].genValue = sqlutils.ToRowFn(
		// Foreign key needs a shifted modulo.
		sqlutils.RowModuloShiftedFn(tableArgs["parent2"].nRows),
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tableArgs["child1"].modFactor),
	)

	tableArgs["grandchild1"].genValue = sqlutils.ToRowFn(
		// Foreign keys need a shifted modulo.
		sqlutils.RowModuloShiftedFn(
			tableArgs["child1"].nRows,
			tableArgs["parent2"].nRows,
		),
		sqlutils.RowModuloShiftedFn(tableArgs["child1"].nRows),
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tableArgs["grandchild1"].modFactor),
	)

	tableArgs["grandgrandchild1"].genValue = sqlutils.ToRowFn(
		// Foreign keys need a shifted modulo.
		sqlutils.RowModuloShiftedFn(
			tableArgs["grandchild1"].nRows,
			tableArgs["child1"].nRows,
			tableArgs["parent2"].nRows,
		),
		sqlutils.RowModuloShiftedFn(
			tableArgs["grandchild1"].nRows,
			tableArgs["child1"].nRows,
		),
		sqlutils.RowModuloShiftedFn(tableArgs["grandchild1"].nRows),
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tableArgs["grandgrandchild1"].modFactor),
	)

	tableArgs["child2"].genValue = sqlutils.ToRowFn(
		// Foreign key needs a shifted modulo.
		sqlutils.RowModuloShiftedFn(tableArgs["parent2"].nRows),
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tableArgs["child2"].modFactor),
	)

	tableArgs["parent3"].genValue = sqlutils.ToRowFn(
		sqlutils.RowIdxFn,
		sqlutils.RowModuloFn(tableArgs["parent3"].modFactor),
	)

	ggc1idx := *tableArgs["grandgrandchild1"]
	// This is only possible since nrows(ggc1) < nrows(p2) thus c1 is
	// unique.
	ggc1idx.indexSchema = fmt.Sprintf(
		`CREATE UNIQUE INDEX ggc1_unique_idx ON %s.grandgrandchild1 (p2) INTERLEAVE IN PARENT %s.parent2 (p2);`,
		sqlutils.TestDB,
		sqlutils.TestDB,
	)
	ggc1idx.indexName = "ggc1_unique_idx"
	ggc1idx.indexIdx = 1
	// Last column v (idx 4) is not stored in this index.
	ggc1idx.valNeededForCol = ggc1idx.valNeededForCol.Copy()
	ggc1idx.valNeededForCol.Remove(4)
	ggc1idx.nVals = 4

	// We need an ordering of the tables in order to execute the interleave
	// DDL statements.
	interleaveEntries := []fetcherEntryArgs{
		*tableArgs["parent1"],
		*tableArgs["parent2"],
		*tableArgs["child1"],
		*tableArgs["grandchild1"],
		*tableArgs["grandgrandchild1"],
		*tableArgs["child2"],
		ggc1idx,
		*tableArgs["parent3"],
	}

	for _, table := range interleaveEntries {
		if table.indexSchema != "" {
			// Create interleaved secondary indexes.
			r := sqlutils.MakeSQLRunner(sqlDB)
			r.Exec(t, table.indexSchema)
		} else {
			// Create tables (primary indexes).
			sqlutils.CreateTableInterleaved(
				t, sqlDB, table.tableName,
				table.schema,
				table.interleaveSchema,
				table.nRows,
				table.genValue,
			)
		}
	}

	alloc := &rowenc.DatumAlloc{}
	// Retrieve rows from every non-empty subset of the tables/indexes.
	for _, idxs := range generateIdxSubsets(len(interleaveEntries)-1, nil) {
		// Initialize our subset of tables/indexes.
		entries := make([]*fetcherEntryArgs, len(idxs))
		testNames := make([]string, len(entries))
		for i, idx := range idxs {
			entries[i] = &interleaveEntries[idx]
			testNames[i] = entries[i].tableName
			// Use the index name instead if we're scanning an index.
			if entries[i].indexName != "" {
				testNames[i] = entries[i].indexName
			}
		}

		testName := strings.Join(testNames, "-")

		t.Run(testName, func(t *testing.T) {
			// Initialize the RowFetcher.
			args := make([]initFetcherArgs, len(entries))
			lookupSpans := make([]roachpb.Span, len(entries))
			// Used during NextRow to see if tableID << 32 |
			// indexID (key) are with what we initialize
			// RowFetcher.
			idLookups := make(map[uint64]*fetcherEntryArgs, len(entries))
			for i, entry := range entries {
				tableDesc := catalogkv.TestingGetImmutableTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, entry.tableName)
				indexID := tableDesc.ActiveIndexes()[entry.indexIdx].GetID()
				idLookups[idLookupKey(tableDesc.GetID(), indexID)] = entry

				// We take every entry's index span (primary or
				// secondary) and use it to start our scan.
				lookupSpans[i] = tableDesc.IndexSpan(keys.SystemSQLCodec, indexID)

				args[i] = initFetcherArgs{
					tableDesc:       tableDesc,
					indexIdx:        entry.indexIdx,
					valNeededForCol: entry.valNeededForCol,
					spans:           roachpb.Spans{lookupSpans[i]},
				}
			}

			lookupSpans, _ = roachpb.MergeSpans(&lookupSpans)

			rf, err := initFetcher(args, false /*reverseScan*/, alloc, nil /*memMon*/)
			if err != nil {
				t.Fatal(err)
			}

			if err := rf.StartScan(
				context.Background(),
				kv.NewTxn(ctx, kvDB, 0),
				lookupSpans,
				false, /*limitBatches*/
				0,     /*limitHint*/
				false, /*traceKV*/
				false, /*forceProductionKVBatchSize*/
			); err != nil {
				t.Fatal(err)
			}

			// Running count of rows processed for each table-index.
			count := make(map[string]int, len(entries))

			for {
				datums, desc, index, err := rf.NextRowDecoded(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if datums == nil {
					break
				}

				entry, found := idLookups[idLookupKey(desc.GetID(), index.GetID())]
				if !found {
					t.Fatalf(
						"unexpected row from table %s - index %s",
						desc.GetName(), index.GetName(),
					)
				}

				tableIdxName := fmt.Sprintf("%s@%s", entry.tableName, entry.indexName)
				count[tableIdxName]++

				// Check that the correct # of columns is returned.
				if entry.nCols != len(datums) {
					t.Fatalf("for table %s expected %d columns, got %d columns", tableIdxName, entry.nCols, len(datums))
				}

				// Verify that the correct # of values are returned.
				numVals := 0
				for _, datum := range datums {
					if datum != tree.DNull {
						numVals++
					}
				}
				if entry.nVals != numVals {
					t.Fatalf("for table %s expected %d non-NULL values, got %d", tableIdxName, entry.nVals, numVals)
				}

				// Verify the value in the value column is
				// correct if it is requested.
				if entry.nVals == entry.nCols {
					id := int64(*datums[entry.nCols-2].(*tree.DInt))
					val := int64(*datums[entry.nCols-1].(*tree.DInt))

					if id%int64(entry.modFactor) != val {
						t.Fatalf("for table %s row id %d, expected %d value, got %d", tableIdxName, id, id%int64(entry.modFactor), val)
					}
				}
			}

			for _, entry := range entries {
				lookup := fmt.Sprintf("%s@%s", entry.tableName, entry.indexName)

				actual, ok := count[lookup]
				if !ok {
					t.Errorf("no rows were retrieved for table %s, expected %d rows", entry.tableName, entry.nRows)
					continue
				}

				if entry.nRows != actual {
					t.Errorf("for table %s expected %d rows, got %d rows", entry.tableName, entry.nRows, actual)
				}
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
	tableDesc := catalogkv.TestingGetImmutableTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "foo")
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, 1)
	args := []initFetcherArgs{
		{
			tableDesc:       tableDesc,
			indexIdx:        0,
			valNeededForCol: valNeededForCol,
		},
	}
	da := rowenc.DatumAlloc{}
	fetcher, err := initFetcher(args, false, &da, nil /*memMon*/)
	if err != nil {
		t.Fatal(err)
	}

	resetFetcher, err := initFetcher(args, false /*reverseScan*/, &da, nil /*memMon*/)
	if err != nil {
		t.Fatal(err)
	}

	resetFetcher.Reset()
	if len(resetFetcher.tables) != 0 || cap(resetFetcher.tables) != 1 {
		t.Fatal("Didn't find saved slice:", resetFetcher.tables)
	}

	// Now re-init the reset fetcher and make sure its the same as the fetcher we
	// didn't reset.

	fetcherArgs := makeFetcherArgs(args)
	if err := resetFetcher.Init(
		ctx,
		keys.SystemSQLCodec,
		false, /*reverse*/
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /* isCheck */
		&da,
		nil, /* memMonitor */
		fetcherArgs...,
	); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(resetFetcher, fetcher) {
		t.Fatal("unequal before and after reset", resetFetcher, fetcher)
	}

}

func idLookupKey(tableID descpb.ID, indexID descpb.IndexID) uint64 {
	return (uint64(tableID) << 32) | uint64(indexID)
}

func TestFetcherUninitialized(t *testing.T) {
	// Regression test for #39013: make sure it's okay to call GetBytesReader even
	// before the fetcher was fully initialized.
	var fetcher Fetcher
	assert.Zero(t, fetcher.GetBytesRead())
}
