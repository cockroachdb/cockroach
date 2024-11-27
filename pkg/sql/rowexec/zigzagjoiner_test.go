// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

type zigzagJoinerTestCase struct {
	desc          string
	spec          execinfrapb.ZigzagJoinerSpec
	outCols       []uint32
	fixedValues   []rowenc.EncDatumRow
	expectedTypes []*types.T
	expected      string
}

func intCols(numCols int) []*types.T {
	cols := make([]*types.T, numCols)
	for i := range cols {
		cols[i] = types.Int
	}
	return cols
}

func encInt(i int) rowenc.EncDatum {
	return rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
}

func TestZigzagJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	null := tree.DNull

	identity := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row))
	}
	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 5))
	}
	bFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 5))
	}
	cFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row%3 + 3))
	}
	dFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt((row+1)%4 + 4))
	}
	eFn := func(row int) tree.Datum {
		if row%5 == 0 {
			return null
		}
		return tree.NewDInt(tree.DInt((row+1)%4 + 4))
	}

	offsetFn := func(oldFunc func(int) tree.Datum, offset int) func(int) tree.Datum {
		offsetFunc := func(row int) tree.Datum {
			return oldFunc(row + offset)
		}
		return offsetFunc
	}

	sqlutils.CreateTable(t, sqlDB, "empty",
		"a INT, b INT, x INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		0,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
	)

	// Drop a column to test https://github.com/cockroachdb/cockroach/issues/37196
	_, err := sqlDB.Exec("ALTER TABLE test.empty DROP COLUMN x")
	require.NoError(t, err)

	// Drop and add an index to test
	// https://github.com/cockroachdb/cockroach/issues/42164.
	_, err = sqlDB.Exec("DROP INDEX test.empty@d")
	require.NoError(t, err)
	_, err = sqlDB.Exec("CREATE INDEX d ON test.empty(d)")
	require.NoError(t, err)

	sqlutils.CreateTable(t, sqlDB, "single",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		1,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
	)

	sqlutils.CreateTable(t, sqlDB, "small",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		10,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
	)

	sqlutils.CreateTable(t, sqlDB, "med",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		22,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
	)

	sqlutils.CreateTable(t, sqlDB, "overlapping",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), INDEX ac (a, c), INDEX d (d)",
		22,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
	)

	sqlutils.CreateTable(t, sqlDB, "comp",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), INDEX cab (c, a, b), INDEX d (d)",
		22,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
	)

	sqlutils.CreateTable(t, sqlDB, "rev",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), INDEX cba (c, b, a), INDEX d (d)",
		22,
		sqlutils.ToRowFn(aFn, bFn, cFn, dFn),
	)

	offset := 44
	sqlutils.CreateTable(t, sqlDB, "offset",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		20,
		sqlutils.ToRowFn(offsetFn(aFn, offset), offsetFn(bFn, offset), offsetFn(cFn, offset), offsetFn(dFn, offset)),
	)

	unqOffset := 20
	sqlutils.CreateTable(t, sqlDB, "unq",
		"a INT, b INT, c INT UNIQUE, d INT, PRIMARY KEY (a, b), INDEX cb (c, b), INDEX d (d)",
		20,
		sqlutils.ToRowFn(
			offsetFn(aFn, unqOffset),
			offsetFn(bFn, unqOffset),
			offsetFn(identity, unqOffset),
			offsetFn(dFn, unqOffset),
		),
	)

	sqlutils.CreateTable(t, sqlDB, "t2",
		"b INT, a INT, PRIMARY KEY (b, a)",
		10,
		sqlutils.ToRowFn(bFn, aFn),
	)

	sqlutils.CreateTable(t, sqlDB, "nullable",
		"a INT, b INT, e INT, d INT, PRIMARY KEY (a, b), INDEX e (e), INDEX d (d)",
		10,
		sqlutils.ToRowFn(aFn, bFn, eFn, dFn),
	)

	empty := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "empty")
	single := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "single")
	smallDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "small")
	medDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "med")
	highRangeDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "offset")
	overlappingDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "overlapping")
	compDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "comp")
	revCompDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "rev")
	compUnqDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "unq")
	t2Desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t2")
	nullableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "nullable")

	testCases := []zigzagJoinerTestCase{
		{
			desc: "join on an empty table with itself on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, empty, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, empty, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 3},
			expectedTypes: intCols(4),
			expected:      "[]",
		},
		{
			desc: "join an empty table on the left with a populated table on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, empty, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, highRangeDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 3},
			expectedTypes: intCols(4),
			expected:      "[]",
		},
		{
			desc: "join a populated table on the left with an empty table on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, highRangeDesc, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, empty, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 3},
			expectedTypes: intCols(4),
			expected:      "[]",
		},
		{
			desc: "join an empty table on the left with a populated table on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, empty, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, highRangeDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 3},
			expectedTypes: intCols(4),
			expected:      "[]",
		},
		{
			desc: "join a table with a single row with itself on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, single, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, single, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 3},
			expectedTypes: intCols(4),
			expected:      "[]",
		},
		{
			desc: "join a table with a few rows with itself on its primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, smallDesc, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, smallDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 5},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7]]",
		},
		{
			desc: "join a populated table that has a match in the last row with itself",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, medDesc, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, medDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 5},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7] [3 3 3 7]]",
		},
		{
			desc: "(a) is free, and outputs cartesian product",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, medDesc, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
					{
						FetchSpec: makeFetchSpec(t, medDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 3, 4, 5},
			expectedTypes: intCols(6),
			expected: "[[0 3 3 0 2 7] [1 4 3 1 1 7] [1 1 3 1 1 7] [2 2 3 2 4 7] [2 2 3 2 0 7] [3 3 3 3 3 7] " +
				"[3 0 3 3 3 7] [4 1 3 4 2 7]]",
		},
		{
			desc: "set the fixed columns to be a part of the primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, medDesc, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{1}},
					},
					{
						FetchSpec: makeFetchSpec(t, medDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3), encInt(1)}, {encInt(7), encInt(1)}},
			outCols:       []uint32{0, 1, 2, 5},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7]]",
		},
		{
			desc: "join should work when there is a block of matches",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, highRangeDesc, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
					{
						FetchSpec: makeFetchSpec(t, highRangeDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 3, 4, 5},
			expectedTypes: intCols(6),
			expected: "[[9 3 3 9 1 7] [9 0 3 9 1 7] [10 4 3 10 4 7] [10 4 3 10 0 7] [10 1 3 10 4 7] " +
				"[10 1 3 10 0 7] [11 2 3 11 3 7] [12 3 3 12 2 7] [12 0 3 12 2 7]]",
		},
		{
			desc: "join two different tables where first one is larger",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, medDesc, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, smallDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 3, 4, 5},
			expectedTypes: intCols(6),
			expected:      "[[1 1 3 1 1 7]]",
		},
		{
			desc: "join two different tables where second is larger",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, smallDesc, "c", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, medDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 3, 4, 5},
			expectedTypes: intCols(6),
			expected:      "[[1 1 3 1 1 7]]",
		},
		{
			desc: "join on an index containing primary key columns explicitly",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, overlappingDesc, "ac", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{1}},
					},
					{
						FetchSpec: makeFetchSpec(t, overlappingDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3) /*a*/, encInt(3) /*c*/}, {encInt(7) /*d*/, encInt(3) /*a*/}},
			outCols:       []uint32{0, 1, 2, 5},
			expectedTypes: intCols(4),
			expected:      "[[3 3 3 7]]",
		},
		{
			desc: "join two tables with different schemas",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, smallDesc, "small_pkey", "a,b"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, t2Desc, "t2_pkey", "b,a"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{}, {}},
			outCols:       []uint32{0, 1},
			expectedTypes: intCols(2),
			expected:      "[[0 1] [0 2] [1 0] [1 1] [2 0]]",
		},
		{
			desc: "join two tables with different schemas flipped",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, t2Desc, "t2_pkey", "b,a"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, smallDesc, "small_pkey", "a,b"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{}, {}},
			outCols:       []uint32{0, 1},
			expectedTypes: intCols(2),
			expected:      "[[0 1] [0 2] [1 0] [1 1] [2 0]]",
		},
		{
			desc: "join on a populated table with no fixed columns",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, smallDesc, "small_pkey", "a,b"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, smallDesc, "small_pkey", "a,b"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{}, {}},
			outCols:       []uint32{0, 1},
			expectedTypes: intCols(2),
			expected:      "[[0 1] [0 2] [0 3] [0 4] [1 0] [1 1] [1 2] [1 3] [1 4] [2 0]]",
		},
		{
			desc: "join tables with different schemas with no locked columns",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, smallDesc, "small_pkey", "a,b"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{1}},
					},
					{
						FetchSpec: makeFetchSpec(t, t2Desc, "t2_pkey", "b,a"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(1)}, {encInt(1)}},
			outCols:       []uint32{0, 1},
			expectedTypes: intCols(2),
			expected:      "[[1 0] [1 1]]",
		},
		{
			desc: "join a composite index with itself",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, compDesc, "cab", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, compDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 5},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7] [3 3 3 7]]",
		},
		{
			desc: "join a composite index with the primary key reversed with itself",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, revCompDesc, "cba", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}}, // join on a
					},
					{
						FetchSpec: makeFetchSpec(t, revCompDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3), encInt(1)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 5},
			expectedTypes: intCols(4),
			expected:      "[[1 1 3 7] [4 1 3 7]]",
		},
		{
			desc: "join a composite index with the primary key reversed with itself with onExpr on value on one side",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, revCompDesc, "cba", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
					{
						FetchSpec: makeFetchSpec(t, revCompDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
				},
				Type:   descpb.InnerJoin,
				OnExpr: execinfrapb.Expression{Expr: "@1 > 1"},
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3), encInt(1)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 5},
			expectedTypes: intCols(4),
			expected:      "[[4 1 3 7]]",
		},
		{
			desc: "join a composite index with the primary key reversed with itself and with onExpr comparing both sides",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, revCompDesc, "cba", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
					{
						FetchSpec: makeFetchSpec(t, revCompDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
				},
				Type:   descpb.InnerJoin,
				OnExpr: execinfrapb.Expression{Expr: "@6 < 2 * @1"},
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(3), encInt(1)}, {encInt(7)}},
			outCols:       []uint32{0, 1, 2, 5},
			expectedTypes: intCols(4),
			expected:      "[[4 1 3 7]]",
		},
		{
			desc: "join a composite index that doesn't contain the full primary key with itself",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, compUnqDesc, "cb", "a,b,c"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{1}},
					},
					{
						FetchSpec: makeFetchSpec(t, compUnqDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{encInt(21) /* c */}, {encInt(6), encInt(4) /* d, a */}},
			outCols:       []uint32{0, 1, 2, 5},
			expectedTypes: intCols(4),
			expected:      "[[4 1 21 6]]",
		},
		{
			desc: "test when equality columns may be null",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, nullableDesc, "e", "a,b,e"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{2}},
					},
					{
						FetchSpec: makeFetchSpec(t, nullableDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{2}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{}, {}},
			outCols:       []uint32{0, 1, 2, 3, 4, 5},
			expectedTypes: intCols(6),
			expected: "[[1 2 4 1 2 4] [1 2 4 0 3 4] [0 3 4 1 2 4] [0 3 4 0 3 4] [1 3 5 1 3 5] " +
				"[1 3 5 0 4 5] [0 4 5 1 3 5] [0 4 5 0 4 5] [1 4 6 1 4 6] [1 4 6 1 0 6] [1 4 6 0 1 6] " +
				"[0 1 6 1 4 6] [0 1 6 1 0 6] [0 1 6 0 1 6] [1 1 7 2 0 7] [1 1 7 1 1 7] [1 1 7 0 2 7] " +
				"[0 2 7 2 0 7] [0 2 7 1 1 7] [0 2 7 0 2 7]]",
		},
		{
			desc: "test joining with primary key",
			spec: execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, medDesc, "med_pkey", "a,b,c,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0}},
					},
					{
						FetchSpec: makeFetchSpec(t, medDesc, "d", "a,b,d"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{2}},
					},
				},
				Type: descpb.InnerJoin,
			},
			fixedValues:   []rowenc.EncDatumRow{{}, {}},
			outCols:       []uint32{0, 1, 2, 3, 4, 5, 6},
			expectedTypes: intCols(7),
			expected: "[[4 2 4 7 3 4 4] [4 1 3 6 3 4 4] [4 0 5 5 3 4 4] [4 2 4 7 3 0 4] [4 1 3 6 3 0 4] " +
				"[4 0 5 5 3 0 4] [4 2 4 7 2 1 4] [4 1 3 6 2 1 4] [4 0 5 5 2 1 4] [4 2 4 7 1 2 4] " +
				"[4 1 3 6 1 2 4] [4 0 5 5 1 2 4] [4 2 4 7 0 3 4] [4 1 3 6 0 3 4] [4 0 5 5 0 3 4]]",
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			st := cluster.MakeTestingClusterSettings()
			evalCtx := eval.MakeTestingEvalContext(st)
			defer evalCtx.Stop(ctx)
			flowCtx := execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				Mon:     evalCtx.TestingMon,
				Cfg:     &execinfra.ServerConfig{Settings: st},
				Txn:     kv.NewTxn(ctx, s.DB(), s.NodeID()),
			}

			out := &distsqlutils.RowBuffer{}
			post := execinfrapb.PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			z, err := newZigzagJoiner(ctx, &flowCtx, 0 /* processorID */, &c.spec, c.fixedValues, &post)
			if err != nil {
				t.Fatal(err)
			}

			z.Run(ctx, out)

			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}

			var res rowenc.EncDatumRows
			for {
				row, meta := out.Next()
				if meta != nil && meta.Metrics == nil {
					t.Fatalf("unexpected metadata %+v", meta)
				}
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if result := res.String(c.expectedTypes); result != c.expected {
				t.Errorf("invalid results for test '%s': %s, expected %s'", c.desc, result, c.expected)
			}
		})
	}
}

// TestJoinReaderDrain tests various scenarios in which a zigzagJoiner's consumer
// is closed.
func TestZigzagJoinerDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	v := [10]tree.Datum{}
	for i := range v {
		v[i] = tree.NewDInt(tree.DInt(i))
	}
	encThree := rowenc.DatumToEncDatum(types.Int, v[3])
	encSeven := rowenc.DatumToEncDatum(types.Int, v[7])

	sqlutils.CreateTable(
		t,
		sqlDB,
		"t",
		"a INT, b INT, c INT, d INT, PRIMARY KEY (a,b), INDEX c (c), INDEX d (d)",
		1, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowIdxFn, sqlutils.RowIdxFn, sqlutils.RowIdxFn),
	)
	td := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	// Run the flow in a verbose trace so that we can test for tracing info.
	tracer := s.TracerI().(*tracing.Tracer)
	ctx, sp := tracer.StartSpanCtx(context.Background(), "test flow ctx", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer sp.Finish()
	evalCtx := eval.MakeTestingEvalContext(s.ClusterSettings())
	defer evalCtx.Stop(ctx)

	rootTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	leafInputState, err := rootTxn.GetLeafTxnInputState(ctx)
	require.NoError(t, err)
	leafTxn := kv.NewLeafTxn(ctx, s.DB(), s.NodeID(), leafInputState)
	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg:     &execinfra.ServerConfig{Settings: s.ClusterSettings()},
		Txn:     leafTxn,
		Gateway: false,
	}

	testReaderProcessorDrain(ctx, t, func() (execinfra.Processor, error) {
		return newZigzagJoiner(
			ctx,
			&flowCtx,
			0, /* processorID */
			&execinfrapb.ZigzagJoinerSpec{
				Sides: []execinfrapb.ZigzagJoinerSpec_Side{
					{
						FetchSpec: makeFetchSpec(t, td, "t_pkey", "a,b"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
					{
						FetchSpec: makeFetchSpec(t, td, "c", "a,b"),
						EqColumns: execinfrapb.Columns{Columns: []uint32{0, 1}},
					},
				},
				Type: descpb.InnerJoin,
			},
			[]rowenc.EncDatumRow{{encThree}, {encSeven}},
			&execinfrapb.PostProcessSpec{Projection: true, OutputColumns: []uint32{0, 1}},
		)
	})
}
