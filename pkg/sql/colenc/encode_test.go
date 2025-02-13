// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colenc_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colenc"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/stretchr/testify/require"
)

// TestEncoderEquality tests that the vector encoder and the row based encoder
// produce the exact same KV batches. Check constraints and partial indexes
// are left to copy datadriven tests so we don't have to muck with generating
// predicate columns.
func TestEncoderEqualityDatums(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV

	compDec, err := tree.ParseDDecimal("-0")
	require.NoError(t, err)

	colString, err := tree.NewDCollatedString("asdf", "en", &tree.CollationEnvironment{})
	require.NoError(t, err)

	rng, _ := randutil.NewTestRand()
	testCases := []struct {
		cols     string
		datums   tree.Datums
		extraDDL []string
	}{
		// NB: Some coverage of things that are a pain to construct in go (like
		// enum Datums) is left to the copy data driven tests. Also
		// check constraints and partial index support.

		{"a INT ARRAY PRIMARY KEY", []tree.Datum{randgen.RandArray(rng, types.MakeArray(types.Int), 2)}, nil},
		{"i INT PRIMARY KEY, a INT ARRAY", []tree.Datum{tree.NewDInt(1234), randgen.RandArray(rng, types.MakeArray(types.Int), 2)}, nil},
		{"i INT PRIMARY KEY, a INT ARRAY UNIQUE", []tree.Datum{tree.NewDInt(1234), randgen.RandArray(rng, types.MakeArray(types.Int), 2)}, nil},
		{"i INT PRIMARY KEY, a INT ARRAY, INDEX(a ASC) ", []tree.Datum{tree.NewDInt(1234), randgen.RandArray(rng, types.MakeArray(types.Int), 2)}, nil},
		{"i INT PRIMARY KEY, a INT ARRAY, INDEX(a DESC)", []tree.Datum{tree.NewDInt(1234), randgen.RandArray(rng, types.MakeArray(types.Int), 2)}, nil},
		{"i INT PRIMARY KEY, b INT, a INT ARRAY, INVERTED INDEX(a ASC) ", []tree.Datum{tree.NewDInt(1234), tree.NewDInt(-1234), randgen.RandArray(rng, types.MakeArray(types.Int), 2)}, nil},

		// N.B:  If you get:
		// Received unexpected error:
		// geos: no locations to init GEOS
		// Wraps: (2) Ensure you have the spatial libraries installed as per the instructions in https://www.cockroachlabs.com/docs/v23.1/install-cockroachdb-mac
		// running './dev build geos' should fix it.
		{`id INT PRIMARY KEY,
		geog GEOGRAPHY(geometry, 4326),
		geom GEOMETRY(point),
		orphan GEOGRAPHY,
		INVERTED INDEX (geog),
		INVERTED INDEX (geom),
		FAMILY f (orphan)`,
			[]tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Geography), randgen.RandDatumSimple(rng, types.Geometry), randgen.RandDatumSimple(rng, types.Geography)},
			[]string{"CREATE INDEX ON %s USING GIST(geom)"}},

		// composite encoded types
		{"d DECIMAL, PRIMARY KEY (d ASC)", []tree.Datum{compDec}, nil},
		{"f FLOAT8, PRIMARY KEY (f ASC)", []tree.Datum{tree.NewDFloat(-0)}, nil},
		{"a STRING COLLATE en, PRIMARY KEY (a)", []tree.Datum{colString}, nil},
		{"a FLOAT ARRAY PRIMARY KEY", []tree.Datum{randgen.RandArray(rng, types.MakeArray(types.Float), 2)}, nil},

		{"i INT PRIMARY KEY, bi BIT, n NAME, ine INET, INDEX(i) STORING (bi,n,ine)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.VarBit), randgen.RandDatumSimple(rng, types.Name), randgen.RandDatumSimple(rng, types.INet)}, nil},

		// inverted json indexes w/ and w/o families
		{"i INT PRIMARY KEY, j JSON, k INT,INVERTED INDEX (k,j), INDEX(k,i), FAMILY (j)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json), randgen.RandDatumSimple(rng, types.Int)}, nil},
		{"i INT PRIMARY KEY, j JSON, k INT,INVERTED INDEX (k,j), INDEX(k,i), FAMILY (j)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json), randgen.RandDatumSimple(rng, types.Int)}, nil},
		{"i INT PRIMARY KEY, j JSON, k INT,INVERTED INDEX (k,j), UNIQUE INDEX(k,i), FAMILY (j)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json), randgen.RandDatumSimple(rng, types.Int)}, nil},
		{"i INT PRIMARY KEY, j JSON, k INT,INVERTED INDEX (k,j), INDEX(k,i), FAMILY (j), FAMILY (k)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json), randgen.RandDatumSimple(rng, types.Int)}, nil},

		// inverted string indexes w/ and w/o families
		{"i INT PRIMARY KEY, s STRING, k INT, INDEX(k,i)",
			[]tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.String), randgen.RandDatumSimple(rng, types.Int)},
			[]string{"CREATE INVERTED INDEX ON %s (s gin_trgm_ops)"}},
		{"i INT PRIMARY KEY, s STRING, k INT, INDEX(k,i), FAMILY (s), FAMILY (k)",
			[]tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.String), randgen.RandDatumSimple(rng, types.Int)},
			[]string{"CREATE INVERTED INDEX ON %s (s gin_trgm_ops)"}},
		{"i INT PRIMARY KEY, s STRING, k INT, UNIQUE INDEX(k,i), FAMILY (s), FAMILY (k)",
			[]tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.String), randgen.RandDatumSimple(rng, types.Int)},
			[]string{"CREATE INVERTED INDEX ON %s (s gin_trgm_ops)"}},

		// Test some datums that aren't vectorized.
		{"i INT PRIMARY KEY, bi BIT, n NAME, ine INET", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.VarBit), randgen.RandDatumSimple(rng, types.Name), randgen.RandDatumSimple(rng, types.INet)}, nil},
		{"i INT PRIMARY KEY, bi BIT, n NAME, ine INET UNIQUE", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.VarBit), randgen.RandDatumSimple(rng, types.Name), randgen.RandDatumSimple(rng, types.INet)}, nil},

		{"i INT PRIMARY KEY, j JSON, k INT,INVERTED INDEX (k,j)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json), randgen.RandDatumSimple(rng, types.Int)}, nil},
		{"i INT PRIMARY KEY, j JSON, INVERTED INDEX (j)", []tree.Datum{tree.NewDInt(1234), randgen.RandDatumSimple(rng, types.Json)}, nil},
		// Key encoding of vector types, both directions
		{"b BOOL, PRIMARY KEY (b ASC)", []tree.Datum{tree.DBoolFalse}, nil},
		{"b BOOL, PRIMARY KEY (b DESC)", []tree.Datum{tree.DBoolFalse}, nil},
		{"i INT, PRIMARY KEY (i ASC)", []tree.Datum{tree.NewDInt(1234)}, nil},
		{"i INT, PRIMARY KEY (i DESC)", []tree.Datum{tree.NewDInt(1234)}, nil},
		{"d DATE, PRIMARY KEY (d ASC)", []tree.Datum{tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(314159))}, nil},
		{"d DATE, PRIMARY KEY (d DESC)", []tree.Datum{tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(314159))}, nil},
		{"f FLOAT8, PRIMARY KEY (f ASC)", []tree.Datum{tree.NewDFloat(1.234)}, nil},
		{"f FLOAT8, PRIMARY KEY (f DESC)", []tree.Datum{tree.NewDFloat(1.234)}, nil},
		{"d DECIMAL, PRIMARY KEY (d ASC)", []tree.Datum{&tree.DDecimal{Decimal: *apd.New(123, 2)}}, nil},
		{"d DECIMAL, PRIMARY KEY (d DESC)", []tree.Datum{&tree.DDecimal{Decimal: *apd.New(123, 2)}}, nil},

		{"b BYTES, PRIMARY KEY (b ASC)", []tree.Datum{randgen.RandDatum(rng, types.Bytes, false)}, nil},
		{"b BYTES, PRIMARY KEY (b DESC)", []tree.Datum{randgen.RandDatum(rng, types.Bytes, false)}, nil},

		{"i INT PRIMARY KEY, b BYTES", []tree.Datum{tree.NewDInt(1234), randgen.RandDatum(rng, types.Bytes, true)}, nil},

		{"s STRING, PRIMARY KEY (s ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.String)}, nil},
		{"s STRING, PRIMARY KEY (s DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.String)}, nil},

		{"u UUID, PRIMARY KEY (u ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Uuid)}, nil},
		{"u UUID, PRIMARY KEY (u DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Uuid)}, nil},

		{"ts TIMESTAMP, PRIMARY KEY (ts ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Timestamp)}, nil},
		{"ts TIMESTAMP, PRIMARY KEY (ts DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Timestamp)}, nil},

		{"ts TIMESTAMPTZ, PRIMARY KEY (ts ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.TimestampTZ)}, nil},
		{"ts TIMESTAMPTZ, PRIMARY KEY (ts DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.TimestampTZ)}, nil},

		{"i INTERVAL, PRIMARY KEY (i ASC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Interval)}, nil},
		{"i INTERVAL, PRIMARY KEY (i DESC)", []tree.Datum{randgen.RandDatumSimple(rng, types.Interval)}, nil},

		// Now do all valueside types, cover all vector types and one datum type.
		{"i INT PRIMARY KEY, a BOOL, b INT, c DATE, d FLOAT8, e DECIMAL, f BYTES, g STRING, h UUID, t TIMESTAMP, j TIMESTAMPTZ, k INTERVAL, l JSON, m INET",
			[]tree.Datum{
				randgen.RandDatumSimple(rng, types.Int),
				randgen.RandDatumSimple(rng, types.Bool),
				randgen.RandDatumSimple(rng, types.Int),
				randgen.RandDatumSimple(rng, types.Date),
				randgen.RandDatumSimple(rng, types.Float),
				randgen.RandDatumSimple(rng, types.Decimal),
				randgen.RandDatumSimple(rng, types.Bytes),
				randgen.RandDatumSimple(rng, types.String),
				randgen.RandDatumSimple(rng, types.Uuid),
				randgen.RandDatumSimple(rng, types.Timestamp),
				randgen.RandDatumSimple(rng, types.TimestampTZ),
				randgen.RandDatumSimple(rng, types.Interval),
				randgen.RandDatumSimple(rng, types.Json),
				randgen.RandDatumSimple(rng, types.INet),
			}, nil},

		// Now do them in family by themselves for MashalLegacy coverage.
		{`i INT PRIMARY KEY, a BOOL, b INT, c DATE, d FLOAT8, e DECIMAL, f BYTES, g STRING, h UUID, t TIMESTAMP, j TIMESTAMPTZ, k INTERVAL, l JSON, m INET,
		FAMILY (b), FAMILY (c), FAMILY (d), FAMILY (e), FAMILY (f), FAMILY (g), FAMILY (h), FAMILY (t), FAMILY (j), FAMILY (k), FAMILY (l), FAMILY (m)`,
			[]tree.Datum{
				randgen.RandDatumSimple(rng, types.Int),
				randgen.RandDatumSimple(rng, types.Bool),
				randgen.RandDatumSimple(rng, types.Int),
				randgen.RandDatumSimple(rng, types.Date),
				randgen.RandDatumSimple(rng, types.Float),
				randgen.RandDatumSimple(rng, types.Decimal),
				randgen.RandDatumSimple(rng, types.Bytes),
				randgen.RandDatumSimple(rng, types.String),
				randgen.RandDatumSimple(rng, types.Uuid),
				randgen.RandDatumSimple(rng, types.Timestamp),
				randgen.RandDatumSimple(rng, types.TimestampTZ),
				randgen.RandDatumSimple(rng, types.Interval),
				randgen.RandDatumSimple(rng, types.Json),
				randgen.RandDatumSimple(rng, types.INet),
			}, nil},

		// Column families and null handling
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL, INDEX (a,b,c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}, nil},
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL, FAMILY (a),  FAMILY (b), FAMILY (c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}, nil},
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL, FAMILY (a,b), FAMILY (c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}, nil},
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL, FAMILY (a,b,c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}, nil},
		{"i INT PRIMARY KEY, a BOOL, b BOOL, c BOOL", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}, nil},
		{"i INT PRIMARY KEY, a BOOL, b BOOL UNIQUE, c BOOL, INDEX(c,b,a), INDEX(b,a), FAMILY (a),  FAMILY (b), FAMILY (c)", []tree.Datum{tree.NewDInt(1234), tree.DBoolFalse, tree.DBoolTrue, tree.DNull}, nil},
		{"i INT PRIMARY KEY, a INT, b INT, c INT, d INT, e INT, f INT", []tree.Datum{tree.NewDInt(1234), tree.NewDInt(1), tree.NewDInt(0), tree.NewDInt(-1), tree.NewDInt(math.MaxInt64), tree.NewDInt(math.MinInt64), tree.DNull}, nil},
		{"i INT PRIMARY KEY, a FLOAT8, b FLOAT8, c FLOAT8, d FLOAT8", []tree.Datum{tree.NewDInt(1234), tree.DNaNFloat, tree.NewDFloat(0.0), tree.NewDFloat(math.MaxFloat64), tree.DNull}, nil},
		{"i INT PRIMARY KEY, d DATE, e DATE", []tree.Datum{tree.NewDInt(1234), tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(314159)), tree.DNull}, nil},
		{"i INT PRIMARY KEY, d DECIMAL, e DECIMAL", []tree.Datum{tree.NewDInt(1234), &tree.DDecimal{Decimal: *apd.New(123, 2)}, tree.DNull}, nil},
		{"i INT PRIMARY KEY, b BYTES", []tree.Datum{tree.NewDInt(1234), tree.DNull}, nil},
		{"i INT PRIMARY KEY, b STRING", []tree.Datum{tree.NewDInt(1234), tree.DNull}, nil},
		{"i INT PRIMARY KEY, b UUID", []tree.Datum{tree.NewDInt(1234), tree.DNull}, nil},
		{"i INT PRIMARY KEY, b TIMESTAMP", []tree.Datum{tree.NewDInt(1234), tree.DNull}, nil},
		{"i INT PRIMARY KEY, b TIMESTAMPTZ", []tree.Datum{tree.NewDInt(1234), tree.DNull}, nil},
		{"i INT PRIMARY KEY, b INTERVAL", []tree.Datum{tree.NewDInt(1234), tree.DNull}, nil},
	}
	for i, tc := range testCases {
		tableName := fmt.Sprintf("t1%d", i)
		tableDef := fmt.Sprintf("CREATE TABLE %s ("+tc.cols+")", tableName)
		r := sqlutils.MakeSQLRunner(db)
		r.Exec(t, tableDef)
		for _, s := range tc.extraDDL {
			s = fmt.Sprintf(s, tableName)
			r.Exec(t, s)
		}
		desc := desctestutils.TestingGetTableDescriptor(
			kvdb, codec, "defaultdb", "public", tableName)
		runComparison(t, desc, []tree.Datums{tc.datums}, tableDef, sv, codec)
	}
	// Now test these schemas with bunch of rows of rand datums
	for i, tc := range testCases {
		tableName := fmt.Sprintf("t2%d", i)
		tableDef := fmt.Sprintf("CREATE TABLE %s ("+tc.cols+")", tableName)
		r := sqlutils.MakeSQLRunner(db)
		r.Exec(t, tableDef)
		for _, s := range tc.extraDDL {
			s = fmt.Sprintf(s, tableName)
			r.Exec(t, s)
		}
		desc := desctestutils.TestingGetTableDescriptor(
			kvdb, codec, "defaultdb", "public", tableName)
		cols := desc.PublicColumns()
		datums := make([]tree.Datums, 100)
		for i := 0; i < len(datums); i++ {
			datums[i] = makeRow(rng, cols)
		}
		runComparison(t, desc, datums, tableDef, sv, codec)
	}
}

func TestEncoderEqualityRand(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 100; i++ {
		tableName := fmt.Sprintf("t%d", i)
		ct := randgen.RandCreateTableWithName(ctx, rng, tableName, i, randgen.TableOptNone)
		tableDef := tree.Serialize(ct)
		r := sqlutils.MakeSQLRunner(db)
		r.Exec(t, tableDef)
		desc := desctestutils.TestingGetTableDescriptor(
			kvdb, codec, "defaultdb", "public", tableName)
		cols := desc.WritableColumns()
		datums := make([]tree.Datums, 10)
		for i := 0; i < len(datums); i++ {
			datums[i] = makeRow(rng, cols)
		}
		runComparison(t, desc, datums, tableDef, sv, codec)
	}
}

// Interesting cases that arose from TestEncoderEqualityRand
func TestEncoderEqualityString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV

	for i, tc := range []struct {
		tableDef string
		strs     [][]string
	}{
		{
			`CREATE TABLE %s ("col1_%p0" UUID NOT NULL, col1_1 NAME NOT NULL, col1_2 OID, col1_3 REGNAMESPACE, col1_4 TIMETZ NOT NULL, " c%qol1_5" INET NULL, col1_6 TIMESTAMP NOT NULL, "co""l1_7" STRING NOT NULL, "col1_8" TIMESTAMP NOT NULL,
			col1_9 STRING NOT NULL, col1_10 TIMETZ NOT NULL, col1_11 STRING NOT NULL AS (lower(col1_1)) VIRTUAL, INDEX (col1_9, "co""l1_7", col1_4, " c%qol1_5" ASC, lower(col1_1), col1_3 ASC, col1_1 DESC, "col1_%p0" DESC, col1_2, "col1_8") STORING (col1_10), INDEX (col1_3) STORING ("col1_%p0", col1_2, col1_4, "co""l1_7", "col1_8") PARTITION BY LIST (col1_3) (PARTITION "t_part\\u1A610" VALUES IN ((0:::OID,), (450378638:::OID,), (2410246492:::OID,), (2240791775:::OID,), (3030354773:::OID,), (3170405976:::OID,), (3251775314:::OID,), (2950447600:::OID,), (3559114848:::OID,), (2775952537:::OID,)), PARTITION t_part1 VALUES IN ((1935904156:::OID,), (1163570843:::OID,), (1998420586:::OID,), (2278388050:::OID,), (2446211845:::OID,), (3435295479:::OID,), (2893510673:::OID,), (2644742458:::OID,), (2090628842:::OID,)), PARTITION t_part2 VALUES IN ((87330471:::OID,), (403262940:::OID,), (4095725716:::OID,), (3830229328:::OID,), (327736362:::OID,), (2577206186:::OID,), (3387146301:::OID,), (1949450190:::OID,), (1283612650:::OID,)), PARTITION "t_pa͖{r-t3" VALUES IN ((516296979:::OID,), (2281484377:::OID,), (806912586:::OID,), (4047357395:::OID,), (1418317903:::OID,), (3878293384:::OID,), (3630220576:::OID,), (3973335880:::OID,), (3205315203:::OID,), (2347143004:::OID,)), PARTITION "t_par't4" VALUES IN ((3655084477:::OID,), (2794985577:::OID,), (3570685391:::OID,), (1589510581:::OID,), (1336532745:::OID,), (4198862000:::OID,), (1043346335:::OID,), (2095414171:::OID,), (2507601232:::OID,)), PARTITION "DEFAULT" VALUES IN ((DEFAULT,))), INDEX (col1_2 DESC, col1_6 ASC, "col1_8", col1_1 ASC, "col1_%p0" DESC, "co""l1_7" DESC, col1_11 ASC, col1_3 DESC, col1_4 DESC, " c%qol1_5" DESC, col1_9 ASC, col1_10 DESC), INDEX (col1_6 DESC, col1_2 DESC, col1_9 ASC, col1_1 DESC, " c%qol1_5" DESC, "col1_%p0", col1_3 ASC, "col1_8" ASC, col1_10) STORING ("co""l1_7"), UNIQUE (col1_11 DESC, (CASE WHEN col1_3 IS NULL THEN e'\x0bP\x05&dv':::STRING ELSE e'A:SG(\x179':::STRING END) DESC, col1_1 DESC, "col1_%p0" DESC, "col1_8" ASC, col1_4 ASC, lower(CAST(col1_2 AS STRING))) STORING (col1_2, col1_3, " c%qol1_5", col1_6, "co""l1_7", col1_10),
			FAMILY ("col1_8"), FAMILY (" c%qol1_5"), FAMILY ("co""l1_7", "col1_%p0", col1_10), FAMILY (col1_2, col1_3, col1_6), FAMILY (col1_4, col1_1, col1_9))`, [][]string{
				{`'55c3002b-0a0e-4521-a3bb-9789efc4670e'`, `'Z<=$hxr'`, `41238650`, `2496362237`, `'13:15:03.982728-07:40'`, `NULL`, `'1976-03-25 22:48:24.000584'`, `'jjEc@Y'`, `'2032-08-22 20:10:52.000523'`, `'X'`, `'06:00:58.712954+13:02'`, `''`, `'"'`, `''`, `e'\x0bO'`, `-2970195760089706503`},
			},
		},
		{`CREATE TABLE %s ("coL1'0_0" FLOAT4, col10_1 TIMESTAMP NOT NULL, " col10_2" VARCHAR NOT NULL, col10_3 STRING NOT NULL AS (lower(" col10_2")) STORED, UNIQUE ("coL1'0_0" ASC) STORING (" col10_2", col10_3), UNIQUE ("coL1'0_0" ASC), FAMILY (col10_1), FAMILY (" col10_2"), FAMILY ("coL1'0_0"), FAMILY (col10_3))`,
			[][]string{
				{"-0.3569428026676178", "2016-05-08 23:51:15.000173", `e'@ UF\x0e9'`, `e'LD/\x10p'`, "4415568921600674112"},
			},
		},
		{`CREATE TABLE %s ("coL1'0_0" FLOAT4, col10_1 TIMESTAMP NOT NULL, " col10_2" VARCHAR NOT NULL, col10_3 STRING NOT NULL AS (lower(" col10_2")) STORED, UNIQUE ("coL1'0_0" ASC) STORING (" col10_2", col10_3), UNIQUE ("coL1'0_0" ASC), FAMILY (col10_1), FAMILY (" col10_2"), FAMILY ("coL1'0_0"), FAMILY (col10_3))`,
			[][]string{
				{"-0.3569428026676178", "2016-05-08 23:51:15.000173", `e'@ UF\x0e9'`, `e'LD/\x10p'`, "4415568921600674112"},
			},
		},
	} {
		tableName := fmt.Sprintf("t%d", i)
		r := sqlutils.MakeSQLRunner(db)
		tableDef := fmt.Sprintf(tc.tableDef, tableName)
		r.Exec(t, tableDef)
		desc := desctestutils.TestingGetTableDescriptor(
			kvdb, codec, "defaultdb", "public", tableName)
		var typs []*types.T
		cols := desc.PublicColumns()
		for _, c := range cols {
			typs = append(typs, c.GetType())
		}
		var datums []tree.Datums
		for _, row := range tc.strs {
			ds := make(tree.Datums, len(typs))
			for i, s := range row {
				d := tree.DNull
				var err error
				if s != "NULL" {
					// ParseAndRequireString doesn't like the single quotes a lot of datums include
					s = strings.Trim(s, "'")
					d, _, err = tree.ParseAndRequireString(typs[i], s, nil)
					require.NoError(t, err)
				}
				ds[i] = d
			}
			datums = append(datums, ds)
		}
		runComparison(t, desc, datums, tableDef, sv, codec)
	}
}

func TestErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "CREATE TABLE t (i int PRIMARY KEY, s STRING)")
	desc := desctestutils.TestingGetTableDescriptor(
		kvdb, codec, "defaultdb", "public", "t")
	enc := colenc.MakeEncoder(codec, desc, sv, nil, nil,
		nil /*metrics*/, nil /*partialIndexMap*/, func() error { return nil })
	err := enc.PrepareBatch(ctx, nil, 0, 0)
	require.Error(t, err)
	err = enc.PrepareBatch(ctx, nil, 1, 0)
	require.Error(t, err)

	_, err = buildVecKVs([]tree.Datums{{tree.DNull, tree.DNull}}, desc, desc.PublicColumns(), sv, codec)
	require.Error(t, err, `null value in column "i" violates not-null constraint`)

}

func TestColFamDropPKNot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "CREATE TABLE t (i int PRIMARY KEY, s STRING, FAMILY (s), FAMILY (i))")
	r.Exec(t, `INSERT INTO t VALUES (123,'asdf')`)
	r.Exec(t, `ALTER TABLE t DROP COLUMN s`)
	desc := desctestutils.TestingGetTableDescriptor(
		kvdb, codec, "defaultdb", "public", "t")

	datums := []tree.Datum{tree.NewDInt(321)}
	kvs1, err1 := buildRowKVs([]tree.Datums{datums}, desc, desc.PublicColumns(), sv, codec)
	require.NoError(t, err1)
	kvs2, err2 := buildVecKVs([]tree.Datums{datums}, desc, desc.PublicColumns(), sv, codec)
	require.NoError(t, err2)
	checkEqual(t, kvs1, kvs2)
}

func TestColFamilies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "CREATE TABLE t (id INT PRIMARY KEY, c1 INT NOT NULL, c2 INT NOT NULL, FAMILY cf1 (id, c1), FAMILY cf2(c2))")
	desc := desctestutils.TestingGetTableDescriptor(
		kvdb, codec, "defaultdb", "public", "t")

	row1 := []tree.Datum{tree.NewDInt(2), tree.NewDInt(1), tree.NewDInt(2)}
	row2 := []tree.Datum{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(1)}
	kvs1, err1 := buildRowKVs([]tree.Datums{row1, row2}, desc, desc.PublicColumns(), sv, codec)
	require.NoError(t, err1)
	kvs2, err2 := buildVecKVs([]tree.Datums{row1, row2}, desc, desc.PublicColumns(), sv, codec)
	require.NoError(t, err2)
	checkEqual(t, kvs1, kvs2)
}

// TestColIDToRowIndexNull tests case where insert cols is subset of public columns.
func TestColIDToRowIndexNull(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "CREATE TABLE t (i int PRIMARY KEY, s STRING)")
	r.Exec(t, `ALTER TABLE t DROP COLUMN s`)
	desc := desctestutils.TestingGetTableDescriptor(
		kvdb, codec, "defaultdb", "public", "t")
	datums := []tree.Datum{tree.NewDInt(321)}

	var cols []catalog.Column
	for _, col := range desc.PublicColumns() {
		if col.ColName() == tree.Name("i") {
			cols = append(cols, col)
		}
	}

	kvs1, err1 := buildRowKVs([]tree.Datums{datums}, desc, cols, sv, codec)
	require.NoError(t, err1)
	kvs2, err2 := buildVecKVs([]tree.Datums{datums}, desc, cols, sv, codec)
	require.NoError(t, err2)
	checkEqual(t, kvs1, kvs2)
}

func TestMissingNotNullCol(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "CREATE TABLE t (i int PRIMARY KEY, s STRING NOT NULL)")
	desc := desctestutils.TestingGetTableDescriptor(
		kvdb, codec, "defaultdb", "public", "t")
	datums := []tree.Datum{tree.NewDInt(321)}

	var cols []catalog.Column
	for _, col := range desc.PublicColumns() {
		if col.ColName() == tree.Name("i") {
			cols = append(cols, col)
		}
	}

	_, err1 := buildVecKVs([]tree.Datums{datums}, desc, cols, sv, codec)
	require.Error(t, err1, `null value in column "s" violates not-null constraint`)
}

func TestMemoryQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY,s string)")
	desc := desctestutils.TestingGetTableDescriptor(
		kvdb, codec, "defaultdb", "public", "t")
	factory := coldataext.NewExtendedColumnFactory(nil /*evalCtx */)
	numRows := 3
	cols := desc.PublicColumns()
	typs := make([]*types.T, len(cols))
	for i, col := range cols {
		typs[i] = col.GetType()
	}
	cb := coldata.NewMemBatchWithCapacity(typs, numRows, factory)
	txn := kvdb.NewTxn(ctx, t.Name())
	kvb := txn.NewBatch()
	enc := colenc.MakeEncoder(codec, desc, sv, cb, cols,
		nil /*metrics*/, nil /*partialIndexMap*/, func() error {
			if kvb.ApproximateMutationBytes() > 50 {
				return colenc.ErrOverMemLimit
			}
			return nil
		})
	pk := coldataext.MakeVecHandler(cb.ColVec(0))
	strcol := coldataext.MakeVecHandler(cb.ColVec(1))
	rng, _ := randutil.NewTestRand()
	for i := 0; i < numRows; i++ {
		pk.Int(int64(i))
		strcol.String(util.RandString(rng, 20, "asdf"))
	}
	cb.SetLength(numRows)

	p := &row.KVBatchAdapter{Batch: kvb}
	err := enc.PrepareBatch(ctx, p, 0, cb.Length())
	require.Equal(t, err, colenc.ErrOverMemLimit)

	for i := 0; i < numRows; i++ {
		*kvb = *txn.NewBatch()
		p := &row.KVBatchAdapter{Batch: kvb}
		err := enc.PrepareBatch(ctx, p, i, i+1)
		require.NoError(t, err)
	}
}

func TestCheckRowSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	codec, sv := s.ApplicationLayer().Codec(), &s.ApplicationLayer().ClusterSettings().SV
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `SET CLUSTER SETTING sql.guardrails.max_row_size_err = '2KiB'`)
	r.Exec(t, "CREATE TABLE t (i int PRIMARY KEY, s STRING)")
	desc := desctestutils.TestingGetTableDescriptor(
		kvdb, codec, "defaultdb", "public", "t")
	rng, _ := randutil.NewTestRand()
	datums := []tree.Datum{tree.NewDInt(1234), tree.NewDString(randutil.RandString(rng, 3<<10, "asdf"))}
	_, err1 := buildRowKVs([]tree.Datums{datums}, desc, desc.PublicColumns(), sv, codec)
	code1 := pgerror.GetPGCodeInternal(err1, pgerror.ComputeDefaultCode)
	require.Equal(t, pgcode.ProgramLimitExceeded, code1)
	_, err2 := buildVecKVs([]tree.Datums{datums}, desc, desc.PublicColumns(), sv, codec)
	code2 := pgerror.GetPGCodeInternal(err2, pgerror.ComputeDefaultCode)
	require.Equal(t, pgcode.ProgramLimitExceeded, code2)
}

// runComparison compares row and vector output and prints out a test case
// suitable for TestEncoderEqualityString if it fails.
func runComparison(
	t *testing.T,
	desc catalog.TableDescriptor,
	rows []tree.Datums,
	tableDef string,
	sv *settings.Values,
	codec keys.SQLCodec,
) {
	rowKVs, err := buildRowKVs(rows, desc, desc.PublicColumns(), sv, codec)
	require.NoError(t, err)
	vecKVs, err := buildVecKVs(rows, desc, desc.PublicColumns(), sv, codec)
	require.NoError(t, err)
	if eq := checkEqual(t, rowKVs, vecKVs); !eq {
		var sb strings.Builder
		for _, ds := range rows {
			sb.WriteString("{")
			for _, d := range ds {
				sb.WriteString(fmt.Sprintf("`%s`,", d.String()))
			}
			sb.WriteString("},\n")
		}
		t.Fatalf("{`%s`,\n\t[][]string{\n%s},\n},", tableDef, sb.String())
	}
}

func checkEqual(t *testing.T, rowKVs, vecKVs kvs) bool {
	result := true
	require.Equal(t, len(rowKVs.keys), len(rowKVs.values))
	require.Equal(t, len(vecKVs.keys), len(vecKVs.values))
	if len(rowKVs.keys) != len(vecKVs.keys) {
		t.Logf("row keys(%d):\n%s\nvec keys(%d):\n%s\n", len(rowKVs.keys), printKeys(rowKVs.keys), len(vecKVs.keys), printKeys(vecKVs.keys))
		result = false
	}
	if len(rowKVs.values) != len(vecKVs.values) {
		t.Logf("row keys(%d):\n%srow values(%d):\n%s\nvec keys:\n%s\nvec values:\n%s\n", len(rowKVs.keys), printKeys(rowKVs.keys), len(rowKVs.values), printVals(rowKVs.values), printKeys(vecKVs.keys), printVals(vecKVs.values))
		result = false
	}
	for i, k1 := range rowKVs.keys {
		var k2 roachpb.Key
		if len(vecKVs.keys) > i {
			k2 = vecKVs.keys[i]
			if !bytes.Equal(k1, k2) {
				t.Logf("key[%d]:\n%s\n didn't equal: \n%s", i, catalogkeys.PrettyKey(nil, k1, 0), catalogkeys.PrettyKey(nil, k2, 0))
				result = false
			}
		}
		if len(vecKVs.values) > i {
			v1, v2 := rowKVs.values[i], vecKVs.values[i]
			if !bytes.Equal(v1, v2) {
				var val1, val2 roachpb.Value
				val1.RawBytes = v1
				val2.RawBytes = v2
				t.Logf("value[%d]:\n%s key(%s)\n didn't equal: \n%s key(%s)", i, val1.PrettyPrint(), catalogkeys.PrettyKey(nil, k1, 0),
					val2.PrettyPrint(), catalogkeys.PrettyKey(nil, k2, 0))
				result = false
			}
		}
	}
	return result
}

func buildRowKVs(
	datums []tree.Datums,
	desc catalog.TableDescriptor,
	cols []catalog.Column,
	sv *settings.Values,
	codec keys.SQLCodec,
) (kvs, error) {
	inserter, err := row.MakeInserter(context.Background(), nil /*txn*/, codec, desc, nil /* uniqueWithTombstoneIndexes */, cols, nil, sv, false, nil)
	if err != nil {
		return kvs{}, err
	}
	p := &capturePutter{}
	var pm row.PartialIndexUpdateHelper
	for _, d := range datums {
		if err := inserter.InsertRow(context.Background(), p, d, pm, nil, row.CPutOp, true /* traceKV */); err != nil {
			return kvs{}, err
		}
	}
	// Sort the KVs for easy comparison, row and vector approaches result in
	// different order.
	sort.Sort(&p.kvs)
	return p.kvs, nil
}

func buildVecKVs(
	datums []tree.Datums,
	desc catalog.TableDescriptor,
	cols []catalog.Column,
	sv *settings.Values,
	codec keys.SQLCodec,
) (kvs, error) {
	p := &capturePutter{}
	typs := make([]*types.T, len(cols))
	for i, c := range cols {
		typs[i] = c.GetType()
	}
	factory := coldataext.NewExtendedColumnFactory(nil /*evalCtx */)
	b := coldata.NewMemBatchWithCapacity(typs, len(datums), factory)

	for row, d := range datums {
		for col, t := range typs {
			converter := colconv.GetDatumToPhysicalFn(t)
			if d[col] != tree.DNull {
				coldata.SetValueAt(b.ColVec(col), converter(d[col]), row)
			} else {
				b.ColVec(col).Nulls().SetNull(row)
			}
		}
	}
	b.SetLength(len(datums))

	be := colenc.MakeEncoder(codec, desc, sv, b, cols, nil /*metrics*/, nil, /*partialIndexMap*/
		func() error { return nil })
	rng, _ := randutil.NewTestRand()
	if b.Length() > 1 && rng.Intn(2) == 0 {
		for i := 0; i < len(datums); i++ {
			// Use row by row to test start/end correctness.
			if err := be.PrepareBatch(context.Background(), p, i, i+1); err != nil {
				return kvs{}, err
			}
		}
	} else {
		if err := be.PrepareBatch(context.Background(), p, 0, len(datums)); err != nil {
			return kvs{}, err
		}
	}
	// Sort the KVs for easy comparison, row and vector approaches result in
	// different order.
	sort.Sort(&p.kvs)
	return p.kvs, nil
}

func printVals(vals [][]byte) string {
	var buf strings.Builder
	var v roachpb.Value
	for i := range vals {
		v.RawBytes = vals[i]
		buf.WriteString(v.PrettyPrint())
		buf.WriteByte('\n')
	}
	return buf.String()
}

func printKeys(kys []roachpb.Key) string {
	var buf strings.Builder
	for _, k := range kys {
		buf.WriteString(k.String())
		buf.WriteByte('\n')
	}
	return buf.String()
}

type capturePutter struct {
	kvs kvs
}

var _ row.Putter = &capturePutter{}

func copyBytes(k []byte) []byte {
	cpy := make(roachpb.Key, len(k))
	copy(cpy, k)
	return cpy
}

func (c *capturePutter) CPut(key, value interface{}, expValue []byte) {
	if expValue != nil {
		colexecerror.InternalError(errors.New("expValue unexpected"))
	}
	k := key.(*roachpb.Key)
	c.kvs.keys = append(c.kvs.keys, *k)
	v := value.(*roachpb.Value)
	c.kvs.values = append(c.kvs.values, copyBytes(v.RawBytes))
}

func (c *capturePutter) CPutWithOriginTimestamp(
	key, value interface{}, expValue []byte, ts hlc.Timestamp, shouldWinTie bool,
) {
	colexecerror.InternalError(errors.New("unimplemented"))
}

func (c *capturePutter) Put(key, value interface{}) {
	k := key.(*roachpb.Key)
	c.kvs.keys = append(c.kvs.keys, copyBytes(*k))
	v := value.(*roachpb.Value)
	c.kvs.values = append(c.kvs.values, copyBytes(v.RawBytes))
}

func (c *capturePutter) PutMustAcquireExclusiveLock(key, value interface{}) {
	colexecerror.InternalError(errors.New("unimplemented"))
}

func (c *capturePutter) Del(key ...interface{}) {
	colexecerror.InternalError(errors.New("unimplemented"))
}

func (c *capturePutter) CPutBytesEmpty(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if len(k) == 0 {
			continue
		}
		c.kvs.keys = append(c.kvs.keys, k)
		var kvValue roachpb.Value
		kvValue.SetBytes(values[i])
		c.kvs.values = append(c.kvs.values, kvValue.RawBytes)
	}
}

func (c *capturePutter) CPutTuplesEmpty(kys []roachpb.Key, values [][]byte) {
	for i, k := range kys {
		if len(k) == 0 {
			continue
		}
		c.kvs.keys = append(c.kvs.keys, k)
		var kvValue roachpb.Value
		kvValue.SetTuple(values[i])
		c.kvs.values = append(c.kvs.values, kvValue.RawBytes)
	}
}

func (c *capturePutter) CPutValuesEmpty(kys []roachpb.Key, values []roachpb.Value) {
	for i, k := range kys {
		if len(k) == 0 {
			continue
		}
		c.kvs.keys = append(c.kvs.keys, k)
		c.kvs.values = append(c.kvs.values, copyBytes(values[i].RawBytes))
	}
}

// we don't call this
func (c *capturePutter) PutBytes(kys []roachpb.Key, values [][]byte) {
	colexecerror.InternalError(errors.New("unimplemented"))
}

// we don't call this
func (c *capturePutter) PutTuples(kys []roachpb.Key, values [][]byte) {
	colexecerror.InternalError(errors.New("unimplemented"))
}

type kvs struct {
	keys   []roachpb.Key
	values [][]byte
}

var _ sort.Interface = &kvs{}

func (k *kvs) Len() int {
	return len(k.keys)
}

func (k *kvs) Less(i, j int) bool {
	cmp := bytes.Compare(k.keys[i], k.keys[j])
	if cmp == 0 {
		cmp = bytes.Compare(k.values[i], k.values[j])
	}
	return cmp < 0
}

func (k *kvs) Swap(i, j int) {
	k.keys[i], k.keys[j] = k.keys[j], k.keys[i]
	k.values[i], k.values[j] = k.values[j], k.values[i]
}

func makeRow(rng *rand.Rand, cols []catalog.Column) tree.Datums {
	datums := make(tree.Datums, len(cols))
	for i, col := range cols {
		datums[i] = randgen.RandDatum(rng, col.GetType(), col.IsNullable())
	}
	return datums
}
