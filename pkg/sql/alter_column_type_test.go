// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// testKey exists because sqlbase.ColumnType isn't map-compatible
type testKey struct {
	SemanticType sqlbase.ColumnType_SemanticType
	Width        int32
	Precision    int32
}

func (t *testKey) toColumnType() *sqlbase.ColumnType {
	return &sqlbase.ColumnType{
		Precision:    t.Precision,
		SemanticType: t.SemanticType,
		Width:        t.Width,
	}
}

type testMap map[testKey]map[testKey]columnConversionKind

// columnConversionInfo is where we document conversions that
// don't require a fully-generalized conversion path or where there are
// restrictions on conversions that seem non-obvious at first glance.
var columnConversionInfo testMap = map[testKey]map[testKey]columnConversionKind{
	{SemanticType: sqlbase.ColumnType_BYTES}: {
		{SemanticType: sqlbase.ColumnType_STRING}:            columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_STRING, Width: 20}: columnConversionValidate,
	},
	{SemanticType: sqlbase.ColumnType_BYTES, Width: 20}: {
		{SemanticType: sqlbase.ColumnType_BYTES, Width: 10}: columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_BYTES, Width: 30}: columnConversionTrivial,

		{SemanticType: sqlbase.ColumnType_STRING}:           columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_STRING, Width: 4}: columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_STRING, Width: 5}: columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_STRING, Width: 6}: columnConversionValidate,
	},

	{SemanticType: sqlbase.ColumnType_DECIMAL, Width: 4}: {
		{SemanticType: sqlbase.ColumnType_DECIMAL, Width: 8}: columnConversionTrivial,
	},

	{SemanticType: sqlbase.ColumnType_FLOAT, Width: 4}: {
		{SemanticType: sqlbase.ColumnType_FLOAT, Width: 2}: columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_FLOAT, Width: 8}: columnConversionTrivial,
	},
	{SemanticType: sqlbase.ColumnType_FLOAT, Precision: 4}: {
		{SemanticType: sqlbase.ColumnType_FLOAT, Precision: 2}: columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_FLOAT, Precision: 8}: columnConversionTrivial,
	},
	{SemanticType: sqlbase.ColumnType_FLOAT, Width: 4, Precision: 4}: {
		{SemanticType: sqlbase.ColumnType_FLOAT, Width: 2, Precision: 2}: columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_FLOAT, Width: 8, Precision: 8}: columnConversionTrivial,
	},

	{SemanticType: sqlbase.ColumnType_INET}: {
		// This doesn't have an "obvious" conversion to bytes since it's
		// encoded as a netmask length, followed by the actual address bytes.
		{SemanticType: sqlbase.ColumnType_BYTES}: columnConversionImpossible,
	},

	{SemanticType: sqlbase.ColumnType_INT}: {
		{SemanticType: sqlbase.ColumnType_INT, Width: 4}: columnConversionValidate,
	},
	{SemanticType: sqlbase.ColumnType_INT, Width: 4}: {
		{SemanticType: sqlbase.ColumnType_INT, Width: 2}: columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_INT, Width: 8}: columnConversionTrivial,
	},

	{SemanticType: sqlbase.ColumnType_STRING}: {
		{SemanticType: sqlbase.ColumnType_BYTES}:            columnConversionTrivial,
		{SemanticType: sqlbase.ColumnType_BYTES, Width: 20}: columnConversionValidate,
	},
	{SemanticType: sqlbase.ColumnType_STRING, Width: 5}: {
		{SemanticType: sqlbase.ColumnType_BYTES}:            columnConversionTrivial,
		{SemanticType: sqlbase.ColumnType_BYTES, Width: 19}: columnConversionValidate,
		{SemanticType: sqlbase.ColumnType_BYTES, Width: 20}: columnConversionTrivial,
	},

	{SemanticType: sqlbase.ColumnType_TIME}: {
		{SemanticType: sqlbase.ColumnType_TIMETZ}: columnConversionTrivial,
	},
	{SemanticType: sqlbase.ColumnType_TIMETZ}: {
		{SemanticType: sqlbase.ColumnType_TIME}: columnConversionTrivial,
	},

	{SemanticType: sqlbase.ColumnType_TIMESTAMP}: {
		{SemanticType: sqlbase.ColumnType_TIMESTAMPTZ}: columnConversionTrivial,
	},
	{SemanticType: sqlbase.ColumnType_TIMESTAMPTZ}: {
		{SemanticType: sqlbase.ColumnType_TIMESTAMP}: columnConversionTrivial,
	},

	{SemanticType: sqlbase.ColumnType_UUID}: {
		{SemanticType: sqlbase.ColumnType_BYTES}: columnConversionGeneral,
	},
}

// TestTrivialColumnConversions will run a test for each
// conversion documented in columnConversionInfo.
func TestColumnConversions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for from, mid := range columnConversionInfo {
		for to, kind := range mid {
			if kind != columnConversionTrivial {
				continue
			}

			t.Run(fmt.Sprint(from.SemanticType, "->", to.SemanticType), func(t *testing.T) {
				sqlDB.Exec(t, "CREATE DATABASE d")
				defer sqlDB.Exec(t, "DROP DATABASE d")

				sqlDB.Exec(t, fmt.Sprintf(
					"CREATE TABLE d.t (i int primary key, a %s)", from.toColumnType().SQLString()))

				// We're just going to use an ugly, two-dimensional switch
				// structure here to establish some values that we want to
				// test from and various values we expect to be able to
				// convert to.
				var insert []interface{}
				var expect []interface{}

				tx, err := sqlDB.DB.Begin()
				if err != nil {
					t.Fatal(err)
				}

				switch from.SemanticType {
				case sqlbase.ColumnType_BYTES:
					insert = []interface{}{[]uint8{}, []uint8("data")}
					switch to.SemanticType {
					case sqlbase.ColumnType_BYTES:
						expect = insert
					}

				case sqlbase.ColumnType_DECIMAL:
					insert = []interface{}{"-112358", "112358"}
					switch to.SemanticType {
					case sqlbase.ColumnType_DECIMAL:
						// We're going to see decimals returned as strings
						expect = []interface{}{[]uint8("-112358"), []uint8("112358")}
					}

				case sqlbase.ColumnType_FLOAT:
					insert = []interface{}{-1.2, 0.0, 1.2}
					switch to.SemanticType {
					case sqlbase.ColumnType_FLOAT:
						expect = insert
					}

				case sqlbase.ColumnType_INT:
					insert = []interface{}{int64(math.MinInt64), int64(-1), int64(0), int64(1), int64(math.MaxInt64)}
					switch to.SemanticType {
					case sqlbase.ColumnType_INT:
						expect = insert
					}

				case sqlbase.ColumnType_STRING:
					insert = []interface{}{"", "text", "✈"}
					switch to.SemanticType {
					case sqlbase.ColumnType_STRING:
						expect = []interface{}{"", "text"}
					case sqlbase.ColumnType_BYTES:
						expect = []interface{}{[]uint8{}, []uint8("text"), []uint8{0xE2, 0x9C, 0x88}}
					}

				case sqlbase.ColumnType_TIME,
					sqlbase.ColumnType_TIMETZ,
					sqlbase.ColumnType_TIMESTAMP,
					sqlbase.ColumnType_TIMESTAMPTZ:

					const timeOnly = "15:04:05"
					const timeTZ = "15:04:05 -0700"
					const noZone = "2006-01-02 15:04:05"
					const withZone = "2006-01-02 15:04:05 -0700"
					var fromFmt string
					switch from.SemanticType {
					case sqlbase.ColumnType_TIME:
						fromFmt = timeOnly
					case sqlbase.ColumnType_TIMETZ:
						fromFmt = timeTZ
					case sqlbase.ColumnType_TIMESTAMP:
						fromFmt = noZone
					case sqlbase.ColumnType_TIMESTAMPTZ:
						fromFmt = withZone
					}

					// Always use a non-UTC zone for this test
					const tz = "America/New_York"
					sqlDB.Exec(t, fmt.Sprintf("SET SESSION TIME ZONE '%s'", tz))
					loc, err := time.LoadLocation(tz)
					if err != nil {
						t.Fatal(err)
					}
					now := timeutil.Now().In(loc).Format(fromFmt)
					insert = []interface{}{now}

					switch to.SemanticType {
					case
						sqlbase.ColumnType_TIME,
						sqlbase.ColumnType_TIMETZ,
						sqlbase.ColumnType_TIMESTAMP,
						sqlbase.ColumnType_TIMESTAMPTZ:
						// We're going to re-parse the text as though we're in UTC
						// so that we can drop the TZ info.
						if parsed, err := time.ParseInLocation(fromFmt, now, time.UTC); err == nil {
							expect = []interface{}{parsed}
						} else {
							t.Fatal(err)
						}
					}

				case sqlbase.ColumnType_UUID:
					u := uuid.MakeV4()
					insert = []interface{}{u}
					switch to.SemanticType {
					case sqlbase.ColumnType_BYTES:
						expect = []interface{}{u.GetBytes()}
					}

				default:
					t.Fatalf("don't know how to create initial value for %s", from.SemanticType)
				}
				if expect == nil {
					t.Fatalf("expect variable not initialized for %s -> %s", from.SemanticType, to.SemanticType)
				}

				for i, v := range insert {
					sqlDB.Exec(t, "INSERT INTO d.t (i, a) VALUES ($1, $2)", i, v)
				}

				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}

				sqlDB.Exec(t, fmt.Sprintf(
					"ALTER TABLE d.t ALTER COLUMN a SET DATA TYPE %s", to.toColumnType().SQLString()))

				rows := sqlDB.Query(t, "SELECT a FROM d.t")
				defer rows.Close()

				count := 0
				for rows.Next() {
					var actual interface{}
					if err := rows.Scan(&actual); err != nil {
						t.Fatal(err)
					}
					expected := expect[count]

					var success bool
					switch e := expected.(type) {
					case time.Time:
						success = e.Equal(actual.(time.Time))
					default:
						success = reflect.DeepEqual(e, actual)
					}

					if !success {
						t.Errorf("unexpected value found at row %d: %v (%s) vs %v (%s)",
							count, expected, reflect.TypeOf(expected), actual, reflect.TypeOf(actual))
					}
					count++
				}
				if count != len(expect) {
					t.Errorf("expecting %d rows, found %d", len(expect), count)
				}
			})
		}
	}
}

// TestColumnConversionSanity verifies columnConversionInfo.
func TestColumnConversionSanity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for from, mid := range columnConversionInfo {
		for to, expected := range mid {
			actual, err := classifyConversion(from.toColumnType(), to.toColumnType())
			t.Log(from, to, actual)

			// Verify that we only return cannot-coerce errors.
			if err != nil {
				if pgErr, ok := err.(*pgerror.Error); ok {
					if pgErr.Code != pgerror.CodeCannotCoerceError {
						t.Errorf("unexpected error code returned: %s", pgErr.Code)
					}
				} else {
					t.Errorf("unexpected error returned: %s", err)
				}
			}

			if actual != expected {
				t.Errorf("%v -> %v returned %s, expecting %s", from, to, actual, expected)
			}
		}
	}
}
