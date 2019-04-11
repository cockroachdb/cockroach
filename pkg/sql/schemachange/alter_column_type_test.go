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

package schemachange

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/lib/pq/oid"
)

// TestColumnConversions rolls-up a lot of test plumbing to prevent
// top-level namespace pollution.
func TestColumnConversions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// testKey exists because types.ColumnType isn't map-compatible
	type testKey struct {
		SemanticType types.SemanticType
		Width        int32
		Precision    int32
		Oid          oid.Oid
	}

	columnType := func(t testKey) *types.ColumnType {
		return &types.ColumnType{
			Precision:    t.Precision,
			SemanticType: t.SemanticType,
			Width:        t.Width,
			ZZZ_Oid:      t.Oid,
		}
	}

	// columnConversionInfo is where we document conversions that
	// don't require a fully-generalized conversion path or where there are
	// restrictions on conversions that seem non-obvious at first glance.
	columnConversionInfo := map[testKey]map[testKey]ColumnConversionKind{
		{SemanticType: types.BYTES}: {
			{SemanticType: types.STRING}:            ColumnConversionValidate,
			{SemanticType: types.STRING, Width: 20}: ColumnConversionValidate,
		},
		{SemanticType: types.BYTES, Width: 20}: {
			{SemanticType: types.BYTES}:            ColumnConversionTrivial,
			{SemanticType: types.BYTES, Width: 10}: ColumnConversionValidate,
			{SemanticType: types.BYTES, Width: 20}: ColumnConversionTrivial,
			{SemanticType: types.BYTES, Width: 30}: ColumnConversionTrivial,

			{SemanticType: types.STRING}:           ColumnConversionValidate,
			{SemanticType: types.STRING, Width: 4}: ColumnConversionValidate,
			{SemanticType: types.STRING, Width: 5}: ColumnConversionValidate,
			{SemanticType: types.STRING, Width: 6}: ColumnConversionValidate,
		},

		{SemanticType: types.DECIMAL, Width: 4}: {
			{SemanticType: types.DECIMAL, Width: 8}: ColumnConversionTrivial,
		},

		{SemanticType: types.FLOAT, Width: 4}: {
			{SemanticType: types.FLOAT, Width: 2}: ColumnConversionTrivial,
			{SemanticType: types.FLOAT, Width: 8}: ColumnConversionTrivial,
		},
		{SemanticType: types.FLOAT, Precision: 4}: {
			{SemanticType: types.FLOAT, Precision: 2}: ColumnConversionTrivial,
			{SemanticType: types.FLOAT, Precision: 8}: ColumnConversionTrivial,
		},
		{SemanticType: types.FLOAT, Width: 4, Precision: 4}: {
			{SemanticType: types.FLOAT, Width: 2, Precision: 2}: ColumnConversionTrivial,
			{SemanticType: types.FLOAT, Width: 8, Precision: 8}: ColumnConversionTrivial,
		},

		{SemanticType: types.INET}: {
			// This doesn't have an "obvious" conversion to bytes since it's
			// encoded as a netmask length, followed by the actual address bytes.
			{SemanticType: types.BYTES}: ColumnConversionImpossible,
		},

		{SemanticType: types.INT, Width: 64}: {
			{
				SemanticType: types.INT,
				Width:        64,
			}: ColumnConversionTrivial,
			{
				SemanticType: types.INT,
				Width:        32,
				Oid:          oid.T_int4,
			}: ColumnConversionValidate,
			{SemanticType: types.BIT}: ColumnConversionGeneral,
		},
		{SemanticType: types.INT, Width: 32}: {
			{
				SemanticType: types.INT,
				Width:        16,
				Oid:          oid.T_int2,
			}: ColumnConversionValidate,
			{
				SemanticType: types.INT,
				Width:        64,
			}: ColumnConversionTrivial,
		},

		{SemanticType: types.BIT}: {
			{SemanticType: types.INT}:           ColumnConversionGeneral,
			{SemanticType: types.STRING}:        ColumnConversionGeneral,
			{SemanticType: types.BYTES}:         ColumnConversionImpossible,
			{SemanticType: types.BIT, Width: 4}: ColumnConversionValidate,
		},
		{SemanticType: types.BIT, Width: 4}: {
			{SemanticType: types.BIT, Width: 2}: ColumnConversionValidate,
			{SemanticType: types.BIT, Width: 8}: ColumnConversionTrivial,
		},

		{SemanticType: types.STRING}: {
			{SemanticType: types.BIT}:              ColumnConversionGeneral,
			{SemanticType: types.BYTES}:            ColumnConversionTrivial,
			{SemanticType: types.BYTES, Width: 20}: ColumnConversionValidate,
		},
		{SemanticType: types.STRING, Width: 5}: {
			{SemanticType: types.BYTES}:            ColumnConversionTrivial,
			{SemanticType: types.BYTES, Width: 19}: ColumnConversionValidate,
			{SemanticType: types.BYTES, Width: 20}: ColumnConversionTrivial,
		},
		{SemanticType: types.TIMESTAMP}: {
			{SemanticType: types.TIMESTAMPTZ}: ColumnConversionTrivial,
		},
		{SemanticType: types.TIMESTAMPTZ}: {
			{SemanticType: types.TIMESTAMP}: ColumnConversionTrivial,
		},

		{SemanticType: types.UUID}: {
			{SemanticType: types.BYTES}: ColumnConversionGeneral,
		},
	}

	// Verify that columnConversionInfo does what it says.
	t.Run("columnConversionInfo sanity", func(t *testing.T) {
		for from, mid := range columnConversionInfo {
			for to, expected := range mid {
				actual, err := ClassifyConversion(columnType(from), columnType(to))

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
				} else {
					t.Log(from, to, actual)
				}
			}
		}
	})

	t.Run("column conversion checks", func(t *testing.T) {
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(context.Background())
		sqlDB := sqlutils.MakeSQLRunner(db)

		for from, mid := range columnConversionInfo {
			for to, kind := range mid {
				// TODO(bob): Generalize in the next pass.
				if kind != ColumnConversionTrivial {
					continue
				}

				t.Run(fmt.Sprint(from.SemanticType, "->", to.SemanticType), func(t *testing.T) {
					sqlDB.Exec(t, "CREATE DATABASE d")
					defer sqlDB.Exec(t, "DROP DATABASE d")

					sqlDB.Exec(t, fmt.Sprintf(
						"CREATE TABLE d.t (i int8 primary key, a %s)", columnType(from).SQLString()))

					// We're just going to use an ugly, two-dimensional switch
					// structure here to establish some values that we want to
					// test from and various values we expect to be able to
					// convert to.
					var insert []interface{}
					var expect []interface{}

					switch from.SemanticType {
					case types.BYTES:
						insert = []interface{}{[]uint8{}, []uint8("data")}
						switch to.SemanticType {
						case types.BYTES:
							expect = insert
						}

					case types.BIT:
						switch from.Width {
						case 4:
							insert = []interface{}{[]uint8("0110")}
						case 0:
							insert = []interface{}{[]uint8("110"), []uint8("000110")}
						}
						switch to.SemanticType {
						case types.BIT:
							expect = insert
						}

					case types.DECIMAL:
						insert = []interface{}{"-112358", "112358"}
						switch to.SemanticType {
						case types.DECIMAL:
							// We're going to see decimals returned as strings
							expect = []interface{}{[]uint8("-112358"), []uint8("112358")}
						}

					case types.FLOAT:
						insert = []interface{}{-1.2, 0.0, 1.2}
						switch to.SemanticType {
						case types.FLOAT:
							expect = insert
						}

					case types.INT:
						insert = []interface{}{int64(-1), int64(0), int64(1)}
						switch from.Width {
						case 0, 64:
							insert = append(insert, int64(math.MinInt64), int64(math.MaxInt64))
						case 32:
							insert = append(insert, int64(math.MinInt32), int64(math.MaxInt32))
						case 16:
							insert = append(insert, int64(math.MinInt16), int64(math.MaxInt16))
						}
						switch to.SemanticType {
						case types.INT:
							expect = insert
						}

					case types.STRING:
						insert = []interface{}{"", "text", "✈"}
						switch to.SemanticType {
						case types.STRING:
							expect = []interface{}{"", "text"}
						case types.BYTES:
							expect = []interface{}{[]uint8{}, []uint8("text"), []uint8{0xE2, 0x9C, 0x88}}
						}

					case types.TIME,
						types.TIMESTAMP,
						types.TIMESTAMPTZ:

						const timeOnly = "15:04:05"
						const noZone = "2006-01-02 15:04:05"
						const withZone = "2006-01-02 15:04:05 -0700"

						var fromFmt string
						switch from.SemanticType {
						case types.TIME:
							fromFmt = timeOnly
						case types.TIMESTAMP:
							fromFmt = noZone
						case types.TIMESTAMPTZ:
							fromFmt = withZone
						}

						// Always use a non-UTC zone for this test
						const tz = "America/New_York"
						sqlDB.Exec(t, fmt.Sprintf("SET SESSION TIME ZONE '%s'", tz))

						// Use a fixed time so we don't experience weirdness when
						// testing TIME values if the local time would roll over
						// a day boundary when moving between Eastern and UTC.
						// We can re-use the format strings as test data.
						now := fromFmt
						insert = []interface{}{now}

						switch to.SemanticType {
						case
							types.TIME,
							types.TIMESTAMP,
							types.TIMESTAMPTZ:
							// We're going to re-parse the text as though we're in UTC
							// so that we can drop the TZ info.
							if parsed, err := time.ParseInLocation(fromFmt, now, time.UTC); err == nil {
								expect = []interface{}{parsed}
							} else {
								t.Fatal(err)
							}
						}

					case types.UUID:
						u := uuid.MakeV4()
						insert = []interface{}{u}
						switch to.SemanticType {
						case types.BYTES:
							expect = []interface{}{u.GetBytes()}
						}

					default:
						t.Fatalf("don't know how to create initial value for %s", from.SemanticType)
					}
					if expect == nil {
						t.Fatalf("expect variable not initialized for %s -> %s", from.SemanticType, to.SemanticType)
					}

					// Insert the test data.
					if tx, err := db.Begin(); err != nil {
						t.Fatal(err)
					} else {
						for i, v := range insert {
							sqlDB.Exec(t, "INSERT INTO d.t (i, a) VALUES ($1, $2)", i, v)
						}

						if err := tx.Commit(); err != nil {
							t.Fatal(err)
						}
					}

					findColumn := func(colType testKey) bool {
						var a, expr string
						lookFor := fmt.Sprintf("a %s NULL,", columnType(colType).SQLString())
						sqlDB.QueryRow(t, "SHOW CREATE d.t").Scan(&a, &expr)
						t.Log(lookFor, expr)
						return strings.Contains(expr, lookFor)
					}

					// Sanity-check that our findColumn is working.
					if !findColumn(from) {
						t.Fatal("could not find source column")
					}

					// Execute the schema change.
					sqlDB.Exec(t, fmt.Sprintf(
						"ALTER TABLE d.t ALTER COLUMN a SET DATA TYPE %s", columnType(to).SQLString()))

					// Verify that the column descriptor was updated.
					if !findColumn(to) {
						t.Fatal("could not find target column")
					}

					// Verify data conversions.
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
	})
}
