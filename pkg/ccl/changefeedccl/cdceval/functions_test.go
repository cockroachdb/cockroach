// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdceval

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestEvaluatesCDCFunctionOverloads(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, ""+
		"CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, d INT, "+
		"FAMILY most (a,b,c), FAMILY only_d (d))")
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")

	ctx := context.Background()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	defer configSemaForCDC(&semaCtx, hlc.Timestamp{})()

	t.Run("time", func(t *testing.T) {
		expectTSTZ := func(ts hlc.Timestamp) tree.Datum {
			t.Helper()
			d, err := tree.MakeDTimestampTZ(ts.GoTime(), time.Microsecond)
			require.NoError(t, err)
			return d
		}
		expectTS := func(ts hlc.Timestamp) tree.Datum {
			t.Helper()
			d, err := tree.MakeDTimestamp(ts.GoTime(), time.Microsecond)
			require.NoError(t, err)
			return d
		}
		expectHLC := func(ts hlc.Timestamp) tree.Datum {
			t.Helper()
			return eval.TimestampToDecimalDatum(ts)
		}

		type preferredFn func(ts hlc.Timestamp) tree.Datum
		for fn, preferredOverload := range map[string]preferredFn{
			"statement_timestamp":           expectTSTZ,
			"transaction_timestamp":         expectTSTZ,
			"event_schema_timestamp":        expectHLC,
			"changefeed_creation_timestamp": expectHLC,
		} {
			t.Run(fn, func(t *testing.T) {
				createTS := s.Clock().Now().Add(-int64(60*time.Minute), 0)
				schemaTS := s.Clock().Now()
				rowTS := schemaTS.Add(int64(60*time.Minute), 0)

				targetTS := rowTS
				switch fn {
				case "event_schema_timestamp":
					targetTS = schemaTS
				case "changefeed_creation_timestamp":
					targetTS = createTS
				}
				// We'll run tests against some future time stamp to ensure
				// that time functions use correct values.
				testRow := makeEventRow(t, desc, schemaTS, false, rowTS, false)
				e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, fmt.Sprintf("SELECT "+
					"%[1]s() AS preferred,"+ // Preferred overload.
					"%[1]s():::TIMESTAMPTZ  AS tstz,"+ // Force timestamptz overload.
					"%[1]s():::TIMESTAMP AS ts,"+ // Force timestamp overload.
					"%[1]s():::DECIMAL AS dec,"+ // Force decimal overload.
					"%[1]s()::STRING AS str"+ // Casts preferred overload to string.
					" FROM foo", fn))
				require.NoError(t, err)
				defer e.Close()
				e.statementTS = createTS

				p, err := e.Eval(ctx, testRow, cdcevent.Row{})
				require.NoError(t, err)

				initialExpectations := map[string]string{
					"preferred": tree.AsStringWithFlags(preferredOverload(targetTS), tree.FmtExport),
					"tstz":      tree.AsStringWithFlags(expectTSTZ(targetTS), tree.FmtExport),
					"ts":        tree.AsStringWithFlags(expectTS(targetTS), tree.FmtExport),
					"dec":       tree.AsStringWithFlags(eval.TimestampToDecimalDatum(targetTS), tree.FmtExport),
					"str":       tree.AsStringWithFlags(preferredOverload(targetTS), tree.FmtExport),
				}
				require.Equal(t, initialExpectations, slurpValues(t, p))

				// Modify row/schema timestamps, and evaluate again.
				testRow.MvccTimestamp = testRow.MvccTimestamp.Add(int64(time.Hour), 0)
				targetTS = testRow.MvccTimestamp
				testRow.SchemaTS = schemaTS.Add(1, 0)
				e.statementTS = e.statementTS.Add(-1, 0)
				p, err = e.Eval(ctx, testRow, cdcevent.Row{})
				require.NoError(t, err)

				var updatedExpectations map[string]string
				switch fn {
				case "changefeed_creation_timestamp":
					// this function is stable; So, advancing evaluator timestamp
					// should have no bearing on the returned values -- we should see
					// the same thing we saw before.
					updatedExpectations = initialExpectations
				case "event_schema_timestamp":
					targetTS = testRow.SchemaTS
					fallthrough
				default:
					updatedExpectations = map[string]string{
						"preferred": tree.AsStringWithFlags(preferredOverload(targetTS), tree.FmtExport),
						"tstz":      tree.AsStringWithFlags(expectTSTZ(targetTS), tree.FmtExport),
						"ts":        tree.AsStringWithFlags(expectTS(targetTS), tree.FmtExport),
						"dec":       tree.AsStringWithFlags(eval.TimestampToDecimalDatum(targetTS), tree.FmtExport),
						"str":       tree.AsStringWithFlags(preferredOverload(targetTS), tree.FmtExport),
					}
				}
				require.Equal(t, updatedExpectations, slurpValues(t, p))
			})
		}

		t.Run("timezone", func(t *testing.T) {
			// We'll run tests against some future time stamp to ensure
			// that time functions use correct values.
			futureTS := s.Clock().Now().Add(int64(60*time.Minute), 0)

			// Timezone has many overrides, some are immutable, and some are Stable.
			// Call "stable" overload which relies on session data containing
			// timezone. Since we don't do any special setup with session data, the
			// default timezone is UTC. We'll use a "strange" timezone of -1h33m from
			// UTC to test conversion.
			testRow := makeEventRow(t, desc, s.Clock().Now(), false, futureTS, false)
			e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, fmt.Sprintf("SELECT timezone('+01:33:00', '%s'::time) FROM foo",
				futureTS.GoTime().Format("15:04:05")))
			require.NoError(t, err)
			defer e.Close()

			p, err := e.Eval(ctx, testRow, cdcevent.Row{})
			require.NoError(t, err)

			expectedTZ := fmt.Sprintf("%s-01:33",
				futureTS.GoTime().Add(-93*time.Minute).Format("15:04:05"))
			require.Equal(t, map[string]string{"timezone": expectedTZ}, slurpValues(t, p))
		})
	})

	t.Run("event_op", func(t *testing.T) {
		schemaTS := s.Clock().Now()
		row := makeEventRow(t, desc, schemaTS, false, s.Clock().Now(), true)
		deletedRow := makeEventRow(t, desc, schemaTS, true, s.Clock().Now(), true)
		prevRow := makeEventRow(t, desc, schemaTS, false, s.Clock().Now(), false)
		nilRow := cdcevent.Row{}

		for _, tc := range []struct {
			op       string
			row      cdcevent.Row
			prevRow  cdcevent.Row
			withDiff bool
			expect   string
		}{
			{
				op:       "insert",
				row:      row,
				prevRow:  nilRow,
				withDiff: true,
				expect:   "insert",
			},
			{
				op:       "update",
				row:      row,
				prevRow:  prevRow,
				withDiff: true,
				expect:   "update",
			},
			{
				// Without diff, we can't tell an update from insert, so we emit upsert.
				op:       "insert",
				row:      row,
				prevRow:  nilRow,
				withDiff: false,
				expect:   "upsert",
			},
			{
				// Without diff, we can't tell an update from insert, so we emit upsert.
				op:       "update",
				row:      row,
				prevRow:  nilRow,
				withDiff: false,
				expect:   "upsert",
			},
			{
				op:       "delete",
				row:      deletedRow,
				prevRow:  prevRow,
				withDiff: true,
				expect:   "delete",
			},
			{
				op:       "delete",
				row:      deletedRow,
				prevRow:  prevRow,
				withDiff: false,
				expect:   "delete",
			},
		} {
			t.Run(fmt.Sprintf("%s/diff=%t", tc.op, tc.withDiff), func(t *testing.T) {
				e, err := newEvaluator(&execCfg, &semaCtx, tc.row.EventDescriptor, tc.withDiff, "SELECT event_op() FROM foo")
				require.NoError(t, err)
				defer e.Close()

				p, err := e.Eval(ctx, tc.row, tc.prevRow)
				require.NoError(t, err)
				require.Equal(t, map[string]string{"event_op": tc.expect}, slurpValues(t, p))
			})
		}
	})

	mustParseJSON := func(d tree.Datum) jsonb.JSON {
		t.Helper()
		j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		require.NoError(t, err)
		return j
	}

	t.Run("pg_collation_for", func(t *testing.T) {
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now(), false)

		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, `SELECT pg_collation_for('hello' COLLATE de_DE) AS col FROM foo`)
		require.NoError(t, err)
		defer e.Close()

		p, err := e.Eval(ctx, testRow, cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"col": "\"de_DE\""}, slurpValues(t, p))
	})

	for _, fn := range []string{"to_json", "to_jsonb"} {
		t.Run(fn, func(t *testing.T) {
			testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now(), false)
			rowDatums := testRow.EncDatums()

			e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, fmt.Sprintf("SELECT %s(a) FROM foo", fn))
			require.NoError(t, err)
			defer e.Close()

			p, err := e.Eval(ctx, testRow, cdcevent.Row{})
			require.NoError(t, err)
			require.Equal(t,
				map[string]string{fn: mustParseJSON(rowDatums[0].Datum).String()},
				slurpValues(t, p))
		})
	}

	t.Run("row_to_json", func(t *testing.T) {
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now(), false)

		rowDatums := testRow.EncDatums()
		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, "SELECT row_to_json(row(a, b, c)) FROM foo")
		require.NoError(t, err)
		defer e.Close()

		b := jsonb.NewObjectBuilder(len(rowDatums))
		for i, d := range rowDatums {
			b.Add(fmt.Sprintf("f%d", i+1), mustParseJSON(d.Datum))
		}
		expectedJSON := b.Build()

		p, err := e.Eval(ctx, testRow, cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"row_to_json": expectedJSON.String()}, slurpValues(t, p))
	})

	t.Run("jsonb_build_array", func(t *testing.T) {
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now(), false)
		rowDatums := testRow.EncDatums()
		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, "SELECT jsonb_build_array(a, a, 42) AS three_ints FROM foo")
		require.NoError(t, err)
		defer e.Close()

		b := jsonb.NewArrayBuilder(3)
		j := mustParseJSON(rowDatums[0].Datum)
		b.Add(j)
		b.Add(j)
		b.Add(jsonb.FromInt(42))
		expectedJSON := b.Build()

		p, err := e.Eval(ctx, testRow, cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"three_ints": expectedJSON.String()}, slurpValues(t, p))
	})

	t.Run("jsonb_build_object", func(t *testing.T) {
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now(), false)
		rowDatums := testRow.EncDatums()
		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, "SELECT jsonb_build_object('a', a, 'b', b, 'c', c) AS obj FROM foo")
		require.NoError(t, err)
		defer e.Close()

		b := jsonb.NewObjectBuilder(3)
		b.Add("a", mustParseJSON(rowDatums[0].Datum))
		b.Add("b", mustParseJSON(rowDatums[1].Datum))
		b.Add("c", mustParseJSON(rowDatums[2].Datum))
		expectedJSON := b.Build()

		p, err := e.Eval(ctx, testRow, cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"obj": expectedJSON.String()}, slurpValues(t, p))
	})

	for _, fn := range []string{"quote_literal", "quote_nullable"} {
		// These functions have overloads; call the one that's stable overload
		// (i.e. one that needs to convert types.Any to string).
		t.Run(fn, func(t *testing.T) {
			testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now(), false)
			e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, fmt.Sprintf("SELECT %s(42) FROM foo", fn))
			require.NoError(t, err)
			defer e.Close()

			p, err := e.Eval(ctx, testRow, cdcevent.Row{})
			require.NoError(t, err)
			require.Equal(t,
				map[string]string{fn: fmt.Sprintf("'%s'", jsonb.FromInt(42).String())},
				slurpValues(t, p))
		})
	}

	// overlaps has many overloads; most of them are immutable, but 1 is stable.
	t.Run("overlaps", func(t *testing.T) {
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now(), false)

		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, `SELECT  overlaps(transaction_timestamp(), interval '0', transaction_timestamp(), interval '-1s') FROM foo`)
		require.NoError(t, err)
		defer e.Close()

		p, err := e.Eval(ctx, testRow, cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"overlaps": "false"}, slurpValues(t, p))
	})

	// Test that cdc specific functions correctly resolve overload, and that an
	// error is returned when cdc function called with wrong arguments.
	t.Run("cdc function errors", func(t *testing.T) {
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now(), false)
		// currently, all cdc functions take no args, so call these functions with
		// some arguments.
		rng, _ := randutil.NewTestRand()
		fnArgs := func() string {
			switch rng.Int31n(3) {
			case 0:
				return "a"
			case 1:
				return "a, b"
			default:
				return "a,b,c"
			}
		}

		for fn, def := range cdcFunctions {
			// Run this test only for CDC specific functions.
			if def != useDefaultBuiltin && def.Overloads[0].FunctionProperties.Category == cdcFnCategory {
				t.Run(fn, func(t *testing.T) {
					e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, false, fmt.Sprintf("SELECT %s(%s) FROM foo", fn, fnArgs()))
					require.NoError(t, err)
					_, err = e.Eval(ctx, testRow, testRow)
					require.Regexp(t, "unknown signature", err)
				})
			}
		}
	})
}

func makeEventRow(
	t *testing.T,
	desc catalog.TableDescriptor,
	schemaTS hlc.Timestamp,
	deleted bool,
	mvccTS hlc.Timestamp,
	includeMVCCCol bool,
) cdcevent.Row {
	t.Helper()
	rng, _ := randutil.NewTestRand()

	datums := randEncDatumPrimaryFamily(t, rng, desc)
	if includeMVCCCol {
		datums = append(datums, rowenc.EncDatum{Datum: randgen.RandDatum(rng, colinfo.MVCCTimestampColumnType, false)})
	}
	r := cdcevent.TestingMakeEventRow(desc, 0, datums, deleted)
	r.SchemaTS = schemaTS
	r.MvccTimestamp = mvccTS
	return r
}

func newEvaluator(
	execCfg *sql.ExecutorConfig,
	semaCtx *tree.SemaContext,
	ed *cdcevent.EventDescriptor,
	withDiff bool,
	expr string,
) (*Evaluator, error) {
	sc, err := ParseChangefeedExpression(expr)
	if err != nil {
		return nil, err
	}

	defer configSemaForCDC(semaCtx, hlc.Timestamp{})()
	norm, err := normalizeSelectClause(context.Background(), semaCtx, sc, ed)
	if err != nil {
		return nil, err
	}
	return NewEvaluator(norm.SelectClause, execCfg, username.RootUserName(),
		defaultDBSessionData, execCfg.Clock.Now(), withDiff), nil
}
