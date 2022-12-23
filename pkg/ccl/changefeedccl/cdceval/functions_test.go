// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, ""+
		"CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, d INT, "+
		"FAMILY most (a,b,c), FAMILY only_d (d))")
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")

	ctx := context.Background()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	semaCtx := tree.MakeSemaContext()
	defer configSemaForCDC(&semaCtx)()

	t.Run("time", func(t *testing.T) {
		// We'll run tests against some future time stamp to ensure
		// that time functions use correct values.
		futureTS := s.Clock().Now().Add(int64(60*time.Minute), 0)
		expectTSTZ := func(ts hlc.Timestamp) string {
			t.Helper()
			d, err := tree.MakeDTimestampTZ(ts.GoTime(), time.Microsecond)
			require.NoError(t, err)
			return tree.AsStringWithFlags(d, tree.FmtExport)
		}

		for _, tc := range []struct {
			fn     string
			expect string
		}{
			{fn: "statement_timestamp", expect: expectTSTZ(futureTS)},
			{fn: "transaction_timestamp", expect: expectTSTZ(futureTS)},
		} {
			t.Run(tc.fn, func(t *testing.T) {
				testRow := makeEventRow(t, desc, s.Clock().Now(), false, futureTS)
				e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
					fmt.Sprintf("SELECT %s() FROM foo", tc.fn))
				require.NoError(t, err)
				defer e.Close()

				p, err := e.Eval(ctx, testRow, cdcevent.Row{})
				require.NoError(t, err)
				require.Equal(t, map[string]string{tc.fn: tc.expect}, slurpValues(t, p))

				// Emit again, this time advancing MVCC timestamp of the row.
				// We want to make sure that optimizer did not constant fold the call
				// to the function, even though this function is marked stable.
				testRow.MvccTimestamp = testRow.MvccTimestamp.Add(int64(time.Hour), 0)
				p, err = e.Eval(ctx, testRow, cdcevent.Row{})
				require.NoError(t, err)
				require.Equal(t, map[string]string{tc.fn: expectTSTZ(testRow.MvccTimestamp)}, slurpValues(t, p))
			})
		}

		t.Run("timezone", func(t *testing.T) {
			// Timezone has many overrides, some are immutable, and some are Stable.
			// Call "stable" overload which relies on session data containing
			// timezone. Since we don't do any special setup with session data, the
			// default timezone is UTC. We'll use a "strange" timezone of -1h33m from
			// UTC to test conversion.
			testRow := makeEventRow(t, desc, s.Clock().Now(), false, futureTS)
			e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
				fmt.Sprintf("SELECT timezone('+01:33:00', '%s'::time) FROM foo",
					futureTS.GoTime().Format("15:04:05")))
			require.NoError(t, err)
			defer e.Close()

			p, err := e.Eval(ctx, testRow, cdcevent.Row{})
			require.NoError(t, err)

			expectedTZ := fmt.Sprintf("%s-01:33:00",
				futureTS.GoTime().Add(-93*time.Minute).Format("15:04:05"))
			require.Equal(t, map[string]string{"timezone": expectedTZ}, slurpValues(t, p))
		})
	})

	t.Run("cdc_is_delete", func(t *testing.T) {
		schemaTS := s.Clock().Now()
		testRow := makeEventRow(t, desc, schemaTS, false, s.Clock().Now())
		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
			"SELECT cdc_is_delete() FROM foo")
		require.NoError(t, err)
		defer e.Close()

		for _, expectDelete := range []bool{true, false} {
			testRow := makeEventRow(t, desc, schemaTS, expectDelete, s.Clock().Now())
			p, err := e.Eval(ctx, testRow, cdcevent.Row{})
			require.NoError(t, err)
			require.Equal(t,
				map[string]string{"cdc_is_delete": fmt.Sprintf("%t", expectDelete)},
				slurpValues(t, p))
		}
	})

	mustParseJSON := func(d tree.Datum) jsonb.JSON {
		t.Helper()
		j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
		require.NoError(t, err)
		return j
	}

	t.Run("cdc_{mvcc,updated}_timestamp", func(t *testing.T) {
		for _, cast := range []string{"", "::decimal", "::string"} {
			t.Run(cast, func(t *testing.T) {
				schemaTS := s.Clock().Now()
				mvccTS := schemaTS.Add(int64(30*time.Minute), 0)
				testRow := makeEventRow(t, desc, schemaTS, false, mvccTS)
				e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor, fmt.Sprintf(
					"SELECT cdc_mvcc_timestamp()%[1]s as mvcc, cdc_updated_timestamp()%[1]s as updated FROM foo", cast,
				))
				require.NoError(t, err)
				defer e.Close()

				p, err := e.Eval(ctx, testRow, cdcevent.Row{})
				require.NoError(t, err)
				require.Equal(t,
					map[string]string{
						"mvcc":    eval.TimestampToDecimalDatum(mvccTS).String(),
						"updated": eval.TimestampToDecimalDatum(schemaTS).String(),
					},
					slurpValues(t, p),
				)
			})
		}
	})

	t.Run("pg_collation_for", func(t *testing.T) {
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now())

		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
			`SELECT pg_collation_for('hello' COLLATE de_DE) AS col FROM foo`)
		require.NoError(t, err)
		defer e.Close()

		p, err := e.Eval(ctx, testRow, cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"col": "\"de_DE\""}, slurpValues(t, p))
	})

	for _, fn := range []string{"to_json", "to_jsonb"} {
		t.Run(fn, func(t *testing.T) {
			testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now())
			rowDatums := testRow.EncDatums()

			e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
				fmt.Sprintf("SELECT %s(a) FROM foo", fn))
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
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now())

		rowDatums := testRow.EncDatums()
		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
			"SELECT row_to_json(row(a, b, c)) FROM foo")
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
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now())
		rowDatums := testRow.EncDatums()
		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
			"SELECT jsonb_build_array(a, a, 42) AS three_ints FROM foo")
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
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now())
		rowDatums := testRow.EncDatums()
		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
			"SELECT jsonb_build_object('a', a, 'b', b, 'c', c) AS obj FROM foo")
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
			testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now())
			e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
				fmt.Sprintf("SELECT %s(42) FROM foo", fn))
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
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now())

		e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
			`SELECT  overlaps(transaction_timestamp(), interval '0', transaction_timestamp(), interval '-1s') FROM foo`)
		require.NoError(t, err)
		defer e.Close()

		p, err := e.Eval(ctx, testRow, cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"overlaps": "false"}, slurpValues(t, p))
	})

	// Test that cdc specific functions correctly resolve overload, and that an
	// error is returned when cdc function called with wrong arguments.
	t.Run("cdc function errors", func(t *testing.T) {
		testRow := makeEventRow(t, desc, s.Clock().Now(), false, s.Clock().Now())
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
					e, err := newEvaluator(&execCfg, &semaCtx, testRow.EventDescriptor,
						fmt.Sprintf("SELECT %s(%s) FROM foo", fn, fnArgs()))
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
) cdcevent.Row {
	t.Helper()
	datums := randEncDatumPrimaryFamily(t, desc)
	r := cdcevent.TestingMakeEventRow(desc, 0, datums, deleted)
	r.SchemaTS = schemaTS
	r.MvccTimestamp = mvccTS
	return r
}

func newEvaluator(
	execCfg *sql.ExecutorConfig, semaCtx *tree.SemaContext, ed *cdcevent.EventDescriptor, expr string,
) (*Evaluator, error) {
	sc, err := ParseChangefeedExpression(expr)
	if err != nil {
		return nil, err
	}

	norm, err := normalizeSelectClause(context.Background(), semaCtx, sc, ed)
	if err != nil {
		return nil, err
	}
	return NewEvaluator(norm.SelectClause, execCfg, username.RootUserName())
}
