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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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

	t.Run("time", func(t *testing.T) {
		// We'll run tests against some future time stamp to ensure
		// that time functions use correct values.
		futureTS := s.Clock().Now().Add(int64(60*time.Minute), 0)
		expectTSTZ := func() string {
			t.Helper()
			d, err := tree.MakeDTimestampTZ(futureTS.GoTime(), time.Microsecond)
			require.NoError(t, err)
			return tree.AsStringWithFlags(d, tree.FmtExport)
		}()

		for _, tc := range []struct {
			fn     string
			expect string
		}{
			{fn: "statement_timestamp", expect: expectTSTZ},
			{fn: "transaction_timestamp", expect: expectTSTZ},
		} {
			t.Run(tc.fn, func(t *testing.T) {
				testRow := cdcevent.TestingMakeEventRow(desc, 0,
					randEncDatumRow(t, desc, 0), false)
				e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
					fmt.Sprintf("SELECT %s()", tc.fn))
				require.NoError(t, err)
				p, err := e.evalProjection(ctx, testRow, futureTS, testRow)
				require.NoError(t, err)
				require.Equal(t, map[string]string{tc.fn: tc.expect}, slurpValues(t, p))
			})
		}

		t.Run("timezone", func(t *testing.T) {
			// Timezone has many overrides, some are immutable, and some are Stable.
			// Call "stable" overload which relies on session data containing
			// timezone. Since we don't do any special setup with session data, the
			// default timezone is UTC. We'll use a "strange" timezone of -1h33m from
			// UTC to test conversion.
			testRow := cdcevent.TestingMakeEventRow(desc, 0,
				randEncDatumRow(t, desc, 0), false)

			e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
				fmt.Sprintf("SELECT timezone('+01:33:00', '%s'::time)",
					futureTS.GoTime().Format("15:04:05")))
			require.NoError(t, err)
			p, err := e.evalProjection(ctx, testRow, futureTS, testRow)
			require.NoError(t, err)

			expectedTZ := fmt.Sprintf("%s-01:33:00",
				futureTS.GoTime().Add(-93*time.Minute).Format("15:04:05"))
			require.Equal(t, map[string]string{"timezone": expectedTZ}, slurpValues(t, p))
		})
	})

	t.Run("cdc_is_delete", func(t *testing.T) {
		for _, expectDelete := range []bool{true, false} {
			testRow := cdcevent.TestingMakeEventRow(desc, 0,
				randEncDatumRow(t, desc, 0), expectDelete)
			e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
				"SELECT cdc_is_delete()")
			require.NoError(t, err)

			p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), testRow)
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

	t.Run("cdc_prev", func(t *testing.T) {
		rowDatums := randEncDatumRow(t, desc, 0)
		testRow := cdcevent.TestingMakeEventRow(desc, 0, rowDatums, false)
		e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
			"SELECT cdc_prev()")
		require.NoError(t, err)

		// When previous row is not set -- i.e. if running without diff, cdc_prev returns
		// null json.
		p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"cdc_prev": jsonb.NullJSONValue.String()}, slurpValues(t, p))

		// Otherwise, expect to get JSONB.
		b := jsonb.NewObjectBuilder(len(rowDatums))
		for i, d := range rowDatums {
			b.Add(desc.PublicColumns()[i].GetName(), mustParseJSON(d.Datum))
		}

		expectedJSON := b.Build()
		p, err = e.evalProjection(ctx, testRow, s.Clock().Now(), testRow)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"cdc_prev": expectedJSON.String()}, slurpValues(t, p))
	})

	for _, cast := range []string{"", "::decimal", "::string"} {
		t.Run(fmt.Sprintf("cdc_{mvcc,updated}_timestamp()%s", cast), func(t *testing.T) {
			schemaTS := s.Clock().Now().Add(int64(60*time.Minute), 0)
			mvccTS := schemaTS.Add(int64(30*time.Minute), 0)
			testRow := cdcevent.TestingMakeEventRow(desc, 0,
				randEncDatumRow(t, desc, 0), false)
			testRow.EventDescriptor.SchemaTS = schemaTS

			e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
				fmt.Sprintf(
					"SELECT cdc_mvcc_timestamp()%[1]s as mvcc, cdc_updated_timestamp()%[1]s as updated", cast,
				))
			require.NoError(t, err)

			p, err := e.evalProjection(ctx, testRow, mvccTS, testRow)
			require.NoError(t, err)
			require.Equal(t,
				map[string]string{
					"mvcc":    eval.TimestampToDecimalDatum(mvccTS).String(),
					"updated": eval.TimestampToDecimalDatum(schemaTS).String(),
				},
				slurpValues(t, p))
		})
	}

	t.Run("pg_collation_for", func(t *testing.T) {
		testRow := cdcevent.TestingMakeEventRow(desc, 0,
			randEncDatumRow(t, desc, 0), false)
		e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
			`SELECT pg_collation_for('hello' COLLATE de_DE) AS col`)
		require.NoError(t, err)

		p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), testRow)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"col": "\"de_de\""}, slurpValues(t, p))
	})

	for _, fn := range []string{"to_json", "to_jsonb"} {
		t.Run(fn, func(t *testing.T) {
			rowDatums := randEncDatumRow(t, desc, 0)
			testRow := cdcevent.TestingMakeEventRow(desc, 0, rowDatums, false)
			e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
				fmt.Sprintf("SELECT %s(a)", fn))
			require.NoError(t, err)

			p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), cdcevent.Row{})
			require.NoError(t, err)
			require.Equal(t,
				map[string]string{fn: mustParseJSON(rowDatums[0].Datum).String()},
				slurpValues(t, p))
		})
	}

	t.Run("row_to_json", func(t *testing.T) {
		rowDatums := randEncDatumRow(t, desc, 0)
		testRow := cdcevent.TestingMakeEventRow(desc, 0, rowDatums, false)
		e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
			"SELECT row_to_json(row(a, b, c))")
		require.NoError(t, err)

		b := jsonb.NewObjectBuilder(len(rowDatums))
		for i, d := range rowDatums {
			b.Add(fmt.Sprintf("f%d", i+1), mustParseJSON(d.Datum))
		}
		expectedJSON := b.Build()

		p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"row_to_json": expectedJSON.String()}, slurpValues(t, p))
	})

	t.Run("jsonb_build_array", func(t *testing.T) {
		rowDatums := randEncDatumRow(t, desc, 0)
		testRow := cdcevent.TestingMakeEventRow(desc, 0, rowDatums, false)
		e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
			"SELECT jsonb_build_array(a, a, 42) AS three_ints")
		require.NoError(t, err)

		b := jsonb.NewArrayBuilder(3)
		j := mustParseJSON(rowDatums[0].Datum)
		b.Add(j)
		b.Add(j)
		b.Add(jsonb.FromInt(42))
		expectedJSON := b.Build()

		p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"three_ints": expectedJSON.String()}, slurpValues(t, p))
	})

	t.Run("jsonb_build_object", func(t *testing.T) {
		rowDatums := randEncDatumRow(t, desc, 0)
		testRow := cdcevent.TestingMakeEventRow(desc, 0, rowDatums, false)
		e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
			"SELECT jsonb_build_object('a', a, 'b', b, 'c', c) AS obj")
		require.NoError(t, err)

		b := jsonb.NewObjectBuilder(3)
		b.Add("a", mustParseJSON(rowDatums[0].Datum))
		b.Add("b", mustParseJSON(rowDatums[1].Datum))
		b.Add("c", mustParseJSON(rowDatums[2].Datum))
		expectedJSON := b.Build()

		p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"obj": expectedJSON.String()}, slurpValues(t, p))
	})

	for _, fn := range []string{"quote_literal", "quote_nullable"} {
		// These functions have overloads; call the one that's stable overload
		// (i.e. one that needs to convert types.Any to string.
		t.Run(fn, func(t *testing.T) {
			rowDatums := randEncDatumRow(t, desc, 0)
			testRow := cdcevent.TestingMakeEventRow(desc, 0, rowDatums, false)
			e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
				fmt.Sprintf("SELECT %s(42)", fn))
			require.NoError(t, err)

			p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), cdcevent.Row{})
			require.NoError(t, err)
			require.Equal(t,
				map[string]string{fn: fmt.Sprintf("'%s'", jsonb.FromInt(42).String())},
				slurpValues(t, p))
		})
	}

	// overlaps has many overloads; most of them are immutable, but 1 is stable.
	t.Run("overlaps", func(t *testing.T) {
		rowDatums := randEncDatumRow(t, desc, 0)
		testRow := cdcevent.TestingMakeEventRow(desc, 0, rowDatums, false)
		e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
			`SELECT  overlaps(transaction_timestamp(), interval '0', transaction_timestamp(), interval '-1s')`)
		require.NoError(t, err)

		p, err := e.evalProjection(ctx, testRow, s.Clock().Now(), cdcevent.Row{})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"overlaps": "false"}, slurpValues(t, p))
	})

	// Test that cdc specific functions correctly resolve overload, and that an
	// error is returned when cdc function called with wrong arguments.
	t.Run("cdc function errors", func(t *testing.T) {
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

		for fn := range cdcFunctions {
			t.Run(fn, func(t *testing.T) {
				rowDatums := randEncDatumRow(t, desc, 0)
				testRow := cdcevent.TestingMakeEventRow(desc, 0, rowDatums, false)
				_, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor,
					fmt.Sprintf("SELECT %s(%s)", fn, fnArgs()))
				require.Regexp(t, "unknown signature", err)
			})
		}
	})
}
