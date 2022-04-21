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
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestNoopPredicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t,
		"CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, d INT, FAMILY most (a,b,c), FAMILY only_d (d))")
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), kvDB, "foo")

	serverCfg := s.DistSQLServer().(*distsql.ServerImpl).ServerConfig
	ctx := context.Background()
	decoder, err := cdcevent.NewEventDecoder(
		ctx, &serverCfg,
		[]jobspb.ChangefeedTargetSpecification{
			{
				Type:       jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
				TableID:    desc.GetID(),
				FamilyName: "most",
			},
		}, false)
	require.NoError(t, err)

	popRow, cleanup := cdctest.MakeRangeFeedValueReader(t, s.ExecutorConfig(), desc)
	defer cleanup()
	sqlDB.Exec(t, "INSERT INTO foo (a, b, d) VALUES (1, 'one', -1)")
	testRow := decodeRow(t, decoder, popRow(t), false)

	e, err := makeEvaluator(t, s.ClusterSettings(), "")
	require.NoError(t, err)

	matches, err := e.MatchesFilter(ctx, testRow, hlc.Timestamp{}, testRow)
	require.NoError(t, err)
	require.True(t, matches)

	projection, err := e.Projection(ctx, testRow, hlc.Timestamp{}, testRow)
	require.NoError(t, err)
	require.Equal(t, testRow.EventDescriptor, projection.EventDescriptor)
}

// readSortedRangeFeedValues reads n values, and sorts them based on key order.
func readSortedRangeFeedValues(
	t *testing.T, n int, row func(t *testing.T) *roachpb.RangeFeedValue,
) (res []roachpb.RangeFeedValue) {
	t.Helper()
	for i := 0; i < n; i++ {
		v := row(t)
		res = append(res, *v)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key.Compare(res[j].Key) < 0
	})
	return res
}

func TestEvaluator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT, 
  b STRING, 
  c STRING,
  d STRING AS (concat(b, c)) VIRTUAL, 
  e status DEFAULT 'inactive',
  PRIMARY KEY (b, a),
  FAMILY main (a, b, e),
  FAMILY only_c (c)
)`)
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), kvDB, "foo")

	type decodeExpectation struct {
		expectUnwatchedErr bool
		projectionErr      string

		// current value expectations.
		expectFiltered bool
		keyValues      []string
		allValues      map[string]string
	}

	repeatExpectation := func(e decodeExpectation, n int) (repeated []decodeExpectation) {
		for i := 0; i < n; i++ {
			repeated = append(repeated, e)
		}
		return
	}

	// popExpectation removes the first expectation from the provided expectation list and returns it.
	popExpectation := func(t *testing.T, expectations []decodeExpectation) (decodeExpectation, []decodeExpectation) {
		t.Helper()
		require.Less(t, 0, len(expectations))
		return expectations[0], expectations[1:]
	}

	for _, tc := range []struct {
		testName     string
		familyName   string   // Must be set if targetType ChangefeedTargetSpecification_COLUMN_FAMILY
		setupActions []string // SQL statements to execute before starting rangefeed.
		actions      []string // SQL statements to execute after starting rangefeed.
		predicate    string
		predicateErr string // Expect to get an error when configuring predicates

		expectMainFamily  []decodeExpectation
		expectOnlyCFamily []decodeExpectation
	}{
		{
			testName:   "main/star",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, '1st test')"},
			predicate:  "SELECT * FROM _",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"1st test", "1"},
					allValues: map[string]string{"a": "1", "b": "1st test", "e": "inactive"},
				},
			},
		},
		{
			testName:   "main/qualified_star",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, 'qualified')"},
			predicate:  "SELECT foo.* FROM _",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"qualified", "1"},
					allValues: map[string]string{"a": "1", "b": "qualified", "e": "inactive"},
				},
			},
		},
		{
			testName:   "main/star_delete",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b) VALUES (2, '2nd test')",
				"DELETE FROM foo WHERE a=2 AND b='2nd test'",
			},
			predicate: "SELECT *, cdc_is_delete() FROM _",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"2nd test", "2"},
					allValues: map[string]string{"a": "2", "b": "2nd test", "e": "inactive", "cdc_is_delete": "false"},
				},
				{
					keyValues: []string{"2nd test", "2"},
					allValues: map[string]string{"a": "2", "b": "2nd test", "e": "NULL", "cdc_is_delete": "true"},
				},
			},
		},
		{
			testName:   "main/projection",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (3, '3rd test')"},
			predicate:  "SELECT e, a FROM _",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"3rd test", "3"},
					allValues: map[string]string{"a": "3", "e": "inactive"},
				},
			},
		},
		{
			testName:   "main/not_closed",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b, e) VALUES (1, '4th test', 'closed')",
				"INSERT INTO foo (a, b, e) VALUES (2, '4th test', 'open')",
				"INSERT INTO foo (a, b, e) VALUES (3, '4th test', 'closed')",
				"INSERT INTO foo (a, b, e) VALUES (4, '4th test', 'closed')",
				"INSERT INTO foo (a, b, e) VALUES (5, '4th test', 'inactive')",
			},
			predicate: "SELECT a FROM _ WHERE e IN ('open', 'inactive')",
			expectMainFamily: []decodeExpectation{
				{
					expectFiltered: true,
					keyValues:      []string{"4th test", "1"},
				},
				{
					keyValues: []string{"4th test", "2"},
					allValues: map[string]string{"a": "2"},
				},
				{
					expectFiltered: true,
					keyValues:      []string{"4th test", "3"},
				},
				{
					expectFiltered: true,
					keyValues:      []string{"4th test", "4"},
				},
				{
					keyValues: []string{"4th test", "5"},
					allValues: map[string]string{"a": "5"},
				},
			},
		},
		{
			testName:   "main/same_column_many_times",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, '5th test')"},
			predicate:  "SELECT *, a, a as one_more, a FROM _",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"5th test", "1"},
					allValues: map[string]string{
						"a": "1", "b": "5th test", "e": "inactive",
						"a_1": "1", "one_more": "1", "a_2": "1",
					},
				},
			},
		},
		{
			testName:   "main/no_col_c",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, 'no_c')"},
			predicate:  "SELECT *, c FROM _",
			expectMainFamily: []decodeExpectation{
				{
					projectionErr: `column "c" does not exist`,
					keyValues:     []string{"no_c", "1"},
				},
			},
		},
		{
			testName:   "main/non_primary_family_with_var_free",
			familyName: "only_c",
			actions:    []string{"INSERT INTO foo (a, b, c) VALUES (42, '6th test', 'c value')"},
			predicate:  "SELECT sin(pi()/2) AS var_free, c, b ",
			expectMainFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					keyValues: []string{"6th test", "42"},
					allValues: map[string]string{"b": "6th test", "c": "c value", "var_free": "1.0"},
				},
			},
		},
		{
			testName:   "main/cdc_prev_select",
			familyName: "only_c",
			actions: []string{
				"INSERT INTO foo (a, b, c) VALUES (42, 'prev_select', 'c value old')",
				"UPSERT INTO foo (a, b, c) VALUES (42, 'prev_select', 'c value updated')",
			},
			predicate: "SELECT a, b, c, (CASE WHEN cdc_prev()->>'c' IS NULL THEN 'not there' ELSE cdc_prev()->>'c' END) AS old_c",
			expectMainFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
			},
			expectOnlyCFamily: []decodeExpectation{
				{
					keyValues: []string{"prev_select", "42"},
					allValues: map[string]string{"a": "42", "b": "prev_select", "c": "c value old", "old_c": "not there"},
				},
				{
					keyValues: []string{"prev_select", "42"},
					allValues: map[string]string{"a": "42", "b": "prev_select", "c": "c value updated", "old_c": "c value old"},
				},
			},
		},
		{
			testName:   "main/select_if",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b) VALUES (123, 'select_if')",
				"DELETE FROM foo where a=123",
			},
			predicate: "SELECT IF(cdc_is_delete(),'deleted',a::string) AS conditional FROM _",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"select_if", "123"},
					allValues: map[string]string{"conditional": "123"},
				},
				{
					keyValues: []string{"select_if", "123"},
					allValues: map[string]string{"conditional": "deleted"},
				},
			},
		},
		{
			testName:   "main/btrim",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b) VALUES (1, '   spaced out      ')",
			},
			predicate: "SELECT btrim(b), parse_timetz('1:00-0') AS past FROM _",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"   spaced out      ", "1"},
					allValues: map[string]string{"btrim": "spaced out", "past": "01:00:00+00:00:00"},
				},
			},
		},
		{
			testName:   "main/btrim_wrong_type",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b) VALUES (1, '   spaced out      ')",
			},
			predicate: "SELECT btrim(a) FROM _",
			expectMainFamily: []decodeExpectation{
				{
					keyValues:     []string{"   spaced out      ", "1"},
					projectionErr: "unknown signature: btrim\\(int\\)",
				},
			},
		},
		{
			testName:   "main/contradiction",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, 'contradiction')"},
			predicate:  "SELECT * FROM _ WHERE 1 > 2",
			expectMainFamily: []decodeExpectation{
				{
					projectionErr: `filter .* is a contradiction`,
					keyValues:     []string{"contradiction", "1"},
				},
			},
		},
		{
			testName:     "main/no_sleep",
			familyName:   "main",
			predicate:    "SELECT *, pg_sleep(86400) AS wake_up FROM _",
			predicateErr: `function "pg_sleep" unsupported by CDC`,
		},
		{
			testName:     "main/no_subselect",
			familyName:   "main",
			predicate:    "SELECT cdc_prev(), cdc_is_delete(123), (select column1 from (values (1,2,3))) FROM _",
			predicateErr: `subquery expressions not supported by CDC`,
		},
		{
			testName:     "main/no_subselect_in_where",
			familyName:   "main",
			predicate:    "SELECT cdc_prev() FROM _ WHERE a = 2 AND (select 3) = 3",
			predicateErr: `subquery expressions not supported by CDC`,
		},
		{
			testName:   "main/filter_many",
			familyName: "only_c",
			actions: []string{
				"INSERT INTO foo (a, b, c) WITH s AS " +
					"(SELECT generate_series as x FROM generate_series(1, 100)) " +
					"SELECT x, 'filter_many', x::string FROM s",
			},
			predicate:        "SELECT * FROM _ WHERE a % 33 = 0",
			expectMainFamily: repeatExpectation(decodeExpectation{expectUnwatchedErr: true}, 100),
			expectOnlyCFamily: func() (expectations []decodeExpectation) {
				for i := 1; i <= 100; i++ {
					iStr := strconv.FormatInt(int64(i), 10)
					e := decodeExpectation{
						keyValues: []string{"filter_many", iStr},
					}
					if i%33 == 0 {
						e.allValues = map[string]string{"c": iStr}
					} else {
						e.expectFiltered = true
					}
					expectations = append(expectations, e)
				}
				return expectations
			}(),
		},
		{
			testName:   "main/only_some_deleted_values",
			familyName: "only_c",
			setupActions: []string{
				"INSERT INTO foo (a, b, c) WITH s AS " +
					"(SELECT generate_series as x FROM generate_series(1, 100)) " +
					"SELECT x, 'only_some_deleted_values', x::string FROM s",
			},
			actions:          []string{"DELETE FROM foo WHERE b='only_some_deleted_values'"},
			predicate:        `SELECT * FROM _ WHERE cdc_is_delete() AND cast(cdc_prev()->>'a' as int) % 33 = 0`,
			expectMainFamily: repeatExpectation(decodeExpectation{expectUnwatchedErr: true}, 100),
			expectOnlyCFamily: func() (expectations []decodeExpectation) {
				for i := 1; i <= 100; i++ {
					e := decodeExpectation{
						keyValues:      []string{"only_some_deleted_values", strconv.FormatInt(int64(i), 10)},
						expectFiltered: i%33 != 0,
						allValues:      map[string]string{"c": "NULL"},
					}
					expectations = append(expectations, e)
				}
				return expectations
			}(),
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			sqlDB.Exec(t, "DELETE FROM foo WHERE true")

			// Setup evaluator.
			evaluator, err := makeEvaluator(t, s.ClusterSettings(), tc.predicate)
			if tc.predicateErr != "" {
				require.Regexp(t, tc.predicateErr, err)
				return
			}
			require.NoError(t, err)

			targetType := jobspb.ChangefeedTargetSpecification_EACH_FAMILY
			if tc.familyName != "" {
				targetType = jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
			}

			targets := []jobspb.ChangefeedTargetSpecification{
				{
					Type:       targetType,
					TableID:    desc.GetID(),
					FamilyName: tc.familyName,
				},
			}

			serverCfg := s.DistSQLServer().(*distsql.ServerImpl).ServerConfig
			ctx := context.Background()
			decoder, err := cdcevent.NewEventDecoder(ctx, &serverCfg, targets, false)
			require.NoError(t, err)

			for _, action := range tc.setupActions {
				sqlDB.Exec(t, action)
			}

			popRow, cleanup := cdctest.MakeRangeFeedValueReader(t, s.ExecutorConfig(), desc)
			defer cleanup()

			for _, action := range tc.actions {
				sqlDB.Exec(t, action)
			}

			expectedEvents := len(tc.expectMainFamily) + len(tc.expectOnlyCFamily)
			vals := readSortedRangeFeedValues(t, expectedEvents, popRow)
			for _, v := range vals {
				eventFamilyID, err := cdcevent.TestingGetFamilyIDFromKey(decoder, v.Key, v.Timestamp())
				require.NoError(t, err)

				var expect decodeExpectation
				if eventFamilyID == 0 {
					expect, tc.expectMainFamily = popExpectation(t, tc.expectMainFamily)
				} else {
					expect, tc.expectOnlyCFamily = popExpectation(t, tc.expectOnlyCFamily)
				}

				updatedRow, err := decodeRowErr(decoder, &v, false)
				if expect.expectUnwatchedErr {
					require.ErrorIs(t, err, cdcevent.ErrUnwatchedFamily)
					continue
				}

				require.NoError(t, err)
				require.True(t, updatedRow.IsInitialized())
				prevRow := decodeRow(t, decoder, &v, true)
				require.NoError(t, err)

				if expect.expectFiltered {
					require.Equal(t, expect.keyValues, slurpKeys(t, updatedRow), "isDelete=%t fid=%d", updatedRow.IsDeleted(), eventFamilyID)
					matches, err := evaluator.MatchesFilter(ctx, updatedRow, v.Timestamp(), prevRow)
					require.NoError(t, err)
					require.False(t, matches, "keys: %v", slurpKeys(t, updatedRow))
					continue
				}

				projection, err := evaluator.Projection(ctx, updatedRow, v.Timestamp(), prevRow)
				if expect.projectionErr != "" {
					require.Regexp(t, expect.projectionErr, err)
					// Sanity check we get error for the row we expected to get an error for.
					require.Equal(t, expect.keyValues, slurpKeys(t, updatedRow))
				} else {
					require.NoError(t, err)
					require.Equal(t, expect.keyValues, slurpKeys(t, projection))
					require.Equal(t, expect.allValues, slurpValues(t, projection))
				}
			}
		})
	}
}

// Tests that use of volatile functions, without CDC specific override,
// results in an error.
func TestUnsupportedCDCFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY)")

	for fnCall, errFn := range map[string]string{
		// Some volatile functions.
		"version()":                            "version",
		"crdb_internal.trace_id()":             "crdb_internal.trace_id",
		"crdb_internal.locality_value('blah')": "crdb_internal.locality_value",
		"1 + crdb_internal.trace_id()":         "crdb_internal.trace_id",
		"current_user()":                       "current_user",
		"nextval('seq')":                       "nextval",

		// Special form of CURRENT_USER() is SESSION_USER (no parens).
		"SESSION_USER": "session_user",

		// Aggregator functions
		"generate_series(1, 10)": "generate_series",

		// Unsupported functions that take arguments from foo.
		"generate_series(1, a)":            "generate_series",
		"crdb_internal.read_file(b)":       "crdb_internal.read_file",
		"crdb_internal.get_namespace_id()": "crdb_internal.get_namespace_id",
	} {
		t.Run(fmt.Sprintf("select/%s", errFn), func(t *testing.T) {
			_, err := makeEvaluator(t, s.ClusterSettings(), fmt.Sprintf("SELECT %s", fnCall))
			require.Regexp(t, fmt.Sprintf(`function "%s" unsupported by CDC`, errFn), err)
		})

		// Same thing, but with the WHERE clause
		t.Run(fmt.Sprintf("where/%s", errFn), func(t *testing.T) {
			_, err := makeEvaluator(t, s.ClusterSettings(),
				fmt.Sprintf("SELECT 1 WHERE %s IS NOT NULL", fnCall))
			require.Regexp(t, fmt.Sprintf(`function "%s" unsupported by CDC`, errFn), err)
		})
	}
}

func TestEvaluatesProjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, ""+
		"CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, d INT, "+
		"FAMILY most (a,b,c), FAMILY only_d (d))")
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), kvDB, "foo")
	testRow := cdcevent.TestingMakeEventRow(desc, 0, randEncDatumRow(t, desc, 0), false)

	verifyConstantsFolded := func(p *exprEval) {
		for _, expr := range p.selectors {
			_ = expr.(tree.Datum)
		}
	}

	for _, tc := range []struct {
		name        string
		predicate   string
		input       rowenc.EncDatumRow
		expectErr   string
		expectation map[string]string
		verifyFold  bool
	}{
		{
			name:        "constants",
			predicate:   "SELECT 1, 2, 3",
			expectation: map[string]string{"column_1": "1", "column_2": "2", "column_3": "3"},
			verifyFold:  true,
		},
		{
			name:        "constants_functions_and_aliases",
			predicate:   "SELECT 0 as zero, abs(-2) two, 42",
			expectation: map[string]string{"zero": "0", "two": "2", "column_3": "42"},
			verifyFold:  true,
		},
		{
			name:        "trig_fun",
			predicate:   "SELECT cos(0), sin(pi()/2) as sin_90, 39 + pi()::int",
			expectation: map[string]string{"cos": "1.0", "sin_90": "1.0", "column_3": "42"},
			verifyFold:  true,
		},
		{
			name:      "div_by_zero",
			predicate: "SELECT 3 / sin(pi() - pi())  as result",
			expectErr: "division by zero",
		},
		{
			name:        "projection_with_bound_vars",
			predicate:   "SELECT sqrt(a::float) + sin(pi()/2)  as result, foo.*",
			input:       makeEncDatumRow(tree.NewDInt(4), tree.DNull, tree.DNull),
			expectation: map[string]string{"result": "3.0", "a": "4", "b": "NULL", "c": "NULL"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := makeExprEval(t, s.ClusterSettings(), testRow.EventDescriptor, tc.predicate)
			if tc.expectErr != "" {
				require.Regexp(t, tc.expectErr, err)
				return
			}

			require.NoError(t, err)
			if tc.verifyFold {
				verifyConstantsFolded(e)
			}
			row := testRow
			if tc.input != nil {
				row = cdcevent.TestingMakeEventRow(desc, 0, tc.input, false)
			}

			p, err := e.evalProjection(context.Background(), row, hlc.Timestamp{}, row)
			require.NoError(t, err)
			require.Equal(t, tc.expectation, slurpValues(t, p))
		})
	}
}

// makeEvaluator creates Evaluator and configures it with specified
// select statement predicate.
func makeEvaluator(t *testing.T, st *cluster.Settings, selectStr string) (*Evaluator, error) {
	t.Helper()
	s, err := parser.ParseOne(selectStr)
	require.NoError(t, err)
	slct := s.AST.(*tree.Select).Select.(*tree.SelectClause)
	evalCtx := eval.MakeTestingEvalContext(st)
	return NewEvaluator(&evalCtx, slct)
}

func makeExprEval(
	t *testing.T, st *cluster.Settings, ed *cdcevent.EventDescriptor, selectStr string,
) (*exprEval, error) {
	t.Helper()
	e, err := makeEvaluator(t, st, selectStr)
	require.NoError(t, err)

	if err := e.initEval(context.Background(), ed); err != nil {
		return nil, err
	}
	return e.evaluator, nil
}

func decodeRowErr(
	decoder cdcevent.Decoder, v *roachpb.RangeFeedValue, prev bool,
) (cdcevent.Row, error) {
	kv := roachpb.KeyValue{Key: v.Key}
	if prev {
		kv.Value = v.PrevValue
	} else {
		kv.Value = v.Value
	}
	return decoder.DecodeKV(context.Background(), kv, v.Timestamp())
}

func decodeRow(
	t *testing.T, decoder cdcevent.Decoder, v *roachpb.RangeFeedValue, prev bool,
) cdcevent.Row {
	r, err := decodeRowErr(decoder, v, prev)
	require.NoError(t, err)
	return r
}

func slurpKeys(t *testing.T, r cdcevent.Row) (keys []string) {
	t.Helper()
	require.NoError(t, r.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		keys = append(keys, tree.AsStringWithFlags(d, tree.FmtExport))
		return nil
	}))
	return keys
}

func slurpValues(t *testing.T, r cdcevent.Row) map[string]string {
	t.Helper()
	res := make(map[string]string)
	require.NoError(t, r.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		res[col.Name] = tree.AsStringWithFlags(d, tree.FmtExport)
		return nil
	}))
	return res
}

func randEncDatumRow(
	t *testing.T, desc catalog.TableDescriptor, familyID descpb.FamilyID,
) (row rowenc.EncDatumRow) {
	t.Helper()
	rng, _ := randutil.NewTestRand()

	family, err := desc.FindFamilyByID(familyID)
	require.NoError(t, err)
	for _, colID := range family.ColumnIDs {
		col, err := desc.FindColumnWithID(colID)
		require.NoError(t, err)
		row = append(row, rowenc.EncDatum{Datum: randgen.RandDatum(rng, col.GetType(), col.IsNullable())})
	}
	return row
}

func makeEncDatumRow(datums ...tree.Datum) (row rowenc.EncDatumRow) {
	for _, d := range datums {
		row = append(row, rowenc.EncDatum{Datum: d})
	}
	return row
}
