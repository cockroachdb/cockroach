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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestEvaluator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT, 
  b STRING, 
  c VARCHAR,
  d STRING AS (concat(b, c)) VIRTUAL,
  e status DEFAULT 'inactive',
  f STRING,
  g STRING,
  PRIMARY KEY (b, a),
  FAMILY main (a, b, e),
  FAMILY only_c (c),
  FAMILY f_g_fam(f,g)
)`)
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")

	type decodeExpectation struct {
		expectUnwatchedErr bool
		evalErr            string

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

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	for _, tc := range []struct {
		testName     string
		familyName   string   // Must be set if targetType ChangefeedTargetSpecification_COLUMN_FAMILY
		setupActions []string // SQL statements to execute before starting rangefeed.
		actions      []string // SQL statements to execute after starting rangefeed.
		stmt         string
		expectErr    string // Expect to get an error when configuring predicates

		expectMainFamily  []decodeExpectation
		expectOnlyCFamily []decodeExpectation
		expectFGFamily    []decodeExpectation
	}{
		{
			testName:   "main/star",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, '1st test')"},
			stmt:       "SELECT * FROM foo",
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
			stmt:       "SELECT foo.* FROM foo",
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
			stmt: "SELECT *, cdc_is_delete() FROM foo WHERE 'hello' != 'world'",
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
			stmt:       "SELECT e, a FROM foo",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"3rd test", "3"},
					allValues: map[string]string{"a": "3", "e": "inactive"},
				},
			},
		},
		{
			testName:   "main/projection_aliased",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (3, '3rd test')"},
			stmt:       "SELECT bar.e, a FROM foo AS bar",
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
			stmt: "SELECT a FROM _ WHERE e IN ('open', 'inactive')",
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
			testName:   "main/select_udts",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b, e) VALUES (1, '4th test', 'closed')",
			},
			stmt: "SELECT a, 'inactive'::status as inactive, e FROM foo",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"4th test", "1"},
					allValues: map[string]string{"a": "1", "inactive": "inactive", "e": "closed"},
				},
			},
		},
		{
			testName:   "main/same_column_many_times",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, '5th test')"},
			stmt:       "SELECT *, a, a as one_more, a FROM foo",
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
			stmt:       "SELECT a, c FROM _",
			expectErr:  `column "foo.c" does not exist`,
		},
		{
			testName:   "main/no_col_c_star",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, 'no_c')"},
			stmt:       "SELECT *, c FROM _",
			expectErr:  `column "foo.c" does not exist`,
		},
		{
			testName:   "main/non_primary_family_with_var_free",
			familyName: "only_c",
			actions:    []string{"INSERT INTO foo (a, b, c) VALUES (42, '6th test', 'c value')"},
			stmt:       "SELECT sin(pi()/2) AS var_free, c, b FROM foo",
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
			testName:   "main/concat",
			familyName: "f_g_fam",
			actions: []string{
				"INSERT INTO foo (a, b, f) VALUES (42, 'concat', 'hello')",
			},
			stmt: "SELECT a, b, f || f AS ff, g || g AS gg FROM foo",
			expectMainFamily: []decodeExpectation{
				{
					expectUnwatchedErr: true,
				},
			},
			expectFGFamily: []decodeExpectation{
				{
					keyValues: []string{"concat", "42"},
					allValues: map[string]string{"a": "42", "b": "concat", "ff": "hellohello", "gg": "NULL"},
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
			stmt: "SELECT a, b, c, (CASE WHEN cdc_prev.c IS NULL THEN 'not there' ELSE cdc_prev.c END) AS old_c FROM foo",
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
			stmt: "SELECT IF(cdc_is_delete(),'deleted',a::string) AS conditional FROM _",
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
			stmt: "SELECT btrim(b), parse_timetz('1:00-0') AS past FROM _",
			expectMainFamily: []decodeExpectation{
				{
					keyValues: []string{"   spaced out      ", "1"},
					allValues: map[string]string{"btrim": "spaced out", "past": "01:00:00+00:00:00"},
				},
			},
		},
		// TODO(yevgeniy): enable this test.
		// This requires adding support to "fetch" those magic system columns from
		// row fetcher, or replace them with a function call.
		//{
		//	testName:   "main/magic_column",
		//	familyName: "main",
		//	actions: []string{
		//		"INSERT INTO foo (a, b) VALUES (1,  'hello')",
		//	},
		//	stmt: "SELECT a,  crdb_internal_mvcc_timestamp FROM foo",
		//	expectMainFamily: []decodeExpectation{
		//		{
		//			keyValues: []string{"hello", "1"},
		//			allValues: map[string]string{"a": "1", "crdb_internal_mvcc_timestamp": "xxx"},
		//		},
		//	},
		//},
		// {
		//  // TODO(yevgeniy): Test currently disable since session data is not serialized.
		//  // Issue #90421
		//	testName:   "main/trigram",
		//	familyName: "main",
		//	actions: []string{
		//		"INSERT INTO foo (a, b) VALUES (1,  'hello')",
		//	},
		//	stmt: "SELECT a,  b % 'hel' as trigram, b % 'heh' AS trigram2 FROM foo",
		//	expectMainFamily: []decodeExpectation{
		//		{
		//			keyValues: []string{"hello", "1"},
		//			allValues: map[string]string{"a": "1", "trigram": "true", "trigram2": "false"},
		//		},
		//	},
		//},
		{
			testName:   "main/btrim_wrong_type",
			familyName: "main",
			actions: []string{
				"INSERT INTO foo (a, b) VALUES (1, '   spaced out      ')",
			},
			stmt:      "SELECT btrim(a) FROM foo",
			expectErr: "unknown signature: btrim\\(int\\)",
		},
		{
			testName:   "main/contradiction",
			familyName: "main",
			actions:    []string{"INSERT INTO foo (a, b) VALUES (1, 'contradiction')"},
			stmt:       "SELECT * FROM foo WHERE 1 > 2",
			expectErr:  "does not match any rows",
		},
		{
			testName:   "main/no_sleep",
			familyName: "main",
			stmt:       "SELECT *, pg_sleep(86400) AS wake_up FROM _",
			expectErr:  `function "pg_sleep" unsupported by CDC`,
		},
		{
			testName:   "main/no_subselect",
			familyName: "main",
			stmt:       "SELECT cdc_prev, cdc_is_delete(), (select column1 from (values (1,2,3))) FROM foo",
			expectErr:  `sub-query expressions not supported by CDC`,
		},
		{
			testName:   "main/no_subselect_in_where",
			familyName: "main",
			stmt:       "SELECT cdc_prev FROM foo WHERE a = 2 AND (select 3) = 3",
			expectErr:  `sub-query expressions not supported by CDC`,
		},
		{
			testName:   "main/exists_subselect",
			familyName: "main",
			stmt:       "SELECT 1 FROM foo WHERE EXISTS (SELECT true)",
			expectErr:  "sub-query expressions not supported by CDC",
		},
		{
			testName:   "main/filter_many",
			familyName: "only_c",
			actions: []string{
				"INSERT INTO foo (a, b, c) WITH s AS " +
					"(SELECT generate_series as x FROM generate_series(1, 100)) " +
					"SELECT x, 'filter_many', x::string FROM s",
			},
			stmt:             "SELECT * FROM foo WHERE a % 33 = 0",
			expectMainFamily: repeatExpectation(decodeExpectation{expectUnwatchedErr: true}, 100),
			expectOnlyCFamily: func() (expectations []decodeExpectation) {
				for i := 1; i <= 100; i++ {
					iStr := strconv.FormatInt(int64(i), 10)
					e := decodeExpectation{
						keyValues: []string{"filter_many", iStr},
					}
					if i%33 == 0 {
						e.allValues = map[string]string{"a": iStr, "b": "filter_many", "c": iStr}
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
			stmt:             `SELECT * FROM foo WHERE cdc_is_delete() AND cdc_prev.a % 33 = 0`,
			expectMainFamily: repeatExpectation(decodeExpectation{expectUnwatchedErr: true}, 100),
			expectOnlyCFamily: func() (expectations []decodeExpectation) {
				for i := 1; i <= 100; i++ {
					iStr := strconv.FormatInt(int64(i), 10)
					e := decodeExpectation{
						keyValues:      []string{"only_some_deleted_values", iStr},
						expectFiltered: i%33 != 0,
						allValues:      map[string]string{"a": iStr, "b": "only_some_deleted_values", "c": "NULL"},
					}
					expectations = append(expectations, e)
				}
				return expectations
			}(),
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			sqlDB.Exec(t, "DELETE FROM foo WHERE true")
			targetType := jobspb.ChangefeedTargetSpecification_EACH_FAMILY
			if tc.familyName != "" {
				targetType = jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY
			}

			targets := changefeedbase.Targets{}
			target := changefeedbase.Target{
				Type:       targetType,
				TableID:    desc.GetID(),
				FamilyName: tc.familyName,
			}
			targets.Add(target)

			// Setup evaluator.
			e, err := newEvaluatorWithNormCheck(&execCfg, desc, s.Clock().Now(), target, tc.stmt)
			if tc.expectErr != "" {
				require.Regexp(t, tc.expectErr, err, err)
				return
			}
			require.NoError(t, err)
			defer e.Close()

			ctx := context.Background()
			decoder, err := cdcevent.NewEventDecoder(ctx, &execCfg, targets, false, false)
			require.NoError(t, err)

			for _, action := range tc.setupActions {
				sqlDB.Exec(t, action)
			}

			popRow, cleanup := cdctest.MakeRangeFeedValueReader(t, s.ExecutorConfig(), desc)
			defer cleanup()

			for _, action := range tc.actions {
				sqlDB.Exec(t, action)
			}

			expectedEvents := len(tc.expectMainFamily) + len(tc.expectOnlyCFamily) + len(tc.expectFGFamily)
			vals := readSortedRangeFeedValues(t, expectedEvents, popRow)
			for _, v := range vals {
				eventFamilyID, err := cdcevent.TestingGetFamilyIDFromKey(decoder, v.Key, v.Timestamp())
				require.NoError(t, err)

				var expect decodeExpectation
				if eventFamilyID == 0 {
					expect, tc.expectMainFamily = popExpectation(t, tc.expectMainFamily)
				} else if eventFamilyID == 1 {
					expect, tc.expectOnlyCFamily = popExpectation(t, tc.expectOnlyCFamily)
				} else {
					expect, tc.expectFGFamily = popExpectation(t, tc.expectFGFamily)
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

				require.Equal(t, expect.keyValues, slurpKeys(t, updatedRow),
					"isDelete=%t fid=%d", updatedRow.IsDeleted(), eventFamilyID)

				projection, err := e.Eval(ctx, updatedRow, prevRow)
				if expect.evalErr != "" {
					require.Regexp(t, expect.evalErr, err)
					continue
				}
				require.NoError(t, err)

				if expect.expectFiltered {
					require.False(t, projection.IsInitialized(), "keys: %v", slurpKeys(t, updatedRow))
					continue
				}

				require.Equal(t, expect.keyValues, slurpKeys(t, projection))
				require.Equal(t, expect.allValues, slurpValues(t, projection))
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

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY)")
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	target := changefeedbase.Target{
		TableID:    desc.GetID(),
		FamilyName: desc.GetFamilies()[0].Name,
	}
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
			_, err := newEvaluatorWithNormCheck(&execCfg, desc, execCfg.Clock.Now(), target,
				fmt.Sprintf("SELECT %s FROM foo", fnCall))
			require.Regexp(t, fmt.Sprintf(`function "%s" unsupported by CDC`, errFn), err)
		})

		// Same thing, but with the WHERE clause
		t.Run(fmt.Sprintf("where/%s", errFn), func(t *testing.T) {
			_, err := newEvaluatorWithNormCheck(&execCfg, desc, s.Clock().Now(), target,
				fmt.Sprintf("SELECT 1 FROM foo WHERE %s IS NOT NULL", fnCall))
			require.Regexp(t, fmt.Sprintf(`function "%s" unsupported by CDC`, errFn), err)
		})
	}
}

func decodeRowErr(
	decoder cdcevent.Decoder, v *roachpb.RangeFeedValue, prev bool,
) (cdcevent.Row, error) {
	keyVal := roachpb.KeyValue{Key: v.Key}
	if prev {
		keyVal.Value = v.PrevValue
	} else {
		keyVal.Value = v.Value
	}
	const keyOnly = false
	return decoder.DecodeKV(context.Background(), keyVal, v.Timestamp(), keyOnly)
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

func randEncDatumPrimaryFamily(
	t *testing.T, desc catalog.TableDescriptor,
) (row rowenc.EncDatumRow) {
	t.Helper()
	rng, _ := randutil.NewTestRand()

	family, err := desc.FindFamilyByID(0)
	require.NoError(t, err)
	for _, colID := range family.ColumnIDs {
		col, err := desc.FindColumnWithID(colID)
		require.NoError(t, err)
		row = append(row, rowenc.EncDatum{Datum: randgen.RandDatum(rng, col.GetType(), col.IsNullable())})
	}
	return row
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

// Evaluator gets constructed w/ normalization steps already performed.
// This test utility function adds (usually un-needed)  normalization step
// so that errors in expression can be picked up without calling evaluator.Eval().
func newEvaluatorWithNormCheck(
	execCfg *sql.ExecutorConfig,
	desc catalog.TableDescriptor,
	schemaTS hlc.Timestamp,
	target changefeedbase.Target,
	expr string,
) (*Evaluator, error) {
	sc, err := ParseChangefeedExpression(expr)
	if err != nil {
		return nil, err
	}

	const splitFamilies = true
	norm, err := NormalizeExpression(
		context.Background(), execCfg, username.RootUserName(), defaultDBSessionData, desc, schemaTS,
		jobspb.ChangefeedTargetSpecification{
			Type:       target.Type,
			TableID:    target.TableID,
			FamilyName: target.FamilyName,
		},
		sc, splitFamilies,
	)
	if err != nil {
		return nil, err
	}

	return NewEvaluator(norm.SelectClause, execCfg, username.RootUserName())
}

var defaultDBSessionData = sessiondatapb.SessionData{
	Database:   "defaultdb",
	SearchPath: sessiondata.DefaultSearchPath.GetPathArray(),
}
