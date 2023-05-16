// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

func TestExplainAnalyzeDebug(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, godb, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer srv.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(godb)
	r.Exec(t, `CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT UNIQUE);
CREATE SCHEMA s;
CREATE TABLE s.a (a INT PRIMARY KEY);`)

	base := "statement.sql trace.json trace.txt trace-jaeger.json env.sql"
	plans := "schema.sql opt.txt opt-v.txt opt-vv.txt plan.txt"

	// Set a small chunk size to test splitting into chunks. The bundle files are
	// on the order of 10KB.
	r.Exec(t, fmt.Sprintf(
		"SET CLUSTER SETTING sql.stmt_diagnostics.bundle_chunk_size = '%d'",
		5000+rand.Intn(10000),
	))

	t.Run("no-table", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT 123")
		checkBundle(
			t, fmt.Sprint(rows), "", nil,
			base, plans, "distsql.html vec.txt vec-v.txt",
		)
	})

	t.Run("basic", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM abc WHERE c=1")
		checkBundle(
			t, fmt.Sprint(rows), "public.abc", nil,
			base, plans, "stats-defaultdb.public.abc.sql", "distsql.html vec.txt vec-v.txt",
		)
	})

	// Check that we get separate diagrams for subqueries.
	t.Run("subqueries", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT EXISTS (SELECT * FROM abc WHERE c=1)")
		checkBundle(
			t, fmt.Sprint(rows), "public.abc", nil,
			base, plans, "stats-defaultdb.public.abc.sql", "distsql-2-main-query.html distsql-1-subquery.html vec-1-subquery-v.txt vec-1-subquery.txt vec-2-main-query-v.txt vec-2-main-query.txt",
		)
	})

	t.Run("user-defined schema", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM s.a WHERE a=1")
		checkBundle(
			t, fmt.Sprint(rows), "s.a", nil,
			base, plans, "stats-defaultdb.s.a.sql", "distsql.html vec.txt vec-v.txt",
		)
	})

	// Even on query errors we should still get a bundle.
	t.Run("error", func(t *testing.T) {
		_, err := godb.QueryContext(ctx, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM badtable")
		if !testutils.IsError(err, "relation.*does not exist") {
			t.Fatalf("unexpected error %v\n", err)
		}
		// The bundle url is inside the error detail.
		var pqErr *pq.Error
		_ = errors.As(err, &pqErr)
		checkBundle(t, fmt.Sprintf("%+v", pqErr.Detail), "", nil, base, plans, "distsql.html errors.txt")
	})

	// #92920 Make sure schema and opt files are created.
	t.Run("memo-reset", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) CREATE TABLE t (i int)")
		checkBundle(t, fmt.Sprint(rows), "", func(name, contents string) error {
			if name == "opt.txt" {
				if contents == noPlan {
					return errors.Errorf("opt.txt empty")
				}
			}
			return nil
		}, base, plans, "distsql.html vec.txt vec-v.txt")
	})

	// Verify that we can issue the statement with prepare (which can happen
	// depending on the client).
	t.Run("prepare", func(t *testing.T) {
		stmt, err := godb.Prepare("EXPLAIN ANALYZE (DEBUG) SELECT * FROM abc WHERE c=1")
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
		rows, err := stmt.Query()
		if err != nil {
			t.Fatal(err)
		}
		var rowsBuf bytes.Buffer
		for rows.Next() {
			var row string
			if err := rows.Scan(&row); err != nil {
				t.Fatal(err)
			}
			rowsBuf.WriteString(row)
			rowsBuf.WriteByte('\n')
		}
		checkBundle(
			t, rowsBuf.String(), "public.abc", nil,
			base, plans, "stats-defaultdb.public.abc.sql", "distsql.html vec.txt vec-v.txt",
		)
	})

	// This is a regression test for the situation where wrapped into the
	// vectorized flow planNodes in the postqueries were messed up because the
	// generation of EXPLAIN (VEC) diagrams modified planNodeToRowSources in
	// place (#62261).
	t.Run("insert with postquery", func(t *testing.T) {
		// We need to disable the insert fast path so that postqueries are
		// planned.
		r.Exec(t, `SET enable_insert_fast_path = false;
CREATE TABLE promos(id SERIAL PRIMARY KEY);
INSERT INTO promos VALUES (642606224929619969);
CREATE TABLE users(id UUID DEFAULT gen_random_uuid() PRIMARY KEY, promo_id INT REFERENCES promos(id));
`)
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) INSERT INTO users (promo_id) VALUES (642606224929619969);")
		checkBundle(
			t, fmt.Sprint(rows), "public.users", nil, base, plans,
			"stats-defaultdb.public.users.sql", "stats-defaultdb.public.promos.sql",
			"distsql-1-main-query.html distsql-2-postquery.html vec-1-main-query-v.txt vec-1-main-query.txt vec-2-postquery-v.txt vec-2-postquery.txt",
		)
		r.Exec(t, `RESET enable_insert_fast_path;`)
	})

	t.Run("basic when tracing already enabled", func(t *testing.T) {
		r.Exec(t, "SET CLUSTER SETTING sql.trace.txn.enable_threshold='100ms';")
		defer r.Exec(t, "SET CLUSTER SETTING sql.trace.txn.enable_threshold='0ms';")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM abc WHERE c=1")
		checkBundle(
			t, fmt.Sprint(rows), "public.abc", nil,
			base, plans, "stats-defaultdb.public.abc.sql", "distsql.html vec.txt vec-v.txt",
		)
	})

	t.Run("session-settings", func(t *testing.T) {
		testcases := []struct {
			sessionVar, value string
		}{
			{"allow_prepare_as_opt_plan", "on"},
			{"cost_scans_with_default_col_size", "on"},
			{"datestyle", "'ISO, DMY'"},
			{"default_int_size", "4"},
			{"default_transaction_priority", "low"},
			{"default_transaction_quality_of_service", "background"},
			{"default_transaction_read_only", "on"},
			{"disallow_full_table_scans", "on"},
			{"distsql", "always"},
			{"enable_implicit_select_for_update", "off"},
			{"enable_implicit_transaction_for_batch_statements", "off"},
			{"enable_insert_fast_path", "off"},
			{"enable_multiple_modifications_of_table", "on"},
			{"enable_zigzag_join", "off"},
			{"expect_and_ignore_not_visible_columns_in_copy", "on"},
			{"intervalstyle", "iso_8601"},
			{"large_full_scan_rows", "2000"},
			{"locality_optimized_partitioned_index_scan", "off"},
			{"null_ordered_last", "on"},
			{"on_update_rehome_row_enabled", "off"},
			{"opt_split_scan_limit", "1000"},
			{"optimizer_use_histograms", "off"},
			{"optimizer_use_multicol_stats", "off"},
			{"optimizer_use_not_visible_indexes", "on"},
			{"pg_trgm.similarity_threshold", "0.6"},
			{"prefer_lookup_joins_for_fks", "on"},
			{"propagate_input_ordering", "on"},
			{"reorder_joins_limit", "3"},
			{"sql_safe_updates", "on"},
			{"testing_optimizer_cost_perturbation", "0.3"},
			{"testing_optimizer_disable_rule_probability", "0.00000000001"},
			{"testing_optimizer_random_seed", "123"},
			{"timezone", "+8"},
			{"unconstrained_non_covering_index_scan_enabled", "on"},
		}
		for _, tc := range testcases {
			t.Run(tc.sessionVar, func(t *testing.T) {
				r.Exec(t, fmt.Sprintf("SET %s = %s", tc.sessionVar, tc.value))
				defer r.Exec(t, fmt.Sprintf("RESET %s", tc.sessionVar))
				rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM abc WHERE c=1")
				checkBundle(
					t, fmt.Sprint(rows), "public.abc", func(name, contents string) error {
						if name == "env.sql" {
							reg := regexp.MustCompile(fmt.Sprintf("SET %s.*-- default value", tc.sessionVar))
							if reg.FindString(contents) == "" {
								return errors.Errorf("could not find 'SET %s' in env.sql", tc.sessionVar)
							}
						}
						return nil
					},
					base, plans, "stats-defaultdb.public.abc.sql", "distsql.html vec.txt vec-v.txt",
				)
			})
		}
	})

	t.Run("with warnings", func(t *testing.T) {
		// Disable auto stats so that they don't interfere.
		r.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;")
		defer r.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = true;")
		r.Exec(t, "CREATE TABLE warnings (k INT PRIMARY KEY);")
		// Insert fake stats so that the estimate for the scan is inaccurate.
		r.Exec(t, `ALTER TABLE warnings INJECT STATISTICS '[{
                            "columns": ["k"],
                            "created_at": "2022-08-23 00:00:00.000000",
                            "distinct_count": 10000,
                            "name": "__auto__",
                            "null_count": 0,
                            "row_count": 10000
                        }]'`,
		)
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM warnings")
		// Check that we have a warning about inaccurate stats.
		var warningFound bool
		for _, row := range rows {
			if len(row) > 1 {
				t.Fatalf("unexpectedly more than a single string is returned in %v", row)
			}
			if strings.HasPrefix(row[0], "WARNING") {
				warningFound = true
			}
		}
		if !warningFound {
			t.Fatalf("warning not found in %v", rows)
		}
	})

	t.Run("foreign keys", func(t *testing.T) {
		r.Exec(t, "CREATE TABLE parent (pk INT PRIMARY KEY, v INT);")
		r.Exec(t, "CREATE TABLE child (pk INT PRIMARY KEY, fk INT REFERENCES parent(pk));")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM child")
		checkBundle(
			t, fmt.Sprint(rows), "child", func(name, contents string) error {
				if name == "schema.sql" {
					reg := regexp.MustCompile("CREATE TABLE public.parent")
					if reg.FindString(contents) == "" {
						return errors.Newf(
							"could not find 'CREATE TABLE public.parent' in schema.sql:\n%s", contents)
					}
				}
				return nil
			},
			base, plans, "stats-defaultdb.public.parent.sql", "stats-defaultdb.public.child.sql",
			"distsql.html vec.txt vec-v.txt",
		)
	})

	t.Run("redact", func(t *testing.T) {
		r.Exec(t, "CREATE TABLE pterosaur (cardholder STRING PRIMARY KEY, cardno INT, INDEX (cardno));")
		r.Exec(t, "INSERT INTO pterosaur VALUES ('pterodactyl', 5555555555554444);")
		r.Exec(t, "CREATE STATISTICS jurassic FROM pterosaur;")
		r.Exec(t, "CREATE FUNCTION test_redact() RETURNS STRING AS $body$ SELECT 'pterodactyl' $body$ LANGUAGE sql;")
		rows := r.QueryStr(t,
			"EXPLAIN ANALYZE (DEBUG, REDACT) SELECT max(cardno), test_redact() FROM pterosaur WHERE cardholder = 'pterodactyl'",
		)
		verboten := []string{"pterodactyl", "5555555555554444", fmt.Sprintf("%x", 5555555555554444)}
		checkBundle(
			t, fmt.Sprint(rows), "", func(name, contents string) error {
				lowerContents := strings.ToLower(contents)
				for _, pii := range verboten {
					if strings.Contains(lowerContents, pii) {
						return errors.Newf("file %s contained %q:\n%s\n", name, pii, contents)
					}
				}
				return nil
			},
			plans, "statement.sql stats-defaultdb.public.pterosaur.sql env.sql vec.txt vec-v.txt",
		)
	})

	t.Run("udfs", func(t *testing.T) {
		r.Exec(t, "CREATE FUNCTION add(a INT, b INT) RETURNS INT IMMUTABLE LEAKPROOF LANGUAGE SQL AS 'SELECT a + b';")
		r.Exec(t, "CREATE FUNCTION subtract(a INT, b INT) RETURNS INT IMMUTABLE LEAKPROOF LANGUAGE SQL AS 'SELECT a - b';")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT add(3, 4);")
		checkBundle(
			t, fmt.Sprint(rows), "add", func(name, contents string) error {
				if name == "schema.sql" {
					reg := regexp.MustCompile("add")
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for 'add' function in schema.sql")
					}
					reg = regexp.MustCompile("subtract")
					if reg.FindString(contents) != "" {
						return errors.Errorf("Found irrelevant user defined function 'substract' in schema.sql")
					}
				}
				return nil
			}, base, plans,
			"distsql-1-subquery.html distsql-2-main-query.html vec-1-subquery-v.txt vec-1-subquery.txt vec-2-main-query-v.txt vec-2-main-query.txt")
	})
}

// checkBundle searches text strings for a bundle URL and then verifies that the
// bundle contains the expected files. The expected files are passed as an
// arbitrary number of strings; each string contains one or more filenames
// separated by a space.
func checkBundle(
	t *testing.T,
	text, tableName string,
	contentCheck func(name, contents string) error,
	expectedFiles ...string,
) {
	httpClient := httputil.NewClientWithTimeout(30 * time.Second)

	t.Helper()
	reg := regexp.MustCompile("http://[a-zA-Z0-9.:]*/_admin/v1/stmtbundle/[0-9]*")
	url := reg.FindString(text)
	if url == "" {
		t.Fatalf("couldn't find URL in response '%s'", text)
	}
	// Download the zip to a BytesBuffer.
	resp, err := httpClient.Get(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, resp.Body)

	unzip, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Errorf("%q\n", buf.String())
		t.Fatal(err)
	}

	// Make sure the bundle contains the expected list of files.
	var files []string
	foundSchema := false
	for _, f := range unzip.File {
		t.Logf("found file: %s", f.Name)
		if f.UncompressedSize64 == 0 {
			t.Fatalf("file %s is empty", f.Name)
		}
		files = append(files, f.Name)

		r, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		bytes, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		contents := string(bytes)

		if strings.Contains(contents, "-- error") {
			t.Errorf(
				"expected no errors in %s, file contents:\n%s",
				f.Name, contents,
			)
		}

		if f.Name == "schema.sql" {
			foundSchema = true
			if tableName != "" && !strings.Contains(contents, tableName) {
				t.Errorf(
					"expected table name to appear in schema.sql. tableName: %s\nfile contents:\n%s",
					tableName, contents,
				)
			}
		}

		if contentCheck != nil {
			if err := contentCheck(f.Name, contents); err != nil {
				t.Error(err)
			}
		}
	}
	if !foundSchema {
		t.Errorf("expected schema.sql to be included, was missing")
	}

	var expList []string
	for _, s := range expectedFiles {
		expList = append(expList, strings.Split(s, " ")...)
	}
	sort.Strings(files)
	sort.Strings(expList)
	if fmt.Sprint(files) != fmt.Sprint(expList) {
		t.Errorf("unexpected list of files:\n  %v\nexpected:\n  %v", files, expList)
	}
}
