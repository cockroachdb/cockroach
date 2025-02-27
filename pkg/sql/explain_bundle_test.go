// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgtest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestExplainAnalyzeDebugWithTxnRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	retryFilter, verifyRetryHit := testutils.TestingRequestFilterRetryTxnWithPrefix(t, "stmt-diag-", 1)
	srv, godb, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: retryFilter,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(godb)
	r.Exec(t, `CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT UNIQUE);
CREATE SCHEMA s;
CREATE TABLE s.a (a INT PRIMARY KEY);`)

	base := "statement.sql trace.json trace.txt trace-jaeger.json env.sql"
	plans := "schema.sql opt.txt opt-v.txt opt-vv.txt plan.txt"

	// Set a small chunk size to test splitting into chunks. The bundle files are
	// on the order of 10KB.
	r.Exec(t, "SET CLUSTER SETTING sql.stmt_diagnostics.bundle_chunk_size = '2000'")

	rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM abc WHERE c=1")
	checkBundle(
		t, fmt.Sprint(rows), "public.abc", nil, false, /* expectErrors */
		base, plans, "stats-defaultdb.public.abc.sql", "distsql.html vec.txt vec-v.txt",
	)
	verifyRetryHit()
}

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
			t, fmt.Sprint(rows), "", nil, false, /* expectErrors */
			base, plans, "distsql.html vec.txt vec-v.txt",
		)
	})

	t.Run("basic", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM abc WHERE c=1")
		checkBundle(
			t, fmt.Sprint(rows), "public.abc", nil, false, /* expectErrors */
			base, plans, "stats-defaultdb.public.abc.sql", "distsql.html vec.txt vec-v.txt",
		)
	})

	// Check that we get separate diagrams for subqueries.
	t.Run("subqueries", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT EXISTS (SELECT * FROM abc WHERE c=1)")
		checkBundle(
			t, fmt.Sprint(rows), "public.abc", nil, false, /* expectErrors */
			base, plans, "stats-defaultdb.public.abc.sql", "distsql-2-main-query.html distsql-1-subquery.html vec-1-subquery-v.txt vec-1-subquery.txt vec-2-main-query-v.txt vec-2-main-query.txt",
		)
	})

	t.Run("user-defined schema", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM s.a WHERE a=1")
		checkBundle(
			t, fmt.Sprint(rows), "s.a", nil, false, /* expectErrors */
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
		checkBundle(t, fmt.Sprintf("%+v", pqErr.Detail), "", nil, false /* expectErrors */, base, plans, "distsql.html errors.txt")
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
		}, false /* expectErrors */, base, plans, "distsql.html vec.txt vec-v.txt")
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
			t, fmt.Sprint(rows), "public.users", nil, false /* expectErrors */, base, plans,
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
			t, fmt.Sprint(rows), "public.abc", nil, false, /* expectErrors */
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
			{"enable_zigzag_join", "on"},
			{"expect_and_ignore_not_visible_columns_in_copy", "on"},
			{"intervalstyle", "iso_8601"},
			{"large_full_scan_rows", "2000"},
			{"locality_optimized_partitioned_index_scan", "off"},
			// TODO(#129956): Enable this once non-default NULLS ordering with
			// subqueries is allowed in tests.
			// {"null_ordered_last", "on"},
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
			{"default_transaction_isolation", "'read committed'"},
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
							if _, err := parser.Parse(contents); err != nil {
								return errors.Wrap(err, "could not parse env.sql")
							}
						}
						return nil
					}, false, /* expectErrors */
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

	t.Run("foreign keys, mutations", func(t *testing.T) {
		// Use the following hierarchy of tables:
		//
		//               parent
		//            ?/       \
		//         child1     child2
		//          /            \
		//     grandchild1     grandchild2
		//         /
		//   greatgrandchild1
		//
		// Presence of and type of the FK from 'child1' to 'parent' changes
		// between test cases.
		r.Exec(t, "CREATE TABLE parent (pk INT PRIMARY KEY, v INT);")
		r.Exec(t, "CREATE TABLE child1 (pk INT PRIMARY KEY, fk INT);")
		r.Exec(t, "CREATE TABLE child2 (pk INT PRIMARY KEY, fk INT REFERENCES parent(pk));")
		r.Exec(t, "CREATE TABLE grandchild1 (pk INT PRIMARY KEY, fk INT REFERENCES child1(pk));")
		r.Exec(t, "CREATE TABLE grandchild2 (pk INT PRIMARY KEY, fk INT REFERENCES child2(pk));")
		r.Exec(t, "CREATE TABLE greatgrandchild1 (pk INT PRIMARY KEY, fk INT REFERENCES grandchild1(pk));")
		addChildFKDefault := "ALTER TABLE child1 ADD CONSTRAINT fk FOREIGN KEY(fk) REFERENCES parent(pk);"
		addChildFKCascade := "ALTER TABLE child1 ADD CONSTRAINT fk FOREIGN KEY(fk) REFERENCES parent(pk) ON DELETE CASCADE ON UPDATE CASCADE;"
		dropChildFK := "ALTER TABLE child1 DROP CONSTRAINT fk;"
		insertIntoParent := "INSERT INTO parent VALUES (1, 1)"
		getExpectedFiles := func(tables []string, numPostqueries int) string {
			var sb strings.Builder
			// We always have the main query.
			if numPostqueries > 0 {
				sb.WriteString("distsql-1-main-query.html vec-1-main-query-v.txt vec-1-main-query.txt")
			} else {
				sb.WriteString("distsql.html vec-v.txt vec.txt")
			}
			// Each postquery gets these files too.
			for i := 0; i < numPostqueries; i++ {
				sb.WriteString(fmt.Sprintf(" distsql-%[1]d-postquery.html vec-%[1]d-postquery-v.txt vec-%[1]d-postquery.txt", i+2))
			}
			// Every table gets a stats file.
			for _, table := range tables {
				sb.WriteString(fmt.Sprintf(" stats-defaultdb.public.%s.sql", table))
			}
			return sb.String()
		}

		for _, tc := range []struct {
			name          string
			query         string
			setup         []string
			cleanup       []string
			tableName     string
			expectedFiles string
		}{
			// Modifying 'child1', i.e. the origin table. Main focus in these
			// test cases is on whether the referenced table 'parent' is
			// included or not. We also make sure to not pull in 'child2' which
			// is a "sibling".
			{
				// Need to perform FK check only on 'grandchild1'. Because
				// 'grandchild1' is directly referenced by the metadata, we also
				// pull in 'greatgrandchild1' which isn't actually needed.
				name:          "DELETE FROM child",
				query:         "DELETE FROM child1 WHERE true",
				setup:         []string{addChildFKDefault},
				cleanup:       []string{dropChildFK},
				tableName:     "child1",
				expectedFiles: getExpectedFiles([]string{"child1", "grandchild1", "greatgrandchild1"}, 1 /* numPostqueries */),
			},
			{
				// With no FK, inserting into 'child1' shouldn't pull in
				// 'parent'.
				name:          "INSERT INTO child, no FK",
				query:         "INSERT INTO child1 VALUES (1, 1)",
				setup:         []string{insertIntoParent},
				cleanup:       []string{"DELETE FROM child1 WHERE true", "DELETE FROM parent WHERE true"},
				tableName:     "child1",
				expectedFiles: getExpectedFiles([]string{"child1"}, 0 /* numPostqueries */),
			},
			{
				// With FK, inserting into 'child1' should pull in 'parent'.
				name:  "INSERT INTO child",
				query: "INSERT INTO child1 VALUES (1, 1)",
				// Disable insert fast path so that the FK check got planned as
				// a postquery.
				setup:         []string{addChildFKDefault, insertIntoParent, "SET enable_insert_fast_path = false;"},
				cleanup:       []string{dropChildFK, "DELETE FROM child1 WHERE true", "DELETE FROM parent WHERE true"},
				tableName:     "child1",
				expectedFiles: getExpectedFiles([]string{"parent", "child1"}, 1 /* numPostqueries */),
			},
			{
				// 'child1' and 'grandchild1' are directly referenced by the
				// metadata. We also pull in 'greatgrandchild1' which isn't
				// actually needed. Additionally, even though this particular
				// UPDATE doesn't modify 'child1.fk', we still pull in 'parent'.
				name:          "UPDATE child",
				query:         "UPDATE child1 SET pk = pk + 1 WHERE true",
				setup:         []string{addChildFKDefault},
				cleanup:       []string{dropChildFK},
				tableName:     "child1",
				expectedFiles: getExpectedFiles([]string{"parent", "child1", "grandchild1", "greatgrandchild1"}, 1 /* numPostqueries */),
			},

			// Modifying 'parent', i.e. the referenced table.
			{
				// Need to perform FK check only on 'child2'. Because 'child2'
				// is directly referenced by the metadata, we also pull in
				// 'grandchild2' which isn't actually needed.
				name:          "DELETE FROM parent, no FK",
				query:         "DELETE FROM parent WHERE true",
				setup:         []string{},
				cleanup:       []string{},
				tableName:     "parent",
				expectedFiles: getExpectedFiles([]string{"parent", "child2", "grandchild2"}, 1 /* numPostqueries */),
			},
			{
				// Need to perform FK check on 'child1' and 'child2'. Because
				// both tables are directly referenced by the metadata, we also
				// pull in 'grandchild1' and 'grandchild2' which aren't actually
				// needed.
				name:          "DELETE FROM parent, default FK",
				query:         "DELETE FROM parent WHERE true",
				setup:         []string{addChildFKDefault},
				cleanup:       []string{dropChildFK},
				tableName:     "parent",
				expectedFiles: getExpectedFiles([]string{"parent", "child1", "child2", "grandchild1", "grandchild2"}, 2 /* numPostqueries */),
			},
			{
				// Need to perform FK check on 'child1' and 'child2'. Because
				// 'child2' is directly referenced by the metadata, we also pull
				// in 'grandchild2' which isn't actually needed.
				name:      "DELETE FROM parent, short-circuited CASCADE FK",
				query:     "DELETE FROM parent WHERE true",
				setup:     []string{addChildFKCascade},
				cleanup:   []string{dropChildFK},
				tableName: "parent",
				// Note that here the CASCADE is short-circuited, so we omit its
				// postquery.
				expectedFiles: getExpectedFiles([]string{"parent", "child1", "child2", "grandchild1", "grandchild2"}, 1 /* numPostqueries */),
			},
			{
				// Same as above but the CASCADE is actually executed, so we get
				// a CASCADE postquery as well as the FK check for
				// 'grandchild1'.
				name:          "DELETE FROM parent, CASCADE FK",
				query:         "DELETE FROM parent WHERE true",
				setup:         []string{addChildFKCascade, insertIntoParent},
				cleanup:       []string{dropChildFK},
				tableName:     "parent",
				expectedFiles: getExpectedFiles([]string{"parent", "child1", "child2", "grandchild1", "grandchild2"}, 3 /* numPostqueries */),
			},
			{
				// Inserting into 'parent' shouldn't pull in any other tables.
				name:          "INSERT INTO parent",
				query:         "INSERT INTO parent VALUES (1, 1)",
				setup:         []string{addChildFKDefault},
				cleanup:       []string{dropChildFK, "DELETE FROM parent WHERE true"},
				tableName:     "parent",
				expectedFiles: getExpectedFiles([]string{"parent"}, 0 /* numPostqueries */),
			},
			{
				// Need to perform FK check only on 'child2'. Because 'child2'
				// is directly referenced by the metadata, we also pull in
				// 'grandchild2' which isn't actually needed.
				name:          "UPDATE parent, no FK",
				query:         "UPDATE parent SET pk = pk + 1 WHERE true",
				setup:         []string{},
				cleanup:       []string{},
				tableName:     "parent",
				expectedFiles: getExpectedFiles([]string{"parent", "child2", "grandchild2"}, 1 /* numPostqueries */),
			},
			{
				// Need to perform FK check on 'child1' and 'child2'. Because
				// both tables are directly referenced by the metadata, we also
				// pull in 'grandchild1' and 'grandchild2' which aren't actually
				// needed.
				name:          "UPDATE parent, default FK",
				query:         "UPDATE parent SET pk = pk + 1 WHERE true",
				setup:         []string{addChildFKDefault},
				cleanup:       []string{dropChildFK},
				tableName:     "parent",
				expectedFiles: getExpectedFiles([]string{"parent", "child1", "child2", "grandchild1", "grandchild2"}, 2 /* numPostqueries */),
			},
			{
				// Need to perform FK check on 'child1' and 'child2'. Because
				// 'child2' is directly referenced by the metadata, we also pull
				// in 'grandchild2' which isn't actually needed.
				name:      "UPDATE parent, short-circuited CASCADE FK",
				query:     "UPDATE parent SET pk = pk + 1 WHERE true",
				setup:     []string{addChildFKCascade},
				cleanup:   []string{dropChildFK},
				tableName: "parent",
				// Note that here the CASCADE is short-circuited, so we omit its
				// postquery.
				expectedFiles: getExpectedFiles([]string{"parent", "child1", "child2", "grandchild1", "grandchild2"}, 1 /* numPostqueries */),
			},
			{
				// Same as above but the CASCADE is actually executed, so we get
				// a CASCADE postquery as well as the FK check for
				// 'grandchild1'.
				name:          "UPDATE parent, CASCADE FK",
				query:         "UPDATE parent SET pk = pk + 1 WHERE true",
				setup:         []string{addChildFKCascade, insertIntoParent},
				cleanup:       []string{dropChildFK, "DELETE FROM parent WHERE true"},
				tableName:     "parent",
				expectedFiles: getExpectedFiles([]string{"parent", "child1", "child2", "grandchild1", "grandchild2"}, 3 /* numPostqueries */),
			},
			{
				// When we have an FK cycle, we still want to pull in most
				// tables.
				name:      "DELETE FROM parent, FK cycle",
				query:     "DELETE FROM parent WHERE true",
				setup:     []string{addChildFKCascade, "ALTER TABLE parent ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES child1(pk) ON DELETE CASCADE"},
				cleanup:   []string{dropChildFK, "ALTER TABLE parent DROP CONSTRAINT fk"},
				tableName: "parent",
				// Note that here the CASCADE is short-circuited, so we omit its
				// postquery.
				expectedFiles: getExpectedFiles([]string{"parent", "child1", "child2", "grandchild1", "grandchild2"}, 1 /* numPostqueries */),
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				defer func() {
					for _, q := range tc.cleanup {
						r.Exec(t, q)
					}
				}()
				for _, q := range tc.setup {
					r.Exec(t, q)
				}
				rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) "+tc.query)
				// Note that in this subtest we have nil contentCheck because we
				// rely on the presence of stats files to indicate which tables
				// are included. We also assume that necessary FKs are included
				// in the schema.sql file too.
				checkBundle(
					t, fmt.Sprint(rows), tc.tableName, nil /* contentCheck */, false, /* expectErrors */
					base, plans, tc.expectedFiles,
				)
			})
		}
	})

	t.Run("foreign keys, read-only", func(t *testing.T) {
		tableNames := []string{"parent", "child1", "child2", "grandchild1", "grandchild2"}
		r.Exec(t, "DROP TABLE IF EXISTS greatgrandchild1;")
		r.Exec(t, "DROP TABLE IF EXISTS grandchild2;")
		r.Exec(t, "DROP TABLE IF EXISTS grandchild1;")
		r.Exec(t, "DROP TABLE IF EXISTS child2;")
		r.Exec(t, "DROP TABLE IF EXISTS child1;")
		r.Exec(t, "DROP TABLE IF EXISTS parent;")
		r.Exec(t, "CREATE TABLE parent (pk INT PRIMARY KEY, v INT);")
		r.Exec(t, "CREATE TABLE child1 (pk INT PRIMARY KEY, fk INT REFERENCES parent(pk));")
		r.Exec(t, "CREATE TABLE child2 (pk INT PRIMARY KEY, fk INT REFERENCES parent(pk));")
		r.Exec(t, "CREATE TABLE grandchild1 (pk INT PRIMARY KEY, fk INT REFERENCES child1(pk));")
		r.Exec(t, "CREATE TABLE grandchild2 (pk INT PRIMARY KEY, fk INT REFERENCES child2(pk));")
		// Only the target tables should be included since we perform a
		// read-only stmt.
		getContentCheckFn := func(targetTableNames, targetFKs []string) func(name, contents string) error {
			return func(name, contents string) error {
				if name == "schema.sql" {
					for _, targetTableName := range targetTableNames {
						if regexp.MustCompile("USE defaultdb;\nCREATE TABLE public."+targetTableName).FindString(contents) == "" {
							return errors.Newf(
								"could not find target table 'USE defaultdb;\nCREATE TABLE public.%s' in schema.sql:\n%s", targetTableName, contents)
						}
					}
					for _, tableName := range tableNames {
						var isTarget bool
						for _, targetTableName := range targetTableNames {
							if targetTableName == tableName {
								isTarget = true
								break
							}
						}
						if isTarget {
							continue
						}
						if regexp.MustCompile("USE defaultdb;\nCREATE TABLE public."+tableName).FindString(contents) != "" {
							return errors.Newf(
								"unexpectedly found non-target table 'USE defaultdb;\nCREATE TABLE public.%s' in schema.sql:\n%s", tableName, contents)
						}
					}
					// Now confirm that only relevant FKs are included.
					numFoundFKs := strings.Count(contents, "FOREIGN KEY")
					if numFoundFKs != len(targetFKs) {
						return errors.Newf("found %d FKs, expected %d\n%s", numFoundFKs, len(targetFKs), contents)
					}
					for _, fk := range targetFKs {
						if !strings.Contains(contents, fk) {
							return errors.Newf("didn't find target FK: %s\n%s", fk, contents)
						}
					}
				}
				return nil
			}
		}
		// First read each table separately.
		for _, tableName := range tableNames {
			targetTableName := tableName
			// There should be no FKs included.
			contentCheck := getContentCheckFn([]string{targetTableName}, nil /* targetFKs */)
			rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM "+targetTableName)
			checkBundle(
				t, fmt.Sprint(rows), targetTableName, contentCheck, false, /* expectErrors */
				base, plans, fmt.Sprintf("stats-defaultdb.public.%s.sql", targetTableName),
				"distsql.html vec.txt vec-v.txt",
			)
		}
		// Now read different combinations of tables which will influence
		// whether ADD CONSTRAINT ... FOREIGN KEY statements are included.
		contentCheck := getContentCheckFn([]string{"parent", "child1"}, []string{"ALTER TABLE defaultdb.public.child1 ADD CONSTRAINT"})
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM parent, child1")
		checkBundle(
			t, fmt.Sprint(rows), "parent", contentCheck, false, /* expectErrors */
			base, plans, "stats-defaultdb.public.parent.sql stats-defaultdb.public.child1.sql distsql.html vec.txt vec-v.txt",
		)

		// There should be no FKs since there isn't a direct link between the
		// tables.
		contentCheck = getContentCheckFn([]string{"parent", "grandchild1"}, nil /* targetFKs */)
		rows = r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM parent, grandchild1")
		checkBundle(
			t, fmt.Sprint(rows), "parent", contentCheck, false, /* expectErrors */
			base, plans, "stats-defaultdb.public.parent.sql stats-defaultdb.public.grandchild1.sql distsql.html vec.txt vec-v.txt",
		)

		// Note that we omit the FK from grandchild1 since the FK referenced
		// table isn't being read.
		contentCheck = getContentCheckFn([]string{"parent", "child2", "grandchild1"}, []string{"ALTER TABLE defaultdb.public.child2 ADD CONSTRAINT"})
		rows = r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM parent, child2, grandchild1")
		checkBundle(
			t, fmt.Sprint(rows), "parent", contentCheck, false, /* expectErrors */
			base, plans, "stats-defaultdb.public.parent.sql stats-defaultdb.public.child2.sql stats-defaultdb.public.grandchild1.sql distsql.html vec.txt vec-v.txt",
		)

		contentCheck = getContentCheckFn(
			[]string{"parent", "child1", "grandchild1"},
			[]string{
				"ALTER TABLE defaultdb.public.child1 ADD CONSTRAINT",
				"ALTER TABLE defaultdb.public.grandchild1 ADD CONSTRAINT",
			})
		rows = r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM parent, child1, grandchild1")
		checkBundle(
			t, fmt.Sprint(rows), "parent", contentCheck, false, /* expectErrors */
			base, plans, "stats-defaultdb.public.parent.sql stats-defaultdb.public.child1.sql stats-defaultdb.public.grandchild1.sql distsql.html vec.txt vec-v.txt",
		)
	})

	// getBundleThroughBuiltin is a helper function that returns an url to
	// download a stmt bundle that was collected in response to a diagnostics
	// request inserted by the builtin.
	getBundleThroughBuiltin := func(fprint, query, planGist string, redacted bool) string {
		// Delete all old diagnostics to make this test easier.
		r.Exec(t, "DELETE FROM system.statement_diagnostics WHERE true")

		// Insert the diagnostics request via the builtin function.
		row := r.QueryRow(t, `SELECT crdb_internal.request_statement_bundle($1, $2, 0::FLOAT, 0::INTERVAL, 0::INTERVAL, $3);`, fprint, planGist, redacted)
		var inserted bool
		row.Scan(&inserted)
		require.True(t, inserted)

		// Now actually execute the query so that the bundle is collected.
		r.Exec(t, query)

		// Get ID of our bundle.
		var id int
		var bundleFingerprint string
		row = r.QueryRow(t, "SELECT id, statement_fingerprint FROM system.statement_diagnostics LIMIT 1")
		row.Scan(&id, &bundleFingerprint)
		require.Equal(t, fprint, bundleFingerprint)

		// We need to come up with the url to download the bundle from.
		return findBundleDownloadURL(t, r, id)
	}

	t.Run("redact", func(t *testing.T) {
		r.Exec(t, "CREATE TYPE plesiosaur AS ENUM ('pterodactyl', '5555555555554444');")
		r.Exec(t, "CREATE TABLE pterosaur (cardholder STRING PRIMARY KEY, cardno INT, INDEX (cardno));")
		r.Exec(t, "INSERT INTO pterosaur VALUES ('pterodactyl', 5555555555554444);")
		r.Exec(t, "CREATE STATISTICS jurassic FROM pterosaur;")
		r.Exec(t, "CREATE FUNCTION test_redact() RETURNS STRING AS $body$ SELECT 'pterodactyl' $body$ LANGUAGE sql;")
		for _, viaBuiltin := range []bool{false, true} {
			t.Run(fmt.Sprintf("viaBuiltin=%t", viaBuiltin), func(t *testing.T) {
				var url string
				if viaBuiltin {
					fprint := "SELECT max(cardno), test_redact() FROM pterosaur WHERE cardholder = _"
					query := "SELECT max(cardno), test_redact() FROM pterosaur WHERE cardholder = 'pterodactyl';"
					// Collect a bundle in response to a diagnostics request
					// inserted by the builtin.
					url = getBundleThroughBuiltin(fprint, query, "" /* planGist */, true /* redacted */)
				} else {
					rows := r.QueryStr(t,
						"EXPLAIN ANALYZE (DEBUG, REDACT) SELECT max(cardno), test_redact() FROM pterosaur WHERE cardholder = 'pterodactyl'",
					)
					url = getBundleDownloadURL(t, fmt.Sprint(rows))
				}
				verboten := []string{"pterodactyl", "5555555555554444", fmt.Sprintf("%x", 5555555555554444)}
				checkBundleContents(
					t, url, "", func(name, contents string) error {
						lowerContents := strings.ToLower(contents)
						for _, pii := range verboten {
							if strings.Contains(lowerContents, pii) {
								return errors.Newf("file %s contained %q:\n%s\n", name, pii, contents)
							}
						}
						return nil
					}, false, /* expectErrors */
					plans, "statement.sql stats-defaultdb.public.pterosaur.sql env.sql vec.txt vec-v.txt",
				)
			})
		}
	})

	t.Run("types", func(t *testing.T) {
		r.Exec(t, "CREATE TYPE test_type1 AS ENUM ('hello','world');")
		r.Exec(t, "CREATE TYPE test_type2 AS ENUM ('goodbye','earth');")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT 'hello'::test_type1;")
		checkBundle(
			t, fmt.Sprint(rows), "test_type1", func(name, contents string) error {
				if name == "schema.sql" {
					reg := regexp.MustCompile("test_type1")
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for 'test_type1' type in schema.sql")
					}
					reg = regexp.MustCompile("test_type2")
					if reg.FindString(contents) != "" {
						return errors.Errorf("Found irrelevant user defined type 'test_type2' in schema.sql")
					}
				}
				return nil
			}, false, /* expectErrors */
			base, plans, "distsql.html vec.txt vec-v.txt",
		)
	})

	t.Run("udfs", func(t *testing.T) {
		r.Exec(t, "CREATE FUNCTION add_func(a INT, b INT) RETURNS INT IMMUTABLE LEAKPROOF LANGUAGE SQL AS 'SELECT a + b';")
		r.Exec(t, "CREATE FUNCTION subtract_func(a INT, b INT) RETURNS INT IMMUTABLE LEAKPROOF LANGUAGE SQL AS 'SELECT a - b';")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT add_func(3, 4);")
		checkBundle(
			t, fmt.Sprint(rows), "add_func", func(name, contents string) error {
				if name == "schema.sql" {
					reg := regexp.MustCompile("add_func")
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for 'add_func' function in schema.sql")
					}
					reg = regexp.MustCompile("subtract_func")
					if reg.FindString(contents) != "" {
						return errors.Errorf("Found irrelevant user defined function 'subtract_func' in schema.sql")
					}
				}
				return nil
			}, false /* expectErrors */, base, plans,
			"distsql.html vec-v.txt vec.txt")
	})

	t.Run("procedures", func(t *testing.T) {
		r.Exec(t, "CREATE PROCEDURE add_proc(a INT, b INT) LANGUAGE SQL AS 'SELECT a + b';")
		r.Exec(t, "CREATE PROCEDURE subtract_proc(a INT, b INT) LANGUAGE SQL AS 'SELECT a - b';")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) CALL add_proc(3, 4);")
		checkBundle(
			t, fmt.Sprint(rows), "add_proc", func(name, contents string) error {
				if name == "schema.sql" {
					reg := regexp.MustCompile("add_proc")
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for 'add_proc' procedure in schema.sql")
					}
					reg = regexp.MustCompile("subtract_proc")
					if reg.FindString(contents) != "" {
						return errors.Errorf("Found irrelevant procedure 'subtract_proc' in schema.sql")
					}
				}
				return nil
			}, false /* expectErrors */, base, plans,
			"distsql.html vec-v.txt vec.txt")
	})

	t.Run("different schema UDF", func(t *testing.T) {
		r.Exec(t, "CREATE FUNCTION foo() RETURNS INT LANGUAGE SQL AS 'SELECT count(*) FROM abc, s.a';")
		r.Exec(t, "CREATE FUNCTION s.foo() RETURNS INT LANGUAGE SQL AS 'SELECT count(*) FROM abc, s.a';")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT s.foo();")
		checkBundle(
			t, fmt.Sprint(rows), "s.foo", func(name, contents string) error {
				if name == "schema.sql" {
					reg := regexp.MustCompile(`s\.foo`)
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for 's.foo' function in schema.sql")
					}
					reg = regexp.MustCompile(`^CREATE FUNCTION public\.foo`)
					if reg.FindString(contents) != "" {
						return errors.Errorf("found irrelevant function 'foo' in schema.sql")
					}
					reg = regexp.MustCompile(`s\.a`)
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for relation 's.a' in schema.sql")
					}
					reg = regexp.MustCompile("abc")
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for relation 'abc' in schema.sql")
					}
				}
				return nil
			},
			false /* expectErrors */, base, plans,
			"stats-defaultdb.public.abc.sql stats-defaultdb.s.a.sql distsql.html vec-v.txt vec.txt",
		)
	})

	t.Run("different schema procedure", func(t *testing.T) {
		r.Exec(t, "CREATE PROCEDURE bar() LANGUAGE SQL AS 'SELECT count(*) FROM abc, s.a';")
		r.Exec(t, "CREATE PROCEDURE s.bar() LANGUAGE SQL AS 'SELECT count(*) FROM abc, s.a';")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) CALL s.bar();")
		checkBundle(
			t, fmt.Sprint(rows), "s.bar", func(name, contents string) error {
				if name == "schema.sql" {
					reg := regexp.MustCompile(`s\.bar`)
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for 's.bar' procedure in schema.sql")
					}
					reg = regexp.MustCompile(`^CREATE PROCEDURE public\.bar`)
					if reg.FindString(contents) != "" {
						return errors.Errorf("Found irrelevant procedure 'bar' in schema.sql")
					}
					reg = regexp.MustCompile(`s\.a`)
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for relation 's.a' in schema.sql")
					}
					reg = regexp.MustCompile("abc")
					if reg.FindString(contents) == "" {
						return errors.Errorf("could not find definition for relation 'abc' in schema.sql")
					}
				}
				return nil
			},
			false /* expectErrors */, base, plans,
			"stats-defaultdb.public.abc.sql stats-defaultdb.s.a.sql distsql.html vec-v.txt vec.txt",
		)
	})

	t.Run("permission error", func(t *testing.T) {
		r.Exec(t, "CREATE USER test")
		r.Exec(t, "SET ROLE test")
		defer r.Exec(t, "SET ROLE root")
		r.Exec(t, "CREATE TABLE permissions (k PRIMARY KEY) AS SELECT 1")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM permissions")
		// Check that we see an error about missing privileges for the cluster
		// settings as a warnings. (Since `test` is the table owner, it already
		// has permissions on the table itself.)
		var numErrors int
		for _, row := range rows {
			if strings.HasPrefix(row[0], "-- error getting cluster settings:") {
				numErrors++
			}
		}
		if numErrors != 1 {
			t.Fatalf("didn't see 1 error in %v", rows)
		}
		checkBundle(
			t, fmt.Sprint(rows), "permission" /* tableName */, nil /* contentCheck */, true, /* expectErrors */
			base, plans, "distsql.html errors.txt stats-defaultdb.public.permissions.sql vec.txt vec-v.txt",
		)
	})

	t.Run("with in-flight trace", func(t *testing.T) {
		r.Exec(t, "SET CLUSTER SETTING sql.stmt_diagnostics.in_flight_trace_collector.poll_interval = '0.25s'")
		defer r.Exec(t, "SET CLUSTER SETTING sql.stmt_diagnostics.in_flight_trace_collector.poll_interval = '0s'")
		// Sleep for 1s during the query execution to allow for the trace
		// collector goroutine to start.
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT pg_sleep(1)")
		checkBundle(
			t, fmt.Sprint(rows), "" /* tableName */, nil /* contentCheck */, false, /* expectErrors */
			base, plans, "distsql.html vec.txt vec-v.txt inflight-trace-n1.txt inflight-trace-jaeger-n1.json",
		)
	})

	t.Run("virtual table", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT count(*) FROM pg_catalog.pg_class;")
		// tableName is empty since we expect that the table is not included
		// into schema.sql.
		var tableName string
		contentCheck := func(name, contents string) error {
			if name != "schema.sql" {
				return nil
			}
			if strings.Contains(contents, "CREATE TABLE pg_catalog.pg_class") {
				return errors.New("virtual tables should be omitted from schema.sql")
			}
			return nil
		}
		checkBundle(
			t, fmt.Sprint(rows), tableName, contentCheck, false, /* expectErrors */
			// Note that the list of files doesn't include stats for the virtual
			// table - this will probably change when #27611 is addressed.
			base, plans, "distsql.html vec.txt vec-v.txt",
		)
	})

	t.Run("multiple databases", func(t *testing.T) {
		r.Exec(t, "CREATE DATABASE db1;")
		r.Exec(t, "CREATE DATABASE db2;")
		r.Exec(t, "CREATE SCHEMA db2.s2;")
		r.Exec(t, "CREATE TABLE db1.t1 (pk INT PRIMARY KEY);")
		r.Exec(t, "CREATE TABLE db2.s2.t2 (pk INT PRIMARY KEY);")
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM db1.t1, db2.s2.t2;")
		checkBundle(
			t, fmt.Sprint(rows), "public.t1", nil, false, /* expectErrors */
			base, plans, "distsql.html vec.txt vec-v.txt stats-db1.public.t1.sql stats-db2.s2.t2.sql",
		)
		checkBundle(
			t, fmt.Sprint(rows), "s2.t2", nil, false, /* expectErrors */
			base, plans, "distsql.html vec.txt vec-v.txt stats-db1.public.t1.sql stats-db2.s2.t2.sql",
		)
	})

	t.Run("multiple databases and special characters", func(t *testing.T) {
		r.Exec(t, `CREATE DATABASE "db.name";`)
		r.Exec(t, `CREATE DATABASE "db'name";`)
		r.Exec(t, `CREATE SCHEMA "db.name"."sc.name"`)
		r.Exec(t, `CREATE SCHEMA "db'name"."sc'name"`)
		r.Exec(t, `CREATE TABLE "db.name"."sc.name".t (pk INT PRIMARY KEY);`)
		r.Exec(t, `CREATE TABLE "db'name"."sc'name".t (pk INT PRIMARY KEY);`)
		rows := r.QueryStr(t, `EXPLAIN ANALYZE (DEBUG) SELECT * FROM "db.name"."sc.name".t, "db'name"."sc'name".t;`)
		checkBundle(
			t, fmt.Sprint(rows), `"sc.name".t`, nil, false, /* expectErrors */
			base, plans, `distsql.html vec.txt vec-v.txt stats-"db.name"."sc.name".t.sql stats-"db'name"."sc'name".t.sql`,
		)
		checkBundle(
			t, fmt.Sprint(rows), `"sc'name".t`, nil, false, /* expectErrors */
			base, plans, `distsql.html vec.txt vec-v.txt stats-"db.name"."sc.name".t.sql stats-"db'name"."sc'name".t.sql`,
		)
	})

	t.Run("plan-gist matching", func(t *testing.T) {
		r.Exec(t, "CREATE TABLE gist (k INT PRIMARY KEY);")
		r.Exec(t, "INSERT INTO gist SELECT generate_series(1, 10)")
		const fprint = `SELECT * FROM gist`

		// Come up with a target gist.
		row := r.QueryRow(t, "EXPLAIN (GIST) "+fprint)
		var gist string
		row.Scan(&gist)

		url := getBundleThroughBuiltin(fprint, fprint, gist, false /* redacted */)
		checkBundleContents(
			t, url, "gist", func(name, contents string) error {
				if name != "plan.txt" {
					return nil
				}
				// We don't hard-code the full expected output here so that it
				// doesn't need an update every time we change EXPLAIN ANALYZE
				// output format. Instead, we only assert that a few lines are
				// present in the output.
				for _, expectedLine := range []string{
					"• scan",
					"  sql nodes: n1",
					"  actual row count: 10",
					"  table: gist@gist_pkey",
					"  spans: FULL SCAN",
				} {
					if !strings.Contains(contents, expectedLine) {
						return errors.Newf("didn't find %q in the output: %v", expectedLine, contents)
					}
				}
				return nil
			}, false, /* expectErrors */
			base, plans, "distsql.html vec.txt vec-v.txt stats-defaultdb.public.gist.sql",
		)
	})
}

func getBundleDownloadURL(t *testing.T, text string) string {
	reg := regexp.MustCompile("http://[a-zA-Z0-9.:]*/_admin/v1/stmtbundle/[0-9]*")
	url := reg.FindString(text)
	if url == "" {
		t.Fatalf("couldn't find URL in response '%s'", text)
	}
	return url
}

func findBundleDownloadURL(t *testing.T, runner *sqlutils.SQLRunner, id int) string {
	// To come up with the url to download the bundle from, we collect another
	// stmt bundle, and in the output we'll have the url to this other stmt
	// bundle of the form:
	//   Direct link: http://127.0.0.1:65031/_admin/v1/stmtbundle/936793560822546433
	// We'll need to replace the last part with the ID of our bundle to get our
	// url.
	rows := runner.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT 1")
	urlTemplate := getBundleDownloadURL(t, sqlutils.MatrixToStr(rows))
	prefixLength := strings.LastIndex(urlTemplate, "/")
	return urlTemplate[:prefixLength] + "/" + strconv.Itoa(id)
}

func downloadBundle(t *testing.T, url string, dest io.Writer) {
	httpClient := httputil.NewClientWithTimeout(30 * time.Second)
	// Download the zip to a BytesBuffer.
	resp, err := httpClient.Get(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(dest, resp.Body)
}

func downloadAndUnzipBundle(t *testing.T, url string) *zip.Reader {
	// Download the zip to a BytesBuffer.
	var buf bytes.Buffer
	downloadBundle(t, url, &buf)

	unzip, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Errorf("%q\n", buf.String())
		t.Fatal(err)
	}
	return unzip
}

func readUnzippedFile(t *testing.T, f *zip.File) string {
	r, err := f.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	bytes, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(bytes)
}

// checkBundle searches text strings for a bundle URL and then verifies that the
// bundle contains the expected files. The expected files are passed as an
// arbitrary number of strings; each string contains one or more filenames
// separated by a space.
// - tableName: if non-empty, checkBundle asserts that the substring equal to
// tableName is present in schema.sql. It is expected to be either
// schema-qualified or just the table name.
// - expectErrors: if set, indicates that non-critical errors might have
// occurred during the bundle collection and shouldn't fail the test.
func checkBundle(
	t *testing.T,
	text, tableName string,
	contentCheck func(name string, contents string) error,
	expectErrors bool,
	expectedFiles ...string,
) {
	t.Helper()
	url := getBundleDownloadURL(t, text)
	checkBundleContents(t, url, tableName, contentCheck, expectErrors, expectedFiles...)
}

func checkBundleContents(
	t *testing.T,
	url string,
	tableName string,
	contentCheck func(name string, contents string) error,
	expectErrors bool,
	expectedFiles ...string,
) {
	unzip := downloadAndUnzipBundle(t, url)
	// Make sure the bundle contains the expected list of files.
	var files []string
	foundSchema := false
	for _, f := range unzip.File {
		t.Logf("found file: %s", f.Name)
		if f.UncompressedSize64 == 0 {
			t.Fatalf("file %s is empty", f.Name)
		}
		files = append(files, f.Name)

		contents := readUnzippedFile(t, f)
		if !expectErrors && strings.Contains(contents, "-- error") {
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

// TestExplainClientTime verifies that "client time" execution statistic is
// collected correctly. In particular, it executes a query that fetches two rows
// via the limited portal model and adds a sleep between reading two rows. As a
// result, it introduces a client time that should show up in the stmt bundle
// for this query execution.
func TestExplainClientTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Create a table with two rows and insert the diagnostics request for our
	// target query.
	testQuery := `SELECT * FROM t`
	runner.Exec(t, `CREATE TABLE t (k PRIMARY KEY) AS SELECT generate_series(1, 2)`)
	runner.Exec(t, fmt.Sprintf(`SELECT crdb_internal.request_statement_bundle('%s', '', 0.0::FLOAT, 0::INTERVAL, 0::INTERVAL)`, testQuery))

	// Connect to the cluster via the PGWire client.
	p, err := pgtest.NewPGTest(ctx, s.AdvSQLAddr(), username.RootUser)
	require.NoError(t, err)

	// Disable multiple active portals execution model since for some reason we
	// don't have the plan then. This feature is in preview mode and currently
	// disabled.
	// TODO(#118159): investigate this.
	require.NoError(t, p.SendOneLine(`Query {"String": "SET multiple_active_portals_enabled = false"}`))
	until := pgtest.ParseMessages("ReadyForQuery")
	_, err = p.Until(false /* keepErrMsg */, until...)
	require.NoError(t, err)

	// Execute the target query within the txn but only read one row.
	require.NoError(t, p.SendOneLine(`Query {"String": "BEGIN"}`))
	require.NoError(t, p.SendOneLine(fmt.Sprintf(`Parse {"Query": "%s"}`, testQuery)))
	require.NoError(t, p.SendOneLine(`Bind`))
	require.NoError(t, p.SendOneLine(`Execute {"MaxRows": 1}`))
	require.NoError(t, p.SendOneLine(`Sync`))

	// We need to receive until two 'ReadyForQuery' messages are returned (the
	// first one is for "COMMIT" query and the second one is for the limited
	// portal execution).
	until = pgtest.ParseMessages("ReadyForQuery\nReadyForQuery")
	msgs1, err := p.Until(false /* keepErrMsg */, until...)
	require.NoError(t, err)

	// Now inject some client time.
	time.Sleep(time.Second)

	// Now read the remaining row and commit the txn.
	require.NoError(t, p.SendOneLine(`Execute`))
	require.NoError(t, p.SendOneLine(`Sync`))
	require.NoError(t, p.SendOneLine(`Query {"String": "COMMIT"}`))

	// We need to receive until two 'ReadyForQuery' messages are returned (the
	// first one is for completing the target query and the second one is for
	// "COMMIT" query).
	until = pgtest.ParseMessages("ReadyForQuery\nReadyForQuery")
	msgs2, err := p.Until(false /* keepErrMsg */, until...)
	require.NoError(t, err)

	received := pgtest.MsgsToJSONWithIgnore(append(msgs1, msgs2...), &datadriven.TestData{})
	t.Log(received)

	// We should have collected the stmt bundle for the target query execution
	// (and there should only be one stmt bundle in the test server).
	r := runner.QueryRow(t, "SELECT id, statement_fingerprint from system.statement_diagnostics LIMIT 1")
	var id int
	var stmtFingerprint string
	r.Scan(&id, &stmtFingerprint)
	// Sanity check that we got the ID for our bundle.
	require.Equal(t, testQuery, stmtFingerprint)

	// We need to come up with the url to download the bundle from.
	url := findBundleDownloadURL(t, runner, id)
	// Now download the stmt bundle, unzip it and find plan.txt file.
	unzip := downloadAndUnzipBundle(t, url)
	var contents string
	for _, f := range unzip.File {
		if f.Name == "plan.txt" {
			contents = readUnzippedFile(t, f)
			t.Logf("contents of plan.txt\n%s", contents)
			break
		}
	}

	// Finally, the meat of the test - ensure that "client time" execution
	// statistic is at least 1s.
	clientTimeRegEx := regexp.MustCompile(`client time: ([\d\.]+)s`)
	matches := clientTimeRegEx.FindStringSubmatch(contents)
	if len(matches) == 0 {
		t.Fatal("didn't find the client time in the contents")
	}
	clientTime, err := strconv.ParseFloat(matches[1], 64)
	require.NoError(t, err)
	require.LessOrEqual(t, 1.0, clientTime)
}

func TestReplacePlaceholdersWithValuesForBundle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		statement          string
		stmtNoPlaceholders string
		numPlaceholders    int
	}{
		{
			statement:          `SELECT 1;`,
			stmtNoPlaceholders: `SELECT 1;`,
			numPlaceholders:    0,
		},
		{
			statement: `
SELECT * FROM t WHERE k = $1;

-- Arguments:
--  $1: 1
`,
			stmtNoPlaceholders: `SELECT * FROM t WHERE k = 1;`,
			numPlaceholders:    1,
		},
		// This test case abuses the notation a bit (by omitting some of the
		// placeholder values) and tests that substring collisions like $1 vs
		// $10 are handled correctly.
		{
			statement: `
SELECT a || $1 FROM t WHERE k = ($2 - $10);

-- Arguments:
--  $1: 'foo'
--  $2: 42
--  $10: 17
`,
			stmtNoPlaceholders: `SELECT a || 'foo' FROM t WHERE k = (42 - 17);`,
			numPlaceholders:    3,
		},
	} {
		s, p, err := ReplacePlaceholdersWithValuesForBundle(tc.statement)
		require.NoError(t, err)
		require.Equal(t, tc.stmtNoPlaceholders, s)
		require.Equal(t, tc.numPlaceholders, p)
	}
}

// TestExplainBundleEnv is a sanity check that all SET and SET CLUSTER SETTING
// statements in the env.sql file of the bundle are valid.
func TestExplainBundleEnv(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	execCfg := s.ExecutorConfig().(ExecutorConfig)
	sd := NewInternalSessionData(ctx, execCfg.Settings, "test")
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		kv.NewTxn(ctx, db, srv.NodeID()),
		username.NodeUserName(),
		&MemoryMetrics{},
		&execCfg,
		sd,
	)
	defer cleanup()
	p := internalPlanner.(*planner)
	c := makeStmtEnvCollector(ctx, p, s.InternalExecutor().(*InternalExecutor))

	var sb strings.Builder
	require.NoError(t, c.PrintSessionSettings(&sb, &s.ClusterSettings().SV, true /* all */))
	vars := strings.Split(sb.String(), "\n")
	for _, line := range vars {
		_, err := sqlDB.ExecContext(ctx, line)
		if err != nil {
			words := strings.Split(line, " ")
			t.Fatalf("%s\n%v: probably need to add %q into 'sessionVarNeedsEscaping' map", line, err, words[1])
		}
	}

	sb.Reset()
	require.NoError(t, c.PrintClusterSettings(&sb, true /* all */))
	vars = strings.Split(sb.String(), "\n")
	for _, line := range vars {
		_, err := sqlDB.ExecContext(ctx, line)
		if err != nil {
			t.Fatalf("unexpectedly couldn't execute %s: %v", line, err)
		}
	}
}
