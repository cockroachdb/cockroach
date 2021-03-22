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
	"io/ioutil"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"testing"

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

	base := "statement.txt trace.json trace.txt trace-jaeger.json env.sql"
	plans := "schema.sql opt.txt opt-v.txt opt-vv.txt plan.txt"

	// Set a small chunk size to test splitting into chunks. The bundle files are
	// on the order of 10KB.
	r.Exec(t, fmt.Sprintf(
		"SET CLUSTER SETTING sql.stmt_diagnostics.bundle_chunk_size = '%d'",
		5000+rand.Intn(10000),
	))

	t.Run("basic", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM abc WHERE c=1")
		checkBundle(
			t, fmt.Sprint(rows), "public.abc",
			base, plans, "stats-defaultdb.public.abc.sql", "distsql.html vec.txt vec-v.txt",
		)
	})

	// Check that we get separate diagrams for subqueries.
	t.Run("subqueries", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT EXISTS (SELECT * FROM abc WHERE c=1)")
		checkBundle(
			t, fmt.Sprint(rows), "public.abc",
			base, plans, "stats-defaultdb.public.abc.sql", "distsql-2-main-query.html distsql-1-subquery.html vec-1-subquery-v.txt vec-1-subquery.txt vec-2-main-query-v.txt vec-2-main-query.txt",
		)
	})

	t.Run("user-defined schema", func(t *testing.T) {
		rows := r.QueryStr(t, "EXPLAIN ANALYZE (DEBUG) SELECT * FROM s.a WHERE a=1")
		checkBundle(
			t, fmt.Sprint(rows), "s.a",
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
		checkBundle(t, fmt.Sprintf("%+v", pqErr.Detail), "", base)
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
			t, rowsBuf.String(), "public.abc",
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
			t, fmt.Sprint(rows), "public.users", base, plans,
			"stats-defaultdb.public.users.sql", "stats-defaultdb.public.promos.sql",
			"distsql-1-main-query.html distsql-2-postquery.html vec-1-main-query-v.txt vec-1-main-query.txt vec-2-postquery-v.txt vec-2-postquery.txt",
		)
		r.Exec(t, `RESET enable_insert_fast_path;`)
	})
}

// checkBundle searches text strings for a bundle URL and then verifies that the
// bundle contains the expected files. The expected files are passed as an
// arbitrary number of strings; each string contains one or more filenames
// separated by a space.
func checkBundle(t *testing.T, text, tableName string, expectedFiles ...string) {
	t.Helper()
	reg := regexp.MustCompile("http://[a-zA-Z0-9.:]*/_admin/v1/stmtbundle/[0-9]*")
	url := reg.FindString(text)
	if url == "" {
		t.Fatalf("couldn't find URL in response '%s'", text)
	}
	// Download the zip to a BytesBuffer.
	resp, err := httputil.Get(context.Background(), url)
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
	for _, f := range unzip.File {
		if f.UncompressedSize64 == 0 {
			t.Fatalf("file %s is empty", f.Name)
		}
		files = append(files, f.Name)

		if f.Name == "schema.sql" {
			r, err := f.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			contents, err := ioutil.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(contents), tableName) {
				t.Errorf(
					"expected table name to appear in schema.sql. tableName: %s\nfile contents:\n%s",
					tableName,
					string(contents),
				)
			}
		}
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
