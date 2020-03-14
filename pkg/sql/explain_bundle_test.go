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
	"github.com/lib/pq"
)

func TestExplainBundle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	srv, godb, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer srv.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(godb)

	r.Exec(t, "CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT UNIQUE)")
	rows := r.QueryStr(t, "EXPLAIN BUNDLE SELECT * FROM abc WHERE c=1")
	checkBundle(t, fmt.Sprint(rows), "statement.txt opt.txt opt-v.txt opt-vv.txt plan.txt trace.json")

	// Even on query errors, we should still get a bundle.
	_, err := godb.QueryContext(ctx, "EXPLAIN BUNDLE SELECT * FROM badtable")
	if !testutils.IsError(err, "relation.*does not exist") {
		t.Fatalf("unexpected error %v\n", err)
	}
	// The bundle url is inside the error detail.
	checkBundle(t, fmt.Sprintf("%+v", err.(*pq.Error).Detail), "statement.txt trace.json")
}

// checkBundle searches text strings for a bundle URL and then verifies that the
// bundle contains the expected files.
func checkBundle(t *testing.T, text string, expectedFiles string) {
	t.Helper()
	reg := regexp.MustCompile("http://.*/_admin/v1/stmtbundle/[0-9]*")
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
	}
	expList := strings.Split(expectedFiles, " ")
	sort.Strings(files)
	sort.Strings(expList)
	if fmt.Sprint(files) != fmt.Sprint(expList) {
		t.Errorf("unexpected list of files %v", files)
	}
}
