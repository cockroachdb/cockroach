// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestRenderHTML(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cols := []string{"colname"}
	align := "d"
	rows := [][]string{
		{"<b>foo</b>"},
		{"bar"},
	}

	type testCase struct {
		reporter htmlReporter
		out      string
	}

	testCases := []testCase{
		{
			reporter: htmlReporter{},
			out: `<table>
<thead><tr><th>colname</th></tr></thead>
<tbody>
<tr><td><b>foo</b></td></tr>
<tr><td>bar</td></tr>
</tbody>
</table>
`,
		},
		{
			reporter: htmlReporter{escape: true},
			out: `<table>
<thead><tr><th>colname</th></tr></thead>
<tbody>
<tr><td>&lt;b&gt;foo&lt;/b&gt;</td></tr>
<tr><td>bar</td></tr>
</tbody>
</table>
`,
		},
		{
			reporter: htmlReporter{rowStats: true},
			out: `<table>
<thead><tr><th>row</th><th>colname</th></tr></thead>
<tbody>
<tr><td>1</td><td><b>foo</b></td></tr>
<tr><td>2</td><td>bar</td></tr>
</tbody>
<tfoot><tr><td colspan=2>2 rows</td></tr></tfoot></table>
`,
		},
		{
			reporter: htmlReporter{escape: true, rowStats: true},
			out: `<table>
<thead><tr><th>row</th><th>colname</th></tr></thead>
<tbody>
<tr><td>1</td><td>&lt;b&gt;foo&lt;/b&gt;</td></tr>
<tr><td>2</td><td>bar</td></tr>
</tbody>
<tfoot><tr><td colspan=2>2 rows</td></tr></tfoot></table>
`,
		},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("escape=%v/rowStats=%v", tc.reporter.escape, tc.reporter.rowStats)
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := render(&tc.reporter, &buf, io.Discard,
				cols, NewRowSliceIter(rows, align),
				nil /* completedHook */, nil /* noRowsHook */)
			if err != nil {
				t.Fatal(err)
			}
			if tc.out != buf.String() {
				t.Errorf("expected:\n%s\ngot:\n%s", tc.out, buf.String())
			}
		})
	}
}
