// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cli

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSQLFmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runSQLFmt := func(s string, args ...string) string {
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			_, err := io.Copy(w, strings.NewReader(s))
			if err != nil {
				panic(err)
			}
			w.Close()
		}()
		os.Stdin = r

		s, err = captureOutput(func() {
			Run(append([]string{"sqlfmt"}, args...))
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	tests := []struct {
		sql    string
		expect string
		args   []string
	}{
		{
			sql: "select 1,2,3 from a,b,t",
			expect: `SELECT
	1,
	2,
	3
FROM
	a,
	b,
	t
`,
			args: []string{"-n", "10"},
		},
		{
			sql: "select 1,2,3 from a,b,t",
			expect: `SELECT
  1, 2, 3
FROM
  a, b, t
`,
			args: []string{"-n", "10", "--spaces", "--tab-width", "2"},
		},
	}

	for i, tc := range tests {
		s := runSQLFmt(tc.sql, tc.args...)
		if s != tc.expect {
			t.Fatalf("testcase %d: unexpected: %s", i, s)
		}
	}
}
