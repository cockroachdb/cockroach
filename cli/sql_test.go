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
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package cli

import (
	"strings"
	"testing"

	"github.com/chzyer/readline"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestSQLLex tests the usage of the lexer in the sql subcommand.
func TestSQLLex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop()

	pgurl, err := s.(*server.TestServer).Ctx.PGURL("")
	if err != nil {
		t.Fatal(err)
	}
	conn := makeSQLConn(pgurl.String())
	defer conn.Close()

	tests := []struct {
		in     string
		expect string
	}{
		{
			in: `
select '
\?
;
';
`,
			expect: `+---------------+
| e'\n\\?\n;\n' |
+---------------+
| ␤             |
| \?␤           |
| ;␤            |
+---------------+
(1 row)
`,
		},
		{
			in: `
select ''''
;

set syntax = modern;

select ''''
;
''';
`,
			expect: `+-------+
| e'\'' |
+-------+
| '     |
+-------+
(1 row)
SET
+------------+
| e'\'\n;\n' |
+------------+
| '␤         |
| ;␤         |
+------------+
(1 row)
`,
		},
	}

	conf := readline.Config{
		DisableAutoSaveHistory: true,
		FuncOnWidthChanged:     func(func()) {},
	}

	// Some other tests (TestDumpRow) mess with this, so make sure it's set.
	cliCtx.prettyFmt = true

	for _, test := range tests {
		conf.Stdin = strings.NewReader(test.in)
		out, err := captureOutput(func() {
			err := runInteractive(conn, &conf)
			if err != nil {
				t.Fatal(err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if out != test.expect {
			t.Fatalf("%s:\nexpected: %s\ngot: %s", test.in, test.expect, out)
		}
	}
}

func TestIsEndOfStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		syntax parser.Syntax
		in     string
		isEnd  bool
		hasSet bool
	}{
		{
			in:    ";",
			isEnd: true,
		},
		{
			in:    "; /* comment */",
			isEnd: true,
		},
		{
			in: "; SELECT",
		},
		{
			in: "SELECT",
		},
		{
			in:     "SET; SELECT 1;",
			isEnd:  true,
			hasSet: true,
		},
		{
			in:     "SELECT ''''; SET;",
			isEnd:  true,
			hasSet: true,
		},
		{
			in:     "SELECT ''''; SET;",
			syntax: parser.Modern,
		},
	}

	for _, test := range tests {
		syntax := test.syntax
		if syntax == 0 {
			syntax = parser.Traditional
		}
		isEnd, hasSet := isEndOfStatement(syntax, &[]string{test.in})
		if isEnd != test.isEnd {
			t.Errorf("%q: isEnd expected %v, got %v", test.in, test.isEnd, isEnd)
		}
		if hasSet != test.hasSet {
			t.Errorf("%q: hasSet expected %v, got %v", test.in, test.hasSet, hasSet)
		}
	}
}
