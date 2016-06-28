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
	"io/ioutil"
	"strings"
	"testing"

	"github.com/chzyer/readline"
)

// TestSQLLex tests the usage of the lexer in the sql subcommand.
//
// CAUTION: this test cannot be copied and reused. I have not yet found a
// convinent way to set readline.Stdin multiple times. Any subsequent use
// (i.e., if this test were copied) will result in a timeout due to hung go
// routines. For now, we can have one giant input test string.
// TODO(mjibson): enable multiple tests to use readline
func TestSQLLex(t *testing.T) {
	// No leaktest because readline listens for SIGWINCH on a go routine.

	c := newCLITest()
	defer c.stop()

	in := `
/* Make sure \ commands and ; are interpreted as part of the string literal. */
select '
\?
;
';

/* Test traditional vs modern lexing for semicolons using four single
quotes, which differ in how they lex. This effectively tests that the
syntax mode is being set correctly in the client-side lexer. */
select ''''
;

set syntax = modern;

select ''''
;
''';
`
	expect := `sql
1 row
"e'\\n\\\\?\\n;\\n'"
"\n\\?\n;\n"
1 row
"e'\\''"
'
SET
1 row
"e'\\'\\n;\\n'"
"'\n;\n"
`

	readline.Stdin = ioutil.NopCloser(strings.NewReader(in))
	out, err := c.RunWithCapture("sql")
	if err != nil {
		t.Fatal(err)
	}
	if out != expect {
		t.Fatalf("%s:\nexpected: %s\ngot: %s", in, expect, out)
	}
}
