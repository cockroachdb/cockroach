// Copyright 2018 The Cockroach Authors.
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

package parser

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TestingT is a subset of the testing.T struct. It is defined here so this
// package can avoid a dependency on the testing package.
type TestingT interface {
	Helper()
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// VerifyStatementPrettyRoundtrip verifies that the SQL statements in s
// correctly round trip through the pretty printer.
func VerifyStatementPrettyRoundtrip(t TestingT, sql string) {
	t.Helper()

	if strings.Contains(strings.ToUpper(sql), "AS OF SYSTEM TIME") {
		// TODO(mjibson): See #26976
		return
	}

	stmts, err := Parse(sql)
	if err != nil {
		t.Fatalf("%s: %s", err, sql)
	}
	for _, origStmt := range stmts {
		prettyStmt := tree.Pretty(origStmt)
		parsedPretty, err := ParseOne(prettyStmt)
		if err != nil {
			t.Fatalf("%s: %s", err, prettyStmt)
		}
		prettyFormatted := tree.AsStringWithFlags(parsedPretty, tree.FmtSimple)
		origFormatted := tree.AsStringWithFlags(origStmt, tree.FmtParsable)
		if prettyFormatted != origFormatted {
			// Type annotations and unicode strings don't round trip well. Sometimes we
			// need to reparse the original formatted output and format that for these
			// to match.
			reparsedStmt, err := ParseOne(origFormatted)
			if err != nil {
				t.Fatal(err)
			}
			origFormatted = tree.AsStringWithFlags(reparsedStmt, tree.FmtParsable)
			if prettyFormatted != origFormatted {
				t.Fatalf("orig formatted != pretty formatted\norig SQL: %q\norig formatted: %q\npretty printed: %s\npretty formatted: %q",
					sql,
					origFormatted,
					prettyStmt,
					prettyFormatted,
				)
			}
		}
	}
}
