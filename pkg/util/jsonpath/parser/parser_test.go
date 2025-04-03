// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath/parser"
	"github.com/cockroachdb/datadriven"
)

// TestParseDataDriven verifies that we can parse the supplied Jsonpath.
//
// The following commands are allowed:
//
//   - parse
//
//     Parses Jsonpath and verifies that it round-trips. Various forms of the
//     formatted AST are printed as test output.
//
//   - error
//
//     Parses Jsonpath and expects an error. The error is printed as test
//     output.
func TestParseDataDriven(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "parse":
				return VerifyParse(t, d.Input, d.Pos)
			case "error":
				_, err := parser.Parse(d.Input)
				if err == nil {
					d.Fatalf(t, "%s\nexpected error, found none", d.Pos)
				}
				return sqlutils.VerifyParseError(err)
			default:
				d.Fatalf(t, "%s\nunsupported command: %s", d.Pos, d.Cmd)
			}
			return ""
		})
	})
}

func VerifyParse(t *testing.T, input, pos string) string {
	t.Helper()

	jsonpath, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("%s\nunexpected parse error: %v", pos, err)
	}

	ref := jsonpath.String()
	note := ""
	if ref != input {
		note = " -- normalized!"
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s%s\n", ref, note)
	return buf.String()
}
