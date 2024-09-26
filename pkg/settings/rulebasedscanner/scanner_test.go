// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rulebasedscanner

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
)

func TestSpecialCharacters(t *testing.T) {
	// We use Go test cases here instead of datadriven because the input
	// strings would be stripped of whitespace or considered invalid by
	// datadriven.
	testData := []struct {
		input  string
		expErr string
	}{
		{"\"ab\tcd\"", `line 1: invalid characters in quoted string`},
		{"\"ab\fcd\"", `line 1: invalid characters in quoted string`},
		{`0 0 0 0 ` + "\f", `line 1: unsupported character: "\f"`},
		{`0 0 0 0 ` + "\x00", `line 1: unsupported character: "\x00"`},
	}

	for _, tc := range testData {
		_, err := Tokenize(tc.input)
		if err == nil || err.Error() != tc.expErr {
			t.Errorf("expected:\n%s\ngot:\n%v", tc.expErr, err)
		}
	}
}

func TestScanner(t *testing.T) {
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "scan"), func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "token":
			remaining, tok, trailingComma, trailingEqualsOp, err := NextToken(td.Input)
			if err != nil {
				return fmt.Sprintf("error: %v", err)
			}
			return fmt.Sprintf("%# v %v %v %q", pretty.Formatter(tok), trailingComma, trailingEqualsOp, remaining)

		case "field":
			remaining, field, err := nextFieldExpand(td.Input)
			if err != nil {
				return fmt.Sprintf("error: %v", err)
			}
			return fmt.Sprintf("%+v\n%q", field, remaining)

		case "file":
			tokens, err := Tokenize(td.Input)
			if err != nil {
				return fmt.Sprintf("error: %v", err)
			}
			return fmt.Sprintf("%# v", pretty.Formatter(tokens))
		default:
			t.Fatalf("unknown directive: %s", td.Cmd)
		}
		return ""
	})
}
