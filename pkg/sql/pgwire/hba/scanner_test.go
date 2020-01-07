// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hba

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
)

func TestScanner(t *testing.T) {
	datadriven.RunTest(t, "testdata/scan", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "token":
			remaining, tok, trailingComma, err := nextToken(td.Input)
			if err != nil {
				return fmt.Sprintf("error: %v", err)
			}
			return fmt.Sprintf("%# v %v %q", pretty.Formatter(tok), trailingComma, remaining)

		case "field":
			remaining, field, err := nextFieldExpand(td.Input)
			if err != nil {
				return fmt.Sprintf("error: %v", err)
			}
			return fmt.Sprintf("%+v\n%q", field, remaining)

		case "file":
			tokens, err := tokenize(td.Input)
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
