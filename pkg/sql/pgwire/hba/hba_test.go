// Copyright 2018 The Cockroach Authors.
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
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
)

func TestParse(t *testing.T) {
	datadriven.RunTest(t, filepath.Join("testdata", "parse"),
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "multiline":
				conf, err := Parse(td.Input)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				var out strings.Builder
				fmt.Fprintf(&out, "# String render check:\n%s", conf)
				fmt.Fprintf(&out, "# Detail:\n%# v", pretty.Formatter(conf))
				return out.String()

			case "line":
				tokens, err := tokenize(td.Input)
				if err != nil {
					td.Fatalf(t, "%v", err)
				}
				if len(tokens.lines) != 1 {
					td.Fatalf(t, "line parse only valid with one line of input")
				}
				prefix := "" // For debugging, use prefix := pretty.Sprint(tokens.lines[0]) + "\n"
				entry, err := parseHbaLine(tokens.lines[0])
				if err != nil {
					return prefix + fmt.Sprintf("error: %v\n", err)
				}
				return prefix + entry.String()

			default:
				return fmt.Sprintf("unknown directive: %s", td.Cmd)
			}
		})
}

func TestParseAndNormalizeAuthConfig(t *testing.T) {
	datadriven.RunTest(t, filepath.Join("testdata", "normalization"),
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "hba":
				conf, err := ParseAndNormalize(td.Input)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				return conf.String()
			default:
				t.Fatalf("unknown directive: %s", td.Cmd)
			}
			return ""
		})
}

func TestMatchConnType(t *testing.T) {
	testCases := []struct {
		conf, conn ConnType
		match      bool
	}{
		{ConnLocal, ConnHostSSL, false},
		{ConnLocal, ConnHostNoSSL, false},
		{ConnLocal, ConnLocal, true},
		{ConnHostAny, ConnLocal, false},
		{ConnHostAny, ConnHostSSL, true},
		{ConnHostAny, ConnHostNoSSL, true},
		{ConnHostSSL, ConnLocal, false},
		{ConnHostSSL, ConnHostSSL, true},
		{ConnHostSSL, ConnHostNoSSL, false},
		{ConnHostNoSSL, ConnLocal, false},
		{ConnHostNoSSL, ConnHostSSL, false},
		{ConnHostNoSSL, ConnHostNoSSL, true},
	}
	for _, tc := range testCases {
		entry := Entry{ConnType: tc.conf}
		if m := entry.ConnTypeMatches(tc.conn); m != tc.match {
			t.Errorf("%s vs %s: expected %v, got %v", tc.conf, tc.conn, tc.match, m)
		}
	}
}

// TODO(mjibson): these are untested outside ccl +gss builds.
var _ = Entry.GetOption
var _ = Entry.GetOptions
