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
		func(t *testing.T, d *datadriven.TestData) string {
			conf, err := Parse(d.Input)
			if err != nil {
				return fmt.Sprintf("error: %v\n", err)
			}
			var out strings.Builder
			fmt.Fprintf(&out, "# String render check:\n%s", conf)
			fmt.Fprintf(&out, "# Detail:\n%# v", pretty.Formatter(conf))
			return out.String()
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

// TODO(mjibson): these are untested outside ccl +gss builds.
var _ = Entry.GetOption
var _ = Entry.GetOptions
