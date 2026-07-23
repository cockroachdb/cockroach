// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auditlogging

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/rulebasedscanner"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
)

func TestParse(t *testing.T) {
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "parse"),
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "multiline":
				config, err := Parse(td.Input)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				var out strings.Builder
				fmt.Fprintf(&out, "# String render check:\n%s", config)
				fmt.Fprintf(&out, "# Detail:\n%# v", pretty.Formatter(config))
				return out.String()

			case "line":
				tokens, err := rulebasedscanner.Tokenize(td.Input)
				if err != nil {
					td.Fatalf(t, "%v", err)
				}
				if len(tokens.Lines) != 1 {
					td.Fatalf(t, "line parse only valid with one line of input")
				}
				setting, err := parseAuditSetting(tokens.Lines[0])
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				return setting.String()

			default:
				return fmt.Sprintf("unknown directive: %s", td.Cmd)
			}
		})
}
