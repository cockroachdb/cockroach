// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgreplparser

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, "parser"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "parse":
				var expectError bool
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "error":
						expectError = true
					default:
						t.Errorf("unknown cmd arg %s", arg.Key)
					}
				}
				p, err := Parse(td.Input)
				if expectError {
					require.Error(t, err)

					pgerr := pgerror.Flatten(err)
					msg := pgerr.Message
					if pgerr.Detail != "" {
						msg += "\nDETAIL: " + pgerr.Detail
					}
					if pgerr.Hint != "" {
						msg += "\nHINT: " + pgerr.Hint
					}
					return msg
				}
				require.NoError(t, err)
				ref := tree.AsString(p.AST)
				note := ""
				if ref != td.Input {
					note = " -- normalized!"
				}
				var buf bytes.Buffer
				fmt.Fprintf(&buf, "%s%s\n", ref, note)
				constantsHidden := tree.AsStringWithFlags(p.AST, tree.FmtHideConstants)
				fmt.Fprintln(&buf, constantsHidden, "-- literals removed")

				// Test roundtrip.
				reparsed, err := Parse(ref)
				require.NoError(t, err)
				assert.Equal(t, ref, tree.AsString(reparsed.AST))

				return buf.String()
			default:
				t.Errorf("unknown command %s", td.Cmd)
			}
			return ""
		})
	})
}
