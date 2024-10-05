// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package compengine

import (
	"fmt"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// TestCompletionsSketch tests the completionSketch() function,
// as well as the resulting relative indexing of tokens/markers.
func TestCompletionSketch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.RunTest(t, "testdata/completion_sketch", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "comp":
			offset := 0
			if td.HasArg("at") {
				td.ScanArgs(t, "at", &offset)
			}

			tokens, sketch, pos := CompletionSketch(td.Input, offset)
			if err := CheckSketch(tokens, sketch, pos); err != nil {
				t.Fatalf("%s: %v", td.Pos, err)
			}
			var buf strings.Builder

			tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
			for idx := range sketch {
				quoted := byte(' ')
				if tokens[idx].Quoted {
					quoted = 'q'
				}
				tok := tokens[idx]
				fmt.Fprintf(&buf, "%c\t%q\t%c\t%d\t%d\n", sketch[idx], tok.Str, quoted, tok.Start, tok.End)
			}

			fmt.Fprintln(tw, "--")
			// Now print just a few tokens surrounding the cursor position.
			c := completions{tokens: tokens, sketch: sketch, queryTokIdx: pos}
			for i := -2; i <= 2; i++ {
				tok := c.RelToken(i)
				quoted := byte(' ')
				if tok.Quoted {
					quoted = 'q'
				}
				fmt.Fprintf(&buf, "%d\t%q\t%c\t%d\t%d\n", i, tok.Str, quoted, tok.Start, tok.End)
			}

			_ = tw.Flush()
			return buf.String()

		default:
			t.Fatalf("%s: unknown command: %s", td.Pos, td.Cmd)
		}
		return ""
	})
}
