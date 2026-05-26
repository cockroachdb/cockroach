// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package comprules

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/compengine"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/redact"
)

// TestCompletionPatterns checks the traces resulting from running the
// completion heuristics. This validates that the heuristic recognize
// the proper patterns of tokens.
func TestCompletionPatterns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/completion_patterns", func(t *testing.T, path string) {
		curTraceFilter := regexp.MustCompile(`.*`)
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "filter":
				filter, err := regexp.Compile(td.Input)
				if err != nil {
					t.Fatalf("%s: %v", td.Pos, err)
				}
				curTraceFilter = filter

			case "comp":
				offset := 0
				if td.HasArg("at") {
					td.ScanArgs(t, "at", &offset)
				}

				ctx := context.Background()
				var buf strings.Builder
				fakeQueryFn := func(_ context.Context, label redact.RedactableString, q string, args ...interface{}) (compengine.Rows, error) {
					if curTraceFilter.MatchString(label.StripMarkers() + ":") {
						fmt.Fprintf(&buf, "--sql:\n%s\n--placeholders: %#v\n",
							strings.TrimSpace(q), args)
					}
					return nil, nil
				}
				c := compengine.New(fakeQueryFn, GetCompMethods(), offset, td.Input)
				defer c.Close(ctx)

				sketch := c.(interface{ Sketch() string }).Sketch()
				pos := c.(interface{ TestingQueryTokIdx() int }).TestingQueryTokIdx()

				if err := c.(interface{ TestingCheckSketch() error }).TestingCheckSketch(); err != nil {
					t.Fatalf("%s: %v", td.Pos, err)
				}

				fmt.Fprintln(&buf, sketch)
				fmt.Fprintf(&buf, "%*s^\n", pos, "")
				fmt.Fprintln(&buf, "--")

				trace := func(format string, args ...interface{}) {
					msg := fmt.Sprintf(format, args...)
					if curTraceFilter.MatchString(msg) {
						buf.WriteString(msg)
						buf.WriteByte('\n')
					}
				}
				c.(interface {
					TestingSetTraceFn(func(string, ...interface{}))
				}).TestingSetTraceFn(trace)

				var err error
				var hasNext bool
				for hasNext, err = c.Next(ctx); hasNext; hasNext, err = c.Next(ctx) {
					row := c.Values()
					fmt.Fprintf(&buf, "UNEXPECTED ROW: %+v\n", row)
				}
				if err != nil {
					t.Errorf("completion failed: %v", err)
				}
				return buf.String()

			default:
				t.Fatalf("%s: unknown command: %s", td.Pos, td.Cmd)
			}
			return ""
		})
	})
}
