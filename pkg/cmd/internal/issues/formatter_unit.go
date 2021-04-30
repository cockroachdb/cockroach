// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package issues

import (
	"fmt"
	"sort"
)

// UnitTestFormatter is the standard issue formatter for unit tests.
var UnitTestFormatter = IssueFormatter{
	Title: func(data TemplateData) string {
		return fmt.Sprintf("%s: %s failed", data.PackageNameShort, data.TestName)
	},
	Body: func(r *Renderer, data TemplateData) error {
		r.Escaped(fmt.Sprintf("%s.%s ", data.PackageNameShort, data.TestName))
		r.A(
			"failed",
			data.URL,
		)
		if data.ArtifactsURL != "" {
			r.Escaped(" with ")
			r.A(
				"artifacts",
				data.ArtifactsURL,
			)
		}
		r.Escaped(" on " + data.Branch + " @ ")
		r.A(
			data.Commit,
			data.CommitURL,
		)
		r.Escaped(`:

`)
		if fop, ok := data.CondensedMessage.FatalOrPanic(50); ok {
			if fop.Error != "" {
				r.Escaped("Fatal error:")
				r.CodeBlock("", fop.Error)
			}
			if fop.FirstStack != "" {
				r.Escaped("Stack: ")
				r.CodeBlock("", fop.FirstStack)
			}

			r.Collapsed("Log preceding fatal error", func() {
				r.CodeBlock("", fop.LastLines)
			})
		} else if rsgCrash, ok := data.CondensedMessage.RSGCrash(100); ok {
			r.Escaped("Random syntax error:")
			r.CodeBlock("", rsgCrash.Error)
			r.Escaped("Query:")
			r.CodeBlock("", rsgCrash.Query)
			if rsgCrash.Schema != "" {
				r.Escaped("Schema:")
				r.CodeBlock("", rsgCrash.Schema)
			}
		} else {
			r.CodeBlock("", data.CondensedMessage.Digest(50))
		}

		r.Collapsed("Reproduce", func() {
			// TODO(tbg): this should be generated here.
			if data.ReproductionCommand != "" {
				r.P(func() {
					r.Escaped("To reproduce, try:\n")
					r.CodeBlock("bash", data.ReproductionCommand)
				})
			}

			if len(data.Parameters) != 0 {
				r.P(func() {
					r.Escaped("Parameters in this failure:\n")
					for _, p := range data.Parameters {
						r.Escaped("\n- ")
						r.Escaped(p)
						r.Escaped("\n")
					}
				})
			}
		})

		if len(data.RelatedIssues) > 0 {
			r.Collapsed("Same failure on other branches", func() {
				for _, iss := range data.RelatedIssues {
					var ls []string
					for _, l := range iss.Labels {
						ls = append(ls, l.GetName())
					}
					sort.Strings(ls)
					r.Escaped("\n- ")
					r.Escaped(fmt.Sprintf("#%d %s %v", iss.GetNumber(), iss.GetTitle(), ls))
				}
				r.Escaped("\n")
			})
		}

		if data.InternalLog != "" {
			r.Collapsed("Internal log", func() {
				r.CodeBlock("", data.InternalLog)
			})
			r.Escaped("\n")
		}

		if len(data.Mention) > 0 {
			r.Escaped("/cc")
			for _, handle := range data.Mention {
				r.Escaped(" ")
				r.Escaped(handle)
			}
			r.Escaped("\n")
		}

		r.HTML("sub", func() {
			r.Escaped("\n\n") // need blank line to <sub> tag for whatever reason
			r.A(
				"This test on roachdash",
				"https://roachdash.crdb.dev/?filter=status:open%20t:.*"+
					data.TestName+
					".*&sort=title+created&display=lastcommented+project",
			)
			r.Escaped(" | ")
			r.A("Improve this report!",
				"https://github.com/cockroachdb/cockroach/tree/master/pkg/cmd/internal/issues",
			)
			r.Escaped("\n")
		})
		return nil
	},
}
