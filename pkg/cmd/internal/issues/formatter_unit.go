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
var UnitTestFormatter IssueFormatter = unitTestFormatterTyp{}

type unitTestFormatterTyp struct{}

// Title is part of the IssueFormatter interface.
func (unitTestFormatterTyp) Title(data TemplateData) string {
	return fmt.Sprintf("%s: %s failed", data.PackageNameShort, data.TestName)
}

// Body is part of the IssueFormatter interface.
func (unitTestFormatterTyp) Body(r *Renderer, data TemplateData) error {
	for _, note := range data.TopLevelNotes {
		r.Escaped("**Note:** ")
		r.Escaped(note)
		r.printf("\n\n")
	}
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

	if len(data.Parameters) != 0 {
		params := make([]string, 0, len(data.Parameters))
		for name := range data.Parameters {
			params = append(params, name)
		}
		sort.Strings(params)

		if len(params) > 0 {
			r.nl()
			r.printf("\n")
			r.Escaped("Parameters:")
			r.nl()
			for _, name := range params {
				r.Escaped(" - ")
				r.Code(fmt.Sprintf("%s=%s", name, data.Parameters[name]))
				r.nl()
			}
		}
	}

	if data.HelpCommand != nil {
		r.Collapsed("Help", func() {
			data.HelpCommand(r)
		})
	}

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

	if len(data.MentionOnCreate) > 0 {
		r.Escaped("/cc")
		for _, handle := range data.MentionOnCreate {
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
}

// UnitTestHelpCommand is a HelpCommand for use with UnitTestFormatter. It
// renders a reproduction command and helpful links.
func UnitTestHelpCommand(repro string) func(r *Renderer) {
	return func(r *Renderer) {
		r.Escaped("\n") // need this newline or link won't render
		r.Escaped("See also: ")
		r.A("How To Investigate a Go Test Failure (internal)", "https://cockroachlabs.atlassian.net/l/c/HgfXfJgM")
		r.Escaped("\n")
	}
}
