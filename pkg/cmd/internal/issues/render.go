// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package issues

import (
	"fmt"
	"html"
	"strings"
	"unicode/utf8"
)

// An IssueFormatter turns TemplateData for a test failure into markdown
// that can form a GitHub issue comment.
type IssueFormatter interface {
	Title(data TemplateData) string
	Body(r *Renderer, data TemplateData) error
}

// A Renderer facilitates creating a reduced and opinionated subset of markdown.
type Renderer struct {
	buf strings.Builder
}

func (r *Renderer) printf(format string, args ...interface{}) {
	fmt.Fprintf(&r.buf, format, args...)
}

func (r *Renderer) esc(in string, chars string, with rune) string {
	for {
		r, n := utf8.DecodeRuneInString(chars)
		if r == utf8.RuneError {
			return in
		}
		chars = chars[n:]
		s := string(r)
		in = strings.Replace(in, s, string(with)+s, -1)
	}
}

func (r *Renderer) nl() {
	if n := r.buf.Len(); n > 0 && r.buf.String()[n-1] == '\n' {
		return
	}
	r.buf.WriteByte('\n')
}

// A renders a hyperlink.
func (r *Renderer) A(title, href string) {
	r.printf("[")
	r.Escaped(r.esc(title, "[]()", '\\'))
	r.printf("]")
	r.printf("(")
	r.printf("%s", r.esc(href, "[]()", '\\'))
	r.printf(")")
}

// P renders the inner function as a paragraph.
func (r *Renderer) P(inner func()) {
	r.HTML("p", inner)
}

// Escaped renders text, which it HTML escapes.
func (r *Renderer) Escaped(txt string) {
	r.printf("%s", html.EscapeString(txt))
}

// Code renders a word or phrase as code. Instead of using backticks
// here (Markdown), we rely on HTML tags since that works even if the
// this function is called within the context of an HTML tag (such as
// a paragraph).
func (r *Renderer) Code(txt string) {
	r.HTML("code", func() { r.Escaped(txt) })
}

// CodeBlock renders a code block.
func (r *Renderer) CodeBlock(typ string, txt string) {
	r.nl()
	// NB: the leading newline may be spurious, but quotes
	// always need to be preceded by a blank line, or at
	// least GitHub doesn't interpret the ``` right. The
	// below will misbehave, we need a blank line after `<p>`.
	//
	// <details><summary>foo</summary>
	// <p>
	// ```
	// bar
	// ```
	// </p>
	// </details>
	r.printf("\n```%s\n", r.esc(typ, "`", '`'))
	r.printf("%s", r.esc(txt, "`", '`'))
	r.nl()
	r.printf("%s", "```")
	r.nl()
}

// HTML renders inner as enclosed by the supplied HTML tag.
func (r *Renderer) HTML(tag string, inner func()) {
	r.printf("<%s>", tag)
	inner()
	r.printf("</%s>", tag)
	r.nl()
}

// Collapsed renders an expandable section via the details HTML tag.
func (r *Renderer) Collapsed(title string, inner func()) {
	r.HTML("details", func() {
		r.HTML("summary", func() {
			r.Escaped(title)
		})
		r.nl()
		r.P(func() {
			r.nl()
			inner()
		})
	})
	r.nl()
}

// String prints the buffer.
func (r *Renderer) String() string {
	return r.buf.String()
}
