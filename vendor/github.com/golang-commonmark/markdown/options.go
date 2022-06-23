// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

type option func(m *Markdown)

func HTML(b bool) option {
	return func(m *Markdown) {
		m.HTML = b
	}
}

func Linkify(b bool) option {
	return func(m *Markdown) {
		m.Linkify = b
	}
}

func Typographer(b bool) option {
	return func(m *Markdown) {
		m.Typographer = b
	}
}

func Quotes(stringOrArray interface{}) option {
	if s, ok := stringOrArray.(string); ok {
		return func(m *Markdown) {
			for i, r := range []rune(s) {
				m.Quotes[i] = string(r)
			}
		}
	}
	a := stringOrArray.([]string)
	return func(m *Markdown) {
		for i, s := range a {
			m.Quotes[i] = s
		}
	}
}

func MaxNesting(n int) option {
	return func(m *Markdown) {
		m.MaxNesting = n
	}
}

func XHTMLOutput(b bool) option {
	return func(m *Markdown) {
		m.renderOptions.XHTML = b
	}
}

func Breaks(b bool) option {
	return func(m *Markdown) {
		m.renderOptions.Breaks = b
	}
}

func LangPrefix(p string) option {
	return func(m *Markdown) {
		m.renderOptions.LangPrefix = p
	}
}

func Nofollow(b bool) option {
	return func(m *Markdown) {
		m.renderOptions.Nofollow = b
	}
}

func Tables(b bool) option {
	return func(m *Markdown) {
		m.Tables = b
	}
}
