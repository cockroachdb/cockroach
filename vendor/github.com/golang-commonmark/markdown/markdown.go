// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package markdown provides CommonMark-compliant markdown parser and renderer.
package markdown

import (
	"bytes"
	"io"
)

type Markdown struct {
	options
	Block         ParserBlock
	Inline        ParserInline
	renderOptions RenderOptions
}

type RenderOptions struct {
	LangPrefix string // CSS language class prefix for fenced blocks
	XHTML      bool   // render as XHTML instead of HTML
	Breaks     bool   // convert \n in paragraphs into <br>
	Nofollow   bool   // add rel="nofollow" to the links
}

type options struct {
	HTML        bool      // allow raw HTML in the markup
	Tables      bool      // GFM tables
	Linkify     bool      // autoconvert URL-like text to links
	Typographer bool      // enable some typographic replacements
	Quotes      [4]string // double/single quotes replacement pairs
	MaxNesting  int       // maximum nesting level
}

type Environment struct {
	References map[string]map[string]string
}

type CoreRule func(*StateCore)

var coreRules []CoreRule

func New(opts ...option) *Markdown {
	m := &Markdown{
		options: options{
			Tables:      true,
			Linkify:     true,
			Typographer: true,
			Quotes:      [4]string{"“", "”", "‘", "’"},
			MaxNesting:  20,
		},
		renderOptions: RenderOptions{LangPrefix: "language-"},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *Markdown) Parse(src []byte) []Token {
	if len(src) == 0 {
		return nil
	}

	s := &StateCore{
		Md:  m,
		Env: &Environment{},
	}
	s.Tokens = m.Block.Parse(src, m, s.Env)

	for _, r := range coreRules {
		r(s)
	}
	return s.Tokens
}

func (m *Markdown) Render(w io.Writer, src []byte) error {
	if len(src) == 0 {
		return nil
	}

	return NewRenderer(w).Render(m.Parse(src), m.renderOptions)
}

func (m *Markdown) RenderTokens(w io.Writer, tokens []Token) error {
	if len(tokens) == 0 {
		return nil
	}

	return NewRenderer(w).Render(tokens, m.renderOptions)
}

func (m *Markdown) RenderToString(src []byte) string {
	if len(src) == 0 {
		return ""
	}

	var buf bytes.Buffer
	NewRenderer(&buf).Render(m.Parse(src), m.renderOptions)
	return buf.String()
}

func (m *Markdown) RenderTokensToString(tokens []Token) string {
	if len(tokens) == 0 {
		return ""
	}

	var buf bytes.Buffer
	NewRenderer(&buf).Render(tokens, m.renderOptions)
	return buf.String()
}
