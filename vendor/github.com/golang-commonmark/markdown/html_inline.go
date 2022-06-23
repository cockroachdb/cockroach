// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "regexp"

var (
	attrName     = `[a-zA-Z_:][a-zA-Z0-9:._-]*`
	unquoted     = "[^\"'=<>`\\x00-\\x20]+"
	singleQuoted = `'[^']*'`
	doubleQuoted = `"[^"]*"`
	attrValue    = `(?:` + unquoted + `|` + singleQuoted + `|` + doubleQuoted + `)`
	attribute    = `(?:\s+` + attrName + `(?:\s*=\s*` + attrValue + `)?)`
	openTag      = `<[A-Za-z][A-Za-z0-9-]*` + attribute + `*\s*/?>`
	closeTag     = `</[A-Za-z][A-Za-z0-9-]*\s*>`
	comment      = `<!---->|<!--(?:-?[^>-])(?:-?[^-])*-->`
	processing   = `<[?].*?[?]>`
	declaration  = `<![A-Z]+\s+[^>]*>`
	cdata        = `<!\[CDATA\[[\s\S]*?\]\]>`
	rHTMLTag     = regexp.MustCompile(`^(?:` + openTag + `|` + closeTag + `|` + comment +
		`|` + processing + `|` + declaration + `|` + cdata + `)`)
)

func htmlSecond(b byte) bool {
	return b == '!' || b == '/' || b == '?' || isLetter(b)
}

func ruleHTMLInline(s *StateInline, silent bool) bool {
	if !s.Md.HTML {
		return false
	}

	pos := s.Pos
	src := s.Src
	if pos+2 >= s.PosMax || src[pos] != '<' {
		return false
	}

	if !htmlSecond(src[pos+1]) {
		return false
	}

	match := rHTMLTag.FindString(src[pos:])
	if match == "" {
		return false
	}

	if !silent {
		s.PushToken(&HTMLInline{Content: match})
	}

	s.Pos += len(match)

	return true
}
