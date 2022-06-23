// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import (
	"regexp"
	"strings"
)

var (
	htmlBlocks = []string{
		"address",
		"article",
		"aside",
		"base",
		"basefont",
		"blockquote",
		"body",
		"caption",
		"center",
		"col",
		"colgroup",
		"dd",
		"details",
		"dialog",
		"dir",
		"div",
		"dl",
		"dt",
		"fieldset",
		"figcaption",
		"figure",
		"footer",
		"form",
		"frame",
		"frameset",
		"h1",
		"h2",
		"h3",
		"h4",
		"h5",
		"h6",
		"head",
		"header",
		"hr",
		"html",
		"iframe",
		"legend",
		"li",
		"link",
		"main",
		"menu",
		"menuitem",
		"meta",
		"nav",
		"noframes",
		"ol",
		"optgroup",
		"option",
		"p",
		"param",
		"section",
		"source",
		"summary",
		"table",
		"tbody",
		"td",
		"tfoot",
		"th",
		"thead",
		"title",
		"tr",
		"track",
		"ul",
	}

	htmlBlocksSet = make(map[string]bool)

	rStartCond1 = regexp.MustCompile(`(?i)^(pre|script|style)([\n\t >]|$)`)
	rEndCond1   = regexp.MustCompile(`(?i)</(pre|script|style)>`)
	rStartCond6 = regexp.MustCompile(`(?i)^/?(` + strings.Join(htmlBlocks, "|") + `)(\s|$|>|/>)`)
	rStartCond7 = regexp.MustCompile(`(?i)^(/[a-z][a-z0-9-]*|[a-z][a-z0-9-]*(\s+[a-z_:][a-z0-9_.:-]*\s*=\s*("[^"]*"|'[^']*'|[ "'=<>\x60]))*\s*/?)>\s*$`)
)

func init() {
	for _, tag := range htmlBlocks {
		htmlBlocksSet[tag] = true
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func matchTagName(s string) string {
	if len(s) < 2 {
		return ""
	}

	i := 0
	if s[0] == '/' {
		i++
	}
	start := i
	max := min(15+i, len(s))
	for i < max && isLetter(s[i]) {
		i++
	}
	if i >= len(s) {
		return ""
	}

	switch s[i] {
	case ' ', '\n', '/', '>':
		return strings.ToLower(s[start:i])
	default:
		return ""
	}
}

func ruleHTMLBlock(s *StateBlock, startLine, endLine int, silent bool) bool {
	if !s.Md.HTML {
		return false
	}

	pos := s.BMarks[startLine] + s.TShift[startLine]
	max := s.EMarks[startLine]

	if pos+1 >= max {
		return false
	}

	src := s.Src

	if src[pos] != '<' {
		return false
	}

	pos++
	b := src[pos]
	if !htmlSecond(b) {
		return false
	}

	nextLine := startLine + 1

	var endCond func(string) bool

	if pos+2 < max && isLetter(b) && rStartCond1.MatchString(src[pos:]) {
		endCond = func(s string) bool {
			return rEndCond1.MatchString(s)
		}
	} else if strings.HasPrefix(src[pos:], "!--") {
		endCond = func(s string) bool {
			return strings.Contains(s, "-->")
		}
	} else if b == '?' {
		endCond = func(s string) bool {
			return strings.Contains(s, "?>")
		}
	} else if b == '!' && pos+1 < max && isUppercaseLetter(src[pos+1]) {
		endCond = func(s string) bool {
			return strings.Contains(s, ">")
		}
	} else if strings.HasPrefix(src[pos:], "![CDATA[") {
		endCond = func(s string) bool {
			return strings.Contains(s, "]]>")
		}
	} else if pos+2 < max && (isLetter(b) || b == '/' && isLetter(src[pos+1])) {
		terminator := true
		if rStartCond6.MatchString(src[pos:max]) {
		} else if rStartCond7.MatchString(src[pos:max]) {
			terminator = false
		} else {
			return false
		}
		if silent {
			return terminator
		}
		endCond = func(s string) bool {
			return s == ""
		}
	} else {
		return false
	}

	if silent {
		return true
	}

	if !endCond(src[pos:max]) {
		for nextLine < endLine {
			if s.SCount[nextLine] < s.BlkIndent {
				break
			}

			pos := s.BMarks[nextLine] + s.TShift[nextLine]
			max := s.EMarks[nextLine]
			lineText := src[pos:max]
			if endCond(lineText) {
				if pos != max {
					nextLine++
				}
				break
			}
			nextLine++
		}
	}

	s.Line = nextLine
	s.PushToken(&HTMLBlock{
		Content: s.Lines(startLine, nextLine, s.BlkIndent, true),
		Map:     [2]int{startLine, nextLine},
	})

	return true
}
