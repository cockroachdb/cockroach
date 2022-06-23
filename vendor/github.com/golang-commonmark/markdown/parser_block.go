// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "unicode/utf8"

type ParserBlock struct{}

type BlockRule func(*StateBlock, int, int, bool) bool

var blockRules []BlockRule

func (b ParserBlock) Parse(src []byte, md *Markdown, env *Environment) []Token {
	var bMarks, eMarks, tShift, sCount, bsCount []int

	indentFound := false
	start := 0
	pos := 0
	indent := 0
	offset := 0

	for pos < len(src) {
		r, size := utf8.DecodeRune(src[pos:])

		if !indentFound {
			if runeIsSpace(r) {
				indent++
				if r == '\t' {
					offset += 4 - offset%4
				} else {
					offset++
				}
				pos += size
				continue
			}
			indentFound = true
		}

		if r == '\n' || pos == len(src)-1 {
			if r != '\n' {
				pos++
			}
			bMarks = append(bMarks, start)
			eMarks = append(eMarks, pos)
			tShift = append(tShift, indent)
			sCount = append(sCount, offset)
			bsCount = append(bsCount, 0)

			indentFound = false
			indent = 0
			offset = 0
			start = pos + 1
		}

		pos += size
	}

	bMarks = append(bMarks, len(src))
	eMarks = append(eMarks, len(src))
	tShift = append(tShift, 0)
	sCount = append(sCount, 0)
	bsCount = append(bsCount, 0)

	var s StateBlock
	s.BMarks = bMarks
	s.EMarks = eMarks
	s.TShift = tShift
	s.SCount = sCount
	s.BSCount = bsCount
	s.LineMax = len(bMarks) - 1
	s.Src = string(src)
	s.Md = md
	s.Env = env

	b.Tokenize(&s, s.Line, s.LineMax)

	return s.Tokens
}

func (ParserBlock) Tokenize(s *StateBlock, startLine, endLine int) {
	line := startLine
	hasEmptyLines := false
	maxNesting := s.Md.MaxNesting

	for line < endLine {
		line = s.SkipEmptyLines(line)
		s.Line = line
		if line >= endLine {
			break
		}

		if s.SCount[line] < s.BlkIndent {
			break
		}

		if s.Level >= maxNesting {
			s.Line = endLine
			break
		}

		for _, r := range blockRules {
			if r(s, line, endLine, false) {
				break
			}
		}

		s.Tight = !hasEmptyLines

		if s.IsLineEmpty(s.Line - 1) {
			hasEmptyLines = true
		}

		line = s.Line

		if line < endLine && s.IsLineEmpty(line) {
			hasEmptyLines = true
			line++
			s.Line = line
		}
	}
}
