// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "strings"

const (
	ptRoot = iota
	ptList
	ptBlockQuote
	ptParagraph
	ptReference
)

type StateBlock struct {
	StateCore

	BMarks     []int // offsets of the line beginnings
	EMarks     []int // offsets of the line endings
	TShift     []int // indents for each line
	SCount     []int
	BSCount    []int
	BlkIndent  int  // required block content indent (in a list etc.)
	Line       int  // line index in the source string
	LineMax    int  // number of lines
	Tight      bool // loose or tight mode for lists
	ParentType byte // parent block type
	Level      int
}

func (s *StateBlock) IsLineEmpty(n int) bool {
	return s.BMarks[n]+s.TShift[n] >= s.EMarks[n]
}

func (s *StateBlock) SkipEmptyLines(from int) int {
	for from < s.LineMax && s.IsLineEmpty(from) {
		from++
	}
	return from
}

func (s *StateBlock) SkipSpaces(pos int) int {
	src := s.Src
	for pos < len(src) && byteIsSpace(src[pos]) {
		pos++
	}
	return pos
}

func (s *StateBlock) SkipBytes(pos int, b byte) int {
	src := s.Src
	for pos < len(src) && src[pos] == b {
		pos++
	}
	return pos
}

func (s *StateBlock) SkipBytesBack(pos int, b byte, min int) int {
	for pos > min {
		pos--
		if s.Src[pos] != b {
			return pos + 1
		}
	}
	return pos
}

func (s *StateBlock) SkipSpacesBack(pos int, min int) int {
	for pos > min {
		pos--
		if !byteIsSpace(s.Src[pos]) {
			return pos + 1
		}
	}
	return pos
}

func (s *StateBlock) Lines(begin, end, indent int, keepLastLf bool) string {
	if begin == end {
		return ""
	}

	src := s.Src

	queue := make([]string, end-begin)

	for i, line := 0, begin; line < end; i, line = i+1, line+1 {
		lineIndent := 0
		lineStart := s.BMarks[line]
		first := lineStart
		last := s.EMarks[line]
		if (line+1 < end || keepLastLf) && last < len(src) {
			last++
		}

		for first < last && lineIndent < indent {
			ch := src[first]

			if byteIsSpace(ch) {
				if ch == '\t' {
					lineIndent += 4 - (lineIndent+s.BSCount[line])%4
				} else {
					lineIndent++
				}
			} else if first-lineStart < s.TShift[line] {
				lineIndent++
			} else {
				break
			}

			first++
		}

		if lineIndent > indent {
			queue[i] = strings.Repeat(" ", lineIndent-indent) + src[first:last]
		} else {
			queue[i] = src[first:last]
		}
	}

	return strings.Join(queue, "")
}

func (s *StateBlock) PushToken(tok Token) {
	tok.SetLevel(s.Level)
	s.Tokens = append(s.Tokens, tok)
}

func (s *StateBlock) PushOpeningToken(tok Token) {
	tok.SetLevel(s.Level)
	s.Level++
	s.Tokens = append(s.Tokens, tok)
}

func (s *StateBlock) PushClosingToken(tok Token) {
	s.Level--
	tok.SetLevel(s.Level)
	s.Tokens = append(s.Tokens, tok)
}
