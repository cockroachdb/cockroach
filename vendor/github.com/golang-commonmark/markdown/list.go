// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "strconv"

var listTerminatedBy []BlockRule

func skipBulletListMarker(s *StateBlock, startLine int) int {
	pos := s.BMarks[startLine] + s.TShift[startLine]
	max := s.EMarks[startLine]
	src := s.Src

	if pos >= max {
		return -1
	}

	marker := src[pos]
	if marker != '*' && marker != '-' && marker != '+' {
		return -1
	}
	pos++

	if pos < max && !byteIsSpace(src[pos]) {
		return -1
	}

	return pos
}

func skipOrderedListMarker(s *StateBlock, startLine int) int {
	start := s.BMarks[startLine] + s.TShift[startLine]
	pos := start
	max := s.EMarks[startLine]

	if pos+1 >= max {
		return -1
	}

	src := s.Src
	ch := src[pos]
	if ch < '0' || ch > '9' {
		return -1
	}
	pos++

	for {
		if pos >= max {
			return -1
		}

		ch = src[pos]
		pos++

		if ch >= '0' && ch <= '9' {
			if pos-start >= 10 {
				return -1
			}
			continue
		}

		if ch == ')' || ch == '.' {
			break
		}

		return -1
	}

	if pos < max && !byteIsSpace(src[pos]) {
		return -1
	}

	return pos
}

func markParagraphsTight(s *StateBlock, idx int) {
	level := s.Level + 2
	tokens := s.Tokens

	for i := idx + 2; i < len(tokens)-2; i++ {
		if tokens[i].Level() == level {
			if tok, ok := tokens[i].(*ParagraphOpen); ok {
				tok.Hidden = true
				i += 2
				tokens[i].(*ParagraphClose).Hidden = true
			}
		}
	}
}

func ruleList(s *StateBlock, startLine, endLine int, silent bool) bool {
	isTerminatingParagraph := false
	tight := true

	if s.SCount[startLine]-s.BlkIndent >= 4 {
		return false
	}
	src := s.Src

	if silent && s.ParentType == ptParagraph {
		if s.TShift[startLine] >= s.BlkIndent {
			isTerminatingParagraph = true
		}
	}

	var start int
	var markerValue int

	isOrdered := false
	posAfterMarker := skipOrderedListMarker(s, startLine)
	if posAfterMarker > 0 {
		isOrdered = true
		start = s.BMarks[startLine] + s.TShift[startLine]
		markerValue, _ = strconv.Atoi(src[start : posAfterMarker-1])

		if isTerminatingParagraph && markerValue != 1 {
			return false
		}
	} else {
		posAfterMarker = skipBulletListMarker(s, startLine)
		if posAfterMarker < 0 {
			return false
		}
	}

	if isTerminatingParagraph {
		if s.SkipSpaces(posAfterMarker) >= s.EMarks[startLine] {
			return false
		}
	}

	markerChar := src[posAfterMarker-1]

	if silent {
		return true
	}

	tokenIdx := len(s.Tokens)

	var listMap *[2]int
	if isOrdered {
		tok := &OrderedListOpen{
			Order: markerValue,
			Map:   [2]int{startLine, 0},
		}
		s.PushOpeningToken(tok)
		listMap = &tok.Map
	} else {
		tok := &BulletListOpen{
			Map: [2]int{startLine, 0},
		}
		s.PushOpeningToken(tok)
		listMap = &tok.Map
	}

	nextLine := startLine
	prevEmptyEnd := false

	oldParentType := s.ParentType
	s.ParentType = ptList

	var pos int
	var contentStart int

outer:
	for nextLine < endLine {
		pos = posAfterMarker
		max := s.EMarks[nextLine]

		initial := s.SCount[nextLine] + posAfterMarker - (s.BMarks[startLine] + s.TShift[startLine])
		offset := initial

	loop:
		for pos < max {
			switch src[pos] {
			case '\t':
				offset += 4 - (offset+s.BSCount[nextLine])%4
			case ' ':
				offset++
			default:
				break loop
			}
			pos++
		}

		contentStart = pos

		indentAfterMarker := 1
		if contentStart < max {
			if iam := offset - initial; iam <= 4 {
				indentAfterMarker = iam
			}
		}

		indent := initial + indentAfterMarker

		tok := &ListItemOpen{
			Map: [2]int{startLine, 0},
		}
		s.PushOpeningToken(tok)
		itemMap := &tok.Map

		oldIndent := s.BlkIndent
		oldTight := s.Tight
		oldTShift := s.TShift[startLine]
		oldLIndent := s.SCount[startLine]
		s.BlkIndent = indent
		s.Tight = true
		s.TShift[startLine] = contentStart - s.BMarks[startLine]
		s.SCount[startLine] = offset

		if contentStart >= max && s.IsLineEmpty(startLine+1) {
			s.Line = min(s.Line+2, endLine)
		} else {
			s.Md.Block.Tokenize(s, startLine, endLine)
		}

		if !s.Tight || prevEmptyEnd {
			tight = false
		}

		prevEmptyEnd = s.Line-startLine > 1 && s.IsLineEmpty(s.Line-1)

		s.BlkIndent = oldIndent
		s.TShift[startLine] = oldTShift
		s.SCount[startLine] = oldLIndent
		s.Tight = oldTight

		s.PushClosingToken(&ListItemClose{})

		startLine = s.Line
		nextLine = startLine
		(*itemMap)[1] = nextLine
		contentStart = s.BMarks[startLine]

		if nextLine >= endLine {
			break
		}

		if s.SCount[nextLine] < s.BlkIndent {
			break
		}

		for _, r := range listTerminatedBy {
			if r(s, nextLine, endLine, true) {
				break outer
			}
		}

		if isOrdered {
			posAfterMarker = skipOrderedListMarker(s, nextLine)
			if posAfterMarker < 0 {
				break
			}
		} else {
			posAfterMarker = skipBulletListMarker(s, nextLine)
			if posAfterMarker < 0 {
				break
			}
		}

		if markerChar != src[posAfterMarker-1] {
			break
		}
	}

	if isOrdered {
		s.PushClosingToken(&OrderedListClose{})
	} else {
		s.PushClosingToken(&BulletListClose{})
	}

	(*listMap)[1] = nextLine
	s.Line = nextLine
	s.ParentType = oldParentType

	if tight {
		markParagraphsTight(s, tokenIdx)
	}

	return true
}
