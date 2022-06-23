// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "strings"

func ruleHeading(s *StateBlock, startLine, _ int, silent bool) bool {
	pos := s.BMarks[startLine] + s.TShift[startLine]
	max := s.EMarks[startLine]
	src := s.Src

	if s.SCount[startLine]-s.BlkIndent >= 4 {
		return false
	}

	if pos >= max || src[pos] != '#' {
		return false
	}

	level := 1
	pos++
	for pos < max && src[pos] == '#' && level <= 6 {
		level++
		pos++
	}

	if level > 6 || (pos < max && !byteIsSpace(src[pos])) {
		return false
	}

	if silent {
		return true
	}

	max = s.SkipSpacesBack(max, pos)
	tmp := s.SkipBytesBack(max, '#', pos)
	if tmp > pos && byteIsSpace(src[tmp-1]) {
		max = tmp
	}

	s.Line = startLine + 1

	s.PushOpeningToken(&HeadingOpen{
		HLevel: level,
		Map:    [2]int{startLine, s.Line},
	})

	if pos < max {
		s.PushToken(&Inline{
			Content: strings.TrimSpace(src[pos:max]),
			Map:     [2]int{startLine, s.Line},
		})
	}
	s.PushClosingToken(&HeadingClose{HLevel: level})

	return true
}
