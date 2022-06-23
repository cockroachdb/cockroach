// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func ruleHR(s *StateBlock, startLine, endLine int, silent bool) bool {
	pos := s.BMarks[startLine] + s.TShift[startLine]
	max := s.EMarks[startLine]

	if pos >= max {
		return false
	}

	if s.SCount[startLine]-s.BlkIndent >= 4 {
		return false
	}

	src := s.Src
	marker := src[pos]
	pos++

	if marker != '*' && marker != '-' && marker != '_' {
		return false
	}

	cnt := 1
	for pos < max {
		ch := src[pos]
		pos++
		if ch != marker && !byteIsSpace(ch) {
			return false
		}
		if ch == marker {
			cnt++
		}
	}

	if cnt < 3 {
		return false
	}

	if silent {
		return true
	}

	s.Line = startLine + 1

	s.PushToken(&Hr{
		Map: [2]int{startLine, s.Line},
	})

	return true
}
