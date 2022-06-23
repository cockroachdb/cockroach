// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func ruleCode(s *StateBlock, startLine, endLine int, _ bool) bool {
	if s.SCount[startLine]-s.BlkIndent < 4 {
		return false
	}

	nextLine := startLine + 1
	last := nextLine

	for nextLine < endLine {
		if s.IsLineEmpty(nextLine) {
			nextLine++
			continue
		}

		if s.SCount[nextLine]-s.BlkIndent >= 4 {
			nextLine++
			last = nextLine
			continue
		}

		break
	}

	s.Line = last
	s.PushToken(&CodeBlock{
		Content: s.Lines(startLine, last, 4+s.BlkIndent, true),
		Map:     [2]int{startLine, s.Line},
	})

	return true
}
