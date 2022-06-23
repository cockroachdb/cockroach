// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "strings"

var paragraphTerminatedBy []BlockRule

func ruleParagraph(s *StateBlock, startLine, _ int, _ bool) bool {
	nextLine := startLine + 1
	endLine := s.LineMax

	oldParentType := s.ParentType
	s.ParentType = ptParagraph

outer:
	for ; nextLine < endLine && !s.IsLineEmpty(nextLine); nextLine++ {
		if s.SCount[nextLine]-s.BlkIndent > 3 {
			continue
		}

		if s.SCount[nextLine] < 0 {
			continue
		}

		for _, r := range paragraphTerminatedBy {
			if r(s, nextLine, endLine, true) {
				break outer
			}
		}
	}

	content := strings.TrimSpace(s.Lines(startLine, nextLine, s.BlkIndent, false))

	s.Line = nextLine

	s.PushOpeningToken(&ParagraphOpen{
		Map: [2]int{startLine, s.Line},
	})
	s.PushToken(&Inline{
		Content: content,
		Map:     [2]int{startLine, s.Line},
	})
	s.PushClosingToken(&ParagraphClose{})

	s.ParentType = oldParentType

	return true
}
