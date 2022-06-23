// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "strings"

func ruleLHeading(s *StateBlock, startLine, endLine int, silent bool) bool {
	nextLine := startLine + 1

	if s.SCount[startLine]-s.BlkIndent >= 4 {
		return false
	}

	oldParentType := s.ParentType
	s.ParentType = ptParagraph
	src := s.Src

	var pos int
	var hLevel int
outer:
	for ; nextLine < endLine && !s.IsLineEmpty(nextLine); nextLine++ {
		if s.SCount[nextLine]-s.BlkIndent > 3 {
			continue
		}

		if s.SCount[nextLine] >= s.BlkIndent {
			pos = s.BMarks[nextLine] + s.TShift[nextLine]
			max := s.EMarks[nextLine]

			if pos < max {
				marker := src[pos]

				if marker == '-' || marker == '=' {
					pos = s.SkipBytes(pos, marker)
					pos = s.SkipSpaces(pos)

					if pos >= max {
						hLevel = 1
						if marker == '-' {
							hLevel++
						}
						break
					}
				}
			}
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

	if hLevel == 0 {
		return false
	}

	s.Line = nextLine + 1

	s.PushOpeningToken(&HeadingOpen{
		HLevel: hLevel,
		Map:    [2]int{startLine, s.Line},
	})
	s.PushToken(&Inline{
		Content: strings.TrimSpace(s.Lines(startLine, nextLine, s.BlkIndent, false)),
		Map:     [2]int{startLine, s.Line - 1},
	})
	s.PushClosingToken(&HeadingClose{HLevel: hLevel})

	s.ParentType = oldParentType

	return true
}
