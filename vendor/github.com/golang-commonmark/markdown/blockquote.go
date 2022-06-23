// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "unicode/utf8"

var blockquoteTerminatedBy []BlockRule

func ruleBlockQuote(s *StateBlock, startLine, endLine int, silent bool) bool {
	if s.SCount[startLine]-s.BlkIndent >= 4 {
		return false
	}

	pos := s.BMarks[startLine] + s.TShift[startLine]
	max := s.EMarks[startLine]
	src := s.Src

	if pos >= max {
		return false
	}
	if src[pos] != '>' {
		return false
	}
	pos++

	if silent {
		return true
	}

	initial := s.SCount[startLine] + pos - (s.BMarks[startLine] + s.TShift[startLine])
	offset := initial

	spaceAfterMarker := false
	adjustTab := false
	if pos < max {
		if src[pos] == ' ' {
			pos++
			initial++
			offset++
			spaceAfterMarker = true
		} else if src[pos] == '\t' {
			spaceAfterMarker = true
			if (s.BSCount[startLine]+offset)%4 == 3 {
				pos++
				initial++
				offset++
			} else {
				adjustTab = true
			}
		}
	}

	oldBMarks := []int{s.BMarks[startLine]}
	s.BMarks[startLine] = pos

	for pos < max {
		r, size := utf8.DecodeRuneInString(src[pos:])
		if runeIsSpace(r) {
			if r == '\t' {
				d := 0
				if adjustTab {
					d = 1
				}
				offset += 4 - (offset+s.BSCount[startLine]+d)%4
			} else {
				offset++
			}
		} else {
			break
		}
		pos += size
	}

	oldBSCount := []int{s.BSCount[startLine]}
	d := 0
	if spaceAfterMarker {
		d = 1
	}
	s.BSCount[startLine] = s.SCount[startLine] + 1 + d

	lastLineEmpty := pos >= max

	oldSCount := []int{s.SCount[startLine]}
	s.SCount[startLine] = offset - initial

	oldTShift := []int{s.TShift[startLine]}
	s.TShift[startLine] = pos - s.BMarks[startLine]

	oldParentType := s.ParentType
	s.ParentType = ptBlockQuote
	wasOutdented := false

	oldLineMax := s.LineMax

	nextLine := startLine + 1
	for ; nextLine < endLine; nextLine++ {
		if s.SCount[nextLine] < s.BlkIndent {
			wasOutdented = true
		}
		pos = s.BMarks[nextLine] + s.TShift[nextLine]
		max = s.EMarks[nextLine]

		if pos >= max {
			break
		}

		pos++
		if src[pos-1] == '>' && !wasOutdented {
			initial = s.SCount[nextLine] + pos + (s.BMarks[nextLine] + s.TShift[nextLine])
			offset = initial

			if pos >= len(src) || src[pos] != ' ' && src[pos] != '\t' {
				spaceAfterMarker = true
			} else if src[pos] == ' ' {
				pos++
				initial++
				offset++
				adjustTab = false
				spaceAfterMarker = true
			} else if src[pos] == '\t' {
				spaceAfterMarker = true

				if (s.BSCount[nextLine]+offset)%4 == 3 {
					pos++
					initial++
					offset++
					adjustTab = false
				} else {
					adjustTab = true
				}
			}

			oldBMarks = append(oldBMarks, s.BMarks[nextLine])
			s.BMarks[nextLine] = pos

			for pos < max {
				r, size := utf8.DecodeRuneInString(src[pos:])
				if runeIsSpace(r) {
					if r == '\t' {
						d := 0
						if adjustTab {
							d = 1
						}
						offset += 4 - (offset+s.BSCount[startLine]+d)%4
					} else {
						offset++
					}
				} else {
					break
				}
				pos += size
			}

			lastLineEmpty = pos >= max

			oldBSCount = append(oldBSCount, s.BSCount[nextLine])
			d := 0
			if spaceAfterMarker {
				d = 1
			}
			s.BSCount[nextLine] = s.SCount[nextLine] + 1 + d

			oldSCount = append(oldSCount, s.SCount[nextLine])
			s.SCount[nextLine] = offset - initial

			oldTShift = append(oldTShift, s.TShift[nextLine])
			s.TShift[nextLine] = pos - s.BMarks[nextLine]

			continue
		}

		if lastLineEmpty {
			break
		}

		terminate := false
		for _, r := range blockquoteTerminatedBy {
			if r(s, nextLine, endLine, true) {
				terminate = true
				break
			}
		}

		if terminate {
			s.LineMax = nextLine

			if s.BlkIndent != 0 {
				oldBMarks = append(oldBMarks, s.BMarks[nextLine])
				oldBSCount = append(oldBSCount, s.BSCount[nextLine])
				oldTShift = append(oldTShift, s.TShift[nextLine])
				oldSCount = append(oldSCount, s.SCount[nextLine])
				s.SCount[nextLine] -= s.BlkIndent
			}

			break
		}

		oldBMarks = append(oldBMarks, s.BMarks[nextLine])
		oldBSCount = append(oldBSCount, s.BSCount[nextLine])
		oldTShift = append(oldTShift, s.TShift[nextLine])
		oldSCount = append(oldSCount, s.SCount[nextLine])

		s.SCount[nextLine] = -1
	}

	oldIndent := s.BlkIndent
	s.BlkIndent = 0

	tok := &BlockquoteOpen{
		Map: [2]int{startLine, 0},
	}
	s.PushOpeningToken(tok)

	s.Md.Block.Tokenize(s, startLine, nextLine)

	s.PushClosingToken(&BlockquoteClose{})

	s.LineMax = oldLineMax
	s.ParentType = oldParentType
	tok.Map[1] = s.Line

	for i := 0; i < len(oldTShift); i++ {
		s.BMarks[startLine+i] = oldBMarks[i]
		s.TShift[startLine+i] = oldTShift[i]
		s.SCount[startLine+i] = oldSCount[i]
		s.BSCount[startLine+i] = oldBSCount[i]
	}
	s.BlkIndent = oldIndent

	return true
}
