// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "strings"

func ruleFence(s *StateBlock, startLine, endLine int, silent bool) bool {
	haveEndMarker := false
	pos := s.BMarks[startLine] + s.TShift[startLine]
	max := s.EMarks[startLine]

	if s.SCount[startLine]-s.BlkIndent >= 4 {
		return false
	}
	if pos+3 > max {
		return false
	}

	src := s.Src

	marker := src[pos]
	if marker != '~' && marker != '`' {
		return false
	}

	mem := pos
	pos = s.SkipBytes(pos, marker)

	len := pos - mem

	if len < 3 {
		return false
	}

	params := strings.TrimSpace(src[pos:max])

	if strings.IndexByte(params, marker) >= 0 {
		return false
	}

	if silent {
		return true
	}

	nextLine := startLine

	for {
		nextLine++
		if nextLine >= endLine {
			break
		}

		mem = s.BMarks[nextLine] + s.TShift[nextLine]
		pos = mem
		max = s.EMarks[nextLine]

		if pos < max && s.SCount[nextLine] < s.BlkIndent {
			break
		}

		if pos >= max || src[pos] != marker {
			continue
		}

		if s.SCount[nextLine]-s.BlkIndent >= 4 {
			continue
		}

		pos = s.SkipBytes(pos, marker)

		if pos-mem < len {
			continue
		}

		pos = s.SkipSpaces(pos)

		if pos < max {
			continue
		}

		haveEndMarker = true

		break
	}

	len = s.SCount[startLine]

	s.Line = nextLine
	if haveEndMarker {
		s.Line++
	}

	s.PushToken(&Fence{
		Params:  params,
		Content: s.Lines(startLine+1, nextLine, len, true),
		Map:     [2]int{startLine, s.Line},
	})

	return true
}
