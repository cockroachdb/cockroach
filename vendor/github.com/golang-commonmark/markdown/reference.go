// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "strings"

var referenceTerminatedBy []BlockRule

func ruleReference(s *StateBlock, startLine, _ int, silent bool) bool {
	lines := 0
	pos := s.BMarks[startLine] + s.TShift[startLine]
	max := s.EMarks[startLine]
	nextLine := startLine + 1

	if s.SCount[startLine]-s.BlkIndent >= 4 {
		return false
	}

	src := s.Src

	if src[pos] != '[' {
		return false
	}

	pos++
	for pos < max {
		if src[pos] == ']' && src[pos-1] != '\\' {
			if pos+1 == max {
				return false
			}
			if src[pos+1] != ':' {
				return false
			}
			break
		}
		pos++
	}

	endLine := s.LineMax

	oldParentType := s.ParentType
	s.ParentType = ptReference
outer:
	for ; nextLine < endLine && !s.IsLineEmpty(nextLine); nextLine++ {
		if s.SCount[nextLine]-s.BlkIndent > 3 {
			continue
		}

		if s.SCount[nextLine] < 0 {
			continue
		}

		for _, r := range referenceTerminatedBy {
			if r(s, nextLine, endLine, true) {
				break outer
			}
		}
	}

	str := strings.TrimSpace(s.Lines(startLine, nextLine, s.BlkIndent, false))
	max = len(str)

	var labelEnd int
	for pos = 1; pos < max; pos++ {
		b := str[pos]
		if b == '[' {
			return false
		} else if b == ']' {
			labelEnd = pos
			break
		} else if b == '\n' {
			lines++
		} else if b == '\\' {
			pos++
			if pos < max && str[pos] == '\n' {
				lines++
			}
		}
	}

	if labelEnd <= 0 || labelEnd+1 >= max || str[labelEnd+1] != ':' {
		return false
	}

	for pos = labelEnd + 2; pos < max; pos++ {
		b := str[pos]
		if b == '\n' {
			lines++
		} else if !byteIsSpace(b) {
			break
		}
	}

	href, nlines, endpos, ok := parseLinkDestination(str, pos, max)
	if !ok {
		return false
	}
	href = normalizeLink(href)
	if !validateLink(href) {
		return false
	}

	pos = endpos
	lines += nlines

	destEndPos := pos
	destEndLineNo := lines

	start := pos
	for ; pos < max; pos++ {
		b := str[pos]
		if b == '\n' {
			lines++
		} else if !byteIsSpace(b) {
			break
		}
	}

	title, nlines, endpos, ok := parseLinkTitle(str, pos, max)
	if pos < max && start != pos && ok {
		pos = endpos
		lines += nlines
	} else {
		pos = destEndPos
		lines = destEndLineNo
	}

	for pos < max && byteIsSpace(str[pos]) {
		pos++
	}

	if pos < max && str[pos] != '\n' {
		if title != "" {
			title = ""
			pos = destEndPos
			lines = destEndLineNo
			for pos < max && byteIsSpace(src[pos]) {
				pos++
			}
		}
	}

	if pos < max && str[pos] != '\n' {
		return false
	}

	label := normalizeReference(str[1:labelEnd])
	if label == "" {
		return false
	}

	if silent {
		return true
	}

	if s.Env.References == nil {
		s.Env.References = make(map[string]map[string]string)
	}
	if _, ok := s.Env.References[label]; !ok {
		s.Env.References[label] = map[string]string{
			"title": title,
			"href":  href,
		}
	}

	s.ParentType = oldParentType

	s.Line = startLine + lines + 1

	return true
}
