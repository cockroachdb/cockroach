// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func parseLinkLabel(s *StateInline, start int, disableNested bool) int {
	src := s.Src
	labelEnd := -1
	max := s.PosMax
	oldPos := s.Pos

	s.Pos = start + 1
	level := 1
	found := false

	for s.Pos < max {
		marker := src[s.Pos]

		if marker == ']' {
			level--
			if level == 0 {
				found = true
				break
			}
		}

		prevPos := s.Pos

		s.Md.Inline.SkipToken(s)

		if marker == '[' {
			if prevPos == s.Pos-1 {
				level++
			} else if disableNested {
				s.Pos = oldPos
				return -1
			}
		}
	}

	if found {
		labelEnd = s.Pos
	}

	s.Pos = oldPos

	return labelEnd
}

func parseLinkDestination(s string, pos, max int) (url string, lines, endpos int, ok bool) {
	start := pos
	if pos < max && s[pos] == '<' {
		pos++
		for pos < max {
			b := s[pos]
			if b == '\n' || byteIsSpace(b) {
				return
			}
			if b == '>' {
				endpos = pos + 1
				url = unescapeAll(s[start+1 : pos])
				ok = true
				return
			}
			if b == '\\' && pos+1 < max {
				pos += 2
				continue
			}

			pos++
		}

		return
	}

	level := 0
	for pos < max {
		b := s[pos]

		if b == ' ' {
			break
		}

		if b < 0x20 || b == 0x7f {
			break
		}

		if b == '\\' && pos+1 < max {
			pos += 2
			continue
		}

		if b == '(' {
			level++
		}

		if b == ')' {
			if level == 0 {
				break
			}
			level--
		}

		pos++
	}

	if start == pos {
		return
	}
	if level != 0 {
		return
	}

	url = unescapeAll(s[start:pos])
	endpos = pos
	ok = true

	return
}

func parseLinkTitle(s string, pos, max int) (title string, nlines, endpos int, ok bool) {
	lines := 0
	start := pos

	if pos >= max {
		return
	}

	marker := s[pos]

	if marker != '"' && marker != '\'' && marker != '(' {
		return
	}

	pos++

	if marker == '(' {
		marker = ')'
	}

	for pos < max {
		switch s[pos] {
		case marker:
			endpos = pos + 1
			nlines = lines
			title = unescapeAll(s[start+1 : pos])
			ok = true
			return
		case '\n':
			lines++
		case '\\':
			if pos+1 < max {
				pos++
				if s[pos] == '\n' {
					lines++
				}
			}
		}
		pos++
	}

	return
}
