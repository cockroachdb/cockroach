// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func ruleLink(s *StateInline, silent bool) bool {
	pos := s.Pos
	oldPos := s.Pos
	max := s.PosMax
	start := s.Pos
	parseReference := true
	src := s.Src

	if src[pos] != '[' {
		return false
	}

	labelStart := pos + 1
	labelEnd := parseLinkLabel(s, pos, true)

	if labelEnd < 0 {
		return false
	}

	pos = labelEnd + 1

	var title, href, label string
	if pos < max && src[pos] == '(' {
		parseReference = false

		pos++
		for pos < max {
			code := src[pos]
			if !byteIsSpace(code) && code != '\n' {
				break
			}
			pos++
		}
		if pos >= max {
			return false
		}

		start = pos
		url, _, endpos, ok := parseLinkDestination(src, pos, s.PosMax)
		if ok {
			url = normalizeLink(url)
			if validateLink(url) {
				pos = endpos
				href = url
			}
		}

		start = pos
		for pos < max {
			code := src[pos]
			if !byteIsSpace(code) && code != '\n' {
				break
			}
			pos++
		}

		title, _, endpos, ok = parseLinkTitle(src, pos, s.PosMax)
		if pos < max && start != pos && ok {
			pos = endpos
			for pos < max {
				code := src[pos]
				if !byteIsSpace(code) && code != '\n' {
					break
				}
				pos++
			}
		}

		if pos >= max || src[pos] != ')' {
			parseReference = true
		}

		pos++
	}

	if parseReference {
		if s.Env.References == nil {
			return false
		}

		if pos < max && src[pos] == '[' {
			start := pos + 1
			pos = parseLinkLabel(s, pos, false)
			if pos >= 0 {
				label = src[start:pos]
				pos++
			} else {
				pos = labelEnd + 1
			}
		} else {
			pos = labelEnd + 1
		}

		if label == "" {
			label = src[labelStart:labelEnd]
		}

		ref, ok := s.Env.References[normalizeReference(label)]
		if !ok {
			s.Pos = oldPos
			return false
		}

		href = ref["href"]
		title = ref["title"]
	}

	if !silent {
		s.Pos = labelStart
		s.PosMax = labelEnd

		s.PushOpeningToken(&LinkOpen{
			Href:  href,
			Title: title,
		})

		s.Md.Inline.Tokenize(s)

		s.PushClosingToken(&LinkClose{})
	}

	s.Pos = pos
	s.PosMax = max

	return true
}
