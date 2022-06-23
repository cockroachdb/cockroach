// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func ruleImage(s *StateInline, silent bool) bool {
	pos := s.Pos
	max := s.PosMax

	if pos+2 >= max {
		return false
	}

	src := s.Src
	if src[pos] != '!' || src[pos+1] != '[' {
		return false
	}

	labelStart := pos + 2
	labelEnd := parseLinkLabel(s, pos+1, false)
	if labelEnd < 0 {
		return false
	}

	var href, title, label string
	oldPos := pos
	pos = labelEnd + 1
	if pos < max && src[pos] == '(' {
		pos++
		for pos < max {
			b := src[pos]
			if !byteIsSpace(b) && b != '\n' {
				break
			}
			pos++
		}
		if pos >= max {
			return false
		}

		start := pos
		url, _, endpos, ok := parseLinkDestination(src, pos, s.PosMax)
		if ok {
			url = normalizeLink(url)
			if validateLink(url) {
				href = url
				pos = endpos
			}
		}

		start = pos
		for pos < max {
			b := src[pos]
			if !byteIsSpace(b) && b != '\n' {
				break
			}
			pos++
		}
		if pos >= max {
			return false
		}

		title, _, endpos, ok = parseLinkTitle(src, pos, s.PosMax)
		if pos < max && start != pos && ok {
			pos = endpos
			for pos < max {
				b := src[pos]
				if !byteIsSpace(b) && b != '\n' {
					break
				}
				pos++
			}
		}

		if pos >= max || src[pos] != ')' {
			s.Pos = oldPos
			return false
		}

		pos++

	} else {
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
		content := src[labelStart:labelEnd]

		tokens := s.Md.Inline.Parse(content, s.Md, s.Env)

		s.PushToken(&Image{
			Src:    href,
			Title:  title,
			Tokens: tokens,
		})
	}

	s.Pos = pos
	s.PosMax = max

	return true
}
