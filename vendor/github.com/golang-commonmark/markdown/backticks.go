// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "strings"

func ruleBackticks(s *StateInline, silent bool) bool {
	pos := s.Pos
	src := s.Src

	if src[pos] != '`' {
		return false
	}

	start := pos
	pos++
	max := s.PosMax

	for pos < max && src[pos] == '`' {
		pos++
	}

	marker := src[start:pos]

	matchStart := pos
	matchEnd := pos
	for {
		matchStart = strings.IndexByte(src[matchEnd:], '`')
		if matchStart == -1 {
			break
		}
		matchStart += matchEnd

		matchEnd = matchStart + 1

		for matchEnd < max && src[matchEnd] == '`' {
			matchEnd++
		}

		if matchEnd-matchStart == len(marker) {
			if !silent {
				s.PushToken(&CodeInline{
					Content: normalizeInlineCode(src[pos:matchStart]),
				})
			}
			s.Pos = matchEnd
			return true
		}
	}

	if !silent {
		s.Pending.WriteString(marker)
	}

	s.Pos += len(marker)

	return true
}
