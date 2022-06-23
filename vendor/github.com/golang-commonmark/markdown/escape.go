// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "strings"

func escaped(b byte) bool {
	return strings.IndexByte("\\!\"#$%&'()*+,./:;<=>?@[]^_`{|}~-", b) != -1
}

func ruleEscape(s *StateInline, silent bool) bool {
	pos := s.Pos
	src := s.Src

	if src[pos] != '\\' {
		return false
	}

	pos++
	max := s.PosMax

	if pos < max {
		b := src[pos]

		if b < 0x7f && escaped(b) {
			if !silent {
				s.Pending.WriteByte(b)
			}
			s.Pos += 2
			return true
		}

		if b == '\n' {
			if !silent {
				s.PushToken(&Hardbreak{})
			}

			pos++

			for pos < max {
				b := src[pos]
				if !byteIsSpace(b) {
					break
				}
				pos++
			}

			s.Pos = pos
			return true
		}
	}

	if !silent {
		s.Pending.WriteByte('\\')
	}
	s.Pos++

	return true
}
