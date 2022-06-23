// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func ruleNewline(s *StateInline, silent bool) bool {
	pos := s.Pos
	src := s.Src

	if src[pos] != '\n' {
		return false
	}

	pending := s.Pending.Bytes()
	pmax := len(pending) - 1
	max := s.PosMax

	if !silent {
		if pmax >= 0 && pending[pmax] == ' ' {
			if pmax >= 1 && pending[pmax-1] == ' ' {
				pmax -= 2
				for pmax >= 0 && pending[pmax] == ' ' {
					pmax--
				}
				s.Pending.Truncate(pmax + 1)
				s.PushToken(&Hardbreak{})
			} else {
				s.Pending.Truncate(pmax)
				s.PushToken(&Softbreak{})
			}
		} else {
			s.PushToken(&Softbreak{})
		}
	}

	pos++

	for pos < max && byteIsSpace(src[pos]) {
		pos++
	}

	s.Pos = pos

	return true
}
