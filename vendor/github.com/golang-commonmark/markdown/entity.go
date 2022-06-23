// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import "github.com/golang-commonmark/html"

func ruleEntity(s *StateInline, silent bool) bool {
	pos := s.Pos
	src := s.Src

	if src[pos] != '&' {
		return false
	}

	max := s.PosMax

	if pos+1 < max {
		if e, n := html.ParseEntity(src[pos:]); n > 0 {
			if !silent {
				s.Pending.WriteString(e)
			}
			s.Pos += n
			return true
		}
	}

	if !silent {
		s.Pending.WriteByte('&')
	}
	s.Pos++

	return true
}
