// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func ruleTextCollapse(s *StateInline) {
	level := 0
	tokens := s.Tokens
	max := len(tokens)

	curr := 0
	last := 0
	for ; curr < max; curr++ {
		tok := tokens[curr]
		if tok.Opening() {
			level++
		} else if tok.Closing() {
			level--
		}

		tok.SetLevel(level)

		if text, ok := tok.(*Text); ok && curr+1 < max {
			if text2, ok := tokens[curr+1].(*Text); ok {
				text2.Content = text.Content + text2.Content
				continue
			}
		}

		if curr != last {
			tokens[last] = tokens[curr]
		}
		last++
	}

	if curr != last {
		tokens = tokens[:last]
	}

	s.Tokens = tokens
}
