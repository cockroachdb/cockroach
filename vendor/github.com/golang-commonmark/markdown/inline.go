// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func ruleInline(s *StateCore) {
	for _, tok := range s.Tokens {
		if tok, ok := tok.(*Inline); ok {
			tok.Children = s.Md.Inline.Parse(tok.Content, s.Md, s.Env)
		}
	}
}
