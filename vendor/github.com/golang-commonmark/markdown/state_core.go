// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

type StateCore struct {
	Src       string
	Tokens    []Token
	bootstrap [3]Token
	Md        *Markdown
	Env       *Environment
}
