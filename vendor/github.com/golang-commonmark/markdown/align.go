// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

type Align byte

const (
	AlignNone = iota
	AlignLeft
	AlignCenter
	AlignRight
)

func (a Align) String() string {
	switch a {
	case AlignLeft:
		return "left"
	case AlignCenter:
		return "center"
	case AlignRight:
		return "right"
	}
	return ""
}
