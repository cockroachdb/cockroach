// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mdurl

func hexDigit(b byte) bool {
	return digit(b) || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}

func unhex(b byte) byte {
	switch {
	case digit(b):
		return b - '0'
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10
	}
	panic("unhex: not a hex digit")
}

func letter(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

func digit(b byte) bool {
	return b >= '0' && b <= '9'
}

func byteToUpper(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - 'a' + 'A'
	}
	return b
}
