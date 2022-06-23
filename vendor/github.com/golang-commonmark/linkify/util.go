// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package linkify

func digit(b byte) bool {
	return b >= '0' && b <= '9'
}

func hexDigit(b byte) bool {
	return digit(b) || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}

func byteToLower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b - 'A' + 'a'
	}
	return b
}
