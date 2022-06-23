// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package linkify

import "unicode"

var (
	unreserved = [256]bool{'-': true, '.': true, '_': true, '~': true}
	basicPunct = [256]bool{'.': true, ',': true, '?': true, '!': true, ';': true, ':': true}
	subdelims  = [256]bool{'!': true, '$': true, '&': true, '\'': true, '(': true, ')': true,
		'*': true, '+': true, ',': true, ';': true, '=': true}
	emailcs = [256]bool{'a': true, 'b': true, 'c': true, 'd': true, 'e': true, 'f': true, 'g': true,
		'h': true, 'i': true, 'j': true, 'k': true, 'l': true, 'm': true, 'n': true, 'o': true,
		'p': true, 'q': true, 'r': true, 's': true, 't': true, 'u': true, 'v': true, 'w': true,
		'x': true, 'y': true, 'z': true, 'A': true, 'B': true, 'C': true, 'D': true, 'E': true,
		'F': true, 'G': true, 'H': true, 'I': true, 'J': true, 'K': true, 'L': true, 'M': true,
		'N': true, 'O': true, 'P': true, 'Q': true, 'R': true, 'S': true, 'T': true, 'U': true,
		'V': true, 'W': true, 'X': true, 'Y': true, 'Z': true, '0': true, '1': true, '2': true,
		'3': true, '4': true, '5': true, '6': true, '7': true, '8': true, '9': true, '!': true,
		'#': true, '$': true, '%': true, '&': true, '\'': true, '*': true, '+': true, '/': true,
		'=': true, '?': true, '^': true, '_': true, '`': true, '{': true, '|': true, '}': true,
		'~': true, '-': true}
)

func isAllowedInEmail(r rune) bool {
	return r < 0x7f && emailcs[r]
}

func isLetterOrDigit(r rune) bool {
	return unicode.In(r, unicode.Letter, unicode.Digit)
}

func isPunctOrSpaceOrControl(r rune) bool {
	return r == '<' || r == '>' || unicode.In(r, unicode.Punct, unicode.Space, unicode.Cc)
}

func isUnreserved(r rune) bool {
	return (r < 0x7f && unreserved[r]) || isLetterOrDigit(r)
}

func isSubDelimiter(r rune) bool {
	return r < 0x7f && subdelims[r]
}
