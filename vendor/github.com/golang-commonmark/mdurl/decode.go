// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mdurl

import (
	"bytes"
	"unicode/utf8"
)

func advance(s string, pos int) (byte, int) {
	if pos >= len(s) {
		return 0, len(s) + 1
	}
	if s[pos] != '%' {
		return s[pos], pos + 1
	}
	if pos+2 < len(s) &&
		hexDigit(s[pos+1]) &&
		hexDigit(s[pos+2]) {
		return unhex(s[pos+1])<<4 | unhex(s[pos+2]), pos + 3
	}
	return '%', pos + 1
}

// Decode decodes a percent-encoded URL.
// Invalid percent-encoded sequences are left as is.
// Invalid UTF-8 sequences are replaced with U+FFFD.
func Decode(rawurl string) string {
	var buf bytes.Buffer
	i := 0
	const replacement = "\xEF\xBF\xBD"
outer:
	for i < len(rawurl) {
		r, rlen := utf8.DecodeRuneInString(rawurl[i:])
		if r == '%' && i+2 < len(rawurl) &&
			hexDigit(rawurl[i+1]) &&
			hexDigit(rawurl[i+2]) {
			b := unhex(rawurl[i+1])<<4 | unhex(rawurl[i+2])
			if b < 0x80 {
				buf.WriteByte(b)
				i += 3
				continue
			}
			var n int
			if b&0xe0 == 0xc0 {
				n = 1
			} else if b&0xf0 == 0xe0 {
				n = 2
			} else if b&0xf8 == 0xf0 {
				n = 3
			}
			if n == 0 {
				buf.WriteString(replacement)
				i += 3
				continue
			}
			rb := make([]byte, n+1)
			rb[0] = b
			j := i + 3
			for k := 0; k < n; k++ {
				b, j = advance(rawurl, j)
				if j > len(rawurl) || b&0xc0 != 0x80 {
					buf.WriteString(replacement)
					i += 3
					continue outer
				}
				rb[k+1] = b
			}
			r, _ := utf8.DecodeRune(rb)
			buf.WriteRune(r)
			i = j
			continue
		}
		buf.WriteRune(r)
		i += rlen
	}
	return buf.String()
}
