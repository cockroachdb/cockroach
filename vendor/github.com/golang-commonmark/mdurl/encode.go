// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mdurl

import (
	"bytes"
	"strings"
	"unicode/utf8"
)

// Encode percent-encodes rawurl, avoiding double encoding.
// It doesn't touch:
// - alphanumeric characters ([0-9a-zA-Z]);
// - percent-encoded characters (%[0-9a-fA-F]{2});
// - excluded characters ([;/?:@&=+$,-_.!~*'()#]).
// Invalid UTF-8 sequences are replaced with U+FFFD.
func Encode(rawurl string) string {
	const hexdigit = "0123456789ABCDEF"
	var buf bytes.Buffer
	i := 0
	for i < len(rawurl) {
		r, rlen := utf8.DecodeRuneInString(rawurl[i:])
		if r >= 0x80 {
			for j, n := i, i+rlen; j < n; j++ {
				b := rawurl[j]
				buf.WriteByte('%')
				buf.WriteByte(hexdigit[(b>>4)&0xf])
				buf.WriteByte(hexdigit[b&0xf])
			}
		} else if r == '%' {
			if i+2 < len(rawurl) &&
				hexDigit(rawurl[i+1]) &&
				hexDigit(rawurl[i+2]) {
				buf.WriteByte('%')
				buf.WriteByte(byteToUpper(rawurl[i+1]))
				buf.WriteByte(byteToUpper(rawurl[i+2]))
				i += 2
			} else {
				buf.WriteString("%25")
			}
		} else if strings.IndexByte("!#$&'()*+,-./0123456789:;=?@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~", byte(r)) == -1 {
			buf.WriteByte('%')
			buf.WriteByte(hexdigit[(r>>4)&0xf])
			buf.WriteByte(hexdigit[r&0xf])
		} else {
			buf.WriteByte(byte(r))
		}
		i += rlen
	}
	return buf.String()
}
