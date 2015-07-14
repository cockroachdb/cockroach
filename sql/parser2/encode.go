// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser2

import (
	"bytes"
	"fmt"
)

var (
	dontEscape = byte(255)
	// encodeMap specifies how to escape binary data with '\'.
	encodeMap [256]byte
	// decodeMap is the reverse of encodeMap
	decodeMap [256]byte
	hexMap    [256][]byte
)

func encodeSQLString(buf []byte, in []byte) []byte {
	buf = append(buf, '\'')
	for _, ch := range in {
		if encodedChar := encodeMap[ch]; encodedChar == dontEscape {
			buf = append(buf, ch)
		} else {
			buf = append(buf, '\\')
			buf = append(buf, encodedChar)
		}
	}
	buf = append(buf, '\'')
	return buf
}

// TODO(pmattis): This method needs testing.
func encodeSQLIdent(buf *bytes.Buffer, s string) {
	// The string needs quoting if it does not match the ident format.
	if isIdent(s) {
		_, _ = buf.WriteString(s)
		return
	}

	// The only characters we need to escape are '"' and '\\'.
	_ = buf.WriteByte('"')
	start := 0
	for i, n := 0, len(s); i < n; i++ {
		ch := s[i]
		if ch == '"' || ch == '\\' {
			if start != i {
				_, _ = buf.WriteString(s[start:i])
			}
			start = i + 1
			_ = buf.WriteByte('\\')
			_ = buf.WriteByte(ch)
		}
	}
	if start < len(s) {
		_, _ = buf.WriteString(s[start:])
	}
	_ = buf.WriteByte('"')
}

func encodeSQLBytes(buf []byte, v []byte) []byte {
	buf = append(buf, "x'"...)
	for _, d := range v {
		buf = append(buf, hexMap[d]...)
	}
	buf = append(buf, '\'')
	return buf
}

func init() {
	encodeRef := map[byte]byte{
		'\x00': '0',
		'\'':   '\'',
		'"':    '"',
		'\b':   'b',
		'\f':   'f',
		'\n':   'n',
		'\r':   'r',
		'\t':   't',
		'\\':   '\\',
	}

	for i := range encodeMap {
		encodeMap[i] = dontEscape
		decodeMap[i] = dontEscape
	}
	for i := range encodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			encodeMap[byte(i)] = to
			decodeMap[to] = byte(i)
		}
	}

	for i := range hexMap {
		hexMap[i] = []byte(fmt.Sprintf("%02x", i))
	}
}
