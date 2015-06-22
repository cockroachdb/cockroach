// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import "fmt"

var (
	dontEscape = byte(255)
	// encodeMap specifies how to escape binary data with '\'.
	// Complies to http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html
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

func encodeSQLBytes(buf []byte, v []byte) []byte {
	buf = append(buf, "X'"...)
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
		'\n':   'n',
		'\r':   'r',
		'\t':   't',
		26:     'Z', // ctl-Z
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
