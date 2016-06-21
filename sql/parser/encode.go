// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

var (
	dontEscape = byte(255)
	// encodeMap specifies how to escape binary data with '\'.
	encodeMap [256]byte
	hexMap    [256][]byte
)

// encodeSQLString writes a string literal to buf. All unicode and
// non-printable characters are escaped.
func encodeSQLString(buf *bytes.Buffer, in string) {
	// See http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html
	start := 0
	nextStart := false
	var i int
	var r rune
	writeStart := func() {
		if start == 0 {
			buf.WriteString("e'") // begin e'xxx' string
		}
		buf.WriteString(in[start:i])
		// Since we don't know the byte width of the current unicode code point
		// (and thus what to set start to, since it's the current index + length of
		// current code point), set a flag to set start at the next iteration.
		nextStart = true
	}
	// Loop through each unicode code point.
	for i, r = range in {
		if nextStart {
			start = i
			nextStart = false
		}
		if r == utf8.RuneError {
			// Errors are due to invalid unicode points, so escape the byte (Go guarantees
			// that it's a byte in the case of an error).
			writeStart()
			buf.Write(hexMap[in[i]])
		} else if r < 256 {
			// For single-byte runes, do the same as encodeSQLBytes.
			ch := byte(r)
			if encodedChar := encodeMap[ch]; encodedChar != dontEscape {
				writeStart()
				buf.WriteByte('\\')
				buf.WriteByte(encodedChar)
			} else if ch < 0x20 || ch >= 0x7F {
				writeStart()
				// Escape non-printable characters.
				buf.Write(hexMap[ch])
			}
		} else {
			// For multi-byte runes, print them based on their width.
			writeStart()
			if r <= math.MaxInt16 {
				fmt.Fprintf(buf, `\u%04X`, r)
			} else {
				fmt.Fprintf(buf, `\U%08X`, r)
			}
		}
	}
	if start == 0 && !nextStart {
		buf.WriteByte('\'') // begin 'xxx' string if nothing was escaped
	}
	if !nextStart {
		buf.WriteString(in[start:])
	}
	buf.WriteByte('\'')
}

func encodeSQLIdent(buf *bytes.Buffer, s string) {
	if _, ok := reservedKeywords[strings.ToUpper(s)]; ok {
		buf.WriteString(`"`)
		buf.WriteString(s)
		buf.WriteString(`"`)
		return
	}
	// The string needs quoting if it does not match the ident format.
	if isIdent(s) {
		buf.WriteString(s)
		return
	}

	// The only character that requires escaping is a double quote.
	buf.WriteString(`"`)
	start := 0
	for i, n := 0, len(s); i < n; i++ {
		ch := s[i]
		if ch == '"' {
			if start != i {
				buf.WriteString(s[start:i])
			}
			start = i + 1
			buf.WriteByte(ch)
			buf.WriteByte(ch) // add extra copy of ch
		}
	}
	if start < len(s) {
		buf.WriteString(s[start:])
	}
	buf.WriteString(`"`)
}

func encodeSQLBytes(buf *bytes.Buffer, in string) {
	start := 0
	buf.WriteString("b'")
	// Loop over the bytes of the string (i.e., don't use range over unicode
	// code points).
	for i, n := 0, len(in); i < n; i++ {
		ch := in[i]
		if encodedChar := encodeMap[ch]; encodedChar != dontEscape {
			buf.WriteString(in[start:i])
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
			start = i + 1
		} else if ch < 0x20 || ch >= 0x7F {
			buf.WriteString(in[start:i])
			// Escape non-printable characters.
			buf.Write(hexMap[ch])
			start = i + 1
		}
	}
	buf.WriteString(in[start:])
	buf.WriteByte('\'')
}

func init() {
	encodeRef := map[byte]byte{
		'\b': 'b',
		'\f': 'f',
		'\n': 'n',
		'\r': 'r',
		'\t': 't',
		'\\': '\\',
		'\'': '\'',
	}

	for i := range encodeMap {
		encodeMap[i] = dontEscape
	}
	for i := range encodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			encodeMap[byte(i)] = to
		}
	}

	for i := range hexMap {
		hexMap[i] = []byte(fmt.Sprintf("\\x%02x", i))
	}
}
