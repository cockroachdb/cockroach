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
	"fmt"
	"strings"
)

var (
	dontEscape = byte(255)
	// encodeMap specifies how to escape binary data with '\'.
	encodeMap [256]byte
	hexMap    [256][]byte
)

func encodeSQLString(in string) string {
	// See http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html
	start := 0
	buf := make([]byte, 0, len(in)+3)
	for i := range in {
		ch := in[i]
		if encodedChar := encodeMap[ch]; encodedChar != dontEscape {
			if start == 0 {
				buf = append(buf, 'e', '\'') // begin e'xxx' string
			}
			buf = append(buf, in[start:i]...)
			buf = append(buf, '\\', encodedChar)
			start = i + 1
		}
	}
	if start == 0 {
		buf = append(buf, '\'') // begin 'xxx' string if nothing was escaped
	}
	buf = append(buf, in[start:]...)
	buf = append(buf, '\'')
	return string(buf)
}

func encodeSQLIdent(s string) string {
	if _, ok := reservedKeywords[strings.ToUpper(s)]; ok {
		return fmt.Sprintf("\"%s\"", s)
	}
	// The string needs quoting if it does not match the ident format.
	if isIdent(s) {
		return s
	}

	// The only character that requires escaping is a double quote.
	buf := make([]byte, 0, len(s)+2)
	buf = append(buf, '"')
	start := 0
	for i, n := 0, len(s); i < n; i++ {
		ch := s[i]
		if ch == '"' {
			if start != i {
				buf = append(buf, s[start:i]...)
			}
			start = i + 1
			buf = append(buf, ch, ch) // add extra copy of ch
		}
	}
	if start < len(s) {
		buf = append(buf, s[start:]...)
	}
	buf = append(buf, '"')
	return string(buf)
}

func encodeSQLBytes(in string) string {
	start := 0
	buf := make([]byte, 0, len(in)+3)
	buf = append(buf, "b'"...)
	for i := range in {
		ch := in[i]
		if encodedChar := encodeMap[ch]; encodedChar != dontEscape {
			buf = append(buf, in[start:i]...)
			buf = append(buf, '\\', encodedChar)
			start = i + 1
		} else if ch >= 0x80 {
			buf = append(buf, hexMap[ch]...)
			start = i + 1
		}
	}
	buf = append(buf, in[start:]...)
	buf = append(buf, '\'')
	return string(buf)
}

func init() {
	encodeRef := map[byte]byte{
		'\x00': '0',
		'\b':   'b',
		'\f':   'f',
		'\n':   'n',
		'\r':   'r',
		'\t':   't',
		'\\':   '\\',
		'\'':   '\'',
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
