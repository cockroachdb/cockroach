// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This is a fork of pkg/json package.

// Copyright (c) 2020, Dave Cheney <dave@cheney.net>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//   - Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//   - Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package tokenizer

import (
	"sync"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	// ObjectStart indicates the start of JSON object.
	ObjectStart = '{' // {
	// ObjectEnd is the end of the JSON object.
	ObjectEnd = '}' // }
	// String is the start of JSON string.
	String = '"' // "
	// Colon indicates that the token is an object value.
	Colon = ':' // :
	// Comma indicates the next JSON element.
	Comma = ',' // ,
	// ArrayStart is the start of JSON array.
	ArrayStart = '[' // [
	// ArrayEnd is the end of JSON array.
	ArrayEnd = ']' // ]
	// True is 'true' token.
	True = 't' // t
	// False is 'false'.
	False = 'f' // f
	// Null is 'null' token.
	Null = 'n' // n
)

// Scanner implements a JSON scanner as defined in RFC 7159.
type Scanner struct {
	data   []byte
	offset int
	tmpBuf *buffer
}

var whitespace = [256]bool{
	' ':  true,
	'\r': true,
	'\n': true,
	'\t': true,
}

// Next returns a []byte referencing the next lexical token in the stream.
// The []byte is valid until Next is called again.
// If the stream is at its end, or an error has occured, Next returns a zero
// length []byte slice.
//
// A valid token begins with one of the following:
//
//	{ Object start
//	[ Array start
//	} Object end
//	] Array End
//	, Literal comma
//	: Literal colon
//	t JSON true
//	f JSON false
//	n JSON null
//	" A string, possibly containing backslash escaped entites.
//	-, 0-9 A number
func (s *Scanner) Next() []byte {
	w := s.buf()
	for pos, c := range w {
		// Strip any leading whitespace.
		if whitespace[c] {
			continue
		}

		// Simple case.
		switch c {
		case ObjectStart, ObjectEnd, Colon, Comma, ArrayStart, ArrayEnd:
			s.offset += pos + 1
			return w[pos : pos+1]
		}

		s.offset += pos
		switch c {
		case True:
			return s.next(validateToken(s.buf(), "true"))
		case False:
			return s.next(validateToken(s.buf(), "false"))
		case Null:
			return s.next(validateToken(s.buf(), "null"))
		case String:
			return s.parseString()
		default:
			// Ensure the number is correct.
			return s.next(s.parseNumber(c))
		}
	}

	// it's all whitespace, ignore it
	s.offset += len(w)
	return nil // eof
}

var bufferPool = sync.Pool{New: func() interface{} { return &buffer{} }}

// Release releases scanner resources.
func (s *Scanner) Release() {
	if s.tmpBuf != nil {
		bufferPool.Put(s.tmpBuf)
	}
}

func (s *Scanner) scratch() *buffer {
	if s.tmpBuf == nil {
		s.tmpBuf = bufferPool.Get().(*buffer)
	}
	s.tmpBuf.Reset()
	return s.tmpBuf
}

// buf returns unread portion of the input.
func (s *Scanner) buf() []byte {
	if s.offset == len(s.data) {
		return nil
	}
	return s.data[s.offset:]
}

// next returns n bytes from the input, and advances offset by n bytes.
func (s *Scanner) next(n int) (res []byte) {
	res = s.data[s.offset : s.offset+n]
	s.offset += n
	return res
}

// More returns true if scanner has more non-white space tokens.
func (s *Scanner) More() bool {
	for i := s.offset; i < len(s.data); i++ {
		if !whitespace[s.data[i]] {
			return true
		}
	}
	return false
}

func validateToken(w []byte, expected string) int {
	n := len(expected)
	if len(w) >= n {
		if string(w[:n]) != expected {
			// doesn't match
			return 0
		}
		return n
	}
	return 0 // eof
}

// parseString parses the string located at the start of the window. Returns
// parsed string token, including enclosing `"`.
func (s *Scanner) parseString() []byte {
	pos := 1 // skip opening quote.
	w := s.buf()[1:]

	// Fast path: string does not have escape sequences.
	for _, c := range w {
		if c == '\\' {
			// Alas, things are not that simple, we must handle escaped characters.
			buf, n := s.parseStringSlow(pos)
			s.offset += n
			return buf
		}

		pos++
		if c == '"' {
			return s.next(pos)
		}

		if c < ' ' {
			// Unescaped controlled characters < 0x30 not allowed.
			return nil
		}
	}
	return nil // eof
}

// parseStringSlow parses string containing escape sequences.
// Everything up to pos does not have escape sequence, and buf[pos] is the first '\'
// encountered when parsing the string.
func (s *Scanner) parseStringSlow(pos int) ([]byte, int) {
	w := s.buf()
	// Sanity check.
	if pos < 1 || len(w) < pos || w[0] != '"' || w[pos] != '\\' {
		return nil, pos
	}

	// Escaped characters necessitate that the returned token will be
	// different from the input token.  Reset scratch buffer, and copy
	// everything processed so far.
	b := s.scratch()
	b.Append(w[:pos])
	w = w[pos:]

	for wp := 0; wp < len(w); {
		switch c := w[wp]; {
		default:
			b.AppendByte(c)
			pos++
			wp++
		case c < ' ':
			// Control characters < 0x30 must be escaped.
			return nil, pos
		case c == '"':
			b.AppendByte(c)
			pos++
			return b.Bytes(), pos
		case c == '\\':
			switch n := readEscaped(w[wp:], b); n {
			case 0:
				return nil, pos // Error
			default:
				wp += n
				pos += n
			}
		}
	}
	return nil, pos // eof
}

// readEscaped reads escape sequence from the window w, and writes unescaped
// values into provided buffer.
// Returns number of bytes consumed from w.
// Returns 0 if the input wasn't parseable / an error occurred.
func readEscaped(w []byte, buf *buffer) int {
	if len(w) < 2 {
		return 0 // need more data
	}

	switch c := w[1]; {
	case c == 'u':
		if 2+utf8.UTFMax >= len(w) {
			return 0 // need more data
		}

		rr := getu4(w[2:6])
		if rr < 0 {
			return 0
		}

		r := 2 + utf8.UTFMax // number of bytes read so far.
		if utf16.IsSurrogate(rr) {
			if 2*r >= len(w) {
				return 0 // need more data
			}

			if w[r] != '\\' || w[r+1] != 'u' {
				return 0
			}

			rr1 := getu4(w[r+2:])
			dec := utf16.DecodeRune(rr, rr1)
			if dec == unicode.ReplacementChar {
				return 0
			}
			// A valid pair; consume.
			r *= 2
			buf.AppendRune(dec)
		} else {
			buf.AppendRune(rr)
		}

		return r
	default:
		c = unescapeTable[c]
		if c == 0 {
			return 0
		}
		buf.AppendByte(c)
		return 2
	}
}

func (s *Scanner) parseNumber(c byte) int {
	const (
		begin = iota
		leadingzero
		anydigit1
		decimal
		anydigit2
		exponent
		expsign
		anydigit3
	)

	pos := 0
	w := s.buf()
	var state uint8 = begin

	// Handle the case that the first character is a hyphen.
	if c == '-' {
		pos++
		w = w[1:]
	}

	for _, elem := range w {
		switch state {
		case begin:
			if elem >= '1' && elem <= '9' {
				state = anydigit1
			} else if elem == '0' {
				state = leadingzero
			} else {
				// error
				return 0
			}
		case anydigit1:
			if elem >= '0' && elem <= '9' {
				// Stay in this state.
				break
			}
			fallthrough
		case leadingzero:
			if elem == '.' {
				state = decimal
				break
			}
			if elem == 'e' || elem == 'E' {
				state = exponent
				break
			}
			return pos // Finished.
		case decimal:
			if elem >= '0' && elem <= '9' {
				state = anydigit2
			} else {
				return 0 // Error.
			}
		case anydigit2:
			if elem >= '0' && elem <= '9' {
				break
			}
			if elem == 'e' || elem == 'E' {
				state = exponent
				break
			}
			return pos // Finished.
		case exponent:
			if elem == '+' || elem == '-' {
				state = expsign
				break
			}
			fallthrough
		case expsign:
			if elem >= '0' && elem <= '9' {
				state = anydigit3
				break
			}
			return 0 // Error
		case anydigit3:
			if elem < '0' || elem > '9' {
				return pos
			}
		}
		pos++
	}

	// End of the item. However, not necessarily an error. Make
	// sure we are in a state that allows ending the number.
	switch state {
	case leadingzero, anydigit1, anydigit2, anydigit3:
		return pos
	default:
		// Error otherwise, the number isn't complete.
		return 0
	}
}

// hexTable lists quick conversion from byte to a valid
// hex byte; or 0 if invalid.
var hexTable = func() [256]rune {
	var t [256]rune
	for c := 0; c < 256; c++ {
		switch {
		case '0' <= c && c <= '9':
			t[c] = rune(c - '0')
		case 'a' <= c && c <= 'f':
			t[c] = rune(c - 'a' + 10)
		case 'A' <= c && c <= 'F':
			t[c] = rune(c - 'A' + 10)
		default:
			t[c] = utf8.RuneError
		}
	}
	return t
}()

// getu4 decodes \uXXXX from the beginning of s, returning the hex value,
// or it returns -1.
// s must be at least 4 bytes.
func getu4(s []byte) rune {
	r1, r2, r3, r4 := hexTable[s[0]], hexTable[s[1]], hexTable[s[2]], hexTable[s[3]]
	if r1 == utf8.RuneError || r2 == utf8.RuneError || r3 == utf8.RuneError || r4 == utf8.RuneError {
		return -1
	}
	return r1*(1<<12) + r2*(1<<8) + r3*(1<<4) + r4
}

// unescapeTable lists un-escaped characters for a set of valid
// escape sequences.
var unescapeTable = [256]byte{
	'"':  '"',  // \"
	'\\': '\\', // \\
	'/':  '/',  // \/
	'\'': '\'', // \'
	'b':  '\b', // \b
	'f':  '\f', // \f
	'n':  '\n', // \n
	'r':  '\r', // \r
	't':  '\t', // \t
}
