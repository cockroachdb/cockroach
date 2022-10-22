// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

import "io"

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

// NewScanner returns a new Scanner for the io.Reader r.
// A Scanner reads from the supplied io.Reader and produces via Next a stream
// of tokens, expressed as []byte slices.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		br: byteReader{
			r: r,
		},
	}
}

// Scanner implements a JSON scanner as defined in RFC 7159.
type Scanner struct {
	br  byteReader
	pos int
}

var whitespace = [256]bool{
	' ':  true,
	'\r': true,
	'\n': true,
	'\t': true,
}

// Next returns a []byte referencing the the next lexical token in the stream.
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
	s.br.release(s.pos)
	w := s.br.window(0)
loop:
	for pos, c := range w {
		// strip any leading whitespace.
		if whitespace[c] {
			continue
		}

		// simple case
		switch c {
		case ObjectStart, ObjectEnd, Colon, Comma, ArrayStart, ArrayEnd:
			s.pos = pos + 1
			return w[pos:s.pos]
		}

		s.br.release(pos)
		switch c {
		case True:
			s.pos = validateToken(&s.br, "true")
		case False:
			s.pos = validateToken(&s.br, "false")
		case Null:
			s.pos = validateToken(&s.br, "null")
		case String:
			if s.parseString() < 2 {
				return nil
			}
		default:
			// ensure the number is correct.
			s.pos = s.parseNumber(c)
		}
		return s.br.window(0)[:s.pos]
	}

	// it's all whitespace, ignore it
	s.br.release(len(w))

	// refill buffer
	if s.br.extend() == 0 {
		// eof
		return nil
	}
	w = s.br.window(0)
	goto loop
}

func validateToken(br *byteReader, expected string) int {
	for {
		w := br.window(0)
		n := len(expected)
		if len(w) >= n {
			if string(w[:n]) != expected {
				// doesn't match
				return 0
			}
			return n
		}
		// not enough data is left, we need to extend
		if br.extend() == 0 {
			// eof
			return 0
		}
	}
}

// parseString returns the length of the string token
// located at the start of the window or 0 if there is no closing
// " before the end of the byteReader.
func (s *Scanner) parseString() int {
	escaped := false
	w := s.br.window(1)
	pos := 0
	for {
		for _, c := range w {
			pos++
			switch {
			case escaped:
				escaped = false
			case c == '"':
				// finished
				s.pos = pos + 1
				return s.pos
			case c == '\\':
				escaped = true
			}
		}
		// need more data from the pipe
		if s.br.extend() == 0 {
			// EOF.
			return 0
		}
		w = s.br.window(pos + 1)
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
	w := s.br.window(0)
	// int vs uint8 costs 10% on canada.json
	var state uint8 = begin

	// handle the case that the first character is a hyphen
	if c == '-' {
		pos++
		w = s.br.window(1)
	}

	for {
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
					// stay in this state
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
				return pos // finished.
			case decimal:
				if elem >= '0' && elem <= '9' {
					state = anydigit2
				} else {
					// error
					return 0
				}
			case anydigit2:
				if elem >= '0' && elem <= '9' {
					break
				}
				if elem == 'e' || elem == 'E' {
					state = exponent
					break
				}
				return pos // finished.
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
				// error
				return 0
			case anydigit3:
				if elem < '0' || elem > '9' {
					return pos
				}
			}
			pos++
		}

		// need more data from the pipe
		if s.br.extend() == 0 {
			// end of the item. However, not necessarily an error. Make
			// sure we are in a state that allows ending the number.
			switch state {
			case leadingzero, anydigit1, anydigit2, anydigit3:
				return pos
			default:
				// error otherwise, the number isn't complete.
				return 0
			}
		}
		w = s.br.window(pos)
	}
}

// Error returns the first error encountered.
// When underlying reader is exhausted, Error returns io.EOF.
func (s *Scanner) Error() error { return s.br.err }
