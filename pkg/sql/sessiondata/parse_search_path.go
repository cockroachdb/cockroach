// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessiondata

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
)

// gobbleString advances the parser for the remainder of the current string
// until it sees a non-escaped termination character, returning the resulting
// string, not including the termination character.
func (p *parseState) gobbleString(isQuoted bool) (out string, ok bool) {
	result := &strings.Builder{}
	start := 0
	i := 0
	isTerminatingChar := func(ch byte) bool { return false }
	if !isQuoted {
		isTerminatingChar = func(ch byte) bool {
			return ch == ','
		}
	}

	for i < len(p.s) && !isTerminatingChar(p.s[i]) {
		if p.s[i] == '"' {
			if !isQuoted {
				// It's always invalid to see a quote if we are not in a quoted string.
				return "", false
			}
			result.WriteString(p.s[start:i])
			if i+1 < len(p.s) && p.s[i+1] == '"' {
				// If the string is quoted and the following character is also a double
				// quote, then the two characters are treated as an escape sequence for
				// one double quote.
				result.WriteByte(p.s[i+1])
				i += 2
				start = i
				continue
			}
			// Otherwise, this quote is the end of the string.
			start = i
			break
		} else {
			i++
		}
	}
	if isQuoted && i >= len(p.s) {
		return "", false
	} else if i > len(p.s) {
		return "", false
	}
	result.WriteString(p.s[start:i])
	p.s = p.s[i:]
	return result.String(), true
}

type parseState struct {
	s      string
	result []string
}

func (p *parseState) advance() {
	_, l := utf8.DecodeRuneInString(p.s)
	p.s = p.s[l:]
}

func (p *parseState) eatWhitespace() {
	for unicode.IsSpace(p.peek()) {
		p.advance()
	}
}

func (p *parseState) peek() rune {
	r, _ := utf8.DecodeRuneInString(p.s)
	return r
}

func (p *parseState) eof() bool {
	return len(p.s) == 0
}

func (p *parseState) parseQuotedString() (string, bool) {
	return p.gobbleString(true)
}

func (p *parseState) parseUnquotedString() (string, bool) {
	out, ok := p.gobbleString(false)
	if !ok {
		return "", false
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return "", false
	}
	return lexbase.NormalizeName(out), true
}

func (p *parseState) parseElement() bool {
	var next string
	var ok bool
	r := p.peek()
	switch r {
	case '"':

		p.advance()
		next, ok = p.parseQuotedString()
		if !ok {
			return false
		}
		p.advance()
	default:
		if r == ',' {
			return false
		}
		next, ok = p.parseUnquotedString()
		if !ok {
			return false
		}
	}

	p.result = append(p.result, next)
	return true
}

// doParseSearchPathFromString parses a search path string into a list of
// schema names. Each element in the resulting list is guaranteed to be a
// valid bare identifier. If the input cannot be parsed, the function returns
// false.
func doParseSearchPathFromString(s string) ([]string, bool) {
	parser := parseState{
		s:      s,
		result: make([]string, 0, strings.Count(s, ",")),
	}

	// Empty strings are valid search paths.
	if parser.eof() {
		return parser.result, true
	}

	parser.eatWhitespace()
	if parser.eof() {
		return nil, false
	}
	if ok := parser.parseElement(); !ok {
		return nil, false
	}
	parser.eatWhitespace()
	for string(parser.peek()) == "," {
		parser.advance()
		parser.eatWhitespace()
		if ok := parser.parseElement(); !ok {
			return nil, false
		}
	}
	parser.eatWhitespace()
	if !parser.eof() {
		return nil, false
	}

	return parser.result, true
}
