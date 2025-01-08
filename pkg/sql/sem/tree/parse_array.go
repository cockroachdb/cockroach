// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"bytes"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

var enclosingError = pgerror.Newf(pgcode.InvalidTextRepresentation, "array must be enclosed in { and }")
var extraTextError = pgerror.Newf(pgcode.InvalidTextRepresentation, "extra text after closing right brace")
var nestedArraysNotSupportedError = unimplemented.NewWithIssueDetail(32552, "strcast", "nested arrays not supported")
var malformedError = pgerror.Newf(pgcode.InvalidTextRepresentation, "malformed array")

func isQuoteChar(ch byte) bool {
	return ch == '"'
}

// isSpaceInParseArray returns true if the rune is a space. To match Postgres,
// 0x85 and 0xA0 are not treated as whitespace.
func isSpaceInParseArray(r rune) bool {
	if r != 0x85 && r != 0xA0 && unicode.IsSpace(r) {
		return true
	}
	return false
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

// trimSpaceInParseArray returns a slice of the string s, with all leading
// and trailing white space removed, as defined by Postgres COPY. This is a
// reimplementation of strings.TrimSpace from the standard library.
func trimSpaceInParseArray(s string) string {
	// Fast path for ASCII: look for the first ASCII non-space byte
	start := 0
	for ; start < len(s); start++ {
		c := s[start]
		if c >= utf8.RuneSelf {
			// If we run into a non-ASCII byte, fall back to the
			// slower unicode-aware method on the remaining bytes
			return strings.TrimFunc(s[start:], isSpaceInParseArray)
		}
		if asciiSpace[c] == 0 {
			break
		}
	}

	// Now look for the first ASCII non-space byte from the end
	stop := len(s)
	for ; stop > start; stop-- {
		c := s[stop-1]
		if c >= utf8.RuneSelf {
			return strings.TrimFunc(s[start:stop], isSpaceInParseArray)
		}
		if asciiSpace[c] == 0 {
			break
		}
	}

	// At this point s[start:stop] starts and ends with an ASCII
	// non-space bytes, so we're done. Non-ASCII cases have already
	// been handled above.
	return s[start:stop]
}

func (p *parseState) isControlChar(ch byte) bool {
	delim := p.t.Delimiter()[0]
	return ch == '{' || ch == '}' || ch == delim || ch == '"'
}

func (p *parseState) isElementChar(r rune) bool {
	delim, _ := utf8.DecodeRuneInString(p.t.Delimiter())
	return r != '{' && r != '}' && r != delim
}

// gobbleString advances the parser for the remainder of the current string
// until it sees a non-escaped termination character, as specified by
// isTerminatingChar, returning the resulting string, not including the
// termination character.
func (p *parseState) gobbleString(isTerminatingChar func(ch byte) bool) (out string, err error) {
	var result bytes.Buffer
	start := 0
	i := 0
	for i < len(p.s) && !isTerminatingChar(p.s[i]) {
		// In these strings, we just encode directly the character following a
		// '\', even if it would normally be an escape sequence.
		if i < len(p.s) && p.s[i] == '\\' {
			result.WriteString(p.s[start:i])
			i++
			if i < len(p.s) {
				result.WriteByte(p.s[i])
				i++
			}
			start = i
		} else {
			i++
		}
	}
	if i >= len(p.s) {
		return "", malformedError
	}
	result.WriteString(p.s[start:i])
	p.s = p.s[i:]
	return result.String(), nil
}

type parseState struct {
	s                string
	ctx              ParseContext
	dependsOnContext bool
	result           *DArray
	t                *types.T
}

func (p *parseState) advance() {
	_, l := utf8.DecodeRuneInString(p.s)
	p.s = p.s[l:]
}

func (p *parseState) eatWhitespace() {
	for isSpaceInParseArray(p.peek()) {
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

func (p *parseState) parseQuotedString() (string, error) {
	return p.gobbleString(isQuoteChar)
}

func (p *parseState) parseUnquotedString() (string, error) {
	out, err := p.gobbleString(p.isControlChar)
	if err != nil {
		return "", err
	}
	return trimSpaceInParseArray(out), nil
}

func (p *parseState) parseElement() error {
	var next string
	var err error
	r := p.peek()
	switch r {
	case '{':
		return nestedArraysNotSupportedError
	case '"':
		p.advance()
		next, err = p.parseQuotedString()
		if err != nil {
			return err
		}
		p.advance()
	default:
		if !p.isElementChar(r) {
			return malformedError
		}
		next, err = p.parseUnquotedString()
		if err != nil {
			return err
		}
		if strings.EqualFold(next, "null") {
			return p.result.Append(DNull)
		}
	}

	d, dependsOnContext, err := ParseAndRequireString(p.t, next, p.ctx)
	if err != nil {
		return err
	}
	if dependsOnContext {
		p.dependsOnContext = true
	}
	return p.result.Append(d)
}

// ParseDArrayFromString parses the string-form of constructing arrays, handling
// cases such as `'{1,2,3}'::INT[]`. The input type t is the type of the
// parameter of the array to parse.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseContext (either for the time or the local timezone).
func ParseDArrayFromString(
	ctx ParseContext, s string, t *types.T,
) (_ *DArray, dependsOnContext bool, _ error) {
	ret, dependsOnContext, err := doParseDArrayFromString(ctx, s, t)
	if err != nil {
		return ret, false, MakeParseError(s, types.MakeArray(t), err)
	}
	return ret, dependsOnContext, nil
}

// doParseDArrayFromString does most of the work of ParseDArrayFromString,
// except the error it returns isn't prettified as a parsing error.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseContext (either for the time or the local timezone).
func doParseDArrayFromString(
	ctx ParseContext, s string, t *types.T,
) (_ *DArray, dependsOnContext bool, _ error) {
	parser := parseState{
		s:      s,
		ctx:    ctx,
		result: NewDArray(t),
		t:      t,
	}

	parser.eatWhitespace()
	if parser.peek() != '{' {
		return nil, false, enclosingError
	}
	parser.advance()
	parser.eatWhitespace()
	if parser.peek() != '}' {
		if err := parser.parseElement(); err != nil {
			return nil, false, err
		}
		parser.eatWhitespace()
		for string(parser.peek()) == t.Delimiter() {
			parser.advance()
			parser.eatWhitespace()
			if err := parser.parseElement(); err != nil {
				return nil, false, err
			}
		}
	}
	parser.eatWhitespace()
	if parser.eof() {
		return nil, false, enclosingError
	}
	if parser.peek() != '}' {
		return nil, false, malformedError
	}
	parser.advance()
	parser.eatWhitespace()
	if !parser.eof() {
		return nil, false, extraTextError
	}

	return parser.result, parser.dependsOnContext, nil
}
