// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

var isQuoteChar = func(ch byte) bool {
	return ch == '"'
}

var isControlChar = func(ch byte) bool {
	return ch == '{' || ch == '}' || ch == ',' || ch == '"'
}

var isElementChar = func(r rune) bool {
	return r != '{' && r != '}' && r != ','
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
	ctx              ParseTimeContext
	dependsOnContext bool
	result           *DArray
	t                *types.T
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

func (p *parseState) parseQuotedString() (string, error) {
	return p.gobbleString(isQuoteChar)
}

func (p *parseState) parseUnquotedString() (string, error) {
	out, err := p.gobbleString(isControlChar)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
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
		if !isElementChar(r) {
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
// ParseTimeContext (either for the time or the local timezone).
func ParseDArrayFromString(
	ctx ParseTimeContext, s string, t *types.T,
) (_ *DArray, dependsOnContext bool, _ error) {
	ret, dependsOnContext, err := doParseDArrayFromString(ctx, s, t)
	if err != nil {
		return ret, false, makeParseError(s, types.MakeArray(t), err)
	}
	return ret, dependsOnContext, nil
}

// doParseDArrayFromString does most of the work of ParseDArrayFromString,
// except the error it returns isn't prettified as a parsing error.
//
// The dependsOnContext return value indicates if we had to consult the
// ParseTimeContext (either for the time or the local timezone).
func doParseDArrayFromString(
	ctx ParseTimeContext, s string, t *types.T,
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
		for parser.peek() == ',' {
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
