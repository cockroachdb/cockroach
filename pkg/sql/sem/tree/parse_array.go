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
	"github.com/cockroachdb/errors"
)

var enclosingError = pgerror.Newf(pgcode.InvalidTextRepresentation, "array must be enclosed in { and } or ARRAY[ and ]")
var extraTextError = pgerror.Newf(pgcode.InvalidTextRepresentation, "extra text after closing right brace")
var nestedArraysNotSupportedError = unimplemented.NewWithIssueDetail(32552, "strcast", "nested arrays not supported")
var malformedError = pgerror.Newf(pgcode.InvalidTextRepresentation, "malformed array")

var isQuoteChar = func(ch byte) bool {
	return ch == '"'
}

var isControlChar = func(ch byte) bool {
	return ch == '[' || ch == ']' || ch == '{' || ch == '}' || ch == ',' || ch == '"'
}

var isElementChar = func(r rune) bool {
	return r != '{' && r != '}' && r != ','
}

// gobbleString advances the parser for the remainder of the current string
// until it sees a non-escaped termination character, as specified by
// isTerminatingChar, returning the resulting string, not including the
// termination character.
//
// The argument pgArrFormat is used to control how gobbleString deals with
// escape characters. When pgArrFormat is true, we encode the character
// after a '\' even if it would normally be an escape sequence.
func (p *parseState) gobbleString(
	pgArrFormat bool, isTerminatingChar func(ch byte) bool,
) (out string, err error) {
	var result bytes.Buffer
	start := 0
	i := 0
	for i < len(p.s) && !isTerminatingChar(p.s[i]) {
		if i < len(p.s) && p.s[i] == '\\' && pgArrFormat {
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
	s      string
	ctx    ParseTimeContext
	result *DArray
	t      *types.T
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

func (p *parseState) parseQuotedString(pgArrFormat bool) (string, error) {
	return p.gobbleString(pgArrFormat, isQuoteChar)
}

func (p *parseState) parseUnquotedString(pgArrFormat bool) (string, error) {
	out, err := p.gobbleString(pgArrFormat, isControlChar)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func (p *parseState) parseElement(pgArrFormat bool) error {
	var next string
	var err error
	r := p.peek()
	switch r {
	case '{', '[':
		return nestedArraysNotSupportedError
	case '"':
		p.advance()
		next, err = p.parseQuotedString(pgArrFormat)
		if err != nil {
			return err
		}
		p.advance()
	default:
		if !isElementChar(r) {
			return malformedError
		}
		next, err = p.parseUnquotedString(pgArrFormat)
		if err != nil {
			return err
		}
		if strings.EqualFold(next, "null") {
			return p.result.Append(DNull)
		}
	}

	d, err := parseStringAs(p.t, next, p.ctx)
	if d == nil && err == nil {
		return errors.AssertionFailedf("unknown type %s (%T)", p.t, p.t)
	}
	if err != nil {
		return err
	}
	return p.result.Append(d)
}

// ParseDArrayFromString parses the string-form of constructing arrays, handling
// cases such as `'{1,2,3}'::INT[]`. The input type t is the type of the
// parameter of the array to parse.
func ParseDArrayFromString(ctx ParseTimeContext, s string, t *types.T) (*DArray, error) {
	ret, err := doParseDArrayFromString(ctx, s, t)
	if err != nil {
		return ret, makeParseError(s, types.MakeArray(t), err)
	}
	return ret, nil
}

// doParseDArraryFromString does most of the work of ParseDArrayFromString,
// except the error it returns isn't prettified as a parsing error.
func doParseDArrayFromString(ctx ParseTimeContext, s string, t *types.T) (*DArray, error) {
	parser := parseState{
		s:      s,
		ctx:    ctx,
		result: NewDArray(t),
		t:      t,
	}

	parser.eatWhitespace()
	out, err := parser.gobbleString(false, isControlChar)
	if err != nil {
		return nil, enclosingError
	}

	var (
		opening, closing rune
		isPgArrayFormat  bool
	)
	if out == "ARRAY" {
		// Parse the ARRAY[...] format.
		// In this case, we don't want to use the escaping rules
		// for the postgres array from string parsing.
		opening, closing, isPgArrayFormat = '[', ']', false
	} else if out == "" {
		// Parse the {...} format.
		opening, closing, isPgArrayFormat = '{', '}', true
	}

	if parser.peek() != opening {
		return nil, enclosingError
	}
	parser.advance()
	parser.eatWhitespace()
	if parser.peek() != closing {
		if err := parser.parseElement(isPgArrayFormat); err != nil {
			return nil, err
		}
		parser.eatWhitespace()
		for parser.peek() == ',' {
			parser.advance()
			parser.eatWhitespace()
			if err := parser.parseElement(isPgArrayFormat); err != nil {
				return nil, err
			}
		}
	}
	parser.eatWhitespace()
	if parser.eof() {
		return nil, enclosingError
	}
	if parser.peek() != closing {
		return nil, malformedError
	}
	parser.advance()
	parser.eatWhitespace()
	if !parser.eof() {
		return nil, extraTextError
	}

	return parser.result, nil
}
