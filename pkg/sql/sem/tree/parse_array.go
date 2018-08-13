// Copyright 2016 The Cockroach Authors.
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

package tree

import (
	"bytes"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

var enclosingError = pgerror.NewErrorf(pgerror.CodeInvalidTextRepresentationError, "array must be enclosed in { and }")
var extraTextError = pgerror.NewErrorf(pgerror.CodeInvalidTextRepresentationError, "extra text after closing right brace")
var nestedArraysNotSupportedError = pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError, "nested arrays not supported")
var malformedError = pgerror.NewErrorf(pgerror.CodeInvalidTextRepresentationError, "malformed array")

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
	s       string
	evalCtx *EvalContext
	result  *DArray
	t       coltypes.T
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

	d, err := PerformCast(p.evalCtx, NewDString(next), p.t)
	if err != nil {
		return err
	}
	return p.result.Append(d)
}

// StringToColType returns a column type given a string representation of the
// type. Used by dump.
func StringToColType(s string) (coltypes.T, error) {
	switch s {
	case "BOOL":
		return coltypes.Bool, nil
	case "INT":
		return coltypes.Int, nil
	case "FLOAT":
		return coltypes.Float, nil
	case "DECIMAL":
		return coltypes.Decimal, nil
	case "TIMESTAMP":
		return coltypes.Timestamp, nil
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return coltypes.TimestampWithTZ, nil
	case "INTERVAL":
		return coltypes.Interval, nil
	case "UUID":
		return coltypes.UUID, nil
	case "INET":
		return coltypes.INet, nil
	case "DATE":
		return coltypes.Date, nil
	case "TIME":
		return coltypes.Time, nil
	case "STRING":
		return coltypes.String, nil
	case "NAME":
		return coltypes.Name, nil
	case "BYTES":
		return coltypes.Bytes, nil
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected column type %s", s)
	}
}

// ParseDArrayFromString parses the string-form of constructing arrays, handling
// cases such as `'{1,2,3}'::INT[]`.
func ParseDArrayFromString(evalCtx *EvalContext, s string, t coltypes.T) (*DArray, error) {
	parser := parseState{
		s:       s,
		evalCtx: evalCtx,
		result:  NewDArray(coltypes.CastTargetToDatumType(t)),
		t:       t,
	}

	parser.eatWhitespace()
	if parser.peek() != '{' {
		return nil, enclosingError
	}
	parser.advance()
	parser.eatWhitespace()
	if parser.peek() != '}' {
		if err := parser.parseElement(); err != nil {
			return nil, err
		}
		parser.eatWhitespace()
		for parser.peek() == ',' {
			parser.advance()
			parser.eatWhitespace()
			if err := parser.parseElement(); err != nil {
				return nil, err
			}
		}
	}
	parser.eatWhitespace()
	if parser.eof() {
		return nil, enclosingError
	}
	if parser.peek() != '}' {
		return nil, malformedError
	}
	parser.advance()
	parser.eatWhitespace()
	if !parser.eof() {
		return nil, extraTextError
	}

	return parser.result, nil
}
