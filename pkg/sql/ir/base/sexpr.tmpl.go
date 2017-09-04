// Copyright 2017 The Cockroach Authors.
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

package base

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/pkg/errors"
)

type SexprFormatter interface {
	FormatSExpr(buf *bytes.Buffer)
}

func asString(x SexprFormatter) string {
	var buf bytes.Buffer
	x.FormatSExpr(&buf)
	return buf.String()
}

func FormatSExprBool(buf *bytes.Buffer, x bool)       { fmt.Fprintf(buf, "%v", x) }
func FormatSExprInt64(buf *bytes.Buffer, x int64)     { fmt.Fprintf(buf, "%v", x) }
func FormatSExprInt32(buf *bytes.Buffer, x int32)     { fmt.Fprintf(buf, "%v", x) }
func FormatSExprInt16(buf *bytes.Buffer, x int16)     { fmt.Fprintf(buf, "%v", x) }
func FormatSExprInt8(buf *bytes.Buffer, x int8)       { fmt.Fprintf(buf, "%v", x) }
func FormatSExprUint64(buf *bytes.Buffer, x uint64)   { fmt.Fprintf(buf, "%v", x) }
func FormatSExprUint32(buf *bytes.Buffer, x uint32)   { fmt.Fprintf(buf, "%v", x) }
func FormatSExprUint16(buf *bytes.Buffer, x uint16)   { fmt.Fprintf(buf, "%v", x) }
func FormatSExprUint8(buf *bytes.Buffer, x uint8)     { fmt.Fprintf(buf, "%v", x) }
func FormatSExprFloat32(buf *bytes.Buffer, x float32) { fmt.Fprintf(buf, "%v", x) }
func FormatSExprFloat64(buf *bytes.Buffer, x float64) { fmt.Fprintf(buf, "%v", x) }
func FormatSExprString(buf *bytes.Buffer, x string)   { fmt.Fprintf(buf, "%q", x) }

// @for enum

func (x ºEnum) FormatSExpr(buf *bytes.Buffer) {
	buf.WriteString(x.String())
}

// @done enum

// @for struct

func (x ºStruct) FormatSExpr(buf *bytes.Buffer) {
	buf.WriteString("(ºhstruct")

	// @for item

	buf.WriteString(" :ºhitem ")
	// @if isNotPrimitive
	x.ºItem().FormatSExpr(buf)
	// @fi isNotPrimitive
	// @if isPrimitive
	FormatSExprºType(buf, x.ºItem())
	// @fi isPrimitive

	// @done item

	buf.WriteByte(')')
}

func (x ºStruct) String() string { return asString(x) }

// @done struct

// @for sum

func (x ºSum) FormatSExpr(buf *bytes.Buffer) {
	switch x.Tag() {
	// @for item
	case ºSumºtype:
		x.MustBeºtype().FormatSExpr(buf)
		// @done item
	}
}

func (x ºSum) String() string { return asString(x) }

// @done sum

type Parser struct {
	// The string being parsed.
	s string
	// Allocator to use for new nodes.
	alloc *Allocator
	// Offset within the string.
	pos int
	// Current line number (0-indexed).
	lineno int
	// Current column number (0-indexed).
	col int
}

func MakeParser(s string, a *Allocator) Parser {
	return Parser{s: s, alloc: a}
}

var fmtErrDuplicateField = "%s: duplicate field definition for %s"
var fmtErrNoSuchField = "%s: no member named %s"
var fmtErrEOFWhileParsing = "%s: unexpected EOF"
var fmtErrNoSuchVariantName = "%s has no variant named %s"
var fmtErrNoSuchVariantTag = "%s has no variant with tag %d"
var fmtErrUnexpectedVariant = "expected %s variant, got %s"
var fmtErrUnexpectedSym = "expected %s, got %s"
var fmtErrValueMissing = "%s: value missing for:%s"

func (p *Parser) Errorf(format string, args ...interface{}) error {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%d:%d: syntax error: ", p.lineno+1, p.col+1)
	fmt.Fprintf(&buf, format, args...)

	// Find the end of the line containing the last token.
	i := strings.Index(p.s[p.pos:], "\n")
	if i == -1 {
		i = len(p.s)
	} else {
		i += p.pos
	}
	// Find the beginning of the line containing the last token. Note that
	// LastIndex returns -1 if "\n" could not be found.
	j := strings.LastIndex(p.s[:p.pos], "\n") + 1
	// Output everything up to and including the line containing the last token.
	fmt.Fprintf(&buf, "\n%s\n", p.s[:i])
	// Output a caret indicating where the last token starts.
	fmt.Fprintf(&buf, "%*s^\n", p.pos-j, "")

	return errors.New(buf.String())
}

func (p *Parser) Eof() bool {
	return p.pos >= len(p.s)
}

func (p *Parser) skipWhite() bool {
	for true {
		if p.Eof() {
			return true
		}
		switch c := p.s[p.pos]; c {
		case '\n':
			p.pos++
			p.lineno++
			p.col = 0
		case ' ', '\t':
			p.col++
			p.pos++
		case '\r', '\v', '\f':
			p.pos++
		default:
			return false
		}
	}
	return false // not reached
}

// @for enum

func (p *Parser) ParseºEnum() (res ºEnum, err error) {
	sym, err := p.expAtom()
	if err != nil {
		return res, err
	}
	switch sym {
	case "ºhenum":
		if err := p.expChar('('); err != nil {
			return res, err
		}
		v, err := p.expInteger(64)
		if err != nil {
			return res, err
		}
		if err := p.expChar(')'); err != nil {
			return res, err
		}
		switch v {
		// @for item
		case ºtag:
			return ºEnumºItem, nil
			// @done item
		default:
			return res, p.Errorf(fmtErrNoSuchVariantTag, "ºhenum", v)
		}

		// @for item
	case "ºhitem":
		return ºEnumºItem, nil
		// @done item
	default:
		return res, p.Errorf(fmtErrNoSuchVariantName, "ºhenum", sym)
	}
}

// @done enum

// @for sum

func (p *Parser) ParseºSum() (res ºSum, err error) {
	if err = p.expChar('('); err != nil {
		return res, err
	}
	sym, err := p.expAtom()
	if err != nil {
		return res, err
	}
	res, err = p.openºSum(sym)
	if err != nil {
		return res, err
	}
	if err = p.expChar(')'); err != nil {
		return res, err
	}
	return res, nil
}

func (p *Parser) openºSum(sym string) (ºSum, error) {
	switch sym {
	// @for item
	case "ºhtype":
		it, err := p.openºtype()
		if err != nil {
			return ºSum{}, err
		}
		return it.ºSum(), nil
		// @done item
	default:
		return ºSum{}, p.Errorf(fmtErrUnexpectedVariant, "ºhsum", sym)
	}
}

// @done sum

// @for struct

func (p *Parser) ParseºStruct() (res ºStruct, err error) {
	if err = p.expChar('('); err != nil {
		return res, err
	}
	sym, err := p.expAtom()
	if err != nil {
		return res, err
	}
	if sym != "ºhstruct" {
		return res, p.Errorf(fmtErrUnexpectedSym, "ºhstruct", sym)
	}
	res, err = p.openºStruct()
	if err != nil {
		return res, err
	}
	if err = p.expChar(')'); err != nil {
		return res, err
	}
	return res, nil
}

func (p *Parser) openºStruct() (res ºStruct, err error) {
	if p.skipWhite() {
		return res, p.Errorf(fmtErrEOFWhileParsing, "ºhstruct")
	}
	var s ºStructValue
	// @if hasNonPrimitiveFields
	var seen uint64
	// @fi hasNonPrimitiveFields
	for {
		lbl, err := p.expMaybeLabel()
		if err != nil {
			return res, err
		}
		if lbl == "" {
			break
		}
		switch lbl {
		// @for item
		case "ºhitem":
			v, err := p.ParseºType()
			if err != nil {
				return res, err
			}
			s = s.WithºItem(v)
			// @if isNotPrimitive
			if 0 != (seen & (uint64(1) << ºfieldNum)) {
				return res, p.Errorf(fmtErrDuplicateField, "ºhstruct", "ºhitem")
			}
			seen |= uint64(1) << ºfieldNum
			// @fi isNotPrimitive
			// @done item

		default:
			return res, p.Errorf(fmtErrNoSuchField, "ºhstruct", lbl)
		}
	}
	// @if hasNonPrimitiveFields
	if seen != ºnonPrimFieldsMask {
		var buf bytes.Buffer
		// @for item
		// @if isNotPrimitive
		if 0 == (seen & (uint64(1) << ºfieldNum)) {
			// @if isAtomic
			fmt.Fprintf(&buf, " :ºhitem ºhtype")
			// @fi isAtomic
			// @if isNotAtomic
			fmt.Fprintf(&buf, " :ºhitem (ºhtype))")
			// @fi isNotAtomic
		}
		// @fi isNotPrimitive
		// @done item
		return res, p.Errorf(fmtErrValueMissing, "ºhstruct", buf.String())
	}
	// @fi hasNonPrimitiveFields
	return s.R(p.alloc), nil
}

// @done struct

func (p *Parser) expChar(c byte) error {
	if p.skipWhite() {
		return p.Errorf("expected '%c', got EOF", c)
	}
	if p.s[p.pos] != c {
		return p.Errorf("expected '%c', got '%c'", c, p.s[p.pos])
	}
	p.pos++
	return nil
}

func (p *Parser) expMaybeLabel() (string, error) {
	if p.skipWhite() {
		return "", p.Errorf("expected ':' or ')', got EOF")
	}
	if p.s[p.pos] == ':' {
		p.pos++
		return p.expAtom()
	}
	return "", nil
}

func (p *Parser) scanInteger() (string, error) {
	start := p.pos
	for ; p.pos < len(p.s); p.pos++ {
		c := p.s[p.pos]
		if (c < '0' || c > '9') && c != '-' && c != '+' {
			break
		}
	}
	if p.pos == start {
		return "", p.Errorf("expected number, got '%c'", p.s[p.pos])
	}
	return p.s[start:p.pos], nil
}

func (p *Parser) scanRune() (rune, error) {
	s, err := p.scanString('\'')
	if err != nil {
		return 0, err
	}
	v, _, _, err := strconv.UnquoteChar(s, '\'')
	return v, err
}

func (p *Parser) expInteger(width int) (int64, error) {
	if p.skipWhite() {
		return 0, p.Errorf("expected number, got EOF")
	}
	if p.s[p.pos] == '\'' {
		v, err := p.scanRune()
		return int64(v), err
	}
	sv, err := p.scanInteger()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(sv, 0, width)
}

func (p *Parser) expUnsigned(width int) (uint64, error) {
	if p.skipWhite() {
		return 0, p.Errorf("expected number, got EOF")
	}
	if p.s[p.pos] == '\'' {
		v, err := p.scanRune()
		return uint64(v), err
	}
	sv, err := p.scanInteger()
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(sv, 0, width)
}

func (p *Parser) expAtom() (string, error) {
	if p.skipWhite() {
		return "", p.Errorf(fmtErrEOFWhileParsing, "atom")
	}

	i := p.pos
	for w, r := 0, rune(0); i < len(p.s); i += w {
		r, w = utf8.DecodeRuneInString(p.s[i:])
		if r == '(' || r == ')' || unicode.IsSpace(r) {
			break
		}
	}
	res := p.s[p.pos:i]
	p.pos = i
	return res, nil
}

func (p *Parser) scanString(quote byte) (string, error) {
	start := p.pos
	if err := p.expChar(quote); err != nil {
		return "", err
	}
	for ; !p.Eof() && p.s[p.pos] != quote; p.pos++ {
		if p.s[p.pos] == '\\' {
			p.pos++
		}
	}
	if err := p.expChar(quote); err != nil {
		return "", err
	}
	return p.s[start:p.pos], nil
}

func (p *Parser) ParseString() (string, error) {
	if p.skipWhite() {
		return "", p.Errorf(fmtErrEOFWhileParsing, "string")
	}
	s, err := p.scanString('"')
	if err != nil {
		return s, err
	}
	return strconv.Unquote(s)
}

func (p *Parser) ParseFloat64() (float64, error) {
	sym, err := p.expAtom()
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(sym, 64)
}

func (p *Parser) ParseFloat32() (float32, error) {
	sym, err := p.expAtom()
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseFloat(sym, 32)
	return float32(v), err
}

func (p *Parser) ParseBool() (bool, error) {
	sym, err := p.expAtom()
	if err != nil {
		return false, err
	}
	return strconv.ParseBool(sym)
}

func (p *Parser) ParseInt64() (int64, error)   { return p.expInteger(64) }
func (p *Parser) ParseUint64() (uint64, error) { return p.expUnsigned(64) }
func (p *Parser) ParseInt32() (int32, error)   { v, err := p.expInteger(32); return int32(v), err }
func (p *Parser) ParseUint32() (uint32, error) { v, err := p.expUnsigned(32); return uint32(v), err }
func (p *Parser) ParseInt16() (int16, error)   { v, err := p.expInteger(16); return int16(v), err }
func (p *Parser) ParseUint16() (uint16, error) { v, err := p.expUnsigned(16); return uint16(v), err }
func (p *Parser) ParseInt8() (int8, error)     { v, err := p.expInteger(8); return int8(v), err }
func (p *Parser) ParseUint8() (uint8, error)   { v, err := p.expUnsigned(8); return uint8(v), err }
