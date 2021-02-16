// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package wkt

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/twpayne/go-geom"
)

// LexError is an error that occurs during lexing.
type LexError struct {
	expectedTokType string
	pos             int
	str             string
}

func (e *LexError) Error() string {
	return fmt.Sprintf("lex error: invalid %s at pos %d\n%s\n%s^",
		e.expectedTokType, e.pos, e.str, strings.Repeat(" ", e.pos))
}

// ParseError is an error that occurs during parsing, which happens after lexing.
type ParseError struct {
	problem string
	pos     int
	str     string
	hint    string
}

func (e *ParseError) Error() string {
	// TODO(ayang): print only relevant line (with line and pos no) instead of entire input
	err := fmt.Sprintf("%s at pos %d\n%s\n%s^", e.problem, e.pos, e.str, strings.Repeat(" ", e.pos))
	if e.hint != "" {
		err += fmt.Sprintf("\nHINT: %s", e.hint)
	}
	return err
}

// Constant expected by parser when lexer reaches EOF.
const eof = 0

type wktLex struct {
	line      string
	pos       int
	lastPos   int
	ret       geom.T
	curLayout geom.Layout
	lastErr   error
}

// Lex lexes a token from the input.
func (l *wktLex) Lex(yylval *wktSymType) int {
	// Skip leading spaces.
	l.trimLeft()
	l.lastPos = l.pos

	// Lex a token.
	switch c := l.peek(); c {
	case eof:
		return eof
	case '(', ')', ',':
		return int(l.next())
	default:
		if unicode.IsLetter(c) {
			return l.keyword()
		} else if isNumRune(c) {
			return l.num(yylval)
		} else {
			l.next()
			l.setLexError("character")
			return eof
		}
	}
}

func getKeywordToken(tokStr string) int {
	switch tokStr {
	case "EMPTY":
		return EMPTY
	case "POINT":
		return POINT
	case "POINTZ":
		return POINTZ
	case "POINTM":
		return POINTM
	case "POINTZM":
		return POINTZM
	case "LINESTRING":
		return LINESTRING
	case "LINESTRINGM":
		return LINESTRINGM
	case "LINESTRINGZ":
		return LINESTRINGZ
	case "LINESTRINGZM":
		return LINESTRINGZM
	case "POLYGON":
		return POLYGON
	case "POLYGONM":
		return POLYGONM
	case "POLYGONZ":
		return POLYGONZ
	case "POLYGONZM":
		return POLYGONZM
	case "MULTIPOINT":
		return MULTIPOINT
	case "MULTIPOINTM":
		return MULTIPOINTM
	case "MULTIPOINTZ":
		return MULTIPOINTZ
	case "MULTIPOINTZM":
		return MULTIPOINTZM
	case "MULTILINESTRING":
		return MULTILINESTRING
	case "MULTILINESTRINGM":
		return MULTILINESTRINGM
	case "MULTILINESTRINGZ":
		return MULTILINESTRINGZ
	case "MULTILINESTRINGZM":
		return MULTILINESTRINGZM
	case "MULTIPOLYGON":
		return MULTIPOLYGON
	case "MULTIPOLYGONM":
		return MULTIPOLYGONM
	case "MULTIPOLYGONZ":
		return MULTIPOLYGONZ
	case "MULTIPOLYGONZM":
		return MULTIPOLYGONZM
	default:
		return eof
	}
}

// keyword lexes a string keyword.
func (l *wktLex) keyword() int {
	var b strings.Builder

	for {
		c := l.peek()
		if !unicode.IsLetter(c) {
			break
		}
		// Add the uppercase letter to the string builder.
		b.WriteRune(unicode.ToUpper(l.next()))
	}

	// Check for extra dimensions for geometry types.
	if b.String() != "EMPTY" {
		l.trimLeft()
		if unicode.ToUpper(l.peek()) == 'Z' {
			l.next()
			b.WriteRune('Z')
		}
		if unicode.ToUpper(l.peek()) == 'M' {
			l.next()
			b.WriteRune('M')
		}
	}

	ret := getKeywordToken(b.String())
	if ret == eof {
		l.setLexError("keyword")
	}

	return ret
}

func isNumRune(r rune) bool {
	switch r {
	case '-', '.':
		return true
	default:
		return unicode.IsDigit(r)
	}
}

// num lexes a number.
func (l *wktLex) num(yylval *wktSymType) int {
	var b strings.Builder

	for {
		c := l.peek()
		if !isNumRune(c) {
			break
		}
		b.WriteRune(l.next())
	}

	fl, err := strconv.ParseFloat(b.String(), 64)
	if err != nil {
		l.setLexError("number")
		return eof
	}
	yylval.coord = fl
	return NUM
}

func (l *wktLex) peek() rune {
	if l.pos == len(l.line) {
		return eof
	}
	return rune(l.line[l.pos])
}

func (l *wktLex) next() rune {
	c := l.peek()
	if c != eof {
		l.pos++
	}
	return c
}

func (l *wktLex) trimLeft() {
	for {
		c := l.peek()
		if c == eof || !unicode.IsSpace(c) {
			break
		}
		l.next()
	}
}

func getDefaultLayoutForStride(stride int) geom.Layout {
	switch stride {
	case 2:
		return geom.XY
	case 3:
		return geom.XYZ
	case 4:
		return geom.XYZM
	default:
		// This should never happen.
		panic("unsupported stride")
	}
}

func (l *wktLex) validateStrideAndSetLayoutIfNoLayout(stride int) bool {
	if !l.validateStride(stride) {
		return false
	}
	l.setLayoutIfNoLayout(getDefaultLayoutForStride(stride))
	return true
}

func (l *wktLex) validateStride(stride int) bool {
	if !l.isValidStrideForLayout(stride) {
		l.setIncorrectStrideError(stride, "")
		return false
	}
	return true
}

func (l *wktLex) isValidStrideForLayout(stride int) bool {
	switch l.curLayout {
	case geom.NoLayout:
		return true
	case geom.XY:
		return stride == 2
	case geom.XYM:
		return stride == 3
	case geom.XYZ:
		return stride == 3
	case geom.XYZM:
		return stride == 4
	default:
		// This should never happen.
		panic("unknown geom.Layout")
	}
}

func (l *wktLex) setLayout(layout geom.Layout) bool {
	if layout == l.curLayout {
		return true
	}
	if l.curLayout != geom.NoLayout {
		l.setIncorrectLayoutError(layout, "")
		return false
	}
	l.setLayoutIfNoLayout(layout)
	return true
}

func (l *wktLex) setLayoutEmptyInCollection() bool {
	if l.curLayout == geom.XY {
		return true
	}
	if l.curLayout == geom.NoLayout {
		l.curLayout = geom.XY
		return true
	}
	l.setIncorrectLayoutError(geom.XY, "EMPTY is XY layout in base geometry type collection")
	return false
}

func (l *wktLex) setLayoutIfNoLayout(layout geom.Layout) {
	switch l.curLayout {
	case geom.NoLayout:
		l.curLayout = layout
	case geom.XY, geom.XYM, geom.XYZ, geom.XYZM:
		break
	default:
		// This should never happen.
		panic("unknown geom.Layout")
	}
}

func (l *wktLex) setLexError(expectedTokType string) {
	l.lastErr = &LexError{expectedTokType: expectedTokType, pos: l.lastPos, str: l.line}
}

func getLayoutName(layout geom.Layout) string {
	switch layout {
	case geom.XY:
		return "XY"
	case geom.XYM:
		return "XYM"
	case geom.XYZ:
		return "XYZ"
	case geom.XYZM:
		return "XYZM"
	default:
		// This should never happen.
		panic("unknown geom.Layout")
	}
}

func (l *wktLex) setIncorrectStrideError(incorrectStride int, hint string) {
	problem := fmt.Sprintf("mixed dimensionality, parsed layout is %s so expecting %d coords but got %d coords",
		getLayoutName(l.curLayout), l.curLayout.Stride(), incorrectStride)
	l.setParseError(problem, hint)
}

func (l *wktLex) setIncorrectLayoutError(incorrectLayout geom.Layout, hint string) {
	problem := fmt.Sprintf("mixed dimensionality, parsed layout is %s but encountered layout of %s",
		getLayoutName(l.curLayout), getLayoutName(incorrectLayout))
	l.setParseError(problem, hint)
}

func (l *wktLex) setParseError(problem string, hint string) {
	// Lex errors take precedence.
	if l.lastErr != nil {
		return
	}
	errProblem := "syntax error: " + problem
	l.lastErr = &ParseError{
		problem: errProblem,
		pos:     l.lastPos,
		str:     l.line,
		hint:    hint,
	}
}

func (l *wktLex) Error(s string) {
	// NB: Lex errors are set in the Lex function.
	if l.lastErr == nil {
		l.lastErr = &ParseError{problem: s, pos: l.lastPos, str: l.line}
	}
}
