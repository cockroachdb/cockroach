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

// Stack object used for parsing the geometry type.
// layout      is the currently parsed geometry type.
// baseType    is a bool where true means the current context is the top-level or a base type GEOMETRYCOLLECTION.
// mustBeEmpty is a bool used to handle the edge case where a base type geometry is allowed in a GEOMETRYCOLLECTIONM
//             but only if it is EMPTY.
//
// The layout of the geometry is determined by the first geometry type keyword if it is a M, Z, or ZM variant.
// If it is a base type geometry, the layout is determined by the number of coordinates in the first point.
// If it is a geometrycollection, the type is the type of the first geometry in the collection.
// Edge cases involving geometrycollections:
// 1. GEOMETRYCOLLECTION (no type suffix) is allowed to be of type M. Normally a geometry without a type suffix
//    is only allowed to be XY, XYZ, or XYZM.
// 2. A base type empty geometry (e.g. POINT EMPTY) in a GEOMETRYCOLLECTIONM, GEOMETRYCOLLECTIONZ, GEOMETRYCOLLECTIONZM
//    is permitted and takes on the type of the collection. Normally, such a geometry is XY. Note that base type
//    non-empty geometries are still not permitted in a GEOMETRYCOLLECTIONM (e.g. POINT(0 0 0)).
type layoutStackObj struct {
	layout      geom.Layout
	baseType    bool
	mustBeEmpty bool
}

type wktLex struct {
	line        string
	pos         int
	lastPos     int
	ret         geom.T
	layoutStack []layoutStackObj
	lastErr     error
}

func makeWktLex(line string) *wktLex {
	newWktLex := &wktLex{line: line}
	newWktLex.layoutStack = append(newWktLex.layoutStack, layoutStackObj{
		layout:   geom.NoLayout,
		baseType: true,
	})
	return newWktLex
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
	case "GEOMETRYCOLLECTION":
		return GEOMETRYCOLLECTION
	case "GEOMETRYCOLLECTIONM":
		return GEOMETRYCOLLECTIONM
	case "GEOMETRYCOLLECTIONZ":
		return GEOMETRYCOLLECTIONZ
	case "GEOMETRYCOLLECTIONZM":
		return GEOMETRYCOLLECTIONZM
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
		panic(fmt.Sprintf("unsupported stride %d", stride))
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
	switch curLayout := l.getCurLayout(); curLayout {
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
		panic(fmt.Sprintf("unknown geom.Layout %d", curLayout))
	}
}

func (l *wktLex) isNonEmptyAllowedForLayout() bool {
	if l.getCurLayoutMustBeEmpty() {
		if l.getCurLayout() != geom.XYM {
			panic("mustBeEmpty is true but layout is not XYM")
		}
		l.setIncorrectLayoutError(geom.NoLayout, "XYM layout must use the M variant of the geometry type")
		return false
	}
	return true
}

func (l *wktLex) setLayout(layout geom.Layout) bool {
	switch {
	case layout == l.getCurLayout():
		return true
	case l.getCurLayout() != geom.NoLayout:
		l.setIncorrectLayoutError(layout, "")
		return false
	default:
		l.setLayoutIfNoLayout(layout)
		return true
	}
}

func (l *wktLex) setLayoutBaseType() bool {
	if !l.getCurLayoutBaseType() {
		// Handle edge case where a GEOMETRYCOLLECTIONM may have base type EMPTY geometries but not non-EMPTY geometries.
		if l.getCurLayout() == geom.XYM {
			l.setCurLayoutMustBeEmpty(true)
		}
		return true
	}

	if l.getCurLayout() == geom.XYM {
		l.setIncorrectLayoutError(geom.NoLayout, "XYM layout must use the M variant of the geometry type")
		return false
	}

	return true
}

func (l *wktLex) setLayoutBaseTypeEmpty() bool {
	if !l.getCurLayoutBaseType() {
		// Handle edge case where a GEOMETRYCOLLECTIONM may have base type EMPTY geometries but not non-EMPTY geometries.
		if l.getCurLayout() == geom.XYM {
			l.setCurLayoutMustBeEmpty(false)
		}
		return true
	}

	switch curLayout := l.getCurLayout(); curLayout {
	case geom.NoLayout:
		l.setLayout(geom.XY)
		fallthrough
	case geom.XY:
		return true
	default:
		l.setIncorrectLayoutError(geom.XY, "EMPTY is XY layout in base geometry type")
		return false
	}
}

func (l *wktLex) setLayoutIfNoLayout(layout geom.Layout) {
	switch curLayout := l.getCurLayout(); curLayout {
	case geom.NoLayout:
		l.setCurLayout(layout)
	case geom.XY, geom.XYM, geom.XYZ, geom.XYZM:
		break
	default:
		// This should never happen.
		panic(fmt.Sprintf("unknown geom.Layout %d", layout))
	}
}

func (l *wktLex) pushLayoutStack(layout geom.Layout) bool {
	// Check that the new layout is compatible with the previous ones.
	// baseType inherits from outer context.
	newStackObj := layoutStackObj{
		layout:   layout,
		baseType: l.getCurLayoutBaseType(),
	}

	switch layout {
	case geom.NoLayout:
		newStackObj.layout = l.getCurLayout()
	case geom.XYM, geom.XYZ, geom.XYZM:
		if layout != l.getCurLayout() && l.getCurLayout() != geom.NoLayout {
			l.setIncorrectLayoutError(layout, "")
			return false
		}
		newStackObj.baseType = false
	case geom.XY:
		// This should never happen. Base type should be pushing geom.NoLayout.
		fallthrough
	default:
		// This should never happen.
		panic(fmt.Sprintf("unknown geom.Layout %d", layout))
	}

	l.layoutStack = append(l.layoutStack, newStackObj)
	return true
}

func (l *wktLex) popLayoutStack() geom.Layout {
	top := l.getCurLayout()
	l.layoutStack = l.layoutStack[:len(l.layoutStack)-1]
	// Update the outer context with the type we parsed in the inner context.
	ok := l.setLayout(top)
	if !ok {
		// This should never happen. Any layout incompatibility should error at the point it's discovered.
		panic("uncaught layout incompatibility")
	}
	return top
}

func (l *wktLex) topLayoutStack() layoutStackObj {
	l.checkLayoutStackNotEmpty()
	return l.layoutStack[len(l.layoutStack)-1]
}

func (l *wktLex) getCurLayout() geom.Layout {
	return l.topLayoutStack().layout
}

func (l *wktLex) getCurLayoutBaseType() bool {
	return l.topLayoutStack().baseType
}

func (l *wktLex) getCurLayoutMustBeEmpty() bool {
	return l.topLayoutStack().mustBeEmpty
}

func (l *wktLex) setCurLayout(layout geom.Layout) {
	l.checkLayoutStackNotEmpty()
	l.layoutStack[len(l.layoutStack)-1].layout = layout
}

func (l *wktLex) setCurLayoutMustBeEmpty(mustBeEmpty bool) {
	l.checkLayoutStackNotEmpty()
	l.layoutStack[len(l.layoutStack)-1].mustBeEmpty = mustBeEmpty
}

func (l *wktLex) checkLayoutStackNotEmpty() {
	// Layout stack should never be empty.
	if len(l.layoutStack) == 0 {
		panic("layout stack is empty")
	}
}

func (l *wktLex) checkLayoutStackHasNoGeometryCollectionFramesLeft() {
	// The initial stack frame should be the only one remaining at the end.
	l.checkLayoutStackNotEmpty()
	if len(l.layoutStack) > 1 {
		panic("layout stack still has geometrycollection frames")
	}
}

func (l *wktLex) setLexError(expectedTokType string) {
	l.lastErr = &LexError{expectedTokType: expectedTokType, pos: l.lastPos, str: l.line}
}

func getLayoutName(layout geom.Layout) string {
	switch layout {
	case geom.NoLayout:
		return "XY, XYZ, or XYZM"
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
		panic(fmt.Sprintf("unknown geom.Layout %d", layout))
	}
}

func (l *wktLex) setIncorrectStrideError(incorrectStride int, hint string) {
	problem := fmt.Sprintf("mixed dimensionality, parsed layout is %s so expecting %d coords but got %d coords",
		getLayoutName(l.getCurLayout()), l.getCurLayout().Stride(), incorrectStride)
	l.setParseError(problem, hint)
}

func (l *wktLex) setIncorrectLayoutError(incorrectLayout geom.Layout, hint string) {
	problem := fmt.Sprintf("mixed dimensionality, parsed layout is %s but encountered layout of %s",
		getLayoutName(l.getCurLayout()), getLayoutName(incorrectLayout))
	l.setParseError(problem, hint)
}

func (l *wktLex) setParseError(problem string, hint string) {
	// Lex errors take precedence.
	if l.lastErr != nil {
		return
	}
	errProblem := "syntax error: " + problem
	l.setError(&ParseError{
		problem: errProblem,
		pos:     l.lastPos,
		str:     l.line,
		hint:    hint,
	})
}

func (l *wktLex) Error(s string) {
	// NB: Lex errors are set in the Lex function.
	l.setError(&ParseError{
		problem: s,
		pos:     l.lastPos,
		str:     l.line,
	})
}

func (l *wktLex) setError(err error) {
	if l.lastErr == nil {
		l.lastErr = err
	}
}
