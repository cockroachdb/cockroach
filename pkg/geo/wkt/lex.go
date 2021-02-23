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

// Constant expected by parser when lexer reaches EOF.
const eof = 0

// SyntaxError is an error that occurs during parsing of a WKT string.
type SyntaxError struct {
	wkt       string
	problem   string
	lineNum   int
	lineStart int
	linePos   int
	hint      string
}

// Error generates a detailed syntax error message with line and pos numbers as well as a snippet of
// the erroneous input.
func (e *SyntaxError) Error() string {
	// These constants define the maximum number of characters of the line to show on each side of the cursor.
	const (
		leftPadding  = 30
		rightPadding = 30
	)

	// Print the problem along with line and pos number.
	err := fmt.Sprintf("syntax error: %s at line %d, pos %d\n", e.problem, e.lineNum, e.linePos)

	// Find the position of the end of the line.
	lineEnd := strings.IndexRune(e.wkt[e.lineStart:], '\n')
	if lineEnd == -1 {
		lineEnd = len(e.wkt)
	} else {
		lineEnd += e.lineStart
	}

	// Prepend the line with the line number.
	strLinePrefix := fmt.Sprintf("LINE %d: ", e.lineNum)
	strLineSuffix := "\n"

	// Trim the start and end of the line as needed.
	snipPos := e.linePos
	snipStart := e.lineStart
	leftMin := e.lineStart + e.linePos - leftPadding
	if snipStart < leftMin {
		snipPos -= leftMin - snipStart
		snipStart = leftMin
		strLinePrefix += "..."
	}
	snipEnd := lineEnd
	rightMax := e.lineStart + e.linePos + rightPadding
	if snipEnd > rightMax {
		snipEnd = rightMax
		strLineSuffix = "..." + strLineSuffix
	}

	// Print a cursor pointing to the token where the problem occurred.
	err += strLinePrefix + e.wkt[snipStart:snipEnd] + strLineSuffix
	err += fmt.Sprintf("%s^", strings.Repeat(" ", len(strLinePrefix)+snipPos))

	// Print a hint, if applicable.
	if e.hint != "" {
		err += fmt.Sprintf("\nHINT: %s", e.hint)
	}

	return err
}

// We define a base type geometry as a geometry type keyword without a type suffix.
// For example, POINT is a base type and POINTZ is not.
//
// The layout of the geometry is determined by the first geometry type keyword if it is a M, Z, or ZM variant.
// If it is a base type geometry, the layout is determined by the number of coordinates in the first point.
// If it is a geometrycollection, the type is the type of the first geometry in the collection.
//
// Edge cases involving geometrycollections:
// 1. GEOMETRYCOLLECTION (no type suffix) is allowed to be of type M. Normally a geometry without a type suffix
//    is only allowed to be XY, XYZ, or XYZM.
// 2. A base type empty geometry (e.g. POINT EMPTY) in a GEOMETRYCOLLECTIONM, GEOMETRYCOLLECTIONZ, GEOMETRYCOLLECTIONZM
//    is permitted and takes on the type of the collection. Normally, such a geometry is XY.
// 3. As a consequence of 1. and 2., special care must be given to parsing base geometry types inside a XYM
//    geometrycollection since a base geometry type is permitted inside a GEOMETRYCOLECTIONM only if it is empty.
//    For example, GEOMETRYCOLLECTION M (POINT EMPTY) should parse while GEOMETRYCOLLECTION M (POINT(0 0 0)) shouldn't.

// layoutStackObj is a stack object used in the layout parsing stack.
type layoutStackObj struct {
	// layout is the currently parsed geometry type.
	layout geom.Layout
	// inBaseTypeCollection is a bool where true means we are at the top-level or in a base type GEOMETRYCOLLECTION.
	inBaseTypeCollection bool
	// nextPointMustBeEmpty is a bool where true means the next scanned point must be EMPTY. It is used to handle
	// the edge case where a base type geometry is allowed in a GEOMETRYCOLLECTIONM but only if it is EMPTY.
	nextPointMustBeEmpty bool
}

// layoutStack is a stack used for parsing the geometry type. An initial frame is pushed for the top level context.
// After that, a frame is pushed for each (nested) geometrycollection is encountered and it is popped when we
// finish scanning that geometrycollection. The initial frame should never be popped off.
type layoutStack struct {
	data []layoutStackObj
}

// makeLayoutStack returns a newly created layoutStack. An initial frame is pushed for the top level context.
func makeLayoutStack() layoutStack {
	return layoutStack{
		data: []layoutStackObj{{layout: geom.NoLayout, inBaseTypeCollection: true}},
	}
}

// push constructs a layoutStackObj for a layout and pushes it onto the layout stack.
func (s *layoutStack) push(layout geom.Layout) {
	// inBaseTypeCollection inherits from outer context.
	stackObj := layoutStackObj{
		layout:               layout,
		inBaseTypeCollection: s.topInBaseTypeCollection(),
	}

	switch layout {
	case geom.NoLayout:
		stackObj.layout = s.topLayout()
	case geom.XYM, geom.XYZ, geom.XYZM:
		stackObj.inBaseTypeCollection = false
	default:
		// This should never happen.
		panic(fmt.Sprintf("unknown geom.Layout %d", layout))
	}

	s.data = append(s.data, stackObj)
}

// pop pops a layoutStackObj from the layout stack and returns its layout.
func (s *layoutStack) pop() geom.Layout {
	s.checkNotEmpty()
	if s.atTopLevel() {
		panic("top level stack frame should never be popped")
	}
	curTopLayout := s.topLayout()
	s.data = s.data[:len(s.data)-1]
	return curTopLayout
}

// top returns a pointer to the layoutStackObj currently at the top of the stack.
func (s *layoutStack) top() *layoutStackObj {
	s.checkNotEmpty()
	return &s.data[len(s.data)-1]
}

// topLayout returns the layout field of the topmost layoutStackObj.
func (s layoutStack) topLayout() geom.Layout {
	return s.top().layout
}

// topLayout returns the inBaseTypeCollection field of the topmost layoutStackObj.
func (s layoutStack) topInBaseTypeCollection() bool {
	return s.top().inBaseTypeCollection
}

// topLayout returns the nextPointMustBeEmpty field of the topmost layoutStackObj.
func (s layoutStack) topNextPointMustBeEmpty() bool {
	return s.top().nextPointMustBeEmpty
}

// setTopLayout sets the layout field of the topmost layoutStackObj.
func (s layoutStack) setTopLayout(layout geom.Layout) {
	switch layout {
	case geom.XY, geom.XYM, geom.XYZ, geom.XYZM:
		s.top().layout = layout
	default:
		// This should never happen.
		panic(fmt.Sprintf("unknown geom.Layout %d", layout))
	}
}

// setTopNextPointMustBeEmpty sets the nextPointMustBeEmpty field of the topmost layoutStackObj.
func (s layoutStack) setTopNextPointMustBeEmpty(nextPointMustBeEmpty bool) {
	if s.topLayout() != geom.XYM {
		panic("setTopNextPointMustBeEmpty called for non-XYM geometry collection")
	}
	s.top().nextPointMustBeEmpty = nextPointMustBeEmpty
}

// checkNotEmpty checks that the stack is not empty and panics if it is.
func (s layoutStack) checkNotEmpty() {
	// Layout stack should never be empty.
	if len(s.data) == 0 {
		panic("layout stack is empty")
	}
}

// checkNoGeometryCollectionFramesLeft checks that no frames corresponding to geometrycollections are left on the stack.
func (s layoutStack) checkNoGeometryCollectionFramesLeft() {
	// The initial stack frame should be the only one remaining at the end.
	if !s.atTopLevel() {
		panic("layout stack still has geometrycollection frames")
	}
}

// atTopLevel returns whether or not the stack has only the first frame which represents that we are currently
// not inside a geometrycollection.
func (s layoutStack) atTopLevel() bool {
	return len(s.data) == 1
}

// lexPos is a struct for keeping track of both the actual and human-readable lexed position in the string.
type lexPos struct {
	wktPos    int
	lineNum   int
	lineStart int
	linePos   int
}

// advanceOne advances a lexPos by one position on the same line.
func (lp *lexPos) advanceOne() {
	lp.wktPos++
	lp.linePos++
}

// advanceLine advances a lexPos by a newline.
func (lp *lexPos) advanceLine() {
	lp.wktPos++
	lp.lineNum++
	lp.lineStart = lp.wktPos
	lp.linePos = 0
}

// wktLex is the lexer for lexing WKT tokens.
type wktLex struct {
	wkt      string
	curPos   lexPos
	lastPos  lexPos
	ret      geom.T
	lytStack layoutStack
	lastErr  error
}

// newWKTLex returns a pointer to a newly created wktLex.
func newWKTLex(wkt string) *wktLex {
	return &wktLex{wkt: wkt, lytStack: makeLayoutStack()}
}

// Lex lexes a token from the input.
func (l *wktLex) Lex(yylval *wktSymType) int {
	// Skip leading spaces.
	l.trimLeft()
	l.lastPos = l.curPos

	// Lex a token.
	switch c := l.peek(); c {
	case eof:
		return eof
	case '(', ')', ',':
		return int(l.next())
	default:
		if unicode.IsLetter(c) {
			return l.keyword()
		} else if isValidFirstNumRune(c) {
			return l.num(yylval)
		} else {
			l.next()
			l.setLexError("character")
			return eof
		}
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

	ret := keywordToken(b.String())
	if ret == eof {
		l.setLexError("keyword")
	}

	return ret
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

// peek returns the next rune to be read.
func (l *wktLex) peek() rune {
	if l.curPos.wktPos == len(l.wkt) {
		return eof
	}
	return rune(l.wkt[l.curPos.wktPos])
}

// next returns the next rune to be read and advances the curPos counter.
func (l *wktLex) next() rune {
	c := l.peek()
	if c != eof {
		if c == '\n' {
			l.curPos.advanceLine()
		} else {
			l.curPos.advanceOne()
		}
	}
	return c
}

// trimLeft increments the curPos counter until the next rune to be read is no longer a whitespace character.
func (l *wktLex) trimLeft() {
	for {
		c := l.peek()
		if c == eof || !unicode.IsSpace(c) {
			break
		}
		l.next()
	}
}

// validateStrideAndSetDefaultLayoutIfNoLayout validates whether a stride is consistent with the currently parsed
// layout and sets the layout with the default layout for that stride if no layout has been determined yet.
func (l *wktLex) validateStrideAndSetDefaultLayoutIfNoLayout(stride int) bool {
	if !isValidStrideForLayout(stride, l.curLayout()) {
		l.setIncorrectStrideError(stride, "")
		return false
	}
	l.setLayoutIfNoLayout(defaultLayoutForStride(stride))
	return true
}

// validateNonEmptyGeometryAllowed validates whether a non-empty geometry is allowed given the currently
// parsed layout. It is used to handle the edge case where a GEOMETRYCOLLECTIONM may have base type
// geometries only if they are empty.
func (l *wktLex) validateNonEmptyGeometryAllowed() bool {
	if l.nextScannedPointMustBeEmpty() {
		if l.curLayout() != geom.XYM {
			panic("nextPointMustBeEmpty is true but layout is not XYM")
		}
		l.setIncorrectUsageOfBaseTypeInsteadOfMVariantInGeometryCollectionError()
		return false
	}
	return true
}

// validateAndSetLayoutIfNoLayout validates whether a newly parsed layout is compatible with the currently parsed
// layout and sets the layout if the current layout is unknown.
func (l *wktLex) validateAndSetLayoutIfNoLayout(layout geom.Layout) bool {
	if !isCompatibleLayout(l.curLayout(), layout) {
		l.setIncorrectLayoutError(layout, "")
		return false
	}
	l.setLayoutIfNoLayout(layout)
	return true
}

// validateBaseGeometryTypeAllowed validates whether a base geometry type is permitted based on the parsed layout.
func (l *wktLex) validateBaseGeometryTypeAllowed() bool {
	// Base type geometry are permitted in GEOMETRYCOLLECTIONM, GEOMETRYCOLLECTIONZ, GEOMETRYCOLLECTIONZM.
	// The stride of the coordinates/whether EMPTY is allowed will be validated later.
	if !l.currentlyInBaseTypeCollection() {
		// A base type is only permitted in a GEOMETRYCOLLECTIONM if it is EMPTY. We require an EMPTY instead of
		// coordinates follow this base type keyword.
		if l.curLayout() == geom.XYM {
			l.lytStack.setTopNextPointMustBeEmpty(true)
		}
		return true
	}

	// At the top level, a base geometry type is permitted. In a base type GEOMETRYCOLLECTION, a base type geometry
	// is only not permitted if the parsed layout is XYM.
	switch l.curLayout() {
	case geom.XYM:
		if l.lytStack.atTopLevel() {
			panic("base geometry check for XYM layout should not happen at top level")
		}
		l.setIncorrectUsageOfBaseTypeInsteadOfMVariantInGeometryCollectionError()
		return false
	default:
		return true
	}
}

// validateBaseTypeEmptyAllowed validates whether a base type EMPTY is permitted based on the parsed layout.
func (l *wktLex) validateBaseTypeEmptyAllowed() bool {
	// EMPTY is always permitted in a non-base type collection.
	if !l.currentlyInBaseTypeCollection() {
		// A base type EMPTY geometry is the only permitted base type geometry in a GEOMETRYCOLLECTIONM
		// and we have now finished reading one.
		if l.curLayout() == geom.XYM {
			l.lytStack.setTopNextPointMustBeEmpty(false)
		}
		return true
	}

	// In a base type collection (or at the top level), EMPTY can only be XY.
	switch l.curLayout() {
	case geom.NoLayout:
		l.setLayoutIfNoLayout(geom.XY)
		fallthrough
	case geom.XY:
		return true
	default:
		l.setIncorrectLayoutError(geom.XY, "EMPTY is XY layout in base geometry type")
		return false
	}
}

// validateAndPushLayoutStackFrame validates that a given layout is valid and pushes a frame to the layout stack.
func (l *wktLex) validateAndPushLayoutStackFrame(layout geom.Layout) bool {
	// Check that the new layout is compatible with the previous one.
	// Note a base type GEOMETRYCOLLECTION is permitted inside every layout.
	if layout != geom.NoLayout && !isCompatibleLayout(l.curLayout(), layout) {
		l.setIncorrectLayoutError(layout, "")
		return false
	}
	l.lytStack.push(layout)
	return true
}

// validateAndPopLayoutStackFrame pops a frame from the layout stack and validates that the type is valid.
func (l *wktLex) validateAndPopLayoutStackFrame() bool {
	poppedLayout := l.lytStack.pop()
	// Update the outer context with the type we parsed in the inner context.
	if !isCompatibleLayout(l.curLayout(), poppedLayout) {
		// This should never happen. Any layout incompatibility should error at the point it's discovered.
		panic("uncaught layout incompatibility")
	}
	l.setLayoutIfNoLayout(poppedLayout)
	return true
}

// validateLayoutStackAtEnd returns whether the layout stack is in the expected state at the end of parsing.
func (l *wktLex) validateLayoutStackAtEnd() bool {
	l.lytStack.checkNoGeometryCollectionFramesLeft()
	return true
}

// setLayoutIfNoLayout sets the parsed layout if no layout has been determined yet.
func (l *wktLex) setLayoutIfNoLayout(layout geom.Layout) {
	if l.curLayout() == geom.NoLayout {
		l.lytStack.setTopLayout(layout)
	}
}

// setIncorrectUsageOfBaseTypeInsteadOfMVariantInGeometryCollectionError sets the error when a
// base type geometry is used in a base type GEOMETRYCOLLECTION when the parsed layout is XYM.
func (l *wktLex) setIncorrectUsageOfBaseTypeInsteadOfMVariantInGeometryCollectionError() {
	l.setIncorrectLayoutError(
		geom.NoLayout,
		"the M variant is required for non-empty XYM geometries in GEOMETRYCOLLECTIONs",
	)
}

// setIncorrectStrideError sets the error when a newly parsed stride doesn't match the currently parsed layout.
func (l *wktLex) setIncorrectStrideError(incorrectStride int, hint string) {
	problem := fmt.Sprintf("mixed dimensionality, parsed layout is %s so expecting %d coords but got %d coords",
		layoutName(l.curLayout()), l.curLayout().Stride(), incorrectStride)
	l.setParseError(problem, hint)
}

// setIncorrectLayoutError sets the error when a newly parsed layout doesn't match the currently parsed layout.
func (l *wktLex) setIncorrectLayoutError(incorrectLayout geom.Layout, hint string) {
	problem := fmt.Sprintf("mixed dimensionality, parsed layout is %s but encountered layout of %s",
		layoutName(l.curLayout()), layoutName(incorrectLayout))
	l.setParseError(problem, hint)
}

// curLayout returns the currently parsed layout.
func (l *wktLex) curLayout() geom.Layout {
	return l.lytStack.topLayout()
}

// currentlyInBaseTypeCollection returns whether we are currently scanning inside a base type GEOMETRYCOLLECTION.
func (l *wktLex) currentlyInBaseTypeCollection() bool {
	return l.lytStack.topInBaseTypeCollection()
}

// nextScannedPointMustBeEmpty returns whether the next scanned point must be empty.
func (l *wktLex) nextScannedPointMustBeEmpty() bool {
	return l.lytStack.topNextPointMustBeEmpty()
}

// setLexError is called by Lex when a lexing (tokenizing) error is detected.
func (l *wktLex) setLexError(expectedTokType string) {
	l.Error(fmt.Sprintf("invalid %s", expectedTokType))
}

// setParseError is called when a context-sensitive error is detected during parsing.
// The generated wktParse function can only catch context-free errors.
func (l *wktLex) setParseError(problem string, hint string) {
	l.setSyntaxError(problem, hint)
}

// Error is called by wktParse if an error is encountered during parsing (takes place after lexing).
func (l *wktLex) Error(s string) {
	l.setSyntaxError(strings.TrimPrefix(s, "syntax error: "), "")
}

// setSyntaxError is called when a syntax error occurs.
func (l *wktLex) setSyntaxError(problem string, hint string) {
	l.setError(&SyntaxError{
		wkt:       l.wkt,
		problem:   problem,
		lineNum:   l.lastPos.lineNum + 1,
		lineStart: l.lastPos.lineStart,
		linePos:   l.lastPos.linePos,
		hint:      hint,
	})
}

// setError sets the lastErr field of the wktLex object with the given error.
func (l *wktLex) setError(err error) {
	// Lex errors take precedence.
	if l.lastErr == nil {
		l.lastErr = err
	}
}

// isValidFirstNumRune returns whether a rune is valid as the first rune in a number (coordinate).
func isValidFirstNumRune(r rune) bool {
	switch r {
	// PostGIS doesn't seem to accept numbers with a leading '+'.
	case '+':
		return false
	// Scientific notation number must have a number before the e.
	// Checking this case explicitly helps disambiguate between a number and a keyword.
	case 'e', 'E':
		return false
	default:
		return isNumRune(r)
	}
}

// isNumRune returns whether a rune could potentially be a part of a number (coordinate).
func isNumRune(r rune) bool {
	switch r {
	case '-', '.', 'e', 'E', '+':
		return true
	default:
		return unicode.IsDigit(r)
	}
}

// keywordToken returns the yacc token for a WKT keyword.
func keywordToken(tokStr string) int {
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

// isValidStrideForLayout returns whether a stride is consistent with a parsed layout.
// It is used for ensuring points have the right number of coordinates for the parsed layout.
func isValidStrideForLayout(stride int, layout geom.Layout) bool {
	switch layout {
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
		panic(fmt.Sprintf("unknown geom.Layout %d", layout))
	}
}

// defaultLayoutForStride returns the default layout for a base type geometry with the given stride.
func defaultLayoutForStride(stride int) geom.Layout {
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

// isCompatibleLayout returns whether a second layout is compatible with the first layout.
// It is used for ensuring the layout of each nested geometry is consistent with the previously parsed layout.
func isCompatibleLayout(outerLayout geom.Layout, innerLayout geom.Layout) bool {
	assertValidLayout(outerLayout)
	assertValidLayout(innerLayout)
	if outerLayout != innerLayout && outerLayout != geom.NoLayout {
		return false
	}
	return true
}

// layoutName returns the string representation of each layout.
func layoutName(layout geom.Layout) string {
	switch layout {
	// geom.NoLayout is used when a base type geometry is read.
	case geom.NoLayout:
		return "not XYM"
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

// assertValidLayout asserts that a given layout is valid and panics if it is not.
func assertValidLayout(layout geom.Layout) {
	switch layout {
	case geom.NoLayout, geom.XY, geom.XYM, geom.XYZ, geom.XYZM:
		return
	default:
		panic(fmt.Sprintf("unknown geom.Layout %d", layout))
	}
}
