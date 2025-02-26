// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scanner

import (
	"fmt"
	"go/constant"
	"go/token"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
)

const eof = -1
const errUnterminated = "unterminated string"
const errInvalidUTF8 = "invalid UTF-8 byte sequence"
const errInvalidHexNumeric = "invalid hexadecimal numeric literal"
const singleQuote = '\''
const identQuote = '"'

// NewNumValFn allows us to use tree.NewNumVal without a dependency on tree.
var NewNumValFn = func(constant.Value, string, bool) interface{} {
	return struct{}{}
}

// NewPlaceholderFn allows us to use tree.NewPlaceholder without a dependency on
// tree.
var NewPlaceholderFn = func(string) (interface{}, error) {
	return struct{}{}, nil
}

// ScanSymType is the interface for accessing the fields of a yacc symType.
type ScanSymType interface {
	ID() int32
	SetID(int32)
	Pos() int32
	SetPos(int32)
	Str() string
	SetStr(string)
	UnionVal() interface{}
	SetUnionVal(interface{})
}

// Scanner lexes statements.
type Scanner struct {
	in            string
	pos           int
	bytesPrealloc []byte

	// Comments is the list of parsed comments from the statement.
	Comments []string

	// lastAttemptedID indicates the ID of the last attempted
	// token. Used to recognizd which token an error was encountered
	// on.
	lastAttemptedID int32
	// quoted indicates if the last identifier scanned was
	// quoted. Used to distinguish between quoted and non-quoted in
	// Inspect.
	quoted bool
	// retainComments indicates that comments should be collected in the
	// Comments field. If it is false, they are discarded.
	retainComments bool
}

// SQLScanner is a scanner with a SQL specific scan function
type SQLScanner struct {
	Scanner
}

// In returns the input string.
func (s *Scanner) In() string {
	return s.in
}

// Pos returns the current position being lexed.
func (s *Scanner) Pos() int {
	return s.pos
}

// Init initializes a new Scanner that will process str.
func (s *Scanner) Init(str string) {
	*s = Scanner{
		in:  str,
		pos: 0,
		// Preallocate some buffer space for identifiers etc.
		bytesPrealloc: make([]byte, len(str)),
	}
}

// RetainComments instructs the scanner to collect SQL comments in the Comments
// field.
func (s *Scanner) RetainComments() {
	s.retainComments = true
}

// Cleanup is used to avoid holding on to memory unnecessarily (for the cases
// where we reuse a Scanner).
func (s *Scanner) Cleanup() {
	s.bytesPrealloc = nil
	s.Comments = nil
	s.retainComments = false
}

func (s *Scanner) allocBytes(length int) []byte {
	if cap(s.bytesPrealloc) >= length {
		res := s.bytesPrealloc[:length:length]
		s.bytesPrealloc = s.bytesPrealloc[length:cap(s.bytesPrealloc)]
		return res
	}
	return make([]byte, length)
}

// buffer returns an empty []byte buffer that can be appended to. Any unused
// portion can be returned later using returnBuffer.
func (s *Scanner) buffer() []byte {
	buf := s.bytesPrealloc[:0]
	s.bytesPrealloc = nil
	return buf
}

// returnBuffer returns the unused portion of buf to the Scanner, to be used for
// future allocBytes() or buffer() calls. The caller must not use buf again.
func (s *Scanner) returnBuffer(buf []byte) {
	if len(buf) < cap(buf) {
		s.bytesPrealloc = buf[len(buf):]
	}
}

// finishString casts the given buffer to a string and returns the unused
// portion of the buffer. The caller must not use buf again.
func (s *Scanner) finishString(buf []byte) string {
	str := *(*string)(unsafe.Pointer(&buf))
	s.returnBuffer(buf)
	return str
}

func (s *Scanner) scanSetup(lval ScanSymType) (int, bool) {
	lval.SetID(0)
	lval.SetPos(int32(s.pos))
	lval.SetStr("EOF")
	s.quoted = false
	s.lastAttemptedID = 0

	if _, ok := s.skipWhitespace(lval, true); !ok {
		return 0, true
	}

	ch := s.next()
	if ch == eof {
		lval.SetPos(int32(s.pos))
		return ch, false
	}

	lval.SetID(int32(ch))
	lval.SetPos(int32(s.pos - 1))
	lval.SetStr(s.in[lval.Pos():s.pos])
	s.lastAttemptedID = int32(ch)
	return ch, false
}

// Scan scans the next token and populates its information into lval.
func (s *SQLScanner) Scan(lval ScanSymType) {
	ch, skipWhiteSpace := s.scanSetup(lval)

	if skipWhiteSpace {
		return
	}

	switch ch {
	case '$':
		// placeholder? $[0-9]+
		if lexbase.IsDigit(s.peek()) {
			s.scanPlaceholder(lval)
			return
		} else if s.scanDollarQuotedString(lval) {
			lval.SetID(lexbase.SCONST)
			return
		}
		return

	case identQuote:
		// "[^"]"
		s.lastAttemptedID = int32(lexbase.IDENT)
		s.quoted = true
		if s.scanString(lval, identQuote, false /* allowEscapes */, true /* requireUTF8 */) {
			lval.SetID(lexbase.IDENT)
		}
		return

	case singleQuote:
		// '[^']'
		s.lastAttemptedID = int32(lexbase.SCONST)
		if s.scanString(lval, ch, false /* allowEscapes */, true /* requireUTF8 */) {
			lval.SetID(lexbase.SCONST)
		}
		return

	case 'b':
		// Bytes?
		if s.peek() == singleQuote {
			// b'[^']'
			s.lastAttemptedID = int32(lexbase.BCONST)
			s.pos++
			if s.scanString(lval, singleQuote, true /* allowEscapes */, false /* requireUTF8 */) {
				lval.SetID(lexbase.BCONST)
			}
			return
		}
		s.scanIdent(lval)
		return

	case 'r', 'R':
		s.scanIdent(lval)
		return

	case 'e', 'E':
		// Escaped string?
		if s.peek() == singleQuote {
			// [eE]'[^']'
			s.lastAttemptedID = int32(lexbase.SCONST)
			s.pos++
			if s.scanString(lval, singleQuote, true /* allowEscapes */, true /* requireUTF8 */) {
				lval.SetID(lexbase.SCONST)
			}
			return
		}
		s.scanIdent(lval)
		return

	case 'B':
		// Bit array literal?
		if s.peek() == singleQuote {
			// B'[01]*'
			s.pos++
			s.scanBitString(lval, singleQuote)
			return
		}
		s.scanIdent(lval)
		return

	case 'x', 'X':
		// Hex literal?
		if s.peek() == singleQuote {
			// [xX]'[a-f0-9]'
			s.pos++
			s.scanHexString(lval, singleQuote)
			return
		}
		s.scanIdent(lval)
		return

	case '.':
		switch t := s.peek(); {
		case t == '.': // ..
			s.pos++
			lval.SetID(lexbase.DOT_DOT)
			return
		case lexbase.IsDigit(t):
			s.lastAttemptedID = int32(lexbase.FCONST)
			s.scanNumber(lval, ch)
			return
		}
		return

	case '!':
		switch s.peek() {
		case '=': // !=
			s.pos++
			lval.SetID(lexbase.NOT_EQUALS)
			return
		case '~': // !~
			s.pos++
			switch s.peek() {
			case '*': // !~*
				s.pos++
				lval.SetID(lexbase.NOT_REGIMATCH)
				return
			}
			lval.SetID(lexbase.NOT_REGMATCH)
			return
		}
		return

	case '?':
		switch s.peek() {
		case '?': // ??
			s.pos++
			lval.SetID(lexbase.HELPTOKEN)
			return
		case '|': // ?|
			s.pos++
			lval.SetID(lexbase.JSON_SOME_EXISTS)
			return
		case '&': // ?&
			s.pos++
			lval.SetID(lexbase.JSON_ALL_EXISTS)
			return
		}
		return

	case '<':
		switch s.peek() {
		case '<': // <<
			s.pos++
			switch s.peek() {
			case '=': // <<=
				s.pos++
				lval.SetID(lexbase.INET_CONTAINED_BY_OR_EQUALS)
				return
			}
			lval.SetID(lexbase.LSHIFT)
			return
		case '>': // <>
			s.pos++
			lval.SetID(lexbase.NOT_EQUALS)
			return
		case '=': // <=
			s.pos++
			switch s.peek() {
			case '>': // <=>
				s.pos++
				lval.SetID(lexbase.COS_DISTANCE)
				return
			}
			lval.SetID(lexbase.LESS_EQUALS)
			return
		case '@': // <@
			s.pos++
			lval.SetID(lexbase.CONTAINED_BY)
			return
		case '-': // <-
			switch s.peekN(1) {
			case '>': // <->
				s.pos += 2
				lval.SetID(lexbase.DISTANCE)
				return
			}
		case '#': // <#
			switch s.peekN(1) {
			case '>': // <#>
				s.pos += 2
				lval.SetID(lexbase.NEG_INNER_PRODUCT)
				return
			}
		}
		return

	case '>':
		switch s.peek() {
		case '>': // >>
			s.pos++
			switch s.peek() {
			case '=': // >>=
				s.pos++
				lval.SetID(lexbase.INET_CONTAINS_OR_EQUALS)
				return
			}
			lval.SetID(lexbase.RSHIFT)
			return
		case '=': // >=
			s.pos++
			lval.SetID(lexbase.GREATER_EQUALS)
			return
		}
		return

	case ':':
		switch s.peek() {
		case ':': // ::
			if s.peekN(1) == ':' {
				// :::
				s.pos += 2
				lval.SetID(lexbase.TYPEANNOTATE)
				return
			}
			s.pos++
			lval.SetID(lexbase.TYPECAST)
			return
		}
		return

	case '|':
		switch s.peek() {
		case '|': // ||
			s.pos++
			switch s.peek() {
			case '/': // ||/
				s.pos++
				lval.SetID(lexbase.CBRT)
				return
			}
			lval.SetID(lexbase.CONCAT)
			return
		case '/': // |/
			s.pos++
			lval.SetID(lexbase.SQRT)
			return
		}
		return

	case '/':
		switch s.peek() {
		case '/': // //
			s.pos++
			lval.SetID(lexbase.FLOORDIV)
			return
		}
		return

	case '~':
		switch s.peek() {
		case '*': // ~*
			s.pos++
			lval.SetID(lexbase.REGIMATCH)
			return
		}
		return

	case '@':
		switch s.peek() {
		case '>': // @>
			s.pos++
			lval.SetID(lexbase.CONTAINS)
			return
		case '@': // @@
			s.pos++
			lval.SetID(lexbase.AT_AT)
			return
		}
		return

	case '&':
		switch s.peek() {
		case '&': // &&
			s.pos++
			lval.SetID(lexbase.AND_AND)
			return
		}
		return

	case '-':
		switch s.peek() {
		case '>': // ->
			if s.peekN(1) == '>' {
				// ->>
				s.pos += 2
				lval.SetID(lexbase.FETCHTEXT)
				return
			}
			s.pos++
			lval.SetID(lexbase.FETCHVAL)
			return
		}
		return

	case '#':
		switch s.peek() {
		case '>': // #>
			if s.peekN(1) == '>' {
				// #>>
				s.pos += 2
				lval.SetID(lexbase.FETCHTEXT_PATH)
				return
			}
			s.pos++
			lval.SetID(lexbase.FETCHVAL_PATH)
			return
		case '-': // #-
			s.pos++
			lval.SetID(lexbase.REMOVE_PATH)
			return
		}
		return

	default:
		if lexbase.IsDigit(ch) {
			s.lastAttemptedID = int32(lexbase.ICONST)
			s.scanNumber(lval, ch)
			return
		}
		if lexbase.IsIdentStart(ch) {
			s.scanIdent(lval)
			return
		}
	}

	// Everything else is a single character token which we already initialized
	// lval for above.
}

func (s *Scanner) peek() int {
	if s.pos >= len(s.in) {
		return eof
	}
	return int(s.in[s.pos])
}

func (s *Scanner) peekN(n int) int {
	pos := s.pos + n
	if pos >= len(s.in) {
		return eof
	}
	return int(s.in[pos])
}

func (s *Scanner) next() int {
	ch := s.peek()
	if ch != eof {
		s.pos++
	}
	return ch
}

func (s *Scanner) skipWhitespace(lval ScanSymType, allowComments bool) (newline, ok bool) {
	newline = false
	for {
		startPos := s.pos
		ch := s.peek()
		if ch == '\n' {
			s.pos++
			newline = true
			continue
		}
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\f' {
			s.pos++
			continue
		}
		if allowComments {
			if present, cok := s.ScanComment(lval); !cok {
				return false, false
			} else if present {
				if s.retainComments {
					// Mark down the comments that we found.
					s.Comments = append(s.Comments, s.in[startPos:s.pos])
				}
				continue
			}
		}
		break
	}
	return newline, true
}

// ScanComment scans the input as a comment.
func (s *Scanner) ScanComment(lval ScanSymType) (present, ok bool) {
	start := s.pos
	ch := s.peek()

	if ch == '/' {
		s.pos++
		if s.peek() != '*' {
			s.pos--
			return false, true
		}
		s.pos++
		depth := 1
		for {
			switch s.next() {
			case '*':
				if s.peek() == '/' {
					s.pos++
					depth--
					if depth == 0 {
						return true, true
					}
					continue
				}

			case '/':
				if s.peek() == '*' {
					s.pos++
					depth++
					continue
				}

			case eof:
				lval.SetID(lexbase.ERROR)
				lval.SetPos(int32(start))
				lval.SetStr("unterminated comment")
				return false, false
			}
		}
	}

	if ch == '-' {
		s.pos++
		if s.peek() != '-' {
			s.pos--
			return false, true
		}
		for {
			switch s.next() {
			case eof, '\n':
				return true, true
			}
		}
	}

	return false, true
}

// normalizeIdent takes in a function that determines if a character is a legal
// identifier character, and a boolean toLower that indicates whether to set the
// identifier to lowercase when normalizing.
func (s *Scanner) normalizeIdent(lval ScanSymType, isIdentMiddle func(int) bool, toLower bool) {
	s.lastAttemptedID = int32(lexbase.IDENT)
	s.pos--
	start := s.pos
	isASCII := true
	isLower := true

	// Consume the Scanner character by character, stopping after the last legal
	// identifier character. By the end of this function, we need to
	// lowercase and unicode normalize this identifier, which is expensive if
	// there are actual unicode characters in it. If not, it's quite cheap - and
	// if it's lowercase already, there's no work to do. Therefore, we keep track
	// of whether the string is only ASCII or only ASCII lowercase for later.
	for {
		ch := s.peek()
		if ch >= utf8.RuneSelf {
			isASCII = false
		} else if ch >= 'A' && ch <= 'Z' {
			isLower = false
		}

		if !isIdentMiddle(ch) {
			break
		}

		s.pos++
	}

	if toLower && !isLower && isASCII {
		// We know that the identifier we've seen so far is ASCII, so we don't
		// to unicode normalize. Instead, just lowercase as normal.
		b := s.allocBytes(s.pos - start)
		_ = b[s.pos-start-1] // For bounds check elimination.
		for i, c := range s.in[start:s.pos] {
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			b[i] = byte(c)
		}
		lval.SetStr(*(*string)(unsafe.Pointer(&b)))
	} else if toLower && !isASCII {
		// The string has unicode in it. No choice but to normalize and lowercase.
		lval.SetStr(lexbase.NormalizeName(s.in[start:s.pos]))
	} else if !toLower && !isASCII {
		// The string has unicode in it. No choice but to normalize.
		lval.SetStr(lexbase.NormalizeString(s.in[start:s.pos]))
	} else {
		// Don't do anything.
		lval.SetStr(s.in[start:s.pos])
	}
}

func (s *Scanner) scanIdent(lval ScanSymType) {
	s.normalizeIdent(lval, lexbase.IsIdentMiddle, true /* toLower */)

	isExperimental := false
	kw := lval.Str()
	switch {
	case strings.HasPrefix(lval.Str(), "experimental_"):
		kw = lval.Str()[13:]
		isExperimental = true
	case strings.HasPrefix(lval.Str(), "testing_"):
		kw = lval.Str()[8:]
		isExperimental = true
	}
	lval.SetID(lexbase.GetKeywordID(kw))
	if lval.ID() != lexbase.IDENT {
		if isExperimental {
			if _, ok := lexbase.AllowedExperimental[kw]; !ok {
				// If the parsed token is not on the allowlisted set of keywords,
				// then it might have been intended to be parsed as something else.
				// In that case, re-tokenize the original string.
				lval.SetID(lexbase.GetKeywordID(lval.Str()))
			} else {
				// It is a allowlisted keyword, so remember the shortened
				// keyword for further processing.
				lval.SetStr(kw)
			}
		}
	} else {
		// If the word after experimental_ or testing_ is an identifier,
		// then we might have classified it incorrectly after removing the
		// experimental_/testing_ prefix.
		lval.SetID(lexbase.GetKeywordID(lval.Str()))
	}
}

func (s *Scanner) scanNumber(lval ScanSymType, ch int) {
	start := s.pos - 1
	isHex := false
	hasDecimal := ch == '.'
	hasExponent := false

	for {
		ch := s.peek()
		if (isHex && lexbase.IsHexDigit(ch)) || lexbase.IsDigit(ch) {
			s.pos++
			continue
		}
		if ch == 'x' || ch == 'X' {
			if isHex || s.in[start] != '0' || s.pos != start+1 {
				lval.SetID(lexbase.ERROR)
				lval.SetStr(errInvalidHexNumeric)
				return
			}
			s.pos++
			isHex = true
			continue
		}
		if isHex {
			break
		}
		if ch == '.' {
			if hasDecimal || hasExponent {
				break
			}
			s.pos++
			if s.peek() == '.' {
				// Found ".." while scanning a number: back up to the end of the
				// integer.
				s.pos--
				break
			}
			hasDecimal = true
			continue
		}
		if ch == 'e' || ch == 'E' {
			if hasExponent {
				break
			}
			hasExponent = true
			s.pos++
			ch = s.peek()
			if ch == '-' || ch == '+' {
				s.pos++
			}
			ch = s.peek()
			if !lexbase.IsDigit(ch) {
				lval.SetID(lexbase.ERROR)
				lval.SetStr("invalid floating point literal")
				return
			}
			continue
		}
		break
	}

	// Disallow identifier after numerical constants e.g. "124foo".
	if lexbase.IsIdentStart(s.peek()) {
		lval.SetID(lexbase.ERROR)
		lval.SetStr(fmt.Sprintf("trailing junk after numeric literal at or near %q", s.in[start:s.pos+1]))
		return
	}

	lval.SetStr(s.in[start:s.pos])
	if hasDecimal || hasExponent {
		lval.SetID(lexbase.FCONST)
		floatConst := constant.MakeFromLiteral(lval.Str(), token.FLOAT, 0)
		if floatConst.Kind() == constant.Unknown {
			lval.SetID(lexbase.ERROR)
			lval.SetStr(fmt.Sprintf("could not make constant float from literal %q", lval.Str()))
			return
		}
		lval.SetUnionVal(NewNumValFn(floatConst, lval.Str(), false /* negative */))
	} else {
		if isHex && s.pos == start+2 {
			lval.SetID(lexbase.ERROR)
			lval.SetStr(errInvalidHexNumeric)
			return
		}

		// Strip off leading zeros from non-hex (decimal) literals so that
		// constant.MakeFromLiteral doesn't inappropriately interpret the
		// string as an octal literal. Note: we can't use strings.TrimLeft
		// here, because it will truncate '0' to ''.
		if !isHex {
			for len(lval.Str()) > 1 && lval.Str()[0] == '0' {
				lval.SetStr(lval.Str()[1:])
			}
		}

		lval.SetID(lexbase.ICONST)
		intConst := constant.MakeFromLiteral(lval.Str(), token.INT, 0)
		if intConst.Kind() == constant.Unknown {
			lval.SetID(lexbase.ERROR)
			lval.SetStr(fmt.Sprintf("could not make constant int from literal %q", lval.Str()))
			return
		}
		lval.SetUnionVal(NewNumValFn(intConst, lval.Str(), false /* negative */))
	}
}

func (s *Scanner) scanPlaceholder(lval ScanSymType) {
	s.lastAttemptedID = int32(lexbase.PLACEHOLDER)
	start := s.pos
	for lexbase.IsDigit(s.peek()) {
		s.pos++
	}
	lval.SetStr(s.in[start:s.pos])

	placeholder, err := NewPlaceholderFn(lval.Str())
	if err != nil {
		lval.SetID(lexbase.ERROR)
		lval.SetStr(err.Error())
		return
	}
	lval.SetID(lexbase.PLACEHOLDER)
	lval.SetUnionVal(placeholder)
}

// scanHexString scans the content inside x'....'.
func (s *Scanner) scanHexString(lval ScanSymType, ch int) bool {
	s.lastAttemptedID = int32(lexbase.BCONST)
	buf := s.buffer()

	var curbyte byte
	bytep := 0
	const errInvalidBytesLiteral = "invalid hexadecimal bytes literal"
outer:
	for {
		b := s.next()
		switch b {
		case ch:
			newline, ok := s.skipWhitespace(lval, false)
			if !ok {
				return false
			}
			// SQL allows joining adjacent strings separated by whitespace
			// as long as that whitespace contains at least one
			// newline. Kind of strange to require the newline, but that
			// is the standard.
			if s.peek() == ch && newline {
				s.pos++
				continue
			}
			break outer

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			curbyte = (curbyte << 4) | byte(b-'0')
		case 'a', 'b', 'c', 'd', 'e', 'f':
			curbyte = (curbyte << 4) | byte(b-'a'+10)
		case 'A', 'B', 'C', 'D', 'E', 'F':
			curbyte = (curbyte << 4) | byte(b-'A'+10)
		default:
			lval.SetID(lexbase.ERROR)
			lval.SetStr(errInvalidBytesLiteral)
			return false
		}
		bytep++

		if bytep > 1 {
			buf = append(buf, curbyte)
			bytep = 0
			curbyte = 0
		}
	}

	if bytep != 0 {
		lval.SetID(lexbase.ERROR)
		lval.SetStr(errInvalidBytesLiteral)
		return false
	}

	lval.SetID(lexbase.BCONST)
	lval.SetStr(s.finishString(buf))
	return true
}

// scanBitString scans the content inside B'....'.
func (s *Scanner) scanBitString(lval ScanSymType, ch int) bool {
	s.lastAttemptedID = int32(lexbase.BITCONST)
	buf := s.buffer()
outer:
	for {
		b := s.next()
		switch b {
		case ch:
			newline, ok := s.skipWhitespace(lval, false)
			if !ok {
				return false
			}
			// SQL allows joining adjacent strings separated by whitespace
			// as long as that whitespace contains at least one
			// newline. Kind of strange to require the newline, but that
			// is the standard.
			if s.peek() == ch && newline {
				s.pos++
				continue
			}
			break outer

		case '0', '1':
			buf = append(buf, byte(b))
		default:
			lval.SetID(lexbase.ERROR)
			lval.SetStr(fmt.Sprintf(`"%c" is not a valid binary digit`, rune(b)))
			return false
		}
	}

	lval.SetID(lexbase.BITCONST)
	lval.SetStr(s.finishString(buf))
	return true
}

// scanString scans the content inside '...'. This is used for simple
// string literals '...' but also e'....' and b'...'. For x'...', see
// scanHexString().
func (s *Scanner) scanString(lval ScanSymType, ch int, allowEscapes, requireUTF8 bool) bool {
	buf := s.buffer()
	var runeTmp [utf8.UTFMax]byte
	start := s.pos
outer:
	for {
		switch s.next() {
		case ch:
			buf = append(buf, s.in[start:s.pos-1]...)
			if s.peek() == ch {
				// Double quote is translated into a single quote that is part of the
				// string.
				start = s.pos
				s.pos++
				continue
			}

			newline, ok := s.skipWhitespace(lval, false)
			if !ok {
				return false
			}

			// SQL allows joining adjacent single-quoted strings separated by
			// whitespace as long as that whitespace contains at least one
			// newline. Kind of strange to require the newline, but that is the
			// standard.
			if ch == singleQuote && s.peek() == singleQuote && newline {
				s.pos++
				start = s.pos
				continue
			}
			break outer

		case '\\':
			t := s.peek()

			if allowEscapes {
				buf = append(buf, s.in[start:s.pos-1]...)
				if t == ch {
					start = s.pos
					s.pos++
					continue
				}

				switch t {
				case 'a', 'b', 'f', 'n', 'r', 't', 'v', 'x', 'X', 'u', 'U', '\\',
					'0', '1', '2', '3', '4', '5', '6', '7':
					var tmp string
					if t == 'X' && len(s.in[s.pos:]) >= 3 {
						// UnquoteChar doesn't handle 'X' so we create a temporary string
						// for it to parse.
						tmp = "\\x" + s.in[s.pos+1:s.pos+3]
					} else {
						tmp = s.in[s.pos-1:]
					}
					v, multibyte, tail, err := strconv.UnquoteChar(tmp, byte(ch))
					if err != nil {
						lval.SetID(lexbase.ERROR)
						lval.SetStr(err.Error())
						return false
					}
					if v < utf8.RuneSelf || !multibyte {
						buf = append(buf, byte(v))
					} else {
						n := utf8.EncodeRune(runeTmp[:], v)
						buf = append(buf, runeTmp[:n]...)
					}
					s.pos += len(tmp) - len(tail) - 1
					start = s.pos
					continue
				}

				// If we end up here, it's a redundant escape - simply drop the
				// backslash. For example, e'\"' is equivalent to e'"', and
				// e'\d\b' to e'd\b'. This is what Postgres does:
				// http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
				start = s.pos
			}

		case eof:
			lval.SetID(lexbase.ERROR)
			lval.SetStr(errUnterminated)
			return false
		}
	}

	if requireUTF8 && !utf8.Valid(buf) {
		lval.SetID(lexbase.ERROR)
		lval.SetStr(errInvalidUTF8)
		return false
	}

	if ch == identQuote {
		lval.SetStr(lexbase.NormalizeString(s.finishString(buf)))
	} else {
		lval.SetStr(s.finishString(buf))
	}
	return true
}

// scanDollarQuotedString scans for so called dollar-quoted strings, which start/end with either $$ or $tag$, where
// tag is some arbitrary string.  e.g. $$a string$$ or $escaped$a string$escaped$.
func (s *Scanner) scanDollarQuotedString(lval ScanSymType) bool {
	s.lastAttemptedID = int32(lexbase.SCONST)
	buf := s.buffer()
	start := s.pos

	foundStartTag := false
	possibleEndTag := false
	startTagIndex := -1
	var startTag string

outer:
	for {
		ch := s.peek()
		switch ch {
		case '$':
			s.pos++
			if foundStartTag {
				if possibleEndTag {
					if len(startTag) == startTagIndex {
						// Found end tag.
						buf = append(buf, s.in[start+len(startTag)+1:s.pos-len(startTag)-2]...)
						break outer
					} else {
						// Was not the end tag but the current $ might be the start of the end tag we are looking for, so
						// just reset the startTagIndex.
						startTagIndex = 0
					}
				} else {
					possibleEndTag = true
					startTagIndex = 0
				}
			} else {
				startTag = s.in[start : s.pos-1]
				foundStartTag = true
			}

		case eof:
			if foundStartTag {
				// A start tag was found, therefore we expect an end tag before the eof, otherwise it is an error.
				lval.SetID(lexbase.ERROR)
				lval.SetStr(errUnterminated)
			} else {
				// This is not a dollar-quoted string, reset the pos back to the start.
				s.pos = start
			}
			return false

		default:
			// If we haven't found a start tag yet, check whether the current characters is a valid for a tag.
			if !foundStartTag && !lexbase.IsIdentStart(ch) && !lexbase.IsDigit(ch) {
				return false
			}
			s.pos++
			if possibleEndTag {
				// Check whether this could be the end tag.
				if startTagIndex >= len(startTag) || ch != int(startTag[startTagIndex]) {
					// This is not the end tag we are looking for.
					possibleEndTag = false
					startTagIndex = -1
				} else {
					startTagIndex++
				}
			}
		}
	}

	if !utf8.Valid(buf) {
		lval.SetID(lexbase.ERROR)
		lval.SetStr(errInvalidUTF8)
		return false
	}

	lval.SetStr(s.finishString(buf))
	return true
}

// HasMultipleStatements returns true if the sql string contains more than one
// statements. An error is returned if an invalid token was encountered.
func HasMultipleStatements(sql string) (multipleStmt bool, err error) {
	var s SQLScanner
	var lval fakeSym
	s.Init(sql)
	count := 0
	for {
		done, hasToks, err := s.scanOne(&lval)
		if err != nil {
			return false, err
		}
		if hasToks {
			count++
		}
		if done || count > 1 {
			break
		}
	}
	return count > 1, nil
}

// scanOne is a simplified version of (*Parser).scanOneStmt() for use
// by HasMultipleStatements().
func (s *SQLScanner) scanOne(lval *fakeSym) (done, hasToks bool, err error) {
	// Scan the first token.
	for {
		s.Scan(lval)
		if lval.id == 0 {
			return true, false, nil
		}
		if lval.id != ';' {
			break
		}
	}

	var preValID int32
	// This is used to track the degree of nested `BEGIN ATOMIC ... END` function
	// body context. When greater than zero, it means that we're scanning through
	// the function body of a `CREATE FUNCTION` statement. ';' character is only
	// a separator of sql statements within the body instead of a finishing line
	// of the `CREATE FUNCTION` statement.
	curFuncBodyCnt := 0
	for {
		if lval.id == lexbase.ERROR {
			return true, true, fmt.Errorf("scan error: %s", lval.s)
		}
		preValID = lval.id
		s.Scan(lval)
		if preValID == lexbase.BEGIN && lval.id == lexbase.ATOMIC {
			curFuncBodyCnt++
		}
		if curFuncBodyCnt > 0 && lval.id == lexbase.END {
			curFuncBodyCnt--
		}
		if lval.id == 0 || (curFuncBodyCnt == 0 && lval.id == ';') {
			return (lval.id == 0), true, nil
		}
	}
}

// LastLexicalToken returns the last lexical token. If the string has no lexical
// tokens, returns 0 and ok=false.
func LastLexicalToken(sql string) (lastTok int, ok bool) {
	var s SQLScanner
	var lval fakeSym
	s.Init(sql)
	for {
		last := lval.ID()
		s.Scan(&lval)
		if lval.ID() == 0 {
			return int(last), last != 0
		}
	}
}

// FirstLexicalToken returns the first lexical token.
// Returns 0 if there is no token.
func FirstLexicalToken(sql string) (tok int) {
	var s SQLScanner
	var lval fakeSym
	s.Init(sql)
	s.Scan(&lval)
	id := lval.ID()
	return int(id)
}

// fakeSym is a simplified symbol type for use by
// HasMultipleStatements.
type fakeSym struct {
	id  int32
	pos int32
	s   string
}

var _ ScanSymType = (*fakeSym)(nil)

func (s fakeSym) ID() int32                 { return s.id }
func (s *fakeSym) SetID(id int32)           { s.id = id }
func (s fakeSym) Pos() int32                { return s.pos }
func (s *fakeSym) SetPos(p int32)           { s.pos = p }
func (s fakeSym) Str() string               { return s.s }
func (s *fakeSym) SetStr(v string)          { s.s = v }
func (s fakeSym) UnionVal() interface{}     { return nil }
func (s fakeSym) SetUnionVal(v interface{}) {}

// InspectToken is the type of token that can be scanned by Inspect.
type InspectToken struct {
	ID      int32
	MaybeID int32
	Start   int32
	End     int32
	Str     string
	Quoted  bool
}

// Inspect analyses the string and returns the tokens found in it. If
// an incomplete token was encountered at the end, an InspectToken
// entry with ID -1 is appended.
//
// If a syntax error was encountered, it is returned as a token with
// type ERROR.
//
// See TestInspect and the examples in testdata/inspect for more details.
func Inspect(sql string) []InspectToken {
	var s SQLScanner
	var lval fakeSym
	var tokens []InspectToken
	s.Init(sql)
	for {
		s.Scan(&lval)
		tok := InspectToken{
			ID:      lval.id,
			MaybeID: s.lastAttemptedID,
			Str:     lval.s,
			Start:   lval.pos,
			End:     int32(s.pos),
			Quoted:  s.quoted,
		}

		// A special affordance for unterminated quoted identifiers: try
		// to find the normalized text of the identifier found so far.
		if lval.id == lexbase.ERROR && s.lastAttemptedID == lexbase.IDENT && s.quoted {
			maybeIdent := sql[tok.Start:tok.End] + "\""
			var si SQLScanner
			si.Init(maybeIdent)
			si.Scan(&lval)
			if lval.id == lexbase.IDENT {
				tok.Str = lval.s
			}
		}

		tokens = append(tokens, tok)
		if lval.id == 0 || lval.id == lexbase.ERROR {
			return tokens
		}
	}
}
