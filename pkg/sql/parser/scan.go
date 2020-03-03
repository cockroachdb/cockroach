// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser

import (
	"fmt"
	"go/constant"
	"go/token"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const eof = -1
const errUnterminated = "unterminated string"
const errInvalidUTF8 = "invalid UTF-8 byte sequence"
const errInvalidHexNumeric = "invalid hexadecimal numeric literal"
const singleQuote = '\''
const identQuote = '"'

// scanner lexes SQL statements.
type scanner struct {
	in            string
	pos           int
	bytesPrealloc []byte
}

func makeScanner(str string) scanner {
	var s scanner
	s.init(str)
	return s
}

func (s *scanner) init(str string) {
	s.in = str
	s.pos = 0
	// Preallocate some buffer space for identifiers etc.
	s.bytesPrealloc = make([]byte, len(str))
}

// cleanup is used to avoid holding on to memory unnecessarily (for the cases
// where we reuse a scanner).
func (s *scanner) cleanup() {
	s.bytesPrealloc = nil
}

func (s *scanner) allocBytes(length int) []byte {
	if len(s.bytesPrealloc) >= length {
		res := s.bytesPrealloc[:length:length]
		s.bytesPrealloc = s.bytesPrealloc[length:]
		return res
	}
	return make([]byte, length)
}

// buffer returns an empty []byte buffer that can be appended to. Any unused
// portion can be returned later using returnBuffer.
func (s *scanner) buffer() []byte {
	buf := s.bytesPrealloc[:0]
	s.bytesPrealloc = nil
	return buf
}

// returnBuffer returns the unused portion of buf to the scanner, to be used for
// future allocBytes() or buffer() calls. The caller must not use buf again.
func (s *scanner) returnBuffer(buf []byte) {
	if len(buf) < cap(buf) {
		s.bytesPrealloc = buf[len(buf):]
	}
}

// finishString casts the given buffer to a string and returns the unused
// portion of the buffer. The caller must not use buf again.
func (s *scanner) finishString(buf []byte) string {
	str := *(*string)(unsafe.Pointer(&buf))
	s.returnBuffer(buf)
	return str
}

func (s *scanner) scan(lval *sqlSymType) {
	lval.id = 0
	lval.pos = int32(s.pos)
	lval.str = "EOF"

	if _, ok := s.skipWhitespace(lval, true); !ok {
		return
	}

	ch := s.next()
	if ch == eof {
		lval.pos = int32(s.pos)
		return
	}

	lval.id = int32(ch)
	lval.pos = int32(s.pos - 1)
	lval.str = s.in[lval.pos:s.pos]

	switch ch {
	case '$':
		// placeholder? $[0-9]+
		if lex.IsDigit(s.peek()) {
			s.scanPlaceholder(lval)
			return
		} else if s.scanDollarQuotedString(lval) {
			lval.id = SCONST
			return
		}
		return

	case identQuote:
		// "[^"]"
		if s.scanString(lval, identQuote, false /* allowEscapes */, true /* requireUTF8 */) {
			lval.id = IDENT
		}
		return

	case singleQuote:
		// '[^']'
		if s.scanString(lval, ch, false /* allowEscapes */, true /* requireUTF8 */) {
			lval.id = SCONST
		}
		return

	case 'b':
		// Bytes?
		if s.peek() == singleQuote {
			// b'[^']'
			s.pos++
			if s.scanString(lval, singleQuote, true /* allowEscapes */, false /* requireUTF8 */) {
				lval.id = BCONST
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
			s.pos++
			if s.scanString(lval, singleQuote, true /* allowEscapes */, true /* requireUTF8 */) {
				lval.id = SCONST
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
			lval.id = DOT_DOT
			return
		case lex.IsDigit(t):
			s.scanNumber(lval, ch)
			return
		}
		return

	case '!':
		switch s.peek() {
		case '=': // !=
			s.pos++
			lval.id = NOT_EQUALS
			return
		case '~': // !~
			s.pos++
			switch s.peek() {
			case '*': // !~*
				s.pos++
				lval.id = NOT_REGIMATCH
				return
			}
			lval.id = NOT_REGMATCH
			return
		}
		return

	case '?':
		switch s.peek() {
		case '?': // ??
			s.pos++
			lval.id = HELPTOKEN
			return
		case '|': // ?|
			s.pos++
			lval.id = JSON_SOME_EXISTS
			return
		case '&': // ?&
			s.pos++
			lval.id = JSON_ALL_EXISTS
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
				lval.id = INET_CONTAINED_BY_OR_EQUALS
				return
			}
			lval.id = LSHIFT
			return
		case '>': // <>
			s.pos++
			lval.id = NOT_EQUALS
			return
		case '=': // <=
			s.pos++
			lval.id = LESS_EQUALS
			return
		case '@': // <@
			s.pos++
			lval.id = CONTAINED_BY
			return
		}
		return

	case '>':
		switch s.peek() {
		case '>': // >>
			s.pos++
			switch s.peek() {
			case '=': // >>=
				s.pos++
				lval.id = INET_CONTAINS_OR_EQUALS
				return
			}
			lval.id = RSHIFT
			return
		case '=': // >=
			s.pos++
			lval.id = GREATER_EQUALS
			return
		}
		return

	case ':':
		switch s.peek() {
		case ':': // ::
			if s.peekN(1) == ':' {
				// :::
				s.pos += 2
				lval.id = TYPEANNOTATE
				return
			}
			s.pos++
			lval.id = TYPECAST
			return
		}
		return

	case '|':
		switch s.peek() {
		case '|': // ||
			s.pos++
			lval.id = CONCAT
			return
		}
		return

	case '/':
		switch s.peek() {
		case '/': // //
			s.pos++
			lval.id = FLOORDIV
			return
		}
		return

	case '~':
		switch s.peek() {
		case '*': // ~*
			s.pos++
			lval.id = REGIMATCH
			return
		}
		return

	case '@':
		switch s.peek() {
		case '>': // @>
			s.pos++
			lval.id = CONTAINS
			return
		}
		return

	case '&':
		switch s.peek() {
		case '&': // &&
			s.pos++
			lval.id = AND_AND
			return
		}
		return

	case '-':
		switch s.peek() {
		case '>': // ->
			if s.peekN(1) == '>' {
				// ->>
				s.pos += 2
				lval.id = FETCHTEXT
				return
			}
			s.pos++
			lval.id = FETCHVAL
			return
		}
		return

	case '#':
		switch s.peek() {
		case '>': // #>
			if s.peekN(1) == '>' {
				// #>>
				s.pos += 2
				lval.id = FETCHTEXT_PATH
				return
			}
			s.pos++
			lval.id = FETCHVAL_PATH
			return
		case '-': // #-
			s.pos++
			lval.id = REMOVE_PATH
			return
		}
		return

	default:
		if lex.IsDigit(ch) {
			s.scanNumber(lval, ch)
			return
		}
		if lex.IsIdentStart(ch) {
			s.scanIdent(lval)
			return
		}
	}

	// Everything else is a single character token which we already initialized
	// lval for above.
}

func (s *scanner) peek() int {
	if s.pos >= len(s.in) {
		return eof
	}
	return int(s.in[s.pos])
}

func (s *scanner) peekN(n int) int {
	pos := s.pos + n
	if pos >= len(s.in) {
		return eof
	}
	return int(s.in[pos])
}

func (s *scanner) next() int {
	ch := s.peek()
	if ch != eof {
		s.pos++
	}
	return ch
}

func (s *scanner) skipWhitespace(lval *sqlSymType, allowComments bool) (newline, ok bool) {
	newline = false
	for {
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
			if present, cok := s.scanComment(lval); !cok {
				return false, false
			} else if present {
				continue
			}
		}
		break
	}
	return newline, true
}

func (s *scanner) scanComment(lval *sqlSymType) (present, ok bool) {
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
				lval.id = ERROR
				lval.pos = int32(start)
				lval.str = "unterminated comment"
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

func (s *scanner) scanIdent(lval *sqlSymType) {
	s.pos--
	start := s.pos
	isASCII := true
	isLower := true

	// Consume the scanner character by character, stopping after the last legal
	// identifier character. By the end of this function, we need to
	// lowercase and unicode normalize this identifier, which is expensive if
	// there are actual unicode characters in it. If not, it's quite cheap - and
	// if it's lowercase already, there's no work to do. Therefore, we keep track
	// of whether the string is only ASCII or only ASCII lowercase for later.
	for {
		ch := s.peek()
		//fmt.Println(ch, ch >= utf8.RuneSelf, ch >= 'A' && ch <= 'Z')

		if ch >= utf8.RuneSelf {
			isASCII = false
		} else if ch >= 'A' && ch <= 'Z' {
			isLower = false
		}

		if !lex.IsIdentMiddle(ch) {
			break
		}

		s.pos++
	}
	//fmt.Println("parsed: ", s.in[start:s.pos], isASCII, isLower)

	if isLower {
		// Already lowercased - nothing to do.
		lval.str = s.in[start:s.pos]
	} else if isASCII {
		// We know that the identifier we've seen so far is ASCII, so we don't need
		// to unicode normalize. Instead, just lowercase as normal.
		b := s.allocBytes(s.pos - start)
		_ = b[s.pos-start-1] // For bounds check elimination.
		for i, c := range s.in[start:s.pos] {
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			b[i] = byte(c)
		}
		lval.str = *(*string)(unsafe.Pointer(&b))
	} else {
		// The string has unicode in it. No choice but to run Normalize.
		lval.str = lex.NormalizeName(s.in[start:s.pos])
	}

	isExperimental := false
	kw := lval.str
	switch {
	case strings.HasPrefix(lval.str, "experimental_"):
		kw = lval.str[13:]
		isExperimental = true
	case strings.HasPrefix(lval.str, "testing_"):
		kw = lval.str[8:]
		isExperimental = true
	}
	lval.id = lex.GetKeywordID(kw)
	if lval.id != lex.IDENT {
		if isExperimental {
			if _, ok := lex.AllowedExperimental[kw]; !ok {
				// If the parsed token is not on the whitelisted set of keywords,
				// then it might have been intended to be parsed as something else.
				// In that case, re-tokenize the original string.
				lval.id = lex.GetKeywordID(lval.str)
			} else {
				// It is a whitelisted keyword, so remember the shortened
				// keyword for further processing.
				lval.str = kw
			}
		}
	} else {
		// If the word after experimental_ or testing_ is an identifier,
		// then we might have classified it incorrectly after removing the
		// experimental_/testing_ prefix.
		lval.id = lex.GetKeywordID(lval.str)
	}
}

func (s *scanner) scanNumber(lval *sqlSymType, ch int) {
	start := s.pos - 1
	isHex := false
	hasDecimal := ch == '.'
	hasExponent := false

	for {
		ch := s.peek()
		if (isHex && lex.IsHexDigit(ch)) || lex.IsDigit(ch) {
			s.pos++
			continue
		}
		if ch == 'x' || ch == 'X' {
			if isHex || s.in[start] != '0' || s.pos != start+1 {
				lval.id = ERROR
				lval.str = errInvalidHexNumeric
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
			if !lex.IsDigit(ch) {
				lval.id = ERROR
				lval.str = "invalid floating point literal"
				return
			}
			continue
		}
		break
	}

	lval.str = s.in[start:s.pos]
	if hasDecimal || hasExponent {
		lval.id = FCONST
		floatConst := constant.MakeFromLiteral(lval.str, token.FLOAT, 0)
		if floatConst.Kind() == constant.Unknown {
			lval.id = ERROR
			lval.str = fmt.Sprintf("could not make constant float from literal %q", lval.str)
			return
		}
		lval.union.val = tree.NewNumVal(floatConst, lval.str, false /* negative */)
	} else {
		if isHex && s.pos == start+2 {
			lval.id = ERROR
			lval.str = errInvalidHexNumeric
			return
		}

		// Strip off leading zeros from non-hex (decimal) literals so that
		// constant.MakeFromLiteral doesn't inappropriately interpret the
		// string as an octal literal. Note: we can't use strings.TrimLeft
		// here, because it will truncate '0' to ''.
		if !isHex {
			for len(lval.str) > 1 && lval.str[0] == '0' {
				lval.str = lval.str[1:]
			}
		}

		lval.id = ICONST
		intConst := constant.MakeFromLiteral(lval.str, token.INT, 0)
		if intConst.Kind() == constant.Unknown {
			lval.id = ERROR
			lval.str = fmt.Sprintf("could not make constant int from literal %q", lval.str)
			return
		}
		lval.union.val = tree.NewNumVal(intConst, lval.str, false /* negative */)
	}
}

func (s *scanner) scanPlaceholder(lval *sqlSymType) {
	start := s.pos
	for lex.IsDigit(s.peek()) {
		s.pos++
	}
	lval.str = s.in[start:s.pos]

	placeholder, err := tree.NewPlaceholder(lval.str)
	if err != nil {
		lval.id = ERROR
		lval.str = err.Error()
		return
	}
	lval.id = PLACEHOLDER
	lval.union.val = placeholder
}

// scanHexString scans the content inside x'....'.
func (s *scanner) scanHexString(lval *sqlSymType, ch int) bool {
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
			lval.id = ERROR
			lval.str = errInvalidBytesLiteral
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
		lval.id = ERROR
		lval.str = errInvalidBytesLiteral
		return false
	}

	lval.id = BCONST
	lval.str = s.finishString(buf)
	return true
}

// scanBitString scans the content inside B'....'.
func (s *scanner) scanBitString(lval *sqlSymType, ch int) bool {
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
			lval.id = ERROR
			lval.str = fmt.Sprintf(`"%c" is not a valid binary digit`, rune(b))
			return false
		}
	}

	lval.id = BITCONST
	lval.str = s.finishString(buf)
	return true
}

// scanString scans the content inside '...'. This is used for simple
// string literals '...' but also e'....' and b'...'. For x'...', see
// scanHexString().
func (s *scanner) scanString(lval *sqlSymType, ch int, allowEscapes, requireUTF8 bool) bool {
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
			// SQL allows joining adjacent strings separated by whitespace
			// as long as that whitespace contains at least one
			// newline. Kind of strange to require the newline, but that
			// is the standard.
			if s.peek() == ch && newline {
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
						lval.id = ERROR
						lval.str = err.Error()
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
			lval.id = ERROR
			lval.str = errUnterminated
			return false
		}
	}

	if requireUTF8 && !utf8.Valid(buf) {
		lval.id = ERROR
		lval.str = errInvalidUTF8
		return false
	}

	lval.str = s.finishString(buf)
	return true
}

// scanDollarQuotedString scans for so called dollar-quoted strings, which start/end with either $$ or $tag$, where
// tag is some arbitrary string.  e.g. $$a string$$ or $escaped$a string$escaped$.
func (s *scanner) scanDollarQuotedString(lval *sqlSymType) bool {
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
				lval.id = ERROR
				lval.str = errUnterminated
			} else {
				// This is not a dollar-quoted string, reset the pos back to the start.
				s.pos = start
			}
			return false

		default:
			// If we haven't found a start tag yet, check whether the current characters is a valid for a tag.
			if !foundStartTag && !lex.IsIdentStart(ch) {
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
		lval.id = ERROR
		lval.str = errInvalidUTF8
		return false
	}

	lval.str = s.finishString(buf)
	return true
}

// SplitFirstStatement returns the length of the prefix of the string up to and
// including the first semicolon that separates statements. If there is no
// semicolon, returns ok=false.
func SplitFirstStatement(sql string) (pos int, ok bool) {
	s := makeScanner(sql)
	var lval sqlSymType
	for {
		s.scan(&lval)
		switch lval.id {
		case 0, ERROR:
			return 0, false
		case ';':
			return s.pos, true
		}
	}
}

// Tokens decomposes the input into lexical tokens.
func Tokens(sql string) (tokens []TokenString, ok bool) {
	s := makeScanner(sql)
	for {
		var lval sqlSymType
		s.scan(&lval)
		if lval.id == ERROR {
			return nil, false
		}
		if lval.id == 0 {
			break
		}
		tokens = append(tokens, TokenString{TokenID: lval.id, Str: lval.str})
	}
	return tokens, true
}

// TokenString is the unit value returned by Tokens.
type TokenString struct {
	TokenID int32
	Str     string
}

// LastLexicalToken returns the last lexical token. If the string has no lexical
// tokens, returns 0 and ok=false.
func LastLexicalToken(sql string) (lastTok int, ok bool) {
	s := makeScanner(sql)
	var lval sqlSymType
	for {
		last := lval.id
		s.scan(&lval)
		if lval.id == 0 {
			return int(last), last != 0
		}
	}
}

// EndsInSemicolon returns true if the last lexical token is a semicolon.
func EndsInSemicolon(sql string) bool {
	lastTok, ok := LastLexicalToken(sql)
	return ok && lastTok == ';'
}
