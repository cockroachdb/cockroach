// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lang

import (
	"bufio"
	"bytes"
	"io"
	"unicode"
)

//go:generate stringer -type=Token scanner.go

// Token is the kind of lexical token returned by the scanner (string,
// parentheses, comment, etc).
type Token int

const (
	// ILLEGAL is the invalid token that indicates the scanner has encountered
	// an invalid lexical pattern.
	ILLEGAL Token = iota
	// ERROR indicates that the scanner encountered an error while reading from
	// the input files. The text of the error can be accessed via the Literal
	// method.
	ERROR
	// EOF indicates the scanner has reached the end of the input.
	EOF
	// IDENT is an identifier composed of Unicode letter and number runes:
	// UnicodeLetter (UnicodeLetter | UnicodeNumber)*
	IDENT
	// STRING is a literal string delimited by double quotes that cannot extend
	// past the end of a line: " [^"\n]* "
	STRING
	// NUMBER is an numeric literal composed of Unicode numeric digits:
	// UnicodeDigit+
	NUMBER
	// WHITESPACE is any sequence of Unicode whitespace characters.
	WHITESPACE
	// COMMENT is a code comment that extends to end of line: # .* EOL
	COMMENT
	// LPAREN is the open parentheses rune: (
	LPAREN
	// RPAREN is the close parentheses rune: )
	RPAREN
	// LBRACKET is the open square bracket rune: [
	LBRACKET
	// RBRACKET is the close square bracket rune: ]
	RBRACKET
	// LBRACE is the open curly brace rune: {
	LBRACE
	// RBRACE is the close curly brace rune: }
	RBRACE
	// DOLLAR is the dollar sign rune: $
	DOLLAR
	// COLON is the colon rune: :
	COLON
	// ASTERISK is the asterisk rune: *
	ASTERISK
	// EQUALS is the equals sign rune: =
	EQUALS
	// ARROW is an equals sign followed by a greater than sign: =>
	ARROW
	// AMPERSAND is the ampersand rune: &
	AMPERSAND
	// COMMA is the comma rune: ,
	COMMA
	// CARET is the caret rune: ^
	CARET
	// DOT is the dot rune: .
	DOT
	// ELLIPSES is three periods in succession: ...
	ELLIPSES
	// PIPE is the vertical line rune: |
	PIPE
)

const (
	errRune = rune(-1)
	eofRune = rune(0)
)

// Scanner breaks a sequence of characters into a sequence of lexical tokens
// that are consumed by the parser in order to construct an Optgen AST. Each
// token is associated with a literal that is the string representation of that
// token. For many tokens, its literal is a constant. But for other tokens,
// like string and identifier tokens, the literal is the custom text that was
// scanned from the input file. Scanning stops unrecoverably at EOF, the first
// I/O error, or a token too large to fit in the buffer.
type Scanner struct {
	r   *bufio.Reader
	tok Token
	lit string
	err error

	// lineLoc tracks the current line and position within the current file
	// being scanned.
	lineLoc struct {
		line int
		pos  int
		prev int
	}
}

// NewScanner constructs a new scanner that will tokenize the given input.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r)}
}

// Token returns the last token that was scanned.
func (s *Scanner) Token() Token {
	return s.tok
}

// Literal returns the literal associated with the last token that was scanned.
func (s *Scanner) Literal() string {
	return s.lit
}

// LineLoc returns the current 0-based line number and column position of the
// scanner in the current file.
func (s *Scanner) LineLoc() (line, pos int) {
	return s.lineLoc.line, s.lineLoc.pos
}

// Scan reads the next token from the input and returns it. The Token, Literal,
// and LineLoc methods are also initialized with information about the token
// that was read.
func (s *Scanner) Scan() Token {
	// Read the next rune.
	ch := s.read()

	// If we see whitespace then consume all contiguous whitespace.
	if unicode.IsSpace(ch) {
		s.unread()
		return s.scanWhitespace()
	}

	// If we see a letter or underscore then consume as an identifier or keyword.
	if unicode.IsLetter(ch) || ch == '_' {
		s.unread()
		return s.scanIdentifier()
	}

	// If we see a digit then consume as a numeric literal.
	if unicode.IsDigit(ch) {
		s.unread()
		return s.scanNumericLiteral()
	}

	// Otherwise read the individual character.
	switch ch {
	case errRune:
		s.tok = ERROR
		s.lit = s.err.Error()

	case eofRune:
		s.tok = EOF
		s.lit = ""

	case '(':
		s.tok = LPAREN
		s.lit = "("

	case ')':
		s.tok = RPAREN
		s.lit = ")"

	case '[':
		s.tok = LBRACKET
		s.lit = "["

	case ']':
		s.tok = RBRACKET
		s.lit = "]"

	case '{':
		s.tok = LBRACE
		s.lit = "{"

	case '}':
		s.tok = RBRACE
		s.lit = "}"

	case '$':
		s.tok = DOLLAR
		s.lit = "$"

	case ':':
		s.tok = COLON
		s.lit = ":"

	case '*':
		s.tok = ASTERISK
		s.lit = "*"

	case ',':
		s.tok = COMMA
		s.lit = ","

	case '^':
		s.tok = CARET
		s.lit = "^"

	case '|':
		s.tok = PIPE
		s.lit = "|"

	case '&':
		s.tok = AMPERSAND
		s.lit = "&"

	case '=':
		if s.read() == '>' {
			s.tok = ARROW
			s.lit = "=>"
			break
		}

		s.unread()
		s.tok = EQUALS
		s.lit = "="

	case '.':
		if s.read() == '.' {
			if s.read() == '.' {
				s.tok = ELLIPSES
				s.lit = "..."
			} else {
				s.unread()
				s.tok = ILLEGAL
				s.lit = ".."
			}
			break
		}
		s.unread()
		s.tok = DOT
		s.lit = "."

	case '"':
		s.unread()
		return s.scanStringLiteral('"', false /* multiLine */)

	case '`':
		s.unread()
		return s.scanStringLiteral('`', true /* multiLine */)

	case '#':
		s.unread()
		return s.scanComment()

	default:
		s.tok = ILLEGAL
		s.lit = string(ch)
	}

	return s.tok
}

// read reads the next rune from the buffered reader. If no reader has yet been
// created, or if the current reader is exhausted, then the reader is reset to
// point to the next file. read returns errRune if there is an I/O error and
// eofRune once there are no more files to read.
func (s *Scanner) read() rune {
	// Once the scanner gets in the error state, it stays there.
	if s.err != nil {
		return errRune
	}

	ch, _, err := s.r.ReadRune()
	if err == io.EOF {
		return eofRune
	}

	if err != nil {
		s.err = err
		return errRune
	}

	s.lineLoc.prev = s.lineLoc.pos
	if ch == '\n' {
		s.lineLoc.line++
		s.lineLoc.pos = 0
	} else {
		s.lineLoc.pos++
	}

	return ch
}

// unread places the previously read rune back on the reader.
func (s *Scanner) unread() {
	// Once the scanner gets in the error state, it stays there.
	if s.err != nil {
		return
	}

	err := s.r.UnreadRune()
	if err != nil {
		// Last read wasn't a rune (probably an eof), so no-op.
		return
	}

	s.tok = ILLEGAL
	s.lit = ""

	if s.lineLoc.prev == -1 {
		panic("unread cannot be called twice in succession")
	}

	if s.lineLoc.pos == 0 {
		s.lineLoc.line--
	}

	s.lineLoc.pos = s.lineLoc.prev
	s.lineLoc.prev = -1
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() Token {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent whitespace character into the buffer.
	// Non-whitespace characters and EOF will cause the loop to exit.
	for {
		ch := s.read()
		if ch == eofRune {
			break
		}

		if !unicode.IsSpace(ch) {
			s.unread()
			break
		}

		buf.WriteRune(ch)
	}

	s.tok = WHITESPACE
	s.lit = buf.String()
	return WHITESPACE
}

// scanIdentifier consumes the current rune and all contiguous identifier runes.
func (s *Scanner) scanIdentifier() Token {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	for {
		ch := s.read()
		if ch == eofRune {
			break
		}

		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			s.unread()
			break
		}

		buf.WriteRune(ch)
	}

	s.tok = IDENT
	s.lit = buf.String()
	return s.tok
}

func (s *Scanner) scanStringLiteral(endChar rune, multiLine bool) Token {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read characters until the closing quote is found, or until either error,
	// newline, or EOF is read.
	for {
		ch := s.read()
		if ch == errRune || ch == eofRune || (!multiLine && ch == '\n') {
			s.unread()
			s.tok = ILLEGAL
			break
		}

		buf.WriteRune(ch)

		if ch == endChar {
			s.tok = STRING
			break
		}
	}

	s.lit = buf.String()
	return s.tok
}

func (s *Scanner) scanNumericLiteral() Token {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent Unicode digit character into the buffer.
	// Non-digit characters and EOF will cause the loop to exit.
	for {
		ch := s.read()
		if ch == eofRune {
			break
		}

		if !unicode.IsDigit(ch) {
			s.unread()
			break
		}

		buf.WriteRune(ch)
	}

	s.tok = NUMBER
	s.lit = buf.String()
	return s.tok
}

// scanComment consumes the current rune and all characters until newline.
func (s *Scanner) scanComment() Token {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent character into the buffer until either error,
	// newline, or EOF is read.
	for {
		ch := s.read()
		if ch == errRune || ch == eofRune || ch == '\n' {
			s.unread()
			break
		}

		buf.WriteRune(ch)
	}

	s.tok = COMMENT
	s.lit = buf.String()
	return COMMENT
}
