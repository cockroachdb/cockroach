// Copyright 2017 The Bazel Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package syntax

// A lexical scanner for Skylark.

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// A Token represents a Skylark lexical token.
type Token int8

const (
	ILLEGAL Token = iota
	EOF

	NEWLINE
	INDENT
	OUTDENT

	// Tokens with values
	IDENT  // x
	INT    // 123
	FLOAT  // 1.23e45
	STRING // "foo" or 'foo' or '''foo''' or r'foo' or r"foo"

	// Punctuation
	PLUS          // +
	MINUS         // -
	STAR          // *
	SLASH         // /
	SLASHSLASH    // //
	PERCENT       // %
	AMP           // &
	PIPE          // |
	CIRCUMFLEX    // ^
	LTLT          // <<
	GTGT          // >>
	TILDE         // ~
	DOT           // .
	COMMA         // ,
	EQ            // =
	SEMI          // ;
	COLON         // :
	LPAREN        // (
	RPAREN        // )
	LBRACK        // [
	RBRACK        // ]
	LBRACE        // {
	RBRACE        // }
	LT            // <
	GT            // >
	GE            // >=
	LE            // <=
	EQL           // ==
	NEQ           // !=
	PLUS_EQ       // +=    (keep order consistent with PLUS..GTGT)
	MINUS_EQ      // -=
	STAR_EQ       // *=
	SLASH_EQ      // /=
	SLASHSLASH_EQ // //=
	PERCENT_EQ    // %=
	AMP_EQ        // &=
	PIPE_EQ       // |=
	CIRCUMFLEX_EQ // ^=
	LTLT_EQ       // <<=
	GTGT_EQ       // >>=
	STARSTAR      // **

	// Keywords
	AND
	BREAK
	CONTINUE
	DEF
	ELIF
	ELSE
	FOR
	IF
	IN
	LAMBDA
	LOAD
	NOT
	NOT_IN // synthesized by parser from NOT IN
	OR
	PASS
	RETURN

	maxToken
)

func (tok Token) String() string { return tokenNames[tok] }

// GoString is like String but quotes punctuation tokens.
// Use Sprintf("%#v", tok) when constructing error messages.
func (tok Token) GoString() string {
	if tok >= PLUS && tok <= STARSTAR {
		return "'" + tokenNames[tok] + "'"
	}
	return tokenNames[tok]
}

var tokenNames = [...]string{
	ILLEGAL:       "illegal token",
	EOF:           "end of file",
	NEWLINE:       "newline",
	INDENT:        "indent",
	OUTDENT:       "outdent",
	IDENT:         "identifier",
	INT:           "int literal",
	FLOAT:         "float literal",
	STRING:        "string literal",
	PLUS:          "+",
	MINUS:         "-",
	STAR:          "*",
	SLASH:         "/",
	SLASHSLASH:    "//",
	PERCENT:       "%",
	AMP:           "&",
	PIPE:          "|",
	CIRCUMFLEX:    "^",
	LTLT:          "<<",
	GTGT:          ">>",
	TILDE:         "~",
	DOT:           ".",
	COMMA:         ",",
	EQ:            "=",
	SEMI:          ";",
	COLON:         ":",
	LPAREN:        "(",
	RPAREN:        ")",
	LBRACK:        "[",
	RBRACK:        "]",
	LBRACE:        "{",
	RBRACE:        "]",
	LT:            "<",
	GT:            ">",
	GE:            ">=",
	LE:            "<=",
	EQL:           "==",
	NEQ:           "!=",
	PLUS_EQ:       "+=",
	MINUS_EQ:      "-=",
	STAR_EQ:       "*=",
	SLASH_EQ:      "/=",
	SLASHSLASH_EQ: "//=",
	PERCENT_EQ:    "%=",
	AMP_EQ:        "&=",
	PIPE_EQ:       "|=",
	CIRCUMFLEX_EQ: "^=",
	LTLT_EQ:       "<<=",
	GTGT_EQ:       ">>=",
	STARSTAR:      "**",
	AND:           "and",
	BREAK:         "break",
	CONTINUE:      "continue",
	DEF:           "def",
	ELIF:          "elif",
	ELSE:          "else",
	FOR:           "for",
	IF:            "if",
	IN:            "in",
	LAMBDA:        "lambda",
	LOAD:          "load",
	NOT:           "not",
	NOT_IN:        "not in",
	OR:            "or",
	PASS:          "pass",
	RETURN:        "return",
}

// A Position describes the location of a rune of input.
type Position struct {
	file *string // filename (indirect for compactness)
	Line int32   // 1-based line number
	Col  int32   // 1-based column number (strictly: rune)
}

// IsValid reports whether the position is valid.
func (p Position) IsValid() bool {
	return p.Line >= 1
}

// Filename returns the name of the file containing this position.
func (p Position) Filename() string {
	if p.file != nil {
		return *p.file
	}
	return "<unknown>"
}

// MakePosition returns position with the specified components.
func MakePosition(file *string, line, col int32) Position { return Position{file, line, col} }

// add returns the position at the end of s, assuming it starts at p.
func (p Position) add(s string) Position {
	if n := strings.Count(s, "\n"); n > 0 {
		p.Line += int32(n)
		s = s[strings.LastIndex(s, "\n")+1:]
		p.Col = 1
	}
	p.Col += int32(utf8.RuneCountInString(s))
	return p
}

func (p Position) String() string {
	if p.Col > 0 {
		return fmt.Sprintf("%s:%d:%d", p.Filename(), p.Line, p.Col)
	}
	return fmt.Sprintf("%s:%d", p.Filename(), p.Line)
}

func (p Position) isBefore(q Position) bool {
	if p.Line != q.Line {
		return p.Line < q.Line
	}
	return p.Col < q.Col
}

// An scanner represents a single input file being parsed.
type scanner struct {
	complete       []byte    // entire input
	rest           []byte    // rest of input
	token          []byte    // token being scanned
	pos            Position  // current input position
	depth          int       // nesting of [ ] { } ( )
	indentstk      []int     // stack of indentation levels
	dents          int       // number of saved INDENT (>0) or OUTDENT (<0) tokens to return
	lineStart      bool      // after NEWLINE; convert spaces to indentation tokens
	keepComments   bool      // accumulate comments in slice
	lineComments   []Comment // list of full line comments (if keepComments)
	suffixComments []Comment // list of suffix comments (if keepComments)
}

func newScanner(filename string, src interface{}, keepComments bool) (*scanner, error) {
	data, err := readSource(filename, src)
	if err != nil {
		return nil, err
	}
	return &scanner{
		complete:     data,
		rest:         data,
		pos:          Position{file: &filename, Line: 1, Col: 1},
		indentstk:    make([]int, 1, 10), // []int{0} + spare capacity
		lineStart:    true,
		keepComments: keepComments,
	}, nil
}

func readSource(filename string, src interface{}) (data []byte, err error) {
	switch src := src.(type) {
	case string:
		data = []byte(src)
	case []byte:
		data = src
	case io.Reader:
		data, err = ioutil.ReadAll(src)
	case nil:
		data, err = ioutil.ReadFile(filename)
	default:
		return nil, fmt.Errorf("invalid source: %T", src)
	}
	if err != nil {
		return nil, fmt.Errorf("reading %s: %s", filename, err)
	}
	return data, nil
}

// An Error describes the nature and position of a scanner or parser error.
type Error struct {
	Pos Position
	Msg string
}

func (e Error) Error() string { return e.Pos.String() + ": " + e.Msg }

// errorf is called to report an error.
// errorf does not return: it panics.
func (sc *scanner) error(pos Position, s string) {
	panic(Error{pos, s})
}

func (sc *scanner) errorf(pos Position, format string, args ...interface{}) {
	sc.error(pos, fmt.Sprintf(format, args...))
}

func (sc *scanner) recover(err *error) {
	// The scanner and parser panic both for routine errors like
	// syntax errors and for programmer bugs like array index
	// errors.  Turn both into error returns.  Catching bug panics
	// is especially important when processing many files.
	switch e := recover().(type) {
	case nil:
		// no panic
	case Error:
		*err = e
	default:
		*err = Error{sc.pos, fmt.Sprintf("internal error: %v", e)}
		if debug {
			log.Fatal(*err)
		}
	}
}

// eof reports whether the input has reached end of file.
func (sc *scanner) eof() bool {
	return len(sc.rest) == 0
}

// peekRune returns the next rune in the input without consuming it.
// Newlines in Unix, DOS, or Mac format are treated as one rune, '\n'.
func (sc *scanner) peekRune() rune {
	if len(sc.rest) == 0 {
		return 0
	}

	// fast path: ASCII
	if b := sc.rest[0]; b < utf8.RuneSelf {
		if b == '\r' {
			return '\n'
		}
		return rune(b)
	}

	r, _ := utf8.DecodeRune(sc.rest)
	return r
}

// readRune consumes and returns the next rune in the input.
// Newlines in Unix, DOS, or Mac format are treated as one rune, '\n'.
func (sc *scanner) readRune() rune {
	if len(sc.rest) == 0 {
		sc.error(sc.pos, "internal scanner error: readRune at EOF")
		return 0 // unreachable but eliminates bounds-check below
	}

	// fast path: ASCII
	if b := sc.rest[0]; b < utf8.RuneSelf {
		r := rune(b)
		sc.rest = sc.rest[1:]
		if r == '\r' {
			if len(sc.rest) > 0 && sc.rest[0] == '\n' {
				sc.rest = sc.rest[1:]
			}
			r = '\n'
		}
		if r == '\n' {
			sc.pos.Line++
			sc.pos.Col = 1
		} else {
			sc.pos.Col++
		}
		return r
	}

	r, size := utf8.DecodeRune(sc.rest)
	sc.rest = sc.rest[size:]
	sc.pos.Col++
	return r
}

// tokenValue records the position and value associated with each token.
type tokenValue struct {
	raw    string   // raw text of token
	int    int64    // decoded int
	bigInt *big.Int // decoded integers > int64
	float  float64  // decoded float
	string string   // decoded string
	pos    Position // start position of token
	triple bool     // was string triple quoted?
}

// startToken marks the beginning of the next input token.
// It must be followed by a call to endToken once the token has
// been consumed using readRune.
func (sc *scanner) startToken(val *tokenValue) {
	sc.token = sc.rest
	val.raw = ""
	val.pos = sc.pos
}

// endToken marks the end of an input token.
// It records the actual token string in val.raw if the caller
// has not done that already.
func (sc *scanner) endToken(val *tokenValue) {
	if val.raw == "" {
		val.raw = string(sc.token[:len(sc.token)-len(sc.rest)])
	}
}

// nextToken is called by the parser to obtain the next input token.
// It returns the token value and sets val to the data associated with
// the token.
//
// For all our input tokens, the associated data is val.pos (the
// position where the token begins), val.raw (the input string
// corresponding to the token).  For string and int tokens, the string
// and int fields additionally contain the token's interpreted value.
func (sc *scanner) nextToken(val *tokenValue) Token {

	// The following distribution of tokens guides case ordering:
	//
	//      COMMA          27   %
	//      STRING         23   %
	//      IDENT          15   %
	//      EQL            11   %
	//      LBRACK          5.5 %
	//      RBRACK          5.5 %
	//      NEWLINE         3   %
	//      LPAREN          2.9 %
	//      RPAREN          2.9 %
	//      INT             2   %
	//      others        < 1   %
	//
	// Although NEWLINE tokens are infrequent, and lineStart is
	// usually (~97%) false on entry, skipped newlines account for
	// about 50% of all iterations of the 'start' loop.

start:
	var c rune

	// Deal with leading spaces and indentation.
	blank := false
	savedLineStart := sc.lineStart
	if sc.lineStart {
		sc.lineStart = false
		col := 0
		for {
			c = sc.peekRune()
			if c == ' ' {
				col++
				sc.readRune()
			} else if c == '\t' {
				const tab = 8
				col += int(tab - (sc.pos.Col-1)%tab)
				sc.readRune()
			} else {
				break
			}
		}
		// The third clause is "trailing spaces without newline at EOF".
		if c == '#' || c == '\n' || c == 0 && col > 0 {
			blank = true
		}

		// Compute indentation level for non-blank lines not
		// inside an expression.  This is not the common case.
		if !blank && sc.depth == 0 {
			cur := sc.indentstk[len(sc.indentstk)-1]
			if col > cur {
				// indent
				sc.dents++
				sc.indentstk = append(sc.indentstk, col)
			} else if col < cur {
				// dedent(s)
				for len(sc.indentstk) > 0 && col < sc.indentstk[len(sc.indentstk)-1] {
					sc.dents--
					sc.indentstk = sc.indentstk[:len(sc.indentstk)-1] // pop
				}
				if col != sc.indentstk[len(sc.indentstk)-1] {
					sc.error(sc.pos, "unindent does not match any outer indentation level")
				}
			}
		}
	}

	// Return saved indentation tokens.
	if sc.dents != 0 {
		sc.startToken(val)
		sc.endToken(val)
		if sc.dents < 0 {
			sc.dents++
			return OUTDENT
		} else {
			sc.dents--
			return INDENT
		}
	}

	// start of line proper
	c = sc.peekRune()

	// Skip spaces.
	for c == ' ' || c == '\t' {
		sc.readRune()
		c = sc.peekRune()
	}

	// comment
	if c == '#' {
		if sc.keepComments {
			sc.startToken(val)
		}
		// Consume up to newline (included).
		for c != 0 && c != '\n' {
			sc.readRune()
			c = sc.peekRune()
		}
		if sc.keepComments {
			sc.endToken(val)
			if blank {
				sc.lineComments = append(sc.lineComments, Comment{val.pos, val.raw})
			} else {
				sc.suffixComments = append(sc.suffixComments, Comment{val.pos, val.raw})
			}
		}
	}

	// newline
	if c == '\n' {
		sc.lineStart = true
		if blank || sc.depth > 0 {
			// Ignore blank lines, or newlines within expressions (common case).
			sc.readRune()
			goto start
		}
		// At top-level (not in an expression).
		sc.startToken(val)
		sc.readRune()
		val.raw = "\n"
		return NEWLINE
	}

	// end of file
	if c == 0 {
		// Emit OUTDENTs for unfinished indentation,
		// preceded by a NEWLINE if we haven't just emitted one.
		if len(sc.indentstk) > 1 {
			if savedLineStart {
				sc.dents = 1 - len(sc.indentstk)
				sc.indentstk = sc.indentstk[1:]
				goto start
			} else {
				sc.lineStart = true
				sc.startToken(val)
				val.raw = "\n"
				return NEWLINE
			}
		}

		sc.startToken(val)
		sc.endToken(val)
		return EOF
	}

	// line continuation
	if c == '\\' {
		sc.readRune()
		if sc.peekRune() != '\n' {
			sc.errorf(sc.pos, "stray backslash in program")
		}
		sc.readRune()
		goto start
	}

	// start of the next token
	sc.startToken(val)

	// comma (common case)
	if c == ',' {
		sc.readRune()
		sc.endToken(val)
		return COMMA
	}

	// string literal
	if c == '"' || c == '\'' {
		return sc.scanString(val, c)
	}

	// identifier or keyword
	if isIdentStart(c) {
		// raw string literal
		if c == 'r' && len(sc.rest) > 1 && (sc.rest[1] == '"' || sc.rest[1] == '\'') {
			sc.readRune()
			c = sc.peekRune()
			return sc.scanString(val, c)
		}

		for isIdent(c) {
			sc.readRune()
			c = sc.peekRune()
		}
		sc.endToken(val)
		if k, ok := keywordToken[val.raw]; ok {
			return k
		}

		return IDENT
	}

	// brackets
	switch c {
	case '[', '(', '{':
		sc.depth++
		sc.readRune()
		sc.endToken(val)
		switch c {
		case '[':
			return LBRACK
		case '(':
			return LPAREN
		case '{':
			return LBRACE
		}
		panic("unreachable")

	case ']', ')', '}':
		if sc.depth == 0 {
			sc.error(sc.pos, "indentation error")
		} else {
			sc.depth--
		}
		sc.readRune()
		sc.endToken(val)
		switch c {
		case ']':
			return RBRACK
		case ')':
			return RPAREN
		case '}':
			return RBRACE
		}
		panic("unreachable")
	}

	// int or float literal, or period
	if isdigit(c) || c == '.' {
		return sc.scanNumber(val, c)
	}

	// other punctuation
	defer sc.endToken(val)
	switch c {
	case '=', '<', '>', '!', '+', '-', '%', '/', '&', '|', '^', '~': // possibly followed by '='
		start := sc.pos
		sc.readRune()
		if sc.peekRune() == '=' {
			sc.readRune()
			switch c {
			case '<':
				return LE
			case '>':
				return GE
			case '=':
				return EQL
			case '!':
				return NEQ
			case '+':
				return PLUS_EQ
			case '-':
				return MINUS_EQ
			case '/':
				return SLASH_EQ
			case '%':
				return PERCENT_EQ
			case '&':
				return AMP_EQ
			case '|':
				return PIPE_EQ
			case '^':
				return CIRCUMFLEX_EQ
			}
		}
		switch c {
		case '=':
			return EQ
		case '<':
			if sc.peekRune() == '<' {
				sc.readRune()
				if sc.peekRune() == '=' {
					sc.readRune()
					return LTLT_EQ
				} else {
					return LTLT
				}
			}
			return LT
		case '>':
			if sc.peekRune() == '>' {
				sc.readRune()
				if sc.peekRune() == '=' {
					sc.readRune()
					return GTGT_EQ
				} else {
					return GTGT
				}
			}
			return GT
		case '!':
			sc.error(start, "unexpected input character '!'")
		case '+':
			return PLUS
		case '-':
			return MINUS
		case '/':
			if sc.peekRune() == '/' {
				sc.readRune()
				if sc.peekRune() == '=' {
					sc.readRune()
					return SLASHSLASH_EQ
				} else {
					return SLASHSLASH
				}
			}
			return SLASH
		case '%':
			return PERCENT
		case '&':
			return AMP
		case '|':
			return PIPE
		case '^':
			return CIRCUMFLEX
		case '~':
			return TILDE
		}
		panic("unreachable")

	case ':', ';': // single-char tokens (except comma)
		sc.readRune()
		switch c {
		case ':':
			return COLON
		case ';':
			return SEMI
		}
		panic("unreachable")

	case '*': // possibly followed by '*' or '='
		sc.readRune()
		switch sc.peekRune() {
		case '*':
			sc.readRune()
			return STARSTAR
		case '=':
			sc.readRune()
			return STAR_EQ
		}
		return STAR
	}

	sc.errorf(sc.pos, "unexpected input character %#q", c)
	panic("unreachable")
}

func (sc *scanner) scanString(val *tokenValue, quote rune) Token {
	start := sc.pos
	triple := len(sc.rest) >= 3 && sc.rest[0] == byte(quote) && sc.rest[1] == byte(quote) && sc.rest[2] == byte(quote)
	sc.readRune()
	if triple {
		sc.readRune()
		sc.readRune()
	}

	quoteCount := 0
	for {
		if sc.eof() {
			sc.error(val.pos, "unexpected EOF in string")
		}
		c := sc.readRune()
		if c == '\n' && !triple {
			sc.error(val.pos, "unexpected newline in string")
		}
		if c == quote {
			quoteCount++
			if !triple || quoteCount == 3 {
				break
			}
		} else {
			quoteCount = 0
		}
		if c == '\\' {
			if sc.eof() {
				sc.error(val.pos, "unexpected EOF in string")
			}
			sc.readRune()
		}
	}

	sc.endToken(val)
	s, _, err := unquote(val.raw)
	if err != nil {
		sc.error(start, err.Error())
	}
	val.string = s
	return STRING
}

func (sc *scanner) scanNumber(val *tokenValue, c rune) Token {
	// https://github.com/google/skylark/blob/master/doc/spec.md#lexical-elements
	//
	// Python features not supported:
	// - integer literals of >64 bits of precision
	// - 123L or 123l long suffix
	// - traditional octal: 0755
	// https://docs.python.org/2/reference/lexical_analysis.html#integer-and-long-integer-literals

	start := sc.pos
	fraction, exponent := false, false

	if c == '.' {
		// dot or start of fraction
		sc.readRune()
		c = sc.peekRune()
		if !isdigit(c) {
			sc.endToken(val)
			return DOT
		}
		fraction = true
	} else if c == '0' {
		// hex, octal, binary or float
		sc.readRune()
		c = sc.peekRune()

		if c == '.' {
			fraction = true
		} else if c == 'x' || c == 'X' {
			// hex
			sc.readRune()
			c = sc.peekRune()
			if !isxdigit(c) {
				sc.error(start, "invalid hex literal")
			}
			for isxdigit(c) {
				sc.readRune()
				c = sc.peekRune()
			}
		} else if c == 'o' || c == 'O' {
			// octal
			sc.readRune()
			c = sc.peekRune()
			if !isodigit(c) {
				sc.error(sc.pos, "invalid octal literal")
			}
			for isodigit(c) {
				sc.readRune()
				c = sc.peekRune()
			}
		} else if c == 'b' || c == 'B' {
			// binary
			sc.readRune()
			c = sc.peekRune()
			if !isbdigit(c) {
				sc.error(sc.pos, "invalid binary literal")
			}
			for isbdigit(c) {
				sc.readRune()
				c = sc.peekRune()
			}
		} else {
			// float (or obsolete octal "0755")
			allzeros, octal := true, true
			for isdigit(c) {
				if c != '0' {
					allzeros = false
				}
				if c > '7' {
					octal = false
				}
				sc.readRune()
				c = sc.peekRune()
			}
			if c == '.' {
				fraction = true
			} else if c == 'e' || c == 'E' {
				exponent = true
			} else if octal && !allzeros {
				// We must support old octal until the Java
				// implementation groks the new one.
				// TODO(adonovan): reenable the check.
				if false {
					sc.endToken(val)
					sc.errorf(sc.pos, "obsolete form of octal literal; use 0o%s", val.raw[1:])
				}
			}
		}
	} else {
		// decimal
		for isdigit(c) {
			sc.readRune()
			c = sc.peekRune()
		}

		if c == '.' {
			fraction = true
		} else if c == 'e' || c == 'E' {
			exponent = true
		}
	}

	if fraction {
		sc.readRune() // consume '.'
		c = sc.peekRune()
		for isdigit(c) {
			sc.readRune()
			c = sc.peekRune()
		}

		if c == 'e' || c == 'E' {
			exponent = true
		}
	}

	if exponent {
		sc.readRune() // consume [eE]
		c = sc.peekRune()
		if c == '+' || c == '-' {
			sc.readRune()
			c = sc.peekRune()
			if !isdigit(c) {
				sc.error(sc.pos, "invalid float literal")
			}
		}
		for isdigit(c) {
			sc.readRune()
			c = sc.peekRune()
		}
	}

	sc.endToken(val)
	if fraction || exponent {
		var err error
		val.float, err = strconv.ParseFloat(val.raw, 64)
		if err != nil {
			sc.error(sc.pos, "invalid float literal")
		}
		return FLOAT
	} else {
		var err error
		s := val.raw
		val.bigInt = nil
		if len(s) > 2 && s[0] == '0' && (s[1] == 'o' || s[1] == 'O') {
			val.int, err = strconv.ParseInt(s[2:], 8, 64)
		} else if len(s) > 2 && s[0] == '0' && (s[1] == 'b' || s[1] == 'B') {
			val.int, err = strconv.ParseInt(s[2:], 2, 64)
		} else {
			val.int, err = strconv.ParseInt(s, 0, 64)
			if err != nil {
				num := new(big.Int)
				var ok bool = true
				val.bigInt, ok = num.SetString(s, 0)
				if ok {
					err = nil
				}
			}
		}
		if err != nil {
			sc.error(start, "invalid int literal")
		}
		return INT
	}
}

// isIdent reports whether c is an identifier rune.
func isIdent(c rune) bool {
	return isdigit(c) || isIdentStart(c)
}

func isIdentStart(c rune) bool {
	return 'a' <= c && c <= 'z' ||
		'A' <= c && c <= 'Z' ||
		c == '_' ||
		unicode.IsLetter(c)
}

func isdigit(c rune) bool  { return '0' <= c && c <= '9' }
func isodigit(c rune) bool { return '0' <= c && c <= '7' }
func isxdigit(c rune) bool { return isdigit(c) || 'A' <= c && c <= 'F' || 'a' <= c && c <= 'f' }
func isbdigit(c rune) bool { return '0' == c || c == '1' }

// keywordToken records the special tokens for
// strings that should not be treated as ordinary identifiers.
var keywordToken = map[string]Token{
	"and":      AND,
	"break":    BREAK,
	"continue": CONTINUE,
	"def":      DEF,
	"elif":     ELIF,
	"else":     ELSE,
	"for":      FOR,
	"if":       IF,
	"in":       IN,
	"lambda":   LAMBDA,
	"load":     LOAD,
	"not":      NOT,
	"or":       OR,
	"pass":     PASS,
	"return":   RETURN,

	// reserved words:
	"as": ILLEGAL,
	// "assert":   ILLEGAL, // heavily used by our tests
	"class":    ILLEGAL,
	"del":      ILLEGAL,
	"except":   ILLEGAL,
	"finally":  ILLEGAL,
	"from":     ILLEGAL,
	"global":   ILLEGAL,
	"import":   ILLEGAL,
	"is":       ILLEGAL,
	"nonlocal": ILLEGAL,
	"raise":    ILLEGAL,
	"try":      ILLEGAL,
	"while":    ILLEGAL,
	"with":     ILLEGAL,
	"yield":    ILLEGAL,
}
