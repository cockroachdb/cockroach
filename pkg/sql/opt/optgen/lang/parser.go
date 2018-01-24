// Copyright 2018 The Cockroach Authors.
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

package lang

import (
	"fmt"
	"io"
	"os"
)

// Parser parses Optgen language input files and builds an abstract syntax tree
// (AST) from them. Typically the Optgen compiler invokes the parser and then
// performs semantic checks on the resulting AST. For more details on the
// Optgen language syntax, see the Syntax section of docs.go.
type Parser struct {
	files  []string
	file   int
	r      io.ReadCloser
	s      *Scanner
	src    SourceLoc
	errors []error

	// openFile is called to open a file of the specified name. Tests can hook
	// this in order to avoid actually opening files on disk.
	openFile func(name string) (io.ReadCloser, error)

	// unscanned is true if the last token was unscanned (i.e. put back to be
	// reparsed).
	unscanned bool
}

// NewParser constructs a new instance of the Optgen parser, with the specified
// list of file paths as its input files. The Parse method must be called in
// order to parse the input files.
func NewParser(files ...string) *Parser {
	// By default, open files using os.Open.
	openFile := func(name string) (io.ReadCloser, error) {
		return os.Open(name)
	}
	return &Parser{files: files, openFile: openFile}
}

// Close ensures all open files have been closed.
func (p *Parser) Close() {
	p.closeScanner()
}

// Parse parses the input files and returns the root expression of the AST. If
// there are parse errors, then Parse returns nil, and the errors are returned
// by the Errors function.
func (p *Parser) Parse() *RootExpr {
	root := p.parseRoot()
	if p.errors != nil {
		return nil
	}
	return root
}

// Errors returns the collection of errors that occurred during parsing. If no
// errors occurred, then Errors returns nil.
func (p *Parser) Errors() []error {
	return p.errors
}

// root = tags (define | rule)
func (p *Parser) parseRoot() *RootExpr {
	rootOp := &RootExpr{}

	// Ensure the scanner has been created over the first file.
	if p.s == nil {
		// If no files to parse, then return empty root expression.
		if len(p.files) == 0 {
			return rootOp
		}

		if !p.openScanner() {
			return nil
		}
	}

	for {
		var tags TagsExpr

		tok := p.scan()
		src := p.src

		switch tok {
		case EOF:
			return rootOp

		case LBRACKET:
			p.unscan()

			tags = p.parseTags()
			if tags == nil {
				p.tryRecover()
				break
			}

			if p.scan() != IDENT {
				p.unscan()

				rule := p.parseRule(tags, src)
				if rule == nil {
					p.tryRecover()
					break
				}

				rootOp.Rules = append(rootOp.Rules, rule)
				break
			}

			fallthrough

		case IDENT:
			// Only define identifier is allowed at the top level.
			if !p.isDefineIdent() {
				p.addExpectedTokenErr("define statement")
				p.tryRecover()
				break
			}

			p.unscan()

			define := p.parseDefine(tags, src)
			if define == nil {
				p.tryRecover()
				break
			}

			rootOp.Defines = append(rootOp.Defines, define)

		default:
			p.addExpectedTokenErr("define statement or rule")
			p.tryRecover()
		}
	}
}

// define = 'define' define-name '{' define-field* '}'
func (p *Parser) parseDefine(tags TagsExpr, src SourceLoc) *DefineExpr {
	if !p.scanToken(IDENT, "define statement") || p.s.Literal() != "define" {
		return nil
	}

	if !p.scanToken(IDENT, "define name") {
		return nil
	}

	name := p.s.Literal()
	define := &DefineExpr{Src: &src, Name: StringExpr(name), Tags: tags}

	if !p.scanToken(LBRACE, "'{'") {
		return nil
	}

	for {
		if p.scan() == RBRACE {
			return define
		}

		p.unscan()
		defineField := p.parseDefineField()
		if defineField == nil {
			return nil
		}

		define.Fields = append(define.Fields, defineField)
	}
}

// define-field = field-name field-type
func (p *Parser) parseDefineField() *DefineFieldExpr {
	if !p.scanToken(IDENT, "define field name") {
		return nil
	}

	src := p.src
	name := p.s.Literal()

	if !p.scanToken(IDENT, "define field type") {
		return nil
	}

	typ := p.s.Literal()

	return &DefineFieldExpr{Src: &src, Name: StringExpr(name), Type: StringExpr(typ)}
}

// rule = match '=>' replace
func (p *Parser) parseRule(tags TagsExpr, src SourceLoc) *RuleExpr {
	match := p.parseMatch()
	if match == nil {
		return nil
	}

	if !p.scanToken(ARROW, "'=>'") {
		return nil
	}

	replace := p.parseReplace()
	if replace == nil {
		return nil
	}

	return &RuleExpr{
		Src:     &src,
		Name:    StringExpr(tags[0]),
		Tags:    tags[1:],
		Match:   match,
		Replace: replace,
	}
}

// match = '(' match-opnames match-child* ')'
func (p *Parser) parseMatch() *MatchExpr {
	if !p.scanToken(LPAREN, "match pattern") {
		return nil
	}

	src := p.src
	opNames := p.parseOpNames()
	if opNames == nil {
		return nil
	}

	match := &MatchExpr{Src: &src, Names: opNames}
	for {
		if p.scan() == RPAREN {
			return match
		}

		p.unscan()
		matchChild := p.parseMatchChild()
		if matchChild == nil {
			return nil
		}

		match.Args = append(match.Args, matchChild)
	}
}

// match-opnames = match-opname ('|' match-opname)
func (p *Parser) parseOpNames() OpNamesExpr {
	var names OpNamesExpr
	for {
		if !p.scanToken(IDENT, "operator name") {
			return nil
		}

		names = append(names, OpNameExpr(p.s.Literal()))

		if p.scan() != PIPE {
			p.unscan()
			return names
		}
	}
}

// match-child = (bind-child | match-unbound-child) ('&' match-and)?
func (p *Parser) parseMatchChild() Expr {
	tok := p.scan()
	src := p.src
	p.unscan()

	var match Expr
	if tok == DOLLAR {
		bind := p.parseBindChild()
		if bind == nil {
			return nil
		}
		match = bind
	} else {
		match = p.parseMatchUnboundChild()
	}

	if match == nil {
		return nil
	}

	if p.scan() != AMPERSAND {
		p.unscan()
		return match
	}

	and := p.parseMatchAnd()
	if and == nil {
		return nil
	}

	return &MatchAndExpr{Src: &src, Left: match, Right: and}
}

// bind-child = '$' label ':' match-unbound-child
func (p *Parser) parseBindChild() *BindExpr {
	if !p.scanToken(DOLLAR, "'$'") {
		return nil
	}

	src := p.src

	if !p.scanToken(IDENT, "bind label") {
		return nil
	}

	label := p.s.Literal()

	if !p.scanToken(COLON, "':'") {
		return nil
	}

	target := p.parseMatchUnboundChild()
	if target == nil {
		return nil
	}

	return &BindExpr{Src: &src, Label: StringExpr(label), Target: target}
}

// match-unbound-child = match | STRING | '^' match-unbound-child | match-any |
//                       match-list
func (p *Parser) parseMatchUnboundChild() Expr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		match := p.parseMatch()
		if match == nil {
			return nil
		}
		return match

	case STRING:
		p.unscan()
		return p.parseString()

	case CARET:
		src := p.src
		input := p.parseMatchUnboundChild()
		if input == nil {
			return nil
		}
		return &MatchNotExpr{Src: &src, Input: input}

	case ASTERISK:
		return &MatchAnyExpr{}

	case LBRACKET:
		p.unscan()
		list := p.parseMatchList()
		if list == nil {
			return nil
		}
		return list

	default:
		p.addExpectedTokenErr("match pattern")
		return nil
	}
}

// match-and = match-custom ('&' match-and)?
func (p *Parser) parseMatchAnd() Expr {
	left := p.parseMatchCustom()
	if left == nil {
		return nil
	}

	// Use source location from left operand, unless it's not available, in
	// which case use the location of the last token.
	src := left.Source()
	if src == nil {
		// Don't directly take address of p.src, or the parser won't be
		// eligible for GC due to that reference.
		lastSrc := p.src
		src = &lastSrc
	}

	if p.scan() != AMPERSAND {
		p.unscan()
		return left
	}

	right := p.parseMatchAnd()
	if right == nil {
		return nil
	}
	return &MatchAndExpr{Src: src, Left: left, Right: right}
}

// match-custom = match-invoke | '^' match-custom
func (p *Parser) parseMatchCustom() Expr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		invoke := p.parseMatchInvoke()
		if invoke == nil {
			return nil
		}
		return invoke

	case CARET:
		src := p.src
		input := p.parseMatchCustom()
		if input == nil {
			return nil
		}
		return &MatchNotExpr{Src: &src, Input: input}

	default:
		p.addExpectedTokenErr("custom function call")
		return nil
	}
}

// match-invoke = '(' invoke-name ref* ')'
func (p *Parser) parseMatchInvoke() *MatchInvokeExpr {
	if !p.scanToken(LPAREN, "'('") {
		return nil
	}

	src := p.src

	if !p.scanToken(IDENT, "custom function name") {
		return nil
	}

	matchInvoke := &MatchInvokeExpr{Src: &src, FuncName: StringExpr(p.s.Literal())}

	for {
		switch p.scan() {
		case RPAREN:
			return matchInvoke

		case DOLLAR:
			p.unscan()
			ref := p.parseRef()
			if ref == nil {
				return nil
			}
			matchInvoke.Args = append(matchInvoke.Args, ref)

			// Improve error message in case where attempt is made to bind one
			// of the function call arguments. Example:
			//   (Op * & (Func $bind:(Func2))) => (Op)
			if p.scan() == COLON {
				p.addErr(fmt.Sprintf("cannot bind custom function call arguments"))
				return nil
			}
			p.unscan()

		default:
			p.addExpectedTokenErr("variable reference")
			return nil
		}
	}
}

// match-list = '[' '...' match-child '...' ']'
func (p *Parser) parseMatchList() Expr {
	if !p.scanToken(LBRACKET, "'['") {
		return nil
	}

	src := p.src

	if !p.scanToken(ELLIPSES, "'...'") {
		return nil
	}

	matchItem := p.parseMatchChild()
	if matchItem == nil {
		return nil
	}

	if !p.scanToken(ELLIPSES, "'...'") {
		return nil
	}

	if !p.scanToken(RBRACKET, "']'") {
		return nil
	}

	return &MatchListExpr{Src: &src, MatchItem: matchItem}
}

// replace = construct | STRING | ref
func (p *Parser) parseReplace() Expr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		construct := p.parseConstruct()
		if construct == nil {
			return nil
		}
		return construct

	case STRING:
		p.unscan()
		return p.parseString()

	case DOLLAR:
		p.unscan()
		return p.parseRef()

	default:
		p.addExpectedTokenErr("replace pattern")
		return nil
	}
}

// construct = '(' construct-name replace* ')'
func (p *Parser) parseConstruct() *ConstructExpr {
	if !p.scanToken(LPAREN, "'('") {
		return nil
	}

	src := p.src

	opName := p.parseConstructName()
	if opName == nil {
		return nil
	}

	construct := &ConstructExpr{Src: &src, OpName: opName}
	for {
		if p.scan() == RPAREN {
			return construct
		}

		p.unscan()
		item := p.parseReplace()
		if item == nil {
			return nil
		}

		construct.Args = append(construct.Args, item)
	}
}

// construct-name = IDENT | construct
func (p *Parser) parseConstructName() Expr {
	switch p.scan() {
	case IDENT:
		s := StringExpr(p.s.Literal())
		return &s

	case LPAREN:
		// Constructed name.
		p.unscan()
		construct := p.parseConstruct()
		if construct == nil {
			return nil
		}
		return construct
	}

	p.addExpectedTokenErr("construct name")
	return nil
}

// ref = '$' label
func (p *Parser) parseRef() *RefExpr {
	if !p.scanToken(DOLLAR, "'$'") {
		return nil
	}

	src := p.src

	if !p.scanToken(IDENT, "label") {
		return nil
	}

	return &RefExpr{Src: &src, Label: StringExpr(p.s.Literal())}
}

// tags = '[' IDENT (',' IDENT)* ']'
func (p *Parser) parseTags() TagsExpr {
	var tags TagsExpr

	if !p.scanToken(LBRACKET, "'['") {
		return nil
	}

	for {
		if !p.scanToken(IDENT, "tag name") {
			return nil
		}

		tags = append(tags, TagExpr(p.s.Literal()))

		if p.scan() == RBRACKET {
			return tags
		}

		p.unscan()
		if !p.scanToken(COMMA, "comma") {
			return nil
		}
	}
}

func (p *Parser) parseString() *StringExpr {
	if !p.scanToken(STRING, "literal string") {
		return nil
	}

	// Strip quotes.
	s := p.s.Literal()
	s = s[1 : len(s)-1]

	e := StringExpr(s)
	return &e
}

// scanToken scans the next token. If it does not have the expected token type,
// then scanToken records an error and returns false. Otherwise, it returns
// true.
func (p *Parser) scanToken(expected Token, desc string) bool {
	if p.scan() != expected {
		p.addExpectedTokenErr(desc)
		return false
	}

	return true
}

// scan returns the next non-whitespace, non-comment token from the underlying
// scanner. If a token has been unscanned then read that instead.
func (p *Parser) scan() Token {
	// If we have a token in the buffer, then return it.
	if p.unscanned {
		p.unscanned = false
		return p.s.Token()
	}

	// Read the next token from the scanner.
	for {
		// Set source location of current token.
		p.src.Line, p.src.Pos = p.s.LineLoc()

		tok := p.s.Scan()
		switch tok {
		case EOF:
			// Reached end of current file, so try to open next file.
			if p.file+1 >= len(p.files) {
				// No more files to parse.
				return EOF
			}
			p.file++

			if !p.openScanner() {
				// Error opening file.
				return ERROR
			}

		case ERROR:
			// Error encountered while scanning.
			p.addErr(p.s.Literal())
			return ERROR

		case WHITESPACE, COMMENT:
			// Skip whitespace and comments.

		default:
			return tok
		}
	}
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() {
	if p.unscanned {
		panic("unscan was already called")
	}

	p.unscanned = true
}

// openScanner attempts to open a scanner and reader over the next input file.
// If it succeeds, then it stores the reader and scanner over the file and
// returns true. If it fails, then it stores the error in p.err and returns
// false.
func (p *Parser) openScanner() bool {
	r, err := p.openFile(p.files[p.file])
	if err != nil {
		p.errors = append(p.errors, err)
		return false
	}

	// Close any previous scanner and open a new one.
	p.closeScanner()
	p.r = r
	p.s = NewScanner(r)
	p.src.File = p.files[p.file]
	return true
}

// closeScanner ensures that the current scanner and reader is closed.
func (p *Parser) closeScanner() {
	if p.s != nil {
		p.r.Close()
		p.r = nil
		p.s = nil
	}
}

// addExpectedTokenErr is used when the parser encounters an unexpected token.
// The desc argument describes what the parser expected instead of the current
// unexpected token.
func (p *Parser) addExpectedTokenErr(desc string) {
	if p.s.Token() == EOF {
		p.addErr(fmt.Sprintf("expected %s, found EOF", desc))
	} else {
		p.addErr(fmt.Sprintf("expected %s, found '%s'", desc, p.s.Literal()))
	}
}

// addErr wraps the given error text with file, line, and position context
// information.
func (p *Parser) addErr(text string) {
	err := fmt.Errorf("%s: %s", p.src, text)
	p.errors = append(p.errors, err)
}

// tryRecover attempts to recover from a parse error in order to continue
// reporting additional errors.
func (p *Parser) tryRecover() {
	// Scan ahead, looking for top-level tokens that might allow the parser to
	// recover enough to report further errors.
	for {
		tok := p.scan()
		switch tok {
		case EOF, ERROR:
			// Terminate scan.
			return

		case LBRACKET, IDENT:
			// Look for define identifier and left bracket tokens at start of
			// line, as those are usually good recovery points.
			if p.src.Pos == 0 {
				if tok == LBRACKET || p.isDefineIdent() {
					p.unscan()
				}
				return
			}
		}
	}
}

func (p *Parser) isDefineIdent() bool {
	return p.s.Token() == IDENT && p.s.Literal() == "define"
}
