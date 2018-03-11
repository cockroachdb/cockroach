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
	"strings"
)

// FileResolver is used by the parser to abstract the opening and reading of
// input files. Callers of the parser can override the default behavior
// (os.Open) in order to open files in some other way (e.g. for testing).
type FileResolver func(name string) (io.Reader, error)

// Parser parses Optgen language input files and builds an abstract syntax tree
// (AST) from them. Typically the Optgen compiler invokes the parser and then
// performs semantic checks on the resulting AST. For more details on the
// Optgen language syntax, see the Syntax section of docs.go.
type Parser struct {
	files   []string
	file    int
	r       io.Reader
	s       *Scanner
	src     SourceLoc
	saveSrc SourceLoc
	errors  []error

	// comments accumulates contiguous comments as they are scanned, as long as
	// it has been initialized to a non-nil array. Parser functions initialize
	// it when they want to remember comments. For example, the parser tags
	// each define and rule with any comments that preceded it.
	comments CommentsExpr

	// resolver is invoked to open the input files provided to the parser.
	resolver FileResolver

	// unscanned is true if the last token was unscanned (i.e. put back to be
	// reparsed).
	unscanned bool
}

// NewParser constructs a new instance of the Optgen parser, with the specified
// list of file paths as its input files. The Parse method must be called in
// order to parse the input files.
func NewParser(files ...string) *Parser {
	p := &Parser{files: files}

	// By default, resolve file names by a call to os.Open.
	p.resolver = func(name string) (io.Reader, error) {
		return os.Open(name)
	}

	return p
}

// SetFileResolver overrides the default method of opening input files. The
// default resolver will use os.Open to open input files from disk. Callers
// can use this method to open input files in some other way.
func (p *Parser) SetFileResolver(resolver FileResolver) {
	p.resolver = resolver
}

// Parse parses the input files and returns the root expression of the AST. If
// there are parse errors, then Parse returns nil, and the errors are returned
// by the Errors function.
func (p *Parser) Parse() *RootExpr {
	root := p.parseRoot()

	// Ensure that all open files have been closed.
	p.closeScanner()

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
		var comments CommentsExpr
		var tags TagsExpr

		// Remember any comments at the top-level by initializing p.comments.
		p.comments = make(CommentsExpr, 0)

		tok := p.scan()
		src := p.src

		switch tok {
		case EOF:
			return rootOp

		case LBRACKET:
			p.unscan()

			// Get any comments that have accumulated.
			comments = p.comments
			p.comments = nil

			tags = p.parseTags()
			if tags == nil {
				p.tryRecover()
				break
			}

			if p.scan() != IDENT {
				p.unscan()

				rule := p.parseRule(comments, tags, src)
				if rule == nil {
					p.tryRecover()
					break
				}

				rootOp.Rules = append(rootOp.Rules, rule)
				break
			}

			fallthrough

		case IDENT:
			// Get any comments that have accumulated.
			if comments == nil {
				comments = p.comments
				p.comments = nil
			}

			// Only define identifier is allowed at the top level.
			if !p.isDefineIdent() {
				p.addExpectedTokenErr("define statement")
				p.tryRecover()
				break
			}

			p.unscan()

			define := p.parseDefine(comments, tags, src)
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
func (p *Parser) parseDefine(comments CommentsExpr, tags TagsExpr, src SourceLoc) *DefineExpr {
	if !p.scanToken(IDENT, "define statement") || p.s.Literal() != "define" {
		return nil
	}

	if !p.scanToken(IDENT, "define name") {
		return nil
	}

	name := p.s.Literal()
	define := &DefineExpr{Src: &src, Comments: comments, Name: StringExpr(name), Tags: tags}

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
func (p *Parser) parseRule(comments CommentsExpr, tags TagsExpr, src SourceLoc) *RuleExpr {
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
		Src:      &src,
		Name:     StringExpr(tags[0]),
		Comments: comments,
		Tags:     tags[1:],
		Match:    match.(*MatchExpr),
		Replace:  replace,
	}
}

// match = '(' match-names match-child* ')'
func (p *Parser) parseMatch() Expr {
	if !p.scanToken(LPAREN, "match pattern") {
		return nil
	}

	src := p.src
	names := p.parseMatchNames()
	if names == nil {
		return nil
	}

	match := &MatchExpr{Src: &src, Names: names}
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

// match-names = name ('|' name)*
func (p *Parser) parseMatchNames() NamesExpr {
	var names NamesExpr
	for {
		if !p.scanToken(IDENT, "name") {
			return nil
		}

		names = append(names, NameExpr(p.s.Literal()))

		if p.scan() != PIPE {
			p.unscan()
			return names
		}
	}
}

// match-child = bind | ref | match-and
func (p *Parser) parseMatchChild() Expr {
	tok := p.scan()
	p.unscan()

	if tok == DOLLAR {
		return p.parseBindOrRef()
	}

	return p.parseMatchAnd()
}

// bind = '$' label ':' match-and
// ref  = '$' label
func (p *Parser) parseBindOrRef() Expr {
	if p.scan() != DOLLAR {
		panic("caller should have checked for dollar")
	}

	src := p.src

	if !p.scanToken(IDENT, "label") {
		return nil
	}

	label := StringExpr(p.s.Literal())

	if p.scan() != COLON {
		p.unscan()
		return &RefExpr{Src: &src, Label: label}
	}

	target := p.parseMatchAnd()
	if target == nil {
		return nil
	}
	return &BindExpr{Src: &src, Label: label, Target: target}
}

// match-and = match-item ('&' match-and)
func (p *Parser) parseMatchAnd() Expr {
	src := p.peekNextSource()

	left := p.parseMatchItem()
	if left == nil {
		return nil
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

// match-item = match | match-not | match-list | match-any | name | STRING
func (p *Parser) parseMatchItem() Expr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		return p.parseMatch()

	case CARET:
		p.unscan()
		return p.parseMatchNot()

	case LBRACKET:
		p.unscan()
		return p.parseMatchList()

	case ASTERISK:
		return &MatchAnyExpr{}

	case IDENT:
		name := NameExpr(p.s.Literal())
		return &name

	case STRING:
		p.unscan()
		return p.parseString()

	default:
		p.addExpectedTokenErr("match pattern")
		return nil
	}
}

// match-not = '^' match-item
func (p *Parser) parseMatchNot() Expr {
	if p.scan() != CARET {
		panic("caller should have checked for caret")
	}

	src := p.src

	input := p.parseMatchItem()
	if input == nil {
		return nil
	}
	return &MatchNotExpr{Src: &src, Input: input}
}

// match-list        = match-list-any | match-list-first | match-list-last |
//                     match-list-single | match-list-empty
// match-list-any    = '[' '...' match-child '...' ']'
// match-list-first  = '[' match-child '...' ']'
// match-list-last   = '[' '...' match-child ']'
// match-list-single = '[' match-child ']'
// match-list-empty  = '[' ']'
func (p *Parser) parseMatchList() Expr {
	if p.scan() != LBRACKET {
		panic("caller should have checked for left bracket")
	}

	src := p.src

	var hasStartEllipses, hasEndEllipses bool

	switch p.scan() {
	case ELLIPSES:
		hasStartEllipses = true

	case RBRACKET:
		// Empty list case.
		return &MatchListEmptyExpr{}

	default:
		p.unscan()
	}

	matchItem := p.parseMatchChild()
	if matchItem == nil {
		return nil
	}

	switch p.scan() {
	case ELLIPSES:
		hasEndEllipses = true

	default:
		p.unscan()
	}

	if !p.scanToken(RBRACKET, "']'") {
		return nil
	}

	// Handle various combinations of start and end ellipses.
	if hasStartEllipses {
		if hasEndEllipses {
			return &MatchListAnyExpr{Src: &src, MatchItem: matchItem}
		}
		return &MatchListLastExpr{Src: &src, MatchItem: matchItem}
	} else if hasEndEllipses {
		return &MatchListFirstExpr{Src: &src, MatchItem: matchItem}
	}
	return &MatchListSingleExpr{Src: &src, MatchItem: matchItem}
}

// replace = construct | construct-list | ref | name | STRING
func (p *Parser) parseReplace() Expr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		return p.parseConstruct()

	case LBRACKET:
		p.unscan()
		return p.parseConstructList()

	case DOLLAR:
		p.unscan()
		return p.parseRef()

	case IDENT:
		name := NameExpr(p.s.Literal())
		return &name

	case STRING:
		p.unscan()
		return p.parseString()

	default:
		p.addExpectedTokenErr("replace pattern")
		return nil
	}
}

// construct = '(' construct-name replace* ')'
func (p *Parser) parseConstruct() Expr {
	if p.scan() != LPAREN {
		panic("caller should have checked for left paren")
	}

	src := p.src

	name := p.parseConstructName()
	if name == nil {
		return nil
	}

	construct := &ConstructExpr{Src: &src, Name: name}
	for {
		if p.scan() == RPAREN {
			return construct
		}

		p.unscan()
		arg := p.parseReplace()
		if arg == nil {
			return nil
		}

		construct.Args = append(construct.Args, arg)
	}
}

// construct-list = '[' replace* ']'
func (p *Parser) parseConstructList() Expr {
	if p.scan() != LBRACKET {
		panic("caller should have checked for left bracket")
	}

	src := p.src

	list := &ConstructListExpr{Src: &src}
	for {
		if p.scan() == RBRACKET {
			return list
		}

		p.unscan()
		item := p.parseReplace()
		if item == nil {
			return nil
		}

		list.Items = append(list.Items, item)
	}
}

// construct-name = name | construct
func (p *Parser) parseConstructName() Expr {
	switch p.scan() {
	case IDENT:
		name := NameExpr(p.s.Literal())
		return &name

	case LPAREN:
		// Constructed name.
		p.unscan()
		return p.parseConstruct()
	}

	p.addExpectedTokenErr("construct name")
	return nil
}

// ref = '$' label
func (p *Parser) parseRef() *RefExpr {
	if p.scan() != DOLLAR {
		panic("caller should have checked for dollar")
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

	if p.scan() != LBRACKET {
		panic("caller should have checked for left bracket")
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
	if p.scan() != STRING {
		panic("caller should have checked for literal string")
	}

	// Strip quotes.
	s := p.s.Literal()
	s = s[1 : len(s)-1]

	e := StringExpr(s)
	return &e
}

// peekNextSource returns the source information for the next token, but
// without actually consuming that token.
func (p *Parser) peekNextSource() *SourceLoc {
	p.scan()
	src := p.src
	p.unscan()

	// Don't directly take address of p.src, or the parser won't be
	// eligible for GC due to that reference.
	return &src
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
		// Restore saved current token, and save previous token.
		p.src, p.saveSrc = p.saveSrc, p.src
		p.unscanned = false
		return p.s.Token()
	}

	// Read the next token from the scanner.
	for {
		// Set source location of current token and save previous in case
		// unscan is called.
		p.saveSrc = p.src
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
				// Error opening file, don't try to recover.
				return EOF
			}

		case ERROR:
			// Error encountered while scanning.
			p.addErr(p.s.Literal())
			return ERROR

		case COMMENT:
			// Remember contiguous comments if p.comments is initialized, else
			// skip.
			if p.comments != nil {
				p.comments = append(p.comments, CommentExpr(p.s.Literal()))
			}

		case WHITESPACE:
			// A blank line resets any accumulating comments, since they have
			// to be contiguous.
			if p.comments != nil && strings.Count(p.s.Literal(), "\n") > 1 {
				p.comments = p.comments[:0]
			}

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

	// Save current token and make previous token the current token.
	p.src, p.saveSrc = p.saveSrc, p.src
	p.unscanned = true
}

// openScanner attempts to open a scanner and reader over the next input file.
// If it succeeds, then it stores the reader and scanner over the file and
// returns true. If it fails, then it stores the error in p.err and returns
// false.
func (p *Parser) openScanner() bool {
	r, err := p.resolver(p.files[p.file])
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
		// If the reader has a Close method, call it.
		closer, ok := p.r.(io.Closer)
		if ok {
			closer.Close()
		}
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
