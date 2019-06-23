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
	"fmt"
	"io"
	"os"
	"strconv"
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
		// Remember any comments scanned by p.scan by initializing p.comments.
		p.comments = make(CommentsExpr, 0)

		if p.scan() == RBRACE {
			return define
		}
		p.unscan()

		// Get any comments that have accumulated.
		comments := p.comments
		p.comments = nil

		defineField := p.parseDefineField(comments)
		if defineField == nil {
			return nil
		}

		define.Fields = append(define.Fields, defineField)
	}
}

// define-field = field-name field-type
func (p *Parser) parseDefineField(comments CommentsExpr) *DefineFieldExpr {
	if !p.scanToken(IDENT, "define field name") {
		return nil
	}

	src := p.src
	name := p.s.Literal()

	if !p.scanToken(IDENT, "define field type") {
		return nil
	}

	typ := p.s.Literal()

	return &DefineFieldExpr{
		Src:      &src,
		Name:     StringExpr(name),
		Comments: comments,
		Type:     StringExpr(typ),
	}
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
		Match:    match.(*FuncExpr),
		Replace:  replace,
	}
}

// match = func
func (p *Parser) parseMatch() Expr {
	if !p.scanToken(LPAREN, "match pattern") {
		return nil
	}
	p.unscan()
	return p.parseFunc()
}

// replace = func | ref
func (p *Parser) parseReplace() Expr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		return p.parseFunc()

	case DOLLAR:
		p.unscan()
		return p.parseRef()

	default:
		p.addExpectedTokenErr("replace pattern")
		return nil
	}
}

// func = '(' func-name arg* ')'
func (p *Parser) parseFunc() Expr {
	if p.scan() != LPAREN {
		panic("caller should have checked for left parenthesis")
	}

	src := p.src
	name := p.parseFuncName()
	if name == nil {
		return nil
	}

	fn := &FuncExpr{Src: &src, Name: name}
	for {
		if p.scan() == RPAREN {
			return fn
		}

		p.unscan()
		arg := p.parseArg()
		if arg == nil {
			return nil
		}

		fn.Args = append(fn.Args, arg)
	}
}

// func-name = names | func
func (p *Parser) parseFuncName() Expr {
	switch p.scan() {
	case IDENT:
		p.unscan()
		return p.parseNames()

	case LPAREN:
		// Constructed name.
		p.unscan()
		return p.parseFunc()
	}

	p.addExpectedTokenErr("name")
	return nil
}

// names = name ('|' name)*
func (p *Parser) parseNames() Expr {
	var names NamesExpr
	for {
		if !p.scanToken(IDENT, "name") {
			return nil
		}

		names = append(names, NameExpr(p.s.Literal()))

		if p.scan() != PIPE {
			p.unscan()
			return &names
		}
	}
}

// match-child = bind | ref | match-and
func (p *Parser) parseArg() Expr {
	tok := p.scan()
	p.unscan()

	if tok == DOLLAR {
		return p.parseBindOrRef()
	}

	return p.parseAnd()
}

// bind = '$' label ':' and
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

	target := p.parseAnd()
	if target == nil {
		return nil
	}
	return &BindExpr{Src: &src, Label: label, Target: target}
}

// and = expr ('&' and)
func (p *Parser) parseAnd() Expr {
	src := p.peekNextSource()

	left := p.parseExpr()
	if left == nil {
		return nil
	}

	if p.scan() != AMPERSAND {
		p.unscan()
		return left
	}

	right := p.parseAnd()
	if right == nil {
		return nil
	}
	return &AndExpr{Src: src, Left: left, Right: right}
}

// expr = func | not | list | any | name | STRING | NUMBER
func (p *Parser) parseExpr() Expr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		return p.parseFunc()

	case CARET:
		p.unscan()
		return p.parseNot()

	case LBRACKET:
		p.unscan()
		return p.parseList()

	case ASTERISK:
		src := p.src
		return &AnyExpr{Src: &src}

	case IDENT:
		name := NameExpr(p.s.Literal())
		return &name

	case STRING:
		p.unscan()
		return p.parseString()

	case NUMBER:
		p.unscan()
		return p.parseNumber()

	default:
		p.addExpectedTokenErr("expression")
		return nil
	}
}

// not = '^' expr
func (p *Parser) parseNot() Expr {
	if p.scan() != CARET {
		panic("caller should have checked for caret")
	}

	src := p.src

	input := p.parseExpr()
	if input == nil {
		return nil
	}
	return &NotExpr{Src: &src, Input: input}
}

// list = '[' list-child* ']'
func (p *Parser) parseList() Expr {
	if p.scan() != LBRACKET {
		panic("caller should have checked for left bracket")
	}

	src := p.src

	list := &ListExpr{Src: &src}
	for {
		if p.scan() == RBRACKET {
			return list
		}

		p.unscan()
		item := p.parseListChild()
		if item == nil {
			return nil
		}

		list.Items = append(list.Items, item)
	}
}

// list-child = list-any | arg
func (p *Parser) parseListChild() Expr {
	if p.scan() == ELLIPSES {
		src := p.src
		return &ListAnyExpr{Src: &src}
	}
	p.unscan()
	return p.parseArg()
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

func (p *Parser) parseNumber() *NumberExpr {
	if p.scan() != NUMBER {
		panic("caller should have checked for numeric literal")
	}

	// Convert token literal to int64 value.
	i, err := strconv.ParseInt(p.s.Literal(), 10, 64)
	if err != nil {
		p.addErr(err.Error())
		return nil
	}

	e := NumberExpr(i)
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
