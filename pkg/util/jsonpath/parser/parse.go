// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func init() {
	tree.ValidateJSONPath = func(jsonpath string) (string, error) {
		jp, err := Parse(jsonpath)
		if err != nil {
			return "", err
		}
		return jp.AST.String(), nil
	}
}

type Parser struct {
	scanner    scanner.JSONPathScanner
	lexer      lexer
	parserImpl jsonpathParserImpl
}

func (p *Parser) scan() (query string, tokens []jsonpathSymType, done bool) {
	var lval jsonpathSymType

	p.scanner.Scan(&lval)
	if lval.id == 0 {
		return "", nil, true
	}

	startPos := lval.pos

	lval.pos = 0
	tokens = append(tokens, lval)
	var posBeforeScan int
	for {
		if lval.id == ERROR {
			return p.scanner.In()[startPos:], tokens, true
		}
		lval = jsonpathSymType{}
		posBeforeScan = p.scanner.Pos()
		p.scanner.Scan(&lval)
		if lval.id == 0 {
			return p.scanner.In()[startPos:posBeforeScan], tokens, (lval.id == 0)
		}
		lval.pos -= startPos
		tokens = append(tokens, lval)
	}
}

// parse parses a statement from the given scanned tokens.
func (p *Parser) parse(
	query string, tokens []jsonpathSymType,
) (statements.JsonpathStatement, error) {
	p.lexer.init(query, tokens, &p.parserImpl)
	defer p.lexer.cleanup()
	if p.parserImpl.Parse(&p.lexer) != 0 {
		if p.lexer.lastError == nil {
			// This should never happen -- there should be an error object
			// every time Parse() returns nonzero. We're just playing safe
			// here.
			p.lexer.Error("syntax error")
		}
		err := p.lexer.lastError
		return statements.JsonpathStatement{}, err
	}
	return statements.JsonpathStatement{
		AST: p.lexer.expr,
		SQL: query,
	}, nil
}

func (p *Parser) Parse(jsonpath string) (statements.JsonpathStatement, error) {
	p.scanner.Init(jsonpath)
	defer p.scanner.Cleanup()

	query, tokens, done := p.scan()
	if !done {
		return statements.JsonpathStatement{}, errors.AssertionFailedf("invalid jsonpath query: %s", jsonpath)
	}

	stmt, err := p.parse(query, tokens)
	if err != nil {
		return statements.JsonpathStatement{}, err
	}
	return stmt, nil
}

// Parse parses a jsonpath string and returns a jsonpath.Jsonpath object.
func Parse(jsonpath string) (statements.JsonpathStatement, error) {
	var p Parser
	return p.Parse(jsonpath)
}
