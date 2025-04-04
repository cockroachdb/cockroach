// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
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

var (
	ReCache = tree.NewRegexpCache(64)

	errCurrentInRoot = pgerror.Newf(pgcode.Syntax,
		"@ is not allowed in root expressions")
	errLastInNonArray = pgerror.Newf(pgcode.Syntax,
		"LAST is allowed only in array subscripts")
)

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
	stmt, err := p.Parse(jsonpath)
	if err != nil {
		return statements.JsonpathStatement{}, err
	}
	// Similar to flattenJsonPathParseItem in postgres, we do a pass over the AST
	// to perform some semantic checks.
	if err := walkAST(stmt.AST.Path); err != nil {
		return statements.JsonpathStatement{}, err
	}
	return stmt, nil
}

// TODO(normanchenn): Similarly to flattenJsonPathParseItem, we could use this to
// generate a normalized jsonpath string, rather than calling stmt.AST.String().
func walkAST(path jsonpath.Path) error {
	return walk(path, 0 /* nestingLevel */, false /* insideArraySubscript */)
}

func walk(path jsonpath.Path, nestingLevel int, insideArraySubscript bool) error {
	switch path := path.(type) {
	case jsonpath.Paths:
		for _, p := range path {
			if err := walk(p, nestingLevel, insideArraySubscript); err != nil {
				return err
			}
		}
		return nil
	case jsonpath.ArrayList:
		for _, p := range path {
			if err := walk(p, nestingLevel, true /* insideArraySubscript */); err != nil {
				return err
			}
		}
		return nil
	case jsonpath.ArrayIndexRange:
		if err := walk(path.Start, nestingLevel, insideArraySubscript); err != nil {
			return err
		}
		if err := walk(path.End, nestingLevel, insideArraySubscript); err != nil {
			return err
		}
		return nil
	case jsonpath.Operation:
		if err := walk(path.Left, nestingLevel, insideArraySubscript); err != nil {
			return err
		}
		if path.Right != nil {
			if err := walk(path.Right, nestingLevel, insideArraySubscript); err != nil {
				return err
			}
		}
		return nil
	case jsonpath.Filter:
		if err := walk(path.Condition, nestingLevel+1, insideArraySubscript); err != nil {
			return err
		}
		return nil
	case jsonpath.Current:
		if nestingLevel <= 0 {
			return errCurrentInRoot
		}
		return nil
	case jsonpath.Last:
		if !insideArraySubscript {
			return errLastInNonArray
		}
		return nil
	case jsonpath.Root, jsonpath.Key, jsonpath.Wildcard, jsonpath.Regex,
		jsonpath.AnyKey, jsonpath.Scalar:
		// These are leaf nodes that don't require any further checks.
		return nil
	default:
		panic(errors.AssertionFailedf("unhandled path type: %T", path))
	}
}
