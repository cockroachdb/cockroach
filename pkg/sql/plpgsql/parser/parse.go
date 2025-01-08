// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package parser exposes a parser for plpgsql.
package parser

import (
	"go/constant"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func init() {
	scanner.NewNumValFn = func(a constant.Value, s string, b bool) interface{} { return tree.NewNumVal(a, s, b) }
	scanner.NewPlaceholderFn = func(s string) (interface{}, error) { return tree.NewPlaceholder(s) }
	parser.ParseDoBlockFn = func(options tree.DoBlockOptions) (tree.DoBlockBody, error) {
		doBlockBody, err := makeDoStmt(options)
		if err != nil {
			return nil, err
		}
		return doBlockBody, nil
	}
}

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner    scanner.PLpgSQLScanner
	lexer      lexer
	parserImpl plpgsqlParserImpl
	tokBuf     [8]plpgsqlSymType
}

// INT8 is the historical interpretation of INT. This should be left
// alone in the future, since there are many sql fragments stored
// in various descriptors. Any user input that was created after
// INT := INT4 will simply use INT4 in any resulting code.
var defaultNakedIntType = types.Int

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string) (statements.PLpgStatement, error) {
	return p.parseWithDepth(1, sql, defaultNakedIntType)
}

func (p *Parser) scanFnBlock() (sql string, tokens []plpgsqlSymType, done bool) {
	var lval plpgsqlSymType
	tokens = p.tokBuf[:0]

	// Scan the first token.
	p.scanner.Scan(&lval)
	if lval.id == 0 {
		return "", nil, true
	}

	startPos := lval.pos
	// We make the resulting token positions match the returned string.
	lval.pos = 0
	tokens = append(tokens, lval)
	for {
		if lval.id == ERROR {
			return p.scanner.In()[startPos:], tokens, true
		}
		// Reset the plpgsqlSymType struct before scanning.
		lval = plpgsqlSymType{}
		posBeforeScan := p.scanner.Pos()
		p.scanner.Scan(&lval)
		if lval.id == 0 {
			return p.scanner.In()[startPos:posBeforeScan], tokens, (lval.id == 0)
		}
		lval.pos -= startPos
		tokens = append(tokens, lval)
	}
}

func (p *Parser) parseWithDepth(
	depth int, plpgsql string, nakedIntType *types.T,
) (statements.PLpgStatement, error) {
	p.scanner.Init(plpgsql)
	defer p.scanner.Cleanup()
	sql, tokens, done := p.scanFnBlock()
	stmt, err := p.parse(depth+1, sql, tokens, nakedIntType)
	if err != nil {
		return statements.PLpgStatement{}, err
	}
	if !done {
		return statements.PLpgStatement{}, errors.AssertionFailedf("invalid plpgsql function: %s", plpgsql)
	}
	return stmt, nil
}

// parse parses a statement from the given scanned tokens.
func (p *Parser) parse(
	depth int, sql string, tokens []plpgsqlSymType, nakedIntType *types.T,
) (statements.PLpgStatement, error) {
	p.lexer.init(sql, tokens, nakedIntType, &p.parserImpl)
	defer p.lexer.cleanup()
	if p.parserImpl.Parse(&p.lexer) != 0 {
		if p.lexer.lastError == nil {
			// This should never happen -- there should be an error object
			// every time Parse() returns nonzero. We're just playing safe
			// here.
			p.lexer.Error("syntax error")
		}
		err := p.lexer.lastError

		// Compatibility with 19.1 telemetry: prefix the telemetry keys
		// with the "syntax." prefix.
		// TODO(knz): move the auto-prefixing of feature names to a
		// higher level in the call stack.
		tkeys := errors.GetTelemetryKeys(err)
		if len(tkeys) > 0 {
			for i := range tkeys {
				tkeys[i] = "syntax." + tkeys[i]
			}
			err = errors.WithTelemetry(err, tkeys...)
		}

		return statements.PLpgStatement{}, err
	}
	return statements.PLpgStatement{
		AST:             p.lexer.stmt,
		SQL:             sql,
		NumPlaceholders: p.lexer.numPlaceholders,
		NumAnnotations:  p.lexer.numAnnotations,
	}, nil
}

// Parse parses a sql statement string and returns a list of Statements.
func Parse(sql string) (statements.PLpgStatement, error) {
	var p Parser
	return p.parseWithDepth(1, sql, defaultNakedIntType)
}
