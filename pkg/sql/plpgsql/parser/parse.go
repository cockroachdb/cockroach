// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"go/constant"
)

func init() {
	scanner.NewNumValFn = func(a constant.Value, s string, b bool) interface{} { return tree.NewNumVal(a, s, b) }
	scanner.NewPlaceholderFn = func(s string) (interface{}, error) { return tree.NewPlaceholder(s) }
}

// Statement is the result of parsing a single statement. It contains the AST
// node along with other information.
type Statement struct {
	// AST is the root of the AST tree for the parsed statement.
	AST tree.Statement

	// SQL is the original SQL from which the statement was parsed. Note that this
	// is not appropriate for use in logging, as it may contain passwords and
	// other sensitive data.
	SQL string

	// NumPlaceholders indicates the number of arguments to the statement (which
	// are referenced through placeholders). This corresponds to the highest
	// argument position (i.e. the x in "$x") that appears in the query.
	//
	// Note: where there are "gaps" in the placeholder positions, this number is
	// based on the highest position encountered. For example, for `SELECT $3`,
	// NumPlaceholders is 3. These cases are malformed and will result in a
	// type-check error.
	NumPlaceholders int

	// NumAnnotations indicates the number of annotations in the tree. It is equal
	// to the maximum annotation index.
	NumAnnotations tree.AnnotationIdx
}

// Statements is a list of parsed statements.
type Statements []Statement

// String returns the AST formatted as a string.
func (stmts Statements) String() string {
	return stmts.StringWithFlags(tree.FmtSimple)
}

// StringWithFlags returns the AST formatted as a string (with the given flags).
func (stmts Statements) StringWithFlags(flags tree.FmtFlags) string {
	ctx := tree.NewFmtCtx(flags)
	for i, s := range stmts {
		if i > 0 {
			ctx.WriteString("; ")
		}
		ctx.FormatNode(s.AST)
	}
	return ctx.CloseAndGetString()
}

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner    scanner.PLPGSQLScanner
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
func (p *Parser) Parse(sql string) (Statements, error) {
	return p.parseWithDepth(1, sql, defaultNakedIntType)
}

func (p *Parser) scanOneStmt() (sql string, tokens []plpgsqlSymType, done bool) {
	var lval plpgsqlSymType
	tokens = p.tokBuf[:0]

	// Scan the first token.
	for {
		p.scanner.Scan(&lval)
		if lval.id == 0 {
			return "", nil, true
		}
		if lval.id != ';' {
			break
		}
	}

	startPos := lval.pos
	// We make the resulting token positions match the returned string.
	lval.pos = 0
	tokens = append(tokens, lval)
	for {
		if lval.id == ERROR {
			return p.scanner.In()[startPos:], tokens, true
		}
		posBeforeScan := p.scanner.Pos()
		p.scanner.Scan(&lval)
		if lval.id == 0 || lval.id == ';' {
			return p.scanner.In()[startPos:posBeforeScan], tokens, (lval.id == 0)
		}
		lval.pos -= startPos
		tokens = append(tokens, lval)
	}
}

func (p *Parser) parseWithDepth(depth int, sql string, nakedIntType *types.T) (Statements, error) {
	stmts := Statements(p.stmtBuf[:0])
	p.scanner.Init(sql)
	defer p.scanner.Cleanup()
	for {
		sql, tokens, done := p.scanOneStmt()
		stmt, err := p.parse(depth+1, sql, tokens, nakedIntType)
		if err != nil {
			return nil, err
		}
		if stmt.AST != nil {
			stmts = append(stmts, stmt)
		}
		if done {
			break
		}
	}
	return stmts, nil
}

// parse parses a statement from the given scanned tokens.
func (p *Parser) parse(
	depth int, sql string, tokens []plpgsqlSymType, nakedIntType *types.T,
) (Statement, error) {
	p.lexer.init(sql, tokens, nakedIntType)
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

		return Statement{}, err
	}
	return Statement{
		AST:             p.lexer.stmt,
		SQL:             sql,
		NumPlaceholders: p.lexer.numPlaceholders,
		NumAnnotations:  p.lexer.numAnnotations,
	}, nil
}

// Parse parses a sql statement string and returns a list of Statements.
func Parse(sql string) (Statements, error) {
	var p Parser
	return p.parseWithDepth(1, sql, defaultNakedIntType)
}
