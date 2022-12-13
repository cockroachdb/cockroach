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
	"go/constant"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func init() {
	NewNumValFn = func(a constant.Value, s string, b bool) interface{} { return tree.NewNumVal(a, s, b) }
	NewPlaceholderFn = func(s string) (interface{}, error) { return tree.NewPlaceholder(s) }
}

// Statement is the result of parsing a single statement. It contains the AST
// node along with other information.
type Statement struct {
	// AST is the root of the AST tree for the parsed statement.
	AST *plpgsqltree.PLpgSQLStmtBlock

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

// String returns the AST formatted as a string.
func (stmt Statement) String() string {
	return stmt.StringWithFlags(tree.FmtSimple)
}

// StringWithFlags returns the AST formatted as a string (with the given flags).
func (stmt Statement) StringWithFlags(flags tree.FmtFlags) string {
	ctx := tree.NewFmtCtx(flags)
	stmt.AST.Format(ctx)
	return ctx.CloseAndGetString()
}

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner    Scanner
	lexer      lexer
	parserImpl plpgsqlParserImpl
	tokBuf     [8]plpgsqlSymType
	stmtBuf    [1]Statement
}

// INT8 is the historical interpretation of INT. This should be left
// alone in the future, since there are many sql fragments stored
// in various descriptors. Any user input that was created after
// INT := INT4 will simply use INT4 in any resulting code.
var defaultNakedIntType = types.Int

// NakedIntTypeFromDefaultIntSize given the size in bits or bytes (preferred)
// of how a "naked" INT type should be parsed returns the corresponding integer
// type.
func NakedIntTypeFromDefaultIntSize(defaultIntSize int32) *types.T {
	switch defaultIntSize {
	case 4, 32:
		return types.Int4
	default:
		return types.Int
	}
}

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string) (Statement, error) {
	return p.parseWithDepth(1, sql, defaultNakedIntType)
}

// ParseWithInt parses a sql statement string and returns a list of
// Statements. The INT token will result in the specified TInt type.
func (p *Parser) ParseWithInt(sql string, nakedIntType *types.T) (Statement, error) {
	return p.parseWithDepth(1, sql, nakedIntType)
}

func (p *Parser) parseOneWithInt(sql string, nakedIntType *types.T) (Statement, error) {
	stmt, err := p.parseWithDepth(1, sql, nakedIntType)
	if err != nil {
		return Statement{}, err
	}
	return stmt, nil
}

func (p *Parser) scanOneStmt() (sql string, tokens []plpgsqlSymType, done bool) {
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
) (Statement, error) {
	p.scanner.Init(plpgsql)
	defer p.scanner.Cleanup()
	sql, tokens, done := p.scanOneStmt()
	stmt, err := p.parse(depth+1, sql, tokens, nakedIntType)
	if err != nil {
		return Statement{}, err
	}
	if !done {
		return Statement{}, errors.AssertionFailedf("invalid plpgsql function: %s", plpgsql)
	}
	return stmt, nil
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

// unaryNegation constructs an AST node for a negation. This attempts
// to preserve constant NumVals and embed the negative sign inside
// them instead of wrapping in an UnaryExpr. This in turn ensures
// that negative numbers get considered as a single constant
// for the purpose of formatting and scrubbing.
func unaryNegation(e tree.Expr) tree.Expr {
	if cst, ok := e.(*tree.NumVal); ok {
		cst.Negate()
		return cst
	}

	// Common case.
	return &tree.UnaryExpr{
		Operator: tree.MakeUnaryOperator(tree.UnaryMinus),
		Expr:     e,
	}
}

// Parse parses a sql statement string and returns a list of Statements.
func Parse(sql string) (Statement, error) {
	var p Parser
	return p.parseWithDepth(1, sql, defaultNakedIntType)
}

// ParseOne parses a sql statement string, ensuring that it contains only a
// single statement, and returns that Statement. ParseOne will always
// interpret the INT and SERIAL types as 64-bit types, since this is
// used in various internal-execution paths where we might receive
// bits of SQL from other nodes. In general,earwe expect that all
// user-generated SQL has been run through the ParseWithInt() function.
func ParseOne(sql string) (Statement, error) {
	return ParseOneWithInt(sql, defaultNakedIntType)
}

// ParseOneWithInt is similar to ParseOn but interprets the INT and SERIAL
// types as the provided integer type.
func ParseOneWithInt(sql string, nakedIntType *types.T) (Statement, error) {
	var p Parser
	return p.parseOneWithInt(sql, nakedIntType)
}

var errBitLengthNotPositive = pgerror.WithCandidateCode(
	errors.New("length for type bit must be at least 1"), pgcode.InvalidParameterValue)

// newBitType creates a new BIT type with the given bit width.
func newBitType(width int32, varying bool) (*types.T, error) {
	if width < 1 {
		return nil, errBitLengthNotPositive
	}
	if varying {
		return types.MakeVarBit(width), nil
	}
	return types.MakeBit(width), nil
}

var errFloatPrecAtLeast1 = pgerror.WithCandidateCode(
	errors.New("precision for type float must be at least 1 bit"), pgcode.InvalidParameterValue)
var errFloatPrecMax54 = pgerror.WithCandidateCode(
	errors.New("precision for type float must be less than 54 bits"), pgcode.InvalidParameterValue)

// newFloat creates a type for FLOAT with the given precision.
func newFloat(prec int64) (*types.T, error) {
	if prec < 1 {
		return nil, errFloatPrecAtLeast1
	}
	if prec <= 24 {
		return types.Float4, nil
	}
	if prec <= 54 {
		return types.Float, nil
	}
	return nil, errFloatPrecMax54
}

// newDecimal creates a type for DECIMAL with the given precision and scale.
func newDecimal(prec, scale int32) (*types.T, error) {
	if scale > prec {
		err := pgerror.WithCandidateCode(
			errors.Newf("scale (%d) must be between 0 and precision (%d)", scale, prec),
			pgcode.InvalidParameterValue)
		return nil, err
	}
	return types.MakeDecimal(prec, scale), nil
}

// arrayOf creates a type alias for an array of the given element type and fixed
// bounds. The bounds are currently ignored.
func arrayOf(
	ref tree.ResolvableTypeReference, bounds []int32,
) (tree.ResolvableTypeReference, error) {
	// If the reference is a statically known type, then return an array type,
	// rather than an array type reference.
	if typ, ok := tree.GetStaticallyKnownType(ref); ok {
		// Do not allow type unknown[]. This is consistent with Postgres' behavior.
		if typ.Family() == types.UnknownFamily {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "type unknown[] does not exist")
		}
		if typ.Family() == types.VoidFamily {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "type void[] does not exist")
		}
		if err := types.CheckArrayElementType(typ); err != nil {
			return nil, err
		}
		return types.MakeArray(typ), nil
	}
	return &tree.ArrayTypeReference{ElementType: ref}, nil
}
