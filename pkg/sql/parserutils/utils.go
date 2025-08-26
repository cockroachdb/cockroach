// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parserutils

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// PopulateErrorDetails properly wraps the "last error" field in the lexer.
func PopulateErrorDetails(
	tokID, errTokenID int32, lastTokStr string, lastTokPos int32, lastErr error, lIn string,
) error {
	var retErr error

	if tokID == errTokenID {
		// This is a tokenizer (lexical) error: the scanner
		// will have stored the error message in the string field.
		err := pgerror.WithCandidateCode(errors.Newf("lexical error: %s", lastTokStr), pgcode.Syntax)
		retErr = errors.WithSecondaryError(err, lastErr)
	} else {
		// This is a contextual error. Print the provided error message
		// and the error context.
		if !strings.Contains(lastErr.Error(), "syntax error") {
			// "syntax error" is already prepended when the yacc-generated
			// parser encounters a parsing error.
			lastErr = errors.Wrap(lastErr, "syntax error")
		}
		retErr = errors.Wrapf(lastErr, "at or near \"%s\"", lastTokStr)
	}

	// Find the end of the line containing the last token.
	i := strings.IndexByte(lIn[lastTokPos:], '\n')
	if i == -1 {
		i = len(lIn)
	} else {
		i += int(lastTokPos)
	}
	// Find the beginning of the line containing the last token. Note that
	// LastIndexByte returns -1 if '\n' could not be found.
	j := strings.LastIndexByte(lIn[:lastTokPos], '\n') + 1
	// Output everything up to and including the line containing the last token.
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "source SQL:\n%s\n", lIn[:i])
	// Output a caret indicating where the last token starts.
	fmt.Fprintf(&buf, "%s^", strings.Repeat(" ", int(lastTokPos)-j))
	return errors.WithDetail(retErr, buf.String())
}

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

// Parse is the same as sql/parser.Parse but is injected to avoid a dependency
// on the parser package.
var Parse = func(sql string) (statements.Statements, error) {
	return statements.Statements{}, errors.AssertionFailedf("sql.DoParserInjection hasn't been called")
}

// ParseExpr is the same as sql/parser.ParseExpr but is injected to avoid a
// dependency on the parser package.
var ParseExpr = func(sql string) (tree.Expr, error) {
	return nil, errors.AssertionFailedf("sql.DoParserInjection hasn't been called")
}

// ParseExprs is the same as sql/parser.ParseExprs but is injected to avoid a
// dependency on the parser package.
var ParseExprs = func(exprs []string) (tree.Exprs, error) {
	return nil, errors.AssertionFailedf("sql.DoParserInjection hasn't been called")
}

// ParseOne is the same as sql/parser.ParseOne but is injected to avoid a
// dependency on the parser package.
var ParseOne = func(sql string) (statements.Statement[tree.Statement], error) {
	return statements.Statement[tree.Statement]{}, errors.AssertionFailedf("sql.DoParserInjection hasn't been called")
}

// ParseQualifiedTableName is the same as sql/parser.ParseQualifiedTableName but
// is injected to avoid a dependency on the parser package.
var ParseQualifiedTableName = func(sql string) (*tree.TableName, error) {
	return nil, errors.AssertionFailedf("sql.DoParserInjection hasn't been called")
}
