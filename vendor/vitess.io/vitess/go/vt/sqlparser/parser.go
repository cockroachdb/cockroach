/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"errors"
	"fmt"
	"io"
	"sync"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// parserPool is a pool for parser objects.
var parserPool = sync.Pool{}

// zeroParser is a zero-initialized parser to help reinitialize the parser for pooling.
var zeroParser = *(yyNewParser().(*yyParserImpl))

// yyParsePooled is a wrapper around yyParse that pools the parser objects. There isn't a
// particularly good reason to use yyParse directly, since it immediately discards its parser.  What
// would be ideal down the line is to actually pool the stacks themselves rather than the parser
// objects, as per https://github.com/cznic/goyacc/blob/master/main.go. However, absent an upstream
// change to goyacc, this is the next best option.
//
// N.B: Parser pooling means that you CANNOT take references directly to parse stack variables (e.g.
// $$ = &$4) in sql.y rules. You must instead add an intermediate reference like so:
//    showCollationFilterOpt := $4
//    $$ = &Show{Type: string($2), ShowCollationFilterOpt: &showCollationFilterOpt}
func yyParsePooled(yylex yyLexer) int {
	// Being very particular about using the base type and not an interface type b/c we depend on
	// the implementation to know how to reinitialize the parser.
	var parser *yyParserImpl

	i := parserPool.Get()
	if i != nil {
		parser = i.(*yyParserImpl)
	} else {
		parser = yyNewParser().(*yyParserImpl)
	}

	defer func() {
		*parser = zeroParser
		parserPool.Put(parser)
	}()
	return parser.Parse(yylex)
}

// Instructions for creating new types: If a type
// needs to satisfy an interface, declare that function
// along with that interface. This will help users
// identify the list of types to which they can assert
// those interfaces.
// If the member of a type has a string with a predefined
// list of values, declare those values as const following
// the type.
// For interfaces that define dummy functions to consolidate
// a set of types, define the function as iTypeName.
// This will help avoid name collisions.

// Parse parses the SQL in full and returns a Statement, which
// is the AST representation of the query. If a DDL statement
// is partially parsed but still contains a syntax error, the
// error is ignored and the DDL is returned anyway.
func Parse(sql string) (Statement, error) {
	tokenizer := NewStringTokenizer(sql)
	if yyParsePooled(tokenizer) != 0 {
		if tokenizer.partialDDL != nil {
			if typ, val := tokenizer.Scan(); typ != 0 {
				return nil, fmt.Errorf("extra characters encountered after end of DDL: '%s'", string(val))
			}
			tokenizer.ParseTree = tokenizer.partialDDL
			return tokenizer.ParseTree, nil
		}
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, tokenizer.LastError.Error())
	}
	if tokenizer.ParseTree == nil {
		return nil, ErrEmpty
	}
	return tokenizer.ParseTree, nil
}

// ParseStrictDDL is the same as Parse except it errors on
// partially parsed DDL statements.
func ParseStrictDDL(sql string) (Statement, error) {
	tokenizer := NewStringTokenizer(sql)
	if yyParsePooled(tokenizer) != 0 {
		return nil, tokenizer.LastError
	}
	if tokenizer.ParseTree == nil {
		return nil, ErrEmpty
	}
	return tokenizer.ParseTree, nil
}

// ParseTokenizer is a raw interface to parse from the given tokenizer.
// This does not used pooled parsers, and should not be used in general.
func ParseTokenizer(tokenizer *Tokenizer) int {
	return yyParse(tokenizer)
}

// ParseNext parses a single SQL statement from the tokenizer
// returning a Statement which is the AST representation of the query.
// The tokenizer will always read up to the end of the statement, allowing for
// the next call to ParseNext to parse any subsequent SQL statements. When
// there are no more statements to parse, a error of io.EOF is returned.
func ParseNext(tokenizer *Tokenizer) (Statement, error) {
	return parseNext(tokenizer, false)
}

// ParseNextStrictDDL is the same as ParseNext except it errors on
// partially parsed DDL statements.
func ParseNextStrictDDL(tokenizer *Tokenizer) (Statement, error) {
	return parseNext(tokenizer, true)
}

func parseNext(tokenizer *Tokenizer, strict bool) (Statement, error) {
	if tokenizer.lastChar == ';' {
		tokenizer.next()
		tokenizer.skipBlank()
	}
	if tokenizer.lastChar == eofChar {
		return nil, io.EOF
	}

	tokenizer.reset()
	tokenizer.multi = true
	if yyParsePooled(tokenizer) != 0 {
		if tokenizer.partialDDL != nil && !strict {
			tokenizer.ParseTree = tokenizer.partialDDL
			return tokenizer.ParseTree, nil
		}
		return nil, tokenizer.LastError
	}
	if tokenizer.ParseTree == nil {
		return ParseNext(tokenizer)
	}
	return tokenizer.ParseTree, nil
}

// ErrEmpty is a sentinel error returned when parsing empty statements.
var ErrEmpty = errors.New("empty statement")

// SplitStatement returns the first sql statement up to either a ; or EOF
// and the remainder from the given buffer
func SplitStatement(blob string) (string, string, error) {
	tokenizer := NewStringTokenizer(blob)
	tkn := 0
	for {
		tkn, _ = tokenizer.Scan()
		if tkn == 0 || tkn == ';' || tkn == eofChar {
			break
		}
	}
	if tokenizer.LastError != nil {
		return "", "", tokenizer.LastError
	}
	if tkn == ';' {
		return blob[:tokenizer.Position-2], blob[tokenizer.Position-1:], nil
	}
	return blob, "", nil
}

// SplitStatementToPieces split raw sql statement that may have multi sql pieces to sql pieces
// returns the sql pieces blob contains; or error if sql cannot be parsed
func SplitStatementToPieces(blob string) (pieces []string, err error) {
	pieces = make([]string, 0, 16)
	tokenizer := NewStringTokenizer(blob)

	tkn := 0
	var stmt string
	stmtBegin := 0
	for {
		tkn, _ = tokenizer.Scan()
		if tkn == ';' {
			stmt = blob[stmtBegin : tokenizer.Position-2]
			pieces = append(pieces, stmt)
			stmtBegin = tokenizer.Position - 1

		} else if tkn == 0 || tkn == eofChar {
			blobTail := tokenizer.Position - 2

			if stmtBegin < blobTail {
				stmt = blob[stmtBegin : blobTail+1]
				pieces = append(pieces, stmt)
			}
			break
		}
	}

	err = tokenizer.LastError
	return
}

// String returns a string representation of an SQLNode.
func String(node SQLNode) string {
	if node == nil {
		return "<nil>"
	}

	buf := NewTrackedBuffer(nil)
	buf.Myprintf("%v", node)
	return buf.String()
}
