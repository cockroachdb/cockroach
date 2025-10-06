// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package compengine

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/redact"
)

// Engine is the common interface between the
// sql.completionsNode, used for SHOW COMPLETIONS invoked as statement
// source; and the observer statement logic in connExecutor.
//
// Its return rows contain 5 columns:
// - candidate completion text
// - category
// - description
// - start byte position
// - end byte position
type Engine interface {
	Next(ctx context.Context) (bool, error)
	Values() tree.Datums
	Close(ctx context.Context)
}

// Context represents the input of the completion request and the
// interface with the completion engine.
type Context interface {
	// QueryPos is the byte offset inside the original string where
	// completion was requested (the OFFSET clause).
	QueryPos() int

	// Sketch is a byte representation of the input tokens where each
	// character corresponds to one position in the token array and the
	// value of the character is an approximation of the token type
	// (a value of type Marker).
	// See the unit tests for examples/details.
	Sketch() string

	// CursorInToken returns true iff the completion cursor is inside
	// a token or immediately to the right of it.
	CursorInToken() bool

	// CursorInSpace returns true iff the completion cursor is
	// in-between tokens, or in whitespace at the beginning or at the
	// end of the input, or beyond the end of the input.
	CursorInSpace() bool

	// RelMarker returns the marker at the given position relative to
	// the completion query position. If there is no token at that
	// position (outside of input), zero is returned. If the cursor is
	// on whitespace (CursorInSpace) the marker for the token
	// immediately before is returned.
	RelMarker(pos int) Marker

	// RelToken returns the token at the given position relative to the
	// completion query position. If there is no token at that position
	// (outside of input), a zero token is returned. If the cursor is on
	// whitespace (CursorInSpace) the token immediately before is
	// returned.
	RelToken(pos int) scanner.InspectToken

	// AtWordOrInSpaceFollowingWord is equivalent to .RelMarker(0) ==
	// MarkIdentOrKeyword, and is provided for convenience. This returns
	// true both when the cursor is _on_ an identifier/keyword, or _at
	// any whitespace position afterwards_.
	AtWordOrInSpaceFollowingWord() bool

	// Query perform a SQL query.
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)

	// Trace records an unstructured logging message. Can be used
	// for debugging and testing.
	Trace(format string, args ...interface{})
}

// Rows is the type of the result row iterator returned by a
// completion method.
type Rows = eval.InternalRows

// Marker is the type of one item inside a input sketch.
type Marker int8

// The completion markers used in a sketch.
const (
	MarkCursorInToken  Marker = '\''
	MarkCursorInSpace  Marker = '_'
	MarkIdentOrKeyword Marker = 'i'
	MarkLitB           Marker = 'b'
	MarkLitS           Marker = 's'
	MarkLitI           Marker = '0'
	MarkLitBIT         Marker = 'B'
	MarkTypeOp         Marker = ':'
	MarkOther          Marker = ' '
)

// Method represents one method/heuristic for completions.
type Method interface {
	// Call runs the completion method/heuristic.
	//
	// The Rows produced by Call must conform to the completion
	// schema:
	//    completion STRING
	//    category STRING
	//    description STRING -- preferably 40 characters or less.
	//    start INT
	//    end INT
	//
	// Call may return a nil Rows to indicate that it does
	// not provide any completion candidates.
	Call(ctx context.Context, c Context) (Rows, error)

	// Name returns a label for the method. Used for tracing.
	Name() string
}

// QueryIterFn is an interface through which the completion engine can
// run SQL queries. Typically initialized using an InternalExecutor.
type QueryIterFn func(ctx context.Context, opName redact.RedactableString, query string, args ...interface{}) (Rows, error)

// New creates a completion engine.
func New(queryIter QueryIterFn, methods []Method, offset int, input string) Engine {
	tokens, sketch, cursorPosition := CompletionSketch(input, offset)

	c := &completions{
		queryIter:   queryIter,
		queryPos:    offset,
		queryTokIdx: cursorPosition,
		sketch:      sketch,
		tokens:      tokens,
		compMethods: methods,
	}

	// log.Warningf(context.Background(), "COMPLETIONS qPos %d tokIdx %d\ntokens: %#v\nsketch: %q", c.queryPos, c.queryTokIdx, c.tokens, c.sketch)
	return c
}
