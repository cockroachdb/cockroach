// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package compengine

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// completions is the actual implementation of the completion logic.
type completions struct {
	// traceFn is an interface to report the progress of the completion
	// heuristics. Used during testing.
	traceFn func(format string, args ...interface{})

	// queryIter is an interface to run SQL queries and retrieve
	// result rows.
	queryIter QueryIterFn

	// compMethods is the list of completion methods.
	compMethods []Method

	// mlabel is the name of the current method.
	mlabel string

	// tracePrefix is the current tracing prefix.
	tracePrefix string

	// opName is the current operation name for queries.
	opName redact.RedactableString

	// curMethodIdx is the index of the method being considered.
	curMethodIdx int

	// tokens is the sequence of scanned tokens in the input string.
	tokens []scanner.InspectToken
	// sketch is a byte representation of tokens where each character
	// corresponds to one position in the token array and the
	// value of the character is an approximation of the token type.
	// See the unit tests for examples/details.
	sketch string

	// queryPos is the byte offset inside the original string where
	// completion was requested (the OFFSET clause).
	queryPos int

	// queryTokIdx is the index inside tokens/sketch where the cursor is
	// positioned. Do not use directly; instead use the various Rel
	// methods on Input.
	queryTokIdx int

	// rows represents the currently executing SQL query for a portion
	// of the completions. It is set anew by each completion function.
	rows Rows
	// curVal is the current row of completions, returned by the Values
	// method. It is computed via rows.Cur().
	curVal tree.Datums
}

var _ Engine = (*completions)(nil)

// Next implements the Engine interface.
func (c *completions) Next(ctx context.Context) (bool, error) {
	for {
		select {
		case <-ctx.Done():
			return false, nil
		default:
		}

		compEndIdx := len(c.compMethods)

		if c.rows != nil {
			hasNext, err := c.rows.Next(ctx)
			if err != nil {
				err = errors.CombineErrors(err, c.rows.Close())
				c.rows = nil
				c.curMethodIdx = compEndIdx
				return false, err
			}
			if hasNext {
				c.curVal = c.rows.Cur()
				return true, nil
			}
			err = c.rows.Close()
			if err != nil {
				err = errors.CombineErrors(err, c.rows.Close())
				c.rows = nil
				c.curMethodIdx = compEndIdx
				return false, err
			}
		}

		if c.curMethodIdx >= compEndIdx {
			return false, nil
		}

		fn := c.compMethods[c.curMethodIdx]
		c.tracePrefix = "???: "
		c.opName = "completions"
		if c.mlabel = fn.Name(); c.mlabel != "" {
			c.tracePrefix = c.mlabel + ": "
			c.opName = redact.Sprintf("comp-%s", c.mlabel)
		}
		ctx := logtags.AddTag(ctx, c.opName.StripMarkers(), nil)
		rows, err := fn.Call(ctx, c)
		if err != nil {
			c.curMethodIdx = compEndIdx
			return false, err
		}
		c.rows = rows
		c.curMethodIdx++
	}
}

// Values implements the Engine interface.
func (c *completions) Values() tree.Datums {
	return c.curVal
}

// Close implements the Engine interface.
func (c *completions) Close(ctx context.Context) {
	if c.rows != nil {
		err := c.rows.Close()
		if err != nil {
			log.Warningf(ctx, "closing completion iterator: %v", err)
		}
	}
}

// QueryPos implements the Context interface.
func (c *completions) QueryPos() int {
	return c.queryPos
}

// CursorInToken implements the Context interface.
func (c *completions) CursorInToken() bool {
	return c.sketch[c.queryTokIdx] == byte(MarkCursorInToken)
}

// CursorInSpace implements the Context interface.
func (c *completions) CursorInSpace() bool {
	return c.sketch[c.queryTokIdx] == byte(MarkCursorInSpace)
}

// Sketch implements the Context interface.
func (c *completions) Sketch() string {
	return c.sketch
}

// Trace implements the Context interface.
func (c *completions) Trace(format string, args ...interface{}) {
	if c.traceFn != nil {
		c.traceFn(c.tracePrefix+format, args...)
	}
}

// Query implements the Context interface.
func (c *completions) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return c.queryIter(ctx, c.opName, query, args...)
}

// rel retrieves a token/sketch position relative to the cursor.
//
// rel(0) returns the token on which the cursor is, or the token
// immediately before if the cursor is in-between tokens.
// rel(-1) is the token immediately before rel(0).
//
// rel(1) returns the first token that starts after the cursor.
// rel(2) is the token after that.
//
// If a relative index is not available, -1 is returned.
func (c *completions) relIdx(idx int) int {
	var off int
	if idx <= 0 {
		// -1: b
		// 0: c
		off = c.queryTokIdx - 1 + idx
	} else {
		// +1: d
		// +2: e
		off = c.queryTokIdx + idx
	}
	return off
}

// RelMarker implements the Context interface.
func (c *completions) RelMarker(idx int) Marker {
	off := c.relIdx(idx)
	if off < 0 || off >= len(c.sketch) {
		return 0
	}
	return Marker(c.sketch[off])
}

// RelToken implements the Context interface.
func (c *completions) RelToken(idx int) scanner.InspectToken {
	off := c.relIdx(idx)
	if off < 0 {
		return scanner.InspectToken{}
	}
	if off >= len(c.tokens) {
		return scanner.InspectToken{
			Start: c.tokens[len(c.tokens)-1].End,
			End:   c.tokens[len(c.tokens)-1].End,
		}
	}
	return c.tokens[off]
}

// AtWordOrInSpaceFollowingWord implements the Context interface.
func (c *completions) AtWordOrInSpaceFollowingWord() bool {
	return c.RelMarker(0) == MarkIdentOrKeyword
}

// TestingCheckSketch is exported for use by tests.
func (c *completions) TestingCheckSketch() error {
	return CheckSketch(c.tokens, c.sketch, c.queryTokIdx)
}

// TestingQueryTokIdx is exported for use by tests.
func (c *completions) TestingQueryTokIdx() int {
	return c.queryTokIdx
}

// TestingSetTraceFn is exported for use by tests.
func (c *completions) TestingSetTraceFn(fn func(string, ...interface{})) {
	c.traceFn = fn
}
