// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package compengine

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/errors"
)

// CompletionSketch construct a representation of the lexical
// structure of the string, where 1 character represents 1 token.
// This makes it possible to create completion heuristics that
// manipulate the token sequence using regular expressions.
//
// The position of the cursor is marked either as:
//
// - the byte ' if the cursor is "inside" the token; or
//
// - an underscore, if the cursor is on whitespace in-between tokens.
//
// In any case, the indexes in the resulting sketch are also valid
// indexes in the token array. A pseudo-token with ID 0 is inserted at
// the cursor position to preserve this indexing property.
//
// Note: this function is exported for testing. There should be
// no need to use this function directly; use the Sketch() method
// on the Context instead.
func CompletionSketch(
	input string, offset int,
) (tokens []scanner.InspectToken, sketch string, cursorPosition int) {
	// Analyze the input string.
	tokens = scanner.Inspect(input)
	// Enforce a final EOF token, if there's none yet.
	if len(tokens) == 0 || tokens[len(tokens)-1].ID != 0 {
		tokens = append(tokens, scanner.InspectToken{
			Start: int32(len(input)),
			End:   int32(len(input)),
		})
	}

	var buf strings.Builder
	if offset < int(tokens[0].Start) {
		buf.WriteByte(byte(MarkCursorInSpace))
		cursorPosition = 0
	}

	for idx := range tokens[:len(tokens)-1] {
		tok := &tokens[idx]

		c := mapTokenToCompSketch(*tok)

		buf.WriteByte(byte(c))

		// Are we at the cursor? If so, mark it.
		if offset >= int(tok.Start) && offset <= int(tok.End) {
			buf.WriteByte(byte(MarkCursorInToken))
			cursorPosition = idx + 1
		} else if idx < len(tokens)-1 && offset > int(tok.End) && offset < int(tokens[idx+1].Start) {
			buf.WriteByte(byte(MarkCursorInSpace))
			cursorPosition = idx + 1
		}
	}

	// Requested offset at or beyond EOF.
	if cursorPosition == 0 && offset >= int(tokens[len(tokens)-1].End) {
		buf.WriteByte(byte(MarkCursorInSpace))
		cursorPosition = buf.Len() - 1
		// We need to ensure that the Start/End positions are never
		// outside of the string, so that indexing using them is always
		// possible.
		offset = int(tokens[len(tokens)-1].End)
	}

	// Finally, move the empty token from the end back into position.
	empty := tokens[len(tokens)-1]
	empty.Start = int32(offset)
	empty.End = empty.Start
	empty.Str = ""
	copy(tokens[cursorPosition+1:], tokens[cursorPosition:])
	tokens[cursorPosition] = empty

	return tokens, buf.String(), cursorPosition
}

// mapTokenToCompSketch maps the current token to a completion sketch
// character. This will be the input for the completion methods.
func mapTokenToCompSketch(tok scanner.InspectToken) Marker {
	tokID := tok.ID
	if tok.ID == lexbase.ERROR {
		tokID = tok.MaybeID
	}

	var c Marker
	switch tokID {
	case lexbase.IDENT:
		c = MarkIdentOrKeyword
	case lexbase.SCONST:
		c = MarkLitS
	case lexbase.ICONST:
		c = MarkLitI
	case lexbase.BCONST:
		c = MarkLitB
	case lexbase.BITCONST:
		c = MarkLitBIT
	case lexbase.TYPECAST, lexbase.TYPEANNOTATE:
		c = MarkTypeOp
	default:
		if tok.ID < int32('A') {
			// Single-character punctuation. Keep as-is in sketch.
			c = Marker(tok.ID)
		} else if tok.MaybeID == lexbase.IDENT {
			// An identifier-like keyword. Mark it as an identifier.
			c = MarkIdentOrKeyword
		} else {
			// Some other token: composite punctuation, etc.
			c = MarkOther
		}
	}
	return c
}

// CheckSketch performs some health checks on a sketch. This
// is exported for testing purposes only.
func CheckSketch(tokens []scanner.InspectToken, sketch string, pos int) error {
	if len(tokens) != len(sketch) {
		return errors.Newf("size mismatch: tokens %d vs sketch %d\ntokens: %#v\nsketch: %q",
			len(tokens), len(sketch), tokens, sketch)
	}
	if pos < 0 || pos >= len(sketch) {
		return errors.Newf(" cursor %d out of sketch len %d: %q",
			pos, len(sketch), sketch)
	}
	if sketch[pos] != byte(MarkCursorInSpace) && sketch[pos] != byte(MarkCursorInToken) {
		return errors.Newf("cursor position %d not marked properly: %q\ntokens: %#v",
			pos, sketch, tokens)
	}
	return nil
}
