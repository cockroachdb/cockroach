// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlstream streams an io.Reader into SQL statements.
package sqlstream

import (
	"bufio"
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	// Include this because the parser assumes builtin functions exist.
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Modified from importccl/read_import_pgdump.go.

// Stream streams an io.Reader into tree.Statements.
type Stream struct {
	scan *bufio.Scanner
}

// NewStream returns a new Stream to read from r.
func NewStream(r io.Reader) *Stream {
	const defaultMax = 1024 * 1024 * 32
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, defaultMax), defaultMax)
	p := &Stream{scan: s}
	s.Split(splitSQLSemicolon)
	return p
}

// splitSQLSemicolon is a bufio.SplitFunc that splits on SQL semicolon tokens.
func splitSQLSemicolon(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if pos, ok := parser.SplitFirstStatement(string(data)); ok {
		return pos, data[:pos], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

// Next returns the next statement, or io.EOF if complete.
func (s *Stream) Next() (tree.Statement, error) {
	for s.scan.Scan() {
		t := s.scan.Text()
		stmts, err := parser.Parse(t)
		if err != nil {
			return nil, err
		}
		switch len(stmts) {
		case 0:
			// Got whitespace or comments; try again.
		case 1:
			return stmts[0].AST, nil
		default:
			return nil, errors.Errorf("unexpected: got %d statements", len(stmts))
		}
	}
	if err := s.scan.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			err = errors.HandledWithMessage(err, "line too long")
		}
		return nil, err
	}
	return nil, io.EOF
}
