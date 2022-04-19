// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"io"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type fileHook struct {
	s         *bufio.Scanner
	copy      *postgreStreamCopy
	closeFunc func()
	next      string
	err       error
}

func newFileHook(r io.Reader, closeFunc func()) (importHook, error) {
	s := bufio.NewScanner(r)

	fh := &fileHook{
		closeFunc: closeFunc,
		s:         s,
	}
	// TODO: deal with copy. This assumes all inserts!
	s.Split(fh.split)
	if err := s.Err(); err != nil {
		closeFunc()
		return nil, err
	}
	return fh, nil
}

func (f *fileHook) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if f.copy == nil {
		return splitSQLSemicolon(data, atEOF)
	}
	return bufio.ScanLines(data, atEOF)
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

func (f *fileHook) Done() bool {
	if f.err != nil {
		return true
	}
	if f.copy != nil {
		_, err := f.copy.Next()
		if errors.Is(err, errCopyDone) {
			f.copy = nil
			return f.Done()
		} else if err != nil {
			f.err = err
			return true
		}
		// Keep reading...
		return f.Done()
	}
	for f.s.Scan() {
		stmtRaw := f.s.Text()
		stmtRaw = skipOverComments(stmtRaw)
		f.next = strings.TrimSpace(stmtRaw)
		if len(f.next) > 0 {
			stmts, err := parser.Parse(f.next)
			if err == nil && len(stmts) > 0 {
				if cf, ok := stmts[0].AST.(*tree.CopyFrom); ok && cf.Stdin {
					// Set p.copy which reconfigures the scanner's split func.
					f.copy = newPostgreStreamCopy(f.s, copyDefaultDelimiter, copyDefaultNull)

					// We expect a single newline character following the COPY statement before
					// the copy data starts.
					if !f.s.Scan() {
						f.err = errors.AssertionFailedf("expected empty line")
						return true
					}
					if err := f.s.Err(); err != nil {
						f.err = err
						return true
					}
					if len(f.s.Bytes()) != 0 {
						f.err = errors.AssertionFailedf("expected empty line")
						return true
					}
				}
			}
			return false
		}
	}
	return true
}

func (f *fileHook) Next() (string, string) {
	return f.next, f.next
}

func (f *fileHook) Err() error {
	return f.err
}

func (f *fileHook) Close() {
	f.closeFunc()
}

var (
	ignoreCommentsRe = regexp.MustCompile(`^\s*(--.*)`)
)

func skipOverComments(s string) string {
	// Look for the first line with no whitespace or comments.
	for {
		m := ignoreCommentsRe.FindStringIndex(s)
		if m == nil {
			break
		}
		s = s[m[1]:]
	}
	return s
}
