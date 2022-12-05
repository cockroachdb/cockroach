// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell

import (
	"os"

	"github.com/cockroachdb/errors"
)

// copyEditor redirects the input to either a "main" editor
// (outside of COPY mode) or a "copy" editor (inside COPY).
// This is because the main editor may be multi-line and
// thus inadequate for input of COPY data.
type copyEditor struct {
	sql  sqlShell
	main editor
	copy editor
}

var _ editor = (*copyEditor)(nil)

func (e *copyEditor) init(
	win, wout, werr *os.File, sqlS sqlShell, maxHistEntries int, histFile string,
) (cleanupFn func(), err error) {
	e.sql = sqlS
	c1, err := e.main.init(win, wout, werr, sqlS, maxHistEntries, histFile)
	if err != nil {
		return c1, err
	}
	c2, err := e.copy.init(win, wout, werr, sqlS, maxHistEntries, histFile)
	cleanupFn = func() {
		c1()
		c2()
	}
	return cleanupFn, err
}

func (e *copyEditor) selected() editor {
	if e.sql.inCopy() {
		return e.copy
	}
	return e.main
}

func (e *copyEditor) errInterrupted() error {
	return e.selected().errInterrupted()
}

func (e *copyEditor) getOutputStream() *os.File {
	return e.selected().getOutputStream()
}

func (e *copyEditor) getLine() (string, error) {
	return e.selected().getLine()
}

func (e *copyEditor) addHistory(line string) error {
	return errors.CombineErrors(
		e.copy.addHistory(line),
		e.main.addHistory(line))
}

func (e *copyEditor) canPrompt() bool {
	return e.main.canPrompt()
}

func (e *copyEditor) setPrompt(prompt string) {
	e.copy.setPrompt(prompt)
	e.main.setPrompt(prompt)
}

func (e *copyEditor) multilineEdit() bool {
	return e.selected().multilineEdit()
}
