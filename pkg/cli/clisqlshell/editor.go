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

import "os"

// editor is the interface between the shell and a line editor.
type editor interface {
	init(win, wout, werr *os.File, sqlS sqlShell, maxHistEntries int, histFile string) (cleanupFn func(), err error)
	errInterrupted() error
	getOutputStream() *os.File
	getLine() (string, error)
	addHistory(line string) error
	saveHistory() error
	canPrompt() bool
	setPrompt(prompt string)
	multilineEdit() bool
}

type sqlShell interface {
	enableDebug() bool
	inCopy() bool
	runShowCompletions(sql string, offset int) (rows [][]string, err error)
	serverSideParse(sql string) (string, error)
	reflow(
		allText bool, currentText string, targetWidth int,
	) (changed bool, newText string, info string)
}

// getEditor instantiates an editor compatible with the current configuration.
func getEditor(useEditor bool, displayPrompt bool) editor {
	if !useEditor {
		return &bufioReader{displayPrompt: displayPrompt}
	}
	return &bimodalEditor{
		main: &bubblineReader{},
		copy: &bufioReader{displayPrompt: displayPrompt},
	}
}
