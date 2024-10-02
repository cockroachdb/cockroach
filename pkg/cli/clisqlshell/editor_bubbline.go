// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/bubbles/cursor"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/knz/bubbline"
	"github.com/knz/bubbline/editline"
)

// bubblineReader implements the editor interface.
type bubblineReader struct {
	wout *os.File
	sql  sqlShell
	ins  *bubbline.Editor
}

func (b *bubblineReader) init(
	win, wout, werr *os.File, sqlS sqlShell, maxHistEntries int, histFile string,
) (cleanupFn func(), err error) {
	b.wout = wout
	b.sql = sqlS
	b.ins = bubbline.New()
	cleanupFn = func() { b.ins.Close() }

	b.ins.MaxHistorySize = maxHistEntries
	if sqlS.enableDebug() {
		b.ins.SetDebugEnabled(true)
	}
	b.ins.Reflow = sqlS.reflow
	b.ins.AutoComplete = b.getCompletions
	b.ins.CheckInputComplete = b.checkInputComplete
	b.ins.SetExternalEditorEnabled(true, "sql")
	b.ins.NextPrompt = "-> "

	// We override the style because at this time we don't know how to
	// choose a color theme that works on all our user's terminals.
	prevSiStyle := b.ins.FocusedStyle.SearchInput
	b.ins.FocusedStyle = editline.Style{SearchInput: prevSiStyle}
	b.ins.BlurredStyle = editline.Style{}

	// We override the cursor style (default is a blinking cursor) both
	// for backward-compatibility with the libedit-based editor, because
	// a blinking cursor makes mouse-based copy-pasting more difficult
	// on some terminals (each blink cancels the mouse selection), and
	// also because blinking cursors make our TCL test results harder to
	// read.
	//
	// If this default is ever changed, consider keeping an override
	// specifically for the benefit of tests.
	b.ins.CursorMode = cursor.CursorStatic

	if histFile != "" {
		err = b.ins.LoadHistory(histFile)
		if err != nil {
			fmt.Fprintf(werr, "warning: cannot load the command-line history (file corrupted?): %v\n", err)
			fmt.Fprintf(werr, "note: the history file will be cleared upon first entry\n")
		}
		// SetAutoSaveHistory() does two things:
		// - it preserves the name of the history file, for use
		//   by the final SaveHistory() call.
		// - it decides whether to save the history to file upon
		//   every new command.
		// We disable the latter, since a history file can grow somewhat
		// large and we don't want the excess I/O latency to be interleaved
		// in-between every command.
		b.ins.SetAutoSaveHistory(histFile, false)
		prevCleanup := cleanupFn
		cleanupFn = func() {
			if err := b.ins.SaveHistory(); err != nil {
				fmt.Fprintf(werr, "warning: cannot save command-line history: %v\n", err)
			}
			prevCleanup()
		}
	}

	return cleanupFn, nil
}

func (b *bubblineReader) errInterrupted() error {
	return bubbline.ErrInterrupted
}

func (b *bubblineReader) getOutputStream() *os.File {
	return b.wout
}

func (b *bubblineReader) addHistory(line string) error {
	return b.ins.AddHistory(line)
}

func (b *bubblineReader) saveHistory() error {
	return b.ins.SaveHistory()
}

func (b *bubblineReader) canPrompt() bool {
	return true
}

func (b *bubblineReader) multilineEdit() bool {
	return false
}

func (b *bubblineReader) setPrompt(prompt string) {
	b.ins.Prompt = prompt
}

func (b *bubblineReader) getLine() (string, error) {
	// bubbline's GetLine takes care of handling the newline from the input, so
	// we don't need to check for one or add one if it's missing.
	l, err := b.ins.GetLine()
	if errors.Is(err, bubbline.ErrTerminated) {
		// Bubbline returns this go error when it sees SIGTERM. Convert it
		// back into a signal, so that the process exit code matches.
		if err := sysutil.TerminateSelf(); err != nil {
			return l, err
		}
		select {}
	}
	return l, err
}

func (b *bubblineReader) checkInputComplete(v [][]rune, line, col int) bool {
	if b.sql.inCopy() {
		return true
	}
	return checkInputComplete(v, line, col)
}

func checkInputComplete(v [][]rune, line, col int) bool {
	// The input is complete in either of the following cases:
	// - we're in COPY mode (always 1 line at a time);
	//   (see the checkInputComplete method above)
	// OR
	// - there's just one line of input; AND EITHER:
	//   - the current line of input starts with `\` (client-side command);
	//   - the input is "quit", "exit" or "help";
	// OR:
	// - or the last token in the input is a semicolon.
	//
	// These cases are exercised in TestCheckInputComplete.

	clientSideCommand := len(v) > 0 && len(v[0]) > 0 && v[0][0] == '\\'
	if clientSideCommand {
		return true
	}
	if len(v) == 1 {
		inputLine := strings.TrimSpace(string(v[0]))
		if inputLine == "" || inputLine == "exit" || inputLine == "help" || inputLine == "quit" {
			return true
		}
	}

	// A possibly longer SQL statement. We need to tokenize, which means we need
	// all the input as a string.
	var buf strings.Builder
	for i := 0; i < len(v); i++ {
		buf.WriteString(string(v[i]))
		buf.WriteByte('\n')
	}
	lastTok, ok := scanner.LastLexicalToken(buf.String())
	if !ok {
		return false
	}
	endOfStmt := isEndOfStatement(lastTok)
	return endOfStmt
}
