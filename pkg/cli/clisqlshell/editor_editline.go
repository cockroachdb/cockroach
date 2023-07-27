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
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/errors"
	readline "github.com/knz/go-libedit"
)

// editlineReader implements the editor interface.
type editlineReader struct {
	wout   *os.File
	sql    sqlShell
	prompt string
	ins    readline.EditLine
}

var _ editor = (*editlineReader)(nil)

func (b *editlineReader) init(
	win, wout, werr *os.File, sqlS sqlShell, maxHistEntries int, histFile string,
) (cleanupFn func(), err error) {
	cleanupFn = func() {}

	b.ins, err = readline.InitFiles("cockroach",
		true /* wideChars */, win, wout, werr)
	if errors.Is(err, readline.ErrWidecharNotSupported) {
		fmt.Fprintln(werr, "warning: wide character support disabled")
		b.ins, err = readline.InitFiles("cockroach",
			false, win, wout, werr)
	}
	if err != nil {
		return cleanupFn, err
	}
	cleanupFn = func() { b.ins.Close() }
	b.wout = b.ins.Stdout()
	b.sql = sqlS
	b.ins.SetCompleter(b)

	// If the user has used bind -v or bind -l in their ~/.editrc,
	// this will reset the standard bindings. However we really
	// want in this shell that Ctrl+C, tab, Ctrl+Z and Ctrl+R
	// always have the same meaning.  So reload these bindings
	// explicitly no matter what ~/.editrc may have changed.
	b.ins.RebindControlKeys()

	if err := b.ins.UseHistory(maxHistEntries, true /*dedup*/); err != nil {
		fmt.Fprintf(werr, "warning: cannot enable history: %v\n ", err)
	} else if histFile != "" {
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

func (b *editlineReader) errInterrupted() error {
	return readline.ErrInterrupted
}

func (b *editlineReader) getOutputStream() *os.File {
	return b.wout
}

func (b *editlineReader) addHistory(line string) error {
	return b.ins.AddHistory(line)
}

func (b *editlineReader) saveHistory() error {
	return b.ins.SaveHistory()
}

func (b *editlineReader) canPrompt() bool {
	return true
}

func (b *editlineReader) setPrompt(prompt string) {
	b.prompt = prompt
	b.ins.SetLeftPrompt(prompt)
}

func (b *editlineReader) multilineEdit() bool {
	return false
}

func (b *editlineReader) GetCompletions(word string) []string {
	if b.sql.inCopy() {
		return []string{word + "\t"}
	}
	sql, offset := b.ins.GetLineInfo()
	if !strings.HasSuffix(sql, "??") {
		rows, err := b.sql.runShowCompletions(sql, offset)
		if err != nil {
			clierror.OutputError(b.wout, err, true /*showSeverity*/, false /*verbose*/)
		}

		var completions []string
		for _, row := range rows {
			completions = append(completions, row[0])
		}

		return completions
	}

	helpText, err := b.sql.serverSideParse(sql)
	if helpText != "" {
		// We have a completion suggestion. Use that.
		fmt.Fprintf(b.wout, "\nSuggestion:\n%s\n", helpText)
	} else if err != nil {
		// Some other error. Display it.
		fmt.Fprintln(b.wout)
		clierror.OutputError(b.wout, err, true /*showSeverity*/, false /*verbose*/)
	}

	// After a suggestion or error, redisplay the prompt and current entry.
	fmt.Fprint(b.wout, b.prompt, sql)
	return nil
}

func (b *editlineReader) getLine() (string, error) {
	l, err := b.ins.GetLine()
	if len(l) > 0 && l[len(l)-1] == '\n' {
		// Strip the final newline.
		l = l[:len(l)-1]
	} else {
		// There was no newline at the end of the input
		// (e.g. Ctrl+C was entered). Force one.
		fmt.Fprintln(b.wout)
	}
	return l, err
}
