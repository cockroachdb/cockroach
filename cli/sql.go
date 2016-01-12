// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc berhault (marc@cockroachlabs.com)

package cli

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

const (
	infoMessage = `# Welcome to the cockroach SQL interface.
# All statements must be terminated by a semicolon.
# To exit: CTRL + D.
`
)

// sqlShellCmd opens a sql shell.
var sqlShellCmd = &cobra.Command{
	Use:   "sql [options]",
	Short: "open a sql shell",
	Long: `
Open a sql shell running against the cockroach database at --addr.
`,
	Run: runTerm, // TODO(tschottdorf): should be able to return err code when reading from stdin
}

func runTerm(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		mustUsage(cmd)
		return
	}

	db, dbURL := makeSQLClient()
	defer func() { _ = db.Close() }()

	liner := liner.NewLiner()
	defer func() {
		_ = liner.Close()
	}()

	fmt.Print(infoMessage)

	// Default prompt is part of the connection URL. eg: "marc@localhost>"
	// continued statement prompt is: "        -> "
	fullPrompt := dbURL
	if parsedURL, err := url.Parse(dbURL); err == nil {
		// If parsing fails, we keep the entire URL. The Open call succeeded, and that
		// the important part.
		fullPrompt = fmt.Sprintf("%s@%s", parsedURL.User, parsedURL.Host)
	}

	if len(fullPrompt) == 0 {
		fullPrompt = " "
	}
	continuePrompt := strings.Repeat(" ", len(fullPrompt)-1) + "-"

	fullPrompt += "> "
	continuePrompt += "> "

	// TODO(marc): allow passing statements on the command line,
	// or specifying a flag. This would make testing much simpler.
	// TODO(marc): detect if we're actually on a terminal. If not,
	// we may want to repeat the statements on stdout.
	// TODO(marc): add test.
	var stmt []string
	var l string
	var err error
	for {
		if len(stmt) == 0 {
			l, err = liner.Prompt(fullPrompt)
		} else {
			l, err = liner.Prompt(continuePrompt)
		}
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(osStderr, "Input error: %s\n", err)
			}
			break
		}

		stmt = append(stmt, l)

		// See if we have a semicolon at the end of the line (ignoring
		// trailing whitespace).
		if !strings.HasSuffix(strings.TrimSpace(l), ";") {
			// No semicolon: read some more.
			continue
		}

		// We always insert a newline when continuing statements.
		// However, it causes problems with lines in the middle of:
		// - qualified names (eg: database.<newline>table)
		// - quoted strings (eg: 'foo<newline>bar')
		// This also makes the history replay horrible.
		// mysql replaces newlines with spaces in the history, which works
		// because string concatenation with newlines can also be done with spaces.
		// postgres keeps the line intact in the history.
		fullStmt := strings.Join(stmt, "\n")
		liner.AppendHistory(fullStmt)

		if err := runPrettyQuery(db, os.Stdout, fullStmt); err != nil {
			fmt.Println(err)
		}

		// Clear the saved statement.
		stmt = stmt[:0]
	}
}
