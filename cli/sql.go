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
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/GeertJohan/go.linenoise"
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
Open a sql shell running against a cockroach database.
`,
	RunE:          runTerm,
	SilenceErrors: true,
	SilenceUsage:  true,
}

// runInteractive runs the SQL client interactively, presenting
// a prompt to the user for each statement.
func runInteractive(db *sql.DB, dbURL string) (exitErr error) {
	var stmt []string
	var l string
	var err error

	// Define the prompt.

	// Default prompt is part of the connection URL. eg: "marc@localhost>"
	// continued statement prompt is: "        -> "
	fullPrompt := dbURL
	if parsedURL, err := url.Parse(dbURL); err == nil {
		// If parsing fails, we keep the entire URL. The Open call succeeded, and that
		// is the important part.
		fullPrompt = fmt.Sprintf("%s@%s", parsedURL.User, parsedURL.Host)
	}

	if len(fullPrompt) == 0 {
		fullPrompt = " "
	}
	continuePrompt := strings.Repeat(" ", len(fullPrompt)-1) + "-"

	fullPrompt += "> "
	continuePrompt += "> "

	histFile := ""
	userAcct, err := user.Current()
	if err == nil {
		histFile := filepath.Join(userAcct.HomeDir, ".cockroachdb_history")
		_ = linenoise.LoadHistory(histFile)
	}

	fmt.Print(infoMessage)

	for {
		if len(stmt) == 0 {
			l, err = linenoise.Line(fullPrompt)
		} else {
			l, err = linenoise.Line(continuePrompt)
		}
		if err != nil {
			// Linenoise only returns err != nil, a string, when the input
			// was terminated with Ctrl+C or Ctrl+D. This is not an error,
			// and merely indicates end of input.  io.EOF is never returned.
			break
		}

		tl := strings.TrimSpace(l)
		if len(stmt) == 0 && len(tl) == 0 {
			// Empty line at beginning, simply continue.
			// However, we don't simply continue after the first line since
			// we may be in the middle of a string literal where empty lines
			// may be significant.
			continue
		}

		stmt = append(stmt, l)

		// See if we have a semicolon at the end of the line (ignoring
		// trailing whitespace).
		if !strings.HasSuffix(tl, ";") {
			// No semicolon: read some more.
			continue
		}

		// We join the statements back together with newlines in case
		// there is a significant newline inside a string literal. However
		// we join with spaces for keeping history, because otherwise a
		// history recall will only pull one line from a multi-line
		// statement.
		fullStmt := strings.Join(stmt, "\n")
		_ = linenoise.AddHistory(strings.Join(stmt, " "))
		if histFile != "" {
			// We save the history between each statement: in case something
			// bad happens and the client panics we want to keep the history
			// to reproduce the issue quickly.
			_ = linenoise.SaveHistory(histFile)
		}

		if exitErr = runPrettyQuery(db, os.Stdout, fullStmt); exitErr != nil {
			fmt.Fprintln(osStderr, exitErr)
		}

		// Clear the saved statement.
		stmt = stmt[:0]
	}

	return exitErr
}

// runOneStatement executes one statement and terminates
// on error.
func runStatements(db *sql.DB, stmts []string) error {
	for _, stmt := range stmts {
		fullStmt := stmt + "\n"
		cols, allRows, err := runQuery(db, fullStmt)
		if err != nil {
			return err
		}

		if len(cols) == 0 {
			// No result selected, inform the user.
			fmt.Fprintln(os.Stdout, "OK")
		} else {
			// Some results selected, inform the user about how much data to expect.
			fmt.Fprintf(os.Stdout, "%d row%s\n", len(allRows),
				map[bool]string{true: "", false: "s"}[len(allRows) == 1])

			// Then print the results themselves.
			fmt.Fprintln(os.Stdout, strings.Join(cols, "\t"))
			for _, row := range allRows {
				fmt.Fprintln(os.Stdout, strings.Join(row, "\t"))
			}
		}
	}
	return nil
}

func runTerm(cmd *cobra.Command, args []string) error {
	if !(context.OneShotSQL || len(args) == 0) {
		mustUsage(cmd)
		return errMissingParams
	}

	db, dbURL := makeSQLClient()
	defer func() { _ = db.Close() }()

	if context.OneShotSQL {
		// Single-line sql; run as simple as possible, without noise on stdout.
		return runStatements(db, args)
	}
	return runInteractive(db, dbURL)
}
