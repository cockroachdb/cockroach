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
Open a sql shell running against a cockroach database.
`,
	RunE:          runTerm,
	SilenceErrors: true,
	SilenceUsage:  true,
}

// runInteractive runs the SQL client interactively, presenting
// a prompt to the user for each statement.
func runInteractive(db *sql.DB, dbURL string) (exitErr error) {
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
		// is the important part.
		fullPrompt = fmt.Sprintf("%s@%s", parsedURL.User, parsedURL.Host)
	}

	if len(fullPrompt) == 0 {
		fullPrompt = " "
	}
	continuePrompt := strings.Repeat(" ", len(fullPrompt)-1) + "-"

	fullPrompt += "> "
	continuePrompt += "> "

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
				exitErr = err
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
			fmt.Fprintln(osStderr, err)
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
