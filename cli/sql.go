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

	"github.com/chzyer/readline"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/mattn/go-isatty"
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

	if isatty.IsTerminal(os.Stdout.Fd()) {
		// We only enable history management when the terminal is actually
		// interactive. This saves on memory when e.g. piping a large SQL
		// script through the command-line client.
		userAcct, err := user.Current()
		if err != nil {
			if log.V(2) {
				log.Warningf("cannot retrieve user information: %s", err)
				log.Info("cannot load or save the command-line history")
			}
		} else {
			histFile := filepath.Join(userAcct.HomeDir, ".cockroachdb_history")
			// If histFile exists, readline will try to load them all into
			// memory, or it will create a file. If there is an IO error
			// occurred, just skip it.
			readline.SetHistoryPath(histFile)
		}
	}

	fmt.Print(infoMessage)

	var stmt []string

	for {
		thisPrompt := fullPrompt
		if len(stmt) > 0 {
			thisPrompt = continuePrompt
		}
		l, err := readline.Line(thisPrompt)
		if err != nil {
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

		// We save the history between each statement, This enables
		// reusing history in another SQL shell without closing the
		// current shell.
		//
		// AddHistory will push command into memory and trying to persist
		// to disk(if readline.SetHistoryPath is called).
		// err can be not nil only if it got a IO error while
		// trying to persist.
		if err = readline.AddHistory(strings.Join(stmt, " ")); err != nil {
			log.Warningf("cannot save command-line history: %s", err)
			log.Info("command-line history will not be saved in this session")
			readline.SetHistoryPath("")
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
			fmt.Fprintln(osStderr, err)
			return err
		}

		if len(cols) == 0 {
			// No result selected, inform the user.
			fmt.Fprintln(os.Stdout, "OK")
		} else {
			// Some results selected, inform the user about how much data to expect.
			noun := "rows"
			if len(allRows) == 1 {
				noun = "row"
			}

			fmt.Fprintf(os.Stdout, "%d %s\n", len(allRows), noun)

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
	if !(cliContext.OneShotSQL || len(args) == 0) {
		mustUsage(cmd)
		return errMissingParams
	}

	db, dbURL := makeSQLClient()
	defer func() { _ = db.Close() }()

	if cliContext.OneShotSQL {
		// Single-line sql; run as simple as possible, without noise on stdout.
		return runStatements(db, args)
	}
	return runInteractive(db, dbURL)
}
