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
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/log"
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
	RunE:         runTerm,
	SilenceUsage: true,
}

const (
	cliNextLine     = 0 // Done with this line, ask for another line.
	cliExit         = 1 // User requested to exit.
	cliProcessQuery = 2 // Regular query to send to the server.
)

// printCliHelp prints a short inline help about the CLI.
func printCliHelp() {
	fmt.Print(`You are using 'cockroach sql', CockroachDB's lightweight SQL client.
Type: \q to exit (Ctrl+C/Ctrl+D also supported)
      \! to run an external command and print its results on standard output.
      \| to run an external command and run its output as SQL statements.
      \? or "help" to print this help.

More documentation about our SQL dialect is available online:
http://www.cockroachlabs.com/docs/

`)
}

// addHistory persists a line of input to the readline history
// file.
func addHistory(line string) {
	// readline.AddHistory will push command into memory and try to
	// persist to disk (if readline.SetHistoryPath was called).  err can
	// be not nil only if it got a IO error while trying to persist.
	if err := readline.AddHistory(line); err != nil {
		log.Warningf("cannot save command-line history: %s", err)
		log.Info("command-line history will not be saved in this session")
		readline.SetHistoryPath("")
	}
}

// handleInputLine looks at a single line of text entered
// by the user at the prompt and decides what to do: either
// run a client-side command, print some help or continue with
// a regular query.
func handleInputLine(stmt *[]string, line string) int {
	if len(*stmt) == 0 {
		// Special case: first line of multi-line statement.
		// In this case ignore empty lines, and recognize "help" specially.
		switch line {
		case "":
			return cliNextLine
		case "help":
			printCliHelp()
			return cliNextLine
		}
	}

	if len(line) > 0 && line[0] == '\\' {
		// Client-side commands: process locally.

		addHistory(line)

		cmd := strings.Fields(line)
		switch cmd[0] {
		case `\q`:
			return cliExit
		case `\!`:
			return runSyscmd(line)
		case `\|`:
			return pipeSyscmd(stmt, line)
		case `\`, `\?`:
			printCliHelp()
		default:
			fmt.Fprintf(osStderr, "Invalid command: %s. Try \\? for help.\n", line)
		}

		if strings.HasPrefix(line, `\d`) {
			// Unrecognized command for now, but we want to be helpful.
			fmt.Fprint(osStderr, "Suggestion: use the SQL SHOW statement to inspect your schema.\n")
		}

		return cliNextLine
	}

	*stmt = append(*stmt, line)

	if !strings.HasSuffix(line, ";") {
		// Not yet finished with multi-line statement.
		return cliNextLine
	}

	return cliProcessQuery
}

// execSyscmd executes system commands.
func execSyscmd(command string) (string, error) {
	var cmd *exec.Cmd

	shell := envutil.GetShellCommand(command)
	cmd = exec.Command(shell[0], shell[1:]...)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("error in external command: %s", err)
	}

	return out.String(), nil
}

// runSyscmd runs system commands on the interactive CLI.
func runSyscmd(line string) int {
	command := line[2:]
	if command == "" {
		fmt.Fprintf(osStderr, "Usage:\n  \\! [command]\n")
		return cliNextLine
	}

	cmdOut, err := execSyscmd(command)
	if err != nil {
		fmt.Fprintf(osStderr, "command failed: %s\n", err)
		return cliNextLine
	}

	fmt.Printf(cmdOut)
	return cliNextLine
}

// pipeSyscmd executes system commands and pipe the output into the current SQL.
func pipeSyscmd(stmt *[]string, line string) int {
	command := line[2:]
	if command == "" {
		fmt.Fprintf(osStderr, "Usage:\n  \\| [command]\n")
		return cliNextLine
	}

	cmdOut, err := execSyscmd(command)
	if err != nil {
		fmt.Fprintf(osStderr, "command failed: %s\n", err)
		return cliNextLine
	}

	*stmt = append(*stmt, cmdOut)
	return cliProcessQuery
}

// preparePrompts computes a full and short prompt for the interactive
// CLI.
func preparePrompts(dbURL string) (fullPrompt string, continuePrompt string) {
	// Default prompt is part of the connection URL. eg: "marc@localhost>"
	// continued statement prompt is: "        -> "
	fullPrompt = dbURL
	if parsedURL, err := url.Parse(dbURL); err == nil {
		// If parsing fails, we keep the entire URL. The Open call succeeded, and that
		// is the important part.
		fullPrompt = fmt.Sprintf("%s@%s", parsedURL.User, parsedURL.Host)
	}

	if len(fullPrompt) == 0 {
		fullPrompt = " "
	}

	continuePrompt = strings.Repeat(" ", len(fullPrompt)-1) + "-"

	fullPrompt += "> "
	continuePrompt += "> "

	return fullPrompt, continuePrompt
}

// runInteractive runs the SQL client interactively, presenting
// a prompt to the user for each statement.
func runInteractive(conn *sqlConn) (exitErr error) {
	fullPrompt, continuePrompt := preparePrompts(conn.url)

	if isInteractive {
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
			readline.SetHistoryPath(histFile)
		}
	}

	if isInteractive {
		fmt.Print(infoMessage)
	}

	var stmt []string

	for isFinished := false; !isFinished; {
		thisPrompt := fullPrompt
		if !isInteractive {
			thisPrompt = ""
		} else if len(stmt) > 0 {
			thisPrompt = continuePrompt
		}
		l, err := readline.Line(thisPrompt)

		if !isInteractive && err == io.EOF {
			// In non-interactive mode, we want EOF to finish the last statement.
			err = nil
			isFinished = true
		}
		if err == readline.ErrInterrupt || err == io.EOF {
			break
		} else if err != nil {
			fmt.Fprintf(osStderr, "input error: %s\n", err)
			return err
		}

		tl := strings.TrimSpace(l)

		// Check if this is a request for help or a client-side command.
		// If so, process it directly and skip query processing below.
		status := handleInputLine(&stmt, tl)
		if status == cliExit {
			break
		} else if status == cliNextLine && !isFinished {
			// Ask for more input unless we reached EOF.
			continue
		} else if isFinished && len(stmt) == 0 {
			// Exit if we reached EOF and the currently built-up statement is empty.
			break
		}

		// We join the statements back together with newlines in case
		// there is a significant newline inside a string literal. However
		// we join with spaces for keeping history, because otherwise a
		// history recall will only pull one line from a multi-line
		// statement.
		fullStmt := strings.Join(stmt, "\n")

		if isInteractive {
			// We save the history between each statement, This enables
			// reusing history in another SQL shell without closing the
			// current shell.
			addHistory(strings.Join(stmt, " "))
		}

		if exitErr = runQueryAndFormatResults(conn, os.Stdout, makeQuery(fullStmt), cliCtx.prettyFmt); exitErr != nil {
			fmt.Fprintln(osStderr, exitErr)
		}

		// Clear the saved statement.
		stmt = stmt[:0]
	}

	return exitErr
}

// runOneStatement executes one statement and terminates
// on error.
func runStatements(conn *sqlConn, stmts []string, pretty bool) error {
	for _, stmt := range stmts {
		if err := runQueryAndFormatResults(conn, os.Stdout, makeQuery(stmt), pretty); err != nil {
			return err
		}
	}
	return nil
}

func runTerm(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		mustUsage(cmd)
		return errMissingParams
	}

	conn, err := makeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	if len(sqlCtx.execStmts) > 0 {
		// Single-line sql; run as simple as possible, without noise on stdout.
		return runStatements(conn, sqlCtx.execStmts, cliCtx.prettyFmt)
	}
	return runInteractive(conn)
}
