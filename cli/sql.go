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

	"golang.org/x/net/context"

	"github.com/chzyer/readline"
	"github.com/cockroachdb/cockroach/sql/parser"
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
	RunE:         maybeDecorateGRPCError(runTerm),
	SilenceUsage: true,
}

// options store the client-side options that a user of the sql cli
// can configure with \set and \unset.
// TODO(knz) For now these are booleans but eventually we may need
// to support arbitrary strings.
var options = map[string]bool{
	// The following defaults apply for the interactive shell.
	// These defaults are overridden for non-interactive shells below.

	// Determines whether to stop the client upon encountering an error.
	"errexit": false,

	// Determines whether to perform client-side syntax checking.
	"check_syntax": true,

	// Determines whether to store normalized syntax in the shell history
	// when check_syntax is set.
	// TODO(knz) this can possibly be set back to false by default when
	// the upstream readline library handles multi-line history entries
	// properly.
	"normalize_history": true,
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
  \! CMD            run an external command and print its results on standard output.
  \| CMD            run an external command and run its output as SQL statements.
  \set [NAME]       set a client-side flag or (without argument) print the current settings.
  \unset NAME       unset a flag.
  \? or "help"      print this help.

More documentation about our SQL dialect is available online:
http://www.cockroachlabs.com/docs/

`)
}

// addHistory persists a line of input to the readline history
// file.
func addHistory(ins *readline.Instance, line string) {
	// ins.SaveHistory will push command into memory and try to
	// persist to disk (if ins's config.HistoryFile is set).  err can
	// be not nil only if it got a IO error while trying to persist.
	if err := ins.SaveHistory(line); err != nil {
		log.Warningf(context.TODO(), "cannot save command-line history: %s", err)
		log.Info(context.TODO(), "command-line history will not be saved in this session")
		cfg := ins.Config.Clone()
		cfg.HistoryFile = ""
		ins.SetConfig(cfg)
	}
}

// handleSet supports the \set client-side command.
func handleSet(args []string) {
	if len(args) > 1 {
		fmt.Fprintf(osStderr, "Invalid command: \\set %s. Try \\? for help.\n", strings.Join(args, " "))
		return
	}
	if len(args) == 0 {
		for k, v := range options {
			fmt.Printf("%s = %v\n", strings.ToUpper(k), v)
		}
		return
	}

	opt := strings.ToLower(args[0])
	options[opt] = true
}

// handleUnset supports the \unset client-side command.
func handleUnset(args []string) {
	if len(args) != 1 {
		fmt.Fprintf(osStderr, "Invalid command: \\unset %s. Try \\? for help.\n", strings.Join(args, " "))
		return
	}
	opt := strings.ToLower(args[0])
	options[opt] = false
}

// handleInputLine looks at a single line of text entered
// by the user at the prompt and decides what to do: either
// run a client-side command, print some help or continue with
// a regular query.
func handleInputLine(
	ins *readline.Instance, stmt *[]string, line string, syntax parser.Syntax,
) (status int, hasSet, isEmpty, endsWithSemi bool) {
	if len(*stmt) == 0 {
		// Special case: first line of multi-line statement.
		// In this case ignore empty lines, and recognize "help" specially.
		switch line {
		case "":
			return cliNextLine, false, true, false
		case "help":
			printCliHelp()
			return cliNextLine, false, true, false
		}

		if len(line) > 0 && line[0] == '\\' {
			// Client-side commands: process locally.

			addHistory(ins, line)

			cmd := strings.Fields(line)
			switch cmd[0] {
			case `\q`:
				return cliExit, false, true, false
			case `\!`:
				return runSyscmd(line), false, true, false
			case `\|`:
				status = pipeSyscmd(stmt, line)
				isEmpty, endsWithSemi, hasSet = isEndOfStatement(syntax, stmt)
				return status, hasSet, isEmpty, endsWithSemi
			case `\`, `\?`:
				printCliHelp()
			case `\set`:
				handleSet(cmd[1:])
			case `\unset`:
				handleUnset(cmd[1:])
			default:
				fmt.Fprintf(osStderr, "Invalid command: %s. Try \\? for help.\n", line)
			}

			if strings.HasPrefix(line, `\d`) {
				// Unrecognized command for now, but we want to be helpful.
				fmt.Fprint(osStderr, "Suggestion: use the SQL SHOW statement to inspect your schema.\n")
			}

			return cliNextLine, false, true, false
		}
	}

	*stmt = append(*stmt, line)
	isEmpty, isEnd, hasSet := isEndOfStatement(syntax, stmt)
	if isEmpty || isEnd {
		status = cliProcessQuery
	} else {
		status = cliNextLine
	}
	return status, hasSet, isEmpty, isEnd
}

func isEndOfStatement(syntax parser.Syntax, stmt *[]string) (isEmpty, isEnd, hasSet bool) {
	fullStmt := strings.Join(*stmt, "\n")
	sc := parser.MakeScanner(fullStmt, syntax)
	isEmpty = true
	var last int
	sc.Tokens(func(t int) {
		isEmpty = false
		last = t
		if t == parser.SET {
			hasSet = true
		}
	})
	return isEmpty, last == ';', hasSet
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

	fmt.Print(cmdOut)
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

var cmdHistFile = envutil.EnvOrDefaultString("COCKROACH_SQL_CLI_HISTORY", ".cockroachdb_history")

// runInteractive runs the SQL client interactively, presenting
// a prompt to the user for each statement.
func runInteractive(conn *sqlConn, config *readline.Config) (exitErr error) {
	fullPrompt, continuePrompt := preparePrompts(conn.url)

	ins, err := readline.NewEx(config)
	if err != nil {
		return err
	}
	defer func() { _ = ins.Close() }()

	if isInteractive {
		// We only enable history management when the terminal is actually
		// interactive. This saves on memory when e.g. piping a large SQL
		// script through the command-line client.
		userAcct, err := user.Current()
		if err != nil {
			if log.V(2) {
				log.Warningf(context.TODO(), "cannot retrieve user information: %s", err)
				log.Info(context.TODO(), "cannot load or save the command-line history")
			}
		} else {
			histFile := filepath.Join(userAcct.HomeDir, cmdHistFile)
			cfg := ins.Config.Clone()
			cfg.HistoryFile = histFile
			cfg.HistorySearchFold = true
			ins.SetConfig(cfg)
		}
		fmt.Print(infoMessage)
	} else {
		// When running non-interactive, by default we want errors to stop
		// further processing and all syntax checking to be done
		// server-side.
		options["errexit"] = true
		options["check_syntax"] = false
	}

	var stmt []string
	syntax := parser.Traditional

	for isFinished := false; !isFinished; {
		if isInteractive {
			thisPrompt := fullPrompt
			if len(stmt) > 0 {
				thisPrompt = continuePrompt
			}
			ins.SetPrompt(thisPrompt)
		}

		l, err := ins.Readline()

		if isInteractive && err == readline.ErrInterrupt && (len(stmt) > 0 || l != "") {
			// In interactive mode, Ctrl+C after the beginning of a
			// statement cancels the current statement entirely; only Ctrl+C
			// at the beginning of a new statement terminates the shell.
			stmt = stmt[:0]
			continue
		}
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

		// Check if this is a request for help or a client-side command.
		// If so, process it directly and skip query processing below.
		status, hasSet, isEmpty, endsWithSemi := handleInputLine(ins, &stmt, l, syntax)
		if status == cliExit {
			break
		}
		if isEmpty || (status == cliNextLine && !isFinished) {
			// Ask for more input unless we reached EOF.
			continue
		}
		if isFinished && len(stmt) == 0 {
			// Exit if we reached EOF and the currently built-up statement is empty.
			break
		}

		// We join the statements back together with newlines in case
		// there is a significant newline inside a string literal.
		fullStmt := strings.TrimRight(strings.Join(stmt, "\n"), " \n\t\f")

		// Ensure the statement is terminated with a semicolon. This
		// catches cases where the last line before EOF was not terminated
		// properly.
		if len(fullStmt) > 0 && !endsWithSemi {
			if strings.TrimSpace(fullStmt) == "" {
				// Only whitespace. Nothing to do really.
				continue
			}

			// That was the last line of input but some statement bits
			// were accumulated. Report an error.
			fmt.Fprintf(osStderr, "missing semicolon at end of statement: %q\n", fullStmt)
			return fmt.Errorf("last statement was not executed")
		}

		// Clear the saved statement.
		stmt = stmt[:0]

		skipStmt := false
		exitIfErr, _ := options["errexit"]

		// If client-side syntax checking is enabled, parse the statement
		// before sending it to the server. If it fails to parse, inform
		// the user then stop processing if the `errexit` option is set.
		// Otherwise, pretty-print the resulting AST to populate the
		// history.
		normalizedStmt := fullStmt
		if doCheck, _ := options["check_syntax"]; doCheck {
			var parser parser.Parser
			stmts, err := parser.Parse(fullStmt, syntax)
			if err != nil {
				fmt.Fprintf(osStderr, "statement not sent to server: %v", err)
				exitErr = err
				if exitIfErr {
					break
				}
				skipStmt = true
			} else if normalizeHistory, _ := options["normalize_history"]; normalizeHistory {
				normalizedStmt = stmts.String() + ";"
			}
		}

		if isInteractive {
			// We save the history between each statement, This enables
			// reusing history in another SQL shell without closing the
			// current shell.
			addHistory(ins, normalizedStmt)
		}

		if skipStmt {
			// If a syntax error was detected client-side, don't try to go
			// to the server and simply try again.
			continue
		}

		if exitErr = runQueryAndFormatResults(conn, os.Stdout, makeQuery(fullStmt), cliCtx.prettyFmt); exitErr != nil {
			fmt.Fprintln(osStderr, exitErr)
		}

		if exitErr != nil && exitIfErr {
			break
		}

		if hasSet {
			newSyntax, err := getSyntax(conn)
			if err != nil {
				fmt.Fprintf(osStderr, "could not get session syntax: %s", err)
			} else {
				syntax = newSyntax
			}
		}
	}

	return exitErr
}

func getSyntax(conn *sqlConn) (parser.Syntax, error) {
	_, rows, _, err := runQuery(conn, makeQuery("SHOW SYNTAX"), false)
	if err != nil {
		return 0, err
	}
	switch rows[0][0] {
	case parser.Traditional.String():
		return parser.Traditional, nil
	case parser.Modern.String():
		return parser.Modern, nil
	}
	return 0, fmt.Errorf("unknown syntax: %s", rows[0][0])
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
		return cmd.Usage()
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
	// Use the same as the default global readline config.
	conf := readline.Config{
		DisableAutoSaveHistory: true,
	}
	return runInteractive(conn, &conf)
}
