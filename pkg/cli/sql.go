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
	"errors"
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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// cliState defines the current state of the CLI during
// command-line processing.
type cliState struct {
	conn *sqlConn
	ins  *readline.Instance

	// Options
	//
	// Determines whether to stop the client upon encountering an error.
	errExit bool
	// Determines whether to perform client-side syntax checking.
	checkSyntax bool
	// Determines whether to store normalized syntax in the shell history
	// when check_syntax is set.
	// TODO(knz) this can possibly be set back to false by default when
	// the upstream readline library handles multi-line history entries
	// properly.
	normalizeHistory bool
	// The prompt at the beginning of a multi-line entry.
	fullPrompt string
	// The prompt on a continuation line in a multi-line entry.
	continuePrompt string

	// State
	//
	// lastInputLine is the last valid line obtained from readline.
	lastInputLine string
	// atEOF indicates whether the last call to readline signaled EOF on input.
	atEOF bool

	// partialLines is the array of lines accumulated so far in a
	// multi-line entry. When syntax checking is enabled, partialLines
	// also gets rewritten so that all statements parsed successfully so
	// far have their own, single entry in partialLines (i.e. statements
	// spanning multiple lines are grouped together in one partialLines
	// entry).
	partialLines []string

	// partialStmtsLen represents the number of semicolon-separated
	// statements in `partialLines` parsed successfully so far. It grows
	// larger than zero whenever 1) syntax checking is enabled and 2)
	// multi-statement (as opposed to multi-line) entry starts,
	// i.e. when the shell decides to continue inputting statements even
	// after a full statement followed by a semicolon was read
	// successfully. This is currently used for multi-line transaction
	// editing.
	partialStmtsLen int

	// concatLines is the concatenation of partialLines, computed during
	// doCheckStatement and then reused in doRunStatement().
	concatLines string

	// querySyntax determines whether to query the current syntax mode
	// from the server before starting to read the next line of input.
	querySyntax bool
	// syntax determines the current syntax to use for scanning and parsing
	// new lines of input.
	syntax parser.Syntax

	// exitErr defines the error to report to the user upon termination.
	// This can carry over from one line of input to another. For
	// example in the interactive shell, a statement causing a SQL
	// error, followed by lines of whitespace or SQL comments, followed
	// by Ctrl+D, causes the shell to terminate with an error --
	// reporting the status of the last valid SQL statement executed.
	exitErr error
}

// cliStateEnum drives the CLI state machine in runInteractive().
type cliStateEnum int

const (
	cliStart cliStateEnum = iota
	cliStop

	// Querying the server for the current syntax prior to starting
	// input.
	cliQuerySyntax

	// Just before reading the first line of a potentially multi-line
	// statement.
	cliStartLine

	// Just before reading the 2nd line or after in a multi-line
	// statement.
	cliContinueLine

	// Actually reading the input and checking for input errors.
	cliReadLine

	// Determine to do with the newly input line.
	cliDecidePath

	// Process the first line of a (possibly multi-line) entry.
	cliProcessFirstLine

	// Check and handle for client-side commands.
	cliHandleCliCmd

	// Concatenate the inputs so far, discard entry made only of whitespace
	// and/or comments, and check for semicolons.
	cliPrepareStatementLine

	// Perform syntax validation if enabled with check_syntax,
	// and possibly trigger entry for multi-line transactions.
	cliCheckStatement

	// Actually run the SQL buffered so far.
	cliRunStatement
)

// printCliHelp prints a short inline help about the CLI.
func printCliHelp() {
	fmt.Print(`You are using 'cockroach sql', CockroachDB's lightweight SQL client.
Type: \q to exit (Ctrl+C/Ctrl+D also supported)
  \! CMD            run an external command and print its results on standard output.
  \| CMD            run an external command and run its output as SQL statements.
  \set [NAME]       set a client-side flag or (without argument) print the current settings.
  \unset NAME       unset a flag.
  \show             during a multi-line statement or transaction, show the SQL entered so far.
  \? or "help"      print this help.

More documentation about our SQL dialect is available online:
http://www.cockroachlabs.com/docs/

`)
}

// addHistory persists a line of input to the readline history
// file.
func (c *cliState) addHistory(line string) {
	if !isInteractive {
		return
	}

	// ins.SaveHistory will push command into memory and try to
	// persist to disk (if ins's config.HistoryFile is set).  err can
	// be not nil only if it got a IO error while trying to persist.
	if err := c.ins.SaveHistory(line); err != nil {
		log.Warningf(context.TODO(), "cannot save command-line history: %s", err)
		log.Info(context.TODO(), "command-line history will not be saved in this session")
		cfg := c.ins.Config.Clone()
		cfg.HistoryFile = ""
		c.ins.SetConfig(cfg)
	}
}

func (c *cliState) invalidSyntax(
	nextState cliStateEnum, format string, args ...interface{},
) cliStateEnum {
	fmt.Fprint(osStderr, "invalid syntax: ")
	fmt.Fprintf(osStderr, format, args...)
	fmt.Fprintln(osStderr)
	c.exitErr = errInvalidSyntax
	return nextState
}

func (c *cliState) invalidOption(nextState cliStateEnum, opt string) cliStateEnum {
	fmt.Fprintf(osStderr, "option not recognized: %s\n", opt)
	return nextState
}

func (c *cliState) invalidOptionChange(nextState cliStateEnum, opt string) cliStateEnum {
	fmt.Fprintf(osStderr, "cannot change option during multi-line editing: %s\n", opt)
	return nextState
}

// handleSet supports the \set client-side command.
func (c *cliState) handleSet(args []string, nextState, errState cliStateEnum) cliStateEnum {
	if len(args) != 1 {
		return c.invalidSyntax(errState, `\set %s. Try \? for help.`, strings.Join(args, " "))
	}
	opt := strings.ToLower(args[0])
	switch opt {
	case `errexit`:
		c.errExit = true
	case `check_syntax`:
		if len(c.partialLines) > 0 {
			return c.invalidOptionChange(errState, opt)
		}
		c.checkSyntax = true
	case `normalize_history`:
		if len(c.partialLines) > 0 {
			return c.invalidOptionChange(errState, opt)
		}
		c.normalizeHistory = true
	default:
		return c.invalidOption(errState, opt)
	}
	return nextState
}

// handleUnset supports the \unset client-side command.
func (c *cliState) handleUnset(args []string, nextState, errState cliStateEnum) cliStateEnum {
	if len(args) != 1 {
		return c.invalidSyntax(errState, `\unset %s. Try \? for help.`, strings.Join(args, " "))
	}
	opt := strings.ToLower(args[0])
	switch opt {
	case `errexit`:
		c.errExit = false
	case `check_syntax`:
		if len(c.partialLines) > 0 {
			return c.invalidOptionChange(errState, opt)
		}
		c.checkSyntax = false
	case `normalize_history`:
		if len(c.partialLines) > 0 {
			return c.invalidOptionChange(errState, opt)
		}
		c.normalizeHistory = false
	default:
		return c.invalidOption(errState, opt)
	}
	return nextState
}

func isEndOfStatement(fullStmt string, syntax parser.Syntax) (isEmpty, isEnd, hasSet bool) {
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

var errInvalidSyntax = errors.New("invalid syntax")

// runSyscmd runs system commands on the interactive CLI.
func (c *cliState) runSyscmd(line string, nextState, errState cliStateEnum) cliStateEnum {
	command := strings.Trim(line[2:], " \r\n\t\f")
	if command == "" {
		fmt.Fprintf(osStderr, "Usage:\n  \\! [command]\n")
		c.exitErr = errInvalidSyntax
		return errState
	}

	cmdOut, err := execSyscmd(command)
	if err != nil {
		fmt.Fprintf(osStderr, "command failed: %s\n", err)
		c.exitErr = err
		return errState
	}

	fmt.Print(cmdOut)
	return nextState
}

// pipeSyscmd executes system commands and pipe the output into the current SQL.
func (c *cliState) pipeSyscmd(line string, nextState, errState cliStateEnum) cliStateEnum {
	command := strings.Trim(line[2:], " \n\r\t\f")
	if command == "" {
		fmt.Fprintf(osStderr, "Usage:\n  \\| [command]\n")
		c.exitErr = errInvalidSyntax
		return errState
	}

	cmdOut, err := execSyscmd(command)
	if err != nil {
		fmt.Fprintf(osStderr, "command failed: %s\n", err)
		c.exitErr = err
		return errState
	}

	c.lastInputLine = cmdOut
	return nextState
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
		fullPrompt = fmt.Sprintf("%s@%s", parsedURL.User.Username(), parsedURL.Host)
	}

	if len(fullPrompt) == 0 {
		fullPrompt = " "
	}

	continuePrompt = strings.Repeat(" ", len(fullPrompt)-1) + "-"

	fullPrompt += "> "
	continuePrompt += "> "

	return fullPrompt, continuePrompt
}

// endsWithIncompleteTxn returns true if and only if its
// argument ends with an incomplete transaction prefix (BEGIN without
// ROLLBACK/COMMIT).
func endsWithIncompleteTxn(stmts parser.StatementList) bool {
	txnStarted := false
	for _, stmt := range stmts {
		switch stmt.(type) {
		case *parser.BeginTransaction:
			txnStarted = true
		case *parser.CommitTransaction, *parser.RollbackTransaction:
			txnStarted = false
		}
	}
	return txnStarted
}

var cmdHistFile = envutil.EnvOrDefaultString("COCKROACH_SQL_CLI_HISTORY", ".cockroachdb_history")

func (c *cliState) doStart(nextState cliStateEnum) cliStateEnum {
	// Common initialization.
	c.syntax = parser.Traditional
	c.querySyntax = true
	c.partialLines = []string{}

	if isInteractive {
		c.fullPrompt, c.continuePrompt = preparePrompts(c.conn.url)

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
			cfg := c.ins.Config.Clone()
			cfg.HistoryFile = histFile
			cfg.HistorySearchFold = true
			c.ins.SetConfig(cfg)
		}

		// The user only gets to see the info screen on interactive session.
		fmt.Print(infoMessage)

		c.checkSyntax = true
		c.normalizeHistory = true
		c.errExit = false
	} else {
		// When running non-interactive, by default we want errors to stop
		// further processing and all syntax checking to be done
		// server-side.
		c.errExit = true
		c.checkSyntax = false
	}

	return nextState
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

func (c *cliState) doQuerySyntax(nextState cliStateEnum) cliStateEnum {
	if !c.querySyntax {
		return nextState
	}

	newSyntax, err := getSyntax(c.conn)
	if err != nil {
		fmt.Fprintf(osStderr, "warning: could not get session syntax: %s\n", err)
		// For now this is just a warning.
	} else {
		c.syntax = newSyntax
	}
	return nextState
}

func (c *cliState) doStartLine(nextState cliStateEnum) cliStateEnum {
	// Clear the input buffer.
	c.atEOF = false
	c.querySyntax = false
	c.partialLines = c.partialLines[:0]
	c.partialStmtsLen = 0

	if isInteractive {
		c.ins.SetPrompt(c.fullPrompt)
	}

	return nextState
}

func (c *cliState) doContinueLine(nextState cliStateEnum) cliStateEnum {
	c.atEOF = false

	if isInteractive {
		c.ins.SetPrompt(c.continuePrompt)
	}

	return nextState
}

// doReadline reads a line of input and check the input status.  If
// input was successful it populates c.lastInputLine.  Otherwise
// c.exitErr is set in some cases and an error/retry state is returned.
func (c *cliState) doReadLine(nextState cliStateEnum) cliStateEnum {
	l, err := c.ins.Readline()

	switch err {
	case nil:
		// Good to go.

	case readline.ErrInterrupt:
		if !isInteractive {
			// Ctrl+C terminates non-interactive shells in all cases.
			c.exitErr = err
			return cliStop
		}

		if l != "" {
			// Ctrl+C after the beginning of a line cancels the current
			// line.
			return cliReadLine
		}

		if len(c.partialLines) > 0 {
			// Ctrl+C at the beginning of a line in a multi-line statement
			// cancels the multi-line statement.
			return cliStartLine
		}

		// Otherwise, also terminate with an interrupt error.
		c.exitErr = err
		return cliStop

	case io.EOF:
		c.atEOF = true

		if isInteractive {
			// In interactive mode, EOF terminates.
			// exitErr is left to be whatever has set it previously.
			return cliStop
		}

		// Non-interactive: if no partial statement, EOF terminates.
		// exitErr is left to be whatever has set it previously.
		if len(c.partialLines) == 0 {
			return cliStop
		}

		// Otherwise, give the shell a chance to process the last input line.

	default:
		// Other errors terminate the shell.
		fmt.Fprintf(osStderr, "input error: %s\n", err)
		c.exitErr = err
		return cliStop
	}

	c.lastInputLine = l
	return nextState
}

func (c *cliState) doProcessFirstLine(startState, nextState cliStateEnum) cliStateEnum {
	// Special case: first line of multi-line statement.
	// In this case ignore empty lines, and recognize "help" specially.
	switch c.lastInputLine {
	case "":
		// Ignore empty lines, just continue reading.
		return startState

	case "help", "quit", "exit":
		printCliHelp()
		return startState
	}

	return nextState
}

func (c *cliState) doHandleCliCmd(loopState, nextState cliStateEnum) cliStateEnum {
	if len(c.lastInputLine) == 0 || c.lastInputLine[0] != '\\' {
		return nextState
	}

	errState := loopState
	if c.errExit {
		// If exiterr is set, an error in a client-side command also
		// terminates the shell.
		errState = cliStop
	}

	// This is a client-side command. Whatever happens, we are not going
	// to handle it as a statement, so save the history.
	c.addHistory(c.lastInputLine)

	cmd := strings.Fields(c.lastInputLine)
	switch cmd[0] {
	case `\q`, `\quit`, `\exit`:
		return cliStop

	case `\`, `\?`, `\help`:
		printCliHelp()

	case `\set`:
		return c.handleSet(cmd[1:], loopState, errState)

	case `\unset`:
		return c.handleUnset(cmd[1:], loopState, errState)

	case `\!`:
		return c.runSyscmd(c.lastInputLine, loopState, errState)

	case `\show`:
		if len(c.partialLines) == 0 {
			fmt.Fprintf(osStderr, "No input so far. Did you mean SHOW?\n")
		} else {
			for _, s := range c.partialLines {
				fmt.Println(s)
			}
		}

	case `\|`:
		return c.pipeSyscmd(c.lastInputLine, nextState, errState)

	default:
		if strings.HasPrefix(cmd[0], `\d`) {
			// Unrecognized command for now, but we want to be helpful.
			fmt.Fprint(osStderr, "Suggestion: use the SQL SHOW statement to inspect your schema.\n")
		}
		return c.invalidSyntax(errState, `%s. Try \? for help.`, c.lastInputLine)
	}

	return loopState
}

func (c *cliState) doPrepareStatementLine(
	startState, contState, checkState, execState cliStateEnum,
) cliStateEnum {
	c.partialLines = append(c.partialLines, c.lastInputLine)

	// We join the statements back together with newlines in case
	// there is a significant newline inside a string literal.
	c.concatLines = strings.Trim(strings.Join(c.partialLines, "\n"), " \r\n\t\f")

	if c.concatLines == "" {
		// Only whitespace.
		return startState
	}

	isEmpty, endsWithSemi, hasSet := isEndOfStatement(c.concatLines, c.syntax)
	if c.partialStmtsLen == 0 && isEmpty {
		// More whitespace, or comments. Still nothing to do. However
		// if the syntax was non-trivial to arrive here,
		// keep it for history.
		if c.lastInputLine != "" {
			c.addHistory(c.lastInputLine)
		}
		return startState
	}

	c.querySyntax = hasSet

	if c.atEOF {
		// Definitely no more input expected.
		if !endsWithSemi {
			fmt.Fprintf(osStderr, "missing semicolon at end of statement: %s\n", c.concatLines)
			c.exitErr = fmt.Errorf("last statement was not executed: %s", c.concatLines)
			return cliStop
		}
	}

	if !endsWithSemi {
		return contState
	}

	if !c.checkSyntax {
		// If syntax checking is not enabled, put the raw statement into
		// history, then go and run it.
		c.addHistory(c.concatLines)
		return execState
	}

	return checkState
}

func (c *cliState) doCheckStatement(startState, contState, execState cliStateEnum) cliStateEnum {
	// From here on, client-side syntax checking is enabled.
	var parser parser.Parser
	parsedStmts, err := parser.Parse(c.concatLines, c.syntax)
	if err != nil {
		_ = c.invalidSyntax(0, "statement ignored: %v", err)

		// Even on failure, add the last (erroneous) lines as-is to the
		// history, so that the user can recall them later to fix them.
		for i := c.partialStmtsLen; i < len(c.partialLines); i++ {
			c.addHistory(c.partialLines[i])
		}

		// Stop here if exiterr is set.
		if c.errExit {
			return cliStop
		}

		// Otherwise, remove the erroneous lines from the buffered input,
		// then try again.
		c.partialLines = c.partialLines[:c.partialStmtsLen]
		if len(c.partialLines) == 0 {
			return startState
		}
		return contState
	}

	if !isInteractive {
		return execState
	}

	if c.normalizeHistory {
		// Add statements, not lines, to the history.
		for i := c.partialStmtsLen; i < len(parsedStmts); i++ {
			c.addHistory(parsedStmts[i].String() + ";")
		}
	} else {
		// Add the last lines received to the history.
		for i := c.partialStmtsLen; i < len(c.partialLines); i++ {
			c.addHistory(c.partialLines[i])
		}
	}

	// Replace the last entered lines by the last entered statements.
	c.partialLines = c.partialLines[:c.partialStmtsLen]
	for i := c.partialStmtsLen; i < len(parsedStmts); i++ {
		c.partialLines = append(c.partialLines, parsedStmts[i].String()+";")
	}

	nextState := execState

	// In interactive mode, we make some additional effort to help the user:
	// if the entry so far is starting an incomplete transaction, push
	// the user to enter input over multiple lines.
	if endsWithIncompleteTxn(parsedStmts) && c.lastInputLine != "" {
		if c.partialStmtsLen == 0 {
			fmt.Fprintln(osStderr, "Now adding input for a multi-line SQL transaction client-side.\n"+
				"Press Enter two times to send the SQL text collected so far to the server, or Ctrl+C to cancel.")
		}

		nextState = contState
	}

	c.partialStmtsLen = len(parsedStmts)

	return nextState
}

func (c *cliState) doRunStatement(nextState cliStateEnum) cliStateEnum {
	c.exitErr = runQueryAndFormatResults(c.conn, os.Stdout, makeQuery(c.concatLines), cliCtx.prettyFmt)
	if c.exitErr != nil {
		fmt.Fprintln(osStderr, c.exitErr)
		if c.errExit {
			return cliStop
		}
	}
	return nextState
}

func (c *cliState) doDecidePath() cliStateEnum {
	if len(c.partialLines) == 0 {
		return cliProcessFirstLine
	} else if isInteractive {
		// In interactive mode, we allow client-side commands to be
		// issued on intermediate lines.
		return cliHandleCliCmd
	}
	// Neither interactive nor at start, continue with processing.
	return cliPrepareStatementLine
}

// runInteractive runs the SQL client interactively, presenting
// a prompt to the user for each statement.
func runInteractive(conn *sqlConn, config *readline.Config) (exitErr error) {
	c := cliState{conn: conn}

	state := cliStart
	for {
		if state == cliStop {
			break
		}
		switch state {
		case cliStart:
			// The readline initialization is not placed in
			// the doStart() method because of the defer.
			c.ins, c.exitErr = readline.NewEx(config)
			if c.exitErr != nil {
				return c.exitErr
			}
			defer func() { _ = c.ins.Close() }()

			state = c.doStart(cliQuerySyntax)

		case cliQuerySyntax:
			state = c.doQuerySyntax(cliStartLine)

		case cliStartLine:
			state = c.doStartLine(cliReadLine)

		case cliContinueLine:
			state = c.doContinueLine(cliReadLine)

		case cliReadLine:
			state = c.doReadLine(cliDecidePath)

		case cliDecidePath:
			state = c.doDecidePath()

		case cliProcessFirstLine:
			state = c.doProcessFirstLine(cliReadLine, cliHandleCliCmd)

		case cliHandleCliCmd:
			state = c.doHandleCliCmd(cliReadLine, cliPrepareStatementLine)

		case cliPrepareStatementLine:
			state = c.doPrepareStatementLine(
				cliStartLine, cliContinueLine, cliCheckStatement, cliRunStatement,
			)

		case cliCheckStatement:
			state = c.doCheckStatement(cliStartLine, cliContinueLine, cliRunStatement)

		case cliRunStatement:
			state = c.doRunStatement(cliQuerySyntax)

		default:
			panic(fmt.Sprintf("unknown state: %d", state))
		}
	}

	return c.exitErr
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
		return usageAndError(cmd)
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	// Open the connection to make sure everything is OK before running any
	// statements. Performs authentication.
	if err := conn.ensureConn(); err != nil {
		return err
	}

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
