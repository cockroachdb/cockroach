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

package cli

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/net/context"

	"github.com/chzyer/readline"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq"
	"github.com/spf13/cobra"
)

const (
	infoMessage = `# Welcome to the cockroach SQL interface.
# All statements must be terminated by a semicolon.
# To exit: CTRL + D.
#
`
)

// sqlShellCmd opens a sql shell.
var sqlShellCmd = &cobra.Command{
	Use:   "sql [options]",
	Short: "open a sql shell",
	Long: `
Open a sql shell running against a cockroach database.
`,
	RunE: MaybeDecorateGRPCError(runTerm),
}

// cliState defines the current state of the CLI during
// command-line processing.
type cliState struct {
	conn *sqlConn
	// ins is used to read lines if isInteractive is true.
	ins *readline.Instance
	// buf is used to read lines if isInteractive is false.
	buf *bufio.Reader

	// Options
	//
	// Determines whether to stop the client upon encountering an error.
	errExit bool
	// Determines whether to perform client-side syntax checking.
	checkSyntax bool
	// Determines whether to store normalized syntax in the shell history
	// when check_syntax is set.
	// TODO(knz): this can possibly be set back to false by default when
	// the upstream readline library handles multi-line history entries
	// properly.
	normalizeHistory bool
	// smartPrompt indicates whether to update the prompt using queries
	// to the server. See the state cliRefreshPrompt and
	// doRefreshPrompt() below.
	smartPrompt bool

	// The prefix at the start of a prompt.
	promptPrefix string
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
	// lastKnownTxnStatus reports the last known transaction
	// status. Erased after every statement executed, until the next
	// query to the server updates it.
	lastKnownTxnStatus string

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

	// Querying the server for the current transaction status
	// and setting the prompt accordingly.
	cliRefreshPrompts

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
Type:
  \q to exit        (Ctrl+C/Ctrl+D also supported)
  \! CMD            run an external command and print its results on standard output.
  \| CMD            run an external command and run its output as SQL statements.
  \set [NAME]       set a client-side flag or (without argument) print the current settings.
  \unset NAME       unset a flag.
  \show             during a multi-line statement or transaction, show the SQL entered so far.
  \? or "help"      print this help.
  \h [NAME]         help on syntax of SQL commands.
  \hf [NAME]        help on SQL built-in functions.

More documentation about our SQL dialect and the CLI shell is available online:
https://www.cockroachlabs.com/docs/stable/sql-statements.html
https://www.cockroachlabs.com/docs/stable/use-the-built-in-sql-client.html

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
	fmt.Fprint(stderr, "invalid syntax: ")
	fmt.Fprintf(stderr, format, args...)
	fmt.Fprintln(stderr)
	c.exitErr = errInvalidSyntax
	return nextState
}

func (c *cliState) invalidOptionChange(nextState cliStateEnum, opt string) cliStateEnum {
	fmt.Fprintf(stderr, "cannot change option during multi-line editing: %s\n", opt)
	return nextState
}

var options = map[string]struct {
	numExpectedArgs           int
	validDuringMultilineEntry bool
	set                       func(c *cliState, args []string) error
	unset                     func(c *cliState) error
}{
	`display_format`: {
		1,
		true,
		func(_ *cliState, args []string) error {
			return cliCtx.tableDisplayFormat.Set(args[0])
		},
		func(_ *cliState) error {
			displayFormat := tableDisplayTSV
			if isInteractive {
				displayFormat = tableDisplayPretty
			}
			cliCtx.tableDisplayFormat = displayFormat
			return nil
		},
	},
	`echo`: {
		0,
		false,
		func(c *cliState, _ []string) error { sqlCtx.echo = true; return nil },
		func(c *cliState) error { sqlCtx.echo = false; return nil },
	},
	`errexit`: {
		0,
		true,
		func(c *cliState, _ []string) error { c.errExit = true; return nil },
		func(c *cliState) error { c.errExit = false; return nil },
	},
	`check_syntax`: {
		0,
		false,
		func(c *cliState, _ []string) error { c.checkSyntax = true; return nil },
		func(c *cliState) error { c.checkSyntax = false; return nil },
	},
	`normalize_history`: {
		0,
		false,
		func(c *cliState, _ []string) error { c.normalizeHistory = true; return nil },
		func(c *cliState) error { c.normalizeHistory = false; return nil },
	},
	`show_times`: {
		0,
		true,
		func(c *cliState, _ []string) error { cliCtx.showTimes = true; return nil },
		func(c *cliState) error { cliCtx.showTimes = false; return nil },
	},
	`smart_prompt`: {
		0,
		true,
		func(c *cliState, _ []string) error { c.smartPrompt = true; return nil },
		func(c *cliState) error { c.smartPrompt = false; return nil },
	},
}

// handleSet supports the \set client-side command.
func (c *cliState) handleSet(args []string, nextState, errState cliStateEnum) cliStateEnum {
	if len(args) == 0 {
		err := printQueryOutput(os.Stdout,
			[]string{"Option", "Value"},
			newRowSliceIter([][]string{
				{"display_format", cliCtx.tableDisplayFormat.String()},
				{"errexit", strconv.FormatBool(c.errExit)},
				{"echo", strconv.FormatBool(sqlCtx.echo)},
				{"check_syntax", strconv.FormatBool(c.checkSyntax)},
				{"normalize_history", strconv.FormatBool(c.normalizeHistory)},
				{"show_times", strconv.FormatBool(cliCtx.showTimes)},
				{"smart_prompt", strconv.FormatBool(c.smartPrompt)},
			}))
		if err != nil {
			panic(err)
		}

		return nextState
	}
	opt, ok := options[args[0]]
	if !ok || len(args)-1 != opt.numExpectedArgs {
		return c.invalidSyntax(errState, `\set %s. Try \? for help.`, strings.Join(args, " "))
	}
	if len(c.partialLines) > 0 && !opt.validDuringMultilineEntry {
		return c.invalidOptionChange(errState, args[0])
	}
	if err := opt.set(c, args[1:]); err != nil {
		fmt.Fprintf(stderr, "\\set %s: %v\n", strings.Join(args, " "), err)
		return errState
	}
	return nextState
}

// handleUnset supports the \unset client-side command.
func (c *cliState) handleUnset(args []string, nextState, errState cliStateEnum) cliStateEnum {
	if len(args) != 1 {
		return c.invalidSyntax(errState, `\unset %s. Try \? for help.`, strings.Join(args, " "))
	}
	opt, ok := options[args[0]]
	if !ok {
		return c.invalidSyntax(errState, `\unset %s. Try \? for help.`, strings.Join(args, " "))
	}
	if len(c.partialLines) > 0 && !opt.validDuringMultilineEntry {
		return c.invalidOptionChange(errState, args[0])
	}
	if err := opt.unset(c); err != nil {
		fmt.Fprintf(stderr, "\\unset %s: %v\n", args[0], err)
		return errState
	}
	return nextState
}

func isEndOfStatement(fullStmt string) (isEmpty, isEnd bool) {
	sc := parser.MakeScanner(fullStmt)
	isEmpty = true
	var last int
	sc.Tokens(func(t int) {
		isEmpty = false
		last = t
	})
	return isEmpty, last == ';' || last == parser.HELPTOKEN
}

// handleHelp prints SQL help.
func (c *cliState) handleHelp(cmd []string, nextState, errState cliStateEnum) cliStateEnum {
	cmdrest := strings.TrimSpace(strings.Join(cmd, " "))
	command := strings.ToUpper(cmdrest)
	if command == "" {
		fmt.Print(parser.AllHelp)
	} else {
		if h, ok := parser.HelpMessages[command]; ok {
			msg := parser.HelpMessage{Command: command, HelpMessageBody: h}
			msg.Format(os.Stdout)
			fmt.Println()
		} else {
			fmt.Fprintf(stderr,
				"no help available for %q.\nTry \\h with no argument to see available help.\n", cmdrest)
			return errState
		}
	}
	return nextState
}

// handleFunctionHelp prints help about built-in functions.
func (c *cliState) handleFunctionHelp(cmd []string, nextState, errState cliStateEnum) cliStateEnum {
	funcName := strings.TrimSpace(strings.Join(cmd, " "))
	if funcName == "" {
		for _, f := range parser.AllBuiltinNames {
			fmt.Println(f)
		}
		fmt.Println()
	} else {
		_, err := parser.Parse(fmt.Sprintf("select %s(?", funcName))
		pgerr, ok := pgerror.GetPGCause(err)
		if !ok || !strings.HasPrefix(pgerr.Hint, "help:") {
			fmt.Fprintf(stderr,
				"no help available for %q.\nTry \\hf with no argument to see available help.\n", funcName)
			return errState
		}
		fmt.Println(pgerr.Hint[6:])
	}
	return nextState
}

// execSyscmd executes system commands.
func execSyscmd(command string) (string, error) {
	var cmd *exec.Cmd

	shell := envutil.GetShellCommand(command)
	cmd = exec.Command(shell[0], shell[1:]...)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = stderr

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
		fmt.Fprintf(stderr, "Usage:\n  \\! [command]\n")
		c.exitErr = errInvalidSyntax
		return errState
	}

	cmdOut, err := execSyscmd(command)
	if err != nil {
		fmt.Fprintf(stderr, "command failed: %s\n", err)
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
		fmt.Fprintf(stderr, "Usage:\n  \\| [command]\n")
		c.exitErr = errInvalidSyntax
		return errState
	}

	cmdOut, err := execSyscmd(command)
	if err != nil {
		fmt.Fprintf(stderr, "command failed: %s\n", err)
		c.exitErr = err
		return errState
	}

	c.lastInputLine = cmdOut
	return nextState
}

// doRefreshPrompts refreshes the prompts of the client depending on the
// status of the current transaction.
func (c *cliState) doRefreshPrompts(nextState cliStateEnum) cliStateEnum {
	if !isInteractive {
		return nextState
	}

	c.fullPrompt = c.promptPrefix

	if c.smartPrompt {
		c.refreshTransactionStatus()
		dbName, hasDbName := c.refreshDatabaseName()

		dbStr := ""
		if hasDbName {
			dbStr = "/" + dbName
		}

		c.fullPrompt += dbStr + c.lastKnownTxnStatus
	}

	c.continuePrompt = strings.Repeat(" ", len(c.fullPrompt)-1) + "-> "
	c.fullPrompt += "> "

	return nextState
}

// refreshTransactionStatus retrieves and sets the current transaction status.
func (c *cliState) refreshTransactionStatus() {
	c.lastKnownTxnStatus = " ?"

	dbVal, hasVal := c.conn.getServerValue("transaction status", `SHOW TRANSACTION STATUS`)
	if !hasVal {
		return
	}

	txnString := formatVal(dbVal,
		false /* showPrintableUnicode */, false /* shownewLinesAndTabs */)

	// Change the prompt based on the response from the server.
	switch txnString {
	case sql.NoTxn.String():
		c.lastKnownTxnStatus = ""
	case sql.Aborted.String():
		c.lastKnownTxnStatus = " ERROR"
	case sql.CommitWait.String():
		c.lastKnownTxnStatus = "  DONE"
	case sql.RestartWait.String():
		c.lastKnownTxnStatus = " RETRY"
	case sql.Open.String():
		// The state AutoRetry is reported by the server as Open, so no need to
		// handle it here.
		c.lastKnownTxnStatus = "  OPEN"
	}
}

// refreshDatabaseName retrieves the current database name from the server.
// The database name is only queried if there is no transaction ongoing,
// or the transaction is fully open.
func (c *cliState) refreshDatabaseName() (string, bool) {
	if !(c.lastKnownTxnStatus == "" /*NoTxn*/ ||
		c.lastKnownTxnStatus == "  OPEN" ||
		c.lastKnownTxnStatus == " ?" /* Unknown */) {
		return "", false
	}

	dbVal, hasVal := c.conn.getServerValue("database name", `SHOW DATABASE`)
	if !hasVal {
		return "", false
	}

	dbName := formatVal(dbVal.(string),
		false /* showPrintableUnicode */, false /* shownewLinesAndTabs */)

	// Preserve the current database name in case of reconnects.
	c.conn.dbName = dbName

	return dbName, true
}

// preparePrompts computes a full and short prompt for the interactive
// CLI.
func preparePrompts(dbURL string) (promptPrefix, fullPrompt, continuePrompt string) {
	// Default prompt is part of the connection URL. eg: "marc@localhost>"
	// continued statement prompt is: "        -> "
	promptPrefix = dbURL
	if parsedURL, err := url.Parse(dbURL); err == nil {
		username := ""
		if parsedURL.User != nil {
			username = parsedURL.User.Username()
		}
		// If parsing fails, we keep the entire URL. The Open call succeeded, and that
		// is the important part.
		promptPrefix = fmt.Sprintf("%s@%s", username, parsedURL.Host)
	}

	if len(promptPrefix) == 0 {
		promptPrefix = " "
	}

	continuePrompt = strings.Repeat(" ", len(promptPrefix)-1) + "-> "
	fullPrompt = promptPrefix + "> "

	return promptPrefix, fullPrompt, continuePrompt
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

// Do implements the readline.AutoCompleter interface.
func (c *cliState) Do(line []rune, pos int) (newLine [][]rune, length int) {
	var p parser.Parser
	sql := string(line)
	if !strings.HasSuffix(sql, "?") {
		fmt.Fprintf(c.ins.Stdout(),
			"\ntab completion not supported; append '?' and press tab for contextual help\n\n%s", sql)
		return nil, 0
	}

	_, err := p.Parse(sql)
	if err != nil {
		if pgErr, ok := pgerror.GetPGCause(err); ok &&
			err.Error() == "help token in input" &&
			strings.HasPrefix(pgErr.Hint, "help:") {
			fmt.Fprintf(c.ins.Stdout(), "\nSuggestion:\n%s\n", pgErr.Hint[6:])
		} else {
			fmt.Fprintf(c.ins.Stdout(), "\n%v\n", err)
			maybeShowErrorDetails(c.ins.Stdout(), err, false)
		}
	}
	return nil, 0
}

func (c *cliState) doStart(nextState cliStateEnum) cliStateEnum {
	// Common initialization.
	c.partialLines = []string{}

	if isInteractive {
		c.promptPrefix, c.fullPrompt, c.continuePrompt = preparePrompts(c.conn.url)

		// We only enable history management when the terminal is actually
		// interactive. This saves on memory when e.g. piping a large SQL
		// script through the command-line client.
		homeDir, err := envutil.HomeDir()
		if err != nil {
			if log.V(2) {
				log.Warningf(context.TODO(), "cannot retrieve user information: %v", err)
				log.Info(context.TODO(), "cannot load or save the command-line history")
			}
		} else {
			histFile := filepath.Join(homeDir, cmdHistFile)
			cfg := c.ins.Config.Clone()
			cfg.HistoryFile = histFile
			cfg.HistorySearchFold = true
			cfg.AutoComplete = c
			c.ins.SetConfig(cfg)
		}

		fmt.Println("#\n# Enter \\? for a brief introduction.\n#")

		c.checkSyntax = true
		c.normalizeHistory = true
		c.errExit = false
		c.smartPrompt = true
	} else {
		// When running non-interactive, by default we want errors to stop
		// further processing and all syntax checking to be done
		// server-side.
		c.errExit = true
		c.checkSyntax = false
	}

	return nextState
}

func (c *cliState) doStartLine(nextState cliStateEnum) cliStateEnum {
	// Clear the input buffer.
	c.atEOF = false
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
	var l string
	var err error
	if c.ins != nil {
		l, err = c.ins.Readline()
	} else {
		l, err = c.buf.ReadString('\n')
		// bufio.ReadString() differs from readline.Readline in the handling of
		// EOF. Readline only returns EOF when there is nothing left to read and
		// there is no partial line while bufio.ReadString() returns EOF when the
		// end of input has been reached but will return the non-empty partial line
		// as well. We workaround this by converting the bufio behavior to match
		// the Readline behavior.
		if err == io.EOF && len(l) != 0 {
			err = nil
		} else if err == nil {
			// From the bufio.ReadString docs: ReadString returns err != nil if and
			// only if the returned data does not end in delim. To match the behavior
			// of readline.Readline, we strip off the trailing delimiter.
			l = l[:len(l)-1]
		}
	}

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
		fmt.Fprintf(stderr, "input error: %s\n", err)
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
		// Ignore empty lines, just continue reading if it isn't interactive mode.
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
			fmt.Fprintf(stderr, "No input so far. Did you mean SHOW?\n")
		} else {
			for _, s := range c.partialLines {
				fmt.Println(s)
			}
		}

	case `\|`:
		return c.pipeSyscmd(c.lastInputLine, nextState, errState)

	case `\h`:
		return c.handleHelp(cmd[1:], loopState, errState)

	case `\hf`:
		return c.handleFunctionHelp(cmd[1:], loopState, errState)

	default:
		if strings.HasPrefix(cmd[0], `\d`) {
			// Unrecognized command for now, but we want to be helpful.
			fmt.Fprint(stderr, "Suggestion: use the SQL SHOW statement to inspect your schema.\n")
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

	isEmpty, endsWithSemi := isEndOfStatement(c.concatLines)
	if c.partialStmtsLen == 0 && isEmpty {
		// More whitespace, or comments. Still nothing to do. However
		// if the syntax was non-trivial to arrive here,
		// keep it for history.
		if c.lastInputLine != "" {
			c.addHistory(c.lastInputLine)
		}
		return startState
	}
	if c.atEOF {
		// Definitely no more input expected.
		if !endsWithSemi {
			fmt.Fprintf(stderr, "missing semicolon at end of statement: %s\n", c.concatLines)
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
	parsedStmts, err := parser.Parse(c.concatLines)
	if err != nil {
		if pgErr, ok := pgerror.GetPGCause(err); ok &&
			err.Error() == "help token in input" &&
			strings.HasPrefix(pgErr.Hint, "help:") {
			fmt.Println(pgErr.Hint[6:])
		} else {
			_ = c.invalidSyntax(0, "statement ignored: %v", err)
			maybeShowErrorDetails(stderr, err, false)

			// Stop here if exiterr is set.
			if c.errExit {
				return cliStop
			}
		}

		// Even on failure, add the last (erroneous) lines as-is to the
		// history, so that the user can recall them later to fix them.
		for i := c.partialStmtsLen; i < len(c.partialLines); i++ {
			c.addHistory(c.partialLines[i])
		}

		// Remove the erroneous lines from the buffered input,
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

	var lastInputChunk bytes.Buffer
	if c.normalizeHistory {
		// Add statements, not lines, to the history.
		// The last input chunk is one unit of input by the user, so
		// we want the user to be able to recall it all at once
		// (= one history entry)
		for i := c.partialStmtsLen; i < len(parsedStmts); i++ {
			if i > c.partialStmtsLen {
				lastInputChunk.WriteByte(' ')
			}
			fmt.Fprint(&lastInputChunk, parser.AsStringWithFlags(parsedStmts[i], parser.FmtParsable)+";")
		}
	} else {
		// Add the last lines received to the history.
		// Like above, this is one input chunk. However there may be
		// sensitive newlines in string literals or SQL comments, so we
		// can't blindly concatenate all the partial input into a single
		// line. Leave them separate.
		for i := c.partialStmtsLen; i < len(c.partialLines); i++ {
			fmt.Fprintln(&lastInputChunk, c.partialLines[i])
		}
	}
	c.addHistory(lastInputChunk.String())

	// Replace the last entered lines by the last entered statements.
	c.partialLines = c.partialLines[:c.partialStmtsLen]
	for i := c.partialStmtsLen; i < len(parsedStmts); i++ {
		c.partialLines = append(c.partialLines,
			parser.AsStringWithFlags(parsedStmts[i], parser.FmtSimpleWithPasswords)+";")
	}

	nextState := execState

	// In interactive mode, we make some additional effort to help the user:
	// if the entry so far is starting an incomplete transaction, push
	// the user to enter input over multiple lines.
	if c.lastKnownTxnStatus == "" && endsWithIncompleteTxn(parsedStmts) && c.lastInputLine != "" {
		if c.partialStmtsLen == 0 {
			fmt.Fprintln(stderr, "Now adding input for a multi-line SQL transaction client-side.\n"+
				"Press Enter two times to send the SQL text collected so far to the server, or Ctrl+C to cancel.\n"+
				"You can also use \\show to display the statements entered so far.")
		}

		nextState = contState
	}

	c.partialStmtsLen = len(parsedStmts)

	return nextState
}

func (c *cliState) doRunStatement(nextState cliStateEnum) cliStateEnum {
	// Once we send something to the server, the txn status may change arbitrarily.
	// Clear the known state so that further entries do not assume anything.
	c.lastKnownTxnStatus = " ?"

	// Now run the statement/query.
	c.exitErr = runQueryAndFormatResults(c.conn, os.Stdout, makeQuery(c.concatLines))
	if c.exitErr != nil {
		fmt.Fprintln(stderr, c.exitErr)
		maybeShowErrorDetails(stderr, c.exitErr, false)
		if c.errExit {
			return cliStop
		}
	}
	return nextState
}

// maybeShowErrorDetails displays the pg "Detail" and "Hint" fields
// embedded in the error, if any, to the user. If printNewline is set,
// a newline character is printed before anything else.
func maybeShowErrorDetails(w io.Writer, err error, printNewline bool) {
	var hint, detail string
	if pqErr, ok := err.(*pq.Error); ok {
		hint, detail = pqErr.Hint, pqErr.Detail
	} else if pgErr, ok := pgerror.GetPGCause(err); ok {
		hint, detail = pgErr.Hint, pgErr.Detail
	}
	if detail != "" {
		if printNewline {
			fmt.Fprintln(w)
			printNewline = false
		}
		fmt.Fprintln(w, "DETAIL:", detail)
	}
	if hint != "" {
		if printNewline {
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w, "HINT:", hint)
	}
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
			// chzyer/readline is exceptionally slow at reading long lines, so we
			// only use it in interactive mode.
			if isInteractive {
				// If interactive and the table display format is "pretty", also
				// enable printing of times.
				if cliCtx.tableDisplayFormat == tableDisplayPretty {
					cliCtx.showTimes = true
				}

				// The readline initialization is not placed in
				// the doStart() method because of the defer.
				c.ins, c.exitErr = readline.NewEx(config)
				if c.exitErr != nil {
					return c.exitErr
				}
				defer func() { _ = c.ins.Close() }()
			} else {
				stdin := config.Stdin
				if stdin == nil {
					stdin = os.Stdin
				}
				c.buf = bufio.NewReader(stdin)
			}

			state = c.doStart(cliRefreshPrompts)

		case cliRefreshPrompts:
			state = c.doRefreshPrompts(cliStartLine)

		case cliStartLine:
			state = c.doStartLine(cliReadLine)

		case cliContinueLine:
			state = c.doContinueLine(cliReadLine)

		case cliReadLine:
			state = c.doReadLine(cliDecidePath)

		case cliDecidePath:
			state = c.doDecidePath()

		case cliProcessFirstLine:
			state = c.doProcessFirstLine(cliRefreshPrompts, cliHandleCliCmd)

		case cliHandleCliCmd:
			state = c.doHandleCliCmd(cliReadLine, cliPrepareStatementLine)

		case cliPrepareStatementLine:
			state = c.doPrepareStatementLine(
				cliRefreshPrompts, cliContinueLine, cliCheckStatement, cliRunStatement,
			)

		case cliCheckStatement:
			state = c.doCheckStatement(cliRefreshPrompts, cliContinueLine, cliRunStatement)

		case cliRunStatement:
			state = c.doRunStatement(cliRefreshPrompts)

		default:
			panic(fmt.Sprintf("unknown state: %d", state))
		}
	}

	return c.exitErr
}

// runOneStatement executes one statement and terminates
// on error.
func runStatements(conn *sqlConn, stmts []string) error {
	for _, stmt := range stmts {
		if err := runQueryAndFormatResults(conn, os.Stdout, makeQuery(stmt)); err != nil {
			// Expand the details and hints so that they are printed to the user.
			var buf bytes.Buffer
			buf.WriteString(err.Error())
			maybeShowErrorDetails(&buf, err, true)
			return errors.New(strings.TrimSuffix(buf.String(), "\n"))
		}
	}
	return nil
}

func runTerm(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return usageAndError(cmd)
	}

	if isInteractive && len(sqlCtx.execStmts) == 0 {
		// The user only gets to see the info screen on interactive sessions.
		fmt.Print(infoMessage)
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

	if !sqlCtx.unsafeUpdates {
		if err := conn.Exec("SET sql_safe_updates = TRUE", nil); err != nil {
			return err
		}
	}

	if len(sqlCtx.execStmts) > 0 {
		// Single-line sql; run as simple as possible, without noise on stdout.
		return runStatements(conn, sqlCtx.execStmts)
	}
	// Use the same as the default global readline config.
	conf := readline.Config{
		DisableAutoSaveHistory: true,
	}
	return runInteractive(conn, &conf)
}
