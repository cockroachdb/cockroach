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
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	readline "github.com/knz/go-libedit"
	"github.com/lib/pq"
	isatty "github.com/mattn/go-isatty"
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
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runTerm),
}

// cliState defines the current state of the CLI during
// command-line processing.
type cliState struct {
	conn *sqlConn
	// ins is used to read lines if isInteractive is true.
	ins readline.EditLine
	// buf is used to read lines if isInteractive is false.
	buf *bufio.Reader

	// Options
	//
	// Determines whether to stop the client upon encountering an error.
	errExit bool
	// Determines whether to perform client-side syntax checking.
	checkSyntax bool
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
	// The current prompt, either fullPrompt or continuePrompt.
	currentPrompt string

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
	// multi-line entry.
	partialLines []string

	// partialStmtsLen represents the number of entries in partialLines
	// parsed successfully so far. It grows larger than zero whenever 1)
	// syntax checking is enabled and 2) multi-statement (as opposed to
	// multi-line) entry starts, i.e. when the shell decides to continue
	// inputting statements even after a full statement followed by a
	// semicolon was read successfully. This is currently used for
	// multi-line transaction editing.
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
	fmt.Printf(`You are using 'cockroach sql', CockroachDB's lightweight SQL client.
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
%s
%s`,
		base.DocsURL("sql-statements.html"),
		base.DocsURL("use-the-built-in-sql-client.html"),
	)
	fmt.Println()
}

const noLineEditor readline.EditLine = -1

func (c *cliState) hasEditor() bool {
	return c.ins != noLineEditor
}

// addHistory persists a line of input to the readline history
// file.
func (c *cliState) addHistory(line string) {
	if !c.hasEditor() || len(line) == 0 {
		return
	}

	// ins.AddHistory will push command into memory and try to
	// persist to disk (if ins's history file is set). err can
	// be not nil only if it got a IO error while trying to persist.
	if err := c.ins.AddHistory(line); err != nil {
		log.Warningf(context.TODO(), "cannot save command-line history: %s", err)
		log.Info(context.TODO(), "command-line history will not be saved in this session")
		c.ins.SetAutoSaveHistory("", false)
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
	description               string
	numExpectedArgs           int
	validDuringMultilineEntry bool
	set                       func(c *cliState, args []string) error
	unset                     func(c *cliState) error
	display                   func(c *cliState) string
}{
	`display_format`: {
		"the output format for tabular data (pretty, csv, tsv, html, sql, records, raw)",
		1,
		true,
		func(_ *cliState, args []string) error {
			return cliCtx.tableDisplayFormat.Set(args[0])
		},
		func(_ *cliState) error {
			displayFormat := tableDisplayTSV
			if cliCtx.terminalOutput {
				displayFormat = tableDisplayPretty
			}
			cliCtx.tableDisplayFormat = displayFormat
			return nil
		},
		func(_ *cliState) string { return cliCtx.tableDisplayFormat.String() },
	},
	`echo`: {
		"show SQL queries before they are sent to the server",
		0,
		false,
		func(_ *cliState, _ []string) error { sqlCtx.echo = true; return nil },
		func(_ *cliState) error { sqlCtx.echo = false; return nil },
		func(_ *cliState) string { return strconv.FormatBool(sqlCtx.echo) },
	},
	`errexit`: {
		"exit the shell upon a query error",
		0,
		true,
		func(c *cliState, _ []string) error { c.errExit = true; return nil },
		func(c *cliState) error { c.errExit = false; return nil },
		func(c *cliState) string { return strconv.FormatBool(c.errExit) },
	},
	`check_syntax`: {
		"check the SQL syntax before running a query (needs SHOW SYNTAX support on the server)",
		0,
		false,
		func(c *cliState, _ []string) error { c.checkSyntax = true; return nil },
		func(c *cliState) error { c.checkSyntax = false; return nil },
		func(c *cliState) string { return strconv.FormatBool(c.checkSyntax) },
	},
	`show_times`: {
		"display the execution time after each query",
		0,
		true,
		func(_ *cliState, _ []string) error { cliCtx.showTimes = true; return nil },
		func(_ *cliState) error { cliCtx.showTimes = false; return nil },
		func(_ *cliState) string { return strconv.FormatBool(cliCtx.showTimes) },
	},
	`smart_prompt`: {
		"print connection and session metadata in the prompt",
		0,
		true,
		func(c *cliState, _ []string) error { c.smartPrompt = true; return nil },
		func(c *cliState) error { c.smartPrompt = false; return nil },
		func(c *cliState) string { return strconv.FormatBool(c.smartPrompt) },
	},
}

// optionNames retains the names of every option in the map above in sorted
// order. We want them sorted to ensure the output of \? is deterministic.
var optionNames = func() []string {
	names := make([]string, 0, len(options))
	for k := range options {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}()

// handleSet supports the \set client-side command.
func (c *cliState) handleSet(args []string, nextState, errState cliStateEnum) cliStateEnum {
	if len(args) == 0 {
		optData := make([][]string, 0, len(options))
		for _, n := range optionNames {
			optData = append(optData, []string{n, options[n].display(c), options[n].description})
		}
		err := printQueryOutput(os.Stdout,
			[]string{"Option", "Value", "Description"},
			newRowSliceIter(optData, "lll" /*align*/))
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

func checkTokens(fullStmt string) (isEmpty bool, lastTok int) {
	sc := parser.MakeScanner(fullStmt)
	isEmpty = true
	var last int
	sc.Tokens(func(t int) {
		isEmpty = false
		last = t
	})
	return isEmpty, last
}

func isEndOfStatement(lastTok int) bool {
	return lastTok == ';' || lastTok == parser.HELPTOKEN
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
		for _, f := range builtins.AllBuiltinNames {
			fmt.Println(f)
		}
		fmt.Println()
	} else {
		_, err := parser.Parse(fmt.Sprintf("select %s(??", funcName))
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
	if !c.hasEditor() {
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
	} else {
		c.lastKnownTxnStatus = ""
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
	// If parsing fails, we'll keep the entire URL. The Open call succeeded, and that
	// is the important part.
	promptPrefix = dbURL
	if parsedURL, err := url.Parse(dbURL); err == nil {
		username := ""
		if parsedURL.User != nil {
			username = parsedURL.User.Username()
		}
		promptPrefix = fmt.Sprintf("%s@%s", username, parsedURL.Host)

		if parsedURL.Path == "" {
			// Attempt to be helpful to new users.
			fmt.Fprintln(stderr, "warning: no current database set."+
				" Use SET database = <dbname> to change, CREATE DATABASE to make a new database.")
		}
	}

	if len(promptPrefix) == 0 {
		promptPrefix = " "
	}

	// Default prompt is part of the connection URL. eg: "marc@localhost>"
	// continued statement prompt is: "        -> "
	continuePrompt = strings.Repeat(" ", len(promptPrefix)-1) + "-> "
	fullPrompt = promptPrefix + "> "

	return promptPrefix, fullPrompt, continuePrompt
}

// endsWithIncompleteTxn returns true if and only if its
// argument ends with an incomplete transaction prefix (BEGIN without
// ROLLBACK/COMMIT).
func endsWithIncompleteTxn(stmts []string) bool {
	txnStarted := false
	for _, stmt := range stmts {
		if strings.HasPrefix(stmt, "BEGIN TRANSACTION") {
			txnStarted = true
		} else if strings.HasPrefix(stmt, "COMMIT TRANSACTION") ||
			strings.HasPrefix(stmt, "ROLLBACK TRANSACTION") {
			txnStarted = false
		}
	}
	return txnStarted
}

var cmdHistFile = envutil.EnvOrDefaultString("COCKROACH_SQL_CLI_HISTORY", ".cockroachsql_history")

// GetCompletions implements the readline.CompletionGenerator interface.
func (c *cliState) GetCompletions(_ string) []string {
	sql, _ := c.ins.GetLineInfo()

	if !strings.HasSuffix(sql, "??") {
		fmt.Fprintf(c.ins.Stdout(),
			"\ntab completion not supported; append '??' and press tab for contextual help\n\n%s", sql)
		return nil
	}

	_, pgErr := c.serverSideParse(sql)
	if pgErr != nil {
		if pgErr.Message == "help token in input" && strings.HasPrefix(pgErr.Hint, "help:") {
			fmt.Fprintf(c.ins.Stdout(), "\nSuggestion:\n%s\n", pgErr.Hint[6:])
		} else {
			fmt.Fprintf(c.ins.Stdout(), "\n%v\n", pgErr)
			maybeShowErrorDetails(c.ins.Stdout(), pgErr, false)
		}
		fmt.Fprint(c.ins.Stdout(), c.currentPrompt, sql)
	}
	return nil
}

func (c *cliState) doStart(nextState cliStateEnum) cliStateEnum {
	// Common initialization.
	c.partialLines = []string{}

	if cliCtx.isInteractive {
		// If a human user is providing the input, we want to help them with
		// what they are entering:
		c.errExit = false // let the user retry failing commands
		// Also, try to enable syntax checking if supported by the server.
		// This is a form of client-side error checking to help with large txns.
		c.tryEnableCheckSyntax()

		fmt.Println("#\n# Enter \\? for a brief introduction.\n#")
	} else {
		// When running non-interactive, by default we want errors to stop
		// further processing and we can just let syntax checking to be
		// done server-side to avoid client-side churn.
		c.errExit = true
		c.checkSyntax = false
		// We also don't need (smart) prompts at all.
	}

	if c.hasEditor() {
		// We only enable prompt and history management when the
		// interactive input prompter is enabled. This saves on churn and
		// memory when e.g. piping a large SQL script through the
		// command-line client.

		c.smartPrompt = true // enquire the db in between statements
		c.promptPrefix, c.fullPrompt, c.continuePrompt = preparePrompts(c.conn.url)

		c.ins.SetCompleter(c)
		if err := c.ins.UseHistory(-1 /*maxEntries*/, true /*dedup*/); err != nil {
			log.Warningf(context.TODO(), "cannot enable history: %v", err)
		} else {
			homeDir, err := envutil.HomeDir()
			if err != nil {
				log.Warningf(context.TODO(), "cannot retrieve user information: %v", err)
				log.Warning(context.TODO(), "history will not be saved")
			} else {
				histFile := filepath.Join(homeDir, cmdHistFile)
				err = c.ins.LoadHistory(histFile)
				if err != nil {
					log.Warningf(context.TODO(), "cannot load the command-line history (file corrupted?): %v", err)
					log.Warning(context.TODO(), "the history file will be cleared upon first entry")
				}
				c.ins.SetAutoSaveHistory(histFile, true)
			}
		}
	}

	return nextState
}

func (c *cliState) doStartLine(nextState cliStateEnum) cliStateEnum {
	// Clear the input buffer.
	c.atEOF = false
	c.partialLines = c.partialLines[:0]
	c.partialStmtsLen = 0

	if c.hasEditor() {
		c.currentPrompt = c.fullPrompt
		c.ins.SetLeftPrompt(c.currentPrompt)
	}

	return nextState
}

func (c *cliState) doContinueLine(nextState cliStateEnum) cliStateEnum {
	c.atEOF = false

	if c.hasEditor() {
		c.currentPrompt = c.continuePrompt
		c.ins.SetLeftPrompt(c.currentPrompt)
	}

	return nextState
}

// doReadline reads a line of input and check the input status.  If
// input was successful it populates c.lastInputLine.  Otherwise
// c.exitErr is set in some cases and an error/retry state is returned.
func (c *cliState) doReadLine(nextState cliStateEnum) cliStateEnum {
	var l string
	var err error
	if c.buf == nil {
		l, err = c.ins.GetLine()
		if len(l) > 0 && l[len(l)-1] == '\n' {
			// Strip the final newline.
			l = l[:len(l)-1]
		} else {
			// There was no newline at the end of the input
			// (e.g. Ctrl+C was entered). Force one.
			fmt.Fprintln(c.ins.Stdout())
		}
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

	case readline.ErrInterrupted:
		if !cliCtx.isInteractive {
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

		if cliCtx.isInteractive {
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

	// As a convenience to the user, we strip the final semicolon, if
	// any, in all cases.
	line := strings.TrimRight(c.lastInputLine, "; ")

	cmd := strings.Fields(line)
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

	isEmpty, lastTok := checkTokens(c.concatLines)
	endOfStmt := isEndOfStatement(lastTok)
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
		if !endOfStmt {
			fmt.Fprintf(stderr, "missing semicolon at end of statement: %s\n", c.concatLines)
			c.exitErr = fmt.Errorf("last statement was not executed: %s", c.concatLines)
			return cliStop
		}
	}

	if !endOfStmt {
		if lastTok == '?' {
			fmt.Fprintf(c.ins.Stdout(),
				"Note: a single '?' is a JSON operator. If you want contextual help, use '??'.\n")
		}
		return contState
	}

	// Complete input. Remember it in the history.
	c.addHistory(c.concatLines)

	if !c.checkSyntax {
		return execState
	}

	return checkState
}

func (c *cliState) doCheckStatement(startState, contState, execState cliStateEnum) cliStateEnum {
	// From here on, client-side syntax checking is enabled.
	parsedStmts, pgErr := c.serverSideParse(c.concatLines)
	if pgErr != nil {
		if pgErr.Message == "help token in input" && strings.HasPrefix(pgErr.Hint, "help:") {
			fmt.Println(pgErr.Hint[6:])
		} else {
			_ = c.invalidSyntax(0, "statement ignored: %v", pgErr)
			maybeShowErrorDetails(stderr, pgErr, false)

			// Stop here if exiterr is set.
			if c.errExit {
				return cliStop
			}
		}

		// Remove the erroneous lines from the buffered input,
		// then try again.
		c.partialLines = c.partialLines[:c.partialStmtsLen]
		if len(c.partialLines) == 0 {
			return startState
		}
		return contState
	}

	if !cliCtx.isInteractive {
		return execState
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

	c.partialStmtsLen = len(c.partialLines)

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
	} else if cliCtx.isInteractive {
		// In interactive mode, we allow client-side commands to be
		// issued on intermediate lines.
		return cliHandleCliCmd
	}
	// Neither interactive nor at start, continue with processing.
	return cliPrepareStatementLine
}

// runInteractive runs the SQL client interactively, presenting
// a prompt to the user for each statement.
func runInteractive(conn *sqlConn) (exitErr error) {
	c := cliState{conn: conn}

	state := cliStart
	for {
		if state == cliStop {
			break
		}
		switch state {
		case cliStart:
			if cliCtx.terminalOutput {
				// If results are shown on a terminal also enable printing of
				// times by default.
				cliCtx.showTimes = true
			}

			// An interactive readline prompter is comparatively slow at
			// reading input, so we only use it in interactive mode and when
			// there is also a terminal on stdout.
			if cliCtx.isInteractive && cliCtx.terminalOutput {
				// The readline initialization is not placed in
				// the doStart() method because of the defer.
				c.ins, c.exitErr = readline.InitFiles("cockroach",
					true, /* wideChars */
					stdin, os.Stdout, stderr)
				if c.exitErr == readline.ErrWidecharNotSupported {
					log.Warning(context.TODO(), "wide character support disabled")
					c.ins, c.exitErr = readline.InitFiles("cockroach",
						false, stdin, os.Stdout, stderr)
				}
				if c.exitErr != nil {
					return c.exitErr
				}
				// If the user has used bind -v or bind -l in their ~/.editrc,
				// this will reset the standard bindings. However we really
				// want in this shell that Ctrl+C, tab, Ctrl+Z and Ctrl+R
				// always have the same meaning.  So reload these bindings
				// explicitly no matter what ~/.editrc may have changed.
				c.ins.RebindControlKeys()
				defer c.ins.Close()
			} else {
				c.ins = noLineEditor
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
	// We don't consider sessions interactives unless we have a
	// serious hunch they are. For now, only `cockroach sql` *without*
	// `-e` has the ability to input from a (presumably) human user,
	// and we'll also assume that there is no human if the standard
	// input is not terminal-like -- likely redirected from a file,
	// etc.
	cliCtx.isInteractive = len(sqlCtx.execStmts) == 0 && isatty.IsTerminal(os.Stdin.Fd())

	if cliCtx.isInteractive {
		// The user only gets to see the info screen on interactive sessions.
		fmt.Print(infoMessage)
	}

	conn, err := getPasswordAndMakeSQLClient("cockroach sql")
	if err != nil {
		return err
	}
	defer conn.Close()

	// Open the connection to make sure everything is OK before running any
	// statements. Performs authentication.
	if err := conn.ensureConn(); err != nil {
		return err
	}

	// Enable safe updates, unless disabled.
	setupSafeUpdates(cmd, conn)

	if len(sqlCtx.execStmts) > 0 {
		// Single-line sql; run as simple as possible, without noise on stdout.
		return runStatements(conn, sqlCtx.execStmts)
	}
	return runInteractive(conn)
}

// setupSafeUpdates attempts to enable "safe mode" if the session is
// interactive and the user is not disabling this behavior with
// --safe-updates=false.
func setupSafeUpdates(cmd *cobra.Command, conn *sqlConn) {
	pf := cmd.Flags()
	vf := pf.Lookup(cliflags.SafeUpdates.Name)

	if !vf.Changed {
		// If `--safe-updates` was not specified, we need to set the default
		// based on whether the session is interactive. We cannot do this
		// earlier, because "session is interactive" depends on knowing
		// whether `-e` is also provided.
		sqlCtx.safeUpdates = cliCtx.isInteractive
	}

	if !sqlCtx.safeUpdates {
		// nothing to do.
		return
	}

	if err := conn.Exec("SET sql_safe_updates = TRUE", nil); err != nil {
		// We only enable the setting in interactive sessions. Ignoring
		// the error with a warning is acceptable, because the user is
		// there to decide what they want to do if it doesn't work.
		fmt.Fprintf(stderr, "warning: cannot enable safe updates: %v\n", err)
	}
}

// tryEnableCheckSyntax attempts to enable check_syntax.
// The option is enabled if the SHOW SYNTAX statement is recognized
// by the server.
func (c *cliState) tryEnableCheckSyntax() {
	if err := c.conn.Exec("SHOW SYNTAX 'SHOW SYNTAX ''1'';'", nil); err != nil {
		fmt.Fprintf(stderr, "warning: cannot enable check_syntax: %v", err)
	} else {
		c.checkSyntax = true
	}
}

// serverSideParse uses the SHOW SYNTAX statement to analyze the given string.
// If the syntax is correct, the function returns the statement
// decomposition in the first return value. If it is not, the function
// assembles a pgerror.Error with suitable Detail and Hint fields.
func (c *cliState) serverSideParse(sql string) (stmts []string, pgErr *pgerror.Error) {
	cols, rows, err := runQuery(c.conn, makeQuery("SHOW SYNTAX "+lex.EscapeSQLString(sql)), true)
	if err != nil {
		// The query failed with some error. This is not a syntax error
		// detected by SHOW SYNTAX (those show up as valid rows) but
		// instead something else. Do our best to convert that something
		// else back to a pgerror.Error.
		if pgErr, ok := err.(*pgerror.Error); ok {
			return nil, pgErr
		} else if pqErr, ok := err.(*pq.Error); ok {
			return nil, pgerror.NewError(
				string(pqErr.Code), pqErr.Message).SetHintf("%s", pqErr.Hint).SetDetailf("%s", pqErr.Detail)
		}
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "%v", err)
	}

	if len(cols) < 2 {
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
			"invalid results for SHOW SYNTAX: %q %q", cols, rows)
	}

	// If SHOW SYNTAX reports an error, then it does so on the first row.
	if len(rows) >= 1 && rows[0][0] == "error" {
		var message, code, detail, hint string
		for _, row := range rows {
			switch row[0] {
			case "error":
				message = row[1]
			case "detail":
				detail = row[1]
			case "hint":
				hint = row[1]
			case "code":
				code = row[1]
			}
		}
		return nil, pgerror.NewError(code, message).SetHintf("%s", hint).SetDetailf("%s", detail)
	}

	// Otherwise, hopefully we got some SQL statements.
	stmts = make([]string, len(rows))
	for i := range rows {
		if rows[i][0] != "sql" {
			return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
				"invalid results for SHOW SYNTAX: %q %q", cols, rows)
		}
		stmts[i] = rows[i][1]
	}
	return stmts, nil
}
