// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package cli

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	"github.com/cockroachdb/errors"
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

const defaultPromptPattern = "%n@%M/%/%x>"

// debugPromptPattern avoids substitution patterns that require a db roundtrip.
const debugPromptPattern = "%n@%M>"

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
	// smartPrompt indicates whether to detect the txn status and offer
	// multi-line statements at the start of fresh transactions.
	smartPrompt bool

	// The prompt at the beginning of a multi-line entry.
	fullPrompt string
	// The prompt on a continuation line in a multi-line entry.
	continuePrompt string
	// Which prompt to use to populate currentPrompt.
	useContinuePrompt bool
	// The current prompt, either fullPrompt or continuePrompt.
	currentPrompt string
	// The string used to produce the value of fullPrompt.
	customPromptPattern string

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

	// forwardLines is the array of lookahead lines. This gets
	// populated when there is more than one line of input
	// in the data read by ReadLine(), which can happen
	// when copy-pasting.
	forwardLines []string

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

	// autoTrace, when non-empty, encloses the executed statements
	// by suitable SET TRACING and SHOW TRACE FOR SESSION statements.
	autoTrace string
}

// cliStateEnum drives the CLI state machine in runInteractive().
type cliStateEnum int

const (
	cliStart cliStateEnum = iota
	cliStop

	// Querying the server for the current transaction status
	// and setting the prompt accordingly.
	cliRefreshPrompt

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
  \q, quit, exit    exit the shell (Ctrl+C/Ctrl+D also supported)
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

func (c *cliState) invalidOptSet(nextState cliStateEnum, args []string) cliStateEnum {
	return c.invalidSyntax(nextState, `\set %s. Try \? for help.`, strings.Join(args, " "))
}

func (c *cliState) invalidOptionChange(nextState cliStateEnum, opt string) cliStateEnum {
	fmt.Fprintf(stderr, "cannot change option during multi-line editing: %s\n", opt)
	return nextState
}

var options = map[string]struct {
	description               string
	isBoolean                 bool
	validDuringMultilineEntry bool
	set                       func(c *cliState, val string) error
	reset                     func(c *cliState) error
	// display is used to retrieve the current value.
	display func(c *cliState) string
}{
	`auto_trace`: {
		description:               "automatically run statement tracing on each executed statement",
		isBoolean:                 false,
		validDuringMultilineEntry: false,
		set: func(c *cliState, val string) error {
			val = strings.ToLower(strings.TrimSpace(val))
			switch val {
			case "false", "0", "off":
				c.autoTrace = ""
			case "true", "1":
				val = "on"
				fallthrough
			default:
				c.autoTrace = "on, " + val
			}
			return nil
		},
		reset: func(c *cliState) error {
			c.autoTrace = ""
			return nil
		},
		display: func(c *cliState) string {
			if c.autoTrace == "" {
				return "off"
			}
			return c.autoTrace
		},
	},
	`display_format`: {
		description:               "the output format for tabular data (table, csv, tsv, html, sql, records, raw)",
		isBoolean:                 false,
		validDuringMultilineEntry: true,
		set: func(_ *cliState, val string) error {
			return cliCtx.tableDisplayFormat.Set(val)
		},
		reset: func(_ *cliState) error {
			displayFormat := tableDisplayTSV
			if cliCtx.terminalOutput {
				displayFormat = tableDisplayTable
			}
			cliCtx.tableDisplayFormat = displayFormat
			return nil
		},
		display: func(_ *cliState) string { return cliCtx.tableDisplayFormat.String() },
	},
	`echo`: {
		description:               "show SQL queries before they are sent to the server",
		isBoolean:                 true,
		validDuringMultilineEntry: false,
		set:                       func(_ *cliState, _ string) error { sqlCtx.echo = true; return nil },
		reset:                     func(_ *cliState) error { sqlCtx.echo = false; return nil },
		display:                   func(_ *cliState) string { return strconv.FormatBool(sqlCtx.echo) },
	},
	`errexit`: {
		description:               "exit the shell upon a query error",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(c *cliState, _ string) error { c.errExit = true; return nil },
		reset:                     func(c *cliState) error { c.errExit = false; return nil },
		display:                   func(c *cliState) string { return strconv.FormatBool(c.errExit) },
	},
	`check_syntax`: {
		description:               "check the SQL syntax before running a query (needs SHOW SYNTAX support on the server)",
		isBoolean:                 true,
		validDuringMultilineEntry: false,
		set:                       func(c *cliState, _ string) error { c.checkSyntax = true; return nil },
		reset:                     func(c *cliState) error { c.checkSyntax = false; return nil },
		display:                   func(c *cliState) string { return strconv.FormatBool(c.checkSyntax) },
	},
	`show_times`: {
		description:               "display the execution time after each query",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(_ *cliState, _ string) error { sqlCtx.showTimes = true; return nil },
		reset:                     func(_ *cliState) error { sqlCtx.showTimes = false; return nil },
		display:                   func(_ *cliState) string { return strconv.FormatBool(sqlCtx.showTimes) },
	},
	`smart_prompt`: {
		description:               "detect open transactions and propose entering multi-line statements",
		isBoolean:                 true,
		validDuringMultilineEntry: false,
		set:                       func(c *cliState, _ string) error { c.smartPrompt = true; return nil },
		reset:                     func(c *cliState) error { c.smartPrompt = false; return nil },
		display:                   func(c *cliState) string { return strconv.FormatBool(c.smartPrompt) },
	},
	`prompt1`: {
		description:               "prompt string to use before each command (the following are expanded: %M full host, %m host, %> port number, %n user, %/ database, %x txn status)",
		isBoolean:                 false,
		validDuringMultilineEntry: true,
		set: func(c *cliState, val string) error {
			c.customPromptPattern = val
			return nil
		},
		reset: func(c *cliState) error {
			c.customPromptPattern = defaultPromptPattern
			return nil
		},
		display: func(c *cliState) string { return c.customPromptPattern },
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

	if len(args) == 1 {
		// Try harder to find a value.
		args = strings.SplitN(args[0], "=", 2)
	}

	opt, ok := options[args[0]]
	if !ok {
		return c.invalidOptSet(errState, args)
	}
	if len(c.partialLines) > 0 && !opt.validDuringMultilineEntry {
		return c.invalidOptionChange(errState, args[0])
	}

	// Determine which value to use.
	var val string
	switch len(args) {
	case 1:
		val = "true"
	case 2:
		val = args[1]
	default:
		return c.invalidOptSet(errState, args)
	}

	// Run the command.
	var err error
	if !opt.isBoolean {
		err = opt.set(c, val)
	} else {
		switch val {
		case "true", "1", "on":
			err = opt.set(c, "true")
		case "false", "0", "off":
			err = opt.reset(c)
		default:
			return c.invalidOptSet(errState, args)
		}
	}

	if err != nil {
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
	if err := opt.reset(c); err != nil {
		fmt.Fprintf(stderr, "\\unset %s: %v\n", args[0], err)
		return errState
	}
	return nextState
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
		pgerr := pgerror.Flatten(err)
		if !strings.HasPrefix(pgerr.Hint, "help:") {
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

// rePromptFmt recognizes every substitution pattern in the prompt format string.
var rePromptFmt = regexp.MustCompile("(%.)")

// rePromptDbState recognizes every substitution pattern that requires
// access to the current database state.
// Currently:
// %/ database name
// %x txn status
var rePromptDbState = regexp.MustCompile("(?:^|[^%])%[/x]")

// unknownDbName is the string to use in the prompt when
// the database cannot be determined.
const unknownDbName = "?"

// unknownTxnStatus is the string to use in the prompt when the txn status cannot be determined.
const unknownTxnStatus = " ?"

// doRefreshPrompts refreshes the prompts of the client depending on the
// status of the current transaction.
func (c *cliState) doRefreshPrompts(nextState cliStateEnum) cliStateEnum {
	if !c.hasEditor() {
		return nextState
	}

	parsedURL, err := url.Parse(c.conn.url)
	if err != nil {
		// If parsing fails, we'll keep the entire URL. The Open call succeeded, and that
		// is the important part.
		c.fullPrompt = c.conn.url + "> "
		c.continuePrompt = strings.Repeat(" ", len(c.fullPrompt)-3) + "-> "
		return nextState
	}

	userName := ""
	if parsedURL.User != nil {
		userName = parsedURL.User.Username()
	}

	dbName := unknownDbName
	c.lastKnownTxnStatus = unknownTxnStatus

	wantDbStateInPrompt := rePromptDbState.MatchString(c.customPromptPattern)
	if wantDbStateInPrompt || c.smartPrompt {
		// Even if the prompt does not need it, the transaction status is needed
		// for the multi-line smart prompt.
		c.refreshTransactionStatus()
	}
	if wantDbStateInPrompt {
		// refreshDatabaseName() must be called *after* refreshTransactionStatus(),
		// even when %/ appears before %x in the prompt format.
		// This is because the database name should not be queried during
		// some transaction phases.
		dbName = c.refreshDatabaseName()
	}

	c.fullPrompt = rePromptFmt.ReplaceAllStringFunc(c.customPromptPattern, func(m string) string {
		switch m {
		case "%M":
			return parsedURL.Host // full host name.
		case "%m":
			return parsedURL.Hostname() // host name.
		case "%>":
			return parsedURL.Port() // port.
		case "%n": // user name.
			return userName
		case "%/": // database name.
			return dbName
		case "%x": // txn status.
			return c.lastKnownTxnStatus
		case "%%":
			return "%"
		default:
			err = fmt.Errorf("unrecognized format code in prompt: %q", m)
			return ""
		}

	})
	if err != nil {
		c.fullPrompt = err.Error()
	}

	c.fullPrompt += " "

	if len(c.fullPrompt) < 3 {
		c.continuePrompt = "> "
	} else {
		// continued statement prompt is: "        -> ".
		c.continuePrompt = strings.Repeat(" ", len(c.fullPrompt)-3) + "-> "
	}

	switch c.useContinuePrompt {
	case true:
		c.currentPrompt = c.continuePrompt
	case false:
		c.currentPrompt = c.fullPrompt
	}

	// Configure the editor to use the new prompt.
	c.ins.SetLeftPrompt(c.currentPrompt)

	return nextState
}

// refreshTransactionStatus retrieves and sets the current transaction status.
func (c *cliState) refreshTransactionStatus() {
	c.lastKnownTxnStatus = unknownTxnStatus

	dbVal, hasVal := c.conn.getServerValue("transaction status", `SHOW TRANSACTION STATUS`)
	if !hasVal {
		return
	}

	txnString := formatVal(dbVal,
		false /* showPrintableUnicode */, false /* shownewLinesAndTabs */)

	// Change the prompt based on the response from the server.
	switch txnString {
	case sql.NoTxnStr:
		c.lastKnownTxnStatus = ""
	case sql.AbortedStateStr:
		c.lastKnownTxnStatus = " ERROR"
	case sql.CommitWaitStateStr:
		c.lastKnownTxnStatus = "  DONE"
	case sql.RestartWaitStateStr:
		c.lastKnownTxnStatus = " RETRY"
	case sql.OpenStateStr:
		// The state AutoRetry is reported by the server as Open, so no need to
		// handle it here.
		c.lastKnownTxnStatus = "  OPEN"
	}
}

// refreshDatabaseName retrieves the current database name from the server.
// The database name is only queried if there is no transaction ongoing,
// or the transaction is fully open.
func (c *cliState) refreshDatabaseName() string {
	if !(c.lastKnownTxnStatus == "" /*NoTxn*/ ||
		c.lastKnownTxnStatus == "  OPEN" ||
		c.lastKnownTxnStatus == unknownTxnStatus) {
		return unknownDbName
	}

	dbVal, hasVal := c.conn.getServerValue("database name", `SHOW DATABASE`)
	if !hasVal {
		return unknownDbName
	}

	if dbVal == "" {
		// Attempt to be helpful to new users.
		fmt.Fprintln(stderr, "warning: no current database set."+
			" Use SET database = <dbname> to change, CREATE DATABASE to make a new database.")
	}

	dbName := formatVal(dbVal.(string),
		false /* showPrintableUnicode */, false /* shownewLinesAndTabs */)

	// Preserve the current database name in case of reconnects.
	c.conn.dbName = dbName

	return dbName
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
		if !sqlCtx.debugMode {
			// Also, try to enable syntax checking if supported by the server.
			// This is a form of client-side error checking to help with large txns.
			c.tryEnableCheckSyntax()
		}

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

		// Default prompt is part of the connection URL. eg: "marc@localhost:26257>".
		c.customPromptPattern = defaultPromptPattern
		if sqlCtx.debugMode {
			c.customPromptPattern = debugPromptPattern
		}

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

	c.useContinuePrompt = false

	return nextState
}

func (c *cliState) doContinueLine(nextState cliStateEnum) cliStateEnum {
	c.atEOF = false

	c.useContinuePrompt = true

	return nextState
}

// doReadline reads a line of input and check the input status.  If
// input was successful it populates c.lastInputLine.  Otherwise
// c.exitErr is set in some cases and an error/retry state is returned.
func (c *cliState) doReadLine(nextState cliStateEnum) cliStateEnum {
	if len(c.forwardLines) > 0 {
		// Are there some lines accumulated from a previous multi-line
		// readline input? If so, consume one.
		c.lastInputLine = c.forwardLines[0]
		c.forwardLines = c.forwardLines[1:]
		return nextState
	}

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
		// Do we have multiple lines of input?
		lines := strings.Split(l, "\n")
		if len(lines) > 1 {
			// Yes: only keep the first one for now, queue the remainder for
			// next time the shell needs a line.
			l = lines[0]
			c.forwardLines = lines[1:]
		}
		// In any case, process one line.

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

	case "help":
		printCliHelp()
		return startState

	case "exit", "quit":
		return cliStop
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

	lastTok, ok := parser.LastLexicalToken(c.concatLines)
	endOfStmt := isEndOfStatement(lastTok)
	if c.partialStmtsLen == 0 && !ok {
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
			stdout := os.Stdout
			if c.hasEditor() {
				stdout = c.ins.Stdout()
			}
			fmt.Fprintf(stdout,
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

	// When the smart prompt is enabled, we make some additional effort
	// to help the user: if the entry so far is starting an incomplete
	// transaction, push the user to enter input over multiple lines.
	if c.smartPrompt &&
		c.lastKnownTxnStatus == "" && endsWithIncompleteTxn(parsedStmts) && c.lastInputLine != "" {
		if c.partialStmtsLen == 0 {
			fmt.Fprintln(stderr, "Now adding input for a multi-line SQL transaction client-side (smart_prompt enabled).\n"+
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

	// Are we tracing?
	if c.autoTrace != "" {
		// Clear the trace by disabling tracing, then restart tracing
		// with the specified options.
		c.exitErr = c.conn.Exec("SET tracing = off; SET tracing = "+c.autoTrace, nil)
		if c.exitErr != nil {
			fmt.Fprintln(stderr, c.exitErr)
			maybeShowErrorDetails(stderr, c.exitErr, false)
			if c.errExit {
				return cliStop
			}
			return nextState
		}
	}

	// Now run the statement/query.
	c.exitErr = runQueryAndFormatResults(c.conn, os.Stdout, makeQuery(c.concatLines))
	if c.exitErr != nil {
		fmt.Fprintln(stderr, c.exitErr)
		maybeShowErrorDetails(stderr, c.exitErr, false)
	}

	// If we are tracing, stop tracing and display the trace. We do
	// this even if there was an error: a trace on errors is useful.
	if c.autoTrace != "" {
		// First, disable tracing.
		if err := c.conn.Exec("SET tracing = off", nil); err != nil {
			// Print the error for the SET tracing statement. This will
			// appear below the error for the main query above, if any,
			fmt.Fprintln(stderr, err)
			maybeShowErrorDetails(stderr, err, false)
			if c.exitErr == nil {
				// The query had encountered no error above, but now we are
				// encountering an error on SET tracing. Consider this to
				// become the query's error.
				c.exitErr = err
			}
			// If the query above had encountered an error already
			// (c.exitErr != nil), we keep that as the main error for the
			// shell.
		} else {
			traceType := ""
			if strings.Contains(c.autoTrace, "kv") {
				traceType = "kv"
			}
			if err := runQueryAndFormatResults(c.conn, os.Stdout,
				makeQuery(fmt.Sprintf("SHOW %s TRACE FOR SESSION", traceType))); err != nil {
				fmt.Fprintln(stderr, err)
				maybeShowErrorDetails(stderr, err, false)
				if c.exitErr == nil {
					// Both the query and SET tracing had encountered no error
					// above, but now we are encountering an error on SHOW TRACE
					// Consider this to become the query's error.
					c.exitErr = err
				}
				// If the query above or SET tracing had encountered an error
				// already (c.exitErr != nil), we keep that as the main error
				// for the shell.
			}
		}
	}

	if c.exitErr != nil && c.errExit {
		return cliStop
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
	} else {
		pgErr := pgerror.Flatten(err)
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
			if len(sqlCtx.setStmts) > 0 {
				// Execute any \set commands to allow setting client variables
				// before statement execution non-interactive mode.
				for i := range sqlCtx.setStmts {
					if c.handleSet(sqlCtx.setStmts[i:i+1], cliStart, cliStop) == cliStop {
						return c.exitErr
					}
				}
			}

			if len(sqlCtx.execStmts) > 0 {
				// Single-line sql; run as simple as possible, without noise on stdout.
				return c.runStatements(sqlCtx.execStmts)
			}

			if cliCtx.terminalOutput {
				// If results are shown on a terminal also enable printing of
				// times by default.
				sqlCtx.showTimes = true
			}
			if cliCtx.isInteractive && !sqlCtx.debugMode {
				// If the terminal is interactive and this was not explicitly disabled by setting the debug mode,
				// enable the smart prompt.
				c.smartPrompt = true
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

			state = c.doStart(cliStartLine)

		case cliRefreshPrompt:
			state = c.doRefreshPrompts(cliReadLine)

		case cliStartLine:
			state = c.doStartLine(cliRefreshPrompt)

		case cliContinueLine:
			state = c.doContinueLine(cliRefreshPrompt)

		case cliReadLine:
			state = c.doReadLine(cliDecidePath)

		case cliDecidePath:
			state = c.doDecidePath()

		case cliProcessFirstLine:
			state = c.doProcessFirstLine(cliStartLine, cliHandleCliCmd)

		case cliHandleCliCmd:
			state = c.doHandleCliCmd(cliRefreshPrompt, cliPrepareStatementLine)

		case cliPrepareStatementLine:
			state = c.doPrepareStatementLine(
				cliStartLine, cliContinueLine, cliCheckStatement, cliRunStatement,
			)

		case cliCheckStatement:
			state = c.doCheckStatement(cliStartLine, cliContinueLine, cliRunStatement)

		case cliRunStatement:
			state = c.doRunStatement(cliStartLine)

		default:
			panic(fmt.Sprintf("unknown state: %d", state))
		}
	}

	return c.exitErr
}

// runOneStatement executes one statement and terminates
// on error.
func (c *cliState) runStatements(stmts []string) error {
	for i, stmt := range stmts {
		// We do not use the logic from doRunStatement here
		// because we need a different error handling mechanism:
		// the error, if any, must not be printed to stderr if
		// we are returning directly.
		c.exitErr = runQueryAndFormatResults(c.conn, os.Stdout, makeQuery(stmt))
		if c.exitErr != nil {
			if !c.errExit && i < len(stmts)-1 {
				// Print the error now because we don't get a chance later.
				fmt.Fprintln(stderr, c.exitErr)
				maybeShowErrorDetails(stderr, c.exitErr, false)
			}
			if c.errExit {
				break
			}
		}
	}

	if c.exitErr != nil {
		// Don't write the error to stderr ourselves. Cobra will do this for
		// us on the exit path. We do want the details though, so add them.
		var buf bytes.Buffer
		fmt.Fprintln(&buf, c.exitErr)
		maybeShowErrorDetails(&buf, c.exitErr, false)
		c.exitErr = errors.New(strings.TrimSuffix(buf.String(), "\n"))
	}
	return c.exitErr
}

// checkInteractive sets the isInteractive parameter depending on the
// execution environment and the presence of -e flags.
func checkInteractive() {
	// We don't consider sessions interactives unless we have a
	// serious hunch they are. For now, only `cockroach sql` *without*
	// `-e` has the ability to input from a (presumably) human user,
	// and we'll also assume that there is no human if the standard
	// input is not terminal-like -- likely redirected from a file,
	// etc.
	cliCtx.isInteractive = len(sqlCtx.execStmts) == 0 && isatty.IsTerminal(os.Stdin.Fd())
}

func runTerm(cmd *cobra.Command, args []string) error {
	checkInteractive()

	if cliCtx.isInteractive {
		// The user only gets to see the info screen on interactive sessions.
		fmt.Print(infoMessage)
	}

	conn, err := getPasswordAndMakeSQLClient("cockroach sql")
	if err != nil {
		return err
	}
	defer conn.Close()

	return runClient(cmd, conn)
}

func runClient(cmd *cobra.Command, conn *sqlConn) error {
	// Open the connection to make sure everything is OK before running any
	// statements. Performs authentication.
	if err := conn.ensureConn(); err != nil {
		return err
	}

	// Enable safe updates, unless disabled.
	setupSafeUpdates(cmd, conn)

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
		fmt.Fprintf(stderr, "warning: cannot enable check_syntax: %v\n", err)
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
			err = pgerror.New(string(pqErr.Code), pqErr.Message)
			err = errors.WithHint(err, pqErr.Hint)
			err = errors.WithDetail(err, pqErr.Detail)
			pgErr := pgerror.Flatten(err)
			return nil, pgErr
		}
		return nil, pgerror.Flatten(pgerror.Newf(pgerror.CodeDataExceptionError,
			"unexpected error: %v", err))
	}

	if len(cols) < 2 {
		return nil, pgerror.Flatten(pgerror.Newf(pgerror.CodeDataExceptionError,
			"invalid results for SHOW SYNTAX: %q %q", cols, rows))
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
		err := pgerror.New(code, message)
		err = errors.WithHint(err, hint)
		err = errors.WithDetail(err, detail)
		return nil, pgerror.Flatten(err)
	}

	// Otherwise, hopefully we got some SQL statements.
	stmts = make([]string, len(rows))
	for i := range rows {
		if rows[i][0] != "sql" {
			return nil, pgerror.Flatten(pgerror.Newf(pgerror.CodeDataExceptionError,
				"invalid results for SHOW SYNTAX: %q %q", cols, rows))
		}
		stmts[i] = rows[i][1]
	}
	return stmts, nil
}
