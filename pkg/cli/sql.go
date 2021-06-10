// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	readline "github.com/knz/go-libedit"
	isatty "github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
)

const (
	// Refer to README.md to understand the general design guidelines for
	// help texts.

	welcomeMessage = `#
# Welcome to the CockroachDB SQL shell.
# All statements must be terminated by a semicolon.
# To exit, type: \q.
#
`
	helpMessageFmt = `You are using 'cockroach sql', CockroachDB's lightweight SQL client.
General
  \q, quit, exit    exit the shell (Ctrl+C/Ctrl+D also supported).

Help
  \? or "help"      print this help.
  \h [NAME]         help on syntax of SQL commands.
  \hf [NAME]        help on SQL built-in functions.

Query Buffer
  \p                during a multi-line statement, show the SQL entered so far.
  \r                during a multi-line statement, erase all the SQL entered so far.
  \| CMD            run an external command and run its output as SQL statements.

Connection
  \c, \connect [DB] connect to a new database

Input/Output
  \echo [STRING]    write the provided string to standard output.
  \i                execute commands from the specified file.
  \ir               as \i, but relative to the location of the current script.

Informational
  \l                list all databases in the CockroachDB cluster.
  \dt               show the tables of the current schema in the current database.
  \dT               show the user defined types of the current database.
  \du               list the users for all databases.
  \d [TABLE]        show details about columns in the specified table, or alias for '\dt' if no table is specified.

Formatting
  \x [on|off]       toggle records display format.

Operating System
  \! CMD            run an external command and print its results on standard output.

Configuration
  \set [NAME]       set a client-side flag or (without argument) print the current settings.
  \unset NAME       unset a flag.

%s
More documentation about our SQL dialect and the CLI shell is available online:
%s
%s`

	demoCommandsHelp = `
Commands specific to the demo shell (EXPERIMENTAL):
  \demo ls                     list the demo nodes and their connection URLs.
  \demo shutdown <nodeid>      stop a demo node.
  \demo restart <nodeid>       restart a stopped demo node.
  \demo decommission <nodeid>  decommission a node.
  \demo recommission <nodeid>  recommission a node.
  \demo add <locality>         add a node (locality specified as "region=<region>,zone=<zone>").
`

	defaultPromptPattern = "%n@%M/%/%x>"

	// debugPromptPattern avoids substitution patterns that require a db roundtrip.
	debugPromptPattern = "%n@%M>"
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
//
// Note: options customizable via \set and \unset should be defined in
// sqlCtx or cliCtx instead, so that the configuration remains globals
// across multiple instances of cliState (e.g. across file inclusion
// with \i).
type cliState struct {
	conn *sqlConn
	// ins is used to read lines if isInteractive is true.
	ins readline.EditLine
	// buf is used to read lines if isInteractive is false.
	buf *bufio.Reader

	// levels is the number of inclusion recursion levels.
	levels int
	// includeDir is the directory relative to which relative
	// includes (\ir) resolve the file name.
	includeDir string

	// The prompt at the beginning of a multi-line entry.
	fullPrompt string
	// The prompt on a continuation line in a multi-line entry.
	continuePrompt string
	// Which prompt to use to populate currentPrompt.
	useContinuePrompt bool
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
	// doPrepareStatementLine and then reused in doRunStatements() and
	// doCheckStatement().
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
func (c *cliState) printCliHelp() {
	demoHelpStr := ""
	if demoCtx.transientCluster != nil {
		demoHelpStr = demoCommandsHelp
	}
	fmt.Printf(helpMessageFmt,
		demoHelpStr,
		docs.URL("sql-statements.html"),
		docs.URL("use-the-built-in-sql-client.html"),
	)
	fmt.Println()
}

const noLineEditor readline.EditLine = -1

func (c *cliState) hasEditor() bool {
	return c.ins != noLineEditor
}

// addHistory persists a line of input to the readline history file.
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
	c.exitErr = errors.Newf("cannot change option during multi-line editing: %s\n", opt)
	fmt.Fprintln(stderr, c.exitErr)
	return nextState
}

func (c *cliState) internalServerError(nextState cliStateEnum, err error) cliStateEnum {
	fmt.Fprintf(stderr, "internal server error: %v\n", err)
	c.exitErr = err
	return nextState
}

var options = map[string]struct {
	description               string
	isBoolean                 bool
	validDuringMultilineEntry bool
	set                       func(val string) error
	reset                     func() error
	// display is used to retrieve the current value.
	display    func() string
	deprecated bool
}{
	`auto_trace`: {
		description:               "automatically run statement tracing on each executed statement",
		isBoolean:                 false,
		validDuringMultilineEntry: false,
		set: func(val string) error {
			b, err := parseBool(val)
			if err != nil {
				sqlCtx.autoTrace = "on, " + val
			} else if b {
				sqlCtx.autoTrace = "on, on"
			} else {
				sqlCtx.autoTrace = ""
			}
			return nil
		},
		reset: func() error {
			sqlCtx.autoTrace = ""
			return nil
		},
		display: func() string {
			if sqlCtx.autoTrace == "" {
				return "off"
			}
			return sqlCtx.autoTrace
		},
	},
	`border`: {
		description:               "the border style for the display format 'table'",
		isBoolean:                 false,
		validDuringMultilineEntry: true,
		set: func(val string) error {
			v, err := strconv.Atoi(val)
			if err != nil {
				return err
			}
			if v < 0 || v > 3 {
				return errors.New("only values between 0 and 4 are supported")
			}
			cliCtx.tableBorderMode = v
			return nil
		},
		display: func() string { return strconv.Itoa(cliCtx.tableBorderMode) },
	},
	`display_format`: {
		description:               "the output format for tabular data (table, csv, tsv, html, sql, records, raw)",
		isBoolean:                 false,
		validDuringMultilineEntry: true,
		set: func(val string) error {
			return cliCtx.tableDisplayFormat.Set(val)
		},
		reset: func() error {
			displayFormat := tableDisplayTSV
			if cliCtx.terminalOutput {
				displayFormat = tableDisplayTable
			}
			cliCtx.tableDisplayFormat = displayFormat
			return nil
		},
		display: func() string { return cliCtx.tableDisplayFormat.String() },
	},
	`echo`: {
		description:               "show SQL queries before they are sent to the server",
		isBoolean:                 true,
		validDuringMultilineEntry: false,
		set:                       func(_ string) error { sqlCtx.echo = true; return nil },
		reset:                     func() error { sqlCtx.echo = false; return nil },
		display:                   func() string { return strconv.FormatBool(sqlCtx.echo) },
	},
	`errexit`: {
		description:               "exit the shell upon a query error",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(_ string) error { sqlCtx.errExit = true; return nil },
		reset:                     func() error { sqlCtx.errExit = false; return nil },
		display:                   func() string { return strconv.FormatBool(sqlCtx.errExit) },
	},
	`check_syntax`: {
		description:               "check the SQL syntax before running a query",
		isBoolean:                 true,
		validDuringMultilineEntry: false,
		set:                       func(_ string) error { sqlCtx.checkSyntax = true; return nil },
		reset:                     func() error { sqlCtx.checkSyntax = false; return nil },
		display:                   func() string { return strconv.FormatBool(sqlCtx.checkSyntax) },
	},
	`show_times`: {
		description:               "display the execution time after each query",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(_ string) error { sqlCtx.showTimes = true; return nil },
		reset:                     func() error { sqlCtx.showTimes = false; return nil },
		display:                   func() string { return strconv.FormatBool(sqlCtx.showTimes) },
	},
	`show_server_times`: {
		description:               "display the server execution times for queries (requires show_times to be set)",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(_ string) error { sqlCtx.enableServerExecutionTimings = true; return nil },
		reset:                     func() error { sqlCtx.enableServerExecutionTimings = false; return nil },
		display:                   func() string { return strconv.FormatBool(sqlCtx.enableServerExecutionTimings) },
	},
	`verbose_times`: {
		description:               "display execution times with more precision (requires show_times to be set)",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(_ string) error { sqlCtx.verboseTimings = true; return nil },
		reset:                     func() error { sqlCtx.verboseTimings = false; return nil },
		display:                   func() string { return strconv.FormatBool(sqlCtx.verboseTimings) },
	},
	`smart_prompt`: {
		description:               "deprecated",
		isBoolean:                 true,
		validDuringMultilineEntry: false,
		set:                       func(_ string) error { return nil },
		reset:                     func() error { return nil },
		display:                   func() string { return "false" },
		deprecated:                true,
	},
	`prompt1`: {
		description:               "prompt string to use before each command (the following are expanded: %M full host, %m host, %> port number, %n user, %/ database, %x txn status)",
		isBoolean:                 false,
		validDuringMultilineEntry: true,
		set: func(val string) error {
			sqlCtx.customPromptPattern = val
			return nil
		},
		reset: func() error {
			sqlCtx.customPromptPattern = defaultPromptPattern
			return nil
		},
		display: func() string { return sqlCtx.customPromptPattern },
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
			if options[n].deprecated {
				continue
			}
			optData = append(optData, []string{n, options[n].display(), options[n].description})
		}
		err := PrintQueryOutput(os.Stdout,
			[]string{"Option", "Value", "Description"},
			NewRowSliceIter(optData, "lll" /*align*/))
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
		err = opt.set(val)
	} else if b, e := parseBool(val); e != nil {
		return c.invalidOptSet(errState, args)
	} else if b {
		err = opt.set("true")
	} else {
		err = opt.reset()
	}

	if err != nil {
		fmt.Fprintf(stderr, "\\set %s: %v\n", strings.Join(args, " "), err)
		c.exitErr = err
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
	if err := opt.reset(); err != nil {
		fmt.Fprintf(stderr, "\\unset %s: %v\n", args[0], err)
		c.exitErr = err
		return errState
	}
	return nextState
}

func isEndOfStatement(lastTok int) bool {
	return lastTok == ';' || lastTok == parser.HELPTOKEN
}

// handleDemo handles operations on \demo.
// This can only be done from `cockroach demo`.
func (c *cliState) handleDemo(cmd []string, nextState, errState cliStateEnum) cliStateEnum {
	// A transient cluster signifies the presence of `cockroach demo`.
	if demoCtx.transientCluster == nil {
		return c.invalidSyntax(errState, `\demo can only be run with cockroach demo`)
	}

	// The \demo command has one of three patterns:
	//
	//	- A lone command (currently, only ls)
	//	- A command followed by a string (add followed by locality string)
	//	- A command followed by a node number (shutdown, restart, decommission, recommission)
	//
	// We parse these commands separately, in the following blocks.
	if len(cmd) == 1 && cmd[0] == "ls" {
		demoCtx.transientCluster.listDemoNodes(os.Stdout, false /* justOne */)
		return nextState
	}

	if len(cmd) != 2 {
		return c.invalidSyntax(errState, `\demo expects 2 parameters, but passed %v`, len(cmd))
	}

	// Special case the add command it takes a string instead of a node number.
	if cmd[0] == "add" {
		return c.handleDemoAddNode(cmd, nextState, errState)
	}

	// If we've made it down here, we're handling the remaining demo node commands.
	return c.handleDemoNodeCommands(cmd, nextState, errState)
}

// handleDemoAddNode handles the `add` node command in demo.
func (c *cliState) handleDemoAddNode(cmd []string, nextState, errState cliStateEnum) cliStateEnum {
	if cmd[0] != "add" {
		return c.internalServerError(errState, fmt.Errorf("bad call to handleDemoAddNode"))
	}

	if err := demoCtx.transientCluster.AddNode(context.Background(), cmd[1]); err != nil {
		return c.internalServerError(errState, err)
	}
	addedNodeID := len(demoCtx.transientCluster.servers)
	fmt.Printf("node %v has been added with locality \"%s\"\n",
		addedNodeID, demoCtx.localities[addedNodeID-1].String())
	return nextState
}

// handleDemoNodeCommands handles the node commands in demo (with the exception of `add` which is handled
// with handleDemoAddNode.
func (c *cliState) handleDemoNodeCommands(
	cmd []string, nextState, errState cliStateEnum,
) cliStateEnum {
	nodeID, err := strconv.ParseInt(cmd[1], 10, 32)
	if err != nil {
		return c.invalidSyntax(
			errState,
			"%s",
			errors.Wrapf(err, "cannot convert %s to string", cmd[1]),
		)
	}

	switch cmd[0] {
	case "shutdown":
		if err := demoCtx.transientCluster.DrainAndShutdown(roachpb.NodeID(nodeID)); err != nil {
			return c.internalServerError(errState, err)
		}
		fmt.Printf("node %d has been shutdown\n", nodeID)
		return nextState
	case "restart":
		if err := demoCtx.transientCluster.RestartNode(roachpb.NodeID(nodeID)); err != nil {
			return c.internalServerError(errState, err)
		}
		fmt.Printf("node %d has been restarted\n", nodeID)
		return nextState
	case "recommission":
		if err := demoCtx.transientCluster.Recommission(roachpb.NodeID(nodeID)); err != nil {
			return c.internalServerError(errState, err)
		}
		fmt.Printf("node %d has been recommissioned\n", nodeID)
		return nextState
	case "decommission":
		if err := demoCtx.transientCluster.Decommission(roachpb.NodeID(nodeID)); err != nil {
			return c.internalServerError(errState, err)
		}
		fmt.Printf("node %d has been decommissioned\n", nodeID)
		return nextState
	}
	return c.invalidSyntax(errState, `command not recognized: %s`, cmd[0])
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
			c.exitErr = errors.New("no help available")
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
		helpText, _ := c.serverSideParse(fmt.Sprintf("select %s(??", funcName))
		if helpText != "" {
			fmt.Println(helpText)
		} else {
			fmt.Fprintf(stderr,
				"no help available for %q.\nTry \\hf with no argument to see available help.\n", funcName)
			c.exitErr = errors.New("no help available")
			return errState
		}
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

	if c.useContinuePrompt {
		if len(c.fullPrompt) < 3 {
			c.continuePrompt = "> "
		} else {
			// continued statement prompt is: "        -> ".
			c.continuePrompt = strings.Repeat(" ", len(c.fullPrompt)-3) + "-> "
		}

		c.ins.SetLeftPrompt(c.continuePrompt)
		return nextState
	}

	// Configure the editor to use the new prompt.

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

	wantDbStateInPrompt := rePromptDbState.MatchString(sqlCtx.customPromptPattern)
	if wantDbStateInPrompt {
		c.refreshTransactionStatus()
		// refreshDatabaseName() must be called *after* refreshTransactionStatus(),
		// even when %/ appears before %x in the prompt format.
		// This is because the database name should not be queried during
		// some transaction phases.
		dbName = c.refreshDatabaseName()
	}

	c.fullPrompt = rePromptFmt.ReplaceAllStringFunc(sqlCtx.customPromptPattern, func(m string) string {
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
	c.currentPrompt = c.fullPrompt

	// Configure the editor to use the new prompt.
	c.ins.SetLeftPrompt(c.currentPrompt)

	return nextState
}

// refreshTransactionStatus retrieves and sets the current transaction status.
func (c *cliState) refreshTransactionStatus() {
	c.lastKnownTxnStatus = unknownTxnStatus

	dbVal, dbColType, hasVal := c.conn.getServerValue("transaction status", `SHOW TRANSACTION STATUS`)
	if !hasVal {
		return
	}

	txnString := formatVal(dbVal, dbColType,
		false /* showPrintableUnicode */, false /* shownewLinesAndTabs */)

	// Change the prompt based on the response from the server.
	switch txnString {
	case sql.NoTxnStateStr:
		c.lastKnownTxnStatus = ""
	case sql.AbortedStateStr:
		c.lastKnownTxnStatus = " ERROR"
	case sql.CommitWaitStateStr:
		c.lastKnownTxnStatus = "  DONE"
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

	dbVal, dbColType, hasVal := c.conn.getServerValue("database name", `SHOW DATABASE`)
	if !hasVal {
		return unknownDbName
	}

	if dbVal == "" {
		// Attempt to be helpful to new users.
		fmt.Fprintln(stderr, "warning: no current database set."+
			" Use SET database = <dbname> to change, CREATE DATABASE to make a new database.")
	}

	dbName := formatVal(dbVal, dbColType,
		false /* showPrintableUnicode */, false /* shownewLinesAndTabs */)

	// Preserve the current database name in case of reconnects.
	c.conn.dbName = dbName

	return dbName
}

var cmdHistFile = envutil.EnvOrDefaultString("COCKROACH_SQL_CLI_HISTORY", ".cockroachsql_history")

// GetCompletions implements the readline.CompletionGenerator interface.
func (c *cliState) GetCompletions(_ string) []string {
	sql, _ := c.ins.GetLineInfo()

	if !strings.HasSuffix(sql, "??") {
		fmt.Fprintf(c.ins.Stdout(),
			"\ntab completion not supported; append '??' and press tab for contextual help\n\n")
	} else {
		helpText, err := c.serverSideParse(sql)
		if helpText != "" {
			// We have a completion suggestion. Use that.
			fmt.Fprintf(c.ins.Stdout(), "\nSuggestion:\n%s\n", helpText)
		} else if err != nil {
			// Some other error. Display it.
			fmt.Fprintln(c.ins.Stdout())
			cliOutputError(c.ins.Stdout(), err, true /*showSeverity*/, false /*verbose*/)
		}
	}

	// After the suggestion or error, re-display the prompt and current entry.
	fmt.Fprint(c.ins.Stdout(), c.currentPrompt, sql)
	return nil
}

func (c *cliState) doStart(nextState cliStateEnum) cliStateEnum {
	// Common initialization.
	c.partialLines = []string{}

	if cliCtx.isInteractive {
		fmt.Println("#\n# Enter \\? for a brief introduction.\n#")
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

	switch {
	case err == nil:
		// Do we have multiple lines of input?
		lines := strings.Split(l, "\n")
		if len(lines) > 1 {
			// Yes: only keep the first one for now, queue the remainder for
			// next time the shell needs a line.
			l = lines[0]
			c.forwardLines = lines[1:]
		}
		// In any case, process one line.

	case errors.Is(err, readline.ErrInterrupted):
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

	case errors.Is(err, io.EOF):
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
		c.printCliHelp()
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
	if sqlCtx.errExit {
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
		c.printCliHelp()

	case `\echo`:
		fmt.Println(strings.Join(cmd[1:], " "))

	case `\set`:
		return c.handleSet(cmd[1:], loopState, errState)

	case `\unset`:
		return c.handleUnset(cmd[1:], loopState, errState)

	case `\!`:
		return c.runSyscmd(c.lastInputLine, loopState, errState)

	case `\i`:
		return c.runInclude(cmd[1:], loopState, errState, false /* relative */)

	case `\ir`:
		return c.runInclude(cmd[1:], loopState, errState, true /* relative */)

	case `\p`:
		// This is analogous to \show but does not need a special case.
		// Implemented for compatibility with psql.
		fmt.Println(strings.Join(c.partialLines, "\n"))

	case `\r`:
		// Reset the input buffer so far. This is useful when e.g. a user
		// got confused with string delimiters and multi-line input.
		return cliStartLine

	case `\show`:
		fmt.Fprintln(stderr, `warning: \show is deprecated. Use \p.`)
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

	case `\l`:
		c.concatLines = `SHOW DATABASES`
		return cliRunStatement

	case `\dt`:
		c.concatLines = `SHOW TABLES`
		return cliRunStatement

	case `\dT`:
		c.concatLines = `SHOW TYPES`
		return cliRunStatement

	case `\du`:
		c.concatLines = `SHOW USERS`
		return cliRunStatement

	case `\d`:
		if len(cmd) == 1 {
			c.concatLines = `SHOW TABLES`
			return cliRunStatement
		} else if len(cmd) == 2 {
			c.concatLines = `SHOW COLUMNS FROM ` + cmd[1]
			return cliRunStatement
		}
		return c.invalidSyntax(errState, `%s. Try \? for help.`, c.lastInputLine)

	case `\connect`, `\c`:
		if len(cmd) == 2 {
			c.concatLines = `USE ` + cmd[1]
			return cliRunStatement

		}
		return c.invalidSyntax(errState, `%s. Try \? for help`, c.lastInputLine)

	case `\x`:
		format := tableDisplayRecords
		switch len(cmd) {
		case 1:
			if cliCtx.tableDisplayFormat == tableDisplayRecords {
				format = tableDisplayTable
			}
		case 2:
			b, err := parseBool(cmd[1])
			if err != nil {
				return c.invalidSyntax(errState, `%s. Try \? for help.`, c.lastInputLine)
			} else if b {
				format = tableDisplayRecords
			} else {
				format = tableDisplayTable
			}
		default:
			return c.invalidSyntax(errState, `%s. Try \? for help.`, c.lastInputLine)
		}
		cliCtx.tableDisplayFormat = format
		return loopState

	case `\demo`:
		return c.handleDemo(cmd[1:], loopState, errState)

	default:
		if strings.HasPrefix(cmd[0], `\d`) {
			// Unrecognized command for now, but we want to be helpful.
			fmt.Fprint(stderr, "Suggestion: use the SQL SHOW statement to inspect your schema.\n")
		}
		return c.invalidSyntax(errState, `%s. Try \? for help.`, c.lastInputLine)
	}

	return loopState
}

const maxRecursionLevels = 10

func (c *cliState) runInclude(
	cmd []string, contState, errState cliStateEnum, relative bool,
) (resState cliStateEnum) {
	if len(cmd) != 1 {
		return c.invalidSyntax(errState, `%s. Try \? for help.`, c.lastInputLine)
	}

	if c.levels >= maxRecursionLevels {
		c.exitErr = errors.Newf(`\i: too many recursion levels (max %d)`, maxRecursionLevels)
		fmt.Fprintf(stderr, "%v\n", c.exitErr)
		return errState
	}

	if len(c.partialLines) > 0 {
		return c.invalidSyntax(errState, `cannot use \i during multi-line entry.`)
	}

	filename := cmd[0]
	if !filepath.IsAbs(filename) && relative {
		// In relative mode, the filename is resolved relative to the
		// surrounding script.
		filename = filepath.Join(c.includeDir, filename)
	}

	f, err := os.Open(filename)
	if err != nil {
		fmt.Fprintln(stderr, err)
		c.exitErr = err
		return errState
	}
	// Close the file at the end.
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Fprintf(stderr, "error: closing %s: %v\n", filename, err)
			c.exitErr = errors.CombineErrors(c.exitErr, err)
			resState = errState
		}
	}()

	newState := cliState{
		conn:       c.conn,
		includeDir: filepath.Dir(filename),
		ins:        noLineEditor,
		buf:        bufio.NewReader(f),
		levels:     c.levels + 1,
	}

	if err := newState.doRunShell(cliStartLine, f); err != nil {
		// Note: a message was already printed on stderr at the point at
		// which the error originated. No need to repeat it here.
		c.exitErr = errors.Wrapf(err, "%v", filename)
		return errState
	}

	return contState
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

	if !sqlCtx.checkSyntax {
		return execState
	}

	return checkState
}

func (c *cliState) doCheckStatement(startState, contState, execState cliStateEnum) cliStateEnum {
	// From here on, client-side syntax checking is enabled.
	helpText, err := c.serverSideParse(c.concatLines)
	if err != nil {
		if helpText != "" {
			// There was a help text included. Use it.
			fmt.Println(helpText)
		}

		_ = c.invalidSyntax(cliStart, "statement ignored: %v",
			&formattedError{err: err, showSeverity: false, verbose: false})

		// Stop here if exiterr is set.
		if sqlCtx.errExit {
			return cliStop
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

	c.partialStmtsLen = len(c.partialLines)

	return nextState
}

// doRunStatements runs all the statements that have been accumulated by
// concatLines.
func (c *cliState) doRunStatements(nextState cliStateEnum) cliStateEnum {
	// Once we send something to the server, the txn status may change arbitrarily.
	// Clear the known state so that further entries do not assume anything.
	c.lastKnownTxnStatus = " ?"

	// Are we tracing?
	if sqlCtx.autoTrace != "" {
		// Clear the trace by disabling tracing, then restart tracing
		// with the specified options.
		c.exitErr = c.conn.Exec("SET tracing = off; SET tracing = "+sqlCtx.autoTrace, nil)
		if c.exitErr != nil {
			cliOutputError(stderr, c.exitErr, true /*showSeverity*/, false /*verbose*/)
			if sqlCtx.errExit {
				return cliStop
			}
			return nextState
		}
	}

	// Now run the statement/query.
	c.exitErr = runQueryAndFormatResults(c.conn, os.Stdout, makeQuery(c.concatLines))
	if c.exitErr != nil {
		cliOutputError(stderr, c.exitErr, true /*showSeverity*/, false /*verbose*/)
	}

	// If we are tracing, stop tracing and display the trace. We do
	// this even if there was an error: a trace on errors is useful.
	if sqlCtx.autoTrace != "" {
		// First, disable tracing.
		if err := c.conn.Exec("SET tracing = off", nil); err != nil {
			// Print the error for the SET tracing statement. This will
			// appear below the error for the main query above, if any,
			cliOutputError(stderr, err, true /*showSeverity*/, false /*verbose*/)
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
			if strings.Contains(sqlCtx.autoTrace, "kv") {
				traceType = "kv"
			}
			if err := runQueryAndFormatResults(c.conn, os.Stdout,
				makeQuery(fmt.Sprintf("SHOW %s TRACE FOR SESSION", traceType))); err != nil {
				cliOutputError(stderr, err, true /*showSeverity*/, false /*verbose*/)
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

	if c.exitErr != nil && sqlCtx.errExit {
		return cliStop
	}

	return nextState
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
func runInteractive(conn *sqlConn, cmdIn *os.File) (exitErr error) {
	c := cliState{
		conn:       conn,
		includeDir: ".",
	}
	return c.doRunShell(cliStart, cmdIn)
}

func (c *cliState) doRunShell(state cliStateEnum, cmdIn *os.File) (exitErr error) {
	for {
		if state == cliStop {
			break
		}
		switch state {
		case cliStart:
			cleanupFn, err := c.configurePreShellDefaults(cmdIn)
			defer cleanupFn()
			if err != nil {
				return err
			}
			if len(sqlCtx.execStmts) > 0 {
				// Single-line sql; run as simple as possible, without noise on stdout.
				if err := c.runStatements(sqlCtx.execStmts); err != nil {
					return err
				}
				if sqlCtx.quitAfterExecStmts {
					return nil
				}
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
			state = c.doRunStatements(cliStartLine)

		default:
			panic(fmt.Sprintf("unknown state: %d", state))
		}
	}

	return c.exitErr
}

// configurePreShellDefaults should be called after command-line flags
// have been loaded into the cliCtx/sqlCtx and .isInteractive /
// .terminalOutput have been initialized, but before the SQL shell or
// execution starts.
//
// The returned cleanupFn must be called even when the err return is
// not nil.
func (c *cliState) configurePreShellDefaults(cmdIn *os.File) (cleanupFn func(), err error) {
	if cliCtx.terminalOutput {
		// If results are shown on a terminal also enable printing of
		// times by default.
		sqlCtx.showTimes = true
	}

	if cliCtx.isInteractive {
		// If a human user is providing the input, we want to help them with
		// what they are entering:
		sqlCtx.errExit = false // let the user retry failing commands
		if !sqlCtx.debugMode {
			// Also, try to enable syntax checking if supported by the server.
			// This is a form of client-side error checking to help with large txns.
			sqlCtx.checkSyntax = true
		}
	} else {
		// When running non-interactive, by default we want errors to stop
		// further processing and we can just let syntax checking to be
		// done server-side to avoid client-side churn.
		sqlCtx.errExit = true
		sqlCtx.checkSyntax = false
		// We also don't need (smart) prompts at all.
	}

	// An interactive readline prompter is comparatively slow at
	// reading input, so we only use it in interactive mode and when
	// there is also a terminal on stdout.
	if cliCtx.isInteractive && cliCtx.terminalOutput {
		// The readline initialization is not placed in
		// the doStart() method because of the defer.
		c.ins, c.exitErr = readline.InitFiles("cockroach",
			true, /* wideChars */
			cmdIn, os.Stdout, stderr)
		if errors.Is(c.exitErr, readline.ErrWidecharNotSupported) {
			log.Warning(context.TODO(), "wide character support disabled")
			c.ins, c.exitErr = readline.InitFiles("cockroach",
				false, cmdIn, os.Stdout, stderr)
		}
		if c.exitErr != nil {
			return cleanupFn, c.exitErr
		}
		// If the user has used bind -v or bind -l in their ~/.editrc,
		// this will reset the standard bindings. However we really
		// want in this shell that Ctrl+C, tab, Ctrl+Z and Ctrl+R
		// always have the same meaning.  So reload these bindings
		// explicitly no matter what ~/.editrc may have changed.
		c.ins.RebindControlKeys()
		cleanupFn = func() { c.ins.Close() }
	} else {
		c.ins = noLineEditor
		c.buf = bufio.NewReader(cmdIn)
		cleanupFn = func() {}
	}

	if c.hasEditor() {
		// We only enable prompt and history management when the
		// interactive input prompter is enabled. This saves on churn and
		// memory when e.g. piping a large SQL script through the
		// command-line client.

		// Default prompt is part of the connection URL. eg: "marc@localhost:26257>".
		sqlCtx.customPromptPattern = defaultPromptPattern
		if sqlCtx.debugMode {
			sqlCtx.customPromptPattern = debugPromptPattern
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

	// If any --set flags were set through the command line,
	// synthetize '-e set=xxx' statements for them at the beginning.
	sqlCtx.quitAfterExecStmts = len(sqlCtx.execStmts) > 0
	if len(sqlCtx.setStmts) > 0 {
		setStmts := make([]string, 0, len(sqlCtx.setStmts)+len(sqlCtx.execStmts))
		for _, s := range sqlCtx.setStmts {
			setStmts = append(setStmts, `\set `+s)
		}
		sqlCtx.setStmts = nil
		sqlCtx.execStmts = append(setStmts, sqlCtx.execStmts...)
	}

	return cleanupFn, nil
}

// runStatements executes the given statements and terminates
// on error.
func (c *cliState) runStatements(stmts []string) error {
	for {
		for i, stmt := range stmts {
			// We do not use the logic from doRunStatement here
			// because we need a different error handling mechanism:
			// the error, if any, must not be printed to stderr if
			// we are returning directly.
			if strings.HasPrefix(stmt, `\`) {
				// doHandleCliCmd takes its input from c.lastInputLine.
				c.lastInputLine = stmt
				if nextState := c.doHandleCliCmd(cliRunStatement, cliStop); nextState == cliStop && c.exitErr == nil {
					// The client-side command failed with an error, but
					// did not populate c.exitErr itself. Do it here.
					c.exitErr = errors.New("error in client-side command")
				}
			} else {
				c.exitErr = runQueryAndFormatResults(c.conn, os.Stdout, makeQuery(stmt))
			}
			if c.exitErr != nil {
				if !sqlCtx.errExit && i < len(stmts)-1 {
					// Print the error now because we don't get a chance later.
					cliOutputError(stderr, c.exitErr, true /*showSeverity*/, false /*verbose*/)
				}
				if sqlCtx.errExit {
					break
				}
			}
		}
		// If --watch was specified and no error was encountered,
		// repeat.
		if sqlCtx.repeatDelay > 0 && c.exitErr == nil {
			time.Sleep(sqlCtx.repeatDelay)
			continue
		}
		break
	}

	if c.exitErr != nil {
		// Don't write the error to stderr ourselves. Cobra will do this for
		// us on the exit path. We do want the details though, so add them.
		c.exitErr = &formattedError{err: c.exitErr, showSeverity: true, verbose: false}
	}
	return c.exitErr
}

// checkInteractive sets the isInteractive parameter depending on the
// execution environment and the presence of -e flags.
func checkInteractive(stdin *os.File) {
	// We don't consider sessions interactives unless we have a
	// serious hunch they are. For now, only `cockroach sql` *without*
	// `-e` has the ability to input from a (presumably) human user,
	// and we'll also assume that there is no human if the standard
	// input is not terminal-like -- likely redirected from a file,
	// etc.
	cliCtx.isInteractive = len(sqlCtx.execStmts) == 0 && isatty.IsTerminal(stdin.Fd())
}

// getInputFile establishes where we are reading from.
func getInputFile() (cmdIn *os.File, closeFn func(), err error) {
	if sqlCtx.inputFile == "" {
		return os.Stdin, func() {}, nil
	}

	if len(sqlCtx.execStmts) != 0 {
		return nil, nil, errors.Newf("unsupported combination: --%s and --%s", cliflags.Execute.Name, cliflags.File.Name)
	}

	f, err := os.Open(sqlCtx.inputFile)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = f.Close() }, nil
}

func runTerm(cmd *cobra.Command, args []string) error {
	cmdIn, closeFn, err := getInputFile()
	if err != nil {
		return err
	}
	defer closeFn()

	checkInteractive(cmdIn)

	if cliCtx.isInteractive {
		// The user only gets to see the welcome message on interactive sessions.
		fmt.Print(welcomeMessage)
	}

	conn, err := makeSQLClient("cockroach sql", useDefaultDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	return runClient(cmd, conn, cmdIn)
}

func runClient(cmd *cobra.Command, conn *sqlConn, cmdIn *os.File) error {
	// Open the connection to make sure everything is OK before running any
	// statements. Performs authentication.
	if err := conn.ensureConn(); err != nil {
		return err
	}

	// Enable safe updates, unless disabled.
	setupSafeUpdates(cmd, conn)

	return runInteractive(conn, cmdIn)
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

// serverSideParse uses the SHOW SYNTAX statement to analyze the given string.
// If the syntax is correct, the function returns the statement
// decomposition in the first return value. If it is not, the function
// extracts a help string if available.
func (c *cliState) serverSideParse(sql string) (helpText string, err error) {
	cols, rows, err := runQuery(c.conn, makeQuery("SHOW SYNTAX "+lex.EscapeSQLString(sql)), true)
	if err != nil {
		// The query failed with some error. This is not a syntax error
		// detected by SHOW SYNTAX (those show up as valid rows) but
		// instead something else.
		return "", errors.Wrap(err, "unexpected error")
	}

	if len(cols) < 2 {
		return "", errors.Newf(
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
		// Is it a help text?
		if strings.HasPrefix(message, "help token in input") && strings.HasPrefix(hint, "help:") {
			// Yes: return it.
			helpText = hint[6:]
			hint = ""
		}
		// In any case report that there was an error while parsing.
		err := errors.Newf("%s", message)
		err = pgerror.WithCandidateCode(err, pgcode.MakeCode(code))
		if hint != "" {
			err = errors.WithHint(err, hint)
		}
		if detail != "" {
			err = errors.WithDetail(err, detail)
		}
		return helpText, err
	}
	return "", nil
}

func printlnUnlessEmbedded(args ...interface{}) {
	if !sqlCtx.embeddedMode {
		fmt.Println(args...)
	}
}

func printfUnlessEmbedded(f string, args ...interface{}) {
	if !sqlCtx.embeddedMode {
		fmt.Printf(f, args...)
	}
}
