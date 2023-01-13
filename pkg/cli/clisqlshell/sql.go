// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/pprompt"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlfsm"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/knz/bubbline/editline"
)

const (
	// Refer to README.md to understand the general design guidelines for
	// help texts.

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
  \info             display server details including connection strings.
  \c, \connect {[DB] [USER] [HOST] [PORT] | [URL]}
                    connect to a server or print the current connection URL.
                    (Omitted values reuse previous parameters. Use '-' to skip a field.)
  \password [USERNAME]
                    securely change the password for a user

Input/Output
  \echo [STRING]    write the provided string to standard output.
  \i                execute commands from the specified file.
  \ir               as \i, but relative to the location of the current script.
  \o [FILE]         send all query results to the specified file.
  \qecho [STRING]   write the provided string to the query output stream (see \o).

Informational
  \l                list all databases in the CockroachDB cluster.
  \dt               show the tables of the current schema in the current database.
  \dT               show the user defined types of the current database.
  \du [USER]        list the specified user, or list the users for all databases if no user is specified.
  \d [TABLE]        show details about columns in the specified table, or alias for '\dt' if no table is specified.
  \dd TABLE         show details about constraints on the specified table.
  \df               show the functions that are defined in the current database.

Formatting
  \x [on|off]       toggle records display format.

Operating System
  \! CMD            run an external command and print its results on standard output.

Configuration
  \set [NAME]       set a client-side flag or (without argument) print the current settings.
  \unset NAME       unset a flag.

Statement diagnostics
  \statement-diag list                               list available bundles.
  \statement-diag download <bundle-id> [<filename>]  download bundle.

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

// cliState defines the current state of the CLI during
// command-line processing.
//
// Note: options customizable via \set and \unset should be defined in
// sqlConnCtx or sqlCtx instead, so that the configuration remains global
// across multiple instances of cliState (e.g. across file inclusion
// with \i).
type cliState struct {
	cliCtx     *clicfg.Context
	sqlConnCtx *clisqlclient.Context
	sqlExecCtx *clisqlexec.Context
	sqlCtx     *Context
	iCtx       *internalContext

	conn clisqlclient.Conn
	// ins is used to read lines.
	ins editor
	// singleStatement is set to true when this state level
	// is currently processing for runString(). In that mode:
	// - a missing semicolon at the end of input is ignored:
	//   runString("select 1") is valid.
	// - errors are not printed by runStatement, since the
	//   caller of runString() is responsible for processing/
	//   printing the exitErr.
	singleStatement bool

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

	// State of COPY FROM on the client.
	copyFromState *clisqlclient.CopyFromState

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
	if c.sqlCtx.DemoCluster != nil {
		demoHelpStr = demoCommandsHelp
	}
	fmt.Fprintf(c.iCtx.stdout, helpMessageFmt,
		demoHelpStr,
		docs.URL("sql-statements.html"),
		docs.URL("use-the-built-in-sql-client.html"),
	)
	fmt.Fprintln(c.iCtx.stdout)
}

// addHistory persists a line of input to the readline history file.
func (c *cliState) addHistory(line string) {
	if len(line) == 0 {
		return
	}

	if err := c.ins.addHistory(line); err != nil {
		fmt.Fprintf(c.iCtx.stderr, "warning: cannot add entry to history: %v\n", err)
	}
}

func (c *cliState) invalidSyntax(nextState cliStateEnum) cliStateEnum {
	return c.invalidSyntaxf(nextState, `%s. Try \? for help.`, c.lastInputLine)
}

// inCopy implements the sqlShell interface (to support the editor).
func (c *cliState) inCopy() bool {
	return c.copyFromState != nil
}

func (c *cliState) resetCopy() {
	c.copyFromState = nil
}

func (c *cliState) invalidSyntaxf(
	nextState cliStateEnum, format string, args ...interface{},
) cliStateEnum {
	fmt.Fprint(c.iCtx.stderr, "invalid syntax: ")
	fmt.Fprintf(c.iCtx.stderr, format, args...)
	fmt.Fprintln(c.iCtx.stderr)
	c.exitErr = errInvalidSyntax
	return nextState
}

func (c *cliState) invalidOptionChange(nextState cliStateEnum, opt string) cliStateEnum {
	c.exitErr = errors.Newf("cannot change option during multi-line editing: %s\n", opt)
	fmt.Fprintln(c.iCtx.stderr, c.exitErr)
	return nextState
}

func (c *cliState) internalServerError(nextState cliStateEnum, err error) cliStateEnum {
	fmt.Fprintf(c.iCtx.stderr, "internal server error: %v\n", err)
	c.exitErr = err
	return nextState
}

var options = map[string]struct {
	description               string
	isBoolean                 bool
	validDuringMultilineEntry bool
	set                       func(c *cliState, val string) error
	reset                     func(c *cliState) error
	// display is used to retrieve the current value.
	display    func(c *cliState) string
	deprecated bool
}{
	`auto_trace`: {
		description:               "automatically run statement tracing on each executed statement",
		isBoolean:                 false,
		validDuringMultilineEntry: false,
		set: func(c *cliState, val string) error {
			b, err := clisqlclient.ParseBool(val)
			if err != nil {
				c.iCtx.autoTrace = "on, " + val
			} else if b {
				c.iCtx.autoTrace = "on, on"
			} else {
				c.iCtx.autoTrace = ""
			}
			return nil
		},
		reset: func(c *cliState) error {
			c.iCtx.autoTrace = ""
			return nil
		},
		display: func(c *cliState) string {
			if c.iCtx.autoTrace == "" {
				return "off"
			}
			return c.iCtx.autoTrace
		},
	},
	`border`: {
		description:               "the border style for the display format 'table'",
		isBoolean:                 false,
		validDuringMultilineEntry: true,
		set: func(c *cliState, val string) error {
			v, err := strconv.Atoi(val)
			if err != nil {
				return err
			}
			if v < 0 || v > 3 {
				return errors.New("only values between 0 and 4 are supported")
			}
			c.sqlExecCtx.TableBorderMode = v
			return nil
		},
		display: func(c *cliState) string { return strconv.Itoa(c.sqlExecCtx.TableBorderMode) },
	},
	`display_format`: {
		description:               "the output format for tabular data (table, csv, tsv, html, ndjson, sql, records, raw)",
		isBoolean:                 false,
		validDuringMultilineEntry: true,
		set: func(c *cliState, val string) error {
			return c.sqlExecCtx.TableDisplayFormat.Set(val)
		},
		reset: func(c *cliState) error {
			displayFormat := clisqlexec.TableDisplayTSV
			if c.sqlExecCtx.TerminalOutput {
				displayFormat = clisqlexec.TableDisplayTable
			}
			c.sqlExecCtx.TableDisplayFormat = displayFormat
			return nil
		},
		display: func(c *cliState) string { return c.sqlExecCtx.TableDisplayFormat.String() },
	},
	`echo`: {
		description:               "show SQL queries before they are sent to the server",
		isBoolean:                 true,
		validDuringMultilineEntry: false,
		set:                       func(c *cliState, _ string) error { c.sqlConnCtx.Echo = true; return nil },
		reset:                     func(c *cliState) error { c.sqlConnCtx.Echo = false; return nil },
		display:                   func(c *cliState) string { return strconv.FormatBool(c.sqlConnCtx.Echo) },
	},
	`errexit`: {
		description:               "exit the shell upon a query error",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(c *cliState, _ string) error { c.iCtx.errExit = true; return nil },
		reset:                     func(c *cliState) error { c.iCtx.errExit = false; return nil },
		display:                   func(c *cliState) string { return strconv.FormatBool(c.iCtx.errExit) },
	},
	`check_syntax`: {
		description:               "check the SQL syntax before running a query",
		isBoolean:                 true,
		validDuringMultilineEntry: false,
		set:                       func(c *cliState, _ string) error { c.iCtx.checkSyntax = true; return nil },
		reset:                     func(c *cliState) error { c.iCtx.checkSyntax = false; return nil },
		display:                   func(c *cliState) string { return strconv.FormatBool(c.iCtx.checkSyntax) },
	},
	`reflow_max_width`: {
		description: "maximum output width when reflowing input statements",
		set: func(c *cliState, v string) error {
			i, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			if i <= 0 {
				return errors.New("maximum width must be positive")
			}
			c.iCtx.reflowMaxWidth = i
			return nil
		},
		reset:   func(c *cliState) error { c.iCtx.reflowMaxWidth = defaultReflowMaxWidth; return nil },
		display: func(c *cliState) string { return strconv.Itoa(c.iCtx.reflowMaxWidth) },
	},
	`reflow_align_mode`: {
		description: "how to reflow SQL syntax (0 = no, 1 = partial, 2 = full, 3 = extra)",
		set: func(c *cliState, v string) error {
			i, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			if i < 0 || i > 3 {
				return errors.New("possible values: 0 = no, 1 = partial, 2 = full, 3 = extra")
			}
			c.iCtx.reflowAlignMode = i
			return nil
		},
		reset:   func(c *cliState) error { c.iCtx.reflowAlignMode = defaultReflowAlignMode; return nil },
		display: func(c *cliState) string { return strconv.Itoa(c.iCtx.reflowAlignMode) },
	},
	`reflow_case_mode`: {
		description: "how to change case during reflow (0 = lowercase, 1 = uppercase)",
		set: func(c *cliState, v string) error {
			i, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			if i < 0 || i > 1 {
				return errors.New("value must be 0 (lowercase) or 1 (uppercase)")
			}
			c.iCtx.reflowCaseMode = i
			return nil
		},
		reset:   func(c *cliState) error { c.iCtx.reflowCaseMode = defaultReflowCaseMode; return nil },
		display: func(c *cliState) string { return strconv.Itoa(c.iCtx.reflowCaseMode) },
	},
	`show_times`: {
		description:               "display the execution time after each query",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(c *cliState, _ string) error { c.sqlExecCtx.ShowTimes = true; return nil },
		reset:                     func(c *cliState) error { c.sqlExecCtx.ShowTimes = false; return nil },
		display:                   func(c *cliState) string { return strconv.FormatBool(c.sqlExecCtx.ShowTimes) },
	},
	`show_server_times`: {
		description:               "display the server execution times for queries (requires show_times to be set)",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(c *cliState, _ string) error { c.sqlConnCtx.EnableServerExecutionTimings = true; return nil },
		reset:                     func(c *cliState) error { c.sqlConnCtx.EnableServerExecutionTimings = false; return nil },
		display:                   func(c *cliState) string { return strconv.FormatBool(c.sqlConnCtx.EnableServerExecutionTimings) },
	},
	`verbose_times`: {
		description:               "display execution times with more precision (requires show_times to be set)",
		isBoolean:                 true,
		validDuringMultilineEntry: true,
		set:                       func(c *cliState, _ string) error { c.sqlExecCtx.VerboseTimings = true; return nil },
		reset:                     func(c *cliState) error { c.sqlExecCtx.VerboseTimings = false; return nil },
		display:                   func(c *cliState) string { return strconv.FormatBool(c.sqlExecCtx.VerboseTimings) },
	},
	`smart_prompt`: {
		description:               "deprecated",
		isBoolean:                 true,
		validDuringMultilineEntry: false,
		set:                       func(c *cliState, _ string) error { return nil },
		reset:                     func(c *cliState) error { return nil },
		display:                   func(c *cliState) string { return "false" },
		deprecated:                true,
	},
	`prompt1`: {
		description:               "prompt string to use before each command (the following are expanded: %M full host, %m host, %> port number, %n user, %/ database, %x txn status)",
		isBoolean:                 false,
		validDuringMultilineEntry: true,
		set: func(c *cliState, val string) error {
			c.iCtx.customPromptPattern = val
			return nil
		},
		reset: func(c *cliState) error {
			c.iCtx.customPromptPattern = defaultPromptPattern
			return nil
		},
		display: func(c *cliState) string { return c.iCtx.customPromptPattern },
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

var setArgsRe = regexp.MustCompile(`^\s*` + // zero or more leading space.
	`(?P<option>(?:[a-zA-Z_.0-9]|-)+)` + // 1 or more non-space option characters.
	`(?:` +
	`\s*(?:=\s*|\s+)` + // separator: either some spaces, or an equal sign surrounded by optional spaces.
	`(?P<value>` +
	`"(?:[^"]|\\.)*"` + // value is either a double-quoted string, which might be empty; or
	`|` +
	`[^\s"]+` + // value may be a string of non-space, non-quote characters.
	`))?` + // value, as a whole, is optional.
	`\s*$`, // either a standalone option or the value can be followed by optional spaces.
)

func getSetArgs(sargs string) (ok bool, option string, hasValue bool, value string) {
	m := setArgsRe.FindStringSubmatch(sargs)
	if m == nil {
		return false, "", false, ""
	}
	option = m[1]
	if len(m) == 2 || m[2] == "" {
		return true, option, false, ""
	}
	return true, option, true, m[2]
}

// handleSet supports the \set client-side command.
func (c *cliState) handleSet(args []string, nextState, errState cliStateEnum) cliStateEnum {
	if len(args) == 0 {
		optData := make([][]string, 0, len(options))
		for _, n := range optionNames {
			if options[n].deprecated {
				continue
			}
			optData = append(optData, []string{n, options[n].display(c), options[n].description})
		}
		err := c.sqlExecCtx.PrintQueryOutput(c.iCtx.stdout, c.iCtx.stderr,
			[]string{"Option", "Value", "Description"},
			clisqlexec.NewRowSliceIter(optData, "lll" /*align*/))
		if err != nil {
			panic(err)
		}

		return nextState
	}

	// For \set, we're not trusting the default field separator which
	// simply splits at spaces, because it doesn't help us handle
	// "a=b", "a = b", "a b" and "a =b" effectively.
	// Regular expressions to the rescue.
	sargs := strings.Join(args, " ")

	ok, optName, hasValue, val := getSetArgs(sargs)
	if !ok {
		return c.invalidSyntax(errState)
	}

	opt, ok := options[optName]
	if !ok {
		return c.invalidSyntax(errState)
	}
	if len(c.partialLines) > 0 && !opt.validDuringMultilineEntry {
		return c.invalidOptionChange(errState, optName)
	}

	var err error

	// Determine which value to use.
	if !hasValue {
		val = "true"
	} else if len(val) > 0 && val[0] == '"' {
		val, err = strconv.Unquote(val)
		if err != nil {
			return c.invalidSyntax(errState)
		}
	}

	// Run the command.
	if !opt.isBoolean {
		err = opt.set(c, val)
	} else if b, e := clisqlclient.ParseBool(val); e != nil {
		return c.invalidSyntax(errState)
	} else if b {
		err = opt.set(c, "true")
	} else {
		err = opt.reset(c)
	}

	if err != nil {
		fmt.Fprintf(c.iCtx.stderr, "\\set %s: %v\n", sargs, err)
		c.exitErr = err
		return errState
	}

	return nextState
}

// handleUnset supports the \unset client-side command.
func (c *cliState) handleUnset(args []string, nextState, errState cliStateEnum) cliStateEnum {
	if len(args) != 1 {
		return c.invalidSyntax(errState)
	}
	opt, ok := options[args[0]]
	if !ok {
		return c.invalidSyntax(errState)
	}
	if len(c.partialLines) > 0 && !opt.validDuringMultilineEntry {
		return c.invalidOptionChange(errState, args[0])
	}
	if err := opt.reset(c); err != nil {
		fmt.Fprintf(c.iCtx.stderr, "\\unset %s: %v\n", args[0], err)
		c.exitErr = err
		return errState
	}
	return nextState
}

func isEndOfStatement(lastTok int) bool {
	return lastTok == ';' || lastTok == lexbase.HELPTOKEN
}

// handleDemo handles operations on \demo.
// This can only be done from `cockroach demo`.
func (c *cliState) handleDemo(cmd []string, nextState, errState cliStateEnum) cliStateEnum {
	// A demo cluster signifies the presence of `cockroach demo`.
	if c.sqlCtx.DemoCluster == nil {
		return c.invalidSyntaxf(errState, `\demo can only be run with cockroach demo`)
	}

	// The \demo command has one of three patterns:
	//
	//	- A lone command (currently, only ls)
	//	- A command followed by a string (add followed by locality string)
	//	- A command followed by a node number (shutdown, restart, decommission, recommission)
	//
	// We parse these commands separately, in the following blocks.
	if len(cmd) == 1 && cmd[0] == "ls" {
		c.sqlCtx.DemoCluster.ListDemoNodes(c.iCtx.stdout, c.iCtx.stderr, false /* justOne */, true /* verbose */)
		return nextState
	}

	if len(cmd) != 2 {
		return c.invalidSyntax(errState)
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

	addedNodeID, err := c.sqlCtx.DemoCluster.AddNode(context.Background(), cmd[1])
	if err != nil {
		return c.internalServerError(errState, err)
	}
	fmt.Fprintf(c.iCtx.stdout, "node %v has been added with locality \"%s\"\n",
		addedNodeID, c.sqlCtx.DemoCluster.GetLocality(addedNodeID))
	return nextState
}

func (c *cliState) handleInfo(nextState cliStateEnum) (resState cliStateEnum) {
	w := c.iCtx.stdout
	si := c.conn.GetServerInfo()
	if si.ServerExecutableVersion != "" {
		fmt.Fprintf(w, "Server version: %s\n", si.ServerExecutableVersion)
	}
	if si.ClusterID != "" {
		fmt.Fprintf(w, "Cluster ID: %s\n", si.ClusterID)
	}
	if si.Organization != "" {
		fmt.Fprintf(w, "Organization: %s\n", si.Organization)
	}
	fmt.Fprintln(w)

	// Print the current connection URL, database string and username.
	// We hide the connection string in the demo case because we print it again below.
	if err := c.handleConnectInternal(nil, c.sqlCtx.DemoCluster != nil); err != nil {
		fmt.Fprintln(c.iCtx.stderr, err)
	}
	fmt.Fprintln(w)

	// If running a demo shell, also print out the server details.
	if c.sqlCtx.DemoCluster != nil {
		fmt.Fprintf(w, "You are connected to a demo cluster with %d node(s).\n", c.sqlCtx.DemoCluster.NumNodes())
		fmt.Fprintf(w, "Connection parameters (simplified):\n")
		c.sqlCtx.DemoCluster.ListDemoNodes(w, w, true /* justOne */, false /* verbose */)
		fmt.Fprintf(w, "Use the command '\\demo ls' to list nodes and detailed connection parameters to each.\n")
		fmt.Fprintln(w)
	}

	return nextState
}

// handleDemoNodeCommands handles the node commands in demo (with the exception of `add` which is handled
// with handleDemoAddNode.
func (c *cliState) handleDemoNodeCommands(
	cmd []string, nextState, errState cliStateEnum,
) cliStateEnum {
	nodeID, err := strconv.ParseInt(cmd[1], 10, 32)
	if err != nil {
		return c.invalidSyntaxf(
			errState,
			"%s",
			errors.Wrapf(err, "%q is not a valid node ID", cmd[1]),
		)
	}

	ctx := context.Background()

	switch cmd[0] {
	case "shutdown":
		if err := c.sqlCtx.DemoCluster.DrainAndShutdown(ctx, int32(nodeID)); err != nil {
			return c.internalServerError(errState, err)
		}
		fmt.Fprintf(c.iCtx.stdout, "node %d has been shutdown\n", nodeID)
		return nextState
	case "restart":
		if err := c.sqlCtx.DemoCluster.RestartNode(ctx, int32(nodeID)); err != nil {
			return c.internalServerError(errState, err)
		}
		fmt.Fprintf(c.iCtx.stdout, "node %d has been restarted\n", nodeID)
		return nextState
	case "recommission":
		if err := c.sqlCtx.DemoCluster.Recommission(ctx, int32(nodeID)); err != nil {
			return c.internalServerError(errState, err)
		}
		fmt.Fprintf(c.iCtx.stdout, "node %d has been recommissioned\n", nodeID)
		return nextState
	case "decommission":
		if err := c.sqlCtx.DemoCluster.Decommission(ctx, int32(nodeID)); err != nil {
			return c.internalServerError(errState, err)
		}
		fmt.Fprintf(c.iCtx.stdout, "node %d has been decommissioned\n", nodeID)
		return nextState
	}
	return c.invalidSyntax(errState)
}

// handleHelp prints SQL help.
func (c *cliState) handleHelp(cmd []string, nextState, errState cliStateEnum) cliStateEnum {
	command := strings.TrimSpace(strings.Join(cmd, " "))
	helpText, _ := c.serverSideParse(command + " ??")
	if helpText != "" {
		fmt.Fprintln(c.iCtx.stdout, helpText)
	} else {
		fmt.Fprintf(c.iCtx.stderr,
			"no help available for %q.\nTry \\h with no argument to see available help.\n", command)
		c.exitErr = errors.New("no help available")
		return errState
	}
	return nextState
}

// handleFunctionHelp prints help about built-in functions.
func (c *cliState) handleFunctionHelp(cmd []string, nextState, errState cliStateEnum) cliStateEnum {
	funcName := strings.TrimSpace(strings.Join(cmd, " "))
	helpText, _ := c.serverSideParse(fmt.Sprintf("select %s(??", funcName))
	if helpText != "" {
		fmt.Fprintln(c.iCtx.stdout, helpText)
	} else {
		fmt.Fprintf(c.iCtx.stderr,
			"no help available for %q.\nTry \\hf with no argument to see available help.\n", funcName)
		c.exitErr = errors.New("no help available")
		return errState
	}
	return nextState
}

// execSyscmd executes system commands.
func (c *cliState) execSyscmd(command string) (string, error) {
	var cmd *exec.Cmd

	shell := envutil.GetShellCommand(command)
	cmd = exec.Command(shell[0], shell[1:]...)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = c.iCtx.stderr

	if err := cmd.Run(); err != nil {
		return "", errors.Wrap(err, "error in external command")
	}

	return out.String(), nil
}

var errInvalidSyntax = errors.New("invalid syntax")

// runSyscmd runs system commands on the interactive CLI.
func (c *cliState) runSyscmd(line string, nextState, errState cliStateEnum) cliStateEnum {
	command := strings.Trim(line[2:], " \r\n\t\f")
	if command == "" {
		fmt.Fprintf(c.iCtx.stderr, "Usage:\n  \\! [command]\n")
		c.exitErr = errInvalidSyntax
		return errState
	}

	cmdOut, err := c.execSyscmd(command)
	if err != nil {
		fmt.Fprintf(c.iCtx.stderr, "command failed: %s\n", err)
		c.exitErr = err
		return errState
	}

	fmt.Fprint(c.iCtx.stdout, cmdOut)
	return nextState
}

// pipeSyscmd executes system commands and pipe the output into the current SQL.
func (c *cliState) pipeSyscmd(line string, nextState, errState cliStateEnum) cliStateEnum {
	command := strings.Trim(line[2:], " \n\r\t\f")
	if command == "" {
		fmt.Fprintf(c.iCtx.stderr, "Usage:\n  \\| [command]\n")
		c.exitErr = errInvalidSyntax
		return errState
	}

	cmdOut, err := c.execSyscmd(command)
	if err != nil {
		fmt.Fprintf(c.iCtx.stderr, "command failed: %s\n", err)
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
	if !c.ins.canPrompt() {
		return nextState
	}

	if c.useContinuePrompt {
		if c.inCopy() {
			c.continuePrompt = ">> "
		} else if len(c.fullPrompt) < 3 {
			c.continuePrompt = "> "
		} else {
			// continued statement prompt is: "        -> ".
			c.continuePrompt = strings.Repeat(" ", len(c.fullPrompt)-3) + "-> "
		}

		c.ins.setPrompt(c.continuePrompt)
		return nextState
	}

	if c.inCopy() {
		c.fullPrompt = ">>"
	} else {
		// Configure the editor to use the new prompt.

		parsedURL, err := url.Parse(c.conn.GetURL())
		if err != nil {
			// If parsing fails, we'll keep the entire URL. The Open call succeeded, and that
			// is the important part.
			c.fullPrompt = c.conn.GetURL() + "> "
			c.continuePrompt = strings.Repeat(" ", len(c.fullPrompt)-3) + "-> "
			return nextState
		}

		userName := ""
		if parsedURL.User != nil {
			userName = parsedURL.User.Username()
		}

		dbName := unknownDbName
		c.lastKnownTxnStatus = unknownTxnStatus

		wantDbStateInPrompt := rePromptDbState.MatchString(c.iCtx.customPromptPattern)
		if wantDbStateInPrompt {
			c.refreshTransactionStatus()
			// refreshDatabaseName() must be called *after* refreshTransactionStatus(),
			// even when %/ appears before %x in the prompt format.
			// This is because the database name should not be queried during
			// some transaction phases.
			dbName = c.refreshDatabaseName()
		}

		c.fullPrompt = rePromptFmt.ReplaceAllStringFunc(c.iCtx.customPromptPattern, func(m string) string {
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
	}
	c.fullPrompt += " "
	c.currentPrompt = c.fullPrompt

	// Configure the editor to use the new prompt.
	c.ins.setPrompt(c.currentPrompt)

	return nextState
}

// refreshTransactionStatus retrieves and sets the current transaction status.
func (c *cliState) refreshTransactionStatus() {
	c.lastKnownTxnStatus = unknownTxnStatus

	dbVal, hasVal := c.conn.GetServerValue(
		context.Background(),
		"transaction status", `SHOW TRANSACTION STATUS`)
	if !hasVal {
		return
	}

	txnString := clisqlexec.FormatVal(
		dbVal, false /* showPrintableUnicode */, false, /* shownewLinesAndTabs */
	)

	// Change the prompt based on the response from the server.
	switch txnString {
	case sqlfsm.NoTxnStateStr:
		c.lastKnownTxnStatus = ""
	case sqlfsm.AbortedStateStr:
		c.lastKnownTxnStatus = " ERROR"
	case sqlfsm.CommitWaitStateStr:
		c.lastKnownTxnStatus = "  DONE"
	case sqlfsm.OpenStateStr:
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

	dbVal, hasVal := c.conn.GetServerValue(
		context.Background(),
		"database name", `SHOW DATABASE`)
	if !hasVal {
		return unknownDbName
	}

	if dbVal == "" {
		// Attempt to be helpful to new users.
		fmt.Fprintln(c.iCtx.stderr, "warning: no current database set."+
			" Use SET database = <dbname> to change, CREATE DATABASE to make a new database.")
	}

	dbName := clisqlexec.FormatVal(
		dbVal, false /* showPrintableUnicode */, false, /* shownewLinesAndTabs */
	)

	// Preserve the current database name in case of reconnects.
	c.conn.SetCurrentDatabase(dbName)
	c.iCtx.dbName = dbName

	return dbName
}

var cmdHistFile = envutil.EnvOrDefaultString("COCKROACH_SQL_CLI_HISTORY", ".cockroachsql_history")

// runShowCompletions implements the sqlShell interface (to support the editor).
func (c *cliState) runShowCompletions(sql string, offset int) (rows [][]string, err error) {
	query := fmt.Sprintf(`SHOW COMPLETIONS AT OFFSET %d FOR %s`, offset, lexbase.EscapeSQLString(sql))
	err = c.runWithInterruptableCtx(func(ctx context.Context) error {
		var err error
		_, rows, err = c.sqlExecCtx.RunQuery(ctx, c.conn, clisqlclient.MakeQuery(query), true /* showMoreChars */)
		return err
	})
	return rows, err
}

func (c *cliState) doStart(nextState cliStateEnum) cliStateEnum {
	// Common initialization.
	c.partialLines = []string{}

	if c.cliCtx.IsInteractive {
		fmt.Fprintln(c.iCtx.stdout, "#\n# Enter \\? for a brief introduction.\n#")
	}

	return nextState
}

func (c *cliState) doStartLine(nextState cliStateEnum) cliStateEnum {
	// Clear the input buffer.
	c.atEOF = false
	c.partialLines = c.partialLines[:0]
	c.partialStmtsLen = 0
	c.concatLines = ""

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

	// TODO(knz): Re-load the previous lines of input, so the user
	// gets a chance to modify them.
	l, err := c.ins.getLine()

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

	case errors.Is(err, c.ins.errInterrupted()):
		if !c.cliCtx.IsInteractive {
			// Ctrl+C terminates non-interactive shells in all cases.
			c.exitErr = err
			return cliStop
		}

		if c.inCopy() {
			// CTRL+C in COPY cancels the copy.
			defer func() {
				c.resetCopy()
				c.partialLines = c.partialLines[:0]
				c.partialStmtsLen = 0
				c.useContinuePrompt = false
			}()
			c.exitErr = errors.CombineErrors(
				pgerror.Newf(pgcode.QueryCanceled, "COPY canceled by user"),
				c.copyFromState.Cancel(),
			)
			if c.exitErr != nil {
				if !c.singleStatement {
					clierror.OutputError(c.iCtx.stderr, c.exitErr, true /*showSeverity*/, false /*verbose*/)
				}
			}
			return cliRefreshPrompt
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

		// If a human is looking, tell them that quitting is done in another way.
		if c.sqlExecCtx.TerminalOutput {
			fmt.Fprintf(c.iCtx.stdout, "^C\nUse \\q or terminate input to exit.\n")
		}
		return cliStartLine

	case errors.Is(err, io.EOF):
		// If we're in COPY and we're interactive, this signifies the copy is complete.
		if c.inCopy() && c.cliCtx.IsInteractive {
			return cliRunStatement
		}

		c.atEOF = true

		if c.cliCtx.IsInteractive {
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
		fmt.Fprintf(c.iCtx.stderr, "input error: %s\n", err)
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

func (c *cliState) setupChangefeedOutput() (undo func(), err error) {
	prevTableFmt := c.sqlExecCtx.TableDisplayFormat
	prevByteaOutput, err := c.getSessionVarValue("bytea_output")
	if err != nil {
		return nil, err
	}
	var undoSteps []func()
	undo = func() {
		for _, s := range undoSteps {
			s()
		}
	}
	// The table display format, default for interactive shells,
	// doesn't work well with changefeeds as it buffers indefinitely.
	// Newline-delimited JSON doesn't need to buffer and can display
	// variably-structured data.
	if prevTableFmt == clisqlexec.TableDisplayTable {
		c.sqlExecCtx.TableDisplayFormat = clisqlexec.TableDisplayNDJSON
		undoSteps = append(undoSteps, func() { c.sqlExecCtx.TableDisplayFormat = prevTableFmt })
	}

	// bytea_output is defined and enforced server-side. Changefeed
	// record values are output as bytea datums, and escaped output
	// is much more readable than the default hex.
	if prevByteaOutput == `hex` {
		err := c.conn.Exec(context.Background(), `SET SESSION bytea_output=escape`)
		if err != nil {
			undo()
			return nil, err
		}
		undoSteps = append(undoSteps, func() {
			c.exitErr = errors.CombineErrors(
				c.exitErr, c.conn.Exec(context.Background(), `SET SESSION bytea_output=hex`),
			)
		})
	}

	return undo, nil

}

func (c *cliState) doHandleCliCmd(loopState, nextState cliStateEnum) cliStateEnum {
	if len(c.lastInputLine) == 0 || c.lastInputLine[0] != '\\' {
		return nextState
	}

	errState := loopState
	if c.iCtx.errExit {
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
		fmt.Fprintln(c.iCtx.stdout, strings.Join(cmd[1:], " "))

	case `\qecho`:
		fmt.Fprintln(c.iCtx.queryOutput, strings.Join(cmd[1:], " "))
		c.maybeFlushOutput()

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

	case `\o`:
		return c.runOpen(cmd[1:], loopState, errState)

	case `\p`:
		if c.ins.multilineEdit() {
			fmt.Fprintln(c.iCtx.stderr, `warning: \p is ineffective with this editor`)
			break
		}
		// This is analogous to \show but does not need a special case.
		// Implemented for compatibility with psql.
		fmt.Fprintln(c.iCtx.stdout, strings.Join(c.partialLines, "\n"))

	case `\r`:
		if c.ins.multilineEdit() {
			fmt.Fprintln(c.iCtx.stderr, `warning: \r is ineffective with this editor`)
			break
		}
		// Reset the input buffer so far. This is useful when e.g. a user
		// got confused with string delimiters and multi-line input.
		return cliStartLine

	case `\show`:
		if c.ins.multilineEdit() {
			fmt.Fprintln(c.iCtx.stderr, `warning: \show is ineffective with this editor`)
			break
		}
		fmt.Fprintln(c.iCtx.stderr, `warning: \show is deprecated. Use \p.`)
		if len(c.partialLines) == 0 {
			fmt.Fprintf(c.iCtx.stderr, "No input so far. Did you mean SHOW?\n")
		} else {
			for _, s := range c.partialLines {
				fmt.Fprintln(c.iCtx.stdout, s)
			}
		}

	case `\password`:
		return c.handlePassword(cmd[1:], cliRunStatement, errState)

	case `\|`:
		return c.pipeSyscmd(c.lastInputLine, nextState, errState)

	case `\h`:
		return c.handleHelp(cmd[1:], loopState, errState)

	case `\hf`:
		if len(cmd) == 1 {
			// The following query lists all functions. It prefixes
			// functions with their schema but only if the schema is not in
			// the search path. This ensures that "common" functions
			// are not prefixed by "pg_catalog", but crdb_internal functions
			// get prefixed with "crdb_internal."
			//
			// TODO(knz): Replace this by the \df logic when that is implemented;
			// see: https://github.com/cockroachdb/cockroach/pull/88061
			c.concatLines = `
SELECT DISTINCT
       IF(n.nspname = ANY current_schemas(TRUE), '',
          pg_catalog.quote_ident(n.nspname) || '.') ||
       pg_catalog.quote_ident(p.proname) AS function
  FROM pg_catalog.pg_proc p
  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
ORDER BY 1`
			return cliRunStatement
		}
		return c.handleFunctionHelp(cmd[1:], loopState, errState)

	case `\l`:
		c.concatLines = `SHOW DATABASES`
		return cliRunStatement

	case `\dt`:
		c.concatLines = `SHOW TABLES`
		return cliRunStatement

	case `\df`:
		c.concatLines = `SHOW FUNCTIONS`
		return cliRunStatement

	case `\copy`:
		c.exitErr = c.runWithInterruptableCtx(func(ctx context.Context) error {
			// Strip out the starting \ in \copy.
			return c.beginCopyFrom(ctx, line[1:])
		})
		if c.exitErr != nil {
			if !c.singleStatement {
				clierror.OutputError(c.iCtx.stderr, c.exitErr, true /*showSeverity*/, false /*verbose*/)
			}
			if c.iCtx.errExit {
				return cliStop
			}
		}
		return cliStartLine

	case `\.`:
		if c.inCopy() {
			c.partialLines = append(c.partialLines, `\.`)
			c.partialStmtsLen++
			return cliRunStatement
		}
		return c.invalidSyntax(errState)

	case `\dT`:
		c.concatLines = `SHOW TYPES`
		return cliRunStatement

	case `\du`:
		if len(cmd) == 1 {
			c.concatLines = `SHOW USERS`
			return cliRunStatement
		} else if len(cmd) == 2 {
			c.concatLines = fmt.Sprintf(`SELECT * FROM [SHOW USERS] WHERE username = %s`, lexbase.EscapeSQLString(cmd[1]))
			return cliRunStatement
		}
		return c.invalidSyntax(errState)

	case `\d`:
		if len(cmd) == 1 {
			c.concatLines = `SHOW TABLES`
			return cliRunStatement
		} else if len(cmd) == 2 {
			c.concatLines = `SHOW COLUMNS FROM ` + cmd[1]
			return cliRunStatement
		}
		return c.invalidSyntax(errState)
	case `\dd`:
		if len(cmd) == 2 {
			c.concatLines = `SHOW CONSTRAINTS FROM ` + cmd[1] + ` WITH COMMENT`
			return cliRunStatement
		}
		return c.invalidSyntax(errState)
	case `\connect`, `\c`:
		return c.handleConnect(cmd[1:], loopState, errState)

	case `\info`:
		return c.handleInfo(loopState)

	case `\x`:
		format := clisqlexec.TableDisplayRecords
		switch len(cmd) {
		case 1:
			if c.sqlExecCtx.TableDisplayFormat == clisqlexec.TableDisplayRecords {
				format = clisqlexec.TableDisplayTable
			}
		case 2:
			b, err := clisqlclient.ParseBool(cmd[1])
			if err != nil {
				return c.invalidSyntax(errState)
			} else if b {
				format = clisqlexec.TableDisplayRecords
			} else {
				format = clisqlexec.TableDisplayTable
			}
		default:
			return c.invalidSyntax(errState)
		}
		c.sqlExecCtx.TableDisplayFormat = format
		return loopState

	case `\demo`:
		return c.handleDemo(cmd[1:], loopState, errState)

	case `\statement-diag`:
		return c.handleStatementDiag(cmd[1:], loopState, errState)

	default:
		if strings.HasPrefix(cmd[0], `\d`) {
			// Unrecognized command for now, but we want to be helpful.
			fmt.Fprint(c.iCtx.stderr, "Suggestion: use the SQL SHOW statement to inspect your schema.\n")
		}
		return c.invalidSyntax(errState)
	}

	return loopState
}

func (c *cliState) handlePassword(
	cmd []string, nextState, errState cliStateEnum,
) (resState cliStateEnum) {
	if len(cmd) > 1 {
		return c.invalidSyntax(errState)
	}

	passwordHashMethod, err := c.getPasswordHashMethod()
	if err != nil {
		fmt.Fprintf(c.iCtx.stderr, "retrieving hash method from server: %v\n", err)
		c.exitErr = err
		return errState
	}

	var escapedUserName string
	if len(cmd) > 0 {
		escapedUserName = lexbase.EscapeSQLIdent(cmd[0])
	} else {
		// "current_user" is a keyword.
		escapedUserName = "current_user"
	}
	fmt.Fprintln(c.iCtx.stdout, "changing password for user", escapedUserName)

	// TODO(jane,knz): currently Ctrl+C during password entry prints out
	// a weird message and it's confusing to the user what they can do
	// to stop the entry. This needs to be cleaned up.
	password1, err := pprompt.PromptForPassword("" /* prompt */)
	if err != nil {
		fmt.Fprintf(c.iCtx.stderr, "reading password: %v\n", err)
		c.exitErr = err
		return errState
	}

	password2, err := pprompt.PromptForPassword("Enter it again: ")
	if err != nil {
		fmt.Fprintf(c.iCtx.stderr, "reading password: %v\n", err)
		c.exitErr = err
		return errState
	}

	if password1 != password2 {
		c.exitErr = errors.New("passwords didn't match")
		fmt.Fprintln(c.iCtx.stderr, c.exitErr)
		return errState
	}

	var hashedPassword []byte
	hashedPassword, err = password.HashPassword(
		context.Background(),
		passwordHashMethod.GetDefaultCost(),
		passwordHashMethod,
		password1,
		nil, /* hashSem - no need to throttle hashing in CLI */
	)

	if err != nil {
		fmt.Fprintf(c.iCtx.stderr, "hashing password: %v\n", err)
		c.exitErr = err
		return errState
	}

	c.concatLines = fmt.Sprintf(
		`ALTER USER %s WITH LOGIN PASSWORD %s`,
		// Note: we need to use .SQLIdentifier() to ensure that any special
		// characters in the username are properly quoted, to make this robust
		// to SQL injection.
		escapedUserName,
		lexbase.EscapeSQLString(string(hashedPassword)),
	)

	return nextState
}

func (c *cliState) handleConnect(
	cmd []string, loopState, errState cliStateEnum,
) (resState cliStateEnum) {
	if err := c.handleConnectInternal(cmd, false /*omitConnString*/); err != nil {
		fmt.Fprintln(c.iCtx.stderr, err)
		c.exitErr = err
		return errState
	}
	return loopState
}

func (c *cliState) handleConnectInternal(cmd []string, omitConnString bool) error {
	firstArgIsURL := len(cmd) > 0 &&
		(strings.HasPrefix(cmd[0], "postgres://") ||
			strings.HasPrefix(cmd[0], "postgresql://"))

	if len(cmd) == 1 && firstArgIsURL {
		// Note: we use a custom URL parser here to inherit the complex
		// custom logic from crdb's own handling of --url in `cockroach
		// sql`.
		parseURL := c.sqlCtx.ParseURL
		if parseURL == nil {
			parseURL = pgurl.Parse
		}
		purl, err := parseURL(cmd[0])
		if err != nil {
			return err
		}
		return c.switchToURL(purl)
	}

	// currURL is the previous connection URL up to this point.
	currURL, err := pgurl.Parse(c.conn.GetURL())
	if err != nil {
		return errors.Wrap(err, "parsing current connection URL")
	}

	// Reuse the current database from the session if known
	// (debug mode disabled, database in prompt), otherwise
	// from the current URL.
	dbName := c.iCtx.dbName
	if dbName == "" {
		dbName = currURL.GetDatabase()
	}

	// Extract the current config.
	prevproto, prevhost, prevport := currURL.GetNetworking()
	tlsUsed, mode, caCertPath := currURL.GetTLSOptions()

	// newURL will be our new connection URL past this point.
	newURL := pgurl.New()
	// Populate the main fields from the current URL.
	newURL.
		WithDefaultHost(prevhost).
		WithDefaultPort(prevport).
		WithDefaultDatabase(dbName).
		WithDefaultUsername(currURL.GetUsername())
	if tlsUsed {
		newURL.WithTransport(pgurl.TransportTLS(mode, caCertPath))
	} else {
		newURL.WithTransport(pgurl.TransportNone())
	}
	if err := newURL.AddOptions(currURL.GetExtraOptions()); err != nil {
		return err
	}

	// Parse the arguments to \connect:
	// it accepts newdb, user, host, port in that order.
	// Each field can be marked as "-" to reuse the current defaults.
	switch len(cmd) {
	case 4:
		if cmd[3] != "-" {
			if err := newURL.SetOption("port", cmd[3]); err != nil {
				return err
			}
		}
		fallthrough
	case 3:
		if cmd[2] != "-" {
			if err := newURL.SetOption("host", cmd[2]); err != nil {
				return err
			}
		}
		fallthrough
	case 2:
		if cmd[1] != "-" {
			if err := newURL.SetOption("user", cmd[1]); err != nil {
				return err
			}
		}
		fallthrough
	case 1:
		if cmd[0] != "-" {
			if err := newURL.SetOption("database", cmd[0]); err != nil {
				return err
			}
		}
	case 0:
		// Just print the current connection settings.
		if !omitConnString {
			fmt.Fprintf(c.iCtx.stdout, "Connection string: %s\n", currURL.ToPQRedacted())
		}
		fmt.Fprintf(c.iCtx.stdout, "You are connected to database %q as user %q.\n", dbName, currURL.GetUsername())
		return nil

	default:
		return errors.Newf(`unknown syntax: \c %s`, strings.Join(cmd, " "))
	}

	// If we are reconnecting to the same server with the same user, reuse
	// the authentication credentials present inside the URL, if any.
	// We do take care to avoid reusing the password however, for two separate
	// reasons:
	// - if the connection has just failed because of an invalid password,
	//   we don't want to fix the invalid password in the URL.
	// - the transport may be insecure, in which case we want to give
	//   the user the opportunity to think twice about entering their
	//   password over an untrusted link.
	if proto, host, port := newURL.GetNetworking(); proto == prevproto &&
		host == prevhost && port == prevport &&
		newURL.GetUsername() == currURL.GetUsername() {
		// Remove the password from the current URL.
		currURL.ClearPassword()

		// Migrate the details from the previous URL to the new one.
		prevAuthn, err := currURL.GetAuthnOption()
		if err != nil {
			return err
		}
		newURL.WithAuthn(prevAuthn)
	}

	if err := newURL.Validate(); err != nil {
		return errors.Wrap(err, "validating the new URL")
	}

	return c.switchToURL(newURL)
}

func (c *cliState) switchToURL(newURL *pgurl.URL) error {
	fmt.Fprintln(c.iCtx.stdout, "using new connection URL:", newURL)

	// Ensure the new connection will prompt for a password if the
	// server requires one.
	usePw, pwSet, _ := newURL.GetAuthnPassword()

	if err := c.conn.Close(); err != nil {
		fmt.Fprintf(c.iCtx.stderr, "warning: error while closing connection: %v\n", err)
	}
	c.conn.SetURL(newURL.ToPQ().String())
	c.conn.SetMissingPassword(!usePw || !pwSet)
	return nil
}

const maxRecursionLevels = 10

func (c *cliState) runInclude(
	cmd []string, contState, errState cliStateEnum, relative bool,
) (resState cliStateEnum) {
	if len(cmd) != 1 {
		return c.invalidSyntax(errState)
	}

	if c.levels >= maxRecursionLevels {
		c.exitErr = errors.Newf(`\i: too many recursion levels (max %d)`, maxRecursionLevels)
		fmt.Fprintf(c.iCtx.stderr, "%v\n", c.exitErr)
		return errState
	}

	if len(c.partialLines) > 0 {
		return c.invalidSyntax(errState)
	}

	filename := cmd[0]
	if !filepath.IsAbs(filename) && relative {
		// In relative mode, the filename is resolved relative to the
		// surrounding script.
		filename = filepath.Join(c.includeDir, filename)
	}

	f, err := os.Open(filename)
	if err != nil {
		fmt.Fprintln(c.iCtx.stderr, err)
		c.exitErr = err
		return errState
	}
	// Close the file at the end.
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Fprintf(c.iCtx.stderr, "error: closing %s: %v\n", filename, err)
			c.exitErr = errors.CombineErrors(c.exitErr, err)
			resState = errState
		}
	}()

	// Including a file: increase the recursion level.
	newLevel := c.levels + 1

	return c.runIncludeInternal(contState, errState,
		filename, bufio.NewReader(f), newLevel, false /* singleStatement */)
}

func (c *cliState) runString(
	contState, errState cliStateEnum, stmt string,
) (resState cliStateEnum) {
	r := strings.NewReader(stmt)
	buf := bufio.NewReader(r)

	// Running a string: don't increase the recursion level. We want
	// that running sql -e '\i ...' gives access to the same maximum
	// number of recursive includes as entering \i on the interactive
	// prompt.
	newLevel := c.levels

	return c.runIncludeInternal(contState, errState,
		"-e", buf, newLevel, true /* singleStatement */)
}

func (c *cliState) runIncludeInternal(
	contState, errState cliStateEnum,
	filename string,
	input *bufio.Reader,
	level int,
	singleStatement bool,
) (resState cliStateEnum) {
	newState := cliState{
		cliCtx:     c.cliCtx,
		sqlConnCtx: c.sqlConnCtx,
		sqlExecCtx: c.sqlExecCtx,
		sqlCtx:     c.sqlCtx,
		iCtx:       c.iCtx,
		ins:        &bufioReader{wout: c.iCtx.stdout, buf: input},
		conn:       c.conn,
		includeDir: filepath.Dir(filename),
		levels:     level,

		singleStatement: singleStatement,
	}

	if err := newState.doRunShell(cliStartLine, nil, nil, nil); err != nil {
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

	lastTok, ok := scanner.LastLexicalToken(c.concatLines)
	if c.partialStmtsLen == 0 && !ok {
		// More whitespace, or comments. Still nothing to do. However
		// if the syntax was non-trivial to arrive here,
		// keep it for history.
		if c.lastInputLine != "" {
			c.addHistory(c.lastInputLine)
		}
		return startState
	}
	endOfStmt := (!c.inCopy() && isEndOfStatement(lastTok)) ||
		// We're always at the end of a statement if we're in COPY and encounter
		// the \. or EOF character.
		(c.inCopy() && (strings.HasSuffix(c.concatLines, "\n"+`\.`) || c.atEOF)) ||
		// We're always at the end of a statement if EOF is reached in the
		// single statement mode.
		(c.singleStatement && c.atEOF)
	if c.atEOF {
		// Definitely no more input expected.
		if !endOfStmt {
			fmt.Fprintf(c.iCtx.stderr, "missing semicolon at end of statement: %s\n", c.concatLines)
			c.exitErr = fmt.Errorf("last statement was not executed: %s", c.concatLines)
			return cliStop
		}
	}

	if !endOfStmt {
		if lastTok == '?' {
			fmt.Fprintf(c.iCtx.stdout,
				"Note: a single '?' is a JSON operator. If you want contextual help, use '??'.\n")
		}
		return contState
	}

	// Complete input. Remember it in the history.
	if !c.inCopy() {
		c.addHistory(c.concatLines)
	}

	if !c.iCtx.checkSyntax {
		return execState
	}

	return checkState
}

// reflow implements the sqlShell interface (to support the editor).
func (c *cliState) reflow(
	allText bool, currentText string, targetWidth int,
) (changed bool, newText string, info string) {
	if targetWidth > c.iCtx.reflowMaxWidth {
		targetWidth = c.iCtx.reflowMaxWidth
	}

	var rows [][]string
	err := c.runWithInterruptableCtx(func(ctx context.Context) error {
		var err error
		_, rows, err = c.sqlExecCtx.RunQuery(ctx, c.conn,
			clisqlclient.MakeQuery(
				`SELECT prettify_statement($1, $2, $3, $4)`,
				currentText, targetWidth,
				c.iCtx.reflowAlignMode,
				c.iCtx.reflowCaseMode,
			), true /* showMoreChars */)
		return err
	})
	if err != nil {
		if pgerror.GetPGCode(err) == pgcode.Syntax {
			// SQL not recognized. That's all right. Just reflow using
			// a simple algorithm.
			return editline.DefaultReflow(allText, currentText, targetWidth)
		}
		// Some other error. Display it.
		var buf strings.Builder
		clierror.OutputError(&buf, err, true /*showSeverity*/, false /*verbose*/)
		return false, currentText, buf.String()
	}
	if len(rows) < 1 || len(rows[0]) < 1 {
		return false, currentText, "incomplete result from prettify_statement"
	}
	newText = rows[0][0]
	// NB: prettify uses tabs. The editor doesn't like them.
	// https://github.com/knz/bubbline/issues/5
	newText = strings.ReplaceAll(newText, "\t", strings.Repeat(" ", c.iCtx.reflowTabWidth))
	// Strip final spaces.
	newText = strings.TrimRight(newText, " \n")
	// NB: prettify removes the final semicolon. Re-add it.
	newText += ";"
	return true, newText, ""
}

func (c *cliState) doCheckStatement(startState, contState, execState cliStateEnum) cliStateEnum {
	// If we are in COPY, we have no valid SQL, so skip directly to the next state.
	if c.inCopy() {
		return execState
	}
	// From here on, client-side syntax checking is enabled.
	helpText, err := c.serverSideParse(c.concatLines)
	if err != nil {
		if helpText != "" {
			// There was a help text included. Use it.
			fmt.Fprintln(c.iCtx.stdout, helpText)
		}

		_ = c.invalidSyntaxf(
			cliStart, "statement ignored: %v",
			clierror.NewFormattedError(err, false /*showSeverity*/, false /*verbose*/),
		)

		// Stop here if exiterr is set.
		if c.iCtx.errExit {
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

	if !c.cliCtx.IsInteractive {
		return execState
	}

	nextState := execState

	c.partialStmtsLen = len(c.partialLines)

	return nextState
}

// doRunStatements runs all the statements that have been accumulated by
// concatLines.
func (c *cliState) doRunStatements(nextState cliStateEnum) cliStateEnum {
	if err := c.iCtx.maybeWrapStatement(context.Background(), c.concatLines, c); err != nil {
		c.exitErr = err
		if !c.singleStatement {
			clierror.OutputError(c.iCtx.stderr, c.exitErr, true /*showSeverity*/, false /*verbose*/)
		}
		if c.iCtx.errExit {
			return cliStop
		}
		return nextState
	}
	if c.iCtx.afterRun != nil {
		defer c.iCtx.afterRun()
		c.iCtx.afterRun = nil
	}

	// Once we send something to the server, the txn status may change arbitrarily.
	// Clear the known state so that further entries do not assume anything.
	c.lastKnownTxnStatus = " ?"

	// Are we tracing?
	if c.iCtx.autoTrace != "" {
		// Clear the trace by disabling tracing, then restart tracing
		// with the specified options.
		c.exitErr = c.conn.Exec(
			context.Background(),
			"SET tracing = off; SET tracing = "+c.iCtx.autoTrace)
		if c.exitErr != nil {
			if !c.singleStatement {
				clierror.OutputError(c.iCtx.stderr, c.exitErr, true /*showSeverity*/, false /*verbose*/)
			}
			if c.iCtx.errExit {
				return cliStop
			}
			return nextState
		}
	}

	// Now run the statement/query.
	c.exitErr = c.runWithInterruptableCtx(func(ctx context.Context) error {
		if scanner.FirstLexicalToken(c.concatLines) == lexbase.COPY {
			return c.beginCopyFrom(ctx, c.concatLines)
		}
		q := clisqlclient.MakeQuery(c.concatLines)
		if c.inCopy() {
			q = c.copyFromState.Commit(
				ctx,
				c.resetCopy,
				c.concatLines,
			)
		}
		defer c.maybeFlushOutput()
		return c.sqlExecCtx.RunQueryAndFormatResults(
			ctx,
			c.conn,
			c.iCtx.queryOutput, // query output.
			c.iCtx.stdout,      // timings.
			c.iCtx.stderr,
			q,
		)
	})
	if c.exitErr != nil {
		if !c.singleStatement {
			clierror.OutputError(c.iCtx.stderr, c.exitErr, true /*showSeverity*/, false /*verbose*/)
		}
	}

	// If we are tracing, stop tracing and display the trace. We do
	// this even if there was an error: a trace on errors is useful.
	if c.iCtx.autoTrace != "" {
		// First, disable tracing.
		if err := c.conn.Exec(context.Background(),
			"SET tracing = off"); err != nil {
			// Print the error for the SET tracing statement. This will
			// appear below the error for the main query above, if any,
			clierror.OutputError(c.iCtx.stderr, err, true /*showSeverity*/, false /*verbose*/)
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
			if strings.Contains(c.iCtx.autoTrace, "kv") {
				traceType = "kv"
			}
			if err := c.runWithInterruptableCtx(func(ctx context.Context) error {
				defer c.maybeFlushOutput()
				return c.sqlExecCtx.RunQueryAndFormatResults(ctx,
					c.conn,
					c.iCtx.queryOutput, // query output
					c.iCtx.stdout,      // timings
					c.iCtx.stderr,      // errors
					clisqlclient.MakeQuery(fmt.Sprintf("SHOW %s TRACE FOR SESSION", traceType)))
			}); err != nil {
				clierror.OutputError(c.iCtx.stderr, err, true /*showSeverity*/, false /*verbose*/)
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

	if c.exitErr != nil && c.iCtx.errExit {
		return cliStop
	}

	return nextState
}

func (c *cliState) beginCopyFrom(ctx context.Context, sql string) error {
	copyFromState, err := clisqlclient.BeginCopyFrom(ctx, c.conn, sql)
	if err != nil {
		return err
	}
	c.copyFromState = copyFromState
	if c.cliCtx.IsInteractive {
		fmt.Fprintln(c.iCtx.stdout, `Enter data to be copied followed by a newline.`)
		fmt.Fprintln(c.iCtx.stdout, `End with a backslash and a period on a line by itself, or an EOF signal.`)
	}
	return nil
}

func (c *cliState) doDecidePath() cliStateEnum {
	if len(c.partialLines) == 0 {
		return cliProcessFirstLine
	} else if c.cliCtx.IsInteractive {
		// In interactive mode, we allow client-side commands to be
		// issued on intermediate lines.
		return cliHandleCliCmd
	}
	// Neither interactive nor at start, continue with processing.
	return cliPrepareStatementLine
}

// NewShell instantiates a cliState.
//
// In simple uses of the SQL shell (e.g. in the standalone
// cockroach-sql), the urlParser argument can be set to pgurl.Parse;
// however CockroachDB's own CLI package has a more advanced URL
// parser that is used instead.
func NewShell(
	cliCtx *clicfg.Context,
	sqlConnCtx *clisqlclient.Context,
	sqlExecCtx *clisqlexec.Context,
	sqlCtx *Context,
	conn clisqlclient.Conn,
) Shell {
	return &cliState{
		cliCtx:     cliCtx,
		sqlConnCtx: sqlConnCtx,
		sqlExecCtx: sqlExecCtx,
		sqlCtx:     sqlCtx,
		iCtx: &internalContext{
			customPromptPattern: defaultPromptPattern,
		},
		conn:       conn,
		includeDir: ".",
	}
}

// RunInteractive implements the Shell interface.
func (c *cliState) RunInteractive(cmdIn, cmdOut, cmdErr *os.File) (exitErr error) {
	finalFn := c.maybeHandleInterrupt()
	defer finalFn()

	return c.doRunShell(cliStart, cmdIn, cmdOut, cmdErr)
}

func (c *cliState) doRunShell(state cliStateEnum, cmdIn, cmdOut, cmdErr *os.File) (exitErr error) {
	for {
		if state == cliStop {
			break
		}
		switch state {
		case cliStart:
			defer func() {
				if err := c.closeOutputFile(); err != nil {
					fmt.Fprintf(cmdErr, "warning: closing output file: %v\n", err)
				}
			}()
			cleanupFn, err := c.configurePreShellDefaults(cmdIn, cmdOut, cmdErr)
			defer cleanupFn()
			if err != nil {
				return err
			}
			if len(c.sqlCtx.ExecStmts) > 0 {
				// Single-line sql; run as simple as possible, without noise on stdout.
				if err := c.runStatements(c.sqlCtx.ExecStmts); err != nil {
					return err
				}
				if c.iCtx.quitAfterExecStmts {
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

const defaultReflowMaxWidth = 60
const defaultReflowAlignMode = 3
const defaultReflowCaseMode = 1
const defaultReflowTabWidth = 4 // the value used internally by prettify_statement

// configurePreShellDefaults should be called after command-line flags
// have been loaded into the cliCtx/sqlCtx and .isInteractive /
// .terminalOutput have been initialized, but before the SQL shell or
// execution starts.
//
// The returned cleanupFn must be called even when the err return is
// not nil.
func (c *cliState) configurePreShellDefaults(
	cmdIn, cmdOut, cmdErr *os.File,
) (cleanupFn func(), err error) {
	c.iCtx.stdout = cmdOut
	c.iCtx.queryOutputFile = cmdOut
	c.iCtx.queryOutput = cmdOut
	c.iCtx.stderr = cmdErr
	c.iCtx.reflowMaxWidth = defaultReflowMaxWidth
	c.iCtx.reflowAlignMode = defaultReflowAlignMode
	c.iCtx.reflowCaseMode = defaultReflowCaseMode
	c.iCtx.reflowTabWidth = defaultReflowTabWidth

	if c.sqlExecCtx.TerminalOutput {
		// If results are shown on a terminal also enable printing of
		// times by default.
		c.sqlExecCtx.ShowTimes = true
	}

	if c.cliCtx.IsInteractive {
		// If a human user is providing the input, we want to help them with
		// what they are entering:
		c.iCtx.errExit = false // let the user retry failing commands
		if !c.sqlConnCtx.DebugMode {
			// Also, try to enable syntax checking if supported by the server.
			// This is a form of client-side error checking to help with large txns.
			c.iCtx.checkSyntax = true
		}
	} else {
		// When running non-interactive, by default we want errors to stop
		// further processing and we can just let syntax checking to be
		// done server-side to avoid client-side churn.
		c.iCtx.errExit = true
		c.iCtx.checkSyntax = false
		// We also don't need (smart) prompts at all.
	}

	// An interactive readline prompter is comparatively slow at
	// reading input, so we only use it in interactive mode and when
	// there is also a terminal on stdout.
	canUseEditor := c.cliCtx.IsInteractive && c.sqlExecCtx.TerminalOutput
	useEditor := canUseEditor && !c.sqlCtx.DisableLineEditor
	c.ins = getEditor(useEditor, canUseEditor)

	// maxHistEntries is the maximum number of entries to
	// preserve. Note that libedit de-duplicates entries under the
	// hood. We expect that folk entering SQL in a shell will often
	// reuse the same queries over time, so we don't expect this limit
	// to ever be reached in practice, or to be an annoyance to
	// anyone. We do prefer a limit however (as opposed to no limit at
	// all), to prevent abnormal situation where a history runs into
	// megabytes and starts slowing down the shell.
	const maxHistEntries = 10000
	var histFile string
	if useEditor {
		homeDir, err := envutil.HomeDir()
		if err != nil {
			fmt.Fprintf(c.iCtx.stderr, "warning: cannot retrieve user information: %v\nwarning: history will not be saved\n", err)
		} else {
			histFile = filepath.Join(homeDir, cmdHistFile)
		}
	}

	cleanupFn, c.exitErr = c.ins.init(cmdIn, c.iCtx.stdout, c.iCtx.stderr, c, maxHistEntries, histFile)
	if c.exitErr != nil {
		return cleanupFn, c.exitErr
	}
	// The readline library may have a custom file descriptor for stdout.
	// Use that for further output.
	c.iCtx.stdout = c.ins.getOutputStream()
	c.iCtx.queryOutputFile = c.ins.getOutputStream()

	if canUseEditor {
		// Default prompt is part of the connection URL. eg: "marc@localhost:26257>".
		c.iCtx.customPromptPattern = defaultPromptPattern
		if c.sqlConnCtx.DebugMode {
			c.iCtx.customPromptPattern = debugPromptPattern
		}
	}

	// If any --set flags were set through the command line,
	// synthetize '-e set=xxx' statements for them at the beginning.
	c.iCtx.quitAfterExecStmts = len(c.sqlCtx.ExecStmts) > 0
	if len(c.sqlCtx.SetStmts) > 0 {
		setStmts := make([]string, 0, len(c.sqlCtx.SetStmts)+len(c.sqlCtx.ExecStmts))
		for _, s := range c.sqlCtx.SetStmts {
			setStmts = append(setStmts, `\set `+s)
		}
		c.sqlCtx.SetStmts = nil
		c.sqlCtx.ExecStmts = append(setStmts, c.sqlCtx.ExecStmts...)
	}

	if c.sqlExecCtx.TerminalOutput {
		c.iCtx.addStatementWrapper(statementWrapper{
			Pattern: createSinklessChangefeed,
			Wrapper: func(ctx context.Context, statement string, state *cliState) error {
				undo, err := c.setupChangefeedOutput()
				if err != nil {
					return err
				}
				c.iCtx.afterRun = undo
				return nil
			},
		})
	}

	return cleanupFn, nil
}

// runStatements executes the given statements and terminates
// on error.
func (c *cliState) runStatements(stmts []string) error {
	for {
		for i, stmt := range stmts {
			c.exitErr = nil
			_ = c.runString(cliRunStatement, cliStop, stmt)
			if c.exitErr != nil {
				if !c.iCtx.errExit && i < len(stmts)-1 {
					// Print the error now because we don't get a chance later.
					clierror.OutputError(c.iCtx.stderr, c.exitErr, true /*showSeverity*/, false /*verbose*/)
				}
				if c.iCtx.errExit {
					break
				}
			}
		}
		// If --watch was specified and no error was encountered,
		// repeat.
		if c.sqlCtx.RepeatDelay > 0 && c.exitErr == nil {
			time.Sleep(c.sqlCtx.RepeatDelay)
			continue
		}
		break
	}

	if c.exitErr != nil {
		// Don't write the error to stderr ourselves. Cobra will do this for
		// us on the exit path. We do want the details though, so add them.
		c.exitErr = clierror.NewFormattedError(c.exitErr, true /*showSeverity*/, false /*verbose*/)
	}
	return c.exitErr
}

// enableDebug implements the sqlShell interface (to support the editor).
func (c *cliState) enableDebug() bool {
	return c.sqlConnCtx.DebugMode
}

// serverSideParse implements the sqlShell interface (to support the editor).
// It uses the SHOW SYNTAX statement to analyze the given string.
// If the syntax is correct, the function returns the statement
// decomposition in the first return value. If it is not, the function
// extracts a help string if available.
func (c *cliState) serverSideParse(sql string) (helpText string, err error) {
	cols, rows, err := c.sqlExecCtx.RunQuery(
		context.Background(),
		c.conn,
		clisqlclient.MakeQuery("SHOW SYNTAX "+lexbase.EscapeSQLString(sql)),
		true, /* showMoreChars */
	)
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
		//
		// The string here must match the constant string
		// parser.specialHelpErrorPrefix. The second string must match
		// the constant string parser.helpHintPrefix.
		//
		// However, we cannot include the 'parser' package here because it
		// would incur a huge dependency overhead.
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

func (c *cliState) maybeHandleInterrupt() func() {
	if !c.cliCtx.IsInteractive {
		return func() {}
	}
	intCh := make(chan os.Signal, 1)
	signal.Notify(intCh, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case <-intCh:
				c.iCtx.mu.Lock()
				cancelFn, doneCh := c.iCtx.mu.cancelFn, c.iCtx.mu.doneCh
				c.iCtx.mu.Unlock()
				if cancelFn == nil {
					// No query currently executing.
					// Are we doing interactive input? If so, do nothing.
					if c.cliCtx.IsInteractive {
						continue
					}
					// Otherwise, ctrl+c interrupts the shell. We do this
					// by re-throwing the signal after stopping the signal capture.
					signal.Reset(os.Interrupt)
					_ = sysutil.InterruptSelf()
					return
				}

				fmt.Fprintf(c.iCtx.stderr, "\nattempting to cancel query...\n")
				// Cancel the query's context, which should make the driver
				// send a cancellation message.
				if err := cancelFn(ctx); err != nil {
					fmt.Fprintf(c.iCtx.stderr, "\nerror while cancelling query: %v\n", err)
				}

				// Now wait for the shell to process the cancellation.
				//
				// If it takes too long (e.g. server has become unresponsive,
				// or we're connected to a pre-22.1 server which does not
				// support cancellation), fall back to the previous behavior
				// which is to interrupt the shell altogether.
				tooLongTimer := time.After(3 * time.Second)
			wait:
				for {
					select {
					case <-doneCh:
						break wait
					case <-tooLongTimer:
						fmt.Fprintln(c.iCtx.stderr, "server does not respond to query cancellation; a second interrupt will stop the shell.")
						signal.Reset(os.Interrupt)
					}
				}
				// Re-arm the signal handler.
				signal.Notify(intCh, os.Interrupt)

			case <-ctx.Done():
				// Shell is terminating.
				return
			}
		}
	}()
	return cancel
}

func (c *cliState) runWithInterruptableCtx(fn func(ctx context.Context) error) error {
	if !c.cliCtx.IsInteractive {
		return fn(context.Background())
	}
	// The cancellation function can be used by the Ctrl+C handler
	// to cancel this query.
	ctx, cancel := context.WithCancel(context.Background())
	// doneCh will be used on the return path to teach the Ctrl+C
	// handler that the query has been cancelled successfully.
	doneCh := make(chan struct{})
	defer func() { close(doneCh) }()

	// Inform the Ctrl+C handler that this query is executing.
	c.iCtx.mu.Lock()
	c.iCtx.mu.cancelFn = c.conn.Cancel
	c.iCtx.mu.doneCh = doneCh
	c.iCtx.mu.Unlock()
	defer func() {
		c.iCtx.mu.Lock()
		cancel()
		c.iCtx.mu.cancelFn = nil
		c.iCtx.mu.doneCh = nil
		c.iCtx.mu.Unlock()
	}()

	// Now run the query.
	err := fn(ctx)
	return err
}

// getPasswordHashMethod checks the session variable `password_encryption`
// to get the password hash method.
func (c *cliState) getPasswordHashMethod() (password.HashMethod, error) {
	passwordHashMethodStr, err := c.getSessionVarValue("password_encryption")
	if err != nil {
		return password.HashInvalidMethod, err
	}

	hashMethod := password.LookupMethod(passwordHashMethodStr)
	if hashMethod == password.HashInvalidMethod {
		return password.HashInvalidMethod, errors.Newf("unknown hash method: %q", passwordHashMethodStr)
	}
	return hashMethod, nil
}

// getSessionVarValue is to get the value for a session variable.
func (c *cliState) getSessionVarValue(sessionVar string) (string, error) {
	query := fmt.Sprintf("SHOW %s", sessionVar)
	var rows [][]string
	var err error
	err = c.runWithInterruptableCtx(func(ctx context.Context) error {
		_, rows, err = c.sqlExecCtx.RunQuery(ctx, c.conn, clisqlclient.MakeQuery(query), true /* showMoreChars */)
		return err
	})

	if err != nil {
		return "", err
	}

	for _, row := range rows {
		if len(row) != 0 {
			return row[0], nil
		}
	}
	return "", nil
}

func (c *cliState) runOpen(cmd []string, contState, errState cliStateEnum) (resState cliStateEnum) {
	if len(cmd) > 1 {
		return c.invalidSyntax(errState)
	}

	outputFile := "" // no file: reset to stdout
	if len(cmd) == 1 {
		outputFile = cmd[0]
	}
	if err := c.openOutputFile(outputFile); err != nil {
		c.exitErr = err
		fmt.Fprintf(c.iCtx.stderr, "%v\n", c.exitErr)
		return errState
	}
	return contState
}

func (c *cliState) openOutputFile(file string) error {
	var f *os.File
	if file != "" {
		// First check whether the new file can be opened.
		// (We keep the previous one otherwise.)
		// NB: permission 0666 mimics what psql does: fopen(file, "w").
		var err error
		f, err = os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
	}
	// Close the previous file.
	if err := c.closeOutputFile(); err != nil {
		return err
	}
	if f != nil {
		c.iCtx.queryOutputFile = f
		c.iCtx.queryOutputBuf = bufio.NewWriter(f)
		c.iCtx.queryOutput = c.iCtx.queryOutputBuf
	}
	return nil
}

func (c *cliState) maybeFlushOutput() {
	if err := c.maybeFlushOutputInternal(); err != nil {
		fmt.Fprintf(c.iCtx.stderr, "warning: flushing output file: %v", err)
	}
}

func (c *cliState) maybeFlushOutputInternal() error {
	if c.iCtx.queryOutputBuf == nil {
		return nil
	}
	return c.iCtx.queryOutputBuf.Flush()
}

func (c *cliState) closeOutputFile() error {
	if c.iCtx.queryOutputBuf == nil {
		return nil
	}
	if err := c.maybeFlushOutputInternal(); err != nil {
		return err
	}
	if c.iCtx.queryOutputFile == c.iCtx.stdout {
		// Nothing to do.
		return nil
	}

	// Close file and reset.
	err := c.iCtx.queryOutputFile.Close()
	c.iCtx.queryOutputFile = c.iCtx.stdout
	c.iCtx.queryOutput = c.iCtx.stdout
	c.iCtx.queryOutputBuf = nil
	return err
}
