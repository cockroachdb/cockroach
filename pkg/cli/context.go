// Copyright 2016 The Cockroach Authors.
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
	"context"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	isatty "github.com/mattn/go-isatty"
)

// serverCfg is used as the client-side copy of default server
// parameters for CLI utilities (other than `cockroach start`, which
// constructs a proper server.Config for the newly created server).
var serverCfg = func() server.Config {
	st := cluster.MakeClusterSettings(cluster.BinaryMinimumSupportedVersion, cluster.BinaryServerVersion)
	settings.SetCanonicalValuesContainer(&st.SV)

	s := server.MakeConfig(context.Background(), st)
	s.SQLAuditLogDirName = &sqlAuditLogDir
	return s
}()

var sqlAuditLogDir log.DirName

// GetServerCfgStores provides direct public access to the StoreSpecList inside
// serverCfg. This is used by CCL code to populate some fields.
//
// WARNING: consider very carefully whether you should be using this.
func GetServerCfgStores() base.StoreSpecList {
	return serverCfg.Stores
}

var baseCfg = serverCfg.Config

// initCLIDefaults serves as the single point of truth for
// configuration defaults. It is suitable for calling between tests of
// the CLI utilities inside a single testing process.
func initCLIDefaults() {
	// We don't reset the pointers (because they are tied into the
	// flags), but instead overwrite the existing structs' values.
	baseCfg.InitDefaults()

	// isInteractive is only set to `true` by `cockroach sql` -- all
	// other client commands are non-interactive, regardless of whether
	// the standard input is a terminal.
	cliCtx.isInteractive = false
	// See also setCLIDefaultForTests() in cli_test.go.
	cliCtx.terminalOutput = isatty.IsTerminal(os.Stdout.Fd())
	cliCtx.tableDisplayFormat = tableDisplayTSV
	if cliCtx.terminalOutput {
		// See also setCLIDefaultForTests() in cli_test.go.
		cliCtx.tableDisplayFormat = tableDisplayPretty
	}
	cliCtx.showTimes = false
	cliCtx.cmdTimeout = 0 // no timeout
	cliCtx.sqlConnURL = ""
	cliCtx.sqlConnUser = ""
	cliCtx.sqlConnDBName = ""

	sqlCtx.execStmts = nil
	sqlCtx.safeUpdates = false
	sqlCtx.echo = false

	dumpCtx.dumpMode = dumpBoth
	dumpCtx.asOf = ""

	debugCtx.startKey = engine.NilKey
	debugCtx.endKey = engine.MVCCKeyMax
	debugCtx.values = false
	debugCtx.sizes = false
	debugCtx.replicated = false
	debugCtx.inputFile = ""
	debugCtx.printSystemConfig = false
	debugCtx.maxResults = 1000

	zoneCtx.zoneConfig = ""
	zoneCtx.zoneDisableReplication = false

	serverCfg.SocketFile = ""
	serverCfg.ListeningURLFile = ""
	serverCfg.PIDFile = ""
	startCtx.serverInsecure = baseCfg.Insecure
	startCtx.serverSSLCertsDir = base.DefaultCertsDirectory
	startCtx.serverConnHost = ""
	startCtx.tempDir = ""
	startCtx.externalIODir = ""

	quitCtx.serverDecommission = false

	nodeCtx.nodeDecommissionWait = nodeDecommissionWaitAll
	nodeCtx.statusShowRanges = false
	nodeCtx.statusShowStats = false
	nodeCtx.statusShowAll = false
	nodeCtx.statusShowDecommission = false

	initPreFlagsDefaults()
}

// cliContext captures the command-line parameters of most CLI commands.
type cliContext struct {
	// Embed the base context.
	*base.Config

	// isInteractive indicates whether the session is interactive, that
	// is, the commands executed are extremely likely to be *input* from
	// a human user: the standard input is a terminal and `-e` was not
	// used (the shell has a prompt).
	isInteractive bool

	// terminalOutput indicates whether output is going to a terminal,
	// that is, it is not going to a file, another program for automated
	// processing, etc.: the standard output is a terminal.
	terminalOutput bool

	// tableDisplayFormat indicates how to format result tables.
	tableDisplayFormat tableDisplayFormat

	// showTimes indicates whether to display query times after each result line.
	showTimes bool

	// cmdTimeout sets the maximum run time for the command.
	// Commands that wish to use this must use cmdTimeoutContext().
	cmdTimeout time.Duration

	// for CLI commands that use the SQL interface, these parameters
	// determine how to connect to the server.
	sqlConnURL, sqlConnUser, sqlConnDBName string
}

// cliCtx captures the command-line parameters common to most CLI utilities.
// Defaults set by InitCLIDefaults() above.
var cliCtx = cliContext{Config: baseCfg}

func cmdTimeoutContext(ctx context.Context) (context.Context, func()) {
	if cliCtx.cmdTimeout != 0 {
		return context.WithTimeout(ctx, cliCtx.cmdTimeout)
	}
	return context.WithCancel(ctx)
}

// sqlCtx captures the command-line parameters of the `sql` command.
// Defaults set by InitCLIDefaults() above.
var sqlCtx = struct {
	*cliContext

	// execStmts is a list of statements to execute.
	execStmts statementsValue

	// safeUpdates indicates whether to set sql_safe_updates in the CLI
	// shell.
	safeUpdates bool

	// echo, when set, requests that SQL queries sent to the server are
	// also printed out on the client.
	echo bool
}{cliContext: &cliCtx}

// dumpCtx captures the command-line parameters of the `sql` command.
// Defaults set by InitCLIDefaults() above.
var dumpCtx struct {
	// dumpMode determines which part of the database should be dumped.
	dumpMode dumpMode

	// asOf determines the time stamp at which the dump should be taken.
	asOf string
}

// debugCtx captures the command-line parameters of the `debug` command.
// Defaults set by InitCLIDefaults() above.
var debugCtx struct {
	startKey, endKey  engine.MVCCKey
	values            bool
	sizes             bool
	replicated        bool
	inputFile         string
	printSystemConfig bool
	maxResults        int64
}

// zoneCtx captures the command-line parameters of the `zone` command.
// Defaults set by InitCLIDefaults() above.
var zoneCtx struct {
	zoneConfig             string
	zoneDisableReplication bool
}

// startCtx captures the command-line arguments for the `start` command.
// Defaults set by InitCLIDefaults() above.
var startCtx struct {
	// server-specific values of some flags.
	serverInsecure    bool
	serverSSLCertsDir string
	serverConnHost    string

	// temporary directory to use to spill computation results to disk.
	tempDir string
	// directory to use for remotely-initiated operations that can
	// specify node-local I/O paths, like BACKUP/RESTORE/IMPORT.
	externalIODir string
}

// quitCtx captures the command-line parameters of the `quit` command.
// Defaults set by InitCLIDefaults() above.
var quitCtx struct {
	serverDecommission bool
}

// nodeCtx captures the command-line parameters of the `node` command.
// Defaults set by InitCLIDefaults() above.
var nodeCtx struct {
	nodeDecommissionWait   nodeDecommissionWaitType
	statusShowRanges       bool
	statusShowStats        bool
	statusShowDecommission bool
	statusShowAll          bool
}
