// Copyright 2016 The Cockroach Authors.
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
	"context"
	"net/url"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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
		cliCtx.tableDisplayFormat = tableDisplayTable
	}
	cliCtx.cmdTimeout = 0 // no timeout
	cliCtx.clientConnHost = ""
	cliCtx.clientConnPort = base.DefaultPort
	cliCtx.sqlConnURL = ""
	cliCtx.sqlConnUser = ""
	cliCtx.sqlConnPasswd = ""
	cliCtx.sqlConnDBName = ""
	cliCtx.extraConnURLOptions = nil

	sqlCtx.setStmts = nil
	sqlCtx.execStmts = nil
	sqlCtx.safeUpdates = false
	sqlCtx.showTimes = false
	sqlCtx.debugMode = false
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
	debugCtx.ballastSize = base.SizeSpec{}

	zoneCtx.zoneConfig = ""
	zoneCtx.zoneDisableReplication = false

	serverCfg.ReadyFn = nil
	serverCfg.DelayedBootstrapFn = nil
	serverCfg.SocketFile = ""
	serverCfg.JoinList = nil

	startCtx.serverInsecure = baseCfg.Insecure
	startCtx.serverSSLCertsDir = base.DefaultCertsDirectory
	startCtx.serverListenAddr = ""
	startCtx.tempDir = ""
	startCtx.externalIODir = ""
	startCtx.listeningURLFile = ""
	startCtx.pidFile = ""
	startCtx.inBackground = false

	quitCtx.serverDecommission = false

	nodeCtx.nodeDecommissionWait = nodeDecommissionWaitAll
	nodeCtx.statusShowRanges = false
	nodeCtx.statusShowStats = false
	nodeCtx.statusShowAll = false
	nodeCtx.statusShowDecommission = false

	cfg := tree.DefaultPrettyCfg()
	sqlfmtCtx.len = cfg.LineWidth
	sqlfmtCtx.useSpaces = !cfg.UseTabs
	sqlfmtCtx.tabWidth = cfg.TabWidth
	sqlfmtCtx.noSimplify = !cfg.Simplify
	sqlfmtCtx.align = (cfg.Align != tree.PrettyNoAlign)
	sqlfmtCtx.execStmts = nil

	systemBenchCtx.concurrency = 1
	systemBenchCtx.duration = 60 * time.Second
	systemBenchCtx.tempDir = "."
	systemBenchCtx.writeSize = 32 << 10
	systemBenchCtx.syncInterval = 512 << 10

	networkBenchCtx.server = true
	networkBenchCtx.port = 8081
	networkBenchCtx.addresses = []string{"localhost:8081"}

	initPreFlagsDefaults()

	// Clear the "Changed" state of all the registered command-line flags.
	clearFlagChanges(cockroachCmd)
}

func clearFlagChanges(cmd *cobra.Command) {
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) { f.Changed = false })
	for _, subCmd := range cmd.Commands() {
		clearFlagChanges(subCmd)
	}
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

	// cmdTimeout sets the maximum run time for the command.
	// Commands that wish to use this must use cmdTimeoutContext().
	cmdTimeout time.Duration

	// clientConnHost is the hostname/address to use to connect to a server.
	clientConnHost string

	// clientConnPort is the port name/number to use to connect to a server.
	clientConnPort string

	// for CLI commands that use the SQL interface, these parameters
	// determine how to connect to the server.
	sqlConnURL, sqlConnUser, sqlConnDBName string

	// The client password to use. This can be set via the --url flag.
	sqlConnPasswd string

	// extraConnURLOptions contains any additional query URL options
	// specified in --url that do not have discrete equivalents.
	extraConnURLOptions url.Values
}

// cliCtx captures the command-line parameters common to most CLI utilities.
// Defaults set by InitCLIDefaults() above.
var cliCtx = cliContext{Config: baseCfg}

// sqlCtx captures the command-line parameters of the `sql` command.
// Defaults set by InitCLIDefaults() above.
var sqlCtx = struct {
	*cliContext

	// setStmts is a list of \set commands to execute before entering the sql shell.
	setStmts statementsValue

	// execStmts is a list of statements to execute.
	execStmts statementsValue

	// safeUpdates indicates whether to set sql_safe_updates in the CLI
	// shell.
	safeUpdates bool

	// showTimes indicates whether to display query times after each result line.
	showTimes bool

	// echo, when set, requests that SQL queries sent to the server are
	// also printed out on the client.
	echo bool

	// debugMode, when set, overrides the defaults to disable as much
	// "intelligent behavior" in the SQL shell as possible and become
	// more verbose (sets echo).
	debugMode bool
}{cliContext: &cliCtx}

// dumpCtx captures the command-line parameters of the `dump` command.
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
	ballastSize       base.SizeSpec
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
	serverListenAddr  string

	// temporary directory to use to spill computation results to disk.
	tempDir string
	// directory to use for remotely-initiated operations that can
	// specify node-local I/O paths, like BACKUP/RESTORE/IMPORT.
	externalIODir string

	// inBackground is set to true when restarting in the
	// background after --background was processed.
	inBackground bool

	// listeningURLFile indicates the file to which the server writes
	// its listening URL when it is ready.
	listeningURLFile string

	// pidFile indicates the file to which the server writes its PID
	// when it is ready.
	pidFile string

	// logging settings specific to file logging.
	logDir     log.DirName
	logDirFlag *pflag.Flag
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

// systemBenchCtx captures the command-line parameters of the `systembench` command.
// Defaults set by InitCLIDefaults() above.
var systemBenchCtx struct {
	concurrency  int
	duration     time.Duration
	tempDir      string
	writeSize    int64
	syncInterval int64
}

var networkBenchCtx struct {
	server    bool
	port      int
	addresses []string
	latency   bool
}

// sqlfmtCtx captures the command-line parameters of the `sqlfmt` command.
// Defaults set by InitCLIDefaults() above.
var sqlfmtCtx struct {
	len        int
	useSpaces  bool
	tabWidth   int
	noSimplify bool
	align      bool
	execStmts  statementsValue
}

// demoCtx captures the command-line parameters of the `demo` command.
// Defaults set by InitCLIDefaults() above.
var demoCtx struct {
	nodes int
}
