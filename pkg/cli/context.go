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
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	isatty "github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// initCLIDefaults serves as the single point of truth for
// configuration defaults. It is suitable for calling between tests of
// the CLI utilities inside a single testing process.
func initCLIDefaults() {
	setServerContextDefaults()
	// We don't reset the pointers (because they are tied into the
	// flags), but instead overwrite the existing structs' values.
	baseCfg.InitDefaults()
	setCliContextDefaults()
	setSQLContextDefaults()
	setZipContextDefaults()
	setDumpContextDefaults()
	setDebugContextDefaults()
	setStartContextDefaults()
	setQuitContextDefaults()
	setNodeContextDefaults()
	setSqlfmtContextDefaults()
	setConvContextDefaults()
	setDemoContextDefaults()
	setStmtDiagContextDefaults()
	setAuthContextDefaults()
	setImportContextDefaults()
	setProxyContextDefaults()
	setTestDirectorySvrContextDefaults()
	setUserfileContextDefaults()

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

// serverCfg is used as the client-side copy of default server
// parameters for CLI utilities.
//
// NB: `cockroach start` further initializes serverCfg for the newly
// created server.
//
// See below for defaults.
var serverCfg = func() server.Config {
	st := cluster.MakeClusterSettings()
	settings.SetCanonicalValuesContainer(&st.SV)

	return server.MakeConfig(context.Background(), st)
}()

// setServerContextDefaults set the default values in serverCfg.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setServerContextDefaults() {
	serverCfg.BaseConfig.DefaultZoneConfig = zonepb.DefaultZoneConfig()

	serverCfg.ClockDevicePath = ""
	serverCfg.ExternalIODirConfig = base.ExternalIODirConfig{}
	serverCfg.GoroutineDumpDirName = ""
	serverCfg.HeapProfileDirName = ""
	serverCfg.CPUProfileDirName = ""

	serverCfg.AutoInitializeCluster = false
	serverCfg.KVConfig.ReadyFn = nil
	serverCfg.KVConfig.DelayedBootstrapFn = nil
	serverCfg.KVConfig.JoinList = nil
	serverCfg.KVConfig.JoinPreferSRVRecords = false
	serverCfg.KVConfig.DefaultSystemZoneConfig = zonepb.DefaultSystemZoneConfig()
	// Reset the store list.
	storeSpec, _ := base.NewStoreSpec(server.DefaultStorePath)
	serverCfg.Stores = base.StoreSpecList{Specs: []base.StoreSpec{storeSpec}}

	serverCfg.TenantKVAddrs = []string{"127.0.0.1:26257"}

	serverCfg.SQLConfig.SocketFile = ""
	// Attempt to default serverCfg.MemoryPoolSize to 25% if possible.
	if bytes, _ := memoryPercentResolver(25); bytes != 0 {
		serverCfg.SQLConfig.MemoryPoolSize = bytes
	}
}

// baseCfg points to the base.Config inside serverCfg.
var baseCfg = serverCfg.Config

// cliContext captures the command-line parameters shared by most CLI
// commands. See below for defaults.
type cliContext struct {
	// Embed the base context.
	*base.Config

	// isInteractive indicates whether the session is interactive, that
	// is, the commands executed are extremely likely to be *input* from
	// a human user: the standard input is a terminal and `-e` was not
	// used (the shell has a prompt).
	//
	// Refer to README.md to understand the general design guidelines for
	// CLI utilities with interactive vs non-interactive input.
	isInteractive bool

	// terminalOutput indicates whether output is going to a terminal,
	// that is, it is not going to a file, another program for automated
	// processing, etc.: the standard output is a terminal.
	//
	// Refer to README.md to understand the general design guidelines for
	// CLI utilities with terminal vs non-terminal output.
	terminalOutput bool

	// tableDisplayFormat indicates how to format result tables.
	tableDisplayFormat tableDisplayFormat

	// tableBorderMode indicates how to format tables when the display
	// format is 'table'. This exists for compatibility
	// with psql: https://www.postgresql.org/docs/12/app-psql.html
	tableBorderMode int

	// cmdTimeout sets the maximum run time for the command.
	// Commands that wish to use this must use cmdTimeoutContext().
	cmdTimeout time.Duration

	// clientConnHost is the hostname/address to use to connect to a server.
	clientConnHost string

	// clientConnPort is the port name/number to use to connect to a server.
	clientConnPort string

	// certPrincipalMap is the cert-principal:db-principal map.
	certPrincipalMap []string

	// for CLI commands that use the SQL interface, these parameters
	// determine how to connect to the server.
	sqlConnUser, sqlConnDBName string

	// sqlConnURL contains any additional query URL options
	// specified in --url that do not have discrete equivalents.
	sqlConnURL *pgurl.URL

	// allowUnencryptedClientPassword enables the CLI commands to use
	// password authentication over non-TLS TCP connections. This is
	// disallowed by default: the user must opt-in and understand that
	// CockroachDB does not guarantee confidentiality of a password
	// provided this way.
	// TODO(knz): Relax this when SCRAM is implemented.
	allowUnencryptedClientPassword bool

	// logConfigInput is the YAML input for the logging configuration.
	logConfigInput settableString
	// logConfig is the resulting logging configuration after the input
	// configuration has been parsed and validated.
	logConfig logconfig.Config
	// deprecatedLogOverrides is the legacy pre-v21.1 discrete flag
	// overrides for the logging configuration.
	// TODO(knz): Deprecated in v21.1. Remove this.
	deprecatedLogOverrides *logConfigFlags
	// ambiguousLogDir is populated during setupLogging() to indicate
	// that no log directory was specified and there were multiple
	// on-disk stores.
	ambiguousLogDir bool

	// For `cockroach version --build-tag`.
	showVersionUsingOnlyBuildTag bool
}

// cliCtx captures the command-line parameters common to most CLI utilities.
// See below for defaults.
var cliCtx = cliContext{
	Config: baseCfg,
	// TODO(knz): Deprecated in v21.1. Remove this.
	deprecatedLogOverrides: newLogConfigOverrides(),
}

// setCliContextDefaults set the default values in cliCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setCliContextDefaults() {
	// isInteractive is only set to `true` by `cockroach sql` -- all
	// other client commands are non-interactive, regardless of whether
	// the standard input is a terminal.
	cliCtx.isInteractive = false
	// See also setCLIDefaultForTests() in cli_test.go.
	cliCtx.terminalOutput = isatty.IsTerminal(os.Stdout.Fd())
	cliCtx.tableDisplayFormat = tableDisplayTSV
	cliCtx.tableBorderMode = 0 /* no outer lines + no inside row lines */
	if cliCtx.terminalOutput {
		// See also setCLIDefaultForTests() in cli_test.go.
		cliCtx.tableDisplayFormat = tableDisplayTable
	}
	cliCtx.cmdTimeout = 0 // no timeout
	cliCtx.clientConnHost = ""
	cliCtx.clientConnPort = base.DefaultPort
	cliCtx.certPrincipalMap = nil
	cliCtx.sqlConnURL = nil
	cliCtx.sqlConnUser = security.RootUser
	cliCtx.sqlConnDBName = ""
	cliCtx.allowUnencryptedClientPassword = false
	cliCtx.logConfigInput = settableString{s: ""}
	cliCtx.logConfig = logconfig.Config{}
	cliCtx.ambiguousLogDir = false
	// TODO(knz): Deprecated in v21.1. Remove this.
	cliCtx.deprecatedLogOverrides.reset()
	cliCtx.showVersionUsingOnlyBuildTag = false
}

// sqlCtx captures the configuration of the `sql` command.
// See below for defaults.
var sqlCtx = struct {
	*cliContext

	// setStmts is a list of \set commands to execute before entering the sql shell.
	setStmts statementsValue

	// execStmts is a list of statements to execute.
	// Only valid if inputFile is empty.
	execStmts statementsValue

	// quitAfterExecStmts tells the shell whether to quit
	// after processing the execStmts.
	quitAfterExecStmts bool

	// inputFile is the file to read from.
	// If empty, os.Stdin is used.
	// Only valid if execStmts is empty.
	inputFile string

	// repeatDelay indicates that the execStmts should be "watched"
	// at the specified time interval. Zero disables
	// the watch.
	repeatDelay time.Duration

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

	// embeddedMode, when set, reduces the amount of informational
	// messages printed out to exclude details that are not
	// under user's control when the shell is run by a playground environment.
	embeddedMode bool

	// Determines whether to display server execution timings in the CLI.
	enableServerExecutionTimings bool

	// Determine whether to show raw durations.
	verboseTimings bool

	// Determines whether to stop the client upon encountering an error.
	errExit bool

	// Determines whether to perform client-side syntax checking.
	checkSyntax bool

	// autoTrace, when non-empty, encloses the executed statements
	// by suitable SET TRACING and SHOW TRACE FOR SESSION statements.
	autoTrace string

	// The string used to produce the value of fullPrompt.
	customPromptPattern string
}{cliContext: &cliCtx}

// setSQLContextDefaults set the default values in sqlCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setSQLContextDefaults() {
	sqlCtx.setStmts = nil
	sqlCtx.execStmts = nil
	sqlCtx.quitAfterExecStmts = false
	sqlCtx.inputFile = ""
	sqlCtx.repeatDelay = 0
	sqlCtx.safeUpdates = false
	sqlCtx.showTimes = false
	sqlCtx.debugMode = false
	sqlCtx.echo = false
	sqlCtx.enableServerExecutionTimings = false
	sqlCtx.verboseTimings = false
	sqlCtx.errExit = false
	sqlCtx.checkSyntax = false
	sqlCtx.autoTrace = ""
	sqlCtx.customPromptPattern = defaultPromptPattern
}

// zipCtx captures the command-line parameters of the `zip` command.
// See below for defaults.
var zipCtx zipContext

type zipContext struct {
	nodes nodeSelection

	// redactLogs indicates whether log files should be redacted
	// server-side during retrieval.
	redactLogs bool

	// Duration (in seconds) to run CPU profile for.
	cpuProfDuration time.Duration

	// How much concurrency to use during the collection. The code
	// attempts to access multiple nodes concurrently by default.
	concurrency int

	// The log/heap/etc files to include.
	files fileSelection
}

// setZipContextDefaults set the default values in zipCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setZipContextDefaults() {
	zipCtx.nodes = nodeSelection{}
	zipCtx.files = fileSelection{}
	zipCtx.redactLogs = false
	zipCtx.cpuProfDuration = 5 * time.Second
	zipCtx.concurrency = 15

	// File selection covers the last 48 hours by default.
	// We add 24 hours to now for the end timestamp to ensure
	// that files created during the zip operation are
	// also included.
	now := timeutil.Now()
	zipCtx.files.startTimestamp = timestampValue(now.Add(-48 * time.Hour))
	zipCtx.files.endTimestamp = timestampValue(now.Add(24 * time.Hour))
}

// dumpCtx captures the command-line parameters of the `dump` command.
// See below for defaults.
var dumpCtx struct {
	// dumpMode determines which part of the database should be dumped.
	dumpMode dumpMode

	// asOf determines the time stamp at which the dump should be taken.
	asOf string

	// dumpAll determines whenever we going to dump all databases
	dumpAll bool
}

// setDumpContextDefaults set the default values in dumpCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setDumpContextDefaults() {
	dumpCtx.dumpMode = dumpBoth
	dumpCtx.asOf = ""
	dumpCtx.dumpAll = false
}

// authCtx captures the command-line parameters of the `auth-session`
// command. See below for defaults.
var authCtx struct {
	onlyCookie     bool
	validityPeriod time.Duration
}

// setAuthContextDefaults set the default values in authCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setAuthContextDefaults() {
	authCtx.onlyCookie = false
	authCtx.validityPeriod = 1 * time.Hour
}

// debugCtx captures the command-line parameters of the `debug` command.
// See below for defaults.
var debugCtx struct {
	startKey, endKey  storage.MVCCKey
	values            bool
	sizes             bool
	replicated        bool
	inputFile         string
	ballastSize       base.SizeSpec
	printSystemConfig bool
	maxResults        int
	decodeAsTableDesc string
	verbose           bool
	keyTypes          keyTypeFilter
}

// setDebugContextDefaults set the default values in debugCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setDebugContextDefaults() {
	debugCtx.startKey = storage.NilKey
	debugCtx.endKey = storage.NilKey
	debugCtx.values = false
	debugCtx.sizes = false
	debugCtx.replicated = false
	debugCtx.inputFile = ""
	debugCtx.ballastSize = base.SizeSpec{InBytes: 1000000000}
	debugCtx.maxResults = 0
	debugCtx.printSystemConfig = false
	debugCtx.decodeAsTableDesc = ""
	debugCtx.verbose = false
	debugCtx.keyTypes = showAll
}

// startCtx captures the command-line arguments for the `start` command.
// See below for defaults.
var startCtx struct {
	// server-specific values of some flags.
	serverInsecure         bool
	serverSSLCertsDir      string
	serverCertPrincipalMap []string
	serverListenAddr       string

	// The TLS auto-handshake parameters.
	initToken             string
	numExpectedNodes      int
	genCertsForSingleNode bool

	// if specified, this forces the HTTP listen addr to localhost
	// and disables TLS on the HTTP listener.
	unencryptedLocalhostHTTP bool

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

	// geoLibsDir is used to specify locations of the GEOS library.
	geoLibsDir string
}

// setStartContextDefaults set the default values in startCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setStartContextDefaults() {
	startCtx.serverInsecure = baseCfg.Insecure
	startCtx.serverSSLCertsDir = base.DefaultCertsDirectory
	startCtx.serverCertPrincipalMap = nil
	startCtx.serverListenAddr = ""
	startCtx.initToken = ""
	startCtx.numExpectedNodes = 0
	startCtx.genCertsForSingleNode = false
	startCtx.unencryptedLocalhostHTTP = false
	startCtx.tempDir = ""
	startCtx.externalIODir = ""
	startCtx.listeningURLFile = ""
	startCtx.pidFile = ""
	startCtx.inBackground = false
	startCtx.geoLibsDir = "/usr/local/lib/cockroach"
}

// quitCtx captures the command-line parameters of the `quit` and
// `node drain` commands.
// See below for defaults.
var quitCtx struct {
	// drainWait is the amount of time to wait for the server
	// to drain. Set to 0 to disable a timeout (let the server decide).
	drainWait time.Duration
}

// setQuitContextDefaults set the default values in quitCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setQuitContextDefaults() {
	quitCtx.drainWait = 10 * time.Minute
}

// nodeCtx captures the command-line parameters of the `node` command.
// See below for defaults.
var nodeCtx struct {
	nodeDecommissionWait   nodeDecommissionWaitType
	nodeDecommissionSelf   bool
	statusShowRanges       bool
	statusShowStats        bool
	statusShowDecommission bool
	statusShowAll          bool
}

// setNodeContextDefaults set the default values in nodeCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setNodeContextDefaults() {
	nodeCtx.nodeDecommissionWait = nodeDecommissionWaitAll
	nodeCtx.statusShowRanges = false
	nodeCtx.statusShowStats = false
	nodeCtx.statusShowAll = false
	nodeCtx.statusShowDecommission = false
}

// sqlfmtCtx captures the command-line parameters of the `sqlfmt` command.
// See below for defaults.
var sqlfmtCtx struct {
	len        int
	useSpaces  bool
	tabWidth   int
	noSimplify bool
	align      bool
	execStmts  statementsValue
}

// setSqlfmtContextDefaults set the default values in sqlfmtCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setSqlfmtContextDefaults() {
	cfg := tree.DefaultPrettyCfg()
	sqlfmtCtx.len = cfg.LineWidth
	sqlfmtCtx.useSpaces = !cfg.UseTabs
	sqlfmtCtx.tabWidth = cfg.TabWidth
	sqlfmtCtx.noSimplify = !cfg.Simplify
	sqlfmtCtx.align = (cfg.Align != tree.PrettyNoAlign)
	sqlfmtCtx.execStmts = nil
}

var convertCtx struct {
	url string
}

// setConvContextDefaults set the default values in convertCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setConvContextDefaults() {
	convertCtx.url = ""
}

// demoCtx captures the command-line parameters of the `demo` command.
// See below for defaults.
var demoCtx struct {
	nodes                     int
	sqlPoolMemorySize         int64
	cacheSize                 int64
	disableTelemetry          bool
	disableLicenseAcquisition bool
	noExampleDatabase         bool
	runWorkload               bool
	localities                demoLocalityList
	geoPartitionedReplicas    bool
	simulateLatency           bool
	transientCluster          *transientCluster
	insecure                  bool
	sqlPort                   int
	httpPort                  int
}

// setDemoContextDefaults set the default values in demoCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setDemoContextDefaults() {
	demoCtx.nodes = 1
	demoCtx.sqlPoolMemorySize = 128 << 20 // 128MB, chosen to fit 9 nodes on 2GB machine.
	demoCtx.cacheSize = 64 << 20          // 64MB, chosen to fit 9 nodes on 2GB machine.
	demoCtx.noExampleDatabase = false
	demoCtx.simulateLatency = false
	demoCtx.runWorkload = false
	demoCtx.localities = nil
	demoCtx.geoPartitionedReplicas = false
	demoCtx.disableTelemetry = false
	demoCtx.disableLicenseAcquisition = false
	demoCtx.transientCluster = nil
	demoCtx.insecure = false
	demoCtx.sqlPort, _ = strconv.Atoi(base.DefaultPort)
	demoCtx.httpPort, _ = strconv.Atoi(base.DefaultHTTPPort)
}

// stmtDiagCtx captures the command-line parameters of the 'statement-diag'
// command.
var stmtDiagCtx struct {
	all bool
}

func setStmtDiagContextDefaults() {
	stmtDiagCtx.all = false
}

// importCtx captures the command-line parameters of the 'import' command.
var importCtx struct {
	maxRowSize           int
	skipForeignKeys      bool
	ignoreUnsupported    bool
	ignoreUnsupportedLog string
	rowLimit             int
}

func setImportContextDefaults() {
	importCtx.maxRowSize = 512 * (1 << 10) // 512 KiB
	importCtx.skipForeignKeys = false
	importCtx.ignoreUnsupported = false
	importCtx.ignoreUnsupportedLog = ""
	importCtx.rowLimit = 0
}

// proxyContext captures the command-line parameters of the `mt start-proxy` command.
var proxyContext sqlproxyccl.ProxyOptions

func setProxyContextDefaults() {
	proxyContext.Denylist = ""
	proxyContext.ListenAddr = "127.0.0.1:46257"
	proxyContext.ListenCert = ""
	proxyContext.ListenKey = ""
	proxyContext.MetricsAddress = "0.0.0.0:8080"
	proxyContext.RoutingRule = ""
	proxyContext.DirectoryAddr = ""
	proxyContext.SkipVerify = false
	proxyContext.Insecure = false
	proxyContext.RatelimitBaseDelay = 50 * time.Millisecond
	proxyContext.ValidateAccessInterval = 30 * time.Second
	proxyContext.PollConfigInterval = 30 * time.Second
	proxyContext.DrainTimeout = 0
}

var testDirectorySvrContext struct {
	port int
}

func setTestDirectorySvrContextDefaults() {
	testDirectorySvrContext.port = 36257
}

// userfileCtx captures the command-line parameters of the
// `userfile` command.
// See below for defaults.
var userfileCtx struct {
	// When set, the entire subtree rooted at the source directory will be
	// uploaded to the destination.
	recursive bool
}

// setUserfileContextDefaults sets the default values in userfileCtx.
// This function is called by initCLIDefaults() and thus re-called in
// every test that exercises command-line parsing.
func setUserfileContextDefaults() {
	userfileCtx.recursive = false
}

// GetServerCfgStores provides direct public access to the StoreSpecList inside
// serverCfg. This is used by CCL code to populate some fields.
//
// WARNING: consider very carefully whether you should be using this.
// If you are not writing CCL code that performs command-line flag
// parsing, you probably should not be using this.
func GetServerCfgStores() base.StoreSpecList {
	return serverCfg.Stores
}
