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
	"io"
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlshell"
	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	isatty "github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// initCLIDefaults serves as the single point of truth for
// configuration defaults. It is suitable for calling between tests of
// the CLI utilities inside a single testing process.
func initCLIDefaults() {
	// We don't reset the pointers (because they are tied into the
	// flags), but instead overwrite the existing structs' values.
	setServerContextDefaults()
	setCliContextDefaults()
	setSQLConnContextDefaults()
	setSQLExecContextDefaults()
	setSQLContextDefaults()
	setZipContextDefaults()
	setDumpContextDefaults()
	setDebugContextDefaults()
	setStartContextDefaults()
	setDrainContextDefaults()
	setNodeContextDefaults()
	setSqlfmtContextDefaults()
	setConvContextDefaults()
	setDemoContextDefaults()
	setStmtDiagContextDefaults()
	setAuthContextDefaults()
	setImportContextDefaults()
	setUserfileContextDefaults()
	setCertContextDefaults()
	setDebugRecoverContextDefaults()
	setDebugSendKVBatchContextDefaults()

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
	logcrash.SetGlobalSettings(&st.SV)

	return server.MakeConfig(context.Background(), st)
}()

// setServerContextDefaults set the default values in serverCfg.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setServerContextDefaults() {
	st := cluster.MakeClusterSettings()
	logcrash.SetGlobalSettings(&st.SV)
	serverCfg.SetDefaults(context.Background(), st)

	serverCfg.TenantKVAddrs = []string{"127.0.0.1:26257"}

	// Attempt to default serverCfg.MemoryPoolSize to 25% if possible.
	if bytes, _ := memoryPercentResolver(25); bytes != 0 {
		serverCfg.SQLConfig.MemoryPoolSize = bytes
	}

	// Attempt to set serverCfg.TimeSeriesServerConfig.QueryMemoryMax to
	// the default (64MiB) or 1% of system memory, whichever is greater.
	if bytes, _ := memoryPercentResolver(1); bytes != 0 {
		if bytes > ts.DefaultQueryMemoryMax {
			serverCfg.TimeSeriesServerConfig.QueryMemoryMax = bytes
		} else {
			serverCfg.TimeSeriesServerConfig.QueryMemoryMax = ts.DefaultQueryMemoryMax
		}
	}
}

// baseCfg points to the base.Config inside serverCfg.
var baseCfg = serverCfg.Config

// cliContext captures the command-line parameters shared by most CLI
// commands. See below for defaults.
type cliContext struct {
	// Embed the base context.
	*base.Config

	// Embed the new-style configuration context.
	clicfg.Context

	// cmdTimeout sets the maximum run time for the command.
	// Commands that wish to use this must use cmdTimeoutContext().
	cmdTimeout time.Duration

	// certPrincipalMap is the cert-principal:db-principal map.
	// This configuration flag is only used for client commands that establish
	// a connection to a server.
	certPrincipalMap []string

	// clientOpts is the set of client options to generate connection
	// URLs. Note that the ClientSecurityOptions field in clientOpts is
	// not used; it is populated by makeClientConnURL() from base.Config
	// instead.
	clientOpts clientsecopts.ClientOptions

	// allowUnencryptedClientPassword enables the CLI commands to use
	// password authentication over non-TLS TCP connections. This is
	// disallowed by default: the user must opt-in and understand that
	// CockroachDB does not guarantee confidentiality of a password
	// provided this way.
	// TODO(knz): Relax this when SCRAM is implemented.
	allowUnencryptedClientPassword bool

	// logConfigInput is the YAML input for the logging configuration.
	logConfigInput settableString
	// logConfigVars is an array of environment variables used in the logging
	// configuration that will be expanded by CRDB.
	logConfigVars []string
	// logConfig is the resulting logging configuration after the input
	// configuration has been parsed and validated.
	logConfig logconfig.Config
	// logShutdownFn is used to close & teardown logging facilities gracefully
	// before exiting the process. This may block until all buffered log sinks
	// are shutdown, if any are enabled, or until a timeout is triggered.
	logShutdownFn func()
	// logOverrides encodes discrete flag overrides for the logging
	// configuration. They are provided for convenience.
	logOverrides *logConfigFlags
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
	Config:       baseCfg,
	logOverrides: newLogConfigOverrides(),
}

// setCliContextDefaults set the default values in cliCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setCliContextDefaults() {
	// isInteractive is only set to `true` by `cockroach sql` -- all
	// other client commands are non-interactive, regardless of whether
	// the standard input is a terminal.
	cliCtx.IsInteractive = false
	cliCtx.EmbeddedMode = false
	cliCtx.cmdTimeout = 0 // no timeout
	cliCtx.clientOpts.ServerHost = getDefaultHost()
	cliCtx.clientOpts.ServerPort = base.DefaultPort
	cliCtx.certPrincipalMap = nil
	cliCtx.clientOpts.ExplicitURL = nil
	cliCtx.clientOpts.User = username.RootUser
	cliCtx.clientOpts.Database = ""
	cliCtx.allowUnencryptedClientPassword = false
	cliCtx.logConfigInput = settableString{s: ""}
	cliCtx.logConfigVars = nil
	cliCtx.logConfig = logconfig.Config{}
	cliCtx.logShutdownFn = func() {}
	cliCtx.ambiguousLogDir = false
	cliCtx.logOverrides.reset()
	cliCtx.showVersionUsingOnlyBuildTag = false
}

// sqlConnContext captures the connection configuration for all SQL
// clients. See below for defaults.
var sqlConnCtx = clisqlclient.Context{
	CliCtx: &cliCtx.Context,
}

// setSQLConnContextDefaults set the default values in sqlConnCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setSQLConnContextDefaults() {
	// See also setCLIDefaultForTests() in cli_test.go.
	sqlConnCtx.DebugMode = false
	sqlConnCtx.Echo = false
	sqlConnCtx.EnableServerExecutionTimings = false
}

// certCtx captures the command-line parameters of the various `cert` commands.
// See below for defaults.
var certCtx struct {
	certsDir              string
	caKey                 string
	keySize               int
	caCertificateLifetime time.Duration
	certificateLifetime   time.Duration
	allowCAKeyReuse       bool
	overwriteFiles        bool
	generatePKCS8Key      bool
	// certPrincipalMap is the cert-principal:db-principal map.
	// This configuration flag is only used for 'cert' commands
	// that generate certificates.
	certPrincipalMap []string
	// tenantScope indicates a tenantID(s) that a certificate is being
	// scoped to. By creating a tenant-scoped certicate, the usage of that certificate
	// is restricted to a specific tenant.
	tenantScope []roachpb.TenantID

	// disableUsernameValidation removes the username syntax check on
	// the input.
	disableUsernameValidation bool
}

func setCertContextDefaults() {
	certCtx.certsDir = base.DefaultCertsDirectory
	certCtx.caKey = ""
	certCtx.keySize = defaultKeySize
	certCtx.caCertificateLifetime = defaultCALifetime
	certCtx.certificateLifetime = defaultCertLifetime
	certCtx.allowCAKeyReuse = false
	certCtx.overwriteFiles = false
	certCtx.generatePKCS8Key = false
	certCtx.disableUsernameValidation = false
	certCtx.certPrincipalMap = nil
	certCtx.tenantScope = []roachpb.TenantID{roachpb.SystemTenantID}
}

var sqlExecCtx = clisqlexec.Context{
	CliCtx: &cliCtx.Context,
}

// PrintQueryOutput takes a list of column names and a list of row
// contents writes a formatted table to 'w'.
//
// This binds PrintQueryOutput to this package's common/global
// CLI configuration, for use by other packages like the CCL CLI.
func PrintQueryOutput(w io.Writer, cols []string, allRows clisqlexec.RowStrIter) error {
	return sqlExecCtx.PrintQueryOutput(w, stderr, cols, allRows)
}

// setSQLConnContextDefaults set the default values in sqlConnCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setSQLExecContextDefaults() {
	// See also setCLIDefaultForTests() in cli_test.go.
	sqlExecCtx.TerminalOutput = isatty.IsTerminal(os.Stdout.Fd())
	sqlExecCtx.TableDisplayFormat = clisqlexec.TableDisplayTSV
	sqlExecCtx.TableBorderMode = 0 /* no outer lines + no inside row lines */
	if sqlExecCtx.TerminalOutput {
		// See also setCLIDefaultForTests() in cli_test.go.
		sqlExecCtx.TableDisplayFormat = clisqlexec.TableDisplayTable
	}
	sqlExecCtx.ShowTimes = false
	sqlExecCtx.VerboseTimings = false
}

var sqlCtx = func() *clisqlcfg.Context {
	cfg := &clisqlcfg.Context{
		CliCtx:  &cliCtx.Context,
		ConnCtx: &sqlConnCtx,
		ExecCtx: &sqlExecCtx,
	}
	return cfg
}()

// setSQLContextDefaults set the default values in sqlCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setSQLContextDefaults() {
	sqlCtx.LoadDefaults(os.Stdout, stderr)
}

// zipCtx captures the command-line parameters of the `zip` command.
// See below for defaults.
var zipCtx zipContext

type zipContext struct {
	nodes nodeSelection

	// DEPRECATED: redactLogs indicates whether log files should be
	// redacted server-side during retrieval. This flag is deprecated
	// in favor of redact.
	redactLogs bool

	// redact indicates whether the entirety of debug zip should
	// be redacted server-side during retrieval, except for
	// range key data, which is necessary to support CockroachDB.
	redact bool

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
	zipCtx.redact = false
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

	// These configuration handles are flag.Value instances that allow
	// configuring other variables using either a percentage or a
	// humanized size value.
	cacheSizeValue           bytesOrPercentageValue
	sqlSizeValue             bytesOrPercentageValue
	diskTempStorageSizeValue bytesOrPercentageValue
	tsdbSizeValue            bytesOrPercentageValue
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
	startCtx.cacheSizeValue = makeBytesOrPercentageValue(&serverCfg.CacheSize, memoryPercentResolver)
	startCtx.sqlSizeValue = makeBytesOrPercentageValue(&serverCfg.MemoryPoolSize, memoryPercentResolver)
	startCtx.diskTempStorageSizeValue = makeBytesOrPercentageValue(nil /* v */, nil /* percentResolver */)
	startCtx.tsdbSizeValue = makeBytesOrPercentageValue(&serverCfg.TimeSeriesServerConfig.QueryMemoryMax, memoryPercentResolver)
}

// drainCtx captures the command-line parameters of the `node drain`
// commands.
// See below for defaults.
var drainCtx struct {
	// drainWait is the amount of time to wait for the server
	// to drain. Set to 0 to disable a timeout (let the server decide).
	drainWait time.Duration
	// nodeDrainSelf indicates that the command should target
	// the node we're connected to (this is the default behavior).
	nodeDrainSelf bool
}

// setDrainContextDefaults set the default values in drainCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setDrainContextDefaults() {
	drainCtx.drainWait = 10 * time.Minute
	drainCtx.nodeDrainSelf = false
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
	nodeCtx.nodeDecommissionSelf = false
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
	execStmts  clisqlshell.StatementsValue
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
var demoCtx = struct {
	democluster.Context
	disableEnterpriseFeatures bool
	pidFile                   string

	demoNodeCacheSizeValue  bytesOrPercentageValue
	demoNodeSQLMemSizeValue bytesOrPercentageValue
}{
	Context: democluster.Context{
		CliCtx: &cliCtx.Context,
	},
}

// setDemoContextDefaults set the default values in demoCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setDemoContextDefaults() {
	demoCtx.NumNodes = 1
	demoCtx.SQLPoolMemorySize = 128 << 20 // 128MB, chosen to fit 9 nodes on 2GB machine.
	demoCtx.CacheSize = 64 << 20          // 64MB, chosen to fit 9 nodes on 2GB machine.
	demoCtx.UseEmptyDatabase = false
	demoCtx.SimulateLatency = false
	demoCtx.RunWorkload = false
	demoCtx.Localities = nil
	demoCtx.GeoPartitionedReplicas = false
	demoCtx.DefaultKeySize = defaultKeySize
	demoCtx.DefaultCALifetime = defaultCALifetime
	demoCtx.DefaultCertLifetime = defaultCertLifetime
	demoCtx.Insecure = false
	demoCtx.SQLPort, _ = strconv.Atoi(base.DefaultPort)
	demoCtx.HTTPPort, _ = strconv.Atoi(base.DefaultHTTPPort)
	demoCtx.WorkloadMaxQPS = 25
	demoCtx.Multitenant = true
	demoCtx.DisableServerController = false
	demoCtx.DefaultEnableRangefeeds = true

	demoCtx.pidFile = ""
	demoCtx.disableEnterpriseFeatures = false

	demoCtx.demoNodeCacheSizeValue = makeBytesOrPercentageValue(
		&demoCtx.CacheSize,
		memoryPercentResolver,
	)
	demoCtx.demoNodeSQLMemSizeValue = makeBytesOrPercentageValue(
		&demoCtx.SQLPoolMemorySize,
		memoryPercentResolver,
	)
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
