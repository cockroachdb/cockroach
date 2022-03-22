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
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl"
	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlshell"
	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
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
	setServerContextDefaults()
	// We don't reset the pointers (because they are tied into the
	// flags), but instead overwrite the existing structs' values.
	baseCfg.InitDefaults()
	setCliContextDefaults()
	setSQLConnContextDefaults()
	setSQLExecContextDefaults()
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
	serverCfg.BaseConfig.DefaultZoneConfig = zonepb.DefaultZoneConfig()

	serverCfg.ClockDevicePath = ""
	serverCfg.ExternalIODirConfig = base.ExternalIODirConfig{}
	serverCfg.GoroutineDumpDirName = ""
	serverCfg.HeapProfileDirName = ""
	serverCfg.CPUProfileDirName = ""
	serverCfg.InflightTraceDirName = ""

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

	// clientConnHost is the hostname/address to use to connect to a server.
	clientConnHost string

	// clientConnPort is the port name/number to use to connect to a server.
	clientConnPort string

	// certPrincipalMap is the cert-principal:db-principal map.
	// This configuration flag is only used for client commands that establish
	// a connection to a server.
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
	cliCtx.IsInteractive = false
	cliCtx.EmbeddedMode = false
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
	certCtx.certPrincipalMap = nil
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
	// nodeDrainSelf indicates that the command should target
	// the node we're connected to (this is the default behavior).
	nodeDrainSelf bool
}

// setQuitContextDefaults set the default values in quitCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setQuitContextDefaults() {
	quitCtx.drainWait = 10 * time.Minute
	quitCtx.nodeDrainSelf = false
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
var demoCtx = democluster.Context{
	CliCtx: &cliCtx.Context,
}

// setDemoContextDefaults set the default values in demoCtx.  This
// function is called by initCLIDefaults() and thus re-called in every
// test that exercises command-line parsing.
func setDemoContextDefaults() {
	demoCtx.NumNodes = 1
	demoCtx.SQLPoolMemorySize = 128 << 20 // 128MB, chosen to fit 9 nodes on 2GB machine.
	demoCtx.CacheSize = 64 << 20          // 64MB, chosen to fit 9 nodes on 2GB machine.
	demoCtx.NoExampleDatabase = false
	demoCtx.SimulateLatency = false
	demoCtx.RunWorkload = false
	demoCtx.Localities = nil
	demoCtx.GeoPartitionedReplicas = false
	demoCtx.DisableTelemetry = false
	demoCtx.DisableLicenseAcquisition = false
	demoCtx.DefaultKeySize = defaultKeySize
	demoCtx.DefaultCALifetime = defaultCALifetime
	demoCtx.DefaultCertLifetime = defaultCertLifetime
	demoCtx.Insecure = false
	demoCtx.SQLPort, _ = strconv.Atoi(base.DefaultPort)
	demoCtx.HTTPPort, _ = strconv.Atoi(base.DefaultHTTPPort)
	demoCtx.WorkloadMaxQPS = 25
	demoCtx.Multitenant = true
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
	proxyContext.ThrottleBaseDelay = time.Second
}

var testDirectorySvrContext struct {
	port          int
	certsDir      string
	kvAddrs       string
	tenantBaseDir string
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
