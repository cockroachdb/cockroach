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
	"flag"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// special global variables used by flag variable definitions below.
// These do not correspond directly to the configuration parameters
// used as input by the CLI commands (these are defined in context
// structs in context.go). Instead, they are used at the *end* of
// command-line parsing to override the defaults in the context
// structs.
//
// Corollaries:
// - it would be a programming error to access these variables directly
//   outside of this file (flags.go)
// - the underlying context parameters must receive defaults in
//   initCLIDefaults() even when they are otherwise overridden by the
//   flags logic, because some tests to not use the flag logic at all.
var serverListenPort, serverAdvertiseAddr, serverAdvertisePort string
var serverHTTPAddr, serverHTTPPort string
var localityAdvertiseHosts localityList

// initPreFlagsDefaults initializes the values of the global variables
// defined above.
func initPreFlagsDefaults() {
	serverListenPort = base.DefaultPort
	serverAdvertiseAddr = ""
	serverAdvertisePort = ""

	serverHTTPAddr = ""
	serverHTTPPort = base.DefaultHTTPPort

	localityAdvertiseHosts = localityList{}
}

// AddPersistentPreRunE add 'fn' as a persistent pre-run function to 'cmd'.
// If the command has an existing pre-run function, it is saved and will be called
// at the beginning of 'fn'.
// This allows an arbitrary number of pre-run functions with ordering based
// on the order in which AddPersistentPreRunE is called (usually package init order).
func AddPersistentPreRunE(cmd *cobra.Command, fn func(*cobra.Command, []string) error) {
	// Save any existing hooks.
	wrapped := cmd.PersistentPreRunE

	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Run the previous hook if it exists.
		if wrapped != nil {
			if err := wrapped(cmd, args); err != nil {
				return err
			}
		}

		// Now we can call the new function.
		return fn(cmd, args)
	}
}

func setFlagFromEnv(f *pflag.FlagSet, flagInfo cliflags.FlagInfo) {
	if flagInfo.EnvVar != "" {
		if value, set := envutil.EnvString(flagInfo.EnvVar, 2); set {
			if err := f.Set(flagInfo.Name, value); err != nil {
				panic(err)
			}
		}
	}
}

// StringFlag creates a string flag and registers it with the FlagSet.
func StringFlag(f *pflag.FlagSet, valPtr *string, flagInfo cliflags.FlagInfo, defaultVal string) {
	f.StringVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	setFlagFromEnv(f, flagInfo)
}

// IntFlag creates an int flag and registers it with the FlagSet.
func IntFlag(f *pflag.FlagSet, valPtr *int, flagInfo cliflags.FlagInfo, defaultVal int) {
	f.IntVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	setFlagFromEnv(f, flagInfo)
}

// BoolFlag creates a bool flag and registers it with the FlagSet.
func BoolFlag(f *pflag.FlagSet, valPtr *bool, flagInfo cliflags.FlagInfo, defaultVal bool) {
	f.BoolVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	setFlagFromEnv(f, flagInfo)
}

// DurationFlag creates a duration flag and registers it with the FlagSet.
func DurationFlag(
	f *pflag.FlagSet, valPtr *time.Duration, flagInfo cliflags.FlagInfo, defaultVal time.Duration,
) {
	f.DurationVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	setFlagFromEnv(f, flagInfo)
}

// VarFlag creates a custom-variable flag and registers it with the FlagSet.
func VarFlag(f *pflag.FlagSet, value pflag.Value, flagInfo cliflags.FlagInfo) {
	f.VarP(value, flagInfo.Name, flagInfo.Shorthand, flagInfo.Usage())

	setFlagFromEnv(f, flagInfo)
}

// StringSlice creates a string slice flag and registers it with the FlagSet.
func StringSlice(
	f *pflag.FlagSet, valPtr *[]string, flagInfo cliflags.FlagInfo, defaultVal []string,
) {
	f.StringSliceVar(valPtr, flagInfo.Name, defaultVal, flagInfo.Usage())
	setFlagFromEnv(f, flagInfo)
}

// aliasStrVar wraps a string configuration option and is meant
// to be used in addition to / next to another flag that targets the
// same option. It does not implement "default values" so that the
// main flag can perform the default logic.
type aliasStrVar struct{ p *string }

// String implements the pflag.Value interface.
func (a aliasStrVar) String() string { return "" }

// Set implements the pflag.Value interface.
func (a aliasStrVar) Set(v string) error {
	if v != "" {
		*a.p = v
	}
	return nil
}

// Type implements the pflag.Value interface.
func (a aliasStrVar) Type() string { return "string" }

// addrSetter wraps a address/port configuration option pair and
// enables setting them both with a single command-line flag.
type addrSetter struct {
	addr *string
	port *string
}

// String implements the pflag.Value interface.
func (a addrSetter) String() string {
	return net.JoinHostPort(*a.addr, *a.port)
}

// Type implements the pflag.Value interface
func (a addrSetter) Type() string { return "<addr/host>[:<port>]" }

// Set implement the pflag.Value interface.
func (a addrSetter) Set(v string) error {
	addr, port, err := netutil.SplitHostPort(v, *a.port)
	if err != nil {
		return err
	}
	*a.addr = addr
	*a.port = port
	return nil
}

const backgroundEnvVar = "COCKROACH_BACKGROUND_RESTART"

func init() {
	initCLIDefaults()

	// Every command but start will inherit the following setting.
	cockroachCmd.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		extraClientFlagInit()
		return setDefaultStderrVerbosity(cmd, log.Severity_WARNING)
	}

	// Add a pre-run command for `start`.
	AddPersistentPreRunE(StartCmd, func(cmd *cobra.Command, _ []string) error {
		extraServerFlagInit()
		return setDefaultStderrVerbosity(cmd, log.Severity_INFO)
	})

	// Map any flags registered in the standard "flag" package into the
	// top-level cockroach command.
	pf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		flag := pflag.PFlagFromGoFlag(f)
		// TODO(peter): Decide if we want to make the lightstep flags visible.
		if strings.HasPrefix(flag.Name, "lightstep_") {
			flag.Hidden = true
		}
		if strings.HasPrefix(flag.Name, "httptest.") {
			// If we test the cli commands in tests, we may end up transitively
			// importing httptest, for example via `testify/assert`. Make sure
			// it doesn't show up in the output or it will confuse tests.
			flag.Hidden = true
		}
		switch flag.Name {
		case logflags.NoRedirectStderrName:
			flag.Hidden = true
		case logflags.ShowLogsName:
			flag.Hidden = true
		case logflags.LogToStderrName:
			// The actual default value for --logtostderr is overridden in
			// cli.Main. We don't override it here as doing so would affect all of
			// the cli tests and any package which depends on cli. The following line
			// is only overriding the default value for the pflag package (and what
			// is visible in help text), not the stdlib flag value.
			flag.DefValue = "NONE"
		case logflags.LogDirName,
			logflags.LogFileMaxSizeName,
			logflags.LogFilesCombinedMaxSizeName,
			logflags.LogFileVerbosityThresholdName:
			// The --log-dir* and --log-file* flags are specified only for the
			// `start` and `demo` commands.
			return
		}
		pf.AddFlag(flag)
	})

	// When a flag is specified but without a value, pflag assigns its
	// NoOptDefVal to it via Set(). This is also the value used to
	// generate the implicit assigned value in the usage text
	// (e.g. "--logtostderr[=XXXXX]"). We can't populate a real default
	// unfortunately, because the default depends on which command is
	// run (`start` vs. the rest), and pflag does not support
	// per-command NoOptDefVals. So we need some sentinel value here
	// that we can recognize when setDefaultStderrVerbosity() is called
	// after argument parsing. We could use UNKNOWN, but to ensure that
	// the usage text is somewhat less confusing to the user, we use the
	// special severity value DEFAULT instead.
	pf.Lookup(logflags.LogToStderrName).NoOptDefVal = log.Severity_DEFAULT.String()

	// Remember we are starting in the background as the `start` command will
	// avoid printing some messages to standard output in that case.
	_, startCtx.inBackground = envutil.EnvString(backgroundEnvVar, 1)

	{
		f := StartCmd.Flags()

		// Server flags.
		VarFlag(f, addrSetter{&startCtx.serverListenAddr, &serverListenPort}, cliflags.ListenAddr)
		VarFlag(f, addrSetter{&serverAdvertiseAddr, &serverAdvertisePort}, cliflags.AdvertiseAddr)
		VarFlag(f, addrSetter{&serverHTTPAddr, &serverHTTPPort}, cliflags.ListenHTTPAddr)

		// Backward-compatibility flags.

		// These are deprecated but until we have qualitatively new
		// functionality in the flags above, there is no need to nudge the
		// user away from them with a deprecation warning. So we keep
		// them, but hidden from docs so that they don't appear as
		// redundant with the main flags.
		VarFlag(f, aliasStrVar{&startCtx.serverListenAddr}, cliflags.ServerHost)
		_ = f.MarkHidden(cliflags.ServerHost.Name)
		VarFlag(f, aliasStrVar{&serverListenPort}, cliflags.ServerPort)
		_ = f.MarkHidden(cliflags.ServerPort.Name)

		VarFlag(f, aliasStrVar{&serverAdvertiseAddr}, cliflags.AdvertiseHost)
		_ = f.MarkHidden(cliflags.AdvertiseHost.Name)
		VarFlag(f, aliasStrVar{&serverAdvertisePort}, cliflags.AdvertisePort)
		_ = f.MarkHidden(cliflags.AdvertisePort.Name)

		VarFlag(f, aliasStrVar{&serverHTTPAddr}, cliflags.ListenHTTPAddrAlias)
		_ = f.MarkHidden(cliflags.ListenHTTPAddrAlias.Name)
		VarFlag(f, aliasStrVar{&serverHTTPPort}, cliflags.ListenHTTPPort)
		_ = f.MarkHidden(cliflags.ListenHTTPPort.Name)

		// More server flags.

		VarFlag(f, &localityAdvertiseHosts, cliflags.LocalityAdvertiseAddr)

		StringFlag(f, &serverCfg.Attrs, cliflags.Attrs, serverCfg.Attrs)
		VarFlag(f, &serverCfg.Locality, cliflags.Locality)

		VarFlag(f, &serverCfg.Stores, cliflags.Store)
		VarFlag(f, &serverCfg.MaxOffset, cliflags.MaxOffset)

		// Usage for the unix socket is odd as we use a real file, whereas
		// postgresql and clients consider it a directory and build a filename
		// inside it using the port.
		// Thus, we keep it hidden and use it for testing only.
		StringFlag(f, &serverCfg.SocketFile, cliflags.Socket, serverCfg.SocketFile)
		_ = f.MarkHidden(cliflags.Socket.Name)

		StringFlag(f, &startCtx.listeningURLFile, cliflags.ListeningURLFile, startCtx.listeningURLFile)

		StringFlag(f, &startCtx.pidFile, cliflags.PIDFile, startCtx.pidFile)

		// Use a separate variable to store the value of ServerInsecure.
		// We share the default with the ClientInsecure flag.
		BoolFlag(f, &startCtx.serverInsecure, cliflags.ServerInsecure, startCtx.serverInsecure)

		// Certificates directory. Use a server-specific flag and value to ignore environment
		// variables, but share the same default.
		StringFlag(f, &startCtx.serverSSLCertsDir, cliflags.ServerCertsDir, startCtx.serverSSLCertsDir)

		// Cluster joining flags.
		VarFlag(f, &serverCfg.JoinList, cliflags.Join)

		// Engine flags.
		VarFlag(f, cacheSizeValue, cliflags.Cache)
		VarFlag(f, sqlSizeValue, cliflags.SQLMem)
		// N.B. diskTempStorageSizeValue.ResolvePercentage() will be called after
		// the stores flag has been parsed and the storage device that a percentage
		// refers to becomes known.
		VarFlag(f, diskTempStorageSizeValue, cliflags.SQLTempStorage)
		StringFlag(f, &startCtx.tempDir, cliflags.TempDir, startCtx.tempDir)
		StringFlag(f, &startCtx.externalIODir, cliflags.ExternalIODir, startCtx.externalIODir)

		VarFlag(f, serverCfg.SQLAuditLogDirName, cliflags.SQLAuditLogDirName)
	}

	// Log flags.
	for _, cmd := range []*cobra.Command{demoCmd, StartCmd} {
		f := cmd.Flags()
		VarFlag(f, &startCtx.logDir, cliflags.LogDir)
		startCtx.logDirFlag = f.Lookup(cliflags.LogDir.Name)
		VarFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFilesCombinedMaxSizeName)).Value,
			cliflags.LogDirMaxSize)
		VarFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFileMaxSizeName)).Value,
			cliflags.LogFileMaxSize)
		VarFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFileVerbosityThresholdName)).Value,
			cliflags.LogFileVerbosity)
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		// All certs commands need the certificate directory.
		StringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir, baseCfg.SSLCertsDir)
	}

	for _, cmd := range []*cobra.Command{createCACertCmd, createClientCACertCmd} {
		f := cmd.Flags()
		// CA certificates have a longer expiration time.
		DurationFlag(f, &caCertificateLifetime, cliflags.CertificateLifetime, defaultCALifetime)
		// The CA key can be re-used if it exists.
		BoolFlag(f, &allowCAKeyReuse, cliflags.AllowCAKeyReuse, false)
	}

	for _, cmd := range []*cobra.Command{createNodeCertCmd, createClientCertCmd} {
		f := cmd.Flags()
		DurationFlag(f, &certificateLifetime, cliflags.CertificateLifetime, defaultCertLifetime)
	}

	// The remaining flags are shared between all cert-generating functions.
	for _, cmd := range []*cobra.Command{createCACertCmd, createClientCACertCmd, createNodeCertCmd, createClientCertCmd} {
		f := cmd.Flags()
		StringFlag(f, &baseCfg.SSLCAKey, cliflags.CAKey, baseCfg.SSLCAKey)
		IntFlag(f, &keySize, cliflags.KeySize, defaultKeySize)
		BoolFlag(f, &overwriteFiles, cliflags.OverwriteFiles, false)
	}
	// PKCS8 key format is only available for the client cert command.
	BoolFlag(createClientCertCmd.Flags(), &generatePKCS8Key, cliflags.GeneratePKCS8Key, false)

	BoolFlag(setUserCmd.Flags(), &password, cliflags.Password, false)

	clientCmds := []*cobra.Command{
		debugGossipValuesCmd,
		debugTimeSeriesDumpCmd,
		debugZipCmd,
		dumpCmd,
		genHAProxyCmd,
		quitCmd,
		sqlShellCmd,
		/* StartCmd is covered above */
	}
	clientCmds = append(clientCmds, userCmds...)
	clientCmds = append(clientCmds, zoneCmds...)
	clientCmds = append(clientCmds, nodeCmds...)
	clientCmds = append(clientCmds, systemBenchCmds...)
	clientCmds = append(clientCmds, initCmd)
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		VarFlag(f, addrSetter{&cliCtx.clientConnHost, &cliCtx.clientConnPort}, cliflags.ClientHost)
		StringFlag(f, &cliCtx.clientConnPort, cliflags.ClientPort, cliCtx.clientConnPort)
		_ = f.MarkHidden(cliflags.ClientPort.Name)

		BoolFlag(f, &baseCfg.Insecure, cliflags.ClientInsecure, baseCfg.Insecure)

		// Certificate flags.
		StringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir, baseCfg.SSLCertsDir)
	}

	timeoutCmds := []*cobra.Command{
		statusNodeCmd,
		lsNodesCmd,
		debugZipCmd,
		// If you add something here, make sure the actual implementation
		// of the command uses `cmdTimeoutContext(.)` or it will ignore
		// the timeout.
	}

	for _, cmd := range timeoutCmds {
		DurationFlag(cmd.Flags(), &cliCtx.cmdTimeout, cliflags.Timeout, cliCtx.cmdTimeout)
	}

	// Node Status command.
	{
		f := statusNodeCmd.Flags()
		BoolFlag(f, &nodeCtx.statusShowRanges, cliflags.NodeRanges, nodeCtx.statusShowRanges)
		BoolFlag(f, &nodeCtx.statusShowStats, cliflags.NodeStats, nodeCtx.statusShowStats)
		BoolFlag(f, &nodeCtx.statusShowAll, cliflags.NodeAll, nodeCtx.statusShowAll)
		BoolFlag(f, &nodeCtx.statusShowDecommission, cliflags.NodeDecommission, nodeCtx.statusShowDecommission)
	}

	// HDD Bench command.
	{
		f := seqWriteBench.Flags()
		VarFlag(f, humanizeutil.NewBytesValue(&systemBenchCtx.writeSize), cliflags.WriteSize)
		VarFlag(f, humanizeutil.NewBytesValue(&systemBenchCtx.syncInterval), cliflags.SyncInterval)
	}

	// Network Bench command.
	{
		f := networkBench.Flags()
		BoolFlag(f, &networkBenchCtx.server, cliflags.BenchServer, networkBenchCtx.server)
		IntFlag(f, &networkBenchCtx.port, cliflags.BenchPort, networkBenchCtx.port)
		StringSlice(f, &networkBenchCtx.addresses, cliflags.BenchAddresses, networkBenchCtx.addresses)
		BoolFlag(f, &networkBenchCtx.latency, cliflags.BenchLatency, networkBenchCtx.latency)
	}

	// Bench command.
	{
		for _, cmd := range systemBenchCmds {
			f := cmd.Flags()
			IntFlag(f, &systemBenchCtx.concurrency, cliflags.BenchConcurrency, systemBenchCtx.concurrency)
			DurationFlag(f, &systemBenchCtx.duration, cliflags.BenchDuration, systemBenchCtx.duration)
			StringFlag(f, &systemBenchCtx.tempDir, cliflags.TempDir, systemBenchCtx.tempDir)
		}
	}

	// Decommission command.
	VarFlag(decommissionNodeCmd.Flags(), &nodeCtx.nodeDecommissionWait, cliflags.Wait)

	// Quit command.
	BoolFlag(quitCmd.Flags(), &quitCtx.serverDecommission, cliflags.Decommission, quitCtx.serverDecommission)

	zf := setZoneCmd.Flags()
	StringFlag(zf, &zoneCtx.zoneConfig, cliflags.ZoneConfig, zoneCtx.zoneConfig)
	BoolFlag(zf, &zoneCtx.zoneDisableReplication, cliflags.ZoneDisableReplication, zoneCtx.zoneDisableReplication)

	for _, cmd := range append([]*cobra.Command{sqlShellCmd, demoCmd}, demoCmd.Commands()...) {
		f := cmd.Flags()
		VarFlag(f, &sqlCtx.setStmts, cliflags.Set)
		VarFlag(f, &sqlCtx.execStmts, cliflags.Execute)
		BoolFlag(f, &sqlCtx.safeUpdates, cliflags.SafeUpdates, sqlCtx.safeUpdates)
		BoolFlag(f, &sqlCtx.debugMode, cliflags.CliDebugMode, sqlCtx.debugMode)
	}

	VarFlag(dumpCmd.Flags(), &dumpCtx.dumpMode, cliflags.DumpMode)
	StringFlag(dumpCmd.Flags(), &dumpCtx.asOf, cliflags.DumpTime, dumpCtx.asOf)

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, dumpCmd, demoCmd}
	sqlCmds = append(sqlCmds, zoneCmds...)
	sqlCmds = append(sqlCmds, userCmds...)
	for _, cmd := range sqlCmds {
		f := cmd.Flags()
		BoolFlag(f, &sqlCtx.echo, cliflags.EchoSQL, sqlCtx.echo)

		if cmd != demoCmd {
			VarFlag(f, urlParser{cmd, &cliCtx, false /* strictSSL */}, cliflags.URL)
			StringFlag(f, &cliCtx.sqlConnUser, cliflags.User, cliCtx.sqlConnUser)
		}

		if cmd == sqlShellCmd {
			StringFlag(f, &cliCtx.sqlConnDBName, cliflags.Database, cliCtx.sqlConnDBName)
		}
	}

	// Make the other non-SQL client commands also recognize --url in
	// strict SSL mode.
	for _, cmd := range clientCmds {
		if f := cmd.Flags().Lookup(cliflags.URL.Name); f != nil {
			// --url already registered above, nothing to do.
			continue
		}
		VarFlag(cmd.PersistentFlags(), urlParser{cmd, &cliCtx, true /* strictSSL */}, cliflags.URL)
	}

	// Commands that print tables.
	tableOutputCommands := append([]*cobra.Command{sqlShellCmd, genSettingsListCmd, demoCmd},
		demoCmd.Commands()...)
	tableOutputCommands = append(tableOutputCommands, userCmds...)
	tableOutputCommands = append(tableOutputCommands, nodeCmds...)

	// By default, these commands print their output as pretty-formatted
	// tables on terminals, and TSV when redirected to a file. The user
	// can override with --format.
	// By default, query times are not displayed. The default is overridden
	// in the CLI shell.
	for _, cmd := range tableOutputCommands {
		f := cmd.PersistentFlags()
		VarFlag(f, &cliCtx.tableDisplayFormat, cliflags.TableDisplayFormat)
	}

	// demo command.
	demoFlags := demoCmd.PersistentFlags()
	// We add this command as a persistent flag so you can do stuff like
	// ./cockroach demo movr --nodes=3.
	IntFlag(demoFlags, &demoCtx.nodes, cliflags.DemoNodes, 1)

	// sqlfmt command.
	fmtFlags := sqlfmtCmd.Flags()
	VarFlag(fmtFlags, &sqlfmtCtx.execStmts, cliflags.Execute)
	cfg := tree.DefaultPrettyCfg()
	IntFlag(fmtFlags, &sqlfmtCtx.len, cliflags.SQLFmtLen, cfg.LineWidth)
	BoolFlag(fmtFlags, &sqlfmtCtx.useSpaces, cliflags.SQLFmtSpaces, !cfg.UseTabs)
	IntFlag(fmtFlags, &sqlfmtCtx.tabWidth, cliflags.SQLFmtTabWidth, cfg.TabWidth)
	BoolFlag(fmtFlags, &sqlfmtCtx.noSimplify, cliflags.SQLFmtNoSimplify, !cfg.Simplify)
	BoolFlag(fmtFlags, &sqlfmtCtx.align, cliflags.SQLFmtAlign, (cfg.Align != tree.PrettyNoAlign))

	// Debug commands.
	{
		f := debugKeysCmd.Flags()
		VarFlag(f, (*mvccKey)(&debugCtx.startKey), cliflags.From)
		VarFlag(f, (*mvccKey)(&debugCtx.endKey), cliflags.To)
		BoolFlag(f, &debugCtx.values, cliflags.Values, debugCtx.values)
		BoolFlag(f, &debugCtx.sizes, cliflags.Sizes, debugCtx.sizes)
	}
	{
		f := debugRangeDataCmd.Flags()
		BoolFlag(f, &debugCtx.replicated, cliflags.Replicated, debugCtx.replicated)
	}
	{
		f := debugGossipValuesCmd.Flags()
		StringFlag(f, &debugCtx.inputFile, cliflags.GossipInputFile, debugCtx.inputFile)
		BoolFlag(f, &debugCtx.printSystemConfig, cliflags.PrintSystemConfig, debugCtx.printSystemConfig)
	}
	{
		f := debugBallastCmd.Flags()
		VarFlag(f, &debugCtx.ballastSize, cliflags.Size)
	}
}

func extraServerFlagInit() {
	serverCfg.Addr = net.JoinHostPort(startCtx.serverListenAddr, serverListenPort)
	if serverAdvertiseAddr == "" {
		serverAdvertiseAddr = startCtx.serverListenAddr
	}
	if serverAdvertisePort == "" {
		serverAdvertisePort = serverListenPort
	}
	serverCfg.AdvertiseAddr = net.JoinHostPort(serverAdvertiseAddr, serverAdvertisePort)
	if serverHTTPAddr == "" {
		serverHTTPAddr = startCtx.serverListenAddr
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPAddr, serverHTTPPort)
	for i, addr := range localityAdvertiseHosts {
		if !strings.Contains(localityAdvertiseHosts[i].Address.AddressField, ":") {
			localityAdvertiseHosts[i].Address.AddressField = net.JoinHostPort(addr.Address.AddressField, serverAdvertisePort)
		}
	}
	serverCfg.LocalityAddresses = localityAdvertiseHosts
}

func extraClientFlagInit() {
	serverCfg.Addr = net.JoinHostPort(cliCtx.clientConnHost, cliCtx.clientConnPort)
	serverCfg.AdvertiseAddr = serverCfg.Addr
	if serverHTTPAddr == "" {
		serverHTTPAddr = startCtx.serverListenAddr
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPAddr, serverHTTPPort)

	// If CLI/SQL debug mode is requested, override the echo mode here,
	// so that the initial client/server handshake reveals the SQL being
	// sent.
	if sqlCtx.debugMode {
		sqlCtx.echo = true
	}
}

func setDefaultStderrVerbosity(cmd *cobra.Command, defaultSeverity log.Severity) error {
	pf := cmd.Flags()

	vf := pf.Lookup(logflags.LogToStderrName)

	// if `--logtostderr` was not specified and no log directory was
	// set, or `--logtostderr` was specified but without explicit level,
	// then set stderr logging to the level considered default by the
	// specific command.
	if (!vf.Changed && !log.DirSet()) ||
		(vf.Changed && vf.Value.String() == log.Severity_DEFAULT.String()) {
		if err := vf.Value.Set(defaultSeverity.String()); err != nil {
			return err
		}
	}

	return nil
}
