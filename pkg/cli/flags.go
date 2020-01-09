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
	"regexp"
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
	"github.com/cockroachdb/errors"
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
var serverListenPort string
var serverAdvertiseAddr, serverAdvertisePort string
var serverSQLAddr, serverSQLPort string
var serverSQLAdvertiseAddr, serverSQLAdvertisePort string
var serverHTTPAddr, serverHTTPPort string
var localityAdvertiseHosts localityList

// initPreFlagsDefaults initializes the values of the global variables
// defined above.
func initPreFlagsDefaults() {
	serverListenPort = base.DefaultPort
	serverAdvertiseAddr = ""
	serverAdvertisePort = ""

	serverSQLAddr = ""
	serverSQLPort = ""
	serverSQLAdvertiseAddr = ""
	serverSQLAdvertisePort = ""

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

// StringFlag creates a string flag and registers it with the FlagSet.
func StringFlag(f *pflag.FlagSet, valPtr *string, flagInfo cliflags.FlagInfo, defaultVal string) {
	f.StringVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// IntFlag creates an int flag and registers it with the FlagSet.
func IntFlag(f *pflag.FlagSet, valPtr *int, flagInfo cliflags.FlagInfo, defaultVal int) {
	f.IntVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// BoolFlag creates a bool flag and registers it with the FlagSet.
func BoolFlag(f *pflag.FlagSet, valPtr *bool, flagInfo cliflags.FlagInfo, defaultVal bool) {
	f.BoolVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// DurationFlag creates a duration flag and registers it with the FlagSet.
func DurationFlag(
	f *pflag.FlagSet, valPtr *time.Duration, flagInfo cliflags.FlagInfo, defaultVal time.Duration,
) {
	f.DurationVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// VarFlag creates a custom-variable flag and registers it with the FlagSet.
func VarFlag(f *pflag.FlagSet, value pflag.Value, flagInfo cliflags.FlagInfo) {
	f.VarP(value, flagInfo.Name, flagInfo.Shorthand, flagInfo.Usage())

	registerEnvVarDefault(f, flagInfo)
}

// StringSlice creates a string slice flag and registers it with the FlagSet.
func StringSlice(
	f *pflag.FlagSet, valPtr *[]string, flagInfo cliflags.FlagInfo, defaultVal []string,
) {
	f.StringSliceVar(valPtr, flagInfo.Name, defaultVal, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo)
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

// Type implements the pflag.Value interface.
func (a addrSetter) Type() string { return "<addr/host>[:<port>]" }

// Set implements the pflag.Value interface.
func (a addrSetter) Set(v string) error {
	addr, port, err := netutil.SplitHostPort(v, *a.port)
	if err != nil {
		return err
	}
	*a.addr = addr
	*a.port = port
	return nil
}

// clusterNameSetter wraps the cluster name variable
// and verifies its format during configuration.
type clusterNameSetter struct {
	clusterName *string
}

// String implements the pflag.Value interface.
func (a clusterNameSetter) String() string { return *a.clusterName }

// Type implements the pflag.Value interface.
func (a clusterNameSetter) Type() string { return "<identifier>" }

// Set implements the pflag.Value interface.
func (a clusterNameSetter) Set(v string) error {
	if v == "" {
		return errors.New("cluster name cannot be empty")
	}
	if len(v) > maxClusterNameLength {
		return errors.Newf(`cluster name can contain at most %d characters`, maxClusterNameLength)
	}
	if !clusterNameRe.MatchString(v) {
		return errClusterNameInvalidFormat
	}
	*a.clusterName = v
	return nil
}

var errClusterNameInvalidFormat = errors.New(`cluster name must contain only letters, numbers or the "-" and "." characters`)

// clusterNameRe matches valid cluster names.
// For example, "a", "a123" and "a-b" are OK,
// but "0123", "a-" and "123a" are not OK.
var clusterNameRe = regexp.MustCompile(`^[a-zA-Z](?:[-a-zA-Z0-9]*[a-zA-Z0-9]|)$`)

const maxClusterNameLength = 256

const backgroundEnvVar = "COCKROACH_BACKGROUND_RESTART"

// flagSetForCmd is a replacement for cmd.Flag() that properly merges
// persistent and local flags, until the upstream bug
// https://github.com/spf13/cobra/issues/961 has been fixed.
func flagSetForCmd(cmd *cobra.Command) *pflag.FlagSet {
	_ = cmd.LocalFlags() // force merge persistent+local flags
	return cmd.Flags()
}

func init() {
	initCLIDefaults()
	defer func() {
		if err := processEnvVarDefaults(); err != nil {
			panic(err)
		}
	}()

	// Every command but start will inherit the following setting.
	AddPersistentPreRunE(cockroachCmd, func(cmd *cobra.Command, _ []string) error {
		extraClientFlagInit()
		return setDefaultStderrVerbosity(cmd, log.Severity_WARNING)
	})

	// Add a pre-run command for `start` and `start-single-node`.
	for _, cmd := range StartCmds {
		AddPersistentPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
			// Finalize the configuration of network and logging settings.
			if err := extraServerFlagInit(cmd); err != nil {
				return err
			}
			return setDefaultStderrVerbosity(cmd, log.Severity_INFO)
		})
	}

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

	for _, cmd := range StartCmds {
		f := cmd.Flags()

		// Server flags.
		VarFlag(f, addrSetter{&startCtx.serverListenAddr, &serverListenPort}, cliflags.ListenAddr)
		VarFlag(f, addrSetter{&serverAdvertiseAddr, &serverAdvertisePort}, cliflags.AdvertiseAddr)
		VarFlag(f, addrSetter{&serverSQLAddr, &serverSQLPort}, cliflags.ListenSQLAddr)
		VarFlag(f, addrSetter{&serverSQLAdvertiseAddr, &serverSQLAdvertisePort}, cliflags.SQLAdvertiseAddr)
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
		VarFlag(f, &serverCfg.StorageEngine, cliflags.StorageEngine)
		VarFlag(f, &serverCfg.MaxOffset, cliflags.MaxOffset)

		StringFlag(f, &serverCfg.SocketFile, cliflags.Socket, serverCfg.SocketFile)

		StringFlag(f, &startCtx.listeningURLFile, cliflags.ListeningURLFile, startCtx.listeningURLFile)

		StringFlag(f, &startCtx.pidFile, cliflags.PIDFile, startCtx.pidFile)

		// Use a separate variable to store the value of ServerInsecure.
		// We share the default with the ClientInsecure flag.
		BoolFlag(f, &startCtx.serverInsecure, cliflags.ServerInsecure, startCtx.serverInsecure)

		// Certificates directory. Use a server-specific flag and value to ignore environment
		// variables, but share the same default.
		StringFlag(f, &startCtx.serverSSLCertsDir, cliflags.ServerCertsDir, startCtx.serverSSLCertsDir)

		// Cluster joining flags. We need to enable this both for 'start'
		// and 'start-single-node' although the latter does not support
		// --join, because it delegates its logic to that of 'start', and
		// 'start' will check that the flag is properly defined.
		VarFlag(f, &serverCfg.JoinList, cliflags.Join)
		VarFlag(f, clusterNameSetter{&baseCfg.ClusterName}, cliflags.ClusterName)
		BoolFlag(f, &baseCfg.DisableClusterNameVerification, cliflags.DisableClusterNameVerification, false)
		if cmd == startSingleNodeCmd {
			// Even though all server flags are supported for
			// 'start-single-node', we intend that command to be used by
			// beginners / developers running on a single machine. To
			// enhance the UX, we hide the flags since they are not directly
			// relevant when running a single node.
			_ = f.MarkHidden(cliflags.Join.Name)
			_ = f.MarkHidden(cliflags.ClusterName.Name)
			_ = f.MarkHidden(cliflags.DisableClusterNameVerification.Name)
			_ = f.MarkHidden(cliflags.MaxOffset.Name)
			_ = f.MarkHidden(cliflags.LocalityAdvertiseAddr.Name)
			_ = f.MarkHidden(cliflags.AdvertiseAddr.Name)
			_ = f.MarkHidden(cliflags.SQLAdvertiseAddr.Name)
		}

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
	logCmds := append(StartCmds, demoCmd)
	logCmds = append(logCmds, demoCmd.Commands()...)
	for _, cmd := range logCmds {
		f := cmd.Flags()
		VarFlag(f, &startCtx.logDir, cliflags.LogDir)
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
		/* StartCmds are covered above */
	}
	clientCmds = append(clientCmds, userCmds...)
	clientCmds = append(clientCmds, nodeCmds...)
	clientCmds = append(clientCmds, systemBenchCmds...)
	clientCmds = append(clientCmds, initCmd)
	clientCmds = append(clientCmds, nodeLocalCmds...)
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

	for _, cmd := range append([]*cobra.Command{sqlShellCmd, demoCmd}, demoCmd.Commands()...) {
		f := cmd.Flags()
		VarFlag(f, &sqlCtx.setStmts, cliflags.Set)
		VarFlag(f, &sqlCtx.execStmts, cliflags.Execute)
		DurationFlag(f, &sqlCtx.repeatDelay, cliflags.Watch, sqlCtx.repeatDelay)
		BoolFlag(f, &sqlCtx.safeUpdates, cliflags.SafeUpdates, sqlCtx.safeUpdates)
		BoolFlag(f, &sqlCtx.debugMode, cliflags.CliDebugMode, sqlCtx.debugMode)
	}

	VarFlag(dumpCmd.Flags(), &dumpCtx.dumpMode, cliflags.DumpMode)
	StringFlag(dumpCmd.Flags(), &dumpCtx.asOf, cliflags.DumpTime, dumpCtx.asOf)

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, dumpCmd, demoCmd}
	sqlCmds = append(sqlCmds, userCmds...)
	sqlCmds = append(sqlCmds, demoCmd.Commands()...)
	sqlCmds = append(sqlCmds, nodeLocalCmds...)
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
		if f := flagSetForCmd(cmd).Lookup(cliflags.URL.Name); f != nil {
			// --url already registered above, nothing to do.
			continue
		}
		VarFlag(cmd.PersistentFlags(), urlParser{cmd, &cliCtx, true /* strictSSL */}, cliflags.URL)
	}

	// Commands that print tables.
	tableOutputCommands := append(
		[]*cobra.Command{sqlShellCmd, genSettingsListCmd, demoCmd},
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
	BoolFlag(demoFlags, &demoCtx.runWorkload, cliflags.RunDemoWorkload, false)
	VarFlag(demoFlags, &demoCtx.localities, cliflags.DemoNodeLocality)
	BoolFlag(demoFlags, &demoCtx.geoPartitionedReplicas, cliflags.DemoGeoPartitionedReplicas, false)
	// Mark the --global flag as hidden until we investigate it more.
	BoolFlag(demoFlags, &demoCtx.simulateLatency, cliflags.Global, false)
	_ = demoFlags.MarkHidden(cliflags.Global.Name)
	// The --empty flag is only valid for the top level demo command,
	// so we use the regular flag set.
	BoolFlag(demoCmd.Flags(), &demoCtx.useEmptyDatabase, cliflags.UseEmptyDatabase, false)

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

// processEnvVarDefaults injects the current value of flag-related
// environment variables into the initial value of the settings linked
// to the flags, during initialization and before the command line is
// actually parsed. For example, it will inject the value of
// $COCKROACH_URL into the urlParser object linked to the --url flag.
func processEnvVarDefaults() error {
	for envVar, d := range envVarDefaults {
		if err := d.flagSet.Set(d.flagName, d.envValue); err != nil {
			return errors.Wrapf(err, "setting --%s from %s", d.flagName, envVar)
		}
	}
	return nil
}

// envVarDefault describes a delayed default initialization of the
// setting covered by a flag from the value of an environment
// variable.
type envVarDefault struct {
	envValue string
	flagName string
	flagSet  *pflag.FlagSet
}

// envVarDefaults records the initializations from environment variables
// for processing at the end of initialization, before flag parsing.
var envVarDefaults = map[string]envVarDefault{}

// registerEnvVarDefault registers a deferred initialization of a flag
// from an environment variable.
func registerEnvVarDefault(f *pflag.FlagSet, flagInfo cliflags.FlagInfo) {
	if flagInfo.EnvVar == "" {
		return
	}
	value, set := envutil.EnvString(flagInfo.EnvVar, 2)
	if !set {
		// Env var not set. Nothing to do.
		return
	}
	envVarDefaults[flagInfo.EnvVar] = envVarDefault{
		envValue: value,
		flagName: flagInfo.Name,
		flagSet:  f,
	}
}

func extraServerFlagInit(cmd *cobra.Command) error {
	// Construct the main RPC listen address.
	serverCfg.Addr = net.JoinHostPort(startCtx.serverListenAddr, serverListenPort)

	// Fill in the defaults for --advertise-addr.
	if serverAdvertiseAddr == "" {
		serverAdvertiseAddr = startCtx.serverListenAddr
	}
	if serverAdvertisePort == "" {
		serverAdvertisePort = serverListenPort
	}
	serverCfg.AdvertiseAddr = net.JoinHostPort(serverAdvertiseAddr, serverAdvertisePort)

	// Fill in the defaults for --sql-addr.
	if serverSQLAddr == "" {
		serverSQLAddr = startCtx.serverListenAddr
	}
	if serverSQLPort == "" {
		serverSQLPort = serverListenPort
	}
	serverCfg.SQLAddr = net.JoinHostPort(serverSQLAddr, serverSQLPort)
	serverCfg.SplitListenSQL = flagSetForCmd(cmd).Lookup(cliflags.ListenSQLAddr.Name).Changed

	// Fill in the defaults for --advertise-sql-addr.
	advSpecified := flagSetForCmd(cmd).Lookup(cliflags.AdvertiseAddr.Name).Changed ||
		flagSetForCmd(cmd).Lookup(cliflags.AdvertiseHost.Name).Changed
	if serverSQLAdvertiseAddr == "" {
		if advSpecified {
			serverSQLAdvertiseAddr = serverAdvertiseAddr
		} else {
			serverSQLAdvertiseAddr = serverSQLAddr
		}
	}
	if serverSQLAdvertisePort == "" {
		if advSpecified && !serverCfg.SplitListenSQL {
			serverSQLAdvertisePort = serverAdvertisePort
		} else {
			serverSQLAdvertisePort = serverSQLPort
		}
	}
	serverCfg.SQLAdvertiseAddr = net.JoinHostPort(serverSQLAdvertiseAddr, serverSQLAdvertisePort)

	// Fill in the defaults for --http-addr.
	if serverHTTPAddr == "" {
		serverHTTPAddr = startCtx.serverListenAddr
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPAddr, serverHTTPPort)

	// Fill the advertise port into the locality advertise addresses.
	for i, addr := range localityAdvertiseHosts {
		host, port, err := netutil.SplitHostPort(addr.Address.AddressField, serverAdvertisePort)
		if err != nil {
			return err
		}
		localityAdvertiseHosts[i].Address.AddressField = net.JoinHostPort(host, port)
	}
	serverCfg.LocalityAddresses = localityAdvertiseHosts

	return nil
}

func extraClientFlagInit() {
	serverCfg.Addr = net.JoinHostPort(cliCtx.clientConnHost, cliCtx.clientConnPort)
	serverCfg.AdvertiseAddr = serverCfg.Addr
	serverCfg.SQLAddr = net.JoinHostPort(cliCtx.clientConnHost, cliCtx.clientConnPort)
	serverCfg.SQLAdvertiseAddr = serverCfg.SQLAddr
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
	vf := flagSetForCmd(cmd).Lookup(logflags.LogToStderrName)

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
