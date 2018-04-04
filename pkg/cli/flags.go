// Copyright 2015 The Cockroach Authors.
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
	"flag"
	"net"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
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
var serverConnPort, serverAdvertiseHost, serverAdvertisePort string
var serverHTTPHost, serverHTTPPort string
var clientConnHost, clientConnPort string

// initPreFlagsDefaults initializes the values of the global variables
// defined above.
func initPreFlagsDefaults() {
	serverConnPort = base.DefaultPort
	serverAdvertiseHost = ""
	serverAdvertisePort = ""

	serverHTTPHost = ""
	serverHTTPPort = base.DefaultHTTPPort

	clientConnHost = ""
	clientConnPort = base.DefaultPort
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

func init() {
	initCLIDefaults()

	// Change the logging defaults for the main cockroach binary.
	// The value is overridden after command-line parsing.
	if err := flag.Lookup(logflags.LogToStderrName).Value.Set("false"); err != nil {
		panic(err)
	}

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
		} else if flag.Name == logflags.NoRedirectStderrName {
			flag.Hidden = true
		} else if flag.Name == logflags.ShowLogsName {
			flag.Hidden = true
		}
		pf.AddFlag(flag)
	})

	// The --log-dir default changes depending on the command. Avoid confusion by
	// simply clearing it.
	pf.Lookup(logflags.LogDirName).DefValue = ""
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

	// Security flags.

	{
		f := StartCmd.Flags()

		// Server flags.
		StringFlag(f, &startCtx.serverConnHost, cliflags.ServerHost, startCtx.serverConnHost)
		StringFlag(f, &serverConnPort, cliflags.ServerPort, serverConnPort)
		StringFlag(f, &serverAdvertiseHost, cliflags.AdvertiseHost, serverAdvertiseHost)
		StringFlag(f, &serverAdvertisePort, cliflags.AdvertisePort, serverAdvertisePort)
		// The advertise port flag is used for testing purposes only and is kept hidden.
		_ = f.MarkHidden(cliflags.AdvertisePort.Name)
		StringFlag(f, &serverHTTPHost, cliflags.ServerHTTPHost, serverHTTPHost)
		StringFlag(f, &serverHTTPPort, cliflags.ServerHTTPPort, serverHTTPPort)
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

		StringFlag(f, &serverCfg.ListeningURLFile, cliflags.ListeningURLFile, serverCfg.ListeningURLFile)

		StringFlag(f, &serverCfg.PIDFile, cliflags.PIDFile, serverCfg.PIDFile)

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

	for _, cmd := range certCmds {
		f := cmd.Flags()
		// All certs commands need the certificate directory.
		StringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir, baseCfg.SSLCertsDir)
	}

	for _, cmd := range []*cobra.Command{createCACertCmd} {
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
	for _, cmd := range []*cobra.Command{createCACertCmd, createNodeCertCmd, createClientCertCmd} {
		f := cmd.Flags()
		StringFlag(f, &baseCfg.SSLCAKey, cliflags.CAKey, baseCfg.SSLCAKey)
		IntFlag(f, &keySize, cliflags.KeySize, defaultKeySize)
		BoolFlag(f, &overwriteFiles, cliflags.OverwriteFiles, false)
	}

	BoolFlag(setUserCmd.Flags(), &password, cliflags.Password, false)

	clientCmds := []*cobra.Command{
		debugGossipValuesCmd,
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
	clientCmds = append(clientCmds, initCmd)
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		StringFlag(f, &clientConnHost, cliflags.ClientHost, clientConnHost)
		StringFlag(f, &clientConnPort, cliflags.ClientPort, clientConnPort)

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

	// Decommission command.
	VarFlag(decommissionNodeCmd.Flags(), &nodeCtx.nodeDecommissionWait, cliflags.Wait)

	// Quit command.
	BoolFlag(quitCmd.Flags(), &quitCtx.serverDecommission, cliflags.Decommission, quitCtx.serverDecommission)

	zf := setZoneCmd.Flags()
	StringFlag(zf, &zoneCtx.zoneConfig, cliflags.ZoneConfig, zoneCtx.zoneConfig)
	BoolFlag(zf, &zoneCtx.zoneDisableReplication, cliflags.ZoneDisableReplication, zoneCtx.zoneDisableReplication)

	VarFlag(sqlShellCmd.Flags(), &sqlCtx.execStmts, cliflags.Execute)
	BoolFlag(sqlShellCmd.Flags(), &sqlCtx.safeUpdates, cliflags.SafeUpdates, sqlCtx.safeUpdates)

	VarFlag(dumpCmd.Flags(), &dumpCtx.dumpMode, cliflags.DumpMode)
	StringFlag(dumpCmd.Flags(), &dumpCtx.asOf, cliflags.DumpTime, dumpCtx.asOf)

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, dumpCmd}
	sqlCmds = append(sqlCmds, zoneCmds...)
	sqlCmds = append(sqlCmds, userCmds...)
	for _, cmd := range sqlCmds {
		f := cmd.PersistentFlags()
		BoolFlag(f, &sqlCtx.echo, cliflags.EchoSQL, sqlCtx.echo)
		StringFlag(f, &cliCtx.sqlConnURL, cliflags.URL, cliCtx.sqlConnURL)
		StringFlag(f, &cliCtx.sqlConnUser, cliflags.User, cliCtx.sqlConnUser)

		if cmd == sqlShellCmd {
			StringFlag(f, &cliCtx.sqlConnDBName, cliflags.Database, cliCtx.sqlConnDBName)
		}
	}

	// Commands that print tables.
	tableOutputCommands := []*cobra.Command{sqlShellCmd}
	tableOutputCommands = append(tableOutputCommands, userCmds...)
	tableOutputCommands = append(tableOutputCommands, nodeCmds...)

	// By default, these commands print their output as pretty-formatted
	// tables on terminals, and TSV when redirected to a file. The user
	// can override with --format.
	// By default, query times are not displayed. The default is overridden
	// in the CLI shell.
	for _, cmd := range tableOutputCommands {
		f := cmd.Flags()
		VarFlag(f, &cliCtx.tableDisplayFormat, cliflags.TableDisplayFormat)
	}

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
}

func extraServerFlagInit() {
	serverCfg.Addr = net.JoinHostPort(startCtx.serverConnHost, serverConnPort)
	if serverAdvertiseHost == "" {
		serverAdvertiseHost = startCtx.serverConnHost
	}
	if serverAdvertisePort == "" {
		serverAdvertisePort = serverConnPort
	}
	serverCfg.AdvertiseAddr = net.JoinHostPort(serverAdvertiseHost, serverAdvertisePort)
	if serverHTTPHost == "" {
		serverHTTPHost = startCtx.serverConnHost
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPHost, serverHTTPPort)
}

func extraClientFlagInit() {
	serverCfg.Addr = net.JoinHostPort(clientConnHost, clientConnPort)
	serverCfg.AdvertiseAddr = serverCfg.Addr
	if serverHTTPHost == "" {
		serverHTTPHost = startCtx.serverConnHost
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPHost, serverHTTPPort)
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
