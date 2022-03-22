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
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
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
var serverListenPort, serverSocketDir string
var serverAdvertiseAddr, serverAdvertisePort string
var serverSQLAddr, serverSQLPort string
var serverSQLAdvertiseAddr, serverSQLAdvertisePort string
var serverHTTPAddr, serverHTTPPort string
var localityAdvertiseHosts localityList
var startBackground bool
var storeSpecs base.StoreSpecList

// initPreFlagsDefaults initializes the values of the global variables
// defined above.
func initPreFlagsDefaults() {
	serverListenPort = base.DefaultPort
	serverSocketDir = ""
	serverAdvertiseAddr = ""
	serverAdvertisePort = ""

	serverSQLAddr = ""
	serverSQLPort = ""
	serverSQLAdvertiseAddr = ""
	serverSQLAdvertisePort = ""

	serverHTTPAddr = ""
	serverHTTPPort = base.DefaultHTTPPort

	localityAdvertiseHosts = localityList{}

	startBackground = false

	storeSpecs = base.StoreSpecList{}
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

// stringFlag creates a string flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See context.go to initialize defaults.
func stringFlag(f *pflag.FlagSet, valPtr *string, flagInfo cliflags.FlagInfo) {
	f.StringVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo)
}

// intFlag creates an int flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See context.go to initialize defaults.
func intFlag(f *pflag.FlagSet, valPtr *int, flagInfo cliflags.FlagInfo) {
	f.IntVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo)
}

// boolFlag creates a bool flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See context.go to initialize defaults.
func boolFlag(f *pflag.FlagSet, valPtr *bool, flagInfo cliflags.FlagInfo) {
	f.BoolVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo)
}

// durationFlag creates a duration flag and registers it with the FlagSet.
// The default value is taken from the variable pointed to by valPtr.
// See context.go to initialize defaults.
func durationFlag(f *pflag.FlagSet, valPtr *time.Duration, flagInfo cliflags.FlagInfo) {
	f.DurationVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, *valPtr, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo)
}

// varFlag creates a custom-variable flag and registers it with the FlagSet.
// The default value is taken from the value's current value.
// See context.go to initialize defaults.
func varFlag(f *pflag.FlagSet, value pflag.Value, flagInfo cliflags.FlagInfo) {
	f.VarP(value, flagInfo.Name, flagInfo.Shorthand, flagInfo.Usage())
	registerEnvVarDefault(f, flagInfo)
}

// stringSliceFlag creates a string slice flag and registers it with the FlagSet.
// The default value is taken from the value's current value.
// See context.go to initialize defaults.
func stringSliceFlag(f *pflag.FlagSet, valPtr *[]string, flagInfo cliflags.FlagInfo) {
	f.StringSliceVar(valPtr, flagInfo.Name, *valPtr, flagInfo.Usage())
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
	addr, port, err := addr.SplitHostPort(v, *a.port)
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

type keyTypeFilter int8

const (
	showAll keyTypeFilter = iota
	showValues
	showIntents
	showTxns
)

// String implements the pflag.Value interface.
func (f *keyTypeFilter) String() string {
	switch *f {
	case showValues:
		return "values"
	case showIntents:
		return "intents"
	case showTxns:
		return "txns"
	}
	return "all"
}

// Type implements the pflag.Value interface.
func (f *keyTypeFilter) Type() string { return "<key type>" }

// Set implements the pflag.Value interface.
func (f *keyTypeFilter) Set(v string) error {
	switch v {
	case "values":
		*f = showValues
	case "intents":
		*f = showIntents
	case "txns":
		*f = showTxns
	default:
		return errors.Newf("invalid key filter type '%s'", v)
	}
	return nil
}

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

	// Every command but start will inherit the following setting.
	AddPersistentPreRunE(cockroachCmd, func(cmd *cobra.Command, _ []string) error {
		return extraClientFlagInit()
	})

	// Add a pre-run command for `start` and `start-single-node`, as well as the
	// multi-tenancy related commands that start long-running servers.
	// Also for `connect` which does not really start a server but uses
	// all the networking flags.
	for _, cmd := range append(serverCmds, connectInitCmd, connectJoinCmd) {
		AddPersistentPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
			// Finalize the configuration of network settings.
			return extraServerFlagInit(cmd)
		})
		AddPersistentPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
			return extraStoreFlagInit(cmd)
		})
	}

	// Add store flag handling for the pebble debug command as it needs store
	// flags configured.
	AddPersistentPreRunE(DebugPebbleCmd, func(cmd *cobra.Command, _ []string) error {
		return extraStoreFlagInit(cmd)
	})

	AddPersistentPreRunE(mtStartSQLCmd, func(cmd *cobra.Command, _ []string) error {
		return mtStartSQLFlagsInit(cmd)
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
		if strings.HasPrefix(flag.Name, "datadriven-") {
			// Same as httptest, but for the datadriven package.
			flag.Hidden = true
		}
		if strings.EqualFold(flag.Name, "log_err_stacks") {
			// Vitess registers flags directly.
			flag.Hidden = true
		}
		if flag.Name == logflags.ShowLogsName || flag.Name == logflags.TestLogConfigName {
			// test-only flag
			flag.Hidden = true
		}
		pf.AddFlag(flag)
	})

	{
		// Since cobra v0.0.7, cobra auto-adds `-v` if not defined. We don't
		// want that: we will likely want to add --verbose for some sub-commands,
		// and -v should remain reserved as an alias for --verbose.
		var unused bool
		pf.BoolVarP(&unused, "verbose", "v", false, "")
		_ = pf.MarkHidden("verbose")
	}

	// Logging flags common to all commands.
	{
		// Logging configuration.
		varFlag(pf, &stringValue{settableString: &cliCtx.logConfigInput}, cliflags.Log)
		varFlag(pf, &fileContentsValue{settableString: &cliCtx.logConfigInput, fileName: "<unset>"}, cliflags.LogConfigFile)

		// Pre-v21.1 overrides. Deprecated.
		// TODO(knz): Remove this.
		varFlag(pf, &cliCtx.deprecatedLogOverrides.stderrThreshold, cliflags.DeprecatedStderrThreshold)
		_ = pf.MarkDeprecated(cliflags.DeprecatedStderrThreshold.Name,
			"use --"+cliflags.Log.Name+" instead to specify 'sinks: {stderr: {filter: ...}}'.")
		// This flag can also be specified without an explicit argument.
		pf.Lookup(cliflags.DeprecatedStderrThreshold.Name).NoOptDefVal = "DEFAULT"

		varFlag(pf, &cliCtx.deprecatedLogOverrides.stderrNoColor, cliflags.DeprecatedStderrNoColor)
		_ = pf.MarkDeprecated(cliflags.DeprecatedStderrNoColor.Name,
			"use --"+cliflags.Log.Name+" instead to specify 'sinks: {stderr: {no-color: true}}'")

		varFlag(pf, &stringValue{&cliCtx.deprecatedLogOverrides.logDir}, cliflags.DeprecatedLogDir)
		_ = pf.MarkDeprecated(cliflags.DeprecatedLogDir.Name,
			"use --"+cliflags.Log.Name+" instead to specify 'file-defaults: {dir: ...}'")

		varFlag(pf, cliCtx.deprecatedLogOverrides.fileMaxSizeVal, cliflags.DeprecatedLogFileMaxSize)
		_ = pf.MarkDeprecated(cliflags.DeprecatedLogFileMaxSize.Name,
			"use --"+cliflags.Log.Name+" instead to specify 'file-defaults: {max-file-size: ...}'")

		varFlag(pf, cliCtx.deprecatedLogOverrides.maxGroupSizeVal, cliflags.DeprecatedLogGroupMaxSize)
		_ = pf.MarkDeprecated(cliflags.DeprecatedLogGroupMaxSize.Name,
			"use --"+cliflags.Log.Name+" instead to specify 'file-defaults: {max-group-size: ...}'")

		varFlag(pf, &cliCtx.deprecatedLogOverrides.fileThreshold, cliflags.DeprecatedFileThreshold)
		_ = pf.MarkDeprecated(cliflags.DeprecatedFileThreshold.Name,
			"use --"+cliflags.Log.Name+" instead to specify 'file-defaults: {filter: ...}'")

		varFlag(pf, &cliCtx.deprecatedLogOverrides.redactableLogs, cliflags.DeprecatedRedactableLogs)
		_ = pf.MarkDeprecated(cliflags.DeprecatedRedactableLogs.Name,
			"use --"+cliflags.Log.Name+" instead to specify 'file-defaults: {redactable: ...}")

		varFlag(pf, &stringValue{&cliCtx.deprecatedLogOverrides.sqlAuditLogDir}, cliflags.DeprecatedSQLAuditLogDir)
		_ = pf.MarkDeprecated(cliflags.DeprecatedSQLAuditLogDir.Name,
			"use --"+cliflags.Log.Name+" instead to specify 'sinks: {file-groups: {sql-audit: {channels: SENSITIVE_ACCESS, dir: ...}}}")
	}

	// Remember we are starting in the background as the `start` command will
	// avoid printing some messages to standard output in that case.
	_, startCtx.inBackground = envutil.EnvString(backgroundEnvVar, 1)

	// Flags common to the start commands, the connect command, and the node join
	// command.
	for _, cmd := range append(StartCmds, connectInitCmd, connectJoinCmd) {
		f := cmd.Flags()

		varFlag(f, addrSetter{&startCtx.serverListenAddr, &serverListenPort}, cliflags.ListenAddr)
		varFlag(f, addrSetter{&serverAdvertiseAddr, &serverAdvertisePort}, cliflags.AdvertiseAddr)
		varFlag(f, addrSetter{&serverSQLAddr, &serverSQLPort}, cliflags.ListenSQLAddr)
		varFlag(f, addrSetter{&serverSQLAdvertiseAddr, &serverSQLAdvertisePort}, cliflags.SQLAdvertiseAddr)
		varFlag(f, addrSetter{&serverHTTPAddr, &serverHTTPPort}, cliflags.ListenHTTPAddr)

		// Certificates directory. Use a server-specific flag and value to ignore environment
		// variables, but share the same default.
		stringFlag(f, &startCtx.serverSSLCertsDir, cliflags.ServerCertsDir)

		// Cluster joining flags. We need to enable this both for 'start'
		// and 'start-single-node' although the latter does not support
		// --join, because it delegates its logic to that of 'start', and
		// 'start' will check that the flag is properly defined.
		varFlag(f, &serverCfg.JoinList, cliflags.Join)
		boolFlag(f, &serverCfg.JoinPreferSRVRecords, cliflags.JoinPreferSRVRecords)
	}

	// Flags common to the start commands and the connect command.
	for _, cmd := range append(StartCmds, connectInitCmd) {
		f := cmd.Flags()

		// The initialization token and expected peers. For 'start' commands this is optional.
		stringFlag(f, &startCtx.initToken, cliflags.InitToken)
		intFlag(f, &startCtx.numExpectedNodes, cliflags.NumExpectedInitialNodes)
		boolFlag(f, &startCtx.genCertsForSingleNode, cliflags.SingleNode)

		if cmd == startSingleNodeCmd {
			// Even though all server flags are supported for
			// 'start-single-node', we intend that command to be used by
			// beginners / developers running on a single machine. To
			// enhance the UX, we hide the flags since they are not directly
			// relevant when running a single node.
			_ = f.MarkHidden(cliflags.Join.Name)
			_ = f.MarkHidden(cliflags.JoinPreferSRVRecords.Name)
			_ = f.MarkHidden(cliflags.AdvertiseAddr.Name)
			_ = f.MarkHidden(cliflags.SQLAdvertiseAddr.Name)
			_ = f.MarkHidden(cliflags.InitToken.Name)
		}

		// Backward-compatibility flags.

		// These are deprecated but until we have qualitatively new
		// functionality in the flags above, there is no need to nudge the
		// user away from them with a deprecation warning. So we keep
		// them, but hidden from docs so that they don't appear as
		// redundant with the main flags.
		varFlag(f, aliasStrVar{&startCtx.serverListenAddr}, cliflags.ServerHost)
		_ = f.MarkHidden(cliflags.ServerHost.Name)
		varFlag(f, aliasStrVar{&serverListenPort}, cliflags.ServerPort)
		_ = f.MarkHidden(cliflags.ServerPort.Name)

		varFlag(f, aliasStrVar{&serverAdvertiseAddr}, cliflags.AdvertiseHost)
		_ = f.MarkHidden(cliflags.AdvertiseHost.Name)
		varFlag(f, aliasStrVar{&serverAdvertisePort}, cliflags.AdvertisePort)
		_ = f.MarkHidden(cliflags.AdvertisePort.Name)

		varFlag(f, aliasStrVar{&serverHTTPAddr}, cliflags.ListenHTTPAddrAlias)
		_ = f.MarkHidden(cliflags.ListenHTTPAddrAlias.Name)
		varFlag(f, aliasStrVar{&serverHTTPPort}, cliflags.ListenHTTPPort)
		_ = f.MarkHidden(cliflags.ListenHTTPPort.Name)

	}

	// Flags common to the start commands only.
	for _, cmd := range StartCmds {
		f := cmd.Flags()

		// Server flags.
		stringFlag(f, &serverSocketDir, cliflags.SocketDir)
		boolFlag(f, &startCtx.unencryptedLocalhostHTTP, cliflags.UnencryptedLocalhostHTTP)

		// The following flag is planned to become non-experimental in 21.1.
		boolFlag(f, &serverCfg.AcceptSQLWithoutTLS, cliflags.AcceptSQLWithoutTLS)
		_ = f.MarkHidden(cliflags.AcceptSQLWithoutTLS.Name)

		// More server flags.

		varFlag(f, &localityAdvertiseHosts, cliflags.LocalityAdvertiseAddr)

		stringFlag(f, &serverCfg.Attrs, cliflags.Attrs)
		varFlag(f, &serverCfg.Locality, cliflags.Locality)

		varFlag(f, &storeSpecs, cliflags.Store)
		varFlag(f, &serverCfg.StorageEngine, cliflags.StorageEngine)
		varFlag(f, &serverCfg.MaxOffset, cliflags.MaxOffset)
		stringFlag(f, &serverCfg.ClockDevicePath, cliflags.ClockDevice)

		stringFlag(f, &startCtx.listeningURLFile, cliflags.ListeningURLFile)

		stringFlag(f, &startCtx.pidFile, cliflags.PIDFile)
		stringFlag(f, &startCtx.geoLibsDir, cliflags.GeoLibsDir)

		// Use a separate variable to store the value of ServerInsecure.
		// We share the default with the ClientInsecure flag.
		//
		// NB: Insecure is deprecated. See #53404.
		boolFlag(f, &startCtx.serverInsecure, cliflags.ServerInsecure)

		// Enable/disable various external storage endpoints.
		boolFlag(f, &serverCfg.ExternalIODirConfig.DisableHTTP, cliflags.ExternalIODisableHTTP)
		boolFlag(f, &serverCfg.ExternalIODirConfig.DisableOutbound, cliflags.ExternalIODisabled)
		boolFlag(f, &serverCfg.ExternalIODirConfig.DisableImplicitCredentials, cliflags.ExternalIODisableImplicitCredentials)
		boolFlag(f, &serverCfg.ExternalIODirConfig.EnableNonAdminImplicitAndArbitraryOutbound, cliflags.ExternalIOEnableNonAdminImplicitAndArbitraryOutbound)

		// Certificate principal map.
		stringSliceFlag(f, &startCtx.serverCertPrincipalMap, cliflags.CertPrincipalMap)

		// Cluster name verification.
		varFlag(f, clusterNameSetter{&baseCfg.ClusterName}, cliflags.ClusterName)
		boolFlag(f, &baseCfg.DisableClusterNameVerification, cliflags.DisableClusterNameVerification)
		if cmd == startSingleNodeCmd {
			// Even though all server flags are supported for
			// 'start-single-node', we intend that command to be used by
			// beginners / developers running on a single machine. To
			// enhance the UX, we hide the flags since they are not directly
			// relevant when running a single node.
			_ = f.MarkHidden(cliflags.ClusterName.Name)
			_ = f.MarkHidden(cliflags.DisableClusterNameVerification.Name)
			_ = f.MarkHidden(cliflags.MaxOffset.Name)
			_ = f.MarkHidden(cliflags.LocalityAdvertiseAddr.Name)
		}

		// Engine flags.
		varFlag(f, cacheSizeValue, cliflags.Cache)
		varFlag(f, sqlSizeValue, cliflags.SQLMem)
		varFlag(f, tsdbSizeValue, cliflags.TSDBMem)
		// N.B. diskTempStorageSizeValue.ResolvePercentage() will be called after
		// the stores flag has been parsed and the storage device that a percentage
		// refers to becomes known.
		varFlag(f, diskTempStorageSizeValue, cliflags.SQLTempStorage)
		stringFlag(f, &startCtx.tempDir, cliflags.TempDir)
		stringFlag(f, &startCtx.externalIODir, cliflags.ExternalIODir)

		if backgroundFlagDefined {
			boolFlag(f, &startBackground, cliflags.Background)
		}
	}

	// Flags that apply to commands that start servers.
	telemetryEnabledCmds := append(serverCmds, demoCmd)
	telemetryEnabledCmds = append(telemetryEnabledCmds, demoCmd.Commands()...)
	for _, cmd := range telemetryEnabledCmds {
		// Report flag usage for server commands in telemetry. We do this
		// only for server commands, as there is no point in accumulating
		// telemetry if there's no telemetry reporting loop being started.
		AddPersistentPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
			prefix := "cli." + cmd.Name()
			// Count flag usage.
			cmd.Flags().Visit(func(fl *pflag.Flag) {
				telemetry.Count(prefix + ".explicitflags." + fl.Name)
			})
			// Also report use of the command on its own. This is necessary
			// so we can compute flag usage as a % of total command invocations.
			telemetry.Count(prefix + ".runs")
			return nil
		})
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		// All certs commands need the certificate directory.
		stringFlag(f, &certCtx.certsDir, cliflags.CertsDir)

		// All certs command want to map CNs to SQL principals.
		stringSliceFlag(f, &certCtx.certPrincipalMap, cliflags.CertPrincipalMap)

		if cmd == listCertsCmd {
			// The 'list' subcommand does not write to files and thus does
			// not need the arguments below.
			continue
		}

		stringFlag(f, &certCtx.caKey, cliflags.CAKey)
		intFlag(f, &certCtx.keySize, cliflags.KeySize)
		boolFlag(f, &certCtx.overwriteFiles, cliflags.OverwriteFiles)

		if strings.HasSuffix(cmd.Name(), "-ca") {
			// CA-only commands.

			// CA certificates have a longer expiration time.
			durationFlag(f, &certCtx.caCertificateLifetime, cliflags.CertificateLifetime)
			// The CA key can be re-used if it exists.
			boolFlag(f, &certCtx.allowCAKeyReuse, cliflags.AllowCAKeyReuse)
		} else {
			// Non-CA commands.

			durationFlag(f, &certCtx.certificateLifetime, cliflags.CertificateLifetime)
		}

		// PKCS8 key format is only available for the client cert command.
		if cmd == createClientCertCmd {
			boolFlag(f, &certCtx.generatePKCS8Key, cliflags.GeneratePKCS8Key)
		}
	}

	clientCmds := []*cobra.Command{
		debugJobTraceFromClusterCmd,
		debugGossipValuesCmd,
		debugTimeSeriesDumpCmd,
		debugZipCmd,
		debugListFilesCmd,
		debugSendKVBatchCmd,
		doctorExamineClusterCmd,
		doctorExamineFallbackClusterCmd,
		doctorRecreateClusterCmd,
		genHAProxyCmd,
		initCmd,
		quitCmd,
		sqlShellCmd,
		/* StartCmds are covered above */
	}
	clientCmds = append(clientCmds, authCmds...)
	clientCmds = append(clientCmds, nodeCmds...)
	clientCmds = append(clientCmds, nodeLocalCmds...)
	clientCmds = append(clientCmds, importCmds...)
	clientCmds = append(clientCmds, userFileCmds...)
	clientCmds = append(clientCmds, stmtDiagCmds...)
	clientCmds = append(clientCmds, debugResetQuorumCmd)
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		varFlag(f, addrSetter{&cliCtx.clientConnHost, &cliCtx.clientConnPort}, cliflags.ClientHost)
		stringFlag(f, &cliCtx.clientConnPort, cliflags.ClientPort)
		_ = f.MarkHidden(cliflags.ClientPort.Name)

		// NB: Insecure is deprecated. See #53404.
		boolFlag(f, &baseCfg.Insecure, cliflags.ClientInsecure)

		// Certificate flags.
		stringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir)
		// Certificate principal map.
		stringSliceFlag(f, &cliCtx.certPrincipalMap, cliflags.CertPrincipalMap)
	}

	// convert-url is not really a client command. It just recognizes (some)
	// client flags.
	{
		f := convertURLCmd.PersistentFlags()
		stringFlag(f, &convertCtx.url, cliflags.URL)
	}

	// Auth commands.
	{
		f := loginCmd.Flags()
		durationFlag(f, &authCtx.validityPeriod, cliflags.AuthTokenValidityPeriod)
		boolFlag(f, &authCtx.onlyCookie, cliflags.OnlyCookie)
	}

	timeoutCmds := []*cobra.Command{
		statusNodeCmd,
		lsNodesCmd,
		debugJobTraceFromClusterCmd,
		debugZipCmd,
		doctorExamineClusterCmd,
		doctorExamineFallbackClusterCmd,
		doctorRecreateClusterCmd,
		// If you add something here, make sure the actual implementation
		// of the command uses `cmdTimeoutContext(.)` or it will ignore
		// the timeout.
	}

	for _, cmd := range timeoutCmds {
		durationFlag(cmd.Flags(), &cliCtx.cmdTimeout, cliflags.Timeout)
	}

	// Node Status command.
	{
		f := statusNodeCmd.Flags()
		boolFlag(f, &nodeCtx.statusShowRanges, cliflags.NodeRanges)
		boolFlag(f, &nodeCtx.statusShowStats, cliflags.NodeStats)
		boolFlag(f, &nodeCtx.statusShowAll, cliflags.NodeAll)
		boolFlag(f, &nodeCtx.statusShowDecommission, cliflags.NodeDecommission)
	}

	// Zip command.
	{
		f := debugZipCmd.Flags()
		boolFlag(f, &zipCtx.redactLogs, cliflags.ZipRedactLogs)
		durationFlag(f, &zipCtx.cpuProfDuration, cliflags.ZipCPUProfileDuration)
		intFlag(f, &zipCtx.concurrency, cliflags.ZipConcurrency)
	}
	// List-files + Zip commands.
	for _, cmd := range []*cobra.Command{debugZipCmd, debugListFilesCmd} {
		f := cmd.Flags()
		varFlag(f, &zipCtx.nodes.inclusive, cliflags.ZipNodes)
		varFlag(f, &zipCtx.nodes.exclusive, cliflags.ZipExcludeNodes)
		stringSliceFlag(f, &zipCtx.files.includePatterns, cliflags.ZipIncludedFiles)
		stringSliceFlag(f, &zipCtx.files.excludePatterns, cliflags.ZipExcludedFiles)
		varFlag(f, &zipCtx.files.startTimestamp, cliflags.ZipFilesFrom)
		varFlag(f, &zipCtx.files.endTimestamp, cliflags.ZipFilesUntil)
	}

	// Decommission command.
	varFlag(decommissionNodeCmd.Flags(), &nodeCtx.nodeDecommissionWait, cliflags.Wait)

	// Decommission and recommission share --self.
	for _, cmd := range []*cobra.Command{decommissionNodeCmd, recommissionNodeCmd} {
		f := cmd.Flags()
		boolFlag(f, &nodeCtx.nodeDecommissionSelf, cliflags.NodeDecommissionSelf)
	}

	// Quit and node drain commands.
	for _, cmd := range []*cobra.Command{quitCmd, drainNodeCmd} {
		f := cmd.Flags()
		durationFlag(f, &quitCtx.drainWait, cliflags.DrainWait)
		boolFlag(f, &quitCtx.nodeDrainSelf, cliflags.NodeDrainSelf)
	}

	// SQL and demo commands.
	for _, cmd := range append([]*cobra.Command{sqlShellCmd, demoCmd}, demoCmd.Commands()...) {
		f := cmd.Flags()
		varFlag(f, &sqlCtx.ShellCtx.SetStmts, cliflags.Set)
		varFlag(f, &sqlCtx.ShellCtx.ExecStmts, cliflags.Execute)
		stringFlag(f, &sqlCtx.InputFile, cliflags.File)
		durationFlag(f, &sqlCtx.ShellCtx.RepeatDelay, cliflags.Watch)
		varFlag(f, &sqlCtx.SafeUpdates, cliflags.SafeUpdates)
		boolFlag(f, &sqlCtx.ReadOnly, cliflags.ReadOnly)
		// The "safe-updates" flag is tri-valued (true, false, not-specified).
		// If the flag is specified on the command line, but is not given a value,
		// then use the value "true".
		f.Lookup(cliflags.SafeUpdates.Name).NoOptDefVal = "true"
		boolFlag(f, &sqlConnCtx.DebugMode, cliflags.CliDebugMode)
		boolFlag(f, &cliCtx.EmbeddedMode, cliflags.EmbeddedMode)
	}

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{
		sqlShellCmd,
		demoCmd,
		debugJobTraceFromClusterCmd,
		doctorExamineClusterCmd,
		doctorExamineFallbackClusterCmd,
		doctorRecreateClusterCmd,
		statementBundleRecreateCmd,
		lsNodesCmd,
		statusNodeCmd,
	}
	sqlCmds = append(sqlCmds, authCmds...)
	sqlCmds = append(sqlCmds, demoCmd.Commands()...)
	sqlCmds = append(sqlCmds, stmtDiagCmds...)
	sqlCmds = append(sqlCmds, nodeLocalCmds...)
	sqlCmds = append(sqlCmds, importCmds...)
	sqlCmds = append(sqlCmds, userFileCmds...)
	for _, cmd := range sqlCmds {
		f := cmd.Flags()
		// The --echo-sql flag is special: it is a marker for CLI tests to
		// recognize SQL-only commands. If/when adding this flag to non-SQL
		// commands, ensure the isSQLCommand() predicate is updated accordingly.
		boolFlag(f, &sqlConnCtx.Echo, cliflags.EchoSQL)

		varFlag(f, urlParser{cmd, &cliCtx, false /* strictSSL */}, cliflags.URL)
		stringFlag(f, &cliCtx.sqlConnUser, cliflags.User)
		if cmd == demoCmd {
			// The 'demo' command does not really support --url or --user.
			// However, we create the pflag instance so that the user
			// can use \connect inside the shell session.
			_ = f.MarkHidden(cliflags.URL.Name)
			_ = f.MarkHidden(cliflags.User.Name)
		}

		// Even though SQL commands take their connection parameters via
		// --url / --user (see above), the urlParser{} struct internally
		// needs the ClientHost and ClientPort flags to be defined -
		// even if they are invisible - due to the way initialization from
		// env vars is implemented.
		//
		// TODO(knz): if/when env var option initialization is deferred
		// to parse time, this can be removed.
		varFlag(f, addrSetter{&cliCtx.clientConnHost, &cliCtx.clientConnPort}, cliflags.ClientHost)
		_ = f.MarkHidden(cliflags.ClientHost.Name)
		stringFlag(f, &cliCtx.clientConnPort, cliflags.ClientPort)
		_ = f.MarkHidden(cliflags.ClientPort.Name)

		if cmd == sqlShellCmd || cmd == demoCmd {
			stringFlag(f, &cliCtx.sqlConnDBName, cliflags.Database)
			if cmd == demoCmd {
				// As above, 'demo' does not really support --database.
				// However, we create the pflag instance so that
				// the user can use \connect inside the shell.
				_ = f.MarkHidden(cliflags.Database.Name)
			}
		}
	}

	// Make the non-SQL client commands also recognize --url in strict SSL mode
	// and ensure they can connect to clusters that use a cluster-name.
	for _, cmd := range clientCmds {
		if fl := flagSetForCmd(cmd).Lookup(cliflags.URL.Name); fl != nil {
			// --url already registered above: this is a SQL client command.
			// The code below is not intended for it.
			continue
		}

		f := cmd.PersistentFlags()
		varFlag(f, urlParser{cmd, &cliCtx, true /* strictSSL */}, cliflags.URL)

		varFlag(f, clusterNameSetter{&baseCfg.ClusterName}, cliflags.ClusterName)
		boolFlag(f, &baseCfg.DisableClusterNameVerification, cliflags.DisableClusterNameVerification)
	}

	// Commands that print tables.
	tableOutputCommands := append(
		[]*cobra.Command{
			sqlShellCmd,
			genSettingsListCmd,
			demoCmd,
			statementBundleRecreateCmd,
			debugListFilesCmd,
			debugJobTraceFromClusterCmd,
		},
		demoCmd.Commands()...)
	tableOutputCommands = append(tableOutputCommands, nodeCmds...)
	tableOutputCommands = append(tableOutputCommands, authCmds...)

	// By default, these commands print their output as pretty-formatted
	// tables on terminals, and TSV when redirected to a file. The user
	// can override with --format.
	// By default, query times are not displayed. The default is overridden
	// in the CLI shell.
	for _, cmd := range tableOutputCommands {
		f := cmd.PersistentFlags()
		varFlag(f, &sqlExecCtx.TableDisplayFormat, cliflags.TableDisplayFormat)
	}

	// demo command.
	{
		// We use the persistent flag set so that the flags apply to every
		// workload sub-command. This enables e.g.
		// ./cockroach demo movr --nodes=3.
		f := demoCmd.PersistentFlags()

		intFlag(f, &demoCtx.NumNodes, cliflags.DemoNodes)
		boolFlag(f, &demoCtx.RunWorkload, cliflags.RunDemoWorkload)
		intFlag(f, &demoCtx.WorkloadMaxQPS, cliflags.DemoWorkloadMaxQPS)
		varFlag(f, &demoCtx.Localities, cliflags.DemoNodeLocality)
		boolFlag(f, &demoCtx.GeoPartitionedReplicas, cliflags.DemoGeoPartitionedReplicas)
		varFlag(f, demoNodeSQLMemSizeValue, cliflags.DemoNodeSQLMemSize)
		varFlag(f, demoNodeCacheSizeValue, cliflags.DemoNodeCacheSize)
		boolFlag(f, &demoCtx.Insecure, cliflags.ClientInsecure)
		// NB: Insecure for `cockroach demo` is deprecated. See #53404.
		_ = f.MarkDeprecated(cliflags.ServerInsecure.Name,
			"to start a test server without any security, run start-single-node --insecure\n"+
				"For details, see: "+build.MakeIssueURL(53404))

		boolFlag(f, &demoCtx.DisableLicenseAcquisition, cliflags.DemoNoLicense)
		boolFlag(f, &demoCtx.Multitenant, cliflags.DemoMultitenant)
		boolFlag(f, &demoCtx.SimulateLatency, cliflags.Global)
		// The --empty flag is only valid for the top level demo command,
		// so we use the regular flag set.
		boolFlag(demoCmd.Flags(), &demoCtx.NoExampleDatabase, cliflags.UseEmptyDatabase)
		_ = f.MarkDeprecated(cliflags.UseEmptyDatabase.Name, "use --no-workload-database")
		boolFlag(demoCmd.Flags(), &demoCtx.NoExampleDatabase, cliflags.NoExampleDatabase)
		// We also support overriding the GEOS library path for 'demo'.
		// Even though the demoCtx uses mostly different configuration
		// variables from startCtx, this is one case where we afford
		// sharing a variable between both.
		stringFlag(f, &startCtx.geoLibsDir, cliflags.GeoLibsDir)

		intFlag(f, &demoCtx.SQLPort, cliflags.DemoSQLPort)
		intFlag(f, &demoCtx.HTTPPort, cliflags.DemoHTTPPort)
		stringFlag(f, &demoCtx.ListeningURLFile, cliflags.ListeningURLFile)
	}

	// statement-diag command.
	{
		boolFlag(stmtDiagDeleteCmd.Flags(), &stmtDiagCtx.all, cliflags.StmtDiagDeleteAll)
		boolFlag(stmtDiagCancelCmd.Flags(), &stmtDiagCtx.all, cliflags.StmtDiagCancelAll)
	}

	// import dump command.
	{
		d := importDumpFileCmd.Flags()
		boolFlag(d, &importCtx.skipForeignKeys, cliflags.ImportSkipForeignKeys)
		intFlag(d, &importCtx.maxRowSize, cliflags.ImportMaxRowSize)
		intFlag(d, &importCtx.rowLimit, cliflags.ImportRowLimit)
		boolFlag(d, &importCtx.ignoreUnsupported, cliflags.ImportIgnoreUnsupportedStatements)
		stringFlag(d, &importCtx.ignoreUnsupportedLog, cliflags.ImportLogIgnoredStatements)
		stringFlag(d, &cliCtx.sqlConnDBName, cliflags.Database)

		t := importDumpTableCmd.Flags()
		boolFlag(t, &importCtx.skipForeignKeys, cliflags.ImportSkipForeignKeys)
		intFlag(t, &importCtx.maxRowSize, cliflags.ImportMaxRowSize)
		intFlag(t, &importCtx.rowLimit, cliflags.ImportRowLimit)
		boolFlag(t, &importCtx.ignoreUnsupported, cliflags.ImportIgnoreUnsupportedStatements)
		stringFlag(t, &importCtx.ignoreUnsupportedLog, cliflags.ImportLogIgnoredStatements)
		stringFlag(t, &cliCtx.sqlConnDBName, cliflags.Database)
	}

	// sqlfmt command.
	{
		f := sqlfmtCmd.Flags()
		varFlag(f, &sqlfmtCtx.execStmts, cliflags.Execute)
		intFlag(f, &sqlfmtCtx.len, cliflags.SQLFmtLen)
		boolFlag(f, &sqlfmtCtx.useSpaces, cliflags.SQLFmtSpaces)
		intFlag(f, &sqlfmtCtx.tabWidth, cliflags.SQLFmtTabWidth)
		boolFlag(f, &sqlfmtCtx.noSimplify, cliflags.SQLFmtNoSimplify)
		boolFlag(f, &sqlfmtCtx.align, cliflags.SQLFmtAlign)
	}

	// version command.
	{
		f := versionCmd.Flags()
		boolFlag(f, &cliCtx.showVersionUsingOnlyBuildTag, cliflags.BuildTag)
	}

	// Debug commands.
	{
		f := debugKeysCmd.Flags()
		varFlag(f, (*mvccKey)(&debugCtx.startKey), cliflags.From)
		varFlag(f, (*mvccKey)(&debugCtx.endKey), cliflags.To)
		intFlag(f, &debugCtx.maxResults, cliflags.Limit)
		boolFlag(f, &debugCtx.values, cliflags.Values)
		boolFlag(f, &debugCtx.sizes, cliflags.Sizes)
		stringFlag(f, &debugCtx.decodeAsTableDesc, cliflags.DecodeAsTable)
		varFlag(f, &debugCtx.keyTypes, cliflags.FilterKeys)
	}
	{
		f := debugCheckLogConfigCmd.Flags()
		varFlag(f, &storeSpecs, cliflags.Store)
	}
	{
		f := debugRangeDataCmd.Flags()
		boolFlag(f, &debugCtx.replicated, cliflags.Replicated)
		intFlag(f, &debugCtx.maxResults, cliflags.Limit)
	}
	{
		f := debugGossipValuesCmd.Flags()
		stringFlag(f, &debugCtx.inputFile, cliflags.GossipInputFile)
		boolFlag(f, &debugCtx.printSystemConfig, cliflags.PrintSystemConfig)
	}
	{
		f := debugBallastCmd.Flags()
		varFlag(f, &debugCtx.ballastSize, cliflags.Size)
	}
	{
		// TODO(ayang): clean up so dir isn't passed to both pebble and --store
		f := DebugPebbleCmd.PersistentFlags()
		varFlag(f, &storeSpecs, cliflags.Store)
	}
	{
		for _, c := range []*cobra.Command{
			debugJobTraceFromClusterCmd,
			doctorExamineClusterCmd,
			doctorExamineZipDirCmd,
			doctorExamineFallbackClusterCmd,
			doctorExamineFallbackZipDirCmd,
			doctorRecreateClusterCmd,
			doctorRecreateZipDirCmd,
		} {
			f := c.Flags()
			if f.Lookup(cliflags.Verbose.Name) == nil {
				boolFlag(f, &debugCtx.verbose, cliflags.Verbose)
			}
		}
	}

	// Multi-tenancy start-sql command flags.
	{
		f := mtStartSQLCmd.Flags()
		varFlag(f, &tenantIDWrapper{&serverCfg.SQLConfig.TenantID}, cliflags.TenantID)
		// NB: serverInsecure populates baseCfg.{Insecure,SSLCertsDir} in this the following method
		// (which is a PreRun for this command):
		_ = extraServerFlagInit // guru assignment
		// NB: Insecure is deprecated. See #53404.
		boolFlag(f, &startCtx.serverInsecure, cliflags.ServerInsecure)

		stringFlag(f, &startCtx.serverSSLCertsDir, cliflags.ServerCertsDir)
		// NB: this also gets PreRun treatment via extraServerFlagInit to populate BaseCfg.SQLAddr.
		varFlag(f, addrSetter{&serverSQLAddr, &serverSQLPort}, cliflags.ListenSQLAddr)
		varFlag(f, addrSetter{&serverHTTPAddr, &serverHTTPPort}, cliflags.ListenHTTPAddr)
		varFlag(f, addrSetter{&serverAdvertiseAddr, &serverAdvertisePort}, cliflags.AdvertiseAddr)

		varFlag(f, &serverCfg.Locality, cliflags.Locality)
		varFlag(f, &serverCfg.MaxOffset, cliflags.MaxOffset)
		varFlag(f, &storeSpecs, cliflags.Store)
		stringFlag(f, &startCtx.pidFile, cliflags.PIDFile)
		stringFlag(f, &startCtx.geoLibsDir, cliflags.GeoLibsDir)

		stringSliceFlag(f, &serverCfg.SQLConfig.TenantKVAddrs, cliflags.KVAddrs)

		// Enable/disable various external storage endpoints.
		boolFlag(f, &serverCfg.ExternalIODirConfig.DisableHTTP, cliflags.ExternalIODisableHTTP)
		boolFlag(f, &serverCfg.ExternalIODirConfig.DisableOutbound, cliflags.ExternalIODisabled)
		boolFlag(f, &serverCfg.ExternalIODirConfig.DisableImplicitCredentials, cliflags.ExternalIODisableImplicitCredentials)

		// Engine flags.
		varFlag(f, sqlSizeValue, cliflags.SQLMem)
		varFlag(f, tsdbSizeValue, cliflags.TSDBMem)
		// N.B. diskTempStorageSizeValue.ResolvePercentage() will be called after
		// the stores flag has been parsed and the storage device that a percentage
		// refers to becomes known.
		varFlag(f, diskTempStorageSizeValue, cliflags.SQLTempStorage)
		stringFlag(f, &startCtx.tempDir, cliflags.TempDir)

		if backgroundFlagDefined {
			boolFlag(f, &startBackground, cliflags.Background)
		}
	}

	// Multi-tenancy proxy command flags.
	{
		f := mtStartSQLProxyCmd.Flags()
		stringFlag(f, &proxyContext.Denylist, cliflags.DenyList)
		stringFlag(f, &proxyContext.ListenAddr, cliflags.ProxyListenAddr)
		stringFlag(f, &proxyContext.ListenCert, cliflags.ListenCert)
		stringFlag(f, &proxyContext.ListenKey, cliflags.ListenKey)
		stringFlag(f, &proxyContext.MetricsAddress, cliflags.ListenMetrics)
		stringFlag(f, &proxyContext.RoutingRule, cliflags.RoutingRule)
		stringFlag(f, &proxyContext.DirectoryAddr, cliflags.DirectoryAddr)
		boolFlag(f, &proxyContext.SkipVerify, cliflags.SkipVerify)
		boolFlag(f, &proxyContext.Insecure, cliflags.InsecureBackend)
		durationFlag(f, &proxyContext.ValidateAccessInterval, cliflags.ValidateAccessInterval)
		durationFlag(f, &proxyContext.PollConfigInterval, cliflags.PollConfigInterval)
		durationFlag(f, &proxyContext.DrainTimeout, cliflags.DrainTimeout)
		durationFlag(f, &proxyContext.ThrottleBaseDelay, cliflags.ThrottleBaseDelay)
	}
	// Multi-tenancy test directory command flags.
	{
		f := mtTestDirectorySvr.Flags()
		intFlag(f, &testDirectorySvrContext.port, cliflags.TestDirectoryListenPort)
		stringFlag(f, &testDirectorySvrContext.certsDir, cliflags.TestDirectoryTenantCertsDir)
		stringFlag(f, &testDirectorySvrContext.tenantBaseDir, cliflags.TestDirectoryTenantBaseDir)
		stringFlag(f, &testDirectorySvrContext.kvAddrs, cliflags.KVAddrs)
	}

	// userfile upload command.
	{
		boolFlag(userFileUploadCmd.Flags(), &userfileCtx.recursive, cliflags.Recursive)
	}
}

type tenantIDWrapper struct {
	tenID *roachpb.TenantID
}

func (w *tenantIDWrapper) String() string {
	return w.tenID.String()
}
func (w *tenantIDWrapper) Set(s string) error {
	tenID, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid tenant ID")
	}
	if tenID == 0 {
		return errors.New("invalid tenant ID")
	}
	*w.tenID = roachpb.MakeTenantID(tenID)
	return nil
}

func (w *tenantIDWrapper) Type() string {
	return "number"
}

// processEnvVarDefaults injects the current value of flag-related
// environment variables into the initial value of the settings linked
// to the flags, during initialization and before the command line is
// actually parsed. For example, it will inject the value of
// $COCKROACH_URL into the urlParser object linked to the --url flag.
func processEnvVarDefaults(cmd *cobra.Command) error {
	fl := flagSetForCmd(cmd)

	var retErr error
	fl.VisitAll(func(f *pflag.Flag) {
		envv, ok := f.Annotations[envValueAnnotationKey]
		if !ok || len(envv) < 2 {
			// No env var associated. Nothing to do.
			return
		}
		varName, value := envv[0], envv[1]
		if err := fl.Set(f.Name, value); err != nil {
			retErr = errors.CombineErrors(retErr,
				errors.Wrapf(err, "setting --%s from %s", f.Name, varName))
		}
	})
	return retErr
}

const (
	// envValueAnnotationKey is the map key used in pflag.Flag instances
	// to associate flags with a possible default value set by an
	// env var.
	envValueAnnotationKey = "envvalue"
)

// registerEnvVarDefault registers a deferred initialization of a flag
// from an environment variable.
// The caller is responsible for ensuring that the flagInfo has been
// defined in the FlagSet already.
func registerEnvVarDefault(f *pflag.FlagSet, flagInfo cliflags.FlagInfo) {
	if flagInfo.EnvVar == "" {
		return
	}

	value, set := envutil.EnvString(flagInfo.EnvVar, 2)
	if !set {
		// Env var is not set. Nothing to do.
		return
	}

	if err := f.SetAnnotation(flagInfo.Name, envValueAnnotationKey, []string{flagInfo.EnvVar, value}); err != nil {
		// This should never happen: an error is only returned if the flag
		// name was not defined yet.
		panic(err)
	}
}

// extraServerFlagInit configures the server.Config based on the command-line flags.
// It is only called when the command being ran is one of the start commands.
func extraServerFlagInit(cmd *cobra.Command) error {
	if err := security.SetCertPrincipalMap(startCtx.serverCertPrincipalMap); err != nil {
		return err
	}
	serverCfg.User = security.NodeUserName()
	serverCfg.Insecure = startCtx.serverInsecure
	serverCfg.SSLCertsDir = startCtx.serverSSLCertsDir

	// Construct the main RPC listen address.
	serverCfg.Addr = net.JoinHostPort(startCtx.serverListenAddr, serverListenPort)

	fs := flagSetForCmd(cmd)

	// Helper for .Changed that is nil-aware as not all of the `cmd`s may have
	// all of the flags.
	changed := func(set *pflag.FlagSet, name string) bool {
		f := set.Lookup(name)
		return f != nil && f.Changed
	}

	// Construct the socket name, if requested. The flags may not be defined for
	// `cmd` so be cognizant of that.
	//
	// If --socket-dir is set, then we'll use that.
	// There are two cases:
	// 1. --socket-dir is set and is empty; in this case the user is telling us
	//    "disable the socket".
	// 2. is set and non-empty. Then it should be used as specified.
	if changed(fs, cliflags.SocketDir.Name) {
		if serverSocketDir == "" {
			serverCfg.SocketFile = ""
		} else {
			serverCfg.SocketFile = filepath.Join(serverSocketDir, ".s.PGSQL."+serverListenPort)
		}
	}

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
	serverCfg.SplitListenSQL = fs.Lookup(cliflags.ListenSQLAddr.Name).Changed

	// Fill in the defaults for --advertise-sql-addr, if the flag exists on `cmd`.
	advSpecified := changed(fs, cliflags.AdvertiseAddr.Name) ||
		changed(fs, cliflags.AdvertiseHost.Name)
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
	if startCtx.unencryptedLocalhostHTTP {
		// If --unencrypted-localhost-http was specified, we want to
		// override whatever was specified or derived from other flags for
		// the host part of --http-addr.
		//
		// Before we do so, we'll check whether the user explicitly
		// specified something contradictory, and tell them that's no
		// good.
		if (changed(fs, cliflags.ListenHTTPAddr.Name) ||
			changed(fs, cliflags.ListenHTTPAddrAlias.Name)) &&
			(serverHTTPAddr != "" && serverHTTPAddr != "localhost") {
			return errors.WithHintf(
				errors.Newf("--unencrypted-localhost-http is incompatible with --http-addr=%s:%s",
					serverHTTPAddr, serverHTTPPort),
				`When --unencrypted-localhost-http is specified, use --http-addr=:%s or omit --http-addr entirely.`, serverHTTPPort)
		}

		// Now do the override proper.
		serverHTTPAddr = "localhost"
		// We then also tell the server to disable TLS for the HTTP
		// listener.
		serverCfg.DisableTLSForHTTP = true
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPAddr, serverHTTPPort)

	// Fill the advertise port into the locality advertise addresses.
	for i, a := range localityAdvertiseHosts {
		host, port, err := addr.SplitHostPort(a.Address.AddressField, serverAdvertisePort)
		if err != nil {
			return err
		}
		localityAdvertiseHosts[i].Address.AddressField = net.JoinHostPort(host, port)
	}
	serverCfg.LocalityAddresses = localityAdvertiseHosts

	return nil
}

// Fill the store paths.
// We have different defaults for server and tenant pod, and we don't want incorrect
// default to show up in flag help. To achieve that we create empty spec in private
// flag copy of spec and then copy this value if it was populated.
// If it isn't populated, default from server config is used for server commands or
// alternative default is generated by PreRun multi-tenant hook.
func extraStoreFlagInit(cmd *cobra.Command) error {
	fs := flagSetForCmd(cmd)
	if fs.Changed(cliflags.Store.Name) {
		serverCfg.Stores = storeSpecs
	}
	return nil
}

func extraClientFlagInit() error {
	// A command can be either a 'cert' command or an actual client command.
	// TODO(knz): Clean this up to not use a global variable for the
	// principal map.
	principalMap := certCtx.certPrincipalMap
	if principalMap == nil {
		principalMap = cliCtx.certPrincipalMap
	}
	if err := security.SetCertPrincipalMap(principalMap); err != nil {
		return err
	}
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
	if sqlConnCtx.DebugMode {
		sqlConnCtx.Echo = true
	}
	return nil
}

func mtStartSQLFlagsInit(cmd *cobra.Command) error {
	// Override default store for mt to use a per tenant store directory.
	fs := flagSetForCmd(cmd)
	if !fs.Changed(cliflags.Store.Name) {
		// We assume that we only need to change top level store as temp dir configs are
		// initialized when start is executed and temp dirs inherit path from first store.
		tenantID := fs.Lookup(cliflags.TenantID.Name).Value.String()
		serverCfg.Stores.Specs[0].Path = server.DefaultSQLNodeStorePathPrefix + tenantID
	}
	return nil
}

// VarFlag is exported for use in package cliccl.
var VarFlag = varFlag
