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
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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
var serverListenPort, serverSocketDir string
var serverAdvertiseAddr, serverAdvertisePort string
var serverSQLAddr, serverSQLPort string
var serverSQLAdvertiseAddr, serverSQLAdvertisePort string
var serverHTTPAddr, serverHTTPPort string
var localityAdvertiseHosts localityList
var sqlAuditLogDir log.DirName
var startBackground bool

// initPreFlagsDefaults initializes the values of the global variables
// defined above.
func initPreFlagsDefaults() {
	initPreFlagsCertDefaults()

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

	if err := sqlAuditLogDir.Set(""); err != nil {
		panic(err)
	}

	startBackground = false
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
		if err := extraClientFlagInit(); err != nil {
			return err
		}
		return setDefaultStderrVerbosity(cmd, log.Severity_WARNING)
	})

	// Add a pre-run command for `start` and `start-single-node`, as well as the
	// multi-tenancy related commands that start long-running servers.
	allStartCmds := append([]*cobra.Command(nil), StartCmds...)
	allStartCmds = append(allStartCmds, mtStartSQLCmd)
	for _, cmd := range allStartCmds {
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
		if strings.HasPrefix(flag.Name, "datadriven-") {
			// Same as httptest, but for the datadriven package.
			flag.Hidden = true
		}
		switch flag.Name {
		case logflags.DeprecatedLogFilesCombinedMaxSizeName:
			flag.Deprecated = "use --" + logflags.LogFilesCombinedMaxSizeName + " instead"
			fallthrough
		case logflags.ShowLogsName, // test-only flag
			logflags.RedactableLogsName: // support-only flag
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
		varFlag(f, addrSetter{&startCtx.serverListenAddr, &serverListenPort}, cliflags.ListenAddr)
		varFlag(f, addrSetter{&serverAdvertiseAddr, &serverAdvertisePort}, cliflags.AdvertiseAddr)
		varFlag(f, addrSetter{&serverSQLAddr, &serverSQLPort}, cliflags.ListenSQLAddr)
		varFlag(f, addrSetter{&serverSQLAdvertiseAddr, &serverSQLAdvertisePort}, cliflags.SQLAdvertiseAddr)
		varFlag(f, addrSetter{&serverHTTPAddr, &serverHTTPPort}, cliflags.ListenHTTPAddr)
		stringFlag(f, &serverSocketDir, cliflags.SocketDir)
		boolFlag(f, &startCtx.unencryptedLocalhostHTTP, cliflags.UnencryptedLocalhostHTTP)

		// The following flag is planned to become non-experimental in 21.1.
		boolFlag(f, &serverCfg.AcceptSQLWithoutTLS, cliflags.AcceptSQLWithoutTLS)
		_ = f.MarkHidden(cliflags.AcceptSQLWithoutTLS.Name)

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

		// More server flags.

		varFlag(f, &localityAdvertiseHosts, cliflags.LocalityAdvertiseAddr)

		stringFlag(f, &serverCfg.Attrs, cliflags.Attrs)
		varFlag(f, &serverCfg.Locality, cliflags.Locality)

		varFlag(f, &serverCfg.Stores, cliflags.Store)
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
		boolFlag(f, &serverCfg.ExternalIODirConfig.DisableImplicitCredentials, cliflags.ExternalIODisableImplicitCredentials)

		// Certificates directory. Use a server-specific flag and value to ignore environment
		// variables, but share the same default.
		stringFlag(f, &startCtx.serverSSLCertsDir, cliflags.ServerCertsDir)

		// Certificate principal map.
		stringSliceFlag(f, &startCtx.serverCertPrincipalMap, cliflags.CertPrincipalMap)

		// Cluster joining flags. We need to enable this both for 'start'
		// and 'start-single-node' although the latter does not support
		// --join, because it delegates its logic to that of 'start', and
		// 'start' will check that the flag is properly defined.
		varFlag(f, &serverCfg.JoinList, cliflags.Join)
		boolFlag(f, &serverCfg.JoinPreferSRVRecords, cliflags.JoinPreferSRVRecords)
		varFlag(f, clusterNameSetter{&baseCfg.ClusterName}, cliflags.ClusterName)
		boolFlag(f, &baseCfg.DisableClusterNameVerification, cliflags.DisableClusterNameVerification)
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
		varFlag(f, cacheSizeValue, cliflags.Cache)
		varFlag(f, sqlSizeValue, cliflags.SQLMem)
		// N.B. diskTempStorageSizeValue.ResolvePercentage() will be called after
		// the stores flag has been parsed and the storage device that a percentage
		// refers to becomes known.
		varFlag(f, diskTempStorageSizeValue, cliflags.SQLTempStorage)
		stringFlag(f, &startCtx.tempDir, cliflags.TempDir)
		stringFlag(f, &startCtx.externalIODir, cliflags.ExternalIODir)

		varFlag(f, serverCfg.AuditLogDirName, cliflags.SQLAuditLogDirName)

		if backgroundFlagDefined {
			boolFlag(f, &startBackground, cliflags.Background)
		}
	}

	// Flags that apply to commands that start servers.
	serverCmds := append(StartCmds, demoCmd)
	serverCmds = append(serverCmds, demoCmd.Commands()...)
	for _, cmd := range serverCmds {
		f := cmd.Flags()
		varFlag(f, &startCtx.logDir, cliflags.LogDir)
		varFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFilesCombinedMaxSizeName)).Value,
			cliflags.LogDirMaxSize)
		varFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFileMaxSizeName)).Value,
			cliflags.LogFileMaxSize)
		varFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFileVerbosityThresholdName)).Value,
			cliflags.LogFileVerbosity)

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
		stringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir)
		// All certs commands get the certificate principal map.
		stringSliceFlag(f, &cliCtx.certPrincipalMap, cliflags.CertPrincipalMap)
	}

	for _, cmd := range []*cobra.Command{
		createCACertCmd,
		createClientCACertCmd,
		mtCreateTenantClientCACertCmd,
	} {
		f := cmd.Flags()
		// CA certificates have a longer expiration time.
		durationFlag(f, &caCertificateLifetime, cliflags.CertificateLifetime)
		// The CA key can be re-used if it exists.
		boolFlag(f, &allowCAKeyReuse, cliflags.AllowCAKeyReuse)
	}

	for _, cmd := range []*cobra.Command{
		createNodeCertCmd,
		createClientCertCmd,
		mtCreateTenantClientCertCmd,
	} {
		f := cmd.Flags()
		durationFlag(f, &certificateLifetime, cliflags.CertificateLifetime)
	}

	// The remaining flags are shared between all cert-generating functions.
	for _, cmd := range []*cobra.Command{
		createCACertCmd,
		createClientCACertCmd,
		createNodeCertCmd,
		createClientCertCmd,
		mtCreateTenantClientCACertCmd,
		mtCreateTenantClientCertCmd,
	} {
		f := cmd.Flags()
		stringFlag(f, &baseCfg.SSLCAKey, cliflags.CAKey)
		intFlag(f, &keySize, cliflags.KeySize)
		boolFlag(f, &overwriteFiles, cliflags.OverwriteFiles)
	}
	// PKCS8 key format is only available for the client cert command.
	boolFlag(createClientCertCmd.Flags(), &generatePKCS8Key, cliflags.GeneratePKCS8Key)

	// The certs dir is given to all clientCmds below, but the following are not clientCmds.
	for _, cmd := range []*cobra.Command{
		mtCreateTenantClientCACertCmd,
		mtCreateTenantClientCertCmd,
	} {
		f := cmd.Flags()
		// Certificate flags.
		stringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir)
	}

	clientCmds := []*cobra.Command{
		debugGossipValuesCmd,
		debugTimeSeriesDumpCmd,
		debugZipCmd,
		doctorClusterCmd,
		dumpCmd,
		genHAProxyCmd,
		initCmd,
		quitCmd,
		sqlShellCmd,
		/* StartCmds are covered above */
	}
	clientCmds = append(clientCmds, authCmds...)
	clientCmds = append(clientCmds, nodeCmds...)
	clientCmds = append(clientCmds, systemBenchCmds...)
	clientCmds = append(clientCmds, nodeLocalCmds...)
	clientCmds = append(clientCmds, userFileCmds...)
	clientCmds = append(clientCmds, stmtDiagCmds...)
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

	// Auth commands.
	{
		f := loginCmd.Flags()
		durationFlag(f, &authCtx.validityPeriod, cliflags.AuthTokenValidityPeriod)
		boolFlag(f, &authCtx.onlyCookie, cliflags.OnlyCookie)
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

	// HDD Bench command.
	{
		f := seqWriteBench.Flags()
		varFlag(f, humanizeutil.NewBytesValue(&systemBenchCtx.writeSize), cliflags.WriteSize)
		varFlag(f, humanizeutil.NewBytesValue(&systemBenchCtx.syncInterval), cliflags.SyncInterval)
	}

	// Network Bench command.
	{
		f := networkBench.Flags()
		boolFlag(f, &networkBenchCtx.server, cliflags.BenchServer)
		intFlag(f, &networkBenchCtx.port, cliflags.BenchPort)
		stringSliceFlag(f, &networkBenchCtx.addresses, cliflags.BenchAddresses)
		boolFlag(f, &networkBenchCtx.latency, cliflags.BenchLatency)
	}

	// Bench command.
	{
		for _, cmd := range systemBenchCmds {
			f := cmd.Flags()
			intFlag(f, &systemBenchCtx.concurrency, cliflags.BenchConcurrency)
			durationFlag(f, &systemBenchCtx.duration, cliflags.BenchDuration)
			stringFlag(f, &systemBenchCtx.tempDir, cliflags.TempDir)
		}
	}

	// Zip command.
	{
		f := debugZipCmd.Flags()
		varFlag(f, &zipCtx.nodes.inclusive, cliflags.ZipNodes)
		varFlag(f, &zipCtx.nodes.exclusive, cliflags.ZipExcludeNodes)
		boolFlag(f, &zipCtx.redactLogs, cliflags.ZipRedactLogs)
		durationFlag(f, &zipCtx.cpuProfDuration, cliflags.ZipCPUProfileDuration)
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
	}

	// SQL and demo commands.
	for _, cmd := range append([]*cobra.Command{sqlShellCmd, demoCmd}, demoCmd.Commands()...) {
		f := cmd.Flags()
		varFlag(f, &sqlCtx.setStmts, cliflags.Set)
		varFlag(f, &sqlCtx.execStmts, cliflags.Execute)
		durationFlag(f, &sqlCtx.repeatDelay, cliflags.Watch)
		boolFlag(f, &sqlCtx.safeUpdates, cliflags.SafeUpdates)
		boolFlag(f, &sqlCtx.debugMode, cliflags.CliDebugMode)
	}

	varFlag(dumpCmd.Flags(), &dumpCtx.dumpMode, cliflags.DumpMode)
	stringFlag(dumpCmd.Flags(), &dumpCtx.asOf, cliflags.DumpTime)
	boolFlag(dumpCmd.Flags(), &dumpCtx.dumpAll, cliflags.DumpAll)

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{
		sqlShellCmd,
		dumpCmd,
		demoCmd,
		doctorClusterCmd,
		lsNodesCmd,
		statusNodeCmd,
	}
	sqlCmds = append(sqlCmds, authCmds...)
	sqlCmds = append(sqlCmds, demoCmd.Commands()...)
	sqlCmds = append(sqlCmds, stmtDiagCmds...)
	sqlCmds = append(sqlCmds, nodeLocalCmds...)
	sqlCmds = append(sqlCmds, userFileCmds...)
	for _, cmd := range sqlCmds {
		f := cmd.Flags()
		// The --echo-sql flag is special: it is a marker for CLI tests to
		// recognize SQL-only commands. If/when adding this flag to non-SQL
		// commands, ensure the isSQLCommand() predicate is updated accordingly.
		boolFlag(f, &sqlCtx.echo, cliflags.EchoSQL)

		if cmd != demoCmd {
			varFlag(f, urlParser{cmd, &cliCtx, false /* strictSSL */}, cliflags.URL)
			stringFlag(f, &cliCtx.sqlConnUser, cliflags.User)

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
		}

		if cmd == sqlShellCmd {
			stringFlag(f, &cliCtx.sqlConnDBName, cliflags.Database)
		}
	}

	// Make the non-SQL client commands also recognize --url in strict SSL mode
	// and ensure they can connect to clusters that use a cluster-name.
	for _, cmd := range clientCmds {
		if f := flagSetForCmd(cmd).Lookup(cliflags.URL.Name); f != nil {
			// --url already registered above, nothing to do.
			continue
		}
		f := cmd.PersistentFlags()
		varFlag(f, urlParser{cmd, &cliCtx, true /* strictSSL */}, cliflags.URL)
		varFlag(f, clusterNameSetter{&baseCfg.ClusterName}, cliflags.ClusterName)
		boolFlag(f, &baseCfg.DisableClusterNameVerification, cliflags.DisableClusterNameVerification)
	}

	// Commands that print tables.
	tableOutputCommands := append(
		[]*cobra.Command{sqlShellCmd, genSettingsListCmd, demoCmd, debugTimeSeriesDumpCmd},
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
		varFlag(f, &cliCtx.tableDisplayFormat, cliflags.TableDisplayFormat)
	}

	// demo command.
	{
		// We use the persistent flag set so that the flags apply to every
		// workload sub-command. This enables e.g.
		// ./cockroach demo movr --nodes=3.
		f := demoCmd.PersistentFlags()

		intFlag(f, &demoCtx.nodes, cliflags.DemoNodes)
		boolFlag(f, &demoCtx.runWorkload, cliflags.RunDemoWorkload)
		varFlag(f, &demoCtx.localities, cliflags.DemoNodeLocality)
		boolFlag(f, &demoCtx.geoPartitionedReplicas, cliflags.DemoGeoPartitionedReplicas)
		varFlag(f, demoNodeSQLMemSizeValue, cliflags.DemoNodeSQLMemSize)
		varFlag(f, demoNodeCacheSizeValue, cliflags.DemoNodeCacheSize)
		boolFlag(f, &demoCtx.insecure, cliflags.ClientInsecure)
		// NB: Insecure for `cockroach demo` is deprecated. See #53404.
		_ = f.MarkDeprecated(cliflags.ServerInsecure.Name,
			"to start a test server without any security, run start-single-node --insecure\n"+
				"For details, see: "+unimplemented.MakeURL(53404))

		boolFlag(f, &demoCtx.disableLicenseAcquisition, cliflags.DemoNoLicense)
		// Mark the --global flag as hidden until we investigate it more.
		boolFlag(f, &demoCtx.simulateLatency, cliflags.Global)
		_ = f.MarkHidden(cliflags.Global.Name)
		// The --empty flag is only valid for the top level demo command,
		// so we use the regular flag set.
		boolFlag(demoCmd.Flags(), &demoCtx.useEmptyDatabase, cliflags.UseEmptyDatabase)
		// We also support overriding the GEOS library path for 'demo'.
		// Even though the demoCtx uses mostly different configuration
		// variables from startCtx, this is one case where we afford
		// sharing a variable between both.
		stringFlag(f, &startCtx.geoLibsDir, cliflags.GeoLibsDir)
	}

	// statement-diag command.
	{
		boolFlag(stmtDiagDeleteCmd.Flags(), &stmtDiagCtx.all, cliflags.StmtDiagDeleteAll)
		boolFlag(stmtDiagCancelCmd.Flags(), &stmtDiagCtx.all, cliflags.StmtDiagCancelAll)
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

	// Multi-tenancy commands.
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

		stringFlag(f, &startCtx.geoLibsDir, cliflags.GeoLibsDir)

		stringSliceFlag(f, &serverCfg.SQLConfig.TenantKVAddrs, cliflags.KVAddrs)
		varFlag(f, &startCtx.logDir, cliflags.LogDir)
		varFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFilesCombinedMaxSizeName)).Value,
			cliflags.LogDirMaxSize)
		varFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFileMaxSizeName)).Value,
			cliflags.LogFileMaxSize)
		varFlag(f,
			pflag.PFlagFromGoFlag(flag.Lookup(logflags.LogFileVerbosityThresholdName)).Value,
			cliflags.LogFileVerbosity)
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
func processEnvVarDefaults() error {
	for _, d := range envVarDefaults {
		f := d.flagSet.Lookup(d.flagName)
		if f == nil {
			panic(errors.AssertionFailedf("unknown flag: %s", d.flagName))
		}
		var err error
		if url, ok := f.Value.(urlParser); ok {
			// URLs are a special case: they can emit a warning if there's
			// excess configuration for certain commands.
			// Since the env-var initialization is ran for all commands
			// all the time, regardless of which particular command is
			// currently active, we want to silence this warning here.
			//
			// TODO(knz): rework this code to only pull env var values
			// for the current command.
			err = url.setInternal(d.envValue, false /* warn */)
		} else {
			err = d.flagSet.Set(d.flagName, d.envValue)
		}
		if err != nil {
			return errors.Wrapf(err, "setting --%s from %s", d.flagName, d.envVar)
		}
	}
	return nil
}

// envVarDefault describes a delayed default initialization of the
// setting covered by a flag from the value of an environment
// variable.
type envVarDefault struct {
	envVar   string
	envValue string
	flagName string
	flagSet  *pflag.FlagSet
}

// envVarDefaults records the initializations from environment variables
// for processing at the end of initialization, before flag parsing.
var envVarDefaults []envVarDefault

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
	envVarDefaults = append(envVarDefaults, envVarDefault{
		envVar:   flagInfo.EnvVar,
		envValue: value,
		flagName: flagInfo.Name,
		flagSet:  f,
	})
}

// extraServerFlagInit configures the server.Config based on the command-line flags.
// It is only called when the command being ran is one of the start commands.
func extraServerFlagInit(cmd *cobra.Command) error {
	if err := security.SetCertPrincipalMap(startCtx.serverCertPrincipalMap); err != nil {
		return err
	}
	serverCfg.User = security.NodeUser
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

func extraClientFlagInit() error {
	if err := security.SetCertPrincipalMap(cliCtx.certPrincipalMap); err != nil {
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
	if sqlCtx.debugMode {
		sqlCtx.echo = true
	}
	return nil
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

// VarFlag is exported for use in package cliccl.
var VarFlag = varFlag
