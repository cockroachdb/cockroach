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
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clientflags"
	"github.com/cockroachdb/cockroach/pkg/cli/clienturl"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
//   - it would be a programming error to access these variables directly
//     outside of this file (flags.go)
//   - the underlying context parameters must receive defaults in
//     initCLIDefaults() even when they are otherwise overridden by the
//     flags logic, because some tests to not use the flag logic at all.
var serverListenPort, serverSocketDir string
var serverAdvertiseAddr, serverAdvertisePort string
var serverSQLAddr, serverSQLPort string
var serverSQLAdvertiseAddr, serverSQLAdvertisePort string
var serverHTTPAddr, serverHTTPPort string
var serverHTTPAdvertiseAddr, serverHTTPAdvertisePort string
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

	serverHTTPAdvertiseAddr = ""
	// We do not set `base.DefaultHTTPPort` on the advertise flag because
	// we want to override it with the `serverHTTPPort` if it's unset by
	// the user.
	serverHTTPAdvertisePort = ""

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

// clusterNameSetter wraps the cluster name variable
// and verifies its format during configuration.
type clusterNameSetter struct {
	clusterName *string
}

// String implements the pflag.Value interface.
func (a clusterNameSetter) String() string { return *a.clusterName }

// Type implements the pflag.Value interface.
func (a clusterNameSetter) Type() string { return "<identifier>" }

// tenantIDSetter wraps a list of roachpb.TenantIDs and enables setting them via a command-line flag.
type tenantIDSetter struct {
	tenantIDs *[]roachpb.TenantID
}

// String implements the pflag.Value interface.
func (t tenantIDSetter) String() string {
	var tenantString strings.Builder
	separator := ""
	for _, tID := range *t.tenantIDs {
		tenantString.WriteString(separator)
		tenantString.WriteString(strconv.FormatUint(tID.ToUint64(), 10))
		separator = ","
	}
	return tenantString.String()
}

// Type implements the pflag.Value interface.
func (t tenantIDSetter) Type() string { return "<[]TenantID>" }

// Set implements the pflag.Value interface.
func (t tenantIDSetter) Set(v string) error {
	// Reset tenantIDs slice as it is initialized to contain the system tenant ID
	// by default.
	*t.tenantIDs = []roachpb.TenantID{}
	tenantScopes := strings.Split(v, "," /* separator */)
	for _, tenantScope := range tenantScopes {
		tenantID, err := roachpb.TenantIDFromString(tenantScope)
		if err != nil {
			return err
		}
		*t.tenantIDs = append(*t.tenantIDs, tenantID)
	}
	return nil
}

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
	showRangeKeys
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
	case showRangeKeys:
		return "rangekeys"
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
	case "rangekeys":
		*f = showRangeKeys
	case "txns":
		*f = showTxns
	default:
		return errors.Newf("invalid key filter type '%s'", v)
	}
	return nil
}

const backgroundEnvVar = "COCKROACH_BACKGROUND_RESTART"

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
		if flag.Name == logflags.ShowLogsName ||
			flag.Name == logflags.TestLogConfigName ||
			flag.Name == serverutils.TenantModeFlagName {
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
		cliflagcfg.VarFlag(pf, &stringValue{settableString: &cliCtx.logConfigInput}, cliflags.Log)
		cliflagcfg.VarFlag(pf, &fileContentsValue{settableString: &cliCtx.logConfigInput, fileName: "<unset>"}, cliflags.LogConfigFile)
		cliflagcfg.StringSliceFlag(pf, &cliCtx.logConfigVars, cliflags.LogConfigVars)

		// Discrete convenience overrides.
		cliflagcfg.VarFlag(pf, &cliCtx.logOverrides.stderrThreshold, cliflags.StderrThresholdOverride)
		// This flag can also be specified without an explicit argument.
		pf.Lookup(cliflags.StderrThresholdOverride.Name).NoOptDefVal = "DEFAULT"

		cliflagcfg.VarFlag(pf, &cliCtx.logOverrides.stderrNoColor, cliflags.StderrNoColorOverride)
		_ = pf.MarkHidden(cliflags.StderrNoColorOverride.Name)
		cliflagcfg.VarFlag(pf, &stringValue{&cliCtx.logOverrides.logDir}, cliflags.LogDirOverride)

		cliflagcfg.VarFlag(pf, cliCtx.logOverrides.fileMaxSizeVal, cliflags.LogFileMaxSizeOverride)
		_ = pf.MarkHidden(cliflags.LogFileMaxSizeOverride.Name)

		cliflagcfg.VarFlag(pf, cliCtx.logOverrides.maxGroupSizeVal, cliflags.LogGroupMaxSizeOverride)
		_ = pf.MarkHidden(cliflags.LogGroupMaxSizeOverride.Name)

		cliflagcfg.VarFlag(pf, &cliCtx.logOverrides.fileThreshold, cliflags.FileThresholdOverride)
		_ = pf.MarkHidden(cliflags.FileThresholdOverride.Name)

		cliflagcfg.VarFlag(pf, &cliCtx.logOverrides.redactableLogs, cliflags.RedactableLogsOverride)

		cliflagcfg.VarFlag(pf, &stringValue{&cliCtx.logOverrides.sqlAuditLogDir}, cliflags.SQLAuditLogDirOverride)
		_ = pf.MarkHidden(cliflags.SQLAuditLogDirOverride.Name)
	}

	// Remember we are starting in the background as the `start` command will
	// avoid printing some messages to standard output in that case.
	_, startCtx.inBackground = envutil.EnvString(backgroundEnvVar, 1)

	// Flags common to KV-only servers.
	for _, cmd := range append(StartCmds, connectInitCmd, connectJoinCmd) {
		f := cmd.Flags()

		// Cluster joining flags. We need to enable this both for 'start'
		// and 'start-single-node' although the latter does not support
		// --join, because it delegates its logic to that of 'start', and
		// 'start' will check that the flag is properly defined.
		cliflagcfg.VarFlag(f, &serverCfg.JoinList, cliflags.Join)
		cliflagcfg.BoolFlag(f, &serverCfg.JoinPreferSRVRecords, cliflags.JoinPreferSRVRecords)

		if cmd != connectJoinCmd {
			// The initialization token and expected peers. For 'start' commands this is optional.
			cliflagcfg.StringFlag(f, &startCtx.initToken, cliflags.InitToken)
			cliflagcfg.IntFlag(f, &startCtx.numExpectedNodes, cliflags.NumExpectedInitialNodes)
			cliflagcfg.BoolFlag(f, &startCtx.genCertsForSingleNode, cliflags.SingleNode)
		}

		if cmd == startSingleNodeCmd {
			// Even though all server flags are supported for
			// 'start-single-node', we intend that command to be used by
			// beginners / developers running on a single machine. To
			// enhance the UX, we hide the flags since they are not directly
			// relevant when running a single node.
			_ = f.MarkHidden(cliflags.Join.Name)
			_ = f.MarkHidden(cliflags.JoinPreferSRVRecords.Name)
			_ = f.MarkHidden(cliflags.InitToken.Name)
		}

		// Node attributes.
		//
		// TODO(knz): do we want SQL-only servers to have node-level
		// attributes too? Would this be useful for e.g. SQL query
		// planning?
		if cmd != connectInitCmd && cmd != connectJoinCmd {
			cliflagcfg.StringFlag(f, &serverCfg.Attrs, cliflags.Attrs)
		}
	}

	// Flags common to the start commands, the connect command, and the node join
	// command.
	for _, cmd := range append(serverCmds, connectInitCmd, connectJoinCmd) {
		f := cmd.Flags()

		// Use a separate variable to store the value of ServerInsecure.
		// We share the default with the ClientInsecure flag.
		//
		// NB: Insecure is deprecated. See #53404.
		cliflagcfg.BoolFlag(f, &startCtx.serverInsecure, cliflags.ServerInsecure)

		// NB: serverInsecure populates baseCfg.{Insecure,SSLCertsDir} in this the following method
		// (which is a PreRun for this command):
		_ = extraServerFlagInit // guru assignment

		// NB: the address flags also gets PreRun treatment via extraServerFlagInit to populate BaseCfg.SQLAddr.
		cliflagcfg.VarFlag(f, addr.NewAddrSetter(&startCtx.serverListenAddr, &serverListenPort), cliflags.ListenAddr)
		cliflagcfg.VarFlag(f, addr.NewAddrSetter(&serverAdvertiseAddr, &serverAdvertisePort), cliflags.AdvertiseAddr)
		cliflagcfg.VarFlag(f, addr.NewAddrSetter(&serverSQLAddr, &serverSQLPort), cliflags.ListenSQLAddr)
		cliflagcfg.VarFlag(f, addr.NewAddrSetter(&serverSQLAdvertiseAddr, &serverSQLAdvertisePort), cliflags.SQLAdvertiseAddr)
		cliflagcfg.VarFlag(f, addr.NewAddrSetter(&serverHTTPAddr, &serverHTTPPort), cliflags.ListenHTTPAddr)
		cliflagcfg.VarFlag(f, addr.NewAddrSetter(&serverHTTPAdvertiseAddr, &serverHTTPAdvertisePort), cliflags.HTTPAdvertiseAddr)

		// Certificates directory. Use a server-specific flag and value to ignore environment
		// variables, but share the same default.
		cliflagcfg.StringFlag(f, &startCtx.serverSSLCertsDir, cliflags.ServerCertsDir)

		if cmd == startSingleNodeCmd {
			// Even though all server flags are supported for
			// 'start-single-node', we intend that command to be used by
			// beginners / developers running on a single machine. To
			// enhance the UX, we hide the flags since they are not directly
			// relevant when running a single node.
			_ = f.MarkHidden(cliflags.AdvertiseAddr.Name)
			_ = f.MarkHidden(cliflags.SQLAdvertiseAddr.Name)
			_ = f.MarkHidden(cliflags.HTTPAdvertiseAddr.Name)
		}

		if cmd == startCmd || cmd == startSingleNodeCmd {
			// Backward-compatibility flags.

			// These are deprecated but until we have qualitatively new
			// functionality in the flags above, there is no need to nudge the
			// user away from them with a deprecation warning. So we keep
			// them, but hidden from docs so that they don't appear as
			// redundant with the main flags.
			cliflagcfg.VarFlag(f, aliasStrVar{&startCtx.serverListenAddr}, cliflags.ServerHost)
			_ = f.MarkHidden(cliflags.ServerHost.Name)
			cliflagcfg.VarFlag(f, aliasStrVar{&serverListenPort}, cliflags.ServerPort)
			_ = f.MarkHidden(cliflags.ServerPort.Name)

			cliflagcfg.VarFlag(f, aliasStrVar{&serverAdvertiseAddr}, cliflags.AdvertiseHost)
			_ = f.MarkHidden(cliflags.AdvertiseHost.Name)
			cliflagcfg.VarFlag(f, aliasStrVar{&serverAdvertisePort}, cliflags.AdvertisePort)
			_ = f.MarkHidden(cliflags.AdvertisePort.Name)

			cliflagcfg.VarFlag(f, aliasStrVar{&serverHTTPAddr}, cliflags.ListenHTTPAddrAlias)
			_ = f.MarkHidden(cliflags.ListenHTTPAddrAlias.Name)
			cliflagcfg.VarFlag(f, aliasStrVar{&serverHTTPPort}, cliflags.ListenHTTPPort)
			_ = f.MarkHidden(cliflags.ListenHTTPPort.Name)
		}

		if cmd != connectInitCmd && cmd != connectJoinCmd {
			cliflagcfg.StringFlag(f, &serverSocketDir, cliflags.SocketDir)
			cliflagcfg.BoolFlag(f, &startCtx.unencryptedLocalhostHTTP, cliflags.UnencryptedLocalhostHTTP)

			// The following flag is planned to become non-experimental in 21.1.
			cliflagcfg.BoolFlag(f, &serverCfg.AcceptSQLWithoutTLS, cliflags.AcceptSQLWithoutTLS)
			_ = f.MarkHidden(cliflags.AcceptSQLWithoutTLS.Name)

			// More server flags.

			if cmd != mtStartSQLCmd {
				// TODO(knz): SQL-only servers should probably also support per-locality server
				// addresses, for multi-region support.
				// See: https://github.com/cockroachdb/cockroach/issues/90172
				cliflagcfg.VarFlag(f, &localityAdvertiseHosts, cliflags.LocalityAdvertiseAddr)
			}

			cliflagcfg.VarFlag(f, &serverCfg.Locality, cliflags.Locality)

			cliflagcfg.VarFlag(f, &storeSpecs, cliflags.Store)
			cliflagcfg.VarFlag(f, &serverCfg.StorageEngine, cliflags.StorageEngine)
			cliflagcfg.VarFlag(f, &serverCfg.MaxOffset, cliflags.MaxOffset)
			cliflagcfg.StringFlag(f, &serverCfg.ClockDevicePath, cliflags.ClockDevice)

			cliflagcfg.StringFlag(f, &startCtx.listeningURLFile, cliflags.ListeningURLFile)

			cliflagcfg.StringFlag(f, &startCtx.pidFile, cliflags.PIDFile)
			cliflagcfg.StringFlag(f, &startCtx.geoLibsDir, cliflags.GeoLibsDir)

			// Enable/disable various external storage endpoints.
			cliflagcfg.BoolFlag(f, &serverCfg.ExternalIODirConfig.DisableHTTP, cliflags.ExternalIODisableHTTP)
			cliflagcfg.BoolFlag(f, &serverCfg.ExternalIODirConfig.DisableOutbound, cliflags.ExternalIODisabled)
			cliflagcfg.BoolFlag(f, &serverCfg.ExternalIODirConfig.DisableImplicitCredentials, cliflags.ExternalIODisableImplicitCredentials)
			cliflagcfg.BoolFlag(f, &serverCfg.ExternalIODirConfig.EnableNonAdminImplicitAndArbitraryOutbound, cliflags.ExternalIOEnableNonAdminImplicitAndArbitraryOutbound)

			// Certificate principal map.
			cliflagcfg.StringSliceFlag(f, &startCtx.serverCertPrincipalMap, cliflags.CertPrincipalMap)

			// Cluster name verification.
			cliflagcfg.VarFlag(f, clusterNameSetter{&baseCfg.ClusterName}, cliflags.ClusterName)
			cliflagcfg.BoolFlag(f, &baseCfg.DisableClusterNameVerification, cliflags.DisableClusterNameVerification)
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
			cliflagcfg.VarFlag(f, &startCtx.cacheSizeValue, cliflags.Cache)
			cliflagcfg.VarFlag(f, &startCtx.sqlSizeValue, cliflags.SQLMem)
			cliflagcfg.VarFlag(f, &startCtx.tsdbSizeValue, cliflags.TSDBMem)
			// N.B. diskTempStorageSizeValue.ResolvePercentage() will be called after
			// the stores flag has been parsed and the storage device that a percentage
			// refers to becomes known.
			cliflagcfg.VarFlag(f, &startCtx.diskTempStorageSizeValue, cliflags.SQLTempStorage)
			cliflagcfg.StringFlag(f, &startCtx.tempDir, cliflags.TempDir)
			cliflagcfg.StringFlag(f, &startCtx.externalIODir, cliflags.ExternalIODir)
		}

		if backgroundFlagDefined {
			cliflagcfg.BoolFlag(f, &startBackground, cliflags.Background)
		}

		// TODO(knz): Remove this port offset mechanism once we implement
		// a shared listener. See: https://github.com/cockroachdb/cockroach/issues/84585
		cliflagcfg.IntFlag(f, &baseCfg.SecondaryTenantPortOffset, cliflags.SecondaryTenantPortOffset)
		_ = f.MarkHidden(cliflags.SecondaryTenantPortOffset.Name)
	}

	// Multi-tenancy start-sql command flags.
	{
		f := mtStartSQLCmd.Flags()
		cliflagcfg.VarFlag(f, &tenantIDWrapper{&serverCfg.SQLConfig.TenantID}, cliflags.TenantID)
		cliflagcfg.StringSliceFlag(f, &serverCfg.SQLConfig.TenantKVAddrs, cliflags.KVAddrs)
	}

	// Flags that apply to commands that start servers.
	telemetryEnabledCmds := append(serverCmds, demoCmd, statementBundleRecreateCmd)
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
		cliflagcfg.StringFlag(f, &certCtx.certsDir, cliflags.CertsDir)

		// All certs command want to map CNs to SQL principals.
		cliflagcfg.StringSliceFlag(f, &certCtx.certPrincipalMap, cliflags.CertPrincipalMap)

		if cmd == listCertsCmd {
			// The 'list' subcommand does not write to files and thus does
			// not need the arguments below.
			continue
		}

		cliflagcfg.StringFlag(f, &certCtx.caKey, cliflags.CAKey)
		cliflagcfg.IntFlag(f, &certCtx.keySize, cliflags.KeySize)
		cliflagcfg.BoolFlag(f, &certCtx.overwriteFiles, cliflags.OverwriteFiles)
		cliflagcfg.VarFlag(f, &tenantIDSetter{tenantIDs: &certCtx.tenantScope}, cliflags.TenantScope)

		if strings.HasSuffix(cmd.Name(), "-ca") {
			// CA-only commands.

			// CA certificates have a longer expiration time.
			cliflagcfg.DurationFlag(f, &certCtx.caCertificateLifetime, cliflags.CertificateLifetime)
			// The CA key can be re-used if it exists.
			cliflagcfg.BoolFlag(f, &certCtx.allowCAKeyReuse, cliflags.AllowCAKeyReuse)
		} else {
			// Non-CA commands.

			cliflagcfg.DurationFlag(f, &certCtx.certificateLifetime, cliflags.CertificateLifetime)
		}

		if cmd == createClientCertCmd {
			// PKCS8 key format is only available for the client cert command.
			cliflagcfg.BoolFlag(f, &certCtx.generatePKCS8Key, cliflags.GeneratePKCS8Key)
			cliflagcfg.BoolFlag(f, &certCtx.disableUsernameValidation, cliflags.DisableUsernameValidation)
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
	clientCmds = append(clientCmds, recoverCommands...)
	for _, cmd := range clientCmds {
		clientflags.AddBaseFlags(cmd, &cliCtx.clientOpts, &baseCfg.Insecure, &baseCfg.SSLCertsDir)

		// Certificate principal map.
		// TODO(knz): I think cert principal map is not needed for SQL clients. It might
		// not even be needed for RPC clients either any more.
		// This needs to be checked (and the flag removed if needed).
		f := cmd.PersistentFlags()
		cliflagcfg.StringSliceFlag(f, &cliCtx.certPrincipalMap, cliflags.CertPrincipalMap)
	}

	// convert-url is not really a client command. It just recognizes (some)
	// client flags.
	{
		f := convertURLCmd.PersistentFlags()
		cliflagcfg.StringFlag(f, &convertCtx.url, cliflags.URL)
	}

	// Auth commands.
	{
		f := loginCmd.Flags()
		cliflagcfg.DurationFlag(f, &authCtx.validityPeriod, cliflags.AuthTokenValidityPeriod)
		cliflagcfg.BoolFlag(f, &authCtx.onlyCookie, cliflags.OnlyCookie)
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
		cliflagcfg.DurationFlag(cmd.Flags(), &cliCtx.cmdTimeout, cliflags.Timeout)
	}

	// Node Status command.
	{
		f := statusNodeCmd.Flags()
		cliflagcfg.BoolFlag(f, &nodeCtx.statusShowRanges, cliflags.NodeRanges)
		cliflagcfg.BoolFlag(f, &nodeCtx.statusShowStats, cliflags.NodeStats)
		cliflagcfg.BoolFlag(f, &nodeCtx.statusShowAll, cliflags.NodeAll)
		cliflagcfg.BoolFlag(f, &nodeCtx.statusShowDecommission, cliflags.NodeDecommission)
	}

	// Zip command.
	{
		f := debugZipCmd.Flags()
		cliflagcfg.BoolFlag(f, &zipCtx.redactLogs, cliflags.ZipRedactLogs)
		_ = f.MarkDeprecated(cliflags.ZipRedactLogs.Name, "use --"+cliflags.ZipRedact.Name+" instead")
		cliflagcfg.BoolFlag(f, &zipCtx.redact, cliflags.ZipRedact)
		cliflagcfg.DurationFlag(f, &zipCtx.cpuProfDuration, cliflags.ZipCPUProfileDuration)
		cliflagcfg.IntFlag(f, &zipCtx.concurrency, cliflags.ZipConcurrency)
	}
	// List-files + Zip commands.
	for _, cmd := range []*cobra.Command{debugZipCmd, debugListFilesCmd} {
		f := cmd.Flags()
		cliflagcfg.VarFlag(f, &zipCtx.nodes.inclusive, cliflags.ZipNodes)
		cliflagcfg.VarFlag(f, &zipCtx.nodes.exclusive, cliflags.ZipExcludeNodes)
		cliflagcfg.StringSliceFlag(f, &zipCtx.files.includePatterns, cliflags.ZipIncludedFiles)
		cliflagcfg.StringSliceFlag(f, &zipCtx.files.excludePatterns, cliflags.ZipExcludedFiles)
		cliflagcfg.VarFlag(f, &zipCtx.files.startTimestamp, cliflags.ZipFilesFrom)
		cliflagcfg.VarFlag(f, &zipCtx.files.endTimestamp, cliflags.ZipFilesUntil)
	}

	// Decommission command.
	cliflagcfg.VarFlag(decommissionNodeCmd.Flags(), &nodeCtx.nodeDecommissionWait, cliflags.Wait)

	// Decommission and recommission share --self.
	for _, cmd := range []*cobra.Command{decommissionNodeCmd, recommissionNodeCmd} {
		f := cmd.Flags()
		cliflagcfg.BoolFlag(f, &nodeCtx.nodeDecommissionSelf, cliflags.NodeDecommissionSelf)
	}

	// node drain command.
	{
		f := drainNodeCmd.Flags()
		cliflagcfg.DurationFlag(f, &drainCtx.drainWait, cliflags.DrainWait)
		cliflagcfg.BoolFlag(f, &drainCtx.nodeDrainSelf, cliflags.NodeDrainSelf)
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
		clientflags.AddSQLFlags(cmd, &cliCtx.clientOpts, sqlCtx,
			cmd == sqlShellCmd, /* isShell */
			cmd == demoCmd || cmd == statementBundleRecreateCmd, /* isDemo */
		)
	}

	// Make the non-SQL client commands also recognize --url in strict SSL mode
	// and ensure they can connect to clusters that use a cluster-name.
	for _, cmd := range clientCmds {
		if fl := cliflagcfg.FlagSetForCmd(cmd).Lookup(cliflags.URL.Name); fl != nil {
			// --url already registered above: this is a SQL client command.
			// The code below is not intended for it.
			continue
		}

		f := cmd.PersistentFlags()
		cliflagcfg.VarFlag(f, clienturl.NewURLParser(cmd, &cliCtx.clientOpts, true /* strictTLS */, func(format string, args ...interface{}) {
			fmt.Fprintf(stderr, format, args...)
		}), cliflags.URL)

		cliflagcfg.VarFlag(f, clusterNameSetter{&baseCfg.ClusterName}, cliflags.ClusterName)
		cliflagcfg.BoolFlag(f, &baseCfg.DisableClusterNameVerification, cliflags.DisableClusterNameVerification)
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
		cliflagcfg.VarFlag(f, &sqlExecCtx.TableDisplayFormat, cliflags.TableDisplayFormat)
	}

	// demo command.
	for _, cmd := range []*cobra.Command{demoCmd, statementBundleRecreateCmd} {
		// We use the persistent flag set so that the flags apply to every
		// workload sub-command. This enables e.g.
		// ./cockroach demo movr --nodes=3.
		f := cmd.PersistentFlags()

		cliflagcfg.IntFlag(f, &demoCtx.NumNodes, cliflags.DemoNodes)
		cliflagcfg.BoolFlag(f, &demoCtx.RunWorkload, cliflags.RunDemoWorkload)
		cliflagcfg.IntFlag(f, &demoCtx.ExpandSchema, cliflags.ExpandDemoSchema)
		cliflagcfg.StringFlag(f, &demoCtx.NameGenOptions, cliflags.DemoNameGenOpts)
		cliflagcfg.IntFlag(f, &demoCtx.WorkloadMaxQPS, cliflags.DemoWorkloadMaxQPS)
		cliflagcfg.VarFlag(f, &demoCtx.Localities, cliflags.DemoNodeLocality)
		cliflagcfg.BoolFlag(f, &demoCtx.GeoPartitionedReplicas, cliflags.DemoGeoPartitionedReplicas)
		cliflagcfg.VarFlag(f, &demoCtx.demoNodeSQLMemSizeValue, cliflags.DemoNodeSQLMemSize)
		cliflagcfg.VarFlag(f, &demoCtx.demoNodeCacheSizeValue, cliflags.DemoNodeCacheSize)
		// NB: Insecure for `cockroach demo` is deprecated. See #53404.
		cliflagcfg.BoolFlag(f, &demoCtx.Insecure, cliflags.ClientInsecure)

		cliflagcfg.BoolFlag(f, &demoCtx.disableEnterpriseFeatures, cliflags.DemoNoLicense)
		cliflagcfg.BoolFlag(f, &demoCtx.DefaultEnableRangefeeds, cliflags.DemoEnableRangefeeds)

		cliflagcfg.BoolFlag(f, &demoCtx.Multitenant, cliflags.DemoMultitenant)
		cliflagcfg.BoolFlag(f, &demoCtx.DisableServerController, cliflags.DemoDisableServerController)
		// TODO(knz): Currently the multitenant UX for 'demo' is not
		// satisfying for end-users. Let's not advertise it too much.
		_ = f.MarkHidden(cliflags.DemoMultitenant.Name)
		_ = f.MarkHidden(cliflags.DemoDisableServerController.Name)

		cliflagcfg.BoolFlag(f, &demoCtx.SimulateLatency, cliflags.Global)
		// We also support overriding the GEOS library path for 'demo'.
		// Even though the demoCtx uses mostly different configuration
		// variables from startCtx, this is one case where we afford
		// sharing a variable between both.
		cliflagcfg.StringFlag(f, &startCtx.geoLibsDir, cliflags.GeoLibsDir)

		cliflagcfg.IntFlag(f, &demoCtx.SQLPort, cliflags.DemoSQLPort)
		cliflagcfg.IntFlag(f, &demoCtx.HTTPPort, cliflags.DemoHTTPPort)
		cliflagcfg.StringFlag(f, &demoCtx.ListeningURLFile, cliflags.ListeningURLFile)
		cliflagcfg.StringFlag(f, &demoCtx.pidFile, cliflags.PIDFile)
	}

	{
		// The --empty flag is only valid for the top level demo command,
		// so we use the regular flag set.
		f := demoCmd.Flags()
		cliflagcfg.BoolFlag(f, &demoCtx.UseEmptyDatabase, cliflags.UseEmptyDatabase)

		// --no-example-database is an old name for --empty.
		cliflagcfg.BoolFlag(f, &demoCtx.UseEmptyDatabase, cliflags.NoExampleDatabase)
		_ = f.MarkHidden(cliflags.NoExampleDatabase.Name)
	}

	// statement-diag command.
	{
		cliflagcfg.BoolFlag(stmtDiagDeleteCmd.Flags(), &stmtDiagCtx.all, cliflags.StmtDiagDeleteAll)
		cliflagcfg.BoolFlag(stmtDiagCancelCmd.Flags(), &stmtDiagCtx.all, cliflags.StmtDiagCancelAll)
	}

	// import dump command.
	{
		d := importDumpFileCmd.Flags()
		cliflagcfg.BoolFlag(d, &importCtx.skipForeignKeys, cliflags.ImportSkipForeignKeys)
		cliflagcfg.IntFlag(d, &importCtx.maxRowSize, cliflags.ImportMaxRowSize)
		cliflagcfg.IntFlag(d, &importCtx.rowLimit, cliflags.ImportRowLimit)
		cliflagcfg.BoolFlag(d, &importCtx.ignoreUnsupported, cliflags.ImportIgnoreUnsupportedStatements)
		cliflagcfg.StringFlag(d, &importCtx.ignoreUnsupportedLog, cliflags.ImportLogIgnoredStatements)
		cliflagcfg.StringFlag(d, &cliCtx.clientOpts.Database, cliflags.Database)

		t := importDumpTableCmd.Flags()
		cliflagcfg.BoolFlag(t, &importCtx.skipForeignKeys, cliflags.ImportSkipForeignKeys)
		cliflagcfg.IntFlag(t, &importCtx.maxRowSize, cliflags.ImportMaxRowSize)
		cliflagcfg.IntFlag(t, &importCtx.rowLimit, cliflags.ImportRowLimit)
		cliflagcfg.BoolFlag(t, &importCtx.ignoreUnsupported, cliflags.ImportIgnoreUnsupportedStatements)
		cliflagcfg.StringFlag(t, &importCtx.ignoreUnsupportedLog, cliflags.ImportLogIgnoredStatements)
		cliflagcfg.StringFlag(t, &cliCtx.clientOpts.Database, cliflags.Database)
	}

	// sqlfmt command.
	{
		f := sqlfmtCmd.Flags()
		cliflagcfg.VarFlag(f, &sqlfmtCtx.execStmts, cliflags.Execute)
		cliflagcfg.IntFlag(f, &sqlfmtCtx.len, cliflags.SQLFmtLen)
		cliflagcfg.BoolFlag(f, &sqlfmtCtx.useSpaces, cliflags.SQLFmtSpaces)
		cliflagcfg.IntFlag(f, &sqlfmtCtx.tabWidth, cliflags.SQLFmtTabWidth)
		cliflagcfg.BoolFlag(f, &sqlfmtCtx.noSimplify, cliflags.SQLFmtNoSimplify)
		cliflagcfg.BoolFlag(f, &sqlfmtCtx.align, cliflags.SQLFmtAlign)
	}

	// version command.
	{
		f := versionCmd.Flags()
		cliflagcfg.BoolFlag(f, &cliCtx.showVersionUsingOnlyBuildTag, cliflags.BuildTag)
	}

	// Debug commands.
	{
		f := debugKeysCmd.Flags()
		cliflagcfg.VarFlag(f, (*mvccKey)(&debugCtx.startKey), cliflags.From)
		cliflagcfg.VarFlag(f, (*mvccKey)(&debugCtx.endKey), cliflags.To)
		cliflagcfg.IntFlag(f, &debugCtx.maxResults, cliflags.Limit)
		cliflagcfg.BoolFlag(f, &debugCtx.values, cliflags.Values)
		cliflagcfg.BoolFlag(f, &debugCtx.sizes, cliflags.Sizes)
		cliflagcfg.StringFlag(f, &debugCtx.decodeAsTableDesc, cliflags.DecodeAsTable)
		cliflagcfg.VarFlag(f, &debugCtx.keyTypes, cliflags.FilterKeys)
	}
	{
		f := debugCheckLogConfigCmd.Flags()
		cliflagcfg.VarFlag(f, &storeSpecs, cliflags.Store)
	}
	{
		f := debugRangeDataCmd.Flags()
		cliflagcfg.BoolFlag(f, &debugCtx.replicated, cliflags.Replicated)
		cliflagcfg.IntFlag(f, &debugCtx.maxResults, cliflags.Limit)
	}
	{
		f := debugGossipValuesCmd.Flags()
		cliflagcfg.StringFlag(f, &debugCtx.inputFile, cliflags.GossipInputFile)
		cliflagcfg.BoolFlag(f, &debugCtx.printSystemConfig, cliflags.PrintSystemConfig)
	}
	{
		f := debugBallastCmd.Flags()
		cliflagcfg.VarFlag(f, &debugCtx.ballastSize, cliflags.Size)
	}
	{
		// TODO(ayang): clean up so dir isn't passed to both pebble and --store
		f := DebugPebbleCmd.PersistentFlags()
		cliflagcfg.VarFlag(f, &storeSpecs, cliflags.Store)
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
				cliflagcfg.BoolFlag(f, &debugCtx.verbose, cliflags.Verbose)
			}
		}
	}

	// userfile upload command.
	{
		cliflagcfg.BoolFlag(userFileUploadCmd.Flags(), &userfileCtx.recursive, cliflags.Recursive)
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
	*w.tenID = roachpb.MustMakeTenantID(tenID)
	return nil
}

func (w *tenantIDWrapper) Type() string {
	return "number"
}

// extraServerFlagInit configures the server.Config based on the command-line flags.
// It is only called when the command being ran is one of the start commands.
func extraServerFlagInit(cmd *cobra.Command) error {
	if err := security.SetCertPrincipalMap(startCtx.serverCertPrincipalMap); err != nil {
		return err
	}
	serverCfg.User = username.NodeUserName()
	serverCfg.Insecure = startCtx.serverInsecure
	serverCfg.SSLCertsDir = startCtx.serverSSLCertsDir

	fs := cliflagcfg.FlagSetForCmd(cmd)

	// Helper for .Changed that is nil-aware as not all of the `cmd`s may have
	// all of the flags.
	changed := func(set *pflag.FlagSet, name string) bool {
		f := set.Lookup(name)
		return f != nil && f.Changed
	}

	if cmd == mtStartSQLCmd {
		if !changed(fs, cliflags.ListenAddr.Name) && changed(fs, cliflags.ListenSQLAddr.Name) {
			// A special affordance for backward-compatibility with previous
			// versions of CockroachDB.
			//
			// In those versions, the 'mt start-sql' command did not support
			// --listen-addr and instead --sql-addr was controlling both the
			// RPC and SQL ports together. To support this, we assume that
			// if the latter is set but the former is not, the user truly
			// wanted to control both.
			startCtx.serverListenAddr, serverListenPort = serverSQLAddr, serverSQLPort
			serverSQLAddr, serverSQLPort = "", ""
			fs.Lookup(cliflags.ListenSQLAddr.Name).Changed = false
		}
	}

	// Construct the main RPC listen address.
	serverCfg.Addr = net.JoinHostPort(startCtx.serverListenAddr, serverListenPort)

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
			socketName := ".s.PGSQL." + serverListenPort
			// On BSD, binding to a socket is limited to a path length of 104 characters
			// (including the NUL terminator). In glibc, this limit is 108 characters.
			// Otherwise, the bind operation fails with "invalid parameter".
			if len(serverSocketDir) >= 104-1-len(socketName) {
				return errors.WithHintf(
					errors.Newf("value of --%s is too long: %s", cliflags.SocketDir.Name, serverSocketDir),
					"The socket directory name must be shorter than %d characters.",
					104-1-len(socketName))
			}
			serverCfg.SocketFile = filepath.Join(serverSocketDir, socketName)
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
	serverCfg.SplitListenSQL = changed(fs, cliflags.ListenSQLAddr.Name)

	// Fill in the defaults for --advertise-sql-addr, if the flag exists on `cmd`.
	advHostSpecified := changed(fs, cliflags.AdvertiseAddr.Name) ||
		changed(fs, cliflags.AdvertiseHost.Name)
	advPortSpecified := changed(fs, cliflags.AdvertiseAddr.Name) ||
		changed(fs, cliflags.AdvertisePort.Name)
	if serverSQLAdvertiseAddr == "" {
		if advHostSpecified {
			serverSQLAdvertiseAddr = serverAdvertiseAddr
		} else {
			serverSQLAdvertiseAddr = serverSQLAddr
		}
	}
	if serverSQLAdvertisePort == "" {
		if advPortSpecified && !serverCfg.SplitListenSQL {
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

	if serverHTTPAdvertiseAddr == "" {
		if advHostSpecified || advPortSpecified {
			serverHTTPAdvertiseAddr = serverAdvertiseAddr
		} else {
			serverHTTPAdvertiseAddr = serverHTTPAddr
		}
	}
	if serverHTTPAdvertisePort == "" {
		// We do not include the `if advSpecified` clause to mirror the
		// logic above for `SQLAdvertiseAddr` which overrides the port from
		// `serverAdvertisePort` because that port is *never* correct here,
		// since it refers to SQL/gRPC connections.
		serverHTTPAdvertisePort = serverHTTPPort
	}
	serverCfg.HTTPAdvertiseAddr = net.JoinHostPort(serverHTTPAdvertiseAddr, serverHTTPAdvertisePort)

	// Fill the advertise port into the locality advertise addresses.
	for i, a := range localityAdvertiseHosts {
		host, port, err := addr.SplitHostPort(a.Address.AddressField, serverAdvertisePort)
		if err != nil {
			return err
		}
		localityAdvertiseHosts[i].Address.AddressField = net.JoinHostPort(host, port)
	}
	serverCfg.LocalityAddresses = localityAdvertiseHosts

	// Ensure that diagnostic reporting is enabled for server startup commands.
	serverCfg.StartDiagnosticsReporting = true

	return nil
}

// Fill the store paths.
// We have different defaults for server and tenant pod, and we don't want incorrect
// default to show up in flag help. To achieve that we create empty spec in private
// flag copy of spec and then copy this value if it was populated.
// If it isn't populated, default from server config is used for server commands or
// alternative default is generated by PreRun multi-tenant hook.
func extraStoreFlagInit(cmd *cobra.Command) error {
	fs := cliflagcfg.FlagSetForCmd(cmd)
	if fs.Changed(cliflags.Store.Name) {
		serverCfg.Stores = storeSpecs
	}
	// Convert all the store paths to absolute paths. We want this to
	// ensure canonical directories across invocations; and also to
	// benefit from the check in GetAbsoluteStorePath() that the user
	// didn't mistakenly assume a heading '~' would get translated by
	// CockroachDB. (The shell should be responsible for that.)
	for i, ss := range serverCfg.Stores.Specs {
		if ss.InMemory {
			continue
		}
		absPath, err := base.GetAbsoluteStorePath("path", ss.Path)
		if err != nil {
			return err
		}
		ss.Path = absPath
		serverCfg.Stores.Specs[i] = ss
	}

	// Configure the external I/O directory.
	if !fs.Changed(cliflags.ExternalIODir.Name) {
		// Try to find a directory from the store configuration.
		for _, ss := range serverCfg.Stores.Specs {
			if ss.InMemory {
				continue
			}
			startCtx.externalIODir = filepath.Join(ss.Path, "extern")
			break
		}
	}
	if startCtx.externalIODir != "" {
		// Make the directory name absolute.
		var err error
		startCtx.externalIODir, err = base.GetAbsoluteStorePath(cliflags.ExternalIODir.Name, startCtx.externalIODir)
		if err != nil {
			return err
		}
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
	serverCfg.Addr = net.JoinHostPort(cliCtx.clientOpts.ServerHost, cliCtx.clientOpts.ServerPort)
	serverCfg.AdvertiseAddr = serverCfg.Addr
	serverCfg.SQLAddr = net.JoinHostPort(cliCtx.clientOpts.ServerHost, cliCtx.clientOpts.ServerPort)
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
	fs := cliflagcfg.FlagSetForCmd(cmd)
	if !fs.Changed(cliflags.Store.Name) {
		// We assume that we only need to change top level store as temp dir configs are
		// initialized when start is executed and temp dirs inherit path from first store.
		tenantID := fs.Lookup(cliflags.TenantID.Name).Value.String()
		serverCfg.Stores.Specs[0].Path += "-tenant-" + tenantID
	}

	// In standalone SQL servers, we do not generate a ballast file,
	// unless a ballast size was specified explicitly by the user.
	for i := range serverCfg.Stores.Specs {
		spec := &serverCfg.Stores.Specs[i]
		if spec.BallastSize == nil {
			// Only override if there was no ballast size specified to start
			// with.
			zero := base.SizeSpec{InBytes: 0, Percent: 0}
			spec.BallastSize = &zero
		}
	}
	return nil
}

// RegisterFlags exists so that other packages can register flags using the
// Register<Type>FlagDepth functions and end up in a call frame in the cli
// package rather than the cliccl package to defeat the duplicate envvar
// registration logic.
func RegisterFlags(f func()) { f() }
