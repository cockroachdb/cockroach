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
//
// Author: Daniel Theophanes (kardianos@gmail.com)

package cli

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/kr/text"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logflags"
)

var maxResults int64

var sqlConnURL, sqlConnUser, sqlConnDBName string
var serverConnHost, serverConnPort, serverAdvertiseHost string
var serverHTTPHost, serverHTTPPort string
var clientConnHost, clientConnPort string
var zoneConfig string
var zoneDisableReplication bool

var serverCfg = server.MakeConfig()
var baseCfg = serverCfg.Config
var cliCtx = cliContext{Config: baseCfg}
var sqlCtx = sqlContext{cliContext: &cliCtx}
var dumpCtx = dumpContext{cliContext: &cliCtx, dumpMode: dumpBoth}
var debugCtx = debugContext{
	startKey:   engine.NilKey,
	endKey:     engine.MVCCKeyMax,
	replicated: false,
}

// InitCLIDefaults is used for testing.
func InitCLIDefaults() {
	cliCtx.tableDisplayFormat = tableDisplayTSV
	dumpCtx.dumpMode = dumpBoth
}

var sqlSize *bytesValue
var cacheSize *bytesValue
var insecure *insecureValue

const usageIndentation = 8
const wrapWidth = 79 - usageIndentation

// wrapDescription wraps the text in a cliflags.FlagInfo.Description.
func wrapDescription(s string) string {
	var result bytes.Buffer

	// split returns the parts of the string before and after the first occurrence
	// of the tag.
	split := func(str, tag string) (before, after string) {
		pieces := strings.SplitN(str, tag, 2)
		switch len(pieces) {
		case 0:
			return "", ""
		case 1:
			return pieces[0], ""
		default:
			return pieces[0], pieces[1]
		}
	}

	for len(s) > 0 {
		var toWrap, dontWrap string
		// Wrap everything up to the next stop wrap tag.
		toWrap, s = split(s, "<PRE>")
		result.WriteString(text.Wrap(toWrap, wrapWidth))
		// Copy everything up to the next start wrap tag.
		dontWrap, s = split(s, "</PRE>")
		result.WriteString(dontWrap)
	}
	return result.String()
}

// makeUsageString returns the usage information for a given flag identifier. The
// identifier is always the flag's name, except in the case where a client/server
// distinction for the same flag is required.
func makeUsageString(flagInfo cliflags.FlagInfo) string {
	s := "\n" + wrapDescription(flagInfo.Description) + "\n"
	if flagInfo.EnvVar != "" {
		// Check that the environment variable name matches the flag name. Note: we
		// don't want to automatically generate the name so that grepping for a flag
		// name in the code yields the flag definition.
		correctName := "COCKROACH_" + strings.ToUpper(strings.Replace(flagInfo.Name, "-", "_", -1))
		if flagInfo.EnvVar != correctName {
			panic(fmt.Sprintf("incorrect EnvVar %s for flag %s (should be %s)",
				flagInfo.EnvVar, flagInfo.Name, correctName))
		}
		s = s + "Environment variable: " + flagInfo.EnvVar + "\n"
	}
	// github.com/spf13/pflag appends the default value after the usage text. Add
	// the correct indentation (7 spaces) here. This is admittedly fragile.
	return text.Indent(s, strings.Repeat(" ", usageIndentation)) +
		strings.Repeat(" ", usageIndentation-1)
}

type bytesValue struct {
	val   *int64
	isSet bool
}

func newBytesValue(val *int64) *bytesValue {
	return &bytesValue{val: val}
}

func (b *bytesValue) Set(s string) error {
	v, err := humanizeutil.ParseBytes(s)
	if err != nil {
		return err
	}
	*b.val = v
	b.isSet = true
	return nil
}

func (b *bytesValue) Type() string {
	return "bytes"
}

func (b *bytesValue) String() string {
	// This uses the MiB, GiB, etc suffixes. If we use humanize.Bytes() we get
	// the MB, GB, etc suffixes, but the conversion is done in multiples of 1000
	// vs 1024.
	return humanizeutil.IBytes(*b.val)
}

type insecureValue struct {
	ctx   *base.Config
	isSet bool
}

func newInsecureValue(ctx *base.Config) *insecureValue {
	return &insecureValue{ctx: ctx}
}

func (b *insecureValue) IsBoolFlag() bool {
	return true
}

func (b *insecureValue) Set(s string) error {
	v, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	b.isSet = true
	b.ctx.Insecure = v
	if b.ctx.Insecure {
		// If --insecure is specified, clear any of the existing security flags if
		// they were set. This allows composition of command lines where a later
		// specification of --insecure clears an earlier security specification.
		b.ctx.SSLCA = ""
		b.ctx.SSLCAKey = ""
		b.ctx.SSLCert = ""
		b.ctx.SSLCertKey = ""
	}
	return nil
}

func (b *insecureValue) Type() string {
	return "bool"
}

func (b *insecureValue) String() string {
	return fmt.Sprint(b.ctx.Insecure)
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

func stringFlag(f *pflag.FlagSet, valPtr *string, flagInfo cliflags.FlagInfo, defaultVal string) {
	f.StringVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))

	setFlagFromEnv(f, flagInfo)
}

func intFlag(f *pflag.FlagSet, valPtr *int, flagInfo cliflags.FlagInfo, defaultVal int) {
	f.IntVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))

	setFlagFromEnv(f, flagInfo)
}

func int64Flag(f *pflag.FlagSet, valPtr *int64, flagInfo cliflags.FlagInfo, defaultVal int64) {
	f.Int64VarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))

	setFlagFromEnv(f, flagInfo)
}

func boolFlag(f *pflag.FlagSet, valPtr *bool, flagInfo cliflags.FlagInfo, defaultVal bool) {
	f.BoolVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))

	setFlagFromEnv(f, flagInfo)
}

func durationFlag(
	f *pflag.FlagSet, valPtr *time.Duration, flagInfo cliflags.FlagInfo, defaultVal time.Duration,
) {
	f.DurationVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))

	setFlagFromEnv(f, flagInfo)
}

func varFlag(f *pflag.FlagSet, value pflag.Value, flagInfo cliflags.FlagInfo) {
	f.VarP(value, flagInfo.Name, flagInfo.Shorthand, makeUsageString(flagInfo))

	setFlagFromEnv(f, flagInfo)
}

func init() {
	// Change the logging defaults for the main cockroach binary.
	if err := flag.Lookup(logflags.LogToStderrName).Value.Set("false"); err != nil {
		panic(err)
	}

	// Every command but start will inherit the following setting.
	cockroachCmd.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		extraClientFlagInit()
		return setDefaultStderrVerbosity(cmd, log.Severity_WARNING)
	}

	// The following only runs for `start`.
	startCmd.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		extraServerFlagInit()
		return setDefaultStderrVerbosity(cmd, log.Severity_INFO)
	}

	// Map any flags registered in the standard "flag" package into the
	// top-level cockroach command.
	pf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		flag := pflag.PFlagFromGoFlag(f)
		// TODO(peter): Decide if we want to make the lightstep flags visible.
		if strings.HasPrefix(flag.Name, "lightstep_") {
			flag.Hidden = true
		} else if flag.Name == "no-redirect-stderr" {
			flag.Hidden = true
		} else if flag.Name == "show-logs" {
			flag.Hidden = true
		}
		pf.AddFlag(flag)
	})

	// The --log-dir default changes depending on the command. Avoid confusion by
	// simply clearing it.
	pf.Lookup(logflags.LogDirName).DefValue = ""
	// If no value is specified for --alsologtostderr output everything.
	pf.Lookup(logflags.AlsoLogToStderrName).NoOptDefVal = "INFO"

	// Security flags.
	baseCfg.Insecure = true
	insecure = newInsecureValue(baseCfg)

	{
		f := startCmd.Flags()

		// Server flags.
		stringFlag(f, &serverConnHost, cliflags.ServerHost, "")
		stringFlag(f, &serverConnPort, cliflags.ServerPort, base.DefaultPort)
		stringFlag(f, &serverAdvertiseHost, cliflags.AdvertiseHost, "")
		stringFlag(f, &serverHTTPHost, cliflags.ServerHTTPHost, "")
		stringFlag(f, &serverHTTPPort, cliflags.ServerHTTPPort, base.DefaultHTTPPort)
		stringFlag(f, &serverCfg.Attrs, cliflags.Attrs, serverCfg.Attrs)
		varFlag(f, &serverCfg.Locality, cliflags.Locality)

		varFlag(f, &serverCfg.Stores, cliflags.Store)
		durationFlag(f, &serverCfg.RaftTickInterval, cliflags.RaftTickInterval, base.DefaultRaftTickInterval)

		// Usage for the unix socket is odd as we use a real file, whereas
		// postgresql and clients consider it a directory and build a filename
		// inside it using the port.
		// Thus, we keep it hidden and use it for testing only.
		stringFlag(f, &serverCfg.SocketFile, cliflags.Socket, "")
		_ = f.MarkHidden(cliflags.Socket.Name)

		stringFlag(f, &serverCfg.PIDFile, cliflags.PIDFile, "")

		varFlag(f, insecure, cliflags.Insecure)
		// Allow '--insecure'
		f.Lookup(cliflags.Insecure.Name).NoOptDefVal = "true"

		// Certificate flags.
		stringFlag(f, &baseCfg.SSLCA, cliflags.CACert, baseCfg.SSLCA)
		stringFlag(f, &baseCfg.SSLCert, cliflags.Cert, baseCfg.SSLCert)
		stringFlag(f, &baseCfg.SSLCertKey, cliflags.Key, baseCfg.SSLCertKey)
		stringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir, base.DefaultCertsDirectory)

		// Cluster joining flags.
		varFlag(f, &serverCfg.JoinList, cliflags.Join)

		// Engine flags.
		setDefaultSizeParameters(&serverCfg)
		cacheSize = newBytesValue(&serverCfg.CacheSize)
		varFlag(f, cacheSize, cliflags.Cache)

		sqlSize = newBytesValue(&serverCfg.SQLMemoryPoolSize)
		varFlag(f, sqlSize, cliflags.SQLMem)
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		// Certificate flags.
		stringFlag(f, &baseCfg.SSLCA, cliflags.CACert, baseCfg.SSLCA)
		stringFlag(f, &baseCfg.SSLCAKey, cliflags.CAKey, baseCfg.SSLCAKey)
		stringFlag(f, &baseCfg.SSLCert, cliflags.Cert, baseCfg.SSLCert)
		stringFlag(f, &baseCfg.SSLCertKey, cliflags.Key, baseCfg.SSLCertKey)
		stringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir, base.DefaultCertsDirectory)
		intFlag(f, &keySize, cliflags.KeySize, defaultKeySize)
	}

	boolFlag(setUserCmd.Flags(), &password, cliflags.Password, false)

	clientCmds := []*cobra.Command{
		debugZipCmd,
		dumpCmd,
		genHAProxyCmd,
		quitCmd,
		sqlShellCmd,
		/* startCmd is covered above */
	}
	clientCmds = append(clientCmds, rangeCmds...)
	clientCmds = append(clientCmds, userCmds...)
	clientCmds = append(clientCmds, zoneCmds...)
	clientCmds = append(clientCmds, nodeCmds...)
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		stringFlag(f, &clientConnHost, cliflags.ClientHost, "")
		stringFlag(f, &clientConnPort, cliflags.ClientPort, base.DefaultPort)

		varFlag(f, insecure, cliflags.Insecure)
		// Allow '--insecure'
		f.Lookup(cliflags.Insecure.Name).NoOptDefVal = "true"

		// Certificate flags.
		stringFlag(f, &baseCfg.SSLCA, cliflags.CACert, baseCfg.SSLCA)
		stringFlag(f, &baseCfg.SSLCert, cliflags.Cert, baseCfg.SSLCert)
		stringFlag(f, &baseCfg.SSLCertKey, cliflags.Key, baseCfg.SSLCertKey)
		stringFlag(f, &baseCfg.SSLCertsDir, cliflags.CertsDir, base.DefaultCertsDirectory)
	}

	zf := setZoneCmd.Flags()
	stringFlag(zf, &zoneConfig, cliflags.ZoneConfig, "")
	boolFlag(zf, &zoneDisableReplication, cliflags.ZoneDisableReplication, false)

	varFlag(sqlShellCmd.Flags(), &sqlCtx.execStmts, cliflags.Execute)
	varFlag(dumpCmd.Flags(), &dumpCtx.dumpMode, cliflags.DumpMode)
	stringFlag(dumpCmd.Flags(), &dumpCtx.asOf, cliflags.DumpTime, "")

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, dumpCmd}
	sqlCmds = append(sqlCmds, zoneCmds...)
	sqlCmds = append(sqlCmds, userCmds...)
	for _, cmd := range sqlCmds {
		f := cmd.PersistentFlags()
		stringFlag(f, &sqlConnURL, cliflags.URL, "")
		stringFlag(f, &sqlConnUser, cliflags.User, security.RootUser)

		if cmd == sqlShellCmd {
			stringFlag(f, &sqlConnDBName, cliflags.Database, "")
		}
	}

	// Commands that print tables.
	tableOutputCommands := []*cobra.Command{sqlShellCmd}
	tableOutputCommands = append(tableOutputCommands, userCmds...)
	tableOutputCommands = append(tableOutputCommands, nodeCmds...)

	// By default, these commands print their output as pretty-formatted
	// tables on terminals, and TSV when redirected to a file. The user
	// can override with --format.
	cliCtx.tableDisplayFormat = tableDisplayTSV
	if isInteractive {
		cliCtx.tableDisplayFormat = tableDisplayPretty
	}
	for _, cmd := range tableOutputCommands {
		f := cmd.Flags()
		varFlag(f, &cliCtx.tableDisplayFormat, cliflags.TableDisplayFormat)
	}

	// Max results flag for range list.
	int64Flag(lsRangesCmd.Flags(), &maxResults, cliflags.MaxResults, 1000)

	// Debug commands.
	{
		f := debugKeysCmd.Flags()
		varFlag(f, (*mvccKey)(&debugCtx.startKey), cliflags.From)
		varFlag(f, (*mvccKey)(&debugCtx.endKey), cliflags.To)
		boolFlag(f, &debugCtx.values, cliflags.Values, false)
		boolFlag(f, &debugCtx.sizes, cliflags.Sizes, false)

		f = debugRangeDataCmd.Flags()
		boolFlag(f, &debugCtx.replicated, cliflags.Replicated, false)
	}
}

func extraSSLInit() {
	// If any of the security flags have been set, clear the insecure
	// setting. Note that we do the inverse when the --insecure flag is
	// set. See insecureValue.Set().
	if baseCfg.SSLCA != "" || baseCfg.SSLCAKey != "" ||
		baseCfg.SSLCert != "" || baseCfg.SSLCertKey != "" {
		baseCfg.Insecure = false
	}
}

func extraServerFlagInit() {
	extraSSLInit()
	serverCfg.Addr = net.JoinHostPort(serverConnHost, serverConnPort)
	if serverAdvertiseHost == "" {
		serverAdvertiseHost = serverConnHost
	}
	serverCfg.AdvertiseAddr = net.JoinHostPort(serverAdvertiseHost, serverConnPort)
	if serverHTTPHost == "" {
		serverHTTPHost = serverConnHost
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPHost, serverHTTPPort)
}

func extraClientFlagInit() {
	extraSSLInit()
	serverCfg.Addr = net.JoinHostPort(clientConnHost, clientConnPort)
	serverCfg.AdvertiseAddr = serverCfg.Addr
	if serverHTTPHost == "" {
		serverHTTPHost = serverConnHost
	}
	serverCfg.HTTPAddr = net.JoinHostPort(serverHTTPHost, serverHTTPPort)
}

func setDefaultStderrVerbosity(cmd *cobra.Command, defaultSeverity log.Severity) error {
	pf := cmd.Flags()

	if vf := pf.Lookup(logflags.AlsoLogToStderrName); !vf.Changed {
		ls := pf.Lookup(logflags.LogToStderrName)

		// If `--logtostderr` is specified, the base default is
		// everything, otherwise it's nothing (subject to the additional
		// setting below).
		if ls.Value.String() == "true" {
			if err := vf.Value.Set(log.Severity_INFO.String()); err != nil {
				return err
			}
		} else {
			if err := vf.Value.Set(log.Severity_NONE.String()); err != nil {
				return err
			}
		}
		// If no log directory has been set, reduce the logging verbosity
		// to the given default.
		if !log.DirSet() {
			if err := vf.Value.Set(defaultSeverity.String()); err != nil {
				return err
			}
		}
	}

	return nil
}
