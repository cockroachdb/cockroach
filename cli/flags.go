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

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/cli/cliflags"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log/logflags"
)

var maxResults int64

var connURL string
var connUser, connHost, connPort, advertiseHost, httpPort, httpAddr, connDBName, zoneConfig string
var zoneDisableReplication bool
var startBackground bool
var undoFreezeCluster bool

var serverCtx = server.MakeContext()
var baseCtx = serverCtx.Context
var cliCtx = cliContext{Context: baseCtx}
var sqlCtx = sqlContext{cliContext: &cliCtx}
var debugCtx = debugContext{
	startKey:   engine.NilKey,
	endKey:     engine.MVCCKeyMax,
	replicated: false,
}

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
	ctx   *base.Context
	isSet bool
}

func newInsecureValue(ctx *base.Context) *insecureValue {
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
	if flagInfo.Shorthand == "" {
		f.StringVar(valPtr, flagInfo.Name, defaultVal, makeUsageString(flagInfo))
	} else {
		f.StringVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))
	}

	setFlagFromEnv(f, flagInfo)
}

func intFlag(f *pflag.FlagSet, valPtr *int, flagInfo cliflags.FlagInfo, defaultVal int) {
	if flagInfo.Shorthand == "" {
		f.IntVar(valPtr, flagInfo.Name, defaultVal, makeUsageString(flagInfo))
	} else {
		f.IntVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))
	}

	setFlagFromEnv(f, flagInfo)
}

func int64Flag(f *pflag.FlagSet, valPtr *int64, flagInfo cliflags.FlagInfo, defaultVal int64) {
	if flagInfo.Shorthand == "" {
		f.Int64Var(valPtr, flagInfo.Name, defaultVal, makeUsageString(flagInfo))
	} else {
		f.Int64VarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))
	}

	setFlagFromEnv(f, flagInfo)
}

func boolFlag(f *pflag.FlagSet, valPtr *bool, flagInfo cliflags.FlagInfo, defaultVal bool) {
	if flagInfo.Shorthand == "" {
		f.BoolVar(valPtr, flagInfo.Name, defaultVal, makeUsageString(flagInfo))
	} else {
		f.BoolVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))
	}

	setFlagFromEnv(f, flagInfo)
}

func durationFlag(
	f *pflag.FlagSet, valPtr *time.Duration, flagInfo cliflags.FlagInfo, defaultVal time.Duration,
) {
	if flagInfo.Shorthand == "" {
		f.DurationVar(valPtr, flagInfo.Name, defaultVal, makeUsageString(flagInfo))
	} else {
		f.DurationVarP(valPtr, flagInfo.Name, flagInfo.Shorthand, defaultVal, makeUsageString(flagInfo))
	}

	setFlagFromEnv(f, flagInfo)
}

func varFlag(f *pflag.FlagSet, value pflag.Value, flagInfo cliflags.FlagInfo) {
	if flagInfo.Shorthand == "" {
		f.Var(value, flagInfo.Name, makeUsageString(flagInfo))
	} else {
		f.VarP(value, flagInfo.Name, flagInfo.Shorthand, makeUsageString(flagInfo))
	}

	setFlagFromEnv(f, flagInfo)
}

func init() {
	// Change the logging defaults for the main cockroach binary.
	if err := flag.Lookup(logflags.LogToStderrName).Value.Set("false"); err != nil {
		panic(err)
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
		pf.AddFlag(flag)
	})

	// The --log-dir default changes depending on the command. Avoid confusion by
	// simply clearing it.
	pf.Lookup(logflags.LogDirName).DefValue = ""
	// If no value is specified for --alsologtostderr output everything.
	pf.Lookup(logflags.AlsoLogToStderrName).NoOptDefVal = "INFO"

	// Security flags.
	baseCtx.Insecure = true
	insecure = newInsecureValue(baseCtx)

	{
		f := startCmd.Flags()

		// Server flags.
		stringFlag(f, &connHost, cliflags.ServerHost, "")
		stringFlag(f, &connPort, cliflags.ServerPort, base.DefaultPort)
		stringFlag(f, &advertiseHost, cliflags.AdvertiseHost, "")
		stringFlag(f, &httpPort, cliflags.ServerHTTPPort, base.DefaultHTTPPort)
		stringFlag(f, &httpAddr, cliflags.ServerHTTPAddr, "")
		stringFlag(f, &serverCtx.Attrs, cliflags.Attrs, serverCtx.Attrs)

		varFlag(f, &serverCtx.Stores, cliflags.Store)
		durationFlag(f, &serverCtx.RaftTickInterval, cliflags.RaftTickInterval, base.DefaultRaftTickInterval)
		boolFlag(f, &startBackground, cliflags.Background, false)

		// Usage for the unix socket is odd as we use a real file, whereas
		// postgresql and clients consider it a directory and build a filename
		// inside it using the port.
		// Thus, we keep it hidden and use it for testing only.
		stringFlag(f, &serverCtx.SocketFile, cliflags.Socket, "")
		_ = f.MarkHidden(cliflags.Socket.Name)

		varFlag(f, insecure, cliflags.Insecure)
		// Allow '--insecure'
		f.Lookup(cliflags.Insecure.Name).NoOptDefVal = "true"

		// Certificate flags.
		stringFlag(f, &baseCtx.SSLCA, cliflags.CACert, baseCtx.SSLCA)
		stringFlag(f, &baseCtx.SSLCert, cliflags.Cert, baseCtx.SSLCert)
		stringFlag(f, &baseCtx.SSLCertKey, cliflags.Key, baseCtx.SSLCertKey)

		// Cluster joining flags.
		varFlag(f, &serverCtx.JoinList, cliflags.Join)

		// Engine flags.
		setDefaultCacheSize(&serverCtx)
		cacheSize = newBytesValue(&serverCtx.CacheSize)
		varFlag(f, cacheSize, cliflags.Cache)
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		// Certificate flags.
		stringFlag(f, &baseCtx.SSLCA, cliflags.CACert, baseCtx.SSLCA)
		stringFlag(f, &baseCtx.SSLCAKey, cliflags.CAKey, baseCtx.SSLCAKey)
		stringFlag(f, &baseCtx.SSLCert, cliflags.Cert, baseCtx.SSLCert)
		stringFlag(f, &baseCtx.SSLCertKey, cliflags.Key, baseCtx.SSLCertKey)
		intFlag(f, &keySize, cliflags.KeySize, defaultKeySize)
	}

	stringFlag(setUserCmd.Flags(), &password, cliflags.Password, "")

	clientCmds := []*cobra.Command{
		sqlShellCmd, quitCmd, freezeClusterCmd, dumpCmd, /* startCmd is covered above */
	}
	clientCmds = append(clientCmds, kvCmds...)
	clientCmds = append(clientCmds, rangeCmds...)
	clientCmds = append(clientCmds, userCmds...)
	clientCmds = append(clientCmds, zoneCmds...)
	clientCmds = append(clientCmds, nodeCmds...)
	clientCmds = append(clientCmds, backupCmds...)
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		stringFlag(f, &connHost, cliflags.ClientHost, "")

		varFlag(f, insecure, cliflags.Insecure)
		// Allow '--insecure'
		f.Lookup(cliflags.Insecure.Name).NoOptDefVal = "true"

		// Certificate flags.
		stringFlag(f, &baseCtx.SSLCA, cliflags.CACert, baseCtx.SSLCA)
		stringFlag(f, &baseCtx.SSLCert, cliflags.Cert, baseCtx.SSLCert)
		stringFlag(f, &baseCtx.SSLCertKey, cliflags.Key, baseCtx.SSLCertKey)

		// By default, client commands print their output as
		// pretty-formatted tables on terminals, and TSV when redirected
		// to a file. The user can override with --pretty.
		boolFlag(f, &cliCtx.prettyFmt, cliflags.Pretty, isInteractive)
	}

	stringFlag(setZoneCmd.Flags(), &zoneConfig, cliflags.ZoneConfig, "")

	varFlag(sqlShellCmd.Flags(), &sqlCtx.execStmts, cliflags.Execute)

	boolFlag(freezeClusterCmd.PersistentFlags(), &undoFreezeCluster, cliflags.UndoFreezeCluster, false)

	// Commands that need the cockroach port.
	simpleCmds := []*cobra.Command{quitCmd, freezeClusterCmd}
	simpleCmds = append(simpleCmds, kvCmds...)
	simpleCmds = append(simpleCmds, rangeCmds...)
	simpleCmds = append(simpleCmds, nodeCmds...)
	for _, cmd := range simpleCmds {
		f := cmd.PersistentFlags()
		stringFlag(f, &connPort, cliflags.ClientPort, base.DefaultPort)
	}

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, dumpCmd}
	sqlCmds = append(sqlCmds, zoneCmds...)
	sqlCmds = append(sqlCmds, userCmds...)
	for _, cmd := range sqlCmds {
		f := cmd.PersistentFlags()
		stringFlag(f, &connURL, cliflags.URL, "")

		stringFlag(f, &connUser, cliflags.User, security.RootUser)
		stringFlag(f, &connPort, cliflags.ClientPort, base.DefaultPort)
		stringFlag(f, &connDBName, cliflags.Database, "")
	}

	// Max results flag for scan, reverse scan, and range list.
	for _, cmd := range []*cobra.Command{scanCmd, reverseScanCmd, lsRangesCmd} {
		f := cmd.Flags()
		int64Flag(f, &maxResults, cliflags.MaxResults, 1000)
	}

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

	boolFlag(versionCmd.Flags(), &versionIncludesDeps, cliflags.Deps, false)

	cobra.OnInitialize(extraFlagInit)
}

// extraFlagInit is a standalone function so we can test more easily.
func extraFlagInit() {
	// If any of the security flags have been set, clear the insecure
	// setting. Note that we do the inverse when the --insecure flag is
	// set. See insecureValue.Set().
	if baseCtx.SSLCA != "" || baseCtx.SSLCAKey != "" ||
		baseCtx.SSLCert != "" || baseCtx.SSLCertKey != "" {
		baseCtx.Insecure = false
	}

	serverCtx.Addr = net.JoinHostPort(connHost, connPort)
	if httpAddr == "" {
		httpAddr = connHost
	}
	if advertiseHost == "" {
		advertiseHost = connHost
	}
	serverCtx.AdvertiseAddr = net.JoinHostPort(advertiseHost, connPort)
	serverCtx.HTTPAddr = net.JoinHostPort(httpAddr, httpPort)
}
