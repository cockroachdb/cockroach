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
	"flag"
	"fmt"
	"net"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/base"
	cflag "github.com/cockroachdb/cockroach/cli/flag"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log/logflags"
)

var maxResults int64

var connURL string
var connUser, connHost, connPort, httpPort, connDBName string

// cliContext is the CLI Context used for the command-line client.
var cliContext = NewContext()
var cacheSize *bytesValue
var insecure *insecureValue

type bytesValue struct {
	val   *int64
	isSet bool
}

func newBytesValue(val *int64) *bytesValue {
	return &bytesValue{val: val}
}

func (b *bytesValue) Set(s string) error {
	v, err := util.ParseBytes(s)
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
	return util.IBytes(*b.val)
}

type insecureValue struct {
	val   *bool
	isSet bool
}

func newInsecureValue(val *bool) *insecureValue {
	return &insecureValue{val: val}
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
	*b.val = v
	if *b.val {
		// If --insecure is specified, clear any of the existing security flags if
		// they were set. This allows composition of command lines where a later
		// specification of --insecure clears an earlier security specification.
		cliContext.SSLCA = ""
		cliContext.SSLCAKey = ""
		cliContext.SSLCert = ""
		cliContext.SSLCertKey = ""
	}
	return nil
}

func (b *insecureValue) Type() string {
	return "bool"
}

func (b *insecureValue) String() string {
	return fmt.Sprint(*b.val)
}

// initFlags sets the cli.Context values to flag values.
// Keep in sync with "server/context.go". Values in Context should be
// settable here.
func initFlags(ctx *Context) {
	// Change the logging defaults for the main cockroach binary.
	if err := flag.Lookup(logflags.LogToStderrName).Value.Set("false"); err != nil {
		panic(err)
	}

	// Map any flags registered in the standard "flag" package into the
	// top-level cockroach command.
	pf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		pf.AddFlag(pflag.PFlagFromGoFlag(f))
	})

	// The --log-dir default changes depending on the command. Avoid confusion by
	// simply clearing it.
	pf.Lookup(logflags.LogDirName).DefValue = ""
	// If no value is specified for --alsologtostderr output everything.
	pf.Lookup(logflags.AlsoLogToStderrName).NoOptDefVal = "INFO"

	{
		f := startCmd.Flags()

		// Server flags.
		f.StringVar(&connHost, cflag.HostName, "", cflag.Usage(cflag.ForServer(cflag.HostName)))
		f.StringVarP(&connPort, cflag.PortName, "p", base.DefaultPort, cflag.Usage(cflag.ForServer(cflag.PortName)))
		f.StringVar(&httpPort, cflag.HTTPPortName, base.DefaultHTTPPort, cflag.Usage(cflag.ForServer(cflag.HTTPPortName)))
		f.StringVar(&ctx.Attrs, cflag.AttrsName, ctx.Attrs, cflag.Usage(cflag.AttrsName))
		f.VarP(&ctx.Stores, cflag.StoreName, "s", cflag.Usage(cflag.StoreName))

		// Usage for the unix socket is odd as we use a real file, whereas
		// postgresql and clients consider it a directory and build a filename
		// inside it using the port.
		// Thus, we keep it hidden and use it for testing only.
		f.StringVar(&ctx.SocketFile, cflag.SocketName, "", cflag.Usage(cflag.SocketName))
		_ = f.MarkHidden(cflag.SocketName)

		// Security flags.
		ctx.Insecure = true
		insecure = newInsecureValue(&ctx.Insecure)
		insecureF := f.VarPF(insecure, cflag.InsecureName, "", cflag.Usage(cflag.InsecureName))
		insecureF.NoOptDefVal = "true"
		// Certificates.
		f.StringVar(&ctx.SSLCA, cflag.CACertName, ctx.SSLCA, cflag.Usage(cflag.CACertName))
		f.StringVar(&ctx.SSLCert, cflag.CertName, ctx.SSLCert, cflag.Usage(cflag.CertName))
		f.StringVar(&ctx.SSLCertKey, cflag.KeyName, ctx.SSLCertKey, cflag.Usage(cflag.KeyName))

		// Cluster joining flags.
		f.StringVar(&ctx.JoinUsing, cflag.JoinName, ctx.JoinUsing, cflag.Usage(cflag.JoinName))

		// Engine flags.
		cacheSize = newBytesValue(&ctx.CacheSize)
		f.Var(cacheSize, cflag.CacheName, cflag.Usage(cflag.CacheName))

		// Clear the cache default value. This flag does have a default, but
		// it is set only when the "start" command is run.
		f.Lookup(cflag.CacheName).DefValue = ""

		if err := startCmd.MarkFlagRequired(cflag.StoreName); err != nil {
			panic(err)
		}
	}

	{
		f := exterminateCmd.Flags()
		f.Var(&ctx.Stores, cflag.StoreName, cflag.Usage(cflag.StoreName))
		if err := exterminateCmd.MarkFlagRequired(cflag.StoreName); err != nil {
			panic(err)
		}
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		// Certificate flags.
		f.StringVar(&ctx.SSLCA, cflag.CACertName, ctx.SSLCA, cflag.Usage(cflag.CACertName))
		f.StringVar(&ctx.SSLCAKey, cflag.CAKeyName, ctx.SSLCAKey, cflag.Usage(cflag.CAKeyName))
		f.StringVar(&ctx.SSLCert, cflag.CertName, ctx.SSLCert, cflag.Usage(cflag.CertName))
		f.StringVar(&ctx.SSLCertKey, cflag.KeyName, ctx.SSLCertKey, cflag.Usage(cflag.KeyName))
		f.IntVar(&keySize, cflag.KeySizeName, defaultKeySize, cflag.Usage(cflag.KeySizeName))
		if err := cmd.MarkFlagRequired(cflag.KeySizeName); err != nil {
			panic(err)
		}
	}

	setUserCmd.Flags().StringVar(&password, cflag.PasswordName, "", cflag.Usage(cflag.PasswordName))

	clientCmds := []*cobra.Command{
		sqlShellCmd, exterminateCmd, quitCmd, /* startCmd is covered above */
	}
	clientCmds = append(clientCmds, kvCmds...)
	clientCmds = append(clientCmds, rangeCmds...)
	clientCmds = append(clientCmds, userCmds...)
	clientCmds = append(clientCmds, zoneCmds...)
	clientCmds = append(clientCmds, nodeCmds...)
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		insecureF := f.VarPF(insecure, cflag.InsecureName, "", cflag.Usage(cflag.InsecureName))
		insecureF.NoOptDefVal = "true"
		f.StringVar(&connHost, cflag.HostName, "", cflag.Usage(cflag.ForClient(cflag.HostName)))

		// Certificate flags.
		f.StringVar(&ctx.SSLCA, cflag.CACertName, ctx.SSLCA, cflag.Usage(cflag.CACertName))
		f.StringVar(&ctx.SSLCert, cflag.CertName, ctx.SSLCert, cflag.Usage(cflag.CertName))
		f.StringVar(&ctx.SSLCertKey, cflag.KeyName, ctx.SSLCertKey, cflag.Usage(cflag.KeyName))
	}

	{
		f := sqlShellCmd.Flags()
		f.VarP(&ctx.execStmts, cflag.ExecuteName, "e", cflag.Usage(cflag.ExecuteName))
	}

	// Commands that need the cockroach port.
	simpleCmds := []*cobra.Command{exterminateCmd}
	simpleCmds = append(simpleCmds, kvCmds...)
	simpleCmds = append(simpleCmds, rangeCmds...)
	for _, cmd := range simpleCmds {
		f := cmd.PersistentFlags()
		f.StringVarP(&connPort, cflag.PortName, "p", base.DefaultPort, cflag.Usage(cflag.ForClient(cflag.PortName)))
	}

	// Commands that need an http port.
	httpCmds := []*cobra.Command{quitCmd}
	httpCmds = append(httpCmds, nodeCmds...)
	for _, cmd := range httpCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&httpPort, cflag.HTTPPortName, base.DefaultHTTPPort, cflag.Usage(cflag.ForClient(cflag.HTTPPortName)))
	}

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd}
	sqlCmds = append(sqlCmds, zoneCmds...)
	sqlCmds = append(sqlCmds, userCmds...)
	for _, cmd := range sqlCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&connURL, cflag.URLName, "", cflag.Usage(cflag.URLName))

		f.StringVarP(&connUser, cflag.UserName, "u", security.RootUser, cflag.Usage(cflag.UserName))
		f.StringVarP(&connPort, cflag.PortName, "p", base.DefaultPort, cflag.Usage(cflag.ForClient(cflag.PortName)))
		f.StringVarP(&connDBName, cflag.DatabaseName, "d", "", cflag.Usage(cflag.DatabaseName))
	}

	// Max results flag for scan, reverse scan, and range list.
	for _, cmd := range []*cobra.Command{scanCmd, reverseScanCmd, lsRangesCmd} {
		f := cmd.Flags()
		f.Int64Var(&maxResults, cflag.MaxResultsName, 1000, cflag.Usage(cflag.MaxResultsName))
	}

	// Debug commands.
	{
		f := debugKeysCmd.Flags()
		f.StringVar(&cliContext.debug.startKey, cflag.FromName, "", cflag.Usage(cflag.FromName))
		f.StringVar(&cliContext.debug.endKey, cflag.ToName, "", cflag.Usage(cflag.ToName))
		f.BoolVar(&cliContext.debug.raw, cflag.RawName, false, cflag.Usage(cflag.RawName))
		f.BoolVar(&cliContext.debug.values, cflag.ValuesName, false, cflag.Usage(cflag.ValuesName))
	}
}

func init() {
	initFlags(cliContext)

	cobra.OnInitialize(func() {
		// If any of the security flags have been set, clear the insecure
		// setting. Note that we do the inverse when the --insecure flag is
		// set. See insecureValue.Set().
		if cliContext.SSLCA != "" || cliContext.SSLCAKey != "" ||
			cliContext.SSLCert != "" || cliContext.SSLCertKey != "" {
			cliContext.Insecure = false
		}

		cliContext.Addr = net.JoinHostPort(connHost, connPort)
		cliContext.HTTPAddr = net.JoinHostPort(connHost, httpPort)
	})
}
