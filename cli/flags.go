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
	"net"
	"reflect"
	"strings"

	"github.com/kr/text"
	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/security"
)

var maxResults int64

var connURL string
var connUser, connHost, connPort, connPGPort, connDBName string

// pflagValue wraps flag.Value and implements the extra methods of the
// pflag.Value interface.
type pflagValue struct {
	flag.Value
}

func (v pflagValue) Type() string {
	t := reflect.TypeOf(v.Value).Elem()
	return t.Kind().String()
}

func (v pflagValue) IsBoolFlag() bool {
	t := reflect.TypeOf(v.Value).Elem()
	return t.Kind() == reflect.Bool
}

var flagUsage = map[string]string{
	"attrs": wrapText(`
An ordered, colon-separated list of node attributes. Attributes are
arbitrary strings specifying topography or machine
capabilities. Topography might include datacenter designation
(e.g. "us-west-1a", "us-west-1b", "us-east-1c"). Machine capabilities
might include specialized hardware or number of cores (e.g. "gpu",
"x16c"). The relative geographic proximity of two nodes is inferred
from the common prefix of the attributes list, so topographic
attributes should be specified first and in the same order for all
nodes. For example:`) + `

  --attrs=us-west-1b,gpu
`,
	"balance-mode": wrapText(`
Determines the criteria used by nodes to make balanced allocation
decisions. Valid options are "usage" (default) or "rangecount".`),
	"cache-size": wrapText(`
Total size in bytes for caches, shared evenly if there are multiple
storage devices.`),
	"certs": wrapText(`
Directory containing RSA key and x509 certs. This flag is required if
--insecure=false.`),
	"dev": wrapText(`
Runs the node as a standalone in-memory cluster and forces --insecure
for all server and client commands. Useful for developing Cockroach
itself.`),
	"execute": wrapText(`
Execute the SQL statement(s) on the command line, then exit.  Each
subsequent positional argument on the command line may contain
one or more SQL statements, separated by semicolons. If an
error occurs in any statement, the command exits with a
non-zero status code and further statements are not
executed. Only the results of the first SQL statement in each
positional argument are printed on the standard output.`),
	"join": wrapText(`
A comma-separated list of addresses to use when a new node is joining
an existing cluster. Each address in the list has an optional type:
[type=]<address>. An unspecified type means ip address or dns.
Type is one of:
- tcp: (default if type is omitted): plain ip address or hostname.
- http-lb: HTTP load balancer: we query
           http(s)://<address>/_status/details/local
`),
	"host": wrapText(`
Database server host.`),
	"insecure": wrapText(`
Run over plain HTTP. WARNING: this is strongly discouraged.`),
	"key-size": wrapText(`
Key size in bits for CA/Node/Client certificates.`),
	"linearizable": wrapText(`
Enables linearizable behaviour of operations on this node by making
sure that no commit timestamp is reported back to the client until all
other node clocks have necessarily passed it.`),
	"max-offset": wrapText(`
The maximum clock offset for the cluster. Clock offset is measured on
all node-to-node links and if any node notices it has clock offset in
excess of --max-offset, it will commit suicide. Setting this value too
high may decrease transaction performance in the presence of
contention.`),
	"max-results": wrapText(`
Define the maximum number of results that will be retrieved.`),
	"memtable-budget": wrapText(`
Total size in bytes for memtables, shared evenly if there are multiple
storage devices.`),
	"metrics-frequency": wrapText(`
Adjust the frequency at which the server records its own internal metrics.
`),
	"password": wrapText(`
The created user's password. If provided, disables prompting. Pass '-' to
provide the password on standard input.`),
	"pgport": wrapText(`
The port to bind for Postgres traffic.`),
	"port": wrapText(`
The port to bind for cockroach traffic.`),
	"scan-interval": wrapText(`
Adjusts the target for the duration of a single scan through a store's
ranges. The scan is slowed as necessary to approximately achieve this
duration.`),
	"scan-max-idle-time": wrapText(`
Adjusts the max idle time of the scanner. This speeds up the scanner on small
clusters to be more responsive.`),
	"stores": wrapText(`
A comma-separated list of stores, specified by a colon-separated list
of device attributes followed by '=' and either a filepath for a
persistent store or an integer size in bytes for an in-memory
store. Device attributes typically include whether the store is flash
(ssd), spinny disk (hdd), fusion-io (fio), in-memory (mem); device
attributes might also include speeds and other specs (7200rpm,
200kiops, etc.). For example:`) + `

  --stores=hdd:7200rpm=/mnt/hda1,ssd=/mnt/ssd01,ssd=/mnt/ssd02,mem=1073741824
`,
	"time-until-store-dead": wrapText(`
Adjusts the timeout for stores.  If there's been no gossiped updated
from a store after this time, the store is considered unavailable.
Replicas on an unavailable store will be moved to available ones.`),
	"url": wrapText(`
Connection url. eg: postgresql://myuser@localhost:15432/mydb
If left empty, the connection flags are used (host, port, user, database, insecure, certs).`),
	"user": wrapText(`
Database user name.`),
}

const usageIndentation = 8
const wrapWidth = 79 - usageIndentation

func wrapText(s string) string {
	return text.Wrap(s, wrapWidth)
}

func usage(name string) string {
	s := flagUsage[name]
	if s[0] != '\n' {
		s = "\n" + s
	}
	if s[len(s)-1] != '\n' {
		s = s + "\n"
	}
	// github.com/spf13/pflag appends the default value after the usage text. Add
	// the correct indentation (7 spaces) here. This is admittedly fragile.
	return text.Indent(s, strings.Repeat(" ", usageIndentation)) +
		strings.Repeat(" ", usageIndentation-1)
}

func normalizeStdFlagName(s string) string {
	return strings.Replace(s, "_", "-", -1)
}

// initFlags sets the cli.Context values to flag values.
// Keep in sync with "server/context.go". Values in Context should be
// settable here.
func initFlags(ctx *Context) {
	// Map any flags registered in the standard "flag" package into the
	// top-level cockroach command.
	pf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		pf.Var(pflagValue{f.Value}, normalizeStdFlagName(f.Name), f.Usage)
	})

	{
		f := startCmd.Flags()
		f.BoolVar(&ctx.EphemeralSingleNode, "dev", ctx.EphemeralSingleNode, usage("dev"))

		// Server flags.
		f.StringVar(&connHost, "host", "", flagUsage["host"])
		f.StringVar(&connPort, "port", "26257", flagUsage["port"])
		f.StringVar(&connPGPort, "pgport", "15432", flagUsage["pgport"])
		f.StringVar(&ctx.Attrs, "attrs", ctx.Attrs, usage("attrs"))
		f.StringVar(&ctx.Stores, "stores", ctx.Stores, usage("stores"))
		f.DurationVar(&ctx.MaxOffset, "max-offset", ctx.MaxOffset, usage("max-offset"))
		f.DurationVar(&ctx.MetricsFrequency, "metrics-frequency", ctx.MetricsFrequency, usage("metrics-frequency"))
		f.Var(&ctx.BalanceMode, "balance-mode", usage("balance-mode"))

		// Security flags.
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, usage("certs"))
		f.BoolVar(&ctx.Insecure, "insecure", ctx.Insecure, usage("insecure"))

		// Cluster joining flags.
		f.StringVar(&ctx.JoinUsing, "join", ctx.JoinUsing, usage("join"))

		// KV flags.
		f.BoolVar(&ctx.Linearizable, "linearizable", ctx.Linearizable, usage("linearizable"))

		// Engine flags.
		f.Int64Var(&ctx.CacheSize, "cache-size", ctx.CacheSize, usage("cache-size"))
		f.Int64Var(&ctx.MemtableBudget, "memtable-budget", ctx.MemtableBudget, usage("memtable-budget"))
		f.DurationVar(&ctx.ScanInterval, "scan-interval", ctx.ScanInterval, usage("scan-interval"))
		f.DurationVar(&ctx.ScanMaxIdleTime, "scan-max-idle-time", ctx.ScanMaxIdleTime, usage("scan-max-idle-time"))
		f.DurationVar(&ctx.TimeUntilStoreDead, "time-until-store-dead", ctx.TimeUntilStoreDead, usage("time-until-store-dead"))

		if err := startCmd.MarkFlagRequired("stores"); err != nil {
			panic(err)
		}
	}

	{
		f := exterminateCmd.Flags()
		f.StringVar(&ctx.Stores, "stores", ctx.Stores, usage("stores"))
		if err := exterminateCmd.MarkFlagRequired("stores"); err != nil {
			panic(err)
		}
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, usage("certs"))
		f.IntVar(&keySize, "key-size", defaultKeySize, usage("key-size"))
		if err := cmd.MarkFlagRequired("certs"); err != nil {
			panic(err)
		}
		if err := cmd.MarkFlagRequired("key-size"); err != nil {
			panic(err)
		}
	}

	setUserCmd.Flags().StringVar(&password, "password", "", usage("password"))

	clientCmds := []*cobra.Command{
		sqlShellCmd, kvCmd, rangeCmd,
		userCmd, zoneCmd, nodeCmd,
		exterminateCmd, quitCmd, /* startCmd is covered above */
	}
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		f.BoolVar(&context.EphemeralSingleNode, "dev", context.EphemeralSingleNode, usage("dev"))
		f.BoolVar(&ctx.Insecure, "insecure", ctx.Insecure, usage("insecure"))
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, usage("certs"))
		f.StringVar(&connHost, "host", "", flagUsage["host"])
	}

	{
		f := sqlShellCmd.Flags()
		f.BoolVarP(&ctx.OneShotSQL, "execute", "e", ctx.OneShotSQL, flagUsage["execute"])
	}

	// Commands that need the cockroach port.
	simpleCmds := []*cobra.Command{kvCmd, nodeCmd, rangeCmd, exterminateCmd, quitCmd}
	for _, cmd := range simpleCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&connPort, "port", "26257", flagUsage["port"])
	}

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, userCmd, zoneCmd}
	for _, cmd := range sqlCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&connURL, "url", "", flagUsage["url"])

		f.StringVar(&connUser, "user", security.RootUser, flagUsage["user"])
		f.StringVar(&connPGPort, "pgport", "15432", flagUsage["pgport"])
		f.StringVar(&connDBName, "database", "", flagUsage["database"])
	}

	// Max results flag for scan, reverse scan, and range list.
	for _, cmd := range []*cobra.Command{scanCmd, reverseScanCmd, lsRangesCmd} {
		f := cmd.Flags()
		f.Int64Var(&maxResults, "max-results", 1000, usage("max-results"))
	}
}

func init() {
	initFlags(context)

	cobra.OnInitialize(func() {
		if context.EphemeralSingleNode {
			context.Insecure = true
		}
		context.Addr = net.JoinHostPort(connHost, connPort)
		context.PGAddr = net.JoinHostPort(connHost, connPGPort)
	})
}
