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
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/kr/text"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
)

var maxResults int64

var connURL string
var connUser, connHost, connPort, connDBName string

// cliContext is the CLI Context used for the command-line client.
var cliContext = NewContext()
var cacheSize *bytesValue

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

  --attrs=us-west-1b:gpu
`,

	"balance-mode": wrapText(`
Determines the criteria used by nodes to make balanced allocation
decisions. Valid options are "usage" (default) or "rangecount".`),

	"cache-size": wrapText(`
Total size in bytes for caches, shared evenly if there are multiple
storage devices. Size suffixes are supported (e.g. 1GB and 1GiB).`),

	"certs": wrapText(`
Directory containing RSA key and x509 certs. This flag is required if
--insecure=false.`),

	"client_host": wrapText(`
Database server host to connect to.`),

	"client_port": wrapText(`
Database server port to connect to.`),

	"database": wrapText(`
The name of the database to connect to.`),

	"execute": wrapText(`
Execute the SQL statement(s) on the command line, then exit. Each
subsequent positional argument on the command line may contain
one or more SQL statements, separated by semicolons. If an
error occurs in any statement, the command exits with a
non-zero status code and further statements are not
executed. The results of each SQL statement are printed on
the standard output.`),

	"join": wrapText(`
A comma-separated list of addresses to use when a new node is joining
an existing cluster. For the first node in a cluster, --join should
NOT be specified. Each address in the list has an optional type:
[type=]<address>. An unspecified type means ip address or dns. Type
is one of:`) + `

  - tcp: (default if type is omitted): plain ip address or hostname.
  - http-lb: HTTP load balancer: we query
             http(s)://<address>/_status/details/local
`,

	"server_host": wrapText(`
The address to listen on. The node will also advertise itself using this
hostname; it must resolve from other nodes in the cluster.`),

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
storage devices. Size suffixes are supported (e.g. 1GB and 1GiB).`),

	"metrics-frequency": wrapText(`
Adjust the frequency at which the server records its own internal metrics.`),

	"password": wrapText(`
The created user's password. If provided, disables prompting. Pass '-' to
provide the password on standard input.`),

	"server_port": wrapText(`
The port to bind to.`),

	"scan-interval": wrapText(`
Adjusts the target for the duration of a single scan through a store's
ranges. The scan is slowed as necessary to approximately achieve this
duration.`),

	"scan-max-idle-time": wrapText(`
Adjusts the max idle time of the scanner. This speeds up the scanner on small
clusters to be more responsive.`),

	"store": wrapText(`
The file path to a storage device. This flag must be specified separately for
each storage device, for example:`) + `

  --store=/mnt/ssd01 --store=/mnt/ssd02 --store=/mnt/hda1

` + wrapText(`
For each store, the "attrs" and "size" fields can be used to specify device
attributes and a maximum store size (see below). When one or both of these
fields are set, the "path" field label must be used for the path to the storage
device, for example:`) + `

  --store=path=/mnt/ssd01,attrs=ssd,size=20GiB

` + wrapText(`
In most cases, node-level attributes are preferable to store-level attributes.
However, the "attrs" field can be used to match capabilities for storage of
individual databases or tables. For example, an OLTP database would probably
want to allocate space for its tables only on solid state devices, whereas
append-only time series might prefer cheaper spinning drives. Typical
attributes include whether the store is flash (ssd), spinny disk (hdd), or
in-memory (mem), as well as speeds and other specs. Attributes can be arbitrary
strings separated by colons, for example: :`) + `

  --store=path=/mnt/hda1,attrs=hdd:7200rpm

` + wrapText(`
The store size in the "size" field is not a guaranteed maximum but is used when
calculating free space for rebalancing purposes. The size can be specified
either in a bytes-based unit or as a percentage of hard drive space,
for example: :`) + `

  --store=path=/mnt/ssd01,size=10000000000     -> 10000000000 bytes
  --store-path=/mnt/ssd01,size=20GB            -> 20000000000 bytes
  --store-path=/mnt/ssd01,size=20GiB           -> 21474836480 bytes
  --store-path=/mnt/ssd01,size=0.02TiB         -> 21474836480 bytes
  --store=path=/mnt/ssd01,size=20%             -> 20% of available space
  --store=path=/mnt/ssd01,size=0.2             -> 20% of available space
  --store=path=/mnt/ssd01,size=.2              -> 20% of available space

` + wrapText(`
For an in-memory store, the "type" and "size" fields are required, and the
"path" field is forbidden. The "type" field must be set to "mem", and the
"size" field must be set to the true maximum bytes or percentage of available
memory that the store may consume, for example:`) + `

  --store=type=mem,size=20GiB
  --store=type=mem,size=90%

` + wrapText(`
Commas are forbidden in all values, since they are used to separate fields.
Also, if you use equal signs in the file path to a store, you must use the
"path" field label.`),
	"time-until-store-dead": wrapText(`
Adjusts the timeout for stores. If there's been no gossiped update
from a store after this time, the store is considered unavailable.
Replicas on an unavailable store will be moved to available ones.`),

	"url": wrapText(`
Connection url. eg: postgresql://myuser@localhost:26257/mydb
If left empty, the connection flags are used (host, port, user,
database, insecure, certs).`),

	"user": wrapText(`
Database user name.`),
}

const usageIndentation = 8
const wrapWidth = 79 - usageIndentation

type bytesValue struct {
	val   *uint64
	isSet bool
}

func newBytesValue(val *uint64) *bytesValue {
	return &bytesValue{val: val}
}

func (b *bytesValue) Set(s string) error {
	v, err := humanize.ParseBytes(s)
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
	return humanize.IBytes(*b.val)
}

func wrapText(s string) string {
	return text.Wrap(s, wrapWidth)
}

func usage(name string) string {
	s, ok := flagUsage[name]
	if !ok {
		panic(fmt.Sprintf("flag usage not defined for %q", name))
	}
	s = "\n" + strings.TrimSpace(s) + "\n"
	// github.com/spf13/pflag appends the default value after the usage text. Add
	// the correct indentation (7 spaces) here. This is admittedly fragile.
	return text.Indent(s, strings.Repeat(" ", usageIndentation)) +
		strings.Repeat(" ", usageIndentation-1)
}

// initFlags sets the cli.Context values to flag values.
// Keep in sync with "server/context.go". Values in Context should be
// settable here.
func initFlags(ctx *Context) {
	// Change the logging defaults for the main cockroach binary.
	if err := flag.Lookup("logtostderr").Value.Set("false"); err != nil {
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
	pf.Lookup("log-dir").DefValue = ""
	// If no value is specified for --alsologtostderr output everything.
	pf.Lookup("alsologtostderr").NoOptDefVal = "INFO"

	{
		f := startCmd.Flags()

		// Server flags.
		f.StringVar(&connHost, "host", "", usage("server_host"))
		f.StringVarP(&connPort, "port", "p", base.DefaultPort, usage("server_port"))
		f.StringVar(&ctx.Attrs, "attrs", ctx.Attrs, usage("attrs"))
		f.VarP(&ctx.Stores, "store", "s", usage("store"))
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
		cacheSize = newBytesValue(&ctx.CacheSize)
		f.Var(cacheSize, "cache-size", usage("cache-size"))
		f.Var(newBytesValue(&ctx.MemtableBudget), "memtable-budget", usage("memtable-budget"))
		f.DurationVar(&ctx.ScanInterval, "scan-interval", ctx.ScanInterval, usage("scan-interval"))
		f.DurationVar(&ctx.ScanMaxIdleTime, "scan-max-idle-time", ctx.ScanMaxIdleTime, usage("scan-max-idle-time"))
		f.DurationVar(&ctx.TimeUntilStoreDead, "time-until-store-dead", ctx.TimeUntilStoreDead, usage("time-until-store-dead"))

		// Clear the cache-size default value. This flag does have a default, but
		// it is set only when the "start" command is run.
		f.Lookup("cache-size").DefValue = ""

		if err := startCmd.MarkFlagRequired("store"); err != nil {
			panic(err)
		}
	}

	{
		f := exterminateCmd.Flags()
		f.Var(&ctx.Stores, "store", usage("store"))
		if err := exterminateCmd.MarkFlagRequired("store"); err != nil {
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
		f.BoolVar(&ctx.Insecure, "insecure", ctx.Insecure, usage("insecure"))
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, usage("certs"))
		f.StringVar(&connHost, "host", "", usage("client_host"))
	}

	{
		f := sqlShellCmd.Flags()
		f.BoolVarP(&ctx.OneShotSQL, "execute", "e", ctx.OneShotSQL, usage("execute"))
	}

	// Commands that need the cockroach port.
	simpleCmds := []*cobra.Command{kvCmd, nodeCmd, rangeCmd, exterminateCmd, quitCmd}
	for _, cmd := range simpleCmds {
		f := cmd.PersistentFlags()
		f.StringVarP(&connPort, "port", "p", base.DefaultPort, usage("client_port"))
	}

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, userCmd, zoneCmd}
	for _, cmd := range sqlCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&connURL, "url", "", usage("url"))

		f.StringVarP(&connUser, "user", "u", security.RootUser, usage("user"))
		f.StringVarP(&connPort, "port", "p", base.DefaultPort, usage("client_port"))
		f.StringVarP(&connDBName, "database", "d", "", usage("database"))
	}

	// Max results flag for scan, reverse scan, and range list.
	for _, cmd := range []*cobra.Command{scanCmd, reverseScanCmd, lsRangesCmd} {
		f := cmd.Flags()
		f.Int64Var(&maxResults, "max-results", 1000, usage("max-results"))
	}
}

func init() {
	initFlags(cliContext)

	cobra.OnInitialize(func() {
		cliContext.Addr = net.JoinHostPort(connHost, connPort)
	})
}
