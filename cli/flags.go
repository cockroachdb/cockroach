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
	"reflect"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
)

var maxResults int64

var connURL string
var connUser, connHost, connCockroachPort, connPort, connDBName string

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
	"attrs": `
        An ordered, colon-separated list of node attributes. Attributes are
        arbitrary strings specifying topography or machine
        capabilities. Topography might include datacenter designation
        (e.g. "us-west-1a", "us-west-1b", "us-east-1c"). Machine capabilities
        might include specialized hardware or number of cores (e.g. "gpu",
        "x16c"). The relative geographic proximity of two nodes is inferred
        from the common prefix of the attributes list, so topographic
        attributes should be specified first and in the same order for all
        nodes. For example:

          --attrs=us-west-1b,gpu
`,
	"balance-mode": `
        Determines the criteria used by nodes to make balanced allocation
        decisions.  Valid options are "usage" (default) or "rangecount".
`,
	"cache-size": `
        Total size in bytes for caches, shared evenly if there are multiple
        storage devices.
`,
	"certs": `
        Directory containing RSA key and x509 certs. This flag is required if
        --insecure=false.
`,
	"cockroach-port": `
        The port to bind for cockroach traffic.
`,
	"database": `
        Database to use.
`,
	"dev": `
        Runs the node as a standalone in-memory cluster and forces --insecure
        for all server and client commands. Useful for developing Cockroach
        itself.
`,
	"gossip": `
        A comma-separated list of gossip addresses or resolvers for gossip
        bootstrap. Each item in the list has an optional type:
        [type=]<address>. An unspecified type means ip address or dns.
        Type is one of:
        - tcp: (default if type is omitted): plain ip address or hostname,
          or "self" for single-node systems.
        - unix: unix socket
        - lb: RPC load balancer forwarding to an arbitrary node
        - http-lb: HTTP load balancer: we query
          http(s)://<address>/_status/details/local
`,
	"host": `
        Database server host.
`,
	"insecure": `
        Run over plain HTTP. WARNING: this is strongly discouraged.
`,
	"key-size": `
        Key size in bits for CA/Node/Client certificates.
`,
	"linearizable": `
        Enables linearizable behaviour of operations on this node by making
        sure that no commit timestamp is reported back to the client until all
        other node clocks have necessarily passed it.
`,
	"max-offset": `
        The maximum clock offset for the cluster. Clock offset is measured on
        all node-to-node links and if any node notices it has clock offset in
        excess of --max-offset, it will commit suicide. Setting this value too
        high may decrease transaction performance in the presence of
        contention.
`,
	"max-results": `
        Define the maximum number of results that will be retrieved.
`,
	"memtable-budget": `
        Total size in bytes for memtables, shared evenly if there are multiple
        storage devices.
`,
	"metrics-frequency": `
        Adjust the frequency at which the server records its own internal metrics.
`,
	"password": `
        The created user's password. If provided, disables prompting. Pass '-' to provide
        the password on standard input.
`,
	"port": `
        The port to bind for Postgres traffic.
`,
	"scan-interval": `
        Adjusts the target for the duration of a single scan through a store's
        ranges. The scan is slowed as necessary to approximately achieve this
        duration.
`,
	"scan-max-idle-time": `
        Adjusts the max idle time of the scanner. This speeds up the scanner on small
        clusters to be more responsive.
`,
	"stores": `
        A comma-separated list of stores, specified by a colon-separated list
        of device attributes followed by '=' and either a filepath for a
        persistent store or an integer size in bytes for an in-memory
        store. Device attributes typically include whether the store is flash
        (ssd), spinny disk (hdd), fusion-io (fio), in-memory (mem); device
        attributes might also include speeds and other specs (7200rpm,
        200kiops, etc.). For example:

          --stores=hdd:7200rpm=/mnt/hda1,ssd=/mnt/ssd01,ssd=/mnt/ssd02,mem=1073741824
`,
	"time-until-store-dead": `
        Adjusts the timeout for stores.  If there's been no gossiped updated
        from a store after this time, the store is considered unavailable.
        Replicas on an unavailable store will be moved to available ones.
`,
	"url": `
        Connection url. eg: postgresql://myuser@localhost:15432/mydb
        If left empty, the connection flags are used (host, port, user, database, insecure, certs).
`,
	"user": `
        Database user name.
`,
}

func normalizeStdFlagName(s string) string {
	return strings.Replace(s, "_", "-", -1)
}

// initFlags sets the server.Context values to flag values.
// Keep in sync with "server/context.go". Values in Context should be
// settable here.
func initFlags(ctx *server.Context) {
	// Map any flags registered in the standard "flag" package into the
	// top-level cockroach command.
	pf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		pf.Var(pflagValue{f.Value}, normalizeStdFlagName(f.Name), f.Usage)
	})

	{
		f := initCmd.Flags()
		f.StringVar(&ctx.Stores, "stores", ctx.Stores, flagUsage["stores"])
		if err := initCmd.MarkFlagRequired("stores"); err != nil {
			panic(err)
		}
	}

	{
		f := startCmd.Flags()
		f.BoolVar(&ctx.EphemeralSingleNode, "dev", ctx.EphemeralSingleNode, flagUsage["dev"])

		// Server flags.
		f.StringVar(&connHost, "host", "", flagUsage["host"])
		f.StringVar(&connCockroachPort, "cockroach-port", "26257", flagUsage["cockroach-port"])
		f.StringVar(&connPort, "port", "15432", flagUsage["port"])
		f.StringVar(&ctx.Attrs, "attrs", ctx.Attrs, flagUsage["attrs"])
		f.StringVar(&ctx.Stores, "stores", ctx.Stores, flagUsage["stores"])
		f.DurationVar(&ctx.MaxOffset, "max-offset", ctx.MaxOffset, flagUsage["max-offset"])
		f.DurationVar(&ctx.MetricsFrequency, "metrics-frequency", ctx.MetricsFrequency, flagUsage["metrics-frequency"])
		f.Var(&ctx.BalanceMode, "balance-mode", flagUsage["balance-mode"])

		// Security flags.
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, flagUsage["certs"])
		f.BoolVar(&ctx.Insecure, "insecure", ctx.Insecure, flagUsage["insecure"])

		// Gossip flags.
		f.StringVar(&ctx.GossipBootstrap, "gossip", ctx.GossipBootstrap, flagUsage["gossip"])

		// KV flags.
		f.BoolVar(&ctx.Linearizable, "linearizable", ctx.Linearizable, flagUsage["linearizable"])

		// Engine flags.
		f.Int64Var(&ctx.CacheSize, "cache-size", ctx.CacheSize, flagUsage["cache-size"])
		f.Int64Var(&ctx.MemtableBudget, "memtable-budget", ctx.MemtableBudget, flagUsage["memtable-budget"])
		f.DurationVar(&ctx.ScanInterval, "scan-interval", ctx.ScanInterval, flagUsage["scan-interval"])
		f.DurationVar(&ctx.ScanMaxIdleTime, "scan-max-idle-time", ctx.ScanMaxIdleTime, flagUsage["scan-max-idle-time"])
		f.DurationVar(&ctx.TimeUntilStoreDead, "time-until-store-dead", ctx.TimeUntilStoreDead, flagUsage["time-until-store-dead"])

		if err := startCmd.MarkFlagRequired("gossip"); err != nil {
			panic(err)
		}
		if err := startCmd.MarkFlagRequired("stores"); err != nil {
			panic(err)
		}
	}

	{
		f := exterminateCmd.Flags()
		f.StringVar(&ctx.Stores, "stores", ctx.Stores, flagUsage["stores"])
		if err := exterminateCmd.MarkFlagRequired("stores"); err != nil {
			panic(err)
		}
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, flagUsage["certs"])
		f.IntVar(&keySize, "key-size", defaultKeySize, flagUsage["key-size"])
		if err := cmd.MarkFlagRequired("certs"); err != nil {
			panic(err)
		}
		if err := cmd.MarkFlagRequired("key-size"); err != nil {
			panic(err)
		}
	}

	setUserCmd.Flags().StringVar(&password, "password", "", flagUsage["password"])

	clientCmds := []*cobra.Command{
		sqlShellCmd, kvCmd, rangeCmd,
		userCmd, zoneCmd,
		exterminateCmd, quitCmd, /* startCmd is covered above */
	}
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		f.BoolVar(&context.EphemeralSingleNode, "dev", context.EphemeralSingleNode, flagUsage["dev"])
		f.BoolVar(&ctx.Insecure, "insecure", ctx.Insecure, flagUsage["insecure"])
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, flagUsage["certs"])
		f.StringVar(&connHost, "host", "", flagUsage["host"])
	}

	// Commands that need the cockroach port.
	simpleCmds := []*cobra.Command{kvCmd, rangeCmd, exterminateCmd, quitCmd}
	for _, cmd := range simpleCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&connCockroachPort, "cockroach-port", "26257", flagUsage["cockroach-port"])
	}

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, userCmd, zoneCmd}
	for _, cmd := range sqlCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&connURL, "url", "", flagUsage["url"])

		f.StringVar(&connUser, "user", security.RootUser, flagUsage["user"])
		f.StringVar(&connPort, "port", "15432", flagUsage["port"])
		f.StringVar(&connDBName, "database", "", flagUsage["database"])
	}

	// Max results flag for scan, reverse scan, and range list.
	for _, cmd := range []*cobra.Command{scanCmd, reverseScanCmd, lsRangesCmd} {
		f := cmd.Flags()
		f.Int64Var(&maxResults, "max-results", 1000, flagUsage["max-results"])
	}
}

func init() {
	initFlags(context)

	cobra.OnInitialize(func() {
		if context.EphemeralSingleNode {
			context.Insecure = true
		}
		context.Addr = fmt.Sprintf("%s:%s", connHost, connCockroachPort)
		context.PGAddr = fmt.Sprintf("%s:%s", connHost, connPort)
	})
}
