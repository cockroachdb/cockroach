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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Daniel Theophanes (kardianos@gmail.com)

package cli

import (
	"flag"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/server"

	"github.com/spf13/cobra"
)

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
	"addr": `
        The host:port to bind for HTTP/RPC traffic
`,
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

          --attrs=us-west-1b,gpu.
`,
	"cache-size": `
        Total size in bytes for caches, shared evenly if there are multiple
        storage devices.
`,
	"certs": `
        Directory containing RSA key and x509 certs. This flag is required if
        --insecure=false.
`,
	"gossip": `
        A comma-separated list of gossip addresses or resolvers for gossip
        bootstrap. Each item in the list has an optional type:
        [type=]<address>. An unspecified type means ip address or dns.
        Type is one of:
        - tcp: (default if type is omitted): plain ip address or hostname,
          or "self" for single-node systems.
        - unix: unix socket
        - lb: RPC load balancer fowarding to an arbitrary node
        - http-lb: HTTP load balancer: we query http(s)://<address>/_status/local
`,
	"gossip-interval": `
        Approximate interval (time.Duration) for gossiping new information to peers.
`,
	"linearizable": `
        Enables linearizable behaviour of operations on this node by making
        sure that no commit timestamp is reported back to the client until all
        other node clocks have necessarily passed it.
`,
	"insecure": `
        Run over plain HTTP. WARNING: this is strongly discouraged.
`,
	"max-offset": `
        The maximum clock offset for the cluster. Clock offset is measured on
        all node-to-node links and if any node notices it has clock offset in
        excess of --max-offset, it will commit suicide. Setting this value too
        high may decrease transaction performance in the presence of
        contention.
`,
	"metrics-frequency": `
        Adjust the frequency at which the server records its own internal metrics.
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

          --stores=hdd:7200rpm=/mnt/hda1,ssd=/mnt/ssd01,ssd=/mnt/ssd02,mem=1073741824.
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

	if f := initCmd.Flags(); true {
		f.StringVar(&ctx.Stores, "stores", ctx.Stores, flagUsage["stores"])
		initCmd.MarkFlagRequired("stores")
	}

	if f := startCmd.Flags(); true {
		// Server flags.
		f.StringVar(&ctx.Addr, "addr", ctx.Addr, flagUsage["addr"])
		f.StringVar(&ctx.Attrs, "attrs", ctx.Attrs, flagUsage["attrs"])
		f.StringVar(&ctx.Stores, "stores", ctx.Stores, flagUsage["stores"])
		f.DurationVar(&ctx.MaxOffset, "max-offset", ctx.MaxOffset, flagUsage["max-offset"])
		f.DurationVar(&ctx.MetricsFrequency, "metrics-frequency", ctx.MetricsFrequency,
			flagUsage["metrics-frequency"])

		// Security flags.
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, flagUsage["certs"])
		f.BoolVar(&ctx.Insecure, "insecure", ctx.Insecure, flagUsage["insecure"])

		// Gossip flags.
		f.StringVar(&ctx.GossipBootstrap, "gossip", ctx.GossipBootstrap, flagUsage["gossip"])
		f.DurationVar(&ctx.GossipInterval, "gossip-interval", ctx.GossipInterval,
			flagUsage["gossip-interval"])

		// KV flags.
		f.BoolVar(&ctx.Linearizable, "linearizable", ctx.Linearizable, flagUsage["linearizable"])

		// Engine flags.
		f.Int64Var(&ctx.CacheSize, "cache-size", ctx.CacheSize, flagUsage["cache-size"])
		f.DurationVar(&ctx.ScanInterval, "scan-interval", ctx.ScanInterval, flagUsage["scan-interval"])
		f.DurationVar(&ctx.ScanMaxIdleTime, "scan-max-idle-time", ctx.ScanMaxIdleTime,
			flagUsage["scan-max-idle-time"])

		startCmd.MarkFlagRequired("gossip")
		startCmd.MarkFlagRequired("stores")
	}

	if f := exterminateCmd.Flags(); true {
		f.StringVar(&ctx.Stores, "stores", ctx.Stores, flagUsage["stores"])
		exterminateCmd.MarkFlagRequired("stores")
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, flagUsage["certs"])
		cmd.MarkFlagRequired("certs")
	}

	clientCmds := []*cobra.Command{kvCmd, rangeCmd, acctCmd, permCmd, zoneCmd, quitCmd}
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&ctx.Addr, "addr", ctx.Addr, flagUsage["addr"])
		f.BoolVar(&ctx.Insecure, "insecure", ctx.Insecure, flagUsage["insecure"])
		f.StringVar(&ctx.Certs, "certs", ctx.Certs, flagUsage["certs"])
	}
}

func init() {
	initFlags(Context)
}
