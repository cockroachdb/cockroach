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

	"github.com/cockroachdb/cockroach/server"
)

// initFlags sets the server.Context values to flag values.
// Keep in sync with "server/context.go". Values in Context should be
// settable here.
func initFlags(ctx *server.Context) {
	// Server flags.
	flag.StringVar(&ctx.Addr, "addr", ctx.Addr, "when run as the server the host:port to bind for "+
		"HTTP/RPC traffic; when run as the client the address for connection to the cockroach cluster")

	flag.StringVar(&ctx.Certs, "certs", ctx.Certs, "directory containing RSA key and x509 certs")

	flag.StringVar(&ctx.Stores, "stores", ctx.Stores, "specify a comma-separated list of stores, "+
		"specified by a colon-separated list of device attributes followed by '=' and "+
		"either a filepath for a persistent store or an integer size in bytes for an "+
		"in-memory store. Device attributes typically include whether the store is "+
		"flash (ssd), spinny disk (hdd), fusion-io (fio), in-memory (mem); device "+
		"attributes might also include speeds and other specs (7200rpm, 200kiops, etc.). "+
		"For example, -store=hdd:7200rpm=/mnt/hda1,ssd=/mnt/ssd01,ssd=/mnt/ssd02,mem=1073741824")

	flag.StringVar(&ctx.Attrs, "attrs", ctx.Attrs, "specify a colon-separated list of node "+
		"attributes. Attributes are arbitrary strings specifying topography or "+
		"machine capabilities. Topography might include datacenter designation (e.g. "+
		"\"us-west-1a\", \"us-west-1b\", \"us-east-1c\"). Machine capabilities "+
		"might include specialized hardware or number of cores (e.g. \"gpu\", "+
		"\"x16c\"). For example: -attrs=us-west-1b,gpu")

	flag.DurationVar(&ctx.MaxOffset, "max_offset", ctx.MaxOffset, "specify "+
		"the maximum clock offset for the cluster. Clock offset is measured on all "+
		"node-to-node links and if any node notices it has clock offset in excess "+
		"of -max_offset, it will commit suicide. Setting this value too high may "+
		"decrease transaction performance in the presence of contention.")

	flag.BoolVar(&ctx.InitAndStart, "init_and_start", ctx.InitAndStart, "specify "+
		"to start the server after bootstrapping with the init command.")

	// Gossip flags.

	flag.StringVar(&ctx.GossipBootstrap, "gossip", ctx.GossipBootstrap,
		"addresses (comma-separated host:port pairs) of node addresses for gossip bootstrap")

	flag.DurationVar(&ctx.GossipInterval, "gossip_interval", ctx.GossipInterval,
		"approximate interval (time.Duration) for gossiping new information to peers")

	// KV flags.

	flag.BoolVar(&ctx.Linearizable, "linearizable", ctx.Linearizable, "enables linearizable behaviour "+
		"of operations on this node by making sure that no commit timestamp is reported "+
		"back to the client until all other node clocks have necessarily passed it.")

	// Engine flags.

	flag.Int64Var(&ctx.CacheSize, "cache_size", ctx.CacheSize, "total size in bytes for "+
		"caches, shared evenly if there are multiple storage devices")
}

func init() {
	initFlags(Context)
}
