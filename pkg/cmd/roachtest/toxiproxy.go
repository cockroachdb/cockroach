// Copyright 2018 The Cockroach Authors.
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

package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
)

// cockroachToxiWrapper replaces the cockroach binary. It modifies the listening port so
// that the nodes in the cluster will communicate through toxiproxy instead of
// directly.
const cockroachToxiWrapper = `#!/usr/bin/env bash
set -eu

cd "$(dirname "${0}")"

orig_port=""

args=()

if [[ "$1" != "start" ]]; then
	./cockroach.real "$@"
	exit $?
fi

for arg in "$@"; do
	capture=$(echo "${arg}" | sed -E 's/^--port=([0-9]+)$/\1/')
	if [[ "${capture}" != "${arg}"  ]] && [[ -z "${orig_port}" ]] && [[ -n "${capture}" ]]; then
		orig_port="${capture}"
	fi
	args+=("${arg}")
done

if [[ -z "${orig_port}" ]]; then
	orig_port=26257
fi

args+=("--advertise-port=$((orig_port+10000))")

echo "toxiproxy interception:"
echo "original args: $@"
echo "modified args: ${args[@]}"
./cockroach.real "${args[@]}"
`

const toxiServerWrapper = `#!/usr/bin/env bash
set -eu

mkdir -p logs
./toxiproxy-server -host 0.0.0.0 -port $1 2>&1 > logs/toxiproxy.log & </dev/null
until nc -z localhost $1; do sleep 0.1; echo "waiting for toxiproxy-server..."; done
`

// A ToxiCluster wraps a cluster and sets it up for use with toxiproxy.
// See Toxify() for details.
type ToxiCluster struct {
	*cluster
	toxClients map[int]*toxiproxy.Client
	toxProxies map[int]*toxiproxy.Proxy
}

// Toxify takes a cluster and sets it up for use with toxiproxy on the given
// nodes. On these nodes, the cockroach binary must already have been populated
// and the cluster must not have been started yet. The returned ToxiCluster
// wraps the original cluster, whose returned addresses will all go through
// toxiproxy. The upstream (i.e. non-intercepted) addresses are accessible via
// getters prefixed with "External".
func Toxify(ctx context.Context, c *cluster, node nodeListOption) *ToxiCluster {
	// NB: we can use upstream once they've released 2.1.4; there's a pretty
	// dramatic perf problem with the "timeout" toxic in 2.1.3.
	toxiURL := "https://github.com/tbg/toxiproxy/releases/download/v2.1.4rc1/toxiproxy-server-linux-amd64"
	if local && runtime.GOOS == "darwin" {
		toxiURL = "https://github.com/tbg/toxiproxy/releases/download/v2.1.4rc1/toxiproxy-server-darwin-amd64"
	}
	c.Run(ctx, c.All(), "curl", "-Lo", "toxiproxy-server", toxiURL)
	c.Run(ctx, c.All(), "chmod", "+x", "toxiproxy-server")

	c.Run(ctx, node, "mv cockroach cockroach.real")
	c.PutString(ctx, cockroachToxiWrapper, "./cockroach", 0755, node)
	c.PutString(ctx, toxiServerWrapper, "./toxiproxyd", 0755, node)

	tc := &ToxiCluster{
		cluster:    c,
		toxClients: make(map[int]*toxiproxy.Client),
		toxProxies: make(map[int]*toxiproxy.Proxy),
	}

	for _, i := range node {
		n := c.Node(i)

		toxPort := 8474 + i
		c.Run(ctx, n, fmt.Sprintf("./toxiproxyd %d 2>/dev/null >/dev/null < /dev/null", toxPort))

		externalAddr, port := addrToHostPort(c, c.ExternalAddr(ctx, n)[0])
		tc.toxClients[i] = toxiproxy.NewClient(fmt.Sprintf("http://%s:%d", externalAddr, toxPort))
		proxy, err := tc.toxClients[i].CreateProxy("cockroach", fmt.Sprintf(":%d", tc.poisonedPort(port)), fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			c.t.Fatal(err)
		}
		tc.toxProxies[i] = proxy
	}

	return tc
}

func (tc *ToxiCluster) poisonedPort(port int) int {
	// NB: to make a change here, you also have to change
	_ = cockroachToxiWrapper
	return port + 10000
}

// Proxy returns the toxiproxy Proxy intercepting the given node's traffic.
func (tc *ToxiCluster) Proxy(i int) *toxiproxy.Proxy {
	proxy, found := tc.toxProxies[i]
	if !found {
		tc.cluster.t.Fatalf("proxy for node %d not found", i)
	}
	return proxy
}

// ExternalAddr gives the external host:port of the node(s), bypassing the
// toxiproxy interception.
func (tc *ToxiCluster) ExternalAddr(ctx context.Context, node nodeListOption) []string {
	return tc.cluster.ExternalAddr(ctx, node)
}

// PoisonedExternalAddr gives the external host:port of the toxiproxy process
// for the given nodes (i.e. the connection will be affected by toxics).
func (tc *ToxiCluster) PoisonedExternalAddr(ctx context.Context, node nodeListOption) []string {
	var out []string

	extAddrs := tc.ExternalAddr(ctx, node)
	for _, addr := range extAddrs {
		host, port := addrToHostPort(tc.cluster, addr)
		out = append(out, fmt.Sprintf("%s:%d", host, tc.poisonedPort(port)))
	}
	return out
}

// PoisonedPGAddr gives a connection to the given node that passes through toxiproxy.
func (tc *ToxiCluster) PoisonedPGAddr(ctx context.Context, node nodeListOption) []string {
	var out []string

	urls := tc.ExternalPGUrl(ctx, node)
	exts := tc.PoisonedExternalAddr(ctx, node)
	for i, s := range urls {
		u, err := url.Parse(s)
		if err != nil {
			tc.cluster.t.Fatal(err)
		}
		u.Host = exts[i]
		out = append(out, u.String())
	}
	return out
}

// PoisonedConn returns an SQL connection to the specified node through toxiproxy.
func (tc *ToxiCluster) PoisonedConn(ctx context.Context, node int) *gosql.DB {
	url := tc.PoisonedPGAddr(ctx, tc.cluster.Node(node))[0]
	db, err := gosql.Open("postgres", url)
	if err != nil {
		tc.cluster.t.Fatal(err)
	}
	return db
}

var _ = (*ToxiCluster)(nil).PoisonedConn
var _ = (*ToxiCluster)(nil).PoisonedPGAddr
var _ = (*ToxiCluster)(nil).PoisonedExternalAddr

var measureRE = regexp.MustCompile(`real[^0-9]+([0-9.]+)`)

// Measure runs a statement on the given node (bypassing toxiproxy for the
// client connection) and measures the duration (including the invocation time
// of `./cockroach sql`. This is simplistic and does not perform proper
// escaping. It's not useful for anything but simple sanity checks.
func (tc *ToxiCluster) Measure(ctx context.Context, fromNode int, stmt string) time.Duration {
	_, port := addrToHostPort(tc.cluster, tc.ExternalAddr(ctx, tc.Node(fromNode))[0])
	b, err := tc.cluster.RunWithBuffer(ctx, tc.cluster.l, tc.cluster.Node(fromNode), "time", "-p", "./cockroach", "sql", "--insecure", "--port", strconv.Itoa(port), "-e", "'"+stmt+"'")
	tc.cluster.l.Printf("%s\n", b)
	if err != nil {
		tc.cluster.t.Fatal(err)
	}
	matches := measureRE.FindSubmatch(b)
	if len(matches) != 2 {
		tc.cluster.t.Fatalf("unable to extract duration from output: %s", b)
	}
	f, err := strconv.ParseFloat(string(matches[1]), 64)
	if err != nil {
		tc.cluster.t.Fatalf("unable to parse %s as float: %s", b, err)
	}
	return time.Duration(f * 1E9)
}
