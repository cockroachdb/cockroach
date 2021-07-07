// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/errors"
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
	t test.Test
	cluster.Cluster
	toxClients map[int]*toxiproxy.Client
	toxProxies map[int]*toxiproxy.Proxy
}

// Toxify takes a cluster and sets it up for use with toxiproxy on the given
// nodes. On these nodes, the cockroach binary must already have been populated
// and the cluster must not have been started yet. The returned ToxiCluster
// wraps the original cluster, whose returned addresses will all go through
// toxiproxy. The upstream (i.e. non-intercepted) addresses are accessible via
// getters prefixed with "External".
func Toxify(
	ctx context.Context, t test.Test, c cluster.Cluster, node option.NodeListOption,
) (*ToxiCluster, error) {
	toxiURL := "https://github.com/Shopify/toxiproxy/releases/download/v2.1.4/toxiproxy-server-linux-amd64"
	if c.IsLocal() && runtime.GOOS == "darwin" {
		toxiURL = "https://github.com/Shopify/toxiproxy/releases/download/v2.1.4/toxiproxy-server-darwin-amd64"
	}
	if err := func() error {
		if err := c.RunE(ctx, c.All(), "curl", "-Lfo", "toxiproxy-server", toxiURL); err != nil {
			return err
		}
		if err := c.RunE(ctx, c.All(), "chmod", "+x", "toxiproxy-server"); err != nil {
			return err
		}

		if err := c.RunE(ctx, node, "mv cockroach cockroach.real"); err != nil {
			return err
		}
		if err := c.PutString(ctx, cockroachToxiWrapper, "./cockroach", 0755, node); err != nil {
			return err
		}
		return c.PutString(ctx, toxiServerWrapper, "./toxiproxyd", 0755, node)
	}(); err != nil {
		return nil, errors.Wrap(err, "toxify")
	}

	tc := &ToxiCluster{
		t:          t,
		Cluster:    c,
		toxClients: make(map[int]*toxiproxy.Client),
		toxProxies: make(map[int]*toxiproxy.Proxy),
	}

	for _, i := range node {
		n := c.Node(i)

		toxPort := 8474 + i
		if err := c.RunE(ctx, n, fmt.Sprintf("./toxiproxyd %d 2>/dev/null >/dev/null < /dev/null", toxPort)); err != nil {
			return nil, errors.Wrap(err, "toxify")
		}

		externalAddrs, err := c.ExternalAddr(ctx, n)
		if err != nil {
			return nil, err
		}
		externalAddr, port, err := tc.addrToHostPort(externalAddrs[0])
		if err != nil {
			return nil, err
		}
		tc.toxClients[i] = toxiproxy.NewClient(fmt.Sprintf("http://%s:%d", externalAddr, toxPort))
		proxy, err := tc.toxClients[i].CreateProxy("cockroach", fmt.Sprintf(":%d", tc.poisonedPort(port)), fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return nil, errors.Wrap(err, "toxify")
		}
		tc.toxProxies[i] = proxy
	}

	return tc, nil
}

func (*ToxiCluster) addrToHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
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
		tc.t.Fatalf("proxy for node %d not found", i)
	}
	return proxy
}

// ExternalAddr gives the external host:port of the node(s), bypassing the
// toxiproxy interception.
func (tc *ToxiCluster) ExternalAddr(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	return tc.Cluster.ExternalAddr(ctx, node)
}

// PoisonedExternalAddr gives the external host:port of the toxiproxy process
// for the given nodes (i.e. the connection will be affected by toxics).
func (tc *ToxiCluster) PoisonedExternalAddr(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	var out []string

	extAddrs, err := tc.ExternalAddr(ctx, node)
	if err != nil {
		return nil, err
	}
	for _, addr := range extAddrs {
		host, port, err := tc.addrToHostPort(addr)
		if err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%s:%d", host, tc.poisonedPort(port)))
	}
	return out, nil
}

// PoisonedPGAddr gives a connection to the given node that passes through toxiproxy.
func (tc *ToxiCluster) PoisonedPGAddr(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	var out []string

	urls, err := tc.ExternalPGUrl(ctx, node)
	if err != nil {
		return nil, err
	}
	exts, err := tc.PoisonedExternalAddr(ctx, node)
	if err != nil {
		return nil, err
	}
	for i, s := range urls {
		u, err := url.Parse(s)
		if err != nil {
			tc.t.Fatal(err)
		}
		u.Host = exts[i]
		out = append(out, u.String())
	}
	return out, nil
}

// PoisonedConn returns an SQL connection to the specified node through toxiproxy.
func (tc *ToxiCluster) PoisonedConn(ctx context.Context, node int) *gosql.DB {
	urls, err := tc.PoisonedPGAddr(ctx, tc.Cluster.Node(node))
	if err != nil {
		tc.t.Fatal(err)
	}
	db, err := gosql.Open("postgres", urls[0])
	if err != nil {
		tc.t.Fatal(err)
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
	externalAddrs, err := tc.ExternalAddr(ctx, tc.Node(fromNode))
	if err != nil {
		tc.t.Fatal(err)
	}
	_, port, err := tc.addrToHostPort(externalAddrs[0])
	if err != nil {
		tc.t.Fatal(err)
	}
	b, err := tc.Cluster.RunWithBuffer(ctx, tc.t.L(), tc.Cluster.Node(fromNode), "time", "-p", "./cockroach", "sql", "--insecure", "--port", strconv.Itoa(port), "-e", "'"+stmt+"'")
	tc.t.L().Printf("%s\n", b)
	if err != nil {
		tc.t.Fatal(err)
	}
	matches := measureRE.FindSubmatch(b)
	if len(matches) != 2 {
		tc.t.Fatalf("unable to extract duration from output: %s", b)
	}
	f, err := strconv.ParseFloat(string(matches[1]), 64)
	if err != nil {
		tc.t.Fatalf("unable to parse %s as float: %s", b, err)
	}
	return time.Duration(f * 1e9)
}
