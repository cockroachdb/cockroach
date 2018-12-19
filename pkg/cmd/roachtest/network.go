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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"runtime"
	"strconv"
	"time"

	client "github.com/Shopify/toxiproxy/client"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/lib/pq"
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
		arg="--port=$((capture+10000))"
	fi
	args+=("${arg}")
done

if [[ -z "${orig_port}" ]]; then
	orig_port=26257
fi

args+=("--advertise-port=${orig_port}")

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

func stringToTemp(t *test, content string, mode os.FileMode) (file string, cleanup func()) {
	temp, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := temp.WriteString(content); err != nil {
		t.Fatal(err)
	}
	temp.Close()
	n := temp.Name()

	if err := os.Chmod(n, 0744); err != nil {
		t.Fatal(err)
	}

	return n, func() {
		_ = os.Remove(n)
	}
}

func runLatency(ctx context.Context, t *test, c *cluster, nodes int) {
	cockroachWrapper, undo1 := stringToTemp(t, cockroachToxiWrapper, 0755)
	defer undo1()
	toxiWrapper, undo2 := stringToTemp(t, toxiServerWrapper, 0755)
	defer undo2()

	c.Put(ctx, cockroachWrapper, "./cockroach", c.All())
	c.Put(ctx, toxiWrapper, "./toxiproxyd", c.All())
	c.Put(ctx, cockroach, "./cockroach.real", c.All())
	c.Put(ctx, workload, "./workload", c.All())

	toxiURL := "https://github.com/Shopify/toxiproxy/releases/download/v2.1.2/toxiproxy-server-linux-amd64"
	if local && runtime.GOOS == "darwin" {
		toxiURL = "https://github.com/Shopify/toxiproxy/releases/download/v2.1.2/toxiproxy-server-darwin-amd64"
	}
	c.Run(ctx, c.All(), "curl", "-Lo", "toxiproxy-server", toxiURL)
	c.Run(ctx, c.All(), "chmod", "+x", "toxiproxy-server")

	const (
		latency = 100 * time.Millisecond
	)

	realHostPort := map[int]string{} // node i -> un-toxied host:port

	for i := 1; i <= nodes; i++ {
		n := c.Node(i)

		toxHost, slowPortStr, err := net.SplitHostPort(c.ExternalAddr(ctx, n)[0])
		if err != nil {
			t.Fatal(err)
		}
		slowPort, err := strconv.Atoi(slowPortStr)
		if err != nil {
			t.Fatal(err)
		}

		toxPort := 8474 + i
		// TODO(tbg): figure out how to launch something in the background
		// and use the toxiproxyd script instead. It'll just hang every way I tried.
		c.Run(ctx, n, fmt.Sprintf("./toxiproxyd %d 2>/dev/null >/dev/null < /dev/null", toxPort))

		toxClient := client.NewClient(fmt.Sprintf("http://%s:%d", toxHost, toxPort))

		realPort := slowPort + 10000
		realHostPort[i] = net.JoinHostPort(toxHost, strconv.Itoa(realPort))
		proxy, err := toxClient.CreateProxy("cockroach", fmt.Sprintf(":%d", slowPort), fmt.Sprintf("127.0.0.1:%d", realPort))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := proxy.AddToxic("", "latency", "downstream", 1, client.Attributes{
			"latency": latency / (2 * time.Millisecond), // ms
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := proxy.AddToxic("", "latency", "upstream", 1, client.Attributes{
			"latency": latency / (2 * time.Millisecond), // ms
		}); err != nil {
			t.Fatal(err)
		}
	}
	c.Start(ctx, t, c.All())

	waitReplication := func(downNode int) error {
		db := c.Conn(ctx, nodes)
		defer db.Close()

		for ok := false; !ok; {
			if err := db.QueryRow(
				"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges WHERE array_position(replicas, $1) IS NULL",
				downNode,
			).Scan(&ok); err != nil {
				return err
			}
		}
		return nil
	}

	if err := waitReplication(0 /* no down node */); err != nil {
		t.Fatal(err)
	}

	db, err := gosql.Open("postgres", "postgresql://root@"+realHostPort[1]+"/?sslmode=disable") // unthrottled db conn
	if err != nil {
		c.t.Fatal(err)
	}
	defer db.Close()

	measure := func(ctx context.Context, stmt string, args ...interface{}) time.Duration {
		// NB: this method also eats the latency from the roachtest runner to the server,
		// which isn't ideal.
		tBegin := timeutil.Now()
		rows, err := db.QueryContext(ctx, stmt, args...)
		if err != nil {
			panic(err)
		}
		if err := rows.Close(); err != nil {
			panic(err)
		}
		c.l.Printf(stmt + "\n")
		return timeutil.Since(tBegin)
	}

	m := newMonitor(ctx, c, c.All())
	m.Go(func(ctx context.Context) error {
		measure(ctx, `SET CLUSTER SETTING trace.debug.enable = true`)
		measure(ctx, "CREATE DATABASE test")
		measure(ctx, `CREATE TABLE test.commit (a INT, b INT, v INT, PRIMARY KEY (a, b))`)

		for i := 0; i < 10; i++ {
			duration := measure(ctx, fmt.Sprintf("BEGIN; INSERT INTO test.commit VALUES (2, %[1]d), (1, %[1]d), (3, %[1]d); COMMIT", i))
			c.l.Printf("%s\n", duration)
		}

		c.RunE(ctx, c.Node(1), "./cockroach sql --insecure -e 'set tracing=on; insert into test.commit values(3,1000), (1,1000), (2,1000); select age, message from [ show trace for session ];'")
		return nil
	})

	m.Wait()
}

func registerNetwork(r *registry) {
	const numNodes = 4

	r.Add(testSpec{
		Name:  fmt.Sprintf("network/latency/nodes=%d", numNodes),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runLatency(ctx, t, c, numNodes)
		},
	})
}
