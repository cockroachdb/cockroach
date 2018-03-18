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
	"os"
	"runtime"
	"time"

	client "github.com/Shopify/toxiproxy/client"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

const toxiWrapper = `#!/usr/bin/env bash
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

func runLatency(ctx context.Context, t *test, c *cluster, nodes int) {
	temp, err := ioutil.TempFile("", "toxiproxy-wrapper")
	if err != nil {
		t.Fatal(err)
	}
	toxi := temp.Name()
	defer func() {
		// _ = os.Remove(toxi)
	}()
	if _, err := temp.WriteString(toxiWrapper); err != nil {
		t.Fatal(err)
	}
	temp.Close()
	if err := os.Chmod(toxi, 0744); err != nil {
		t.Fatal(err)
	}

	c.Put(ctx, toxi, "./cockroach", c.All())
	c.Put(ctx, cockroach, "./cockroach.real", c.All())
	c.Put(ctx, workload, "./workload", c.All())

	// TODO(tschottdorf): better mechanism for handling the `--local` flag wrt binaries.
	// Likely some abstraction is useful (a la binfetcher). This would also help with the
	// cockroach and workload binaries.
	toxiURL := "https://github.com/Shopify/toxiproxy/releases/download/v2.1.2/toxiproxy-server-linux-amd64"
	if local && runtime.GOOS == "darwin" {
		toxiURL = "https://github.com/Shopify/toxiproxy/releases/download/v2.1.2/toxiproxy-server-darwin-amd64"
	}

	slowRoachPort := func(i int) string {
		return fmt.Sprintf("%d", 26257+2*(i-1))
	}
	fastRoachPort := func(i int) string {
		return fmt.Sprintf("%d", 26257+2*(i-1)+10000)
	}

	for i := 1; i <= nodes; i++ {
		i := i
		c.Run(ctx, i, "curl", "-Lo", "toxiproxy-server", toxiURL)
		c.Run(ctx, i, "chmod", "+x", "toxiproxy-server")

		port := fmt.Sprintf("%d", 8474+i)
		go func() {
			err := c.RunE(ctx, i, "mkdir -p logs && ./toxiproxy-server --port "+port+" 2>&1 > logs/toxiproxy.out")
			if err != nil {
				// There will always be an error here due to the kill -9 at test teardown.
				c.l.errorf("%s\n", err)
			}
		}()
		time.Sleep(time.Second) // cheat instead of properly waiting for toxiproxy to listen
		toxClient := client.NewClient("http://localhost:" + port)
		proxy, err := toxClient.CreateProxy("cockroach", ":"+slowRoachPort(i), "127.0.0.1:"+fastRoachPort(i))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := proxy.AddToxic("", "latency", "downstream", 1, client.Attributes{
			"latency": 1000, // ms
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := proxy.AddToxic("", "latency", "upstream", 1, client.Attributes{
			"latency": 1000, // ms
		}); err != nil {
			t.Fatal(err)
		}
	}
	c.Start(ctx, c.All())

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

	db, err := gosql.Open("postgres", "postgres://root@127.0.0.1:"+fastRoachPort(1)+"?sslmode=disable")
	if err != nil {
		c.t.Fatal(err)
	}
	defer db.Close()

	measure := func(ctx context.Context, stmt string, args ...interface{}) time.Duration {
		tBegin := timeutil.Now()
		rows, err := db.QueryContext(ctx, stmt, args...)
		if err != nil {
			panic(err)
		}
		if err := rows.Close(); err != nil {
			panic(err)
		}
		return timeutil.Since(tBegin)
	}

	m := newMonitor(ctx, c, c.All())
	m.Go(func(ctx context.Context) error {
		measure(ctx, `SET CLUSTER SETTING trace.debug.enable = true`)
		measure(ctx, "CREATE DATABASE test")
		measure(ctx, `CREATE TABLE test.commit (a INT, b INT, v INT, PRIMARY KEY (a, b))`)
		measure(ctx, "ALTER TABLE test.commit SPLIT AT VALUES (2, 0), (3, 0)")
		if duration := measure(ctx, `SELECT 1`); duration > time.Second/2 {
			return errors.Errorf("SELECT 1 took %s but should not have been slowed down", duration)
		}

		for i := 0; i < 100; i++ {
			duration := measure(ctx, fmt.Sprintf("BEGIN; INSERT INTO test.commit VALUES (2, %[1]d), (1, %[1]d), (3, %[1]d); COMMIT", i))
			c.l.printf("%s\n", duration)
		}

		time.Sleep(time.Hour)
		return nil
	})

	m.Wait()
}

func init() {
	const numNodes = 3
	tests.Add(testSpec{
		Name:  fmt.Sprintf("network/latency/nodes=%d", numNodes),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runLatency(ctx, t, c, numNodes)
		},
	})
}
