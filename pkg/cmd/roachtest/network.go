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

func init() {
	runLatency := func(t *test, nodes int) {
		ctx := context.Background()
		c := newCluster(ctx, t, nodes)
		defer c.Destroy(ctx)

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

		for i := 1; i <= nodes; i++ {
			i := i
			c.Run(ctx, i, "curl", "-Lo", "toxiproxy-server", toxiURL)
			c.Run(ctx, i, "chmod", "+x", "toxiproxy-server")

			port := fmt.Sprintf("%d", 8474+i)
			roachPort := fmt.Sprintf("%d", 26257+2*(i-1))
			realRoachPort := fmt.Sprintf("%d", 26257+2*(i-1)+10000)
			// QUESTION(petermattis) This thing runs forever and I just want it to be
			// killed at the end of the test. However, when it gets killed, it actually
			// *fails* the test, which is unfortunate and I see no way to avoid that
			// with the current API.
			go c.Run(ctx, i, "mkdir -p logs && ./toxiproxy-server --port "+port+" 2>&1 > logs/toxiproxy.out")
			time.Sleep(time.Second) // cheat instead of properly waiting for toxiproxy to listen
			toxClient := client.NewClient("http://localhost:" + port)
			proxy, err := toxClient.CreateProxy("cockroach", ":"+roachPort, "127.0.0.1:"+realRoachPort)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := proxy.AddToxic("slow things down", "latency", "", 1, client.Attributes{
				"latency": 1000, // ms
			}); err != nil {
				t.Fatal(err)
			}
		}

		c.Start(ctx, c.All())

		m := newMonitor(ctx, c, c.All())
		m.Go(func(ctx context.Context) error {
			run := func(stmt string) {
				t.Status(stmt)
				c.Run(ctx, 1, `./cockroach sql --insecure -e "`+stmt+`"`)
			}

			run(`SET CLUSTER SETTING trace.debug.enable = true`)
			tBefore := timeutil.Now()
			run(`SELECT 1`)
			if duration := timeutil.Since(tBefore); duration < time.Second {
				return errors.Errorf("expected at least one second of delay, got %s", duration)
			}
			return nil
		})
		m.Wait()
	}

	tests.Add("network/latency/nodes=3", func(t *test) {
		runLatency(t, 3)
	})
}
