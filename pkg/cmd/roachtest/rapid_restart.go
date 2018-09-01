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
	"math/rand"
	"net/http"
	"os/exec"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

func runRapidRestart(ctx context.Context, t *test, c *cluster) {
	// Use a single-node cluster which speeds the stop/start cycle.
	nodes := c.Node(1)
	c.Put(ctx, cockroach, "./cockroach", nodes)

	// In a loop, bootstrap a new single-node cluster and immediately kill
	// it. This is more effective at finding problems than restarting an existing
	// node since there are more moving parts the first time around. Since there
	// could be future issues that only occur on a restart, each invocation of
	// the test also restart-kills the existing node twice.
	deadline := timeutil.Now().Add(time.Minute)
	done := func() bool {
		return timeutil.Now().After(deadline)
	}
	for j := 1; !done(); j++ {
		c.Wipe(ctx, nodes)

		// The first 2 iterations we start the cockroach node and kill it right
		// away. The 3rd iteration we let cockroach run so that we can check after
		// the loop that everything is ok.
		for i := 0; i < 3; i++ {
			exitCh := make(chan error, 1)
			go func() {
				err := c.RunE(ctx, nodes,
					`mkdir -p {log-dir} && ./cockroach start --insecure --store={store-dir} `+
						`--log-dir={log-dir} --cache=10% --max-sql-memory=10% `+
						`--listen-addr=:{pgport:1} --http-port=$[{pgport:1}+1] `+
						`> {log-dir}/cockroach.stdout 2> {log-dir}/cockroach.stderr`)
				exitCh <- err
			}()
			if i == 2 {
				break
			}

			waitTime := time.Duration(rand.Int63n(int64(time.Second)))
			if !c.isLocal() {
				// TODO(peter): This is hacky: the signal might be sent before the
				// cockroach process starts, which is especially true on remote
				// clusters. Perhaps combine this with a monitor so that we can detect
				// as soon as the process starts before killing it. Or a custom kill
				// script which loops looking for a cockroach process and kills it as
				// soon as it appears. Using --pid_file or --background isn't quite
				// right as we want to be able to kill the process before it is ready.
				waitTime += time.Second
			}
			time.Sleep(waitTime)

			sig := [2]string{"2", "9"}[rand.Intn(2)]
			c.Stop(ctx, nodes, stopArgs("--sig="+sig))
			select {
			case <-ctx.Done():
				return
			case err := <-exitCh:
				cause := errors.Cause(err)
				if exitErr, ok := cause.(*exec.ExitError); ok {
					switch status := sysutil.ExitStatus(exitErr); status {
					case -1:
						// Received SIGINT before setting up our own signal handlers or
						// SIGKILL.
					case 1:
						// Exit code from a SIGINT received by our signal handlers.
					default:
						t.Fatalf("unexpected exit status %d", status)
					}
				} else {
					t.Fatalf("unexpected exit err: %v", err)
				}
			}
		}

		// Verify the cluster is ok by torturing the prometheus endpoint until it
		// returns success. A side-effect is to prevent regression of #19559.
		for !done() {
			base := `http://` + c.ExternalAdminUIAddr(ctx, nodes)[0]
			// Torture the prometheus endpoint to prevent regression of #19559.
			url := base + `/_status/vars`
			resp, err := http.Get(url)
			if err == nil {
				if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusOK {
					t.Fatalf("unexpected status code from %s: %d", url, resp.StatusCode)
				}
				break
			}
		}

		c.l.Printf("%d OK\n", j)
	}
}
