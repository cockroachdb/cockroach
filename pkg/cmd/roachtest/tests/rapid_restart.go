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
	"math/rand"
	"net/http"
	"os/exec"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func runRapidRestart(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Use a single-node cluster which speeds the stop/start cycle.
	nodes := c.Node(1)
	c.Put(ctx, t.Cockroach(), "./cockroach", nodes)

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
					`mkdir -p {log-dir} && ./cockroach start-single-node --insecure --store={store-dir} `+
						`--log-dir={log-dir} --cache=10% --max-sql-memory=10% `+
						`--listen-addr=:{pgport:1} --http-port=$[{pgport:1}+1] `+
						`> {log-dir}/cockroach.stdout 2> {log-dir}/cockroach.stderr`)
				exitCh <- err
			}()
			if i == 2 {
				break
			}

			waitTime := time.Duration(rand.Int63n(int64(time.Second)))
			time.Sleep(waitTime)

			sig := [2]string{"2", "9"}[rand.Intn(2)]

			var err error
			for err == nil {
				c.Stop(ctx, nodes, option.StopArgs("--sig="+sig))
				select {
				case <-ctx.Done():
					return
				case err = <-exitCh:
				case <-time.After(10 * time.Second):
					// We likely ended up killing before the process spawned.
					// Loop around.
					t.L().Printf("no exit status yet, killing again")
				}
			}
			if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
				switch status := sysutil.ExitStatus(exitErr); status {
				case -1:
					// Received SIGINT before setting up our own signal handlers or
					// SIGKILL.
				case 20:
					// Exit code from a SIGINT received by our signal handlers.
				default:
					t.Fatalf("unexpected exit status %d", status)
				}
			} else {
				t.Fatalf("unexpected exit err: %v", err)
			}
		}

		// The var dump below may take a while to generate, maybe more
		// than the 3 second timeout of the default http client.
		httpClient := httputil.NewClientWithTimeout(15 * time.Second)

		// Verify the cluster is ok by torturing the prometheus endpoint until it
		// returns success. A side-effect is to prevent regression of #19559.
		for !done() {
			adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, nodes)
			if err != nil {
				t.Fatal(err)
			}
			base := `http://` + adminUIAddrs[0]
			// Torture the prometheus endpoint to prevent regression of #19559.
			url := base + `/_status/vars`
			resp, err := httpClient.Get(ctx, url)
			if err == nil {
				if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusOK {
					t.Fatalf("unexpected status code from %s: %d", url, resp.StatusCode)
				}
				break
			}
		}

		t.L().Printf("%d OK\n", j)
	}

	// Clean up for the test harness. Usually we want to leave nodes running so
	// that consistency checks can be run, but in this case there's not much
	// there in the first place anyway.
	c.Stop(ctx, nodes)
	c.Wipe(ctx, nodes)
}
