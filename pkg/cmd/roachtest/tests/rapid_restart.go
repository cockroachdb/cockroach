// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"math/rand"
	"net/http"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func runRapidRestart(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Use a single-node cluster which speeds the stop/start cycle.
	node := c.Node(1)

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
		c.Wipe(ctx, node)

		// The first 2 iterations we start the cockroach node and kill it right
		// away. The 3rd iteration we let cockroach run so that we can check after
		// the loop that everything is ok.
		for i := 0; i < 3; i++ {
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.SkipInit = true
			if err := c.StartE(ctx, t.L(), startOpts, install.MakeClusterSettings(), node); err != nil {
				t.Fatalf("error during start: %v", err)
			}

			if i == 2 {
				break
			}

			waitTime := time.Duration(rand.Int63n(int64(time.Second)))
			time.Sleep(waitTime)

			sig := [2]int{2, 9}[rand.Intn(2)]
			stopOpts := option.DefaultStopOpts()
			stopOpts.RoachprodOpts.Sig = sig
			if err := c.StopE(ctx, t.L(), stopOpts, node); err != nil {
				t.Fatalf("error during stop: %v", err)
			}
		}

		// The var dump below may take a while to generate, maybe more
		// than the 3 second timeout of the default http client.
		httpClient := httputil.NewClientWithTimeout(15 * time.Second)

		// Verify the cluster is ok by torturing the prometheus endpoint until it
		// returns success. A side-effect is to prevent regression of #19559.
		for !done() {
			adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), node)
			if err != nil {
				t.Fatal(err)
			}
			base := `https://` + adminUIAddrs[0]
			// Torture the prometheus endpoint to prevent regression of #19559.
			url := base + `/_status/vars`
			resp, err := httpClient.Get(ctx, url)
			if err == nil {
				resp.Body.Close()
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
	c.Stop(ctx, t.L(), option.DefaultStopOpts(), node)
	c.Wipe(ctx, node)
}
