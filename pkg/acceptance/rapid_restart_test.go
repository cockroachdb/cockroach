// Copyright 2017 The Cockroach Authors.
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

package acceptance

import (
	"context"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestRapidRestarts(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	ctx := context.Background()
	cfg := readConfigFromFlags()
	RunLocal(t, func(t *testing.T) {
		deadline := timeutil.Now().Add(cfg.Duration)
		// In a loop, bootstrap a new node and immediately kill it. This is more
		// effective at finding problems that restarting an existing node since
		// there are more moving parts the first time around. Since there could be
		// future issues that only occur on a restart, each invocation of the test
		// also restart-kills the existing node once.
		for timeutil.Now().Before(deadline) {
			testRapidRestartSingle(ctx, t, cfg)
		}
	})
}

func unexpectedExitCode(exitErr *exec.ExitError) error {
	if exitErr == nil {
		// Server shut down cleanly. Note that returning `err` here would create
		// an error interface wrapping a nil *ExitError, which is *not* nil
		// itself.
		return nil
	}

	switch status := sysutil.ExitStatus(exitErr); status {
	case -1:
		// Received SIGINT before setting up our own signal handlers.
	case 1:
		// Exit code from a SIGINT received by our signal handlers.
	default:
		return errors.Wrapf(exitErr, "unexpected exit status %d", status)
	}
	return nil
}

func testRapidRestartSingle(ctx context.Context, t *testing.T, cfg cluster.TestConfig) {
	// Make this a single-node cluster which unlocks optimizations in
	// LocalCluster that skip all the waiting so that we get to kill the process
	// early in its boot sequence.
	cfg.Nodes = cfg.Nodes[:1]
	// Make sure StartCluster doesn't wait for replication but just hands us the
	// cluster straight away.
	cfg.NoWait = true

	c := StartCluster(ctx, t, cfg)
	defer c.AssertAndStop(ctx, t)

	lc := c.(*localcluster.LocalCluster)

	interrupt := func() {
		t.Helper()
		time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
		lc.Nodes[0].Signal(os.Interrupt)
	}

	check := func() {
		t.Helper()
		if err := unexpectedExitCode(lc.Nodes[0].Wait()); err != nil {
			lc.Cfg.Ephemeral = false // keep log dir
			t.Fatalf("node did not terminate cleanly: %v", err)
		}
	}

	const count = 2
	// NB: the use of Group makes no sense with count=2, but this way you can
	// bump it and the whole thing still works.
	var g errgroup.Group

	getVars := func(ch <-chan error) func() error {
		return func() error {
			for {
				base := c.URL(ctx, 0)
				if base != "" {
					// Torture the prometheus endpoint to prevent regression of #19559.
					const varsEndpoint = "/_status/vars"
					resp, err := cluster.HTTPClient.Get(base + varsEndpoint)
					if err == nil {
						if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusOK {
							return errors.Errorf("unexpected status code from %s: %d", varsEndpoint, resp.StatusCode)
						}
					}
				}
				select {
				case err := <-ch:
					return err
				default:
					time.Sleep(time.Millisecond)
				}
			}
		}
	}
	closedCh := make(chan error)
	close(closedCh)

	for i := 0; i < count; i++ {
		g.Go(getVars(closedCh))

		if i > 0 {
			ch := lc.RestartAsync(ctx, 0)
			g.Go(getVars(ch))
		}

		log.Info(ctx, "interrupting node")
		interrupt()

		log.Info(ctx, "waiting for exit code")
		check()
	}

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}
