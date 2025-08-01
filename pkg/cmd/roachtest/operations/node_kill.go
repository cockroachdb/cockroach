// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type cleanupNodeKill struct {
	nodes    option.NodeListOption
	downtime time.Duration
}

func (cl *cleanupNodeKill) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	o.Status(fmt.Sprintf("waiting for %s before restarting node %s", cl.downtime, cl.nodes))
	select {
	case <-time.After(cl.downtime):
	case <-ctx.Done():
		o.Status("cleanup context cancelled during wait")
		return
	}

	db, err := c.ConnE(ctx, o.L(), cl.nodes[0])
	if err != nil {
		err = c.RunE(ctx, option.WithNodes(cl.nodes), "./cockroach.sh")
		if err != nil {
			o.Status(fmt.Sprintf("restarted node with error %s", err))
		} else {
			o.Status("restarted node with no error")
		}
		return
	}
	defer db.Close()
	_, err = db.Query("SELECT 1")
	if err != nil {
		err = c.RunE(ctx, option.WithNodes(cl.nodes), "./cockroach.sh")
		if err != nil {
			o.Status(fmt.Sprintf("restarted node with error %s", err))
		} else {
			o.Status("restarted node with no error")
		}
	}
}

func nodeKillRunner(
	signal int, drain bool, downtime time.Duration,
) func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		return runNodeKill(ctx, o, c, signal, drain, downtime)
	}
}

func runNodeKill(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	signal int,
	drain bool,
	downtime time.Duration,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()
	node := c.All().SeededRandNode(rng)

	if drain {
		helpers.DrainNode(ctx, o, c, node)
	}

	o.Status(fmt.Sprintf("killing node %s with signal %d", node.NodeIDsString(), signal))
	err := c.RunE(ctx, option.WithNodes(node), "pkill", fmt.Sprintf("-%d", signal), "-f", "cockroach\\ start")
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("sent signal %d to node %s, waiting for process to exit", signal, node.NodeIDsString()))
	for {
		if err := ctx.Err(); err != nil {
			o.Fatal(err)
		}
		err := c.RunE(ctx, option.WithNodes(node), "pgrep", "-f", "cockroach\\ start")
		if err != nil {
			if strings.Contains(err.Error(), "status 1") {
				// pgrep returns error code 1 if no processes are found.
				o.Status(fmt.Sprintf("killed node %s with signal %d", node.NodeIDsString(), signal))
				break
			}
			o.Fatal(err)
		}
		time.Sleep(1 * time.Second)
	}

	// Schedule the cleanup instead of returning it
	go func() {
		cleanup := cleanupNodeKill{
			nodes:    node,
			downtime: downtime,
		}
		cleanup.Cleanup(ctx, o, c)
	}()

	// return nil to avoid the hardcoded 5s + random [0s, 24h] wait
	return nil
}

func registerNodeKill(r registry.Registry) {
	for _, spec := range []struct {
		name     string
		signal   int
		drain    bool
		downtime time.Duration
		timeout  time.Duration
	}{
		{"node-kill/sigkill/drain=true/downtime=1m", 9, true, 1 * time.Minute, 20 * time.Minute},
		{"node-kill/sigkill/drain=true/downtime=10m", 9, true, 10 * time.Minute, 25 * time.Minute},
		{"node-kill/sigkill/drain=true/downtime=1h", 9, true, 1 * time.Hour, 2 * time.Hour},
		{"node-kill/sigkill/drain=true/downtime=5h", 9, true, 5 * time.Hour, 6 * time.Hour},

		{"node-kill/sigkill/drain=false/downtime=1m", 9, false, 1 * time.Minute, 20 * time.Minute},
		{"node-kill/sigkill/drain=false/downtime=10m", 9, false, 10 * time.Minute, 25 * time.Minute},
		{"node-kill/sigkill/drain=false/downtime=1h", 9, false, 1 * time.Hour, 2 * time.Hour},
		{"node-kill/sigkill/drain=false/downtime=5h", 9, true, 5 * time.Hour, 6 * time.Hour},

		{"node-kill/sigterm/drain=true/downtime=1m", 15, true, 1 * time.Minute, 20 * time.Minute},
		{"node-kill/sigterm/drain=true/downtime=10m", 15, true, 10 * time.Minute, 25 * time.Minute},
		{"node-kill/sigterm/drain=true/downtime=1h", 15, true, 1 * time.Hour, 2 * time.Hour},
		{"node-kill/sigterm/drain=true/downtime=5h", 15, true, 5 * time.Hour, 6 * time.Hour},

		{"node-kill/sigterm/drain=false/downtime=1m", 15, false, 1 * time.Minute, 20 * time.Minute},
		{"node-kill/sigterm/drain=false/downtime=10m", 15, false, 10 * time.Minute, 25 * time.Minute},
		{"node-kill/sigterm/drain=false/downtime=1h", 15, false, 1 * time.Hour, 2 * time.Hour},
		{"node-kill/sigterm/drain=false/downtime=5h", 15, true, 5 * time.Hour, 6 * time.Hour},
	} {
		r.AddOperation(registry.OperationSpec{
			Name:               spec.name,
			Owner:              registry.OwnerServer,
			Timeout:            spec.timeout,
			CompatibleClouds:   registry.AllClouds,
			CanRunConcurrently: registry.OperationCannotRunConcurrently,
			Dependencies:       []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
			Run:                nodeKillRunner(spec.signal, spec.drain, spec.downtime),
		})
	}
}
