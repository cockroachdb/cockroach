// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

type failureSmokeTest struct {
	testName    string
	failureName string
	args        failures.FailureArgs

	// Time to wait for the failure to propagate after inject/restore.
	waitForFailureToPropagate time.Duration
	// Validate that the failure was injected correctly, called after Setup() and Inject().
	validateFailure func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error
	// Validate that the failure was restored correctly, called after Restore().
	validateRestore func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error
}

func (t *failureSmokeTest) run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, fr *failures.FailureRegistry,
) error {
	// TODO(darryl): In the future, roachtests should interact with the failure injection library
	// through helper functions in roachtestutil so they don't have to interface with roachprod
	// directly.
	failure, err := fr.GetFailure(c.MakeNodes(), t.failureName, l, c.IsSecure())
	if err != nil {
		return err
	}
	if err = failure.Setup(ctx, l, t.args); err != nil {
		return err
	}
	if err = failure.Inject(ctx, l, t.args); err != nil {
		return err
	}

	// Allow the failure to take effect.
	if t.waitForFailureToPropagate > 0 {
		l.Printf("sleeping for %s to allow failure to take effect", t.waitForFailureToPropagate)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.waitForFailureToPropagate):
		}
	}

	if err = t.validateFailure(ctx, l, c); err != nil {
		return err
	}
	if err = failure.Restore(ctx, l, t.args); err != nil {
		return err
	}

	// Allow the cluster to return to normal.
	if t.waitForFailureToPropagate > 0 {
		l.Printf("sleeping for %s to allow cluster to return to normal", t.waitForFailureToPropagate)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.waitForFailureToPropagate):
		}
	}

	if err = t.validateRestore(ctx, l, c); err != nil {
		return err
	}
	if err = failure.Cleanup(ctx, l, t.args); err != nil {
		return err
	}
	return nil
}

func (t *failureSmokeTest) noopRun(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, fr *failures.FailureRegistry,
) error {
	if err := t.validateFailure(ctx, l, c); err == nil {
		return errors.New("no failure was injected but validation still passed")
	}
	if err := t.validateRestore(ctx, l, c); err != nil {
		return errors.Wrapf(err, "no failure was injected but post restore validation still failed")
	}
	return nil
}

var bidirectionalNetworkPartitionTest = failureSmokeTest{
	testName:    "bidirectional network partition",
	failureName: failures.IPTablesNetworkPartitionName,
	args: failures.NetworkPartitionArgs{
		Partitions: []failures.NetworkPartition{{
			Source:      install.Nodes{1},
			Destination: install.Nodes{3},
			Type:        failures.Bidirectional,
		}},
	},
	waitForFailureToPropagate: 5 * time.Second,
	validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
		blocked, err := checkPortBlocked(ctx, l, c, c.Nodes(1), c.Nodes(3))
		if err != nil {
			return err
		}
		if !blocked {
			return fmt.Errorf("expected connections from node 1 to node 3 to be blocked")
		}

		blocked, err = checkPortBlocked(ctx, l, c, c.Nodes(1), c.Nodes(2))
		if err != nil {
			return err
		}
		if blocked {
			return fmt.Errorf("expected connections from node 1 to node 2 to not be blocked")
		}
		return nil
	},
	validateRestore: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
		blocked, err := checkPortBlocked(ctx, l, c, c.Nodes(1), c.Nodes(3))
		if err != nil {
			return err
		}
		if blocked {
			return fmt.Errorf("expected connections from node 1 to node 3 to not be blocked")
		}
		return err
	},
}

var asymmetricNetworkPartitionTest = failureSmokeTest{
	testName:    "asymmetric network partition",
	failureName: failures.IPTablesNetworkPartitionName,
	args: failures.NetworkPartitionArgs{
		Partitions: []failures.NetworkPartition{{
			Source:      install.Nodes{1},
			Destination: install.Nodes{3},
			Type:        failures.Incoming,
		}},
	},
	waitForFailureToPropagate: 5 * time.Second,
	validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
		blocked, err := checkPortBlocked(ctx, l, c, c.Nodes(1), c.Nodes(3))
		if err != nil {
			return err
		}
		if blocked {
			return fmt.Errorf("expected connections from node 1 to node 3 to be open")
		}

		blocked, err = checkPortBlocked(ctx, l, c, c.Nodes(3), c.Nodes(1))
		if err != nil {
			return err
		}
		if !blocked {
			return fmt.Errorf("expected connections from node 3 to node 1 to be blocked")
		}
		return nil
	},
	validateRestore: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
		blocked, err := checkPortBlocked(ctx, l, c, c.Nodes(1), c.Nodes(3))
		if err != nil {
			return err
		}
		if blocked {
			return fmt.Errorf("expected connections from node 1 to node 3 to not be blocked")
		}
		return err
	},
}

// Helper function that uses nmap to check if connections between two nodes is blocked.
func checkPortBlocked(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, from, to option.NodeListOption,
) (bool, error) {
	// `nmap -oG` example output:
	// Host: {IP} {HOST_NAME}	Status: Up
	// Host: {IP} {HOST_NAME}	Ports: 26257/open/tcp//cockroach///
	// We care about the port scan result and whether it is filtered or open.
	res, err := c.RunWithDetailsSingleNode(ctx, l, option.WithNodes(from), fmt.Sprintf("nmap -p {pgport%[1]s} {ip%[1]s} -oG - | awk '/Ports:/{print $5}'", to))
	if err != nil {
		return false, err
	}
	return strings.Contains(res.Stdout, "filtered"), nil
}

func setupFailureSmokeTests(ctx context.Context, t test.Test, c cluster.Cluster) error {
	// Download any dependencies needed.
	if err := c.Install(ctx, t.L(), c.CRDBNodes(), "nmap"); err != nil {
		return err
	}
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())
	// Run a light workload in the background so we have some traffic in the database.
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload init tpcc --warehouses=100 {pgurl:1}")
	t.Go(func(goCtx context.Context, l *logger.Logger) error {
		return c.RunE(goCtx, option.WithNodes(c.WorkloadNode()), "./cockroach workload run tpcc --tolerate-errors --warehouses=100 {pgurl:1-3}")
	}, task.WithContext(ctx))
	return nil
}

func runFailureSmokeTest(ctx context.Context, t test.Test, c cluster.Cluster, noopFailer bool) {
	if err := setupFailureSmokeTests(ctx, t, c); err != nil {
		t.Fatal(err)
	}
	fr := failures.NewFailureRegistry()
	fr.Register()

	var failureSmokeTests = []failureSmokeTest{
		bidirectionalNetworkPartitionTest,
		asymmetricNetworkPartitionTest,
	}

	// Randomize the order of the tests in case any of the failures have unexpected side
	// effects that may mask failures, e.g. a cgroups disk stall isn't properly restored
	// which causes a dmsetup disk stall to appear to work even if it doesn't.
	rand.Shuffle(len(failureSmokeTests), func(i, j int) {
		failureSmokeTests[i], failureSmokeTests[j] = failureSmokeTests[j], failureSmokeTests[i]
	})

	for _, test := range failureSmokeTests {
		t.L().Printf("running %s test", test.testName)
		if noopFailer {
			if err := test.noopRun(ctx, t.L(), c, fr); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := test.run(ctx, t.L(), c, fr); err != nil {
				t.Fatal(err)
			}
		}
		t.L().Printf("%s test complete", test.testName)
	}
}

func registerFISmokeTest(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "failure-injection-smoke-test",
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode(), spec.CPU(2), spec.WorkloadNodeCPU(2), spec.ReuseNone()),
		CompatibleClouds: registry.OnlyGCE,
		// TODO(darryl): When the FI library starts seeing more use through roachtests, CLI, etc. switch this to Nightly.
		Suites: registry.ManualOnly,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runFailureSmokeTest(ctx, t, c, false /* noopFailer */)
		},
	})
	r.Add(registry.TestSpec{
		Name:             "failure-injection-noop-smoke-test",
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode(), spec.CPU(2), spec.WorkloadNodeCPU(2), spec.ReuseNone()),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.ManualOnly,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runFailureSmokeTest(ctx, t, c, true /* noopFailer */)
		},
	})
}
