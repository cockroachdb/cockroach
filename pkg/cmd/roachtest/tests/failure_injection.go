// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
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
	// Validate that the failure was injected correctly, called after Setup() and Inject().
	validateFailure func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error
	// Validate that the failure was restored correctly, called after Restore().
	validateRestore func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error
}

func (t *failureSmokeTest) run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, fr *failures.FailureRegistry,
) (err error) {
	// TODO(darryl): In the future, roachtests should interact with the failure injection library
	// through helper functions in roachtestutil so they don't have to interface with roachprod
	// directly.
	failureMode, err := fr.GetFailureMode(c.MakeNodes(c.CRDBNodes()), t.failureName, l, c.IsSecure())
	if err != nil {
		return err
	}
	// Make sure to cleanup the failure mode even if the test fails.
	defer func() {
		quietLogger, file, logErr := roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "cleanup")
		if logErr != nil {
			l.Printf("failed to create logger for cleanup: %v", logErr)
			quietLogger = l
		}
		l.Printf("%s: Running Cleanup(); details in %s.log", t.failureName, file)
		err = errors.CombineErrors(err, failureMode.Cleanup(ctx, quietLogger, t.args))
	}()

	quietLogger, file, err := roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "setup")
	if err != nil {
		return err
	}
	l.Printf("%s: Running Setup(); details in %s.log", t.failureName, file)
	if err = failureMode.Setup(ctx, quietLogger, t.args); err != nil {
		return err
	}

	quietLogger, file, err = roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "inject")
	if err != nil {
		return err
	}
	l.Printf("%s: Running Inject(); details in %s.log", t.failureName, file)
	if err = failureMode.Inject(ctx, quietLogger, t.args); err != nil {
		return err
	}

	// Allow the failure to take effect.
	quietLogger, file, err = roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "wait for propagate")
	if err != nil {
		return err
	}
	l.Printf("%s: Running WaitForFailureToPropagate(); details in %s.log", t.failureName, file)
	if err = failureMode.WaitForFailureToPropagate(ctx, quietLogger, t.args); err != nil {
		return err
	}

	l.Printf("validating failure was properly injected")
	if err = t.validateFailure(ctx, l, c); err != nil {
		return err
	}

	quietLogger, file, err = roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "restore")
	if err != nil {
		return err
	}
	l.Printf("%s: Running Restore(); details in %s.log", t.failureName, file)
	if err = failureMode.Restore(ctx, quietLogger, t.args); err != nil {
		return err
	}

	// Allow the cluster to return to normal.
	quietLogger, file, err = roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "wait for restore")
	if err != nil {
		return err
	}
	l.Printf("%s: Running WaitForFailureToRestore(); details in %s.log", t.failureName, file)
	if err = failureMode.WaitForFailureToRestore(ctx, quietLogger, t.args); err != nil {
		return err
	}

	l.Printf("validating failure was properly restored")
	return t.validateRestore(ctx, l, c)
}

func (t *failureSmokeTest) noopRun(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, _ *failures.FailureRegistry,
) error {
	if err := t.validateFailure(ctx, l, c); err == nil {
		return errors.New("no failure was injected but validation still passed")
	}
	if err := t.validateRestore(ctx, l, c); err != nil {
		return errors.Wrapf(err, "no failure was injected but post restore validation still failed")
	}
	return nil
}

var bidirectionalNetworkPartitionTest = func(c cluster.Cluster) failureSmokeTest {
	nodes := c.CRDBNodes()
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	srcNode := nodes[0]
	destNode := nodes[1]
	unaffectedNode := nodes[2]
	return failureSmokeTest{
		testName:    "bidirectional network partition",
		failureName: failures.IPTablesNetworkPartitionName,
		args: failures.NetworkPartitionArgs{
			Partitions: []failures.NetworkPartition{{
				Source:      install.Nodes{install.Node(srcNode)},
				Destination: install.Nodes{install.Node(destNode)},
				Type:        failures.Bidirectional,
			}},
		},
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
			blocked, err := roachtestutil.CheckPortBlocked(ctx, l, c, c.Nodes(srcNode), c.Nodes(destNode), fmt.Sprintf("{pgport:%d}", destNode))
			if err != nil {
				return err
			}
			if !blocked {
				return errors.Errorf("expected connections from node %d to node %d to be blocked", srcNode, destNode)
			}

			blocked, err = roachtestutil.CheckPortBlocked(ctx, l, c, c.Nodes(srcNode), c.Nodes(unaffectedNode), fmt.Sprintf("{pgport:%d}", unaffectedNode))
			if err != nil {
				return err
			}
			if blocked {
				return errors.Errorf("expected connections from node %d to node %d to not be blocked", srcNode, unaffectedNode)
			}
			return nil
		},
		validateRestore: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
			blocked, err := roachtestutil.CheckPortBlocked(ctx, l, c, c.Nodes(srcNode), c.Nodes(destNode), fmt.Sprintf("{pgport:%d}", destNode))
			if err != nil {
				return err
			}
			if blocked {
				return errors.Errorf("expected connections from node %d to node %d to not be blocked", srcNode, destNode)
			}
			return err
		},
	}
}

var asymmetricIncomingNetworkPartitionTest = func(c cluster.Cluster) failureSmokeTest {
	nodes := c.CRDBNodes()
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	srcNode := nodes[0]
	destNode := nodes[1]
	return failureSmokeTest{
		testName:    "asymmetric network partition - incoming traffic dropped",
		failureName: failures.IPTablesNetworkPartitionName,
		args: failures.NetworkPartitionArgs{
			Partitions: []failures.NetworkPartition{{
				Source:      install.Nodes{install.Node(srcNode)},
				Destination: install.Nodes{install.Node(destNode)},
				Type:        failures.Incoming,
			}},
		},
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
			blocked, err := roachtestutil.CheckPortBlocked(ctx, l, c, c.Nodes(srcNode), c.Nodes(destNode), fmt.Sprintf("{pgport:%d}", destNode))
			if err != nil {
				return err
			}
			if blocked {
				return errors.Errorf("expected connections from node %d to node %d to be open", srcNode, destNode)
			}

			blocked, err = roachtestutil.CheckPortBlocked(ctx, l, c, c.Nodes(destNode), c.Nodes(srcNode), fmt.Sprintf("{pgport:%d}", srcNode))
			if err != nil {
				return err
			}
			if !blocked {
				return errors.Errorf("expected connections from node %d to node %d to be blocked", destNode, srcNode)
			}
			return nil
		},
		validateRestore: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
			blocked, err := roachtestutil.CheckPortBlocked(ctx, l, c, c.Nodes(srcNode), c.Nodes(destNode), fmt.Sprintf("{pgport:%d}", destNode))
			if err != nil {
				return err
			}
			if blocked {
				return errors.Errorf("expected connections from node %d to node %d to be open", srcNode, destNode)
			}
			return err
		},
	}
}

var asymmetricOutgoingNetworkPartitionTest = func(c cluster.Cluster) failureSmokeTest {
	nodes := c.CRDBNodes()
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	srcNode := nodes[0]
	destNode := nodes[1]
	return failureSmokeTest{
		testName:    "asymmetric network partition - outgoing traffic dropped",
		failureName: failures.IPTablesNetworkPartitionName,
		args: failures.NetworkPartitionArgs{
			Partitions: []failures.NetworkPartition{{
				Source:      install.Nodes{install.Node(srcNode)},
				Destination: install.Nodes{install.Node(destNode)},
				Type:        failures.Outgoing,
			}},
		},
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
			blocked, err := roachtestutil.CheckPortBlocked(ctx, l, c, c.Nodes(srcNode), c.Nodes(destNode), fmt.Sprintf("{pgport:%d}", destNode))
			if err != nil {
				return err
			}
			if !blocked {
				return errors.Errorf("expected connections from node %d to node %d to be blocked", srcNode, destNode)
			}

			blocked, err = roachtestutil.CheckPortBlocked(ctx, l, c, c.Nodes(destNode), c.Nodes(srcNode), fmt.Sprintf("{pgport:%d}", srcNode))
			if err != nil {
				return err
			}
			if blocked {
				return errors.Errorf("expected connections from node %d to node %d to be open", destNode, srcNode)
			}
			return nil
		},
		validateRestore: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
			blocked, err := roachtestutil.CheckPortBlocked(ctx, l, c, c.Nodes(srcNode), c.Nodes(destNode), fmt.Sprintf("{pgport:%d}", destNode))
			if err != nil {
				return err
			}
			if blocked {
				return errors.Errorf("expected connections from node %d to node %d to be open", srcNode, destNode)
			}
			return err
		},
	}
}

var latencyTest = func(c cluster.Cluster) failureSmokeTest {
	nodes := c.CRDBNodes()
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	srcNode := nodes[0]
	destNode := nodes[1]
	unaffectedNode := nodes[2]
	return failureSmokeTest{
		testName:    "Network Latency",
		failureName: failures.NetworkLatencyName,
		args: failures.NetworkLatencyArgs{
			ArtificialLatencies: []failures.ArtificialLatency{
				{
					Source:      install.Nodes{install.Node(srcNode)},
					Destination: install.Nodes{install.Node(destNode)},
					Delay:       2 * time.Second,
				},
				{
					Source:      install.Nodes{install.Node(destNode)},
					Destination: install.Nodes{install.Node(srcNode)},
					Delay:       2 * time.Second,
				},
			},
		},
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
			// Note that this is one way latency, since the sender doesn't have the matching port.
			delayedLatency, err := roachtestutil.PortLatency(ctx, l, c, c.Nodes(srcNode), c.Nodes(destNode))
			if err != nil {
				return err
			}
			normalLatency, err := roachtestutil.PortLatency(ctx, l, c, c.Nodes(unaffectedNode), c.Nodes(destNode))
			if err != nil {
				return err
			}
			if delayedLatency < normalLatency*2 {
				return errors.Errorf("expected latency between nodes with artificial latency (n%d and n%d) to be much higher than between nodes without (n%d and n%d)", srcNode, destNode, unaffectedNode, destNode)
			}
			if delayedLatency < time.Second || delayedLatency > 3*time.Second {
				return errors.Errorf("expected latency between nodes with artificial latency (n%d and n%d) to be at least within 1s and 3s", srcNode, destNode)
			}
			return nil
		},
		validateRestore: func(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
			delayedLatency, err := roachtestutil.PortLatency(ctx, l, c, c.Nodes(srcNode), c.Nodes(destNode))
			if err != nil {
				return err
			}
			normalLatency, err := roachtestutil.PortLatency(ctx, l, c, c.Nodes(unaffectedNode), c.Nodes(destNode))
			if err != nil {
				return err
			}
			if delayedLatency > 2*normalLatency {
				return errors.Errorf("expected latency between nodes with artificial latency (n%d and n%d) to be close to latency between nodes without (n%d and n%d)", srcNode, destNode, unaffectedNode, destNode)
			}
			if delayedLatency > 500*time.Millisecond {
				return errors.Errorf("expected latency between nodes with artificial latency (n%d and n%d) to have restored to at least less than 500ms", srcNode, destNode)
			}
			return nil
		},
	}
}

func setupFailureSmokeTests(ctx context.Context, t test.Test, c cluster.Cluster) error {
	// Download any dependencies needed.
	if err := c.Install(ctx, t.L(), c.CRDBNodes(), "nmap"); err != nil {
		return err
	}
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())
	// Run a light workload in the background so we have some traffic in the database.
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload init kv {pgurl:1}")
	t.Go(func(goCtx context.Context, l *logger.Logger) error {
		return c.RunE(goCtx, option.WithNodes(c.WorkloadNode()), "./cockroach workload run kv {pgurl:1-3}")
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
		bidirectionalNetworkPartitionTest(c),
		asymmetricIncomingNetworkPartitionTest(c),
		asymmetricOutgoingNetworkPartitionTest(c),
		latencyTest(c),
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
				t.Fatal(errors.Wrapf(err, "%s failed", test.testName))
			}
		}
		t.L().Printf("%s test complete", test.testName)
	}
}

func registerFISmokeTest(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "failure-injection/smoke-test",
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
		Name:             "failure-injection/smoke-test/noop",
		Owner:            registry.OwnerTestEng,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode(), spec.CPU(2), spec.WorkloadNodeCPU(2), spec.ReuseNone()),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.ManualOnly,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runFailureSmokeTest(ctx, t, c, true /* noopFailer */)
		},
	})
}
