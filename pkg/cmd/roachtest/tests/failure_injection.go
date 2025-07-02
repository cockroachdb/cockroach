// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/grafana"
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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// failureSmokeTest is a validation test for a FailureMode. The order of steps is:
// 1. Start workload in the background.
// 2. Setup()
// 3. Inject()
// 4. validateFailure()
// 5. WaitForFailureToPropagate()
// 6. Recover()
// 7. WaitForFailureToRecover()
// 8. validateRecover()
// 9. Cleanup()
//
// Note the asymmetry in the order of validateFailure() being called before
// WaitForFailureToPropagate() and validateRecover() being called after WaitForFailureToRecover().
// Both wait methods block until the cluster has stabilized, so we want to validate the failure
// before the cluster stabilizes but validate the recover after.
type failureSmokeTest struct {
	testName    string
	failureName string
	args        failures.FailureArgs
	// Validate that the failure was injected correctly.
	validateFailure func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error
	// Validate that the failure was recovered correctly.
	validateRecover func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error
	// The workload to be run during the failureSmokeTest, if nil, defaultSmokeTestWorkload is used.
	workload func(ctx context.Context, c cluster.Cluster, args ...string) error
	// The duration to run the workload for before injecting the failure.
	workloadRamp time.Duration
	// The additional duration to let the failure mode stay injected before attempting
	// to recover.
	failureModeRamp time.Duration
}

func (t *failureSmokeTest) run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster,
) (err error) {
	failer, err := c.GetFailer(l, c.CRDBNodes(), t.failureName, failures.ReplicationFactor(3))
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
		// Best effort try to clean up the test even if the test context gets cancelled.
		err = errors.CombineErrors(err, failer.Cleanup(context.Background(), quietLogger))
	}()

	quietLogger, file, err := roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "setup")
	if err != nil {
		return err
	}
	l.Printf("%s: Running Setup(); details in %s.log", t.failureName, file)
	if err = failer.Setup(ctx, quietLogger, t.args); err != nil {
		return err
	}

	if t.workloadRamp > 0 {
		l.Printf("sleeping for %s before injecting failure", t.workloadRamp)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.workloadRamp):
		}
	}

	quietLogger, file, err = roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "inject")
	if err != nil {
		return err
	}
	l.Printf("%s: Running Inject(); details in %s.log", t.failureName, file)
	if err = c.AddGrafanaAnnotation(ctx, l, grafana.AddAnnotationRequest{
		Text: fmt.Sprintf("%s injected", t.testName),
	}); err != nil {
		return err
	}
	if err = failer.Inject(ctx, quietLogger, t.args); err != nil {
		return err
	}

	l.Printf("validating failure was properly injected")
	if err = t.validateFailure(ctx, l, c, failer); err != nil {
		return err
	}

	// Allow the cluster to stabilize after the failure.
	quietLogger, file, err = roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "wait for propagate")
	if err != nil {
		return err
	}
	l.Printf("%s: Running WaitForFailureToPropagate(); details in %s.log", t.failureName, file)
	if err = failer.WaitForFailureToPropagate(ctx, quietLogger); err != nil {
		return err
	}

	if t.failureModeRamp > 0 {
		l.Printf("sleeping for %s before recovering failure", t.failureModeRamp)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.failureModeRamp):
		}
	}

	quietLogger, file, err = roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "recover")
	if err != nil {
		return err
	}
	l.Printf("%s: Running Recover(); details in %s.log", t.failureName, file)
	if err = c.AddGrafanaAnnotation(ctx, l, grafana.AddAnnotationRequest{
		Text: fmt.Sprintf("%s recovered", t.testName),
	}); err != nil {
		return err
	}
	if err = failer.Recover(ctx, quietLogger); err != nil {
		return err
	}

	// Allow the cluster to return to normal.
	quietLogger, file, err = roachtestutil.LoggerForCmd(l, c.CRDBNodes(), t.testName, "wait for recover")
	if err != nil {
		return err
	}
	l.Printf("%s: Running WaitForFailureToRecover(); details in %s.log", t.failureName, file)
	if err = failer.WaitForFailureToRecover(ctx, quietLogger); err != nil {
		return err
	}

	l.Printf("validating failure was properly recovered")
	return t.validateRecover(ctx, l, c, failer)
}

func (t *failureSmokeTest) noopRun(ctx context.Context, l *logger.Logger, c cluster.Cluster) error {
	failer, err := c.GetFailer(l, c.CRDBNodes(), t.failureName)
	if err != nil {
		return err
	}
	if err := t.validateFailure(ctx, l, c, failer); err == nil {
		return errors.New("no failure was injected but validation still passed")
	}
	if err := t.validateRecover(ctx, l, c, failer); err != nil {
		return errors.Wrapf(err, "no failure was injected but post recover validation still failed")
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
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
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
		validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
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
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
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
		validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
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
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
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
		validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
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
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
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
		validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
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
				return errors.Errorf("expected latency between nodes with artificial latency (n%d and n%d) to have recovred to at least less than 500ms", srcNode, destNode)
			}
			return nil
		},
	}
}

var cgroupsDiskStallTests = func(c cluster.Cluster) []failureSmokeTest {
	type rwBytes struct {
		stalledRead     int
		stalledWrite    int
		unaffectedRead  int
		unaffectedWrite int
	}
	getRWBytes := func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.CGroupDiskStaller, stalledNode, unaffectedNode option.NodeListOption) (rwBytes, error) {
		stalledReadBytes, stalledWriteBytes, err := f.GetReadWriteBytes(ctx, l, stalledNode.InstallNodes())
		if err != nil {
			return rwBytes{}, err
		}
		unaffectedNodeReadBytes, unaffectedNodeWriteBytes, err := f.GetReadWriteBytes(ctx, l, unaffectedNode.InstallNodes())
		if err != nil {
			return rwBytes{}, err
		}

		return rwBytes{
			stalledRead:     stalledReadBytes,
			stalledWrite:    stalledWriteBytes,
			unaffectedRead:  unaffectedNodeReadBytes,
			unaffectedWrite: unaffectedNodeWriteBytes,
		}, nil
	}

	// Returns the read and write bytes read/written to disk over the last 30 seconds of the
	// stalled node and a control unaffected node.
	getRWBytesOverTime := func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.CGroupDiskStaller, stalledNode, unaffectedNode option.NodeListOption) (rwBytes, error) {
		beforeRWBytes, err := getRWBytes(ctx, l, c, f, stalledNode, unaffectedNode)
		if err != nil {
			return rwBytes{}, err
		}
		// Evict the store from memory on each VM to force them to read from disk.
		// Without this, we don't run the workload long enough for the process to read
		// from disk instead of memory.
		c.Run(ctx, option.WithNodes(c.CRDBNodes()), "vmtouch -ve /mnt/data1")
		select {
		case <-ctx.Done():
			return rwBytes{}, ctx.Err()
		case <-time.After(30 * time.Second):
		}

		afterRWBytes, err := getRWBytes(ctx, l, c, f, stalledNode, unaffectedNode)
		if err != nil {
			return rwBytes{}, err
		}

		return rwBytes{
			stalledRead:     afterRWBytes.stalledRead - beforeRWBytes.stalledRead,
			stalledWrite:    afterRWBytes.stalledWrite - beforeRWBytes.stalledWrite,
			unaffectedRead:  afterRWBytes.unaffectedRead - beforeRWBytes.unaffectedRead,
			unaffectedWrite: afterRWBytes.unaffectedWrite - beforeRWBytes.unaffectedWrite,
		}, nil
	}

	assertRWBytes := func(ctx context.Context, l *logger.Logger, bytes rwBytes, readsStalled, writesStalled bool) error {
		// The threshold of bytes that we consider i/o to be "stalled". It would be nice to
		// assert that it is 0 (it usually is), but because of how cgroups throttles io,
		// (throughput is not a hard limit and limits can accumulate for bursts of io) we
		// sometimes see some throughput. The threshold of 90k bytes was chosen as double
		// the estimated throughput cgroups will allow over 30 seconds.
		threshold := 90000

		// If writes are stalled, assert that we observe no writes to disk on the stalled node.
		if writesStalled {
			if bytes.stalledWrite > threshold {
				return errors.Errorf("expected stalled node to have written no bytes to disk, but wrote %d", bytes.stalledWrite)
			}
		} else {
			// If writes are not stalled, e.g. read only stall or failure mode was recovered,
			// then assert that we observe writes to disk on the stalled node.
			if bytes.stalledWrite < threshold {
				return errors.Errorf("writes were not stalled on the stalled node, but only %d bytes were written", bytes.stalledWrite)
			}
		}
		// The unaffected node should always be writing to disk.
		if bytes.unaffectedWrite < threshold {
			return errors.Errorf("writes were not stalled on the unaffected node, but only %d bytes were written", bytes.unaffectedWrite)
		}

		// When checking that reads _aren't_ stalled, we have to use a weaker assertion.
		// Trying to force the process to read a consistent amount of bytes from disk
		// proves to be difficult and sometimes the process simply doesn't read that
		// many bytes. One reason is that the smoke test may choose to stall a majority
		// of nodes, so we lose quorum greatly decreasing overall throughput. However,
		// even within the same test configuration we still see high variation.
		readLowerThreshold := 4096

		// Same assertions as above but for reads instead.
		if readsStalled {
			if bytes.stalledRead > threshold {
				return errors.Errorf("expected stalled node to have read no bytes from disk, but read %d", bytes.stalledRead)
			}
		} else {
			if bytes.stalledRead < readLowerThreshold {
				err := errors.Errorf("reads were not stalled on the stalled node, but only %d bytes were read", bytes.stalledRead)
				// If writes are stalled, it may greatly impact read throughput even if reads are
				// not explicitly stalled. We generally still see reads, but it can sometimes be lower
				// than our threshold causing the test to flake In this case, just log a warning.
				if writesStalled {
					l.Printf("WARN: %s", err)
				} else {
					return err
				}
			}
		}
		if bytes.unaffectedRead < readLowerThreshold {
			return errors.Errorf("reads were not stalled on the unaffected node, but only %d bytes were read", bytes.unaffectedRead)
		}
		return nil
	}

	var tests []failureSmokeTest
	for _, stallWrites := range []bool{true, false} {
		for _, stallReads := range []bool{true, false} {
			if !stallWrites && !stallReads {
				continue
			}

			rng, _ := randutil.NewPseudoRand()
			// SeededRandGroups only returns an error if the requested size is larger than the
			// number of nodes, so we can safely ignore the error.
			groups, _ := c.CRDBNodes().SeededRandGroups(rng, 2 /* numGroups */)
			stalledNodeGroup := groups[0]
			unaffectedNodeGroup := groups[1]
			// To simplify the smoke test, only run validation on these two
			// randomly chosen nodes.
			stalledNode := stalledNodeGroup.SeededRandNode(rng)
			unaffectedNode := unaffectedNodeGroup.SeededRandNode(rng)

			testName := fmt.Sprintf("%s/WritesStalled=%t/ReadsStalled=%t", failures.CgroupsDiskStallName, stallWrites, stallReads)
			tests = append(tests, failureSmokeTest{
				testName:    testName,
				failureName: failures.CgroupsDiskStallName,
				args: failures.DiskStallArgs{
					StallWrites:  stallWrites,
					StallReads:   stallReads,
					RestartNodes: true,
					Nodes:        stalledNodeGroup.InstallNodes(),
				},
				validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
					l.Printf("Stalled nodes: %d, Unaffected nodes: %d, Stalled validation node: %d, Unaffected validation node: %d", stalledNodeGroup, unaffectedNodeGroup, stalledNode, unaffectedNode)
					res, err := getRWBytesOverTime(ctx, l, c, f.FailureMode.(*failures.CGroupDiskStaller), stalledNode, unaffectedNode)
					if err != nil {
						return err
					}
					l.Printf("ReadBytes and WriteBytes over last 30 seconds:%+v", res)

					return assertRWBytes(ctx, l, res, stallReads, stallWrites)
				},
				validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
					res, err := getRWBytesOverTime(ctx, l, c, f.FailureMode.(*failures.CGroupDiskStaller), stalledNode, unaffectedNode)
					if err != nil {
						return err
					}
					l.Printf("ReadBytes and WriteBytes over last 30 seconds:%+v", res)

					// The cluster should be fully recovered from the stalls, so assert that
					// reads and writes are not stalled on both the stalled and unaffected nodes.
					return assertRWBytes(ctx, l, res, false /*readsStalled*/, false /*writesStalled*/)
				},
				workload: func(ctx context.Context, c cluster.Cluster, args ...string) error {
					return defaultFailureSmokeTestWorkload(ctx, c,
						// Tolerate errors as we expect nodes to fatal.
						"--tolerate-errors",
						// Without this, kv workload will read from keys with no values.
						"--sequential", "--cycle-length=1000", "--read-percent=50",
						// Bump up the block bytes in attempt to make the test more stable. This will
						// both increase the throughput and reduce the chance we see cgroups allow io
						// since any given io request will be much larger.
						"--min-block-bytes=65536", "--max-block-bytes=65536")
				},
				// Wait for two minutes with the failure injected so the cluster starts rebalancing
				// ranges, and we can properly test WaitForFailureToRecover.
				failureModeRamp: 2 * time.Minute,
			})
		}
	}

	return tests
}

var cgroupStallLogsTest = func(c cluster.Cluster) failureSmokeTest {
	nodes := c.CRDBNodes()
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	// We only want to stall one node for this test. If we stall writes on a quorum
	// of nodes, then auth-session login will fail even if logs are not stalled.
	stalledNode := c.Node(nodes[0])
	unaffectedNode := c.Node(nodes[1])

	getSessionCookie := func(
		ctx context.Context,
		l *logger.Logger,
		c cluster.Cluster,
		node option.NodeListOption,
	) bool {
		loginCmd := fmt.Sprintf(
			"%s auth-session login root --url={pgurl%s} --certs-dir ./certs --only-cookie",
			test.DefaultCockroachPath, node,
		)
		err := c.RunE(ctx, option.WithNodes(node), loginCmd)
		return err == nil
	}

	return failureSmokeTest{
		testName:    fmt.Sprintf("%s/WritesStalled=true/LogsStalled=true", failures.CgroupsDiskStallName),
		failureName: failures.CgroupsDiskStallName,
		args: failures.DiskStallArgs{
			StallWrites:  true,
			RestartNodes: true,
			Nodes:        stalledNode.InstallNodes(),
			StallLogs:    true,
		},
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
			// Confirm symlink exists
			if err := c.RunE(ctx, option.WithNodes(stalledNode), "test -L logs"); err != nil {
				return errors.Wrapf(err, "`logs` is not a symlink on node %d", stalledNode)
			}

			// The cockroach-sql-auth.log file is appended to each time an authenticated session event
			// occurs, e.g. a client logging in. If we attempt to fetch a cookie from the stalled node,
			// we should expect to see our request time out, as it will be unable to write to the log.
			//
			// TODO(darryl): think of ways to deflake this check. Due to the nature of cgroups
			// throttling, sometimes we see that the stalled node is able to write to logs. Logs
			// usually require very low throughput so they are more suspect to cgroups allowing
			// writes on a stalled node.
			if getSessionCookie(ctx, l, c, stalledNode) {
				l.Printf("WARN: was able to successfully get session cookie from stalled node %d", stalledNode)
			}

			// The unaffected node should be able to write to the log, so we should be able to
			// get the session cookie with no issues.
			if !getSessionCookie(ctx, l, c, unaffectedNode) {
				l.Printf("WARN: was unable to get session cookie from unaffected node %d", unaffectedNode)
			}
			return nil
		},
		validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
			if !getSessionCookie(ctx, l, c, stalledNode) {
				return errors.Errorf("was unable to get session cookie from stalled node %d", stalledNode)
			}
			return nil
		},
		workload: func(ctx context.Context, c cluster.Cluster, args ...string) error {
			return nil
		},
	}
}

var dmsetupDiskStallTest = func(c cluster.Cluster) failureSmokeTest {
	rng, _ := randutil.NewPseudoRand()
	// SeededRandGroups only returns an error if the requested size is larger than the
	// number of nodes, so we can safely ignore the error.
	groups, _ := c.CRDBNodes().SeededRandGroups(rng, 2 /* numGroups */)
	stalledNodeGroup := groups[0]
	unaffectedNodeGroup := groups[1]
	// These are the nodes that we will run validation on.
	stalledNode := stalledNodeGroup.SeededRandNode(rng)
	unaffectedNode := unaffectedNodeGroup.SeededRandNode(rng)

	// touchFile attempts to create a file and returns whether it succeeded.
	touchFile := func(ctx context.Context, l *logger.Logger, c cluster.Cluster, node option.NodeListOption) bool {
		timeoutCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		err := c.RunE(timeoutCtx, option.WithNodes(node), "touch /mnt/data1/test.txt")
		if err != nil {
			l.Printf("failed to create file on node %d: %v", node, err)
			return false
		}
		return true
	}

	return failureSmokeTest{
		testName:    failures.DmsetupDiskStallName,
		failureName: failures.DmsetupDiskStallName,
		args: failures.DiskStallArgs{
			Nodes:        stalledNodeGroup.InstallNodes(),
			RestartNodes: true,
		},
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
			l.Printf("Stalled nodes: %d, Unaffected nodes: %d, Stalled validation node: %d, Unaffected validation node: %d", stalledNodeGroup, unaffectedNodeGroup, stalledNode, unaffectedNode)
			if touchFile(ctx, l, c, stalledNode) {
				return errors.Errorf("expected node %d to be stalled and creating a file to hang", stalledNode)
			}
			if !touchFile(ctx, l, c, unaffectedNode) {
				return errors.Errorf("expected creating a file to work on unaffected node %d", stalledNode)
			}
			return nil
		},
		validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
			if !touchFile(ctx, l, c, stalledNode) {
				return errors.Errorf("expected creating a file to work on stalled node %d", stalledNode)
			}
			return nil
		},
		workload: func(ctx context.Context, c cluster.Cluster, args ...string) error {
			// Tolerate errors as we expect nodes to fatal.
			return defaultFailureSmokeTestWorkload(ctx, c, "--tolerate-errors")
		},
		// Wait for two minutes with the failure injected so the cluster starts rebalancing
		// ranges, and we can properly test WaitForFailureToRecover.
		failureModeRamp: 2 * time.Minute,
	}
}

var processKillTests = func(c cluster.Cluster) []failureSmokeTest {
	rng, _ := randutil.NewPseudoRand()
	var tests []failureSmokeTest
	for _, gracefulShutdown := range []bool{true, false} {
		groups, _ := c.CRDBNodes().SeededRandGroups(rng, 2 /* numGroups */)
		killedNodeGroup := groups[0]
		unaffectedNodeGroup := groups[1]

		// These are the nodes that we will run validation on.
		killedNode := killedNodeGroup.SeededRandNode(rng)
		unaffectedNode := unaffectedNodeGroup.SeededRandNode(rng)

		tests = append(tests, failureSmokeTest{
			testName:    fmt.Sprintf("%s/GracefulShutdown=%t", failures.ProcessKillFailureName, gracefulShutdown),
			failureName: failures.ProcessKillFailureName,
			args: failures.ProcessKillArgs{
				Nodes:            killedNodeGroup.InstallNodes(),
				GracefulShutdown: gracefulShutdown,
				GracePeriod:      time.Minute,
			},
			validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
				// If we initiate a graceful shutdown, the cockroach process should
				// intercept it and start draining the node.
				if gracefulShutdown {
					err := testutils.SucceedsSoonError(func() error {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						res, err := c.RunWithDetailsSingleNode(ctx, l, option.WithNodes(unaffectedNode), fmt.Sprintf("./cockroach node status %d --decommission --certs-dir=%s | sed -n '2p' | awk '{print $NF}'", killedNode[0], install.CockroachNodeCertsDir))
						if err != nil {
							return err
						}
						isDraining := strings.TrimSpace(res.Stdout)
						if isDraining != "true" {
							return errors.Errorf("expected node %d to be draining", killedNode[0])
						}
						return nil
					})
					if err != nil {
						return err
					}
				}

				// Check that we aren't able to establish a SQL connection to the killed node.
				// waitForFailureToPropagate already checks system death for us, which is a
				// stronger assertion than checking SQL connections are unavailable. We
				// are mostly doing this to satisfy the smoke test framework since this is
				// a fairly simple failure mode with less to validate.
				err := testutils.SucceedsSoonError(func() error {
					if ctx.Err() != nil {
						return ctx.Err()
					}

					killedDB, err := c.ConnE(ctx, l, killedNode[0])
					if err == nil {
						defer killedDB.Close()
						if err := killedDB.Ping(); err == nil {
							return errors.Errorf("expected node %d to be dead, but it is alive", killedNode)
						} else {
							l.Printf("failed to connect to node %d: %v", killedNode, err)
						}
					} else {
						l.Printf("unable to establish SQL connection to node %d", killedNode)
					}
					return nil
				})

				return err
			},
			// Similar to validateFailure, there is not much to validate here that isn't
			// covered by WaitForFailureToRecover, so just skip it.
			validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
				return nil
			},
			workload: func(ctx context.Context, c cluster.Cluster, args ...string) error {
				return defaultFailureSmokeTestWorkload(ctx, c, "--tolerate-errors")
			},
			// Shutting down the server right after it's started can cause draining to be skipped.
			workloadRamp: 30 * time.Second,
			// Wait for a minute with the failure injected so the cluster starts rebalancing
			// ranges, and we can properly test WaitForFailureToRecover.
			failureModeRamp: time.Minute,
		})
	}

	groups, _ := c.CRDBNodes().SeededRandGroups(rng, 2 /* numGroups */)
	killedNodeGroup := groups[0]
	// This is the node that we will run validation on.
	killedNode := killedNodeGroup.SeededRandNode(rng)
	noopSignal := 0

	// Test that the GracePeriod logic will kick in if the SIGTERM hangs.
	tests = append(tests, failureSmokeTest{
		testName:    fmt.Sprintf("%s/hanging-drain", failures.ProcessKillFailureName),
		failureName: failures.ProcessKillFailureName,
		args: failures.ProcessKillArgs{
			Nodes:       killedNode.InstallNodes(),
			Signal:      &noopSignal,
			GracePeriod: 30 * time.Second,
		},
		// There isn't anything to validate here because our failure is effectively
		// a noop at first. Only after the GracePeriod will we see anything happen.
		// We could block for 30 seconds and then check that the node is dead, but
		// this is the same thing WaitForFailureToPropagate does for us.
		validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
			return nil
		},
		validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
			return nil
		},
		workload: func(ctx context.Context, c cluster.Cluster, args ...string) error {
			return defaultFailureSmokeTestWorkload(ctx, c, "--tolerate-errors")
		},
		// Shutting down the server right after it's started can cause draining to be skipped.
		workloadRamp: 30 * time.Second,
	})
	return tests
}

var resetVMTests = func(c cluster.Cluster) []failureSmokeTest {
	rng, _ := randutil.NewPseudoRand()
	rebootedNode := c.CRDBNodes().SeededRandNode(rng)
	var tests []failureSmokeTest
	for _, stopCluster := range []bool{true, false} {
		tests = append(tests, failureSmokeTest{
			testName:    fmt.Sprintf("%s/StopCluster=%t", failures.ResetVMFailureName, stopCluster),
			failureName: failures.ResetVMFailureName,
			args: failures.ResetVMArgs{
				Nodes:         rebootedNode.InstallNodes(),
				StopProcesses: stopCluster,
			},
			validateFailure: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
				// Check that we aren't able to establish a SQL connection to the rebooted node.
				// waitForFailureToPropagate already does a similar check, but we do it here
				// to satisfy the smoke test framework since this is a fairly simple failure
				// mode with less to validate.
				return testutils.SucceedsSoonError(func() error {
					if ctx.Err() != nil {
						return ctx.Err()
					}

					killedDB, err := c.ConnE(ctx, l, rebootedNode[0])
					if err == nil {
						defer killedDB.Close()
						if err := killedDB.Ping(); err == nil {
							return errors.Errorf("expected node %d to be dead, but it is alive", rebootedNode)
						} else {
							l.Printf("failed to connect to node %d: %v", rebootedNode, err)
						}
					} else {
						l.Printf("unable to establish SQL connection to node %d", rebootedNode)
					}
					return nil
				})
			},
			validateRecover: func(ctx context.Context, l *logger.Logger, c cluster.Cluster, f *failures.Failer) error {
				return nil
			},
			workload: func(ctx context.Context, c cluster.Cluster, args ...string) error {
				return defaultFailureSmokeTestWorkload(ctx, c, "--tolerate-errors")
			},
		})
	}
	return tests
}

func defaultFailureSmokeTestWorkload(ctx context.Context, c cluster.Cluster, args ...string) error {
	workloadArgs := strings.Join(args, " ")
	cmd := roachtestutil.NewCommand("./cockroach workload run kv %s", workloadArgs).
		Arg("{pgurl%s}", c.CRDBNodes()).
		String()
	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
}

func setupFailureSmokeTests(ctx context.Context, t test.Test, c cluster.Cluster) error {
	// Download any dependencies needed.
	if err := c.Install(ctx, t.L(), c.CRDBNodes(), "nmap"); err != nil {
		return err
	}
	if err := c.Install(ctx, t.L(), c.CRDBNodes(), "vmtouch"); err != nil {
		return err
	}
	startSettings := install.MakeClusterSettings()
	startSettings.Env = append(startSettings.Env,
		// Increase the time writes must be stalled before a node fatals. Disk stall tests
		// want to query read/write bytes for 30 seconds to validate that the stall is working,
		// so we set max sync duration to 2 minutes to make sure the node doesn't die before then.
		// Don't disable outright as we still want to test that the node eventually dies.
		fmt.Sprintf("COCKROACH_LOG_MAX_SYNC_DURATION=%s", 2*time.Minute),
		fmt.Sprintf("COCKROACH_ENGINE_MAX_SYNC_DURATION_DEFAULT=%s", 2*time.Minute))

	// Some failure modes may take down a node. To speed up the exercise of waiting
	// for a failure to propagate/restore, set `server.time_until_store_dead` to 30s.
	startSettings.ClusterSettings["server.time_until_store_dead"] = "30s"
	c.Start(ctx, t.L(), option.DefaultStartOpts(), startSettings, c.CRDBNodes())

	// Initialize the workloads we will use.
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload init kv {pgurl:1}")
	return nil
}

func runFailureSmokeTest(ctx context.Context, t test.Test, c cluster.Cluster, noopFailer bool) {
	if err := setupFailureSmokeTests(ctx, t, c); err != nil {
		t.Error(err)
	}

	var failureSmokeTests = []failureSmokeTest{
		bidirectionalNetworkPartitionTest(c),
		asymmetricIncomingNetworkPartitionTest(c),
		asymmetricOutgoingNetworkPartitionTest(c),
		latencyTest(c),
		dmsetupDiskStallTest(c),
		cgroupStallLogsTest(c),
	}
	failureSmokeTests = append(failureSmokeTests, cgroupsDiskStallTests(c)...)
	failureSmokeTests = append(failureSmokeTests, processKillTests(c)...)
	failureSmokeTests = append(failureSmokeTests, resetVMTests(c)...)

	// Randomize the order of the tests in case any of the failures have unexpected side
	// effects that may mask failures, e.g. a cgroups disk stall isn't properly recovered
	// which causes a dmsetup disk stall to appear to work even if it doesn't.
	rand.Shuffle(len(failureSmokeTests), func(i, j int) {
		failureSmokeTests[i], failureSmokeTests[j] = failureSmokeTests[j], failureSmokeTests[i]
	})

	// For testing new failure modes, it may be useful to run only a subset of
	// tests to increase iteration speed.
	if regex := os.Getenv("FAILURE_INJECTION_SMOKE_TEST_FILTER"); regex != "" {
		filter, err := regexp.Compile(regex)
		if err != nil {
			t.Fatal(err)
		}
		var filteredTests []failureSmokeTest
		for _, test := range failureSmokeTests {
			if filter.MatchString(test.testName) {
				filteredTests = append(filteredTests, test)
			}
		}
		failureSmokeTests = filteredTests
	}

	for _, test := range failureSmokeTests {
		t.L().Printf("\n=====running %s test=====", test.testName)
		if noopFailer {
			if err := test.noopRun(ctx, t.L(), c); err != nil {
				t.Fatal(err)
			}
		} else {
			backgroundWorkload := defaultFailureSmokeTestWorkload
			if test.workload != nil {
				backgroundWorkload = test.workload
			}
			cancel := t.GoWithCancel(func(goCtx context.Context, l *logger.Logger) error {
				return backgroundWorkload(goCtx, c)
			}, task.Name(fmt.Sprintf("%s-workload", test.testName)))
			err := test.run(ctx, t.L(), c)
			cancel()
			if err != nil {
				t.Fatal(errors.Wrapf(err, "%s failed", test.testName))
			}

		}
		t.L().Printf("%s test complete", test.testName)
	}
}

func registerFISmokeTest(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "failure-injection/smoke-test",
		Owner: registry.OwnerTestEng,
		// We want at least 4 CRDB nodes so ranges get rebalanced when nodes go down.
		Cluster:          r.MakeClusterSpec(5, spec.WorkloadNode(), spec.CPU(2), spec.WorkloadNodeCPU(2), spec.ReuseNone()),
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
		Cluster:          r.MakeClusterSpec(5, spec.WorkloadNode(), spec.CPU(2), spec.WorkloadNodeCPU(2), spec.ReuseNone()),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.ManualOnly,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runFailureSmokeTest(ctx, t, c, true /* noopFailer */)
		},
	})
}
