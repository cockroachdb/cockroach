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
}

func (t *failureSmokeTest) run(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, fr *failures.FailureRegistry,
) (err error) {
	failer, err := roachtestutil.GetFailer(fr, c, t.failureName, l)
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

func (t *failureSmokeTest) noopRun(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, fr *failures.FailureRegistry,
) error {
	failer, err := roachtestutil.GetFailer(fr, c, t.failureName, l)
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
				return errors.Errorf("reads were not stalled on the stalled node, but only %d bytes were read", bytes.stalledRead)
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
					// Wait for replication since the stalled node may have just restarted.
					// TODO(darryl): The failure mode itself should do this in WaitForFailureToRecover.
					// It should also wait for replicas to rebalance, although this test is not large
					// enough for that to matter.
					db := c.Conn(ctx, l, stalledNode[0])
					defer db.Close()
					if err := roachtestutil.WaitForReplication(ctx, l, db, 3 /* replicationFactor */, roachtestutil.AtLeastReplicationFactor); err != nil {
						return err
					}
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
			})
		}
	}

	return tests
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
	}
}

func defaultFailureSmokeTestWorkload(ctx context.Context, c cluster.Cluster, args ...string) error {
	workloadArgs := strings.Join(args, " ")
	cmd := roachtestutil.NewCommand("./cockroach workload run kv %s", workloadArgs).
		Arg("{pgurl%s}", c.CRDBNodes()).
		String()
	return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd)
}

func setupFailureSmokeTests(
	ctx context.Context, t test.Test, c cluster.Cluster, fr *failures.FailureRegistry,
) error {
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
	c.Start(ctx, t.L(), option.DefaultStartOpts(), startSettings, c.CRDBNodes())

	// Initialize the workloads we will use.
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload init kv {pgurl:1}")
	return nil
}

func runFailureSmokeTest(ctx context.Context, t test.Test, c cluster.Cluster, noopFailer bool) {
	fr := failures.NewFailureRegistry()
	fr.Register()
	if err := setupFailureSmokeTests(ctx, t, c, fr); err != nil {
		t.Error(err)
	}

	var failureSmokeTests = []failureSmokeTest{
		bidirectionalNetworkPartitionTest(c),
		asymmetricIncomingNetworkPartitionTest(c),
		asymmetricOutgoingNetworkPartitionTest(c),
		latencyTest(c),
		dmsetupDiskStallTest(c),
	}
	failureSmokeTests = append(failureSmokeTests, cgroupsDiskStallTests(c)...)

	// Randomize the order of the tests in case any of the failures have unexpected side
	// effects that may mask failures, e.g. a cgroups disk stall isn't properly recovered
	// which causes a dmsetup disk stall to appear to work even if it doesn't.
	rand.Shuffle(len(failureSmokeTests), func(i, j int) {
		failureSmokeTests[i], failureSmokeTests[j] = failureSmokeTests[j], failureSmokeTests[i]
	})

	for _, test := range failureSmokeTests {
		t.L().Printf("\n=====running %s test=====", test.testName)
		if noopFailer {
			if err := test.noopRun(ctx, t.L(), c, fr); err != nil {
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
			err := test.run(ctx, t.L(), c, fr)
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
