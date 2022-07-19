// Copyright 2022 The Cockroach Authors.
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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

const (
	// how many resolved timestamps to wait for before considering the
	// system to be working as intended.
	requestedResolved = 20

	// retryDuration is the duration that we will wait for a validator
	// command to succeed; while the upgrade is running, there could be
	// intermittent errors as the node is brought back up.
	retryDuration = 2 * time.Minute

	// wait for Kafka to be ready before attempting to create a topic
	// for CDC. Without waiting, a `kafka controller not available`
	// error is seen with 1-5% probability
	kafkaWait = 5 * time.Second
)

var (
	// the CDC target (table). We're running the tpcc workload in this
	// test; verifying that the changefeed in one of the tables
	// works is sufficient for our purposes
	target = "tpcc.new_order"
)

func registerCDCMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "cdc/mixed-versions",
		Owner:           registry.OwnerTestEng,
		Cluster:         r.MakeClusterSpec(5),
		Timeout:         30 * time.Minute,
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCMixedVersions(ctx, t, c, *t.BuildVersion())
		},
	})
}

// cdcMixedVersionTester implements mixed-version/upgrade testing for
// CDC. It knows how to set up the cluster to run Kafka, monitor
// events, and run validations that ensure changefeeds work during and
// after upgrade.
type cdcMixedVersionTester struct {
	ctx           context.Context
	crdbNodes     option.NodeListOption
	workloadNodes option.NodeListOption
	kafkaNodes    option.NodeListOption
	monitor       cluster.Monitor
	verifierDone  chan struct{}
	kafka         kafkaManager
	validator     *cdctest.CountValidator
	cleanup       func()
}

func newCDCMixedVersionTester(
	ctx context.Context, t test.Test, c cluster.Cluster,
) cdcMixedVersionTester {
	crdbNodes := c.Range(1, c.Spec().NodeCount-1)
	lastNode := c.Node(c.Spec().NodeCount)

	c.Put(ctx, t.Cockroach(), "./cockroach", lastNode)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", lastNode)

	return cdcMixedVersionTester{
		ctx:           ctx,
		crdbNodes:     crdbNodes,
		workloadNodes: lastNode,
		kafkaNodes:    lastNode,
		monitor:       c.NewMonitor(ctx, crdbNodes),
		verifierDone:  make(chan struct{}),
	}
}

// StartKafka will install and start Kafka on the configured node. It
// will also create the topic where changefeed events will be sent.
func (cmvt *cdcMixedVersionTester) StartKafka(t test.Test, c cluster.Cluster) {
	t.Status("starting Kafka node")
	cmvt.kafka, cmvt.cleanup = setupKafka(cmvt.ctx, t, c, cmvt.kafkaNodes)
	time.Sleep(kafkaWait)
	if err := cmvt.kafka.createTopic(cmvt.ctx, target); err != nil {
		t.Fatal(err)
	}
}

// Cleanup is supposed to be called at the end of tests that use
// `cdcMixedVersionTester`
func (cmvt *cdcMixedVersionTester) Cleanup() {
	if cmvt.cleanup != nil {
		cmvt.cleanup()
	}
}

// installAndStartTPCCWorkload starts a TPCC workload for the given
// duration. This step does not block; if an error is found while
// runing the workflow, the test fails.
func (cmvt *cdcMixedVersionTester) installAndStartTPCCWorkload(d time.Duration) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.Status("installing and running workload")
		tpcc := tpccWorkload{
			sqlNodes:           cmvt.crdbNodes,
			workloadNodes:      cmvt.workloadNodes,
			tpccWarehouseCount: 10,
			tolerateErrors:     true, // we're bringing nodes down while upgrading
		}

		tpcc.install(ctx, u.c)
		cmvt.monitor.Go(func(ctx context.Context) error {
			tpcc.run(ctx, u.c, d.String())
			return nil
		})
	}
}

// waitForVerifier waits for the underlying CDC verifier to reach the
// desired number of resolved timestamps.
func (cmvt *cdcMixedVersionTester) waitForVerifier() versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.Status("waiting for verifier")
		<-cmvt.verifierDone
	}
}

// setupVerifier creates a CDC validator to validate that a changefeed
// created on the `target` table is able to re-create the table
// somewhere else. It also verifies CDC's ordering guarantees. This
// step will not block, but will start the verifier in a separate Go
// routine. Use `waitForVerifier` to wait for the verifier to finish.
func (cmvt *cdcMixedVersionTester) setupVerifier(node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.Status(fmt.Sprintf("setting up changefeed verifier for table %s", target))

		// we could just return the error here and let `Wait` return the
		// error. However, calling t.Fatal directly lets us stop the test
		// earlier
		cmvt.monitor.Go(func(ctx context.Context) error {
			consumer, err := cmvt.kafka.consumer(ctx, "new_order")
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()

			db := u.conn(ctx, t, node)
			if _, err := db.Exec(
				"CREATE TABLE fprint (" +
					"no_o_id INT NOT NULL, " +
					"no_d_id INT NOT NULL, " +
					"no_w_id INT NOT NULL, " +
					"PRIMARY KEY (no_w_id, no_d_id, no_o_id))",
			); err != nil {
				t.Fatal(err)
			}

			fprintV, err := cdctest.NewFingerprintValidator(db, `tpcc.new_order`, `fprint`, consumer.partitions, 0)
			if err != nil {
				t.Fatal(err)
			}
			validators := cdctest.Validators{
				cdctest.NewOrderValidator("tpcc.new_order"),
				fprintV,
			}
			cmvt.validator = cdctest.MakeCountValidator(validators)

			for {
				m := consumer.Next(ctx)
				if m == nil {
					t.Fatal(fmt.Errorf("unexpected end of changefeed"))
					return nil
				}

				updated, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
				if err != nil {
					t.Fatal(err)
					return nil
				}

				partitionStr := strconv.Itoa(int(m.Partition))
				// errors in the calls to the validator below could lead to
				// transient failures if the cluster is upgrading, so we retry
				// for 1 minute.
				if len(m.Key) > 0 {
					if err := retry.ForDuration(retryDuration, func() error {
						return cmvt.validator.NoteRow(partitionStr, string(m.Key), string(m.Value), updated)
					}); err != nil {
						t.Fatal(fmt.Errorf("timed out after %s: %w", retryDuration, err))
						return nil
					}
				} else {
					if err := retry.ForDuration(retryDuration, func() error {
						return cmvt.validator.NoteResolved(partitionStr, resolved)
					}); err != nil {
						t.Fatal(fmt.Errorf("timed out after %s: %w", retryDuration, err))
						return nil
					}
					t.L().Printf("%d of %d resolved timestamps validated, latest is %s behind realtime",
						cmvt.validator.NumResolvedWithRows, requestedResolved, timeutil.Since(resolved.GoTime()))

					if cmvt.validator.NumResolvedWithRows >= requestedResolved {
						break
					}
				}
			}

			close(cmvt.verifierDone)
			return nil
		})
	}
}

// assertValid checks if the validator has found any issues at the
// time the function is called.
func (cmvt *cdcMixedVersionTester) assertValid() versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		if failures := cmvt.validator.Failures(); len(failures) > 0 {
			t.Fatalf("validator failures:\n%s", strings.Join(failures, "\n"))
		}
	}
}

// createChangeFeed issues a call to the given node to create a change
// feed for the target table.
func (cmvt *cdcMixedVersionTester) createChangeFeed(node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.Status("creating changefeed")
		db := u.conn(ctx, t, node)
		cdcClusterSettings(t, sqlutils.MakeSQLRunner(db))

		opts := []string{"updated", "resolved"}
		if _, err := db.Exec(
			fmt.Sprintf("CREATE CHANGEFEED FOR %s INTO $1 WITH %s", target, strings.Join(opts, ", ")),
			cmvt.kafka.sinkURL(ctx),
		); err != nil {
			t.Fatal(err)
		}
	}
}

func runCDCMixedVersions(
	ctx context.Context, t test.Test, c cluster.Cluster, buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	tester := newCDCMixedVersionTester(ctx, t, c)
	tester.StartKafka(t, c)
	defer tester.Cleanup()

	// letWorkloadRun is a step added at different points in the test
	// when we want the workload to have some time to run against a
	// specific version of the database
	letWorkloadRun := sleepStep(10 * time.Second)

	// sqlNode is the node we use to send SQL statements during this
	// test (to do things like change settings, create a changefeed,
	// etc)
	sqlNode := tester.crdbNodes[0]

	// An empty string will lead to the cockroach binary specified by flag
	// `cockroach` to be used.
	const mainVersion = ""
	newVersionUpgradeTest(c,
		uploadAndStartFromCheckpointFixture(tester.crdbNodes, predecessorVersion),
		tester.setupVerifier(sqlNode),
		tester.installAndStartTPCCWorkload(15*time.Minute),
		waitForUpgradeStep(tester.crdbNodes),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.
		preventAutoUpgradeStep(sqlNode),
		tester.createChangeFeed(sqlNode),

		letWorkloadRun,
		// Roll the nodes into the new version one by one in random order
		binaryUpgradeStep(tester.crdbNodes, mainVersion),
		// let the workload run in the new version for a while
		letWorkloadRun,

		tester.assertValid(),

		// Roll back again, which ought to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(tester.crdbNodes, predecessorVersion),
		letWorkloadRun,

		tester.assertValid(),

		// Roll nodes forward and finalize upgrade.
		binaryUpgradeStep(tester.crdbNodes, mainVersion),

		// allow cluster version to update
		allowAutoUpgradeStep(sqlNode),
		waitForUpgradeStep(tester.crdbNodes),

		tester.waitForVerifier(),
		tester.assertValid(),
	).run(ctx, t)
}
