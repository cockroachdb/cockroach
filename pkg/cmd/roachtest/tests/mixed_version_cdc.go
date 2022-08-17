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
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
)

const (
	// how many resolved timestamps to wait for before considering the
	// system to be working as intended at a specific version state
	resolvedTimestampsPerState = 5

	// resolvedInterval is the value passed to the `resolved` option
	// when creating the changefeed
	resolvedInterval = "10s"
)

var (
	// the CDC target, DB and table. We're running the bank workload in
	// this test.
	targetDB    = "bank"
	targetTable = "bank"

	timeout = 30 * time.Minute
)

func registerCDCMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "cdc/mixed-versions",
		Owner:           registry.OwnerTestEng,
		Cluster:         r.MakeClusterSpec(5),
		Timeout:         timeout,
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
	ctx                context.Context
	crdbNodes          option.NodeListOption
	workloadNodes      option.NodeListOption
	kafkaNodes         option.NodeListOption
	monitor            cluster.Monitor
	timestampsResolved struct {
		syncutil.Mutex
		C chan struct{}
	}
	kafka     kafkaManager
	validator *cdctest.CountValidator
	cleanup   func()
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
	}
}

// StartKafka will install and start Kafka on the configured node. It
// will also create the topic where changefeed events will be sent.
func (cmvt *cdcMixedVersionTester) StartKafka(t test.Test, c cluster.Cluster) {
	t.Status("starting Kafka node")
	cmvt.kafka, cmvt.cleanup = setupKafka(cmvt.ctx, t, c, cmvt.kafkaNodes)

	// try to create a Kafka topic for some time. Without waiting, a
	// `kafka controller not available` error is seen with 1-5%
	// probability
	if err := retry.ForDuration(1*time.Minute, func() error {
		return cmvt.kafka.createTopic(cmvt.ctx, targetTable)
	}); err != nil {
		t.Fatal(fmt.Errorf("timed out trying to create kafka topic: %w", err))
	}
}

// Cleanup is supposed to be called at the end of tests that use
// `cdcMixedVersionTester`
func (cmvt *cdcMixedVersionTester) Cleanup() {
	if cmvt.cleanup != nil {
		cmvt.cleanup()
	}
}

// installAndStartWorkload starts a bank workload asynchronously
func (cmvt *cdcMixedVersionTester) installAndStartWorkload() versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.Status("installing and running workload")
		u.c.Run(ctx, cmvt.workloadNodes, "./workload init bank {pgurl:1}")
		cmvt.monitor.Go(func(ctx context.Context) error {
			return u.c.RunE(
				ctx,
				cmvt.workloadNodes,
				fmt.Sprintf("./workload run bank {pgurl%s} --max-rate=10 --tolerate-errors", cmvt.crdbNodes),
			)
		})
	}
}

// waitForResolvedTimestamps waits for the underlying CDC verifier to
// resolve `resolvedTimestampsPerState`
func (cmvt *cdcMixedVersionTester) waitForResolvedTimestamps() versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		t.Status(fmt.Sprintf("waiting for %d resolved timestamps", resolvedTimestampsPerState))
		// create a new channel for the resolved timestamps, allowing any
		// new resolved timestamps to be captured and account for in the
		// loop below
		func() {
			cmvt.timestampsResolved.Lock()
			defer cmvt.timestampsResolved.Unlock()

			cmvt.timestampsResolved.C = make(chan struct{})
		}()

		var resolved int
		for range cmvt.timestampsResolved.C {
			resolved++
			t.L().Printf("%d of %d timestamps resolved", resolved, resolvedTimestampsPerState)
			if resolved == resolvedTimestampsPerState {
				break
			}
		}

		// set the resolved timestamps channel back to `nil`; while in
		// this state, any new resolved timestamps will be ignored
		func() {
			cmvt.timestampsResolved.Lock()
			defer cmvt.timestampsResolved.Unlock()

			cmvt.timestampsResolved.C = nil
		}()
	}
}

// setupVerifier creates a CDC validator to validate that a changefeed
// created on the `target` table is able to re-create the table
// somewhere else. It also verifies CDC's ordering guarantees. This
// step will not block, but will start the verifier in a separate Go
// routine. Use `waitForVerifier` to wait for the verifier to finish.
func (cmvt *cdcMixedVersionTester) setupVerifier(node int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		tableName := targetDB + "." + targetTable
		t.Status(fmt.Sprintf("setting up changefeed verifier for table %s", tableName))

		// we could just return the error here and let `Wait` return the
		// error. However, calling t.Fatal directly lets us stop the test
		// earlier
		cmvt.monitor.Go(func(ctx context.Context) error {
			consumer, err := cmvt.kafka.consumer(ctx, targetTable)
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()

			db := u.conn(ctx, t, node)
			if _, err := db.Exec(
				"CREATE TABLE fprint (id INT PRIMARY KEY, balance INT, payload STRING)",
			); err != nil {
				t.Fatal(err)
			}

			fprintV, err := cdctest.NewFingerprintValidator(db, tableName, `fprint`, consumer.partitions, 0, true)
			if err != nil {
				t.Fatal(err)
			}
			validators := cdctest.Validators{
				cdctest.NewOrderValidator(tableName),
				fprintV,
			}
			cmvt.validator = cdctest.MakeCountValidator(validators)

			for {
				m := consumer.Next(ctx)
				if m == nil {
					t.L().Printf("end of changefeed")
					return nil
				}

				updated, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
				if err != nil {
					t.Fatal(err)
				}

				partitionStr := strconv.Itoa(int(m.Partition))
				if len(m.Key) > 0 {
					if err := cmvt.validator.NoteRow(partitionStr, string(m.Key), string(m.Value), updated); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := cmvt.validator.NoteResolved(partitionStr, resolved); err != nil {
						t.Fatal(err)
					}

					t.L().Printf("%d resolved timestamps validated, latest is %s behind realtime",
						cmvt.validator.NumResolvedWithRows, timeutil.Since(resolved.GoTime()))
					cmvt.timestampResolved()
				}
			}
		})
	}
}

// timestampsResolved updates the underlying channel if set (i.e., if
// we are waiting for resolved timestamps events)
func (cmvt *cdcMixedVersionTester) timestampResolved() {
	cmvt.timestampsResolved.Lock()
	defer cmvt.timestampsResolved.Unlock()

	if cmvt.timestampsResolved.C != nil {
		cmvt.timestampsResolved.C <- struct{}{}
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

		opts := []string{"updated", fmt.Sprintf("resolved='%s'", resolvedInterval)}
		if _, err := db.Exec(
			fmt.Sprintf("CREATE CHANGEFEED FOR %s.%s INTO $1 WITH %s", targetDB, targetTable, strings.Join(opts, ", ")),
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

	rng, seed := randutil.NewPseudoRand()
	t.L().Printf("random seed: %d", seed)

	// sqlNode returns the node to be used when sending SQL statements
	// during this test. It is randomized, but the random seed is logged
	// above.
	sqlNode := func() int {
		return tester.crdbNodes[rng.Intn(len(tester.crdbNodes))]
	}

	// An empty string will lead to the cockroach binary specified by flag
	// `cockroach` to be used.
	const mainVersion = ""
	newVersionUpgradeTest(c,
		uploadAndStartFromCheckpointFixture(tester.crdbNodes, predecessorVersion),
		tester.setupVerifier(sqlNode()),
		tester.installAndStartWorkload(),
		waitForUpgradeStep(tester.crdbNodes),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.
		preventAutoUpgradeStep(sqlNode()),
		tester.createChangeFeed(sqlNode()),

		tester.waitForResolvedTimestamps(),
		// Roll the nodes into the new version one by one in random order
		binaryUpgradeStep(tester.crdbNodes, mainVersion),
		// let the workload run in the new version for a while
		tester.waitForResolvedTimestamps(),

		tester.assertValid(),

		// Roll back again, which ought to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(tester.crdbNodes, predecessorVersion),
		tester.waitForResolvedTimestamps(),

		tester.assertValid(),

		// Roll nodes forward and finalize upgrade.
		binaryUpgradeStep(tester.crdbNodes, mainVersion),

		// allow cluster version to update
		allowAutoUpgradeStep(sqlNode()),
		waitForUpgradeStep(tester.crdbNodes),

		tester.waitForResolvedTimestamps(),
		tester.assertValid(),
	).run(ctx, t)
}
