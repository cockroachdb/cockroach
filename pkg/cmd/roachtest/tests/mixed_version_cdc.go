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
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// how many resolved timestamps to wait for before considering the
	// system to be working as intended at a specific version state
	resolvedTimestampsPerState = 5

	// resolvedInterval is the value passed to the `resolved` option
	// when creating the changefeed
	resolvedInterval = "5s"

	// kafkaBufferMessageSize is the number of messages from kafka
	// we allow to be buffered in memory before validating them.
	kafkaBufferMessageSize = 8192
)

var (
	// the CDC target, DB and table. We're running the bank workload in
	// this test.
	targetDB    = "bank"
	targetTable = "bank"

	timeout = 30 * time.Minute

	// teamcityAgentZone is the zone used in this test. Since this test
	// runs a lot of queries from the TeamCity agent to CRDB nodes, we
	// make sure to create roachprod nodes that are in the same region
	// as the TeamCity agents. Note that a change in the TeamCity agent
	// region could lead to this test timing out (see #92649 for an
	// occurrence of this).
	teamcityAgentZone = "us-east4-b"
)

func registerCDCMixedVersions(r registry.Registry) {
	var zones string
	if r.MakeClusterSpec(1).Cloud == spec.GCE {
		// see rationale in definition of `teamcityAgentZone`
		zones = teamcityAgentZone
	}
	r.Add(registry.TestSpec{
		Name:  "cdc/mixed-versions",
		Owner: registry.OwnerTestEng,
		// N.B. ARM64 is not yet supported, see https://github.com/cockroachdb/cockroach/issues/103888.
		Cluster:         r.MakeClusterSpec(5, spec.Zones(zones), spec.Arch(vm.ArchAMD64)),
		Timeout:         timeout,
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCMixedVersions(ctx, t, c)
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

	// This channel is closed after the workload has been initialized.
	workloadInit chan struct{}

	timestampsResolved struct {
		syncutil.Mutex
		C chan struct{}
	}

	kafka struct {
		manager kafkaManager

		// buf is used to buffer messages received from kafka to be sent
		// to the validator and validated when the cluster is stable (ie. no node restarts in progress).
		//
		// Typically, messages would be immediately forwared messages to the validator,
		// but we cannot do that right when a message is received because
		// some nodes may be offline/upgrading at the time (the validator
		// relies on having a stable DB connection to note the rows and fingerprint).
		buf []*sarama.ConsumerMessage

		consumer *topicConsumer
	}

	validator *validatorWithDBOverride
}

// validatorWithDBOverride is a *cdctest.CountValidator which contains other
// validators such as *cdctest.FingerPrintValidator.
//
// NB: The fingerprint validator depends on an internal DB connection. Because
// this test performs rolling upgrades, these connections aren't always safe to
// use. Thus, the caller must make sure no node restarts should be in progress
// when using the validator (hence the "Unsafe" API). A mixed version state
// during validation is okay, but connections should not be able to be torn
// down while performing validations.
type validatorWithDBOverride struct {
	unsafeCountV       *cdctest.CountValidator
	unsafeFingerPrintV *cdctest.FingerprintValidator
}

// UnsafeGetValidator returns a *cdctest.CountValidator and should not be called
// during node restarts. See comment on receiver for more details.
func (v *validatorWithDBOverride) UnsafeGetValidator() *cdctest.CountValidator {
	return v.unsafeCountV
}

func newValidator(
	orderValidator cdctest.Validator, fPrintV *cdctest.FingerprintValidator,
) *validatorWithDBOverride {
	validators := cdctest.Validators{
		orderValidator,
		fPrintV,
	}
	countValidator := cdctest.MakeCountValidator(validators)
	return &validatorWithDBOverride{unsafeCountV: countValidator, unsafeFingerPrintV: fPrintV}
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
		workloadInit:  make(chan struct{}),
	}
}

// StartKafka will install and start Kafka on the configured node. It
// will also create the topic where changefeed events will be sent and a consumer.
// Returns a function to clean up the consumer and kafka resources.
func (cmvt *cdcMixedVersionTester) StartKafka(t test.Test, c cluster.Cluster) (cleanup func()) {
	t.Status("starting Kafka node")

	manager, tearDown := setupKafka(cmvt.ctx, t, c, cmvt.kafkaNodes)
	if err := manager.createTopic(cmvt.ctx, targetTable); err != nil {
		t.Fatal(err)
	}
	cmvt.kafka.manager = manager

	consumer, err := cmvt.kafka.manager.newConsumer(cmvt.ctx, targetTable)
	if err != nil {
		t.Fatal(err)
	}

	cmvt.kafka.manager = manager
	cmvt.kafka.buf = make([]*sarama.ConsumerMessage, 0, kafkaBufferMessageSize)
	cmvt.kafka.consumer = consumer

	return func() {
		cmvt.kafka.consumer.Close()
		tearDown()
	}
}

// waitForResolvedTimestamps waits for a few resolved timestamps to assert
// that the changefeed is running.
func (cmvt *cdcMixedVersionTester) waitForResolvedTimestamps() mixedversion.UserFunc {
	return func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		l.Printf("waiting for %d resolved timestamps", resolvedTimestampsPerState)
		// create a new channel for the resolved timestamps, allowing any
		// new resolved timestamps to be captured and account for in the
		// loop below
		func() {
			cmvt.timestampsResolved.Lock()
			defer cmvt.timestampsResolved.Unlock()

			cmvt.timestampsResolved.C = make(chan struct{})
		}()

		var resolved int
		for resolved < resolvedTimestampsPerState {
			select {
			case <-cmvt.timestampsResolved.C:
				resolved++
				l.Printf("%d of %d timestamps resolved", resolved, resolvedTimestampsPerState)
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// set the resolved timestamps channel back to `nil`; while in
		// this state, any new resolved timestamps will be ignored
		func() {
			cmvt.timestampsResolved.Lock()
			defer cmvt.timestampsResolved.Unlock()

			cmvt.timestampsResolved.C = nil
		}()
		return nil
	}
}

// runKafkaConsumer creates a CDC validator to validate that a changefeed
// created on the target table is able to re-create the table
// somewhere else. It can also verify changefeed ordering guarantees.
func (cmvt *cdcMixedVersionTester) setupValidator() mixedversion.UserFunc {
	return func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		tableName := fmt.Sprintf("%s.%s", targetDB, targetTable)
		_, db := h.RandomDB(r, cmvt.crdbNodes)
		if _, err := db.Exec(
			"CREATE TABLE fprint (id INT PRIMARY KEY, balance INT, payload STRING)",
		); err != nil {
			return err
		}

		// NB: The fingerprint validator will save this db connection and use it
		// when we submit rows for validation.
		fprintV, err := cdctest.NewFingerprintValidator(db, tableName, `fprint`, cmvt.kafka.consumer.partitions, 0)
		if err != nil {
			return err
		}
		cmvt.validator = newValidator(cdctest.NewOrderValidator(tableName), fprintV)
		return nil
	}
}

// runKafkaConsumer continuously reads from the kafka consumer and places
// messages in a buffer. It expects to be run in a goroutine and does not
// rely on any DB connections.
func (cmvt *cdcMixedVersionTester) runKafkaConsumer() mixedversion.UserFunc {
	return func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		everyN := log.Every(30 * time.Second)

		// This runs until the test finishes, which will be signaled via
		// context cancellation. We rely on consumer.Next() to check
		// the context.
		for {
			m := cmvt.kafka.consumer.Next(ctx)
			if m == nil {
				// this is expected to happen once the test has finished and
				// Kafka is being shut down. If it happens in the middle of
				// the test, it will eventually time out, and this message
				// should allow us to see that the validator finished
				// earlier than it should have
				l.Printf("end of changefeed")
				return nil
			}

			// Forward resolved timetsamps to "heartbeat" that the changefeed is running.
			if len(m.Key) <= 0 {
				_, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
				if err != nil {
					return err
				}
				cmvt.timestampResolved()

				if everyN.ShouldLog() {
					l.Printf("latest resolved timestamp %d behind realtime", timeutil.Since(resolved.GoTime()))
				}
			}

			if len(cmvt.kafka.buf) == kafkaBufferMessageSize {
				return errors.Newf("kafka message buffer limit %d exceeded", kafkaBufferMessageSize)
			}
			cmvt.kafka.buf = append(cmvt.kafka.buf, m)
		}
	}
}

// validate reads rows from the kafka buffer and submits them to the validator
// to be validated.
func (cmvt *cdcMixedVersionTester) validate() mixedversion.UserFunc {
	return func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		// NB: The caller is responsible for making sure this function is run
		// when nodes are running.
		validator := cmvt.validator.UnsafeGetValidator()
		for _, m := range cmvt.kafka.buf {
			updated, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
			if err != nil {
				return err
			}

			partitionStr := strconv.Itoa(int(m.Partition))
			if len(m.Key) > 0 {
				if err := validator.NoteRow(partitionStr, string(m.Key), string(m.Value), updated); err != nil {
					return err
				}
			} else {
				if err := validator.NoteResolved(partitionStr, resolved); err != nil {
					return err
				}

				l.Printf("%d resolved timestamps validated", validator.NumResolvedWithRows)
			}

			if failures := validator.Failures(); len(failures) > 0 {
				return errors.Newf("validator failures:\n%s", strings.Join(failures, "\n"))
			}
		}

		cmvt.kafka.buf = make([]*sarama.ConsumerMessage, 0, kafkaBufferMessageSize)
		return nil
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

// createChangeFeed issues a call to the given node to create a change
// feed for the target table.
func (cmvt *cdcMixedVersionTester) createChangeFeed() func(context.Context, *logger.Logger, *rand.Rand, *mixedversion.Helper) error {
	return func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		// Wait for workload to be initialized so the target table exists.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cmvt.workloadInit:
		}
		_, db := h.RandomDB(r, cmvt.crdbNodes)

		options := map[string]string{
			"updated":  "",
			"resolved": fmt.Sprintf("'%s'", resolvedInterval),
		}

		_, err := newChangefeedCreator(db, fmt.Sprintf("%s.%s", targetDB, targetTable), cmvt.kafka.manager.sinkURL(ctx)).
			With(options).
			Create()
		if err == nil {
			return err
		}

		return nil
	}
}

// runWorkload runs the bank workload in the background.
func (cmvt *cdcMixedVersionTester) runWorkload() func(context.Context, *logger.Logger, *rand.Rand, *mixedversion.Helper) error {
	return func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		bankRun := roachtestutil.NewCommand("%s workload run bank", test.DefaultCockroachPath).
			Flag("max-rate", 10).
			Arg("{pgurl%s}", cmvt.crdbNodes).
			Option("tolerate-errors")

		// Sanity check that the workload has been initlized.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cmvt.workloadInit:
		}

		h.BackgroundCommand(bankRun.String(), option.NodeListOption{h.RandomNode(r, cmvt.crdbNodes)})
		return nil
	}
}

// initWorkload synchronously initializes the workload.
func (cmvt *cdcMixedVersionTester) initWorkload() func(context.Context, *logger.Logger, *rand.Rand, *mixedversion.Helper) error {
	return func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		bankInit := roachtestutil.NewCommand("%s workload init bank", test.DefaultCockroachPath).
			Arg("{pgurl:1}")

		// NB: We add 1 because option.NodeListOptions are indexed starting from 1.
		if err := h.Command(bankInit.String(), option.NodeListOption{h.RandomNode(r, cmvt.crdbNodes) + 1}); err != nil {
			return err
		}
		close(cmvt.workloadInit)
		return nil
	}
}

func runCDCMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster) {
	tester := newCDCMixedVersionTester(ctx, t, c)

	// NB: We rely on the testing framework to choose a random predecessor
	// to upgrade from.
	mvt := mixedversion.NewTest(ctx, t, t.L(), c, tester.crdbNodes)

	cleanupKafka := tester.StartKafka(t, c)
	defer cleanupKafka()

	// Register hooks.
	mvt.OnStartup("start-changefeed", tester.createChangeFeed())
	mvt.OnStartup("create-validator", tester.setupValidator())
	mvt.OnStartup("init-workload", tester.initWorkload())

	// mvt should cancel its context when it is finished running, but we still call
	// the stop functions for safety.
	stopWorkload := mvt.BackgroundFunc("run-workload", tester.runWorkload())
	defer stopWorkload()
	stopKafkaConsumer := mvt.BackgroundFunc("run-kafka-consumer", tester.runKafkaConsumer())
	defer stopKafkaConsumer()
	// NB: mvt.InMixedVersion will run these hooks multiple times at various points during the rolling upgrade, but
	// not when any nodes are offline.
	mvt.InMixedVersion("wait-for-resolve-timestamps", tester.waitForResolvedTimestamps())
	mvt.InMixedVersion("validate-messages", tester.validate())

	mvt.AfterUpgradeFinalized("wait-for-resolve-timestamps", tester.waitForResolvedTimestamps())
	mvt.AfterUpgradeFinalized("validate-messages", tester.validate())

	mvt.Run()
}
