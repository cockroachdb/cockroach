// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	kafkaBufferMessageSize = 1 << 16 // 64 KiB
)

var (
	// the CDC target, DB and table. We're running the bank workload in
	// this test.
	targetDB    = "bank"
	targetTable = "bank"

	// teamcityAgentZone is the zone used in this test. Since this test
	// runs a lot of queries from the TeamCity agent to CRDB nodes, we
	// make sure to create roachprod nodes that are in the same region
	// as the TeamCity agents. Note that a change in the TeamCity agent
	// region could lead to this test timing out (see #92649 for an
	// occurrence of this).
	teamcityAgentZone = "us-east4-b"
)

func registerCDCMixedVersions(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "cdc/mixed-versions",
		Owner:            registry.OwnerCDC,
		Cluster:          r.MakeClusterSpec(5, spec.WorkloadNode(), spec.GCEZones(teamcityAgentZone), spec.Arch(vm.ArchAMD64)),
		Timeout:          3 * time.Hour,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Randomized:       true,
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
	ctx context.Context

	c cluster.Cluster

	crdbNodes     option.NodeListOption
	workloadNodes option.NodeListOption
	kafkaNodes    option.NodeListOption

	// This channel is closed after the workload has been initialized.
	workloadInit chan struct{}

	timestampsResolved struct {
		latest hlc.Timestamp
		syncutil.Mutex
		C chan hlc.Timestamp
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
		mu struct {
			syncutil.Mutex
			buf []*sarama.ConsumerMessage
		}

		consumer *topicConsumer
	}

	validator *cdctest.CountValidator
	fprintV   *cdctest.FingerprintValidator
}

func newCDCMixedVersionTester(ctx context.Context, c cluster.Cluster) cdcMixedVersionTester {
	return cdcMixedVersionTester{
		ctx:           ctx,
		c:             c,
		crdbNodes:     c.CRDBNodes(),
		workloadNodes: c.WorkloadNode(),
		kafkaNodes:    c.WorkloadNode(),
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

	consumer, err := cmvt.kafka.manager.newConsumer(cmvt.ctx, targetTable, nil /* stopper */)
	if err != nil {
		t.Fatal(err)
	}

	cmvt.kafka.manager = manager
	cmvt.kafka.mu.buf = make([]*sarama.ConsumerMessage, 0, kafkaBufferMessageSize)
	cmvt.kafka.consumer = consumer

	return func() {
		consumer.close()
		tearDown()
	}
}

// waitAndValidate waits for a few resolved timestamps to assert
// that the changefeed is running and ensure that there is some data available
// to validate. Then, it returns the result of `cmvt.validate`.
func (cmvt *cdcMixedVersionTester) waitAndValidate(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	l.Printf("waiting for %d resolved timestamps", resolvedTimestampsPerState)
	// create a new channel for the resolved timestamps, allowing any
	// new resolved timestamps to be captured and account for in the
	// loop below
	func() {
		cmvt.timestampsResolved.Lock()
		defer cmvt.timestampsResolved.Unlock()

		cmvt.timestampsResolved.C = make(chan hlc.Timestamp, resolvedTimestampsPerState)
	}()

	var numResolved int
	for numResolved < resolvedTimestampsPerState {
		select {
		case resolvedTs := <-cmvt.timestampsResolved.C:
			if !cmvt.timestampsResolved.latest.Less(resolvedTs) {
				return errors.Newf("expected resolved timestamp %s to be greater than previous "+
					" resolved timestamp %s", resolvedTs, cmvt.timestampsResolved.latest)
			}
			cmvt.timestampsResolved.latest = resolvedTs
			numResolved++
			l.Printf("%d of %d timestamps resolved", numResolved, resolvedTimestampsPerState)
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

	return cmvt.validate(ctx, l, r, h)
}

// setupValidator creates a CDC validator to validate that a changefeed
// created on the target table is able to re-create the table
// somewhere else. It can also verify changefeed ordering guarantees.
func (cmvt *cdcMixedVersionTester) setupValidator(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	tableName := fmt.Sprintf("%s.%s", targetDB, targetTable)
	if err := h.Exec(r, "CREATE TABLE fprint (id INT PRIMARY KEY, balance INT, payload STRING)"); err != nil {
		return err
	}

	// The fingerprint validator will save this db connection and use it
	// when we submit rows for validation. This can be changed later using
	// `(*FingerprintValidator) DBFunc`.
	_, db := h.RandomDB(r)
	fprintV, err := cdctest.NewFingerprintValidator(db, tableName, `fprint`,
		cmvt.kafka.consumer.partitions, 0)
	if err != nil {
		return err
	}
	validators := cdctest.Validators{
		cdctest.NewOrderValidator(tableName),
		fprintV,
	}

	cmvt.validator = cdctest.NewCountValidator(validators)
	cmvt.fprintV = fprintV
	return nil
}

// runKafkaConsumer continuously reads from the kafka consumer and places
// messages in a buffer. It expects to be run in a goroutine and does not
// rely on any DB connections.
func (cmvt *cdcMixedVersionTester) runKafkaConsumer(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	everyN := roachtestutil.Every(30 * time.Second)

	// This runs until the test finishes, which will be signaled via
	// context cancellation. We rely on consumer.Next() to check
	// the context.
	for {
		m, err := cmvt.kafka.consumer.next(ctx)
		if err != nil {
			return err
		}

		// Forward resolved timetsamps to "heartbeat" that the changefeed is running.
		if len(m.Key) <= 0 {
			_, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
			if err != nil {
				return errors.Wrap(err, "failed to parse timestamps from message")
			}
			cmvt.timestampResolved(l, resolved)

			if everyN.ShouldLog() {
				l.Printf("latest resolved timestamp %s behind realtime", timeutil.Since(resolved.GoTime()).String())
			}
		}

		if err := func() error {
			cmvt.kafka.mu.Lock()
			defer cmvt.kafka.mu.Unlock()

			if len(cmvt.kafka.mu.buf) == kafkaBufferMessageSize {
				return errors.Newf("kafka message buffer limit %d exceeded", kafkaBufferMessageSize)
			}
			cmvt.kafka.mu.buf = append(cmvt.kafka.mu.buf, m)
			return nil
		}(); err != nil {
			return err
		}
	}
}

// validate reads rows from the kafka buffer and submits them to the validator
// to be validated.
func (cmvt *cdcMixedVersionTester) validate(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	// Choose a random node to run the validation on.
	n, db := h.RandomDB(r)
	l.Printf("running validation on node %d", n)
	cmvt.fprintV.DBFunc(func(f func(*gosql.DB) error) error {
		return f(db)
	})

	cmvt.kafka.mu.Lock()
	defer cmvt.kafka.mu.Unlock()

	for _, m := range cmvt.kafka.mu.buf {
		updated, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
		if err != nil {
			return errors.Wrap(err, "failed to parse timestamps from message")
		}

		partitionStr := strconv.Itoa(int(m.Partition))
		if len(m.Key) > 0 {
			if err := cmvt.validator.NoteRow(partitionStr, string(m.Key), string(m.Value), updated, m.Topic); err != nil {
				return err
			}
		} else {
			if err := cmvt.validator.NoteResolved(partitionStr, resolved); err != nil {
				return err
			}

			l.Printf("%d resolved timestamps validated", cmvt.validator.NumResolvedWithRows)
		}

		if failures := cmvt.validator.Failures(); len(failures) > 0 {
			return errors.Newf("validator failures:\n%s", strings.Join(failures, "\n"))
		}
	}

	cmvt.kafka.mu.buf = make([]*sarama.ConsumerMessage, 0, kafkaBufferMessageSize)
	return nil
}

// timestampsResolved updates the underlying channel if set (i.e., if
// we are waiting for resolved timestamps events)
func (cmvt *cdcMixedVersionTester) timestampResolved(l *logger.Logger, resolved hlc.Timestamp) {
	cmvt.timestampsResolved.Lock()
	defer cmvt.timestampsResolved.Unlock()

	select {
	case cmvt.timestampsResolved.C <- resolved:
		l.Printf("sent resolved timestamp %s", resolved)
	default:
		// If the channel is full or nil, we drop the resolved timestamp.
	}
}

// createChangeFeed issues a call to the given node to create a change
// feed for the target table.
func (cmvt *cdcMixedVersionTester) createChangeFeed(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	// Wait for workload to be initialized so the target table exists.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-cmvt.workloadInit:
	}
	node, db := h.RandomDB(r)
	systemNode, systemDB := h.System.RandomDB(r)
	l.Printf("starting changefeed on node %d (updating system settings via node %d)", node, systemNode)

	options := map[string]string{
		"updated":  "",
		"resolved": fmt.Sprintf("'%s'", resolvedInterval),
	}

	var ff cdcFeatureFlags
	rangefeedSchedulerSupported, err := cmvt.rangefeedSchedulerSupported(r, h)
	if err != nil {
		return err
	}
	if !rangefeedSchedulerSupported {
		ff.RangeFeedScheduler.v = &featureUnset
	}

	distributionStrategySupported, err := cmvt.distributionStrategySupported(r, h)
	if err != nil {
		return err
	}
	if !distributionStrategySupported {
		ff.DistributionStrategy.v = &featureUnset
	}

	jobID, err := newChangefeedCreator(db, systemDB, l, r, fmt.Sprintf("%s.%s", targetDB, targetTable),
		cmvt.kafka.manager.sinkURL(ctx), ff).
		With(options).
		Create()
	if err != nil {
		return err
	}
	l.Printf("created changefeed job %d", jobID)
	return nil
}

// runWorkloadCmd returns the command that runs the workload.
func (cmvt *cdcMixedVersionTester) runWorkloadCmd(r *rand.Rand) *roachtestutil.Command {
	return roachtestutil.NewCommand("%s workload run bank", test.DefaultCockroachPath).
		// Since all rows are placed in a buffer, setting a low rate of 2 operations / sec
		// helps ensure that we don't exceed the buffer capacity.
		Flag("max-rate", 2).
		Flag("seed", r.Int63()).
		Arg("{pgurl%s}", cmvt.crdbNodes).
		Option("tolerate-errors")
}

// initWorkload synchronously initializes the workload.
func (cmvt *cdcMixedVersionTester) initWorkload(
	ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper,
) error {
	if err := enableTenantSplitScatter(l, r, h); err != nil {
		return err
	}

	bankInit := roachtestutil.NewCommand("%s workload init bank", test.DefaultCockroachPath).
		Flag("seed", r.Int63()).
		Arg("{pgurl%s}", cmvt.crdbNodes)

	initNode := cmvt.workloadNodes[r.Intn(len(cmvt.workloadNodes))]
	if err := cmvt.c.RunE(ctx, option.WithNodes(option.NodeListOption{initNode}), bankInit.String()); err != nil {
		return err
	}
	close(cmvt.workloadInit)
	return nil
}

func (cmvt *cdcMixedVersionTester) muxRangeFeedSupported(
	h *mixedversion.Helper,
) (bool, option.NodeListOption, error) {
	// changefeed.mux_rangefeed.enabled was added in 22.2 and deleted in 24.1.
	return canMixedVersionUseDeletedClusterSetting(h,
		clusterupgrade.MustParseVersion("v22.2.0"),
		clusterupgrade.MustParseVersion("v24.1.0"),
	)
}

const v232CV = "23.2"
const v241CV = "24.1"

func (cmvt *cdcMixedVersionTester) rangefeedSchedulerSupported(
	r *rand.Rand, h *mixedversion.Helper,
) (bool, error) {
	// kv.rangefeed.scheduler.enabled only exists since 23.2.
	return h.ClusterVersionAtLeast(r, v232CV)
}

func (cmvt *cdcMixedVersionTester) distributionStrategySupported(
	r *rand.Rand, h *mixedversion.Helper,
) (bool, error) {
	return h.ClusterVersionAtLeast(r, v241CV)
}

// canMixedVersionUseDeletedClusterSetting returns whether a
// mixed-version cluster can use a deleted (system) cluster
// setting. If it returns true, it will also return the subset of
// nodes that understand the setting.
func canMixedVersionUseDeletedClusterSetting(
	h *mixedversion.Helper,
	addedVersion *clusterupgrade.Version,
	deletedVersion *clusterupgrade.Version,
) (bool, option.NodeListOption, error) {
	fromVersion := h.System.FromVersion
	toVersion := h.System.ToVersion

	// Cluster setting was deleted at or before the from version so no nodes
	// know about the setting.
	if fromVersion.AtLeast(deletedVersion) {
		return false, nil, nil
	}

	// Cluster setting was deleted later than the from version but at or before
	// the to version, so if the from version is at least the added version,
	// all the nodes on that version will know about the setting.
	if toVersion.AtLeast(deletedVersion) {
		if fromVersion.AtLeast(addedVersion) {
			fromVersionNodes := h.System.NodesInPreviousVersion()
			if len(fromVersionNodes) > 0 {
				return true, fromVersionNodes, nil
			}
		}
		return false, nil, nil
	}

	// Cluster setting was deleted later than to version, so any nodes that are
	// at least the added version will know about the setting.

	if fromVersion.AtLeast(addedVersion) {
		return true, h.System.Descriptor.Nodes, nil
	}

	if toVersion.AtLeast(addedVersion) {
		toVersionNodes := h.System.NodesInNextVersion()
		if len(toVersionNodes) > 0 {
			return true, toVersionNodes, nil
		}
		return false, nil, nil
	}

	return false, nil, nil
}

func runCDCMixedVersions(ctx context.Context, t test.Test, c cluster.Cluster) {
	tester := newCDCMixedVersionTester(ctx, c)

	mvt := mixedversion.NewTest(
		ctx, t, t.L(), c, tester.crdbNodes,
		// We set the minimum supported version to 23.2 in this test as it
		// relies on the `kv.rangefeed.enabled` cluster setting. This
		// setting is, confusingly, labeled as `TenantWritable` in
		// 23.1. That mistake was then fixed (#110676) but, to simplify
		// this test, we only create changefeeds in more recent versions.
		mixedversion.MinimumSupportedVersion("v23.2.0"),
		// We limit the total number of plan steps to 80, which is roughly 60% of all plan lengths.
		// See https://github.com/cockroachdb/cockroach/pull/137963#discussion_r1906256740 for more details.
		mixedversion.MaxNumPlanSteps(80),
	)

	cleanupKafka := tester.StartKafka(t, c)
	defer cleanupKafka()

	// MuxRangefeed in various forms is available starting from v22.2.
	setMuxRangeFeedEnabled := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		supported, gatewayNodes, err := tester.muxRangeFeedSupported(h)
		if err != nil {
			return err
		}
		if supported {
			coin := r.Int()%2 == 0
			l.PrintfCtx(ctx, "Setting changefeed.mux_rangefeed.enabled=%t ", coin)
			return h.System.ExecWithGateway(
				r, gatewayNodes, "SET CLUSTER SETTING changefeed.mux_rangefeed.enabled=$1", coin,
			)
		}
		return nil
	}

	// Rangefeed scheduler available in 23.2
	setRangeFeedSchedulerEnabled := func(ctx context.Context, l *logger.Logger, r *rand.Rand, h *mixedversion.Helper) error {
		supported, err := tester.rangefeedSchedulerSupported(r, h)
		if err != nil {
			return err
		}
		l.Printf("kv.rangefeed.scheduler.enabled=%t", supported)
		if supported {
			coin := r.Int()%2 == 0
			l.PrintfCtx(ctx, "Setting kv.rangefeed.scheduler.enabled=%t", coin)
			return h.System.Exec(r, "SET CLUSTER SETTING kv.rangefeed.scheduler.enabled=$1", coin)
		}
		return nil
	}

	// Register hooks.
	mvt.OnStartup("start changefeed", tester.createChangeFeed)
	mvt.OnStartup("create validator", tester.setupValidator)
	mvt.OnStartup("init workload", tester.initWorkload)

	runWorkloadCmd := tester.runWorkloadCmd(mvt.RNG())
	_ = mvt.BackgroundCommand("run workload", tester.workloadNodes, runWorkloadCmd)
	_ = mvt.BackgroundFunc("run kafka consumer", tester.runKafkaConsumer)

	// NB: mvt.InMixedVersion will run these hooks multiple times at various points during the rolling upgrade, but
	// not when any nodes are offline. This is important because the validator relies on a db connection.
	mvt.InMixedVersion("wait and validate", tester.waitAndValidate)

	// Enable/disable mux rangefeed related settings in mixed version.
	mvt.InMixedVersion("use mux", setMuxRangeFeedEnabled)
	mvt.InMixedVersion("use scheduler", setRangeFeedSchedulerEnabled)

	mvt.AfterUpgradeFinalized("use mux", setMuxRangeFeedEnabled)
	mvt.AfterUpgradeFinalized("use scheduler", setRangeFeedSchedulerEnabled)
	mvt.AfterUpgradeFinalized("wait and validate", tester.waitAndValidate)
	mvt.Run()
}
