// Copyright 2018 The Cockroach Authors.
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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	gosql "database/sql"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// kafkaCreateTopicRetryDuration is the retry duration we use while
// trying to create a Kafka topic after setting it up on a
// node. Without retrying, a `kafka controller not available` error is
// seen with a 1-5% probability
var kafkaCreateTopicRetryDuration = 1 * time.Minute

type sinkType string

const (
	cloudStorageSink sinkType = "cloudstorage"
	webhookSink      sinkType = "webhook"
	pubsubSink       sinkType = "pubsub"
	kafkaSink        sinkType = "kafka"
	nullSink         sinkType = "null"
)

type cdcTester struct {
	ctx          context.Context
	t            test.Test
	mon          cluster.Monitor
	cluster      cluster.Cluster
	crdbNodes    option.NodeListOption
	workloadNode option.NodeListOption
	logger       *logger.Logger
	promCfg      *prometheus.Config

	// sinkType -> sinkURI
	sinkCache map[sinkType]string

	workloadWg *sync.WaitGroup
	doneCh     chan struct{}
}

// startStatsCollection sets the start point of the stats collection window
// and returns a function which should be called at the end of the test to dump a
// stats.json file to the artifacts directory.
func (ct *cdcTester) startStatsCollection() func() {
	if ct.promCfg == nil {
		ct.t.Error("prometheus configuration is nil")
	}
	promClient, err := clusterstats.SetupCollectorPromClient(ct.ctx, ct.cluster, ct.t.L(), ct.promCfg)
	if err != nil {
		ct.t.Errorf("error creating prometheus client for stats collector: %s", err)
	}

	statsCollector := clusterstats.NewStatsCollector(ct.ctx, promClient)
	startTime := timeutil.Now()
	return func() {
		endTime := timeutil.Now()
		_, err := statsCollector.Exporter().Export(ct.ctx, ct.cluster, ct.t, false, /* dryRun */
			startTime,
			endTime,
			[]clusterstats.AggQuery{sqlServiceLatencyAgg, changefeedThroughputAgg, cpuUsageAgg},
			func(stats map[string]clusterstats.StatSummary) (string, float64) {
				// TODO(jayant): update this metric to be more accurate.
				// It may be worth plugging in real latency values from the latency
				// verifier here in the future for more accuracy. However, it may not be
				// worth the added complexity. Since latency verifier failures will show
				// up as roachtest failures, we don't need to make them very apparent in
				// roachperf. Note that other roachperf stats, such as the aggregate stats
				// above, will be accurate.
				return "Total Run Time (mins)", endTime.Sub(startTime).Minutes()
			},
		)
		if err != nil {
			ct.t.Errorf("error exporting stats file: %s", err)
		}
	}
}

func (ct *cdcTester) startCRDBChaos() {
	chaosStopper := make(chan time.Time)
	ct.mon.Go(func(ctx context.Context) error {
		select {
		case <-ct.doneCh:
		case <-ctx.Done():
		}
		chaosStopper <- timeutil.Now()
		return nil
	})
	ch := Chaos{
		Timer:   Periodic{Period: 2 * time.Minute, DownTime: 20 * time.Second},
		Target:  ct.crdbNodes.RandNode,
		Stopper: chaosStopper,
	}
	ct.mon.Go(ch.Runner(ct.cluster, ct.t, ct.mon))
}

func (ct *cdcTester) setupSink(args feedArgs) string {
	var sinkURI string
	switch args.sinkType {
	case nullSink:
		sinkURI = "null://"
	case cloudStorageSink:
		ts := timeutil.Now().Format(`20060102150405`)
		// cockroach-tmp is a multi-region bucket with a TTL to clean up old
		// data.
		sinkURI = `experimental-gs://cockroach-tmp/roachtest/` + ts + "?AUTH=implicit"
	case webhookSink:
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		if err != nil {
			ct.t.Fatal(err)
		}
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		if err != nil {
			ct.t.Fatal(err)
		}

		sinkDestHost, err := url.Parse(sinkDest.URL())
		if err != nil {
			ct.t.Fatal(err)
		}

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		sinkURI = fmt.Sprintf("webhook-%s", sinkDestHost.String())
	case pubsubSink:
		sinkURI = changefeedccl.GcpScheme + `://cockroach-ephemeral` + "?AUTH=implicit&topic_name=pubsubSink-roachtest&region=us-east1"
	case kafkaSink:
		kafkaNode := ct.cluster.Node(ct.cluster.Spec().NodeCount)
		kafka := kafkaManager{
			t:     ct.t,
			c:     ct.cluster,
			nodes: kafkaNode,
		}
		kafka.install(ct.ctx)
		kafka.start(ct.ctx)

		if args.kafkaChaos {
			ct.mon.Go(func(ctx context.Context) error {
				period, downTime := 2*time.Minute, 20*time.Second
				return kafka.chaosLoop(ctx, period, downTime, ct.doneCh)
			})
		}

		sinkURI = kafka.sinkURL(ct.ctx)
	default:
		ct.t.Fatalf("unknown sink provided: %s", args.sinkType)
	}

	if args.assumeRole != "" {
		sinkURI = sinkURI + "&ASSUME_ROLE=" + args.assumeRole
	}

	return sinkURI
}

type tpccArgs struct {
	warehouses     int
	duration       string
	tolerateErrors bool
}

func (ct *cdcTester) runTPCCWorkload(args tpccArgs) {
	tpcc := tpccWorkload{
		sqlNodes:           ct.crdbNodes,
		workloadNodes:      ct.workloadNode,
		tpccWarehouseCount: args.warehouses,
		// TolerateErrors if crdbChaos is true; otherwise, the workload will fail
		// if it attempts to use the node which was brought down by chaos.
		tolerateErrors: args.tolerateErrors,
	}

	if !ct.t.SkipInit() {
		ct.t.Status("installing TPCC workload")
		tpcc.install(ct.ctx, ct.cluster)
	} else {
		ct.t.Status("skipping TPCC installation")
	}

	if args.duration != "" {
		// TODO(dan,ajwerner): sleeping momentarily before running the workload
		// mitigates errors like "error in newOrder: missing stock row" from tpcc.
		time.Sleep(2 * time.Second)
		ct.t.Status("initiating TPCC workload")
		ct.mon.Go(func(ctx context.Context) error {
			ct.workloadWg.Add(1)
			defer ct.workloadWg.Done()
			tpcc.run(ctx, ct.cluster, args.duration)
			return nil
		})
	} else {
		ct.t.Status("skipping TPCC run")
	}
}

type ledgerArgs struct {
	duration string
}

func (ct *cdcTester) runLedgerWorkload(args ledgerArgs) {
	lw := ledgerWorkload{
		sqlNodes:      ct.crdbNodes,
		workloadNodes: ct.workloadNode,
	}
	if !ct.t.SkipInit() {
		ct.t.Status("installing Ledger workload")
		lw.install(ct.ctx, ct.cluster)
	} else {
		ct.t.Status("skipping Ledger installation")
	}

	ct.t.Status("initiating Ledger workload")
	ct.mon.Go(func(ctx context.Context) error {
		ct.workloadWg.Add(1)
		defer ct.workloadWg.Done()
		lw.run(ctx, ct.cluster, args.duration)
		return nil
	})
}

func (ct *cdcTester) DB() *gosql.DB {
	return ct.cluster.Conn(ct.ctx, ct.t.L(), 1)
}

func (ct *cdcTester) Close() {
	ct.t.Status("cdcTester closing")
	close(ct.doneCh)
	ct.mon.Wait()

	_, _ = ct.DB().Exec(`CANCEL ALL CHANGEFEED JOBS;`)

	if !ct.t.IsDebug() {
		if err := ct.cluster.StopGrafana(ct.ctx, ct.logger, ct.t.ArtifactsDir()); err != nil {
			ct.t.Errorf("error shutting down prometheus/grafana: %s", err)
		}
	}

	ct.logger.Close()
	ct.t.Status("cdcTester closed")
}

type changefeedJob struct {
	ctx            context.Context
	sinkURI        string
	jobID          int
	targets        []string
	opts           map[string]string
	logger         *logger.Logger
	db             *gosql.DB
	tolerateErrors bool
}

func (cj *changefeedJob) Label() string {
	if label, ok := cj.opts["metrics_label"]; ok {
		return label
	}
	return "default"
}

var allTpccTargets []string = []string{
	`tpcc.warehouse`,
	`tpcc.district`,
	`tpcc.customer`,
	`tpcc.history`,
	`tpcc.order`,
	`tpcc.new_order`,
	`tpcc.item`,
	`tpcc.stock`,
	`tpcc.order_line`,
}

var allLedgerTargets []string = []string{
	`ledger.customer`,
	`ledger.transaction`,
	`ledger.entry`,
	`ledger.session`,
}

type feedArgs struct {
	sinkType       sinkType
	targets        []string
	opts           map[string]string
	kafkaChaos     bool
	assumeRole     string
	tolerateErrors bool
}

// TODO: Maybe move away from feedArgs since its only 3 things
func (ct *cdcTester) newChangefeed(args feedArgs) changefeedJob {
	ct.t.Status(fmt.Sprintf("initiating %s sink", args.sinkType))
	sinkURI := ct.setupSink(args)
	ct.t.Status(fmt.Sprintf("using sinkURI %s", sinkURI))

	targetsStr := strings.Join(args.targets, ", ")

	feedOptions := make(map[string]string)
	feedOptions["min_checkpoint_frequency"] = "'10s'"
	if args.sinkType == cloudStorageSink || args.sinkType == webhookSink {
		// Webhook and cloudstorage don't have a concept of keys and therefore
		// require envelope=wrapped
		feedOptions["envelope"] = "wrapped"

		feedOptions["resolved"] = "'10s'"

	} else {
		feedOptions["resolved"] = ""
	}

	for option, value := range args.opts {
		feedOptions[option] = value
		if option == "initial_scan_only" || (option == "initial_scan" && value == "'only'") {
			delete(feedOptions, "resolved")
		}
	}

	ct.t.Status(fmt.Sprintf(
		"creating %s changefeed into targets %s with options (%+v)",
		args.sinkType, args.targets, feedOptions,
	))
	db := ct.DB()
	jobID, err := newChangefeedCreator(db, targetsStr, sinkURI).
		With(feedOptions).Create()
	if err != nil {
		ct.t.Fatalf("failed to create changefeed: %s", err.Error())
	}

	cj := changefeedJob{
		ctx:            ct.ctx,
		sinkURI:        sinkURI,
		jobID:          jobID,
		targets:        args.targets,
		opts:           feedOptions,
		db:             db,
		tolerateErrors: args.tolerateErrors,
	}

	ct.t.Status(fmt.Sprintf("created changefeed %s with jobID %d", cj.Label(), jobID))

	return cj
}

func (ct *cdcTester) runFeedLatencyVerifier(cj changefeedJob, targets latencyTargets) {
	info, err := getChangefeedInfo(ct.DB(), cj.jobID)
	if err != nil {
		ct.t.Fatalf("failed to get changefeed info: %s", err.Error())
	}

	verifier := makeLatencyVerifier(
		fmt.Sprintf("changefeed[%s]", cj.Label()),
		targets.initialScanLatency,
		targets.steadyLatency,
		ct.logger,
		func(db *gosql.DB, jobID int) (jobInfo, error) { return getChangefeedInfo(db, jobID) },
		ct.t.Status,
		cj.tolerateErrors,
	)
	verifier.statementTime = info.statementTime

	ct.mon.Go(func(ctx context.Context) error {
		err := verifier.pollLatency(ctx, ct.DB(), cj.jobID, time.Second, ct.doneCh)
		if err != nil {
			return err
		}

		verifier.assertValid(ct.t)
		verifier.maybeLogLatencyHist()
		return nil
	})
}

func (cj *changefeedJob) runFeedPoller(
	ctx context.Context,
	pollInterval time.Duration,
	stopper chan struct{},
	onInfo func(info *changefeedInfo),
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stopper:
			return nil
		case <-time.After(pollInterval):
		}

		info, err := getChangefeedInfo(cj.db, cj.jobID)
		if err != nil {
			if cj.tolerateErrors {
				cj.logger.Printf("error getting changefeed info: %s", err)
				continue
			}
			return err
		}

		onInfo(info)
	}
}

func (ct *cdcTester) waitForWorkload() {
	ct.workloadWg.Wait()
}

func (cj *changefeedJob) waitForCompletion() {
	completionCh := make(chan struct{})
	err := cj.runFeedPoller(cj.ctx, time.Second, completionCh, func(info *changefeedInfo) {
		if info.status == "succeeded" || info.status == "failed" {
			close(completionCh)
		}
	})
	if err != nil {
		cj.logger.Printf("completion poller error: %s", err)
	}
}

func newCDCTester(ctx context.Context, t test.Test, c cluster.Cluster) cdcTester {
	tester := cdcTester{
		ctx:          ctx,
		t:            t,
		cluster:      c,
		crdbNodes:    c.Range(1, c.Spec().NodeCount-1),
		workloadNode: c.Node(c.Spec().NodeCount),
		doneCh:       make(chan struct{}),
		sinkCache:    make(map[sinkType]string),
		workloadWg:   &sync.WaitGroup{},
	}
	tester.mon = c.NewMonitor(ctx, tester.crdbNodes)

	changefeedLogger, err := t.L().ChildLogger("changefeed")
	if err != nil {
		t.Fatal(err)
	}
	tester.logger = changefeedLogger

	c.Put(ctx, t.Cockroach(), "./cockroach")

	settings := install.MakeClusterSettings()
	settings.Env = append(settings.Env, "COCKROACH_CHANGEFEED_TESTING_FAST_RETRY=true")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, tester.crdbNodes)

	c.Put(ctx, t.DeprecatedWorkload(), "./workload", tester.workloadNode)

	db := tester.DB()
	// With a target_duration of 10s, we won't see slow span logs from changefeeds untils we are > 100s
	// behind, which is well above the 60s targetSteadyLatency we have in some tests.
	if _, err := db.Exec(
		`SET CLUSTER SETTING changefeed.slow_span_log_threshold='30s'`,
	); err != nil {
		// We don't hard fail here because, not all versions support this setting
		t.L().Printf("failed to set cluster setting: %s", err)
	}
	if _, err := db.Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
		t.L().Printf("failed to set cluster setting: %s", err)
	}
	if _, err := db.Exec("SET CLUSTER SETTING server.child_metrics.enabled = true"); err != nil {
		t.L().Printf("failed to set cluster setting: %s", err)
	}

	tester.startGrafana()
	return tester
}

func (ct *cdcTester) startGrafana() {
	cfg := (&prometheus.Config{}).
		WithPrometheusNode(ct.workloadNode.InstallNodes()[0]).
		WithCluster(ct.crdbNodes.InstallNodes()).
		WithNodeExporter(ct.crdbNodes.InstallNodes()).
		WithGrafanaDashboard("https://go.crdb.dev/p/changefeed-roachtest-grafana-dashboard")
	cfg.Grafana.Enabled = true

	ct.promCfg = cfg

	if !ct.t.SkipInit() {
		err := ct.cluster.StartGrafana(ct.ctx, ct.t.L(), cfg)
		if err != nil {
			ct.t.Errorf("error starting prometheus/grafana: %s", err)
		}
		nodeURLs, err := ct.cluster.ExternalIP(ct.ctx, ct.t.L(), ct.workloadNode)
		if err != nil {
			ct.t.Errorf("error getting grafana node external ip: %s", err)
		}
		ct.t.Status(fmt.Sprintf("started grafana at http://%s:3000/d/928XNlN4k/basic?from=now-15m&to=now", nodeURLs[0]))
	} else {
		ct.t.Status("skipping grafana installation")
	}
}

type latencyTargets struct {
	initialScanLatency time.Duration
	steadyLatency      time.Duration
}

func runCDCBank(ctx context.Context, t test.Test, c cluster.Cluster) {
	if runtime.GOARCH == "arm64" {
		t.Skip("Skipping cdc/bank under ARM64.")
	}
	// Make the logs dir on every node to work around the `roachprod get logs`
	// spam.
	c.Run(ctx, c.All(), `mkdir -p logs`)

	crdbNodes, workloadNode, kafkaNode := c.Range(1, c.Spec().NodeCount-1), c.Node(c.Spec().NodeCount), c.Node(c.Spec().NodeCount)
	c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)
	startOpts := option.DefaultStartOpts()
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
		"--vmodule=changefeed=2",
	)
	c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), crdbNodes)

	kafka, cleanup := setupKafka(ctx, t, c, kafkaNode)
	defer cleanup()

	t.Status("creating kafka topic")
	if err := kafka.createTopic(ctx, "bank"); err != nil {
		t.Fatal(err)
	}

	c.Run(ctx, workloadNode, `./workload init bank {pgurl:1}`)
	db := c.Conn(ctx, t.L(), 1)
	defer stopFeeds(db)

	options := map[string]string{
		"updated":  "",
		"resolved": "",
		// we need to set a min_checkpoint_frequency here because if we
		// use the default 30s duration, the test will likely not be able
		// to finish within 30 minutes
		"min_checkpoint_frequency": "'2s'",
		"diff":                     "",
	}
	_, err := newChangefeedCreator(db, "bank.bank", kafka.sinkURL(ctx)).
		With(options).
		Create()
	if err != nil {
		t.Fatal(err)
	}

	tc, err := kafka.consumer(ctx, "bank")
	if err != nil {
		t.Fatal(errors.Wrap(err, "could not create kafka consumer"))
	}
	defer tc.Close()

	l, err := t.L().ChildLogger(`changefeed`)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	t.Status("running workload")
	workloadCtx, workloadCancel := context.WithCancel(ctx)
	defer workloadCancel()

	m := c.NewMonitor(workloadCtx, crdbNodes)
	var doneAtomic int64
	messageBuf := make(chan *sarama.ConsumerMessage, 4096)
	const requestedResolved = 100

	m.Go(func(ctx context.Context) error {
		err := c.RunE(ctx, workloadNode, `./workload run bank {pgurl:1} --max-rate=10`)
		if atomic.LoadInt64(&doneAtomic) > 0 {
			return nil
		}
		return errors.Wrap(err, "workload failed")
	})
	m.Go(func(ctx context.Context) error {
		defer workloadCancel()
		defer func() { close(messageBuf) }()
		v := cdctest.MakeCountValidator(cdctest.NoOpValidator)
		for {
			m := tc.Next(ctx)
			if m == nil {
				return fmt.Errorf("unexpected end of changefeed")
			}
			messageBuf <- m
			updated, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
			if err != nil {
				return err
			}

			partitionStr := strconv.Itoa(int(m.Partition))
			if len(m.Key) > 0 {
				if err := v.NoteRow(partitionStr, string(m.Key), string(m.Value), updated); err != nil {
					return err
				}
			} else {
				if err := v.NoteResolved(partitionStr, resolved); err != nil {
					return err
				}
				l.Printf("%d of %d resolved timestamps received from kafka, latest is %s behind realtime, %s beind realtime when sent to kafka",
					v.NumResolvedWithRows, requestedResolved, timeutil.Since(resolved.GoTime()), m.Timestamp.Sub(resolved.GoTime()))
				if v.NumResolvedWithRows >= requestedResolved {
					atomic.StoreInt64(&doneAtomic, 1)
					break
				}
			}
		}
		return nil
	})
	m.Go(func(context.Context) error {
		if _, err := db.Exec(
			`CREATE TABLE fprint (id INT PRIMARY KEY, balance INT, payload STRING)`,
		); err != nil {
			return errors.Wrap(err, "CREATE TABLE failed")
		}

		fprintV, err := cdctest.NewFingerprintValidator(db, `bank.bank`, `fprint`, tc.partitions, 0)
		if err != nil {
			return errors.Wrap(err, "error creating validator")
		}
		baV, err := cdctest.NewBeforeAfterValidator(db, `bank.bank`)
		if err != nil {
			return err
		}
		validators := cdctest.Validators{
			cdctest.NewOrderValidator(`bank`),
			fprintV,
			baV,
		}
		v := cdctest.MakeCountValidator(validators)

		timeSpentValidatingRows := 0 * time.Second
		numRowsValidated := 0

		for {
			m, ok := <-messageBuf
			if !ok {
				break
			}
			updated, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
			if err != nil {
				return err
			}

			partitionStr := strconv.Itoa(int(m.Partition))
			if len(m.Key) > 0 {
				startTime := timeutil.Now()
				if err := v.NoteRow(partitionStr, string(m.Key), string(m.Value), updated); err != nil {
					return err
				}
				timeSpentValidatingRows += timeutil.Since(startTime)
				numRowsValidated++
			} else {
				noteResolvedStartTime := timeutil.Now()
				if err := v.NoteResolved(partitionStr, resolved); err != nil {
					return err
				}
				l.Printf("%d of %d resolved timestamps validated, latest is %s behind realtime",
					v.NumResolvedWithRows, requestedResolved, timeutil.Since(resolved.GoTime()))

				l.Printf("%s was spent validating this resolved timestamp: %s", timeutil.Since(noteResolvedStartTime))
				l.Printf("%s was spent validating %d rows", timeSpentValidatingRows, numRowsValidated)

				numRowsValidated = 0
				timeSpentValidatingRows = 0
			}
		}
		if failures := v.Failures(); len(failures) > 0 {
			return errors.Newf("validator failures:\n%s", strings.Join(failures, "\n"))
		}
		return nil
	})
	m.Wait()
}

// This test verifies that the changefeed avro + confluent schema registry works
// end-to-end (including the schema registry default of requiring backward
// compatibility within a topic).
func runCDCSchemaRegistry(ctx context.Context, t test.Test, c cluster.Cluster) {
	crdbNodes, kafkaNode := c.Node(1), c.Node(1)
	c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), crdbNodes)
	kafka := kafkaManager{
		t:     t,
		c:     c,
		nodes: kafkaNode,
	}
	kafka.install(ctx)
	kafka.start(ctx)
	defer kafka.stop(ctx)

	db := c.Conn(ctx, t.L(), 1)
	defer stopFeeds(db)

	if _, err := db.Exec(`CREATE TABLE foo (a INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	options := map[string]string{
		"updated":                   "",
		"resolved":                  "",
		"format":                    "experimental_avro",
		"confluent_schema_registry": "$2",
		"diff":                      "",
	}

	_, err := newChangefeedCreator(db, "foo", kafka.sinkURL(ctx)).
		With(options).
		Args(kafka.schemaRegistryURL(ctx)).
		Create()
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range []string{
		`INSERT INTO foo VALUES (1)`,
		`ALTER TABLE foo ADD COLUMN b STRING`,
		`INSERT INTO foo VALUES (2, '2')`,
		`ALTER TABLE foo ADD COLUMN c INT`,
		`INSERT INTO foo VALUES (3, '3', 3)`,
		`ALTER TABLE foo DROP COLUMN b`,
		`INSERT INTO foo VALUES (4, 4)`,
	} {
		t.L().Printf("Executing SQL: %s", stmt)
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("failed to execute %s: %v", stmt, err)
		}
	}

	// There are various internal races and retries in changefeeds that can
	// produce duplicates. This test is really only to verify that the confluent
	// schema registry works end-to-end, so do the simplest thing and sort +
	// unique the output, pulling more messages if we get a lot of duplicates
	// or resolved messages.
	updatedRE := regexp.MustCompile(`"updated":\{"string":"[^"]+"\}`)
	updatedMap := make(map[string]struct{})
	var resolved []string
	pagesFetched := 0
	pageSize := 7

	for len(updatedMap) < 10 && pagesFetched < 5 {
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), kafkaNode,
			kafka.makeCommand("kafka-avro-console-consumer",
				fmt.Sprintf("--offset=%d", pagesFetched*pageSize),
				"--partition=0",
				"--topic=foo",
				fmt.Sprintf("--max-messages=%d", pageSize),
				"--bootstrap-server=localhost:9092"))
		t.L().Printf("\n%s\n", result.Stdout+result.Stderr)
		if err != nil {
			t.Fatal(err)
		}
		pagesFetched++

		for _, line := range strings.Split(result.Stdout, "\n") {
			if strings.Contains(line, `"updated"`) {
				line = updatedRE.ReplaceAllString(line, `"updated":{"string":""}`)
				updatedMap[line] = struct{}{}
			} else if strings.Contains(line, `"resolved"`) {
				resolved = append(resolved, line)
			}
		}
	}

	updated := make([]string, 0, len(updatedMap))
	for u := range updatedMap {
		updated = append(updated, u)
	}
	sort.Strings(updated)

	expected := []string{
		`{"before":null,"after":{"foo":{"a":{"long":1}}},"updated":{"string":""}}`,
		`{"before":null,"after":{"foo":{"a":{"long":2},"b":{"string":"2"}}},"updated":{"string":""}}`,
		`{"before":null,"after":{"foo":{"a":{"long":3},"b":{"string":"3"},"c":{"long":3}}},"updated":{"string":""}}`,
		`{"before":null,"after":{"foo":{"a":{"long":4},"c":{"long":4}}},"updated":{"string":""}}`,
		`{"before":{"foo_before":{"a":{"long":1},"b":null,"c":null}},"after":{"foo":{"a":{"long":1},"c":null}},"updated":{"string":""}}`,
		`{"before":{"foo_before":{"a":{"long":2},"b":{"string":"2"},"c":null}},"after":{"foo":{"a":{"long":2},"c":null}},"updated":{"string":""}}`,
		`{"before":{"foo_before":{"a":{"long":3},"b":{"string":"3"},"c":{"long":3}}},"after":{"foo":{"a":{"long":3},"c":{"long":3}}},"updated":{"string":""}}`,
	}
	if strings.Join(expected, "\n") != strings.Join(updated, "\n") {
		t.Fatalf("expected\n%s\n\ngot\n%s\n\n",
			strings.Join(expected, "\n"), strings.Join(updated, "\n"))
	}

	if len(resolved) == 0 {
		t.Fatal(`expected at least 1 resolved timestamp`)
	}
}

func runCDCKafkaAuth(ctx context.Context, t test.Test, c cluster.Cluster) {
	lastCrdbNode := c.Spec().NodeCount - 1
	if lastCrdbNode == 0 {
		lastCrdbNode = 1
	}

	crdbNodes, kafkaNode := c.Range(1, lastCrdbNode), c.Node(c.Spec().NodeCount)
	c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), crdbNodes)

	kafka := kafkaManager{
		t:     t,
		c:     c,
		nodes: kafkaNode,
	}
	kafka.install(ctx)
	testCerts := kafka.configureAuth(ctx)
	kafka.start(ctx, "kafka")
	kafka.addSCRAMUsers(ctx)
	defer kafka.stop(ctx)

	db := c.Conn(ctx, t.L(), 1)
	defer stopFeeds(db)

	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, `CREATE TABLE auth_test_table (a INT PRIMARY KEY)`)

	caCert := testCerts.CACertBase64()
	saslURL := kafka.sinkURLSASL(ctx)
	feeds := []struct {
		desc     string
		queryArg string
	}{
		{
			"create changefeed with insecure TLS transport and no auth",
			fmt.Sprintf("%s?tls_enabled=true&insecure_tls_skip_verify=true", kafka.sinkURLTLS(ctx)),
		},
		{
			"create changefeed with TLS transport and no auth",
			fmt.Sprintf("%s?tls_enabled=true&ca_cert=%s", kafka.sinkURLTLS(ctx), testCerts.CACertBase64()),
		},
		{
			"create changefeed with TLS transport and SASL/PLAIN (default mechanism)",
			fmt.Sprintf("%s?tls_enabled=true&ca_cert=%s&sasl_enabled=true&sasl_user=plain&sasl_password=plain-secret", saslURL, caCert),
		},
		{
			"create changefeed with TLS transport and SASL/PLAIN (explicit mechanism)",
			fmt.Sprintf("%s?tls_enabled=true&ca_cert=%s&sasl_enabled=true&sasl_user=plain&sasl_password=plain-secret&sasl_mechanism=PLAIN", saslURL, caCert),
		},
		{
			"create changefeed with TLS transport and SASL/SCRAM-SHA-256",
			fmt.Sprintf("%s?tls_enabled=true&ca_cert=%s&sasl_enabled=true&sasl_user=scram256&sasl_password=scram256-secret&sasl_mechanism=SCRAM-SHA-256", saslURL, caCert),
		},
		{
			"create changefeed with TLS transport and SASL/SCRAM-SHA-512",
			fmt.Sprintf("%s?tls_enabled=true&ca_cert=%s&sasl_enabled=true&sasl_user=scram512&sasl_password=scram512-secret&sasl_mechanism=SCRAM-SHA-512", saslURL, caCert),
		},
	}

	for _, f := range feeds {
		t.Status(f.desc)
		_, err := newChangefeedCreator(db, "auth_test_table", f.queryArg).Create()
		if err != nil {
			t.Fatalf("%s: %s", f.desc, err.Error())
		}
	}
}

func registerCDC(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "cdc/initial-scan-only",
		Owner:           registry.OwnerCDC,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 100})

			exportStatsFile := ct.startStatsCollection()
			feed := ct.newChangefeed(feedArgs{
				sinkType: cloudStorageSink,
				targets:  allTpccTargets,
				opts:     map[string]string{"initial_scan": "'only'"},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
			})
			feed.waitForCompletion()
			exportStatsFile()
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/tpcc-1000",
		Owner:           registry.OwnerCDC,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 1000, duration: "120m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType: kafkaSink,
				targets:  allTpccTargets,
				opts:     map[string]string{"initial_scan": "'no'"},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 3 * time.Minute,
				steadyLatency:      10 * time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/tpcc-1000/sink=null",
		Owner:           registry.OwnerCDC,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		Tags:            []string{"manual"},
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 1000, duration: "120m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType: nullSink,
				targets:  allTpccTargets,
				opts:     map[string]string{"initial_scan": "'no'"},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 3 * time.Minute,
				steadyLatency:      10 * time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/initial-scan",
		Owner:           registry.OwnerCDC,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 100, duration: "30m"})

			feed := ct.newChangefeed(feedArgs{sinkType: nullSink, targets: allTpccTargets})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
				steadyLatency:      time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/sink-chaos",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 100, duration: "30m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType:   kafkaSink,
				targets:    allTpccTargets,
				kafkaChaos: true,
				opts:       map[string]string{"initial_scan": "'no'"},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 3 * time.Minute,
				steadyLatency:      5 * time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/crdb-chaos",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.startCRDBChaos()

			ct.runTPCCWorkload(tpccArgs{warehouses: 100, duration: "30m", tolerateErrors: true})

			feed := ct.newChangefeed(feedArgs{
				sinkType:       kafkaSink,
				targets:        allTpccTargets,
				opts:           map[string]string{"initial_scan": "'no'"},
				tolerateErrors: true,
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 3 * time.Minute,
				steadyLatency:      5 * time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:  "cdc/ledger",
		Owner: `cdc`,
		// TODO(mrtracy): This workload is designed to be running on a 20CPU nodes,
		// but this cannot be allocated without some sort of configuration outside
		// of this test. Look into it.
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			workloadStart := timeutil.Now()
			ct.runLedgerWorkload(ledgerArgs{duration: "28m"})

			alterStmt := "ALTER DATABASE ledger CONFIGURE ZONE USING range_max_bytes = 805306368, range_min_bytes = 134217728"
			_, err := ct.DB().ExecContext(ctx, alterStmt)
			if err != nil {
				t.Fatalf("failed statement %q: %s", alterStmt, err.Error())
			}

			feed := ct.newChangefeed(feedArgs{sinkType: kafkaSink, targets: allLedgerTargets})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 10 * time.Minute,
				steadyLatency:      time.Minute,
			})
			ct.waitForWorkload()

			workloadEnd := timeutil.Now()
			verifyTxnPerSecond(
				ctx, c, t, ct.crdbNodes.RandNode(), workloadStart, workloadEnd, 575, 0.05,
			)
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/cloud-sink-gcs/rangefeed=true",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			// Sending data to Google Cloud Storage is a bit slower than sending to
			// Kafka on an adjacent machine, so use half the data of the
			// initial-scan test. Consider adding a test that writes to nodelocal,
			// which should be much faster, with a larger warehouse count.
			ct.runTPCCWorkload(tpccArgs{warehouses: 50, duration: "30m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType: cloudStorageSink,
				targets:  allTpccTargets,
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
				steadyLatency:      time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/pubsub-sink",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 1, duration: "30m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType: pubsubSink,
				targets:  allTpccTargets,
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
				steadyLatency:      time.Minute,
			})
			ct.waitForWorkload()
		},
	})

	// In order to run this test, the service account corresponding to the
	// implicit credentials must have the Service Account Token Creator role on
	// the first account on the assume-role chain:
	// cdc-roachtest-intermediate@cockroach-ephemeral.iam.gserviceaccount.com. See
	// https://cloud.google.com/iam/docs/create-short-lived-credentials-direct.
	//
	// TODO(rui): Change to a shorter test as it just needs to validate
	// permissions and shouldn't need to run a full 30m workload.
	r.Add(registry.TestSpec{
		Name:            "cdc/pubsub-sink/assume-role",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 1, duration: "30m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType:   pubsubSink,
				assumeRole: "cdc-roachtest-intermediate@cockroach-ephemeral.iam.gserviceaccount.com,cdc-roachtest@cockroach-ephemeral.iam.gserviceaccount.com",
				targets:    allTpccTargets,
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
				steadyLatency:      time.Minute,
			})
			ct.waitForWorkload()
		},
	})

	// In order to run this test, the service account corresponding to the
	// implicit credentials must have the Service Account Token Creator role on
	// the first account on the assume-role chain:
	// cdc-roachtest-intermediate@cockroach-ephemeral.iam.gserviceaccount.com. See
	// https://cloud.google.com/iam/docs/create-short-lived-credentials-direct.
	//
	// TODO(rui): Change to a shorter test as it just needs to validate
	// permissions and shouldn't need to run a full 30m workload.
	r.Add(registry.TestSpec{
		Name:            "cdc/cloud-sink-gcs/assume-role",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 50, duration: "30m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType:   cloudStorageSink,
				assumeRole: "cdc-roachtest-intermediate@cockroach-ephemeral.iam.gserviceaccount.com,cdc-roachtest@cockroach-ephemeral.iam.gserviceaccount.com",
				targets:    allTpccTargets,
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
				steadyLatency:      time.Minute,
			})
			ct.waitForWorkload()
		},
	})

	// TODO(zinger): uncomment once connectivity issue is fixed,
	// currently fails with "initial scan did not complete" because sink
	// URI is set as localhost, need to expose it to the other nodes via IP
	/*
				r.Add(registry.TestSpec{
					Name:            "cdc/webhook-sink",
					Owner:           `cdc`,
					Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
					RequiresLicense: true,
					Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
		        ct := newCDCTester(ctx, t, c)
		        defer ct.Close()

		        ct.runTPCCWorkload(tpccArgs{warehouses: 100, duration: "30m"})

		        feed := ct.newChangefeed(feedArgs{
		          sinkType:   webhookSink,
		          targets:    allTpccTargets,
		        })
		        ct.runFeedLatencyVerifier(feed, latencyTargets{
		          initialScanLatency: 30 * time.Minute,
		          steadyLatency:      time.Minute,
		        })
		        ct.waitForWorkload()
					},
				})
	*/
	r.Add(registry.TestSpec{
		Name:            "cdc/kafka-auth",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(1),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCKafkaAuth(ctx, t, c)
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/bank",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4),
		RequiresLicense: true,
		Timeout:         30 * time.Minute,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCBank(ctx, t, c)
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/schemareg",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(1),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCSchemaRegistry(ctx, t, c)
		},
	})
}

const (
	certLifetime = 30 * 24 * time.Hour
	keyLength    = 2048

	// keystorePassword is the password for any Java KeyStore
	// files we create. The tooling around keystores does not play
	// nicely with passwordless keystores, so we use a wellknown
	// password for testing.
	keystorePassword = "storepassword"
)

type testCerts struct {
	CACert    string
	CAKey     string
	KafkaCert string
	KafkaKey  string
}

func (t *testCerts) CACertBase64() string {
	return base64.StdEncoding.EncodeToString([]byte(t.CACert))
}

func makeTestCerts(kafkaNodeIP string) (*testCerts, error) {
	CAKey, err := rsa.GenerateKey(rand.Reader, keyLength)
	if err != nil {
		return nil, errors.Wrap(err, "CA private key")
	}

	KafkaKey, err := rsa.GenerateKey(rand.Reader, keyLength)
	if err != nil {
		return nil, errors.Wrap(err, "kafka private key")
	}

	CACert, CACertSpec, err := generateCACert(CAKey)
	if err != nil {
		return nil, errors.Wrap(err, "CA cert gen")
	}

	KafkaCert, err := generateKafkaCert(kafkaNodeIP, KafkaKey, CACertSpec, CAKey)
	if err != nil {
		return nil, errors.Wrap(err, "kafka cert gen")
	}

	CAKeyPEM, err := pemEncodePrivateKey(CAKey)
	if err != nil {
		return nil, errors.Wrap(err, "pem encode CA key")
	}

	CACertPEM, err := pemEncodeCert(CACert)
	if err != nil {
		return nil, errors.Wrap(err, "pem encode CA cert")
	}

	KafkaKeyPEM, err := pemEncodePrivateKey(KafkaKey)
	if err != nil {
		return nil, errors.Wrap(err, "pem encode kafka key")
	}

	KafkaCertPEM, err := pemEncodeCert(KafkaCert)
	if err != nil {
		return nil, errors.Wrap(err, "pem encode kafka cert")
	}

	return &testCerts{
		CACert:    CACertPEM,
		CAKey:     CAKeyPEM,
		KafkaCert: KafkaCertPEM,
		KafkaKey:  KafkaKeyPEM,
	}, nil
}

func generateKafkaCert(
	kafkaIP string, priv *rsa.PrivateKey, CACert *x509.Certificate, CAKey *rsa.PrivateKey,
) ([]byte, error) {
	ip := net.ParseIP(kafkaIP)
	if ip == nil {
		return nil, fmt.Errorf("invalid IP address: %s", kafkaIP)
	}

	serial, err := randomSerial()
	if err != nil {
		return nil, err
	}

	certSpec := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Country:            []string{"US"},
			Organization:       []string{"Cockroach Labs"},
			OrganizationalUnit: []string{"Engineering"},
			CommonName:         "kafka-node",
		},
		NotBefore:   timeutil.Now(),
		NotAfter:    timeutil.Now().Add(certLifetime),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDataEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageKeyAgreement,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{ip},
	}

	return x509.CreateCertificate(rand.Reader, certSpec, CACert, &priv.PublicKey, CAKey)
}

func generateCACert(priv *rsa.PrivateKey) ([]byte, *x509.Certificate, error) {
	serial, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}

	certSpec := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Country:            []string{"US"},
			Organization:       []string{"Cockroach Labs"},
			OrganizationalUnit: []string{"Engineering"},
			CommonName:         "Roachtest Temporary Insecure CA",
		},
		NotBefore:             timeutil.Now(),
		NotAfter:              timeutil.Now().Add(certLifetime),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
	}
	cert, err := x509.CreateCertificate(rand.Reader, certSpec, certSpec, &priv.PublicKey, priv)
	return cert, certSpec, err
}

func pemEncode(dataType string, data []byte) (string, error) {
	ret := new(strings.Builder)
	err := pem.Encode(ret, &pem.Block{Type: dataType, Bytes: data})
	if err != nil {
		return "", err
	}

	return ret.String(), nil
}

func pemEncodePrivateKey(key *rsa.PrivateKey) (string, error) {
	return pemEncode("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
}

func pemEncodeCert(cert []byte) (string, error) {
	return pemEncode("CERTIFICATE", cert)
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	ret, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, errors.Wrap(err, "generate random serial")
	}
	return ret, nil
}

const (
	confluentDownloadURL = "https://storage.googleapis.com/cockroach-fixtures/tools/confluent-community-6.1.0.tar.gz"
	confluentSHA256      = "53b0e2f08c4cfc55087fa5c9120a614ef04d306db6ec3bcd7710f89f05355355"
	confluentInstallBase = "confluent-6.1.0"

	confluentCLIVersion         = "1.26.0"
	confluentCLIDownloadURLBase = "https://s3-us-west-2.amazonaws.com/confluent.cloud/confluent-cli/archives"
)

// TODO(ssd): Perhaps something like this could be a roachprod command?
var confluentDownloadScript = fmt.Sprintf(`#!/usr/bin/env bash
set -euo pipefail

CONFLUENT_URL="%s"
CONFLUENT_SHA256="%s"
CONFLUENT_INSTALL_BASE="%s"

CONFLUENT_CLI_VERSION="%s"
CONFLUENT_CLI_URL_BASE="%s"


CONFLUENT_CLI_TAR_PATH="/tmp/confluent-cli-$CONFLUENT_CLI_VERSION.tar.gz"
CONFLUENT_TAR_PATH=/tmp/confluent.tar.gz

CONFLUENT_DIR="$1"

os() {
  uname -s | tr '[:upper:]' '[:lower:]'
}

arch() {
  local arch
  arch=$(uname -m)
  case "$arch" in
    x86_64)
      echo "amd64"
      ;;
    *)
      echo "$arch"
      ;;
  esac
}

checkFile() {
  local file_name="${1}"
  local expected_shasum="${2}"

  local actual_shasum=""
  if command -v sha256sum > /dev/null 2>&1; then
    actual_shasum=$(sha256sum "$file_name" | cut -f1 -d' ')
  elif command -v shasum > /dev/null 2>&1; then
    actual_shasum=$(shasum -a 256 "$file_name" | cut -f1 -d' ')
  else
    echo "sha256sum or shasum not found" >&2
    return 1
  fi

  if [[ "$actual_shasum" == "$expected_shasum" ]]; then
     return 0
  else
    return 1
  fi
}

download() {
  URL="$1"
  OUTPUT_FILE="$2"
  for i in $(seq 1 5); do
    if curl --retry 3 --retry-delay 1 --fail --show-error -o "$OUTPUT_FILE" "$URL"; then
      break
    fi
    sleep 15;
  done
}

PLATFORM="$(os)/$(arch)"
case "$PLATFORM" in
    linux/amd64)
      CONFLUENT_CLI_URL="${CONFLUENT_CLI_URL_BASE}/${CONFLUENT_CLI_VERSION}/confluent_v${CONFLUENT_CLI_VERSION}_linux_amd64.tar.gz"
      ;;
    darwin/amd64)
      CONFLUENT_CLI_URL="${CONFLUENT_CLI_URL_BASE}/${CONFLUENT_CLI_VERSION}/confluent_v${CONFLUENT_CLI_VERSION}_darwin_amd64.tar.gz"
      ;;
    *)
      echo "We don't know how to install the confluent CLI for \"${PLATFORM}\""
      exit 1
      ;;
esac

if ! [[ -f "$CONFLUENT_TAR_PATH" ]] || ! checkFile "$CONFLUENT_TAR_PATH" "$CONFLUENT_SHA256"; then
  download "$CONFLUENT_URL" "$CONFLUENT_TAR_PATH"
fi

tar xvf "$CONFLUENT_TAR_PATH" -C "$CONFLUENT_DIR"

if ! [[ -f "$CONFLUENT_DIR/bin/confluent" ]]; then
  if ! [[ -f "$CONFLUENT_CLI_TAR_PATH" ]]; then
    download "$CONFLUENT_CLI_URL" "$CONFLUENT_CLI_TAR_PATH"
  fi
  tar xvf "$CONFLUENT_CLI_TAR_PATH" -C "$CONFLUENT_DIR/$CONFLUENT_INSTALL_BASE/bin/" --strip-components=1 confluent/confluent
fi
`, confluentDownloadURL, confluentSHA256, confluentInstallBase, confluentCLIVersion, confluentCLIDownloadURLBase)

const (
	// kafkaJAASConfig is a JAAS configuration file that creates a
	// user called "plain" with password "plain-secret" that can
	// authenticate via SASL/PLAIN.
	//
	// Users to test SCRAM authentication are added via
	// kafka-config commands as their credentials are stored in
	// zookeeper.
	//
	// Newer versions of confluent configure this directly in
	// server.properties.
	//
	// This configuration file is used by the kafka-auth tests to
	// enable TLS and SASL. Other tests use an empty file as they
	// require no authentication.
	kafkaJAASConfig = `
KafkaServer {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username="admin"
   password="admin-secret"
   user_admin="admin-secret"
   user_plain="plain-secret";

   org.apache.kafka.common.security.scram.ScramLoginModule required
   username="admin"
   password="admin-secret";
};
`

	// kafkaConfigTmpl is a template for Kafka's server.properties
	// configuration file. This template is used by the kafka-auth
	// tests to enable TLS and SASL. Other tests uses the default
	// configuration contained in the confluent archive we
	// install.
	kafkaConfigTmpl = `
ssl.truststore.location=%s
ssl.truststore.password=%s

ssl.keystore.location=%s
ssl.keystore.password=%s

listeners=PLAINTEXT://:9092,SSL://:9093,SASL_SSL://:9094

sasl.enabled.mechanisms=SCRAM-SHA-256,SCRAM-SHA-512,PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN
inter.broker.listener.name=SASL_SSL

# The following is from the confluent-4.0 default configuration file.
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=1
num.recovery.threads.per.data.dir=1

offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=6000
confluent.support.metrics.enable=false
confluent.support.customer.id=anonymous
`
)

type kafkaManager struct {
	t     test.Test
	c     cluster.Cluster
	nodes option.NodeListOption
}

func (k kafkaManager) basePath() string {
	if k.c.IsLocal() {
		return `/tmp/confluent`
	}
	return `/mnt/data1/confluent`
}

func (k kafkaManager) confluentHome() string {
	return filepath.Join(k.basePath(), confluentInstallBase)
}

func (k kafkaManager) configDir() string {
	return filepath.Join(k.basePath(), confluentInstallBase, "etc/kafka")
}

func (k kafkaManager) binDir() string {
	return filepath.Join(k.basePath(), confluentInstallBase, "bin")
}

func (k kafkaManager) confluentBin() string {
	return filepath.Join(k.binDir(), "confluent")
}

func (k kafkaManager) serverJAASConfig() string {
	return filepath.Join(k.configDir(), "server_jaas.conf")
}

func (k kafkaManager) install(ctx context.Context) {
	k.t.Status("installing kafka")
	folder := k.basePath()

	k.c.Run(ctx, k.nodes, `mkdir -p `+folder)

	downloadScriptPath := filepath.Join(folder, "install.sh")
	err := k.c.PutString(ctx, confluentDownloadScript, downloadScriptPath, 0700, k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	k.c.Run(ctx, k.nodes, downloadScriptPath, folder)
	if !k.c.IsLocal() {
		k.c.Run(ctx, k.nodes, `mkdir -p logs`)
		if err := k.installJRE(ctx); err != nil {
			k.t.Fatal(err)
		}
	}
}

func (k kafkaManager) installJRE(ctx context.Context) error {
	retryOpts := retry.Options{
		InitialBackoff: 1 * time.Minute,
		MaxBackoff:     5 * time.Minute,
	}
	return retry.WithMaxAttempts(ctx, retryOpts, 3, func() error {
		err := k.c.RunE(ctx, k.nodes, `sudo apt-get -q update 2>&1 > logs/apt-get-update.log`)
		if err != nil {
			return err
		}
		return k.c.RunE(ctx, k.nodes, `sudo DEBIAN_FRONTEND=noninteractive apt-get -yq --no-install-recommends install openssl default-jre 2>&1 > logs/apt-get-install.log`)
	})
}

func (k kafkaManager) configureAuth(ctx context.Context) *testCerts {
	k.t.Status("generating TLS certificates")
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	kafkaIP := ips[0]

	testCerts, err := makeTestCerts(kafkaIP)
	if err != nil {
		k.t.Fatal(err)
	}

	configDir := k.configDir()
	// truststorePath is the path to our "truststore", a Java
	// KeyStore that contains any CA certificates we want to
	// trust.
	truststorePath := filepath.Join(configDir, "kafka.truststore.jks")
	// keyStorePath is the path to our "keystore", a Java KeyStore
	// that contains the certificates and private keys that we
	// will use to establish TLS connections.
	keystorePath := filepath.Join(configDir, "kafka.keystore.jks")

	caKeyPath := filepath.Join(configDir, "ca.key")
	caCertPath := filepath.Join(configDir, "ca.crt")

	kafkaKeyPath := filepath.Join(configDir, "kafka.key")
	kafkaCertPath := filepath.Join(configDir, "kafka.crt")
	kafkaBundlePath := filepath.Join(configDir, "kafka.p12")

	kafkaConfigPath := filepath.Join(configDir, "server.properties")
	kafkaJAASPath := filepath.Join(configDir, "server_jaas.conf")

	k.t.Status("writing kafka configuration files")
	kafkaConfig := fmt.Sprintf(kafkaConfigTmpl,
		truststorePath,
		keystorePassword,
		keystorePath,
		keystorePassword,
	)

	k.PutConfigContent(ctx, testCerts.KafkaKey, kafkaKeyPath)
	k.PutConfigContent(ctx, testCerts.KafkaCert, kafkaCertPath)
	k.PutConfigContent(ctx, testCerts.CAKey, caKeyPath)
	k.PutConfigContent(ctx, testCerts.CACert, caCertPath)
	k.PutConfigContent(ctx, kafkaConfig, kafkaConfigPath)
	k.PutConfigContent(ctx, kafkaJAASConfig, kafkaJAASPath)

	k.t.Status("constructing java keystores")
	// Convert PEM cert and key into pkcs12 bundle so that it can be imported into a java keystore.
	k.c.Run(ctx, k.nodes,
		fmt.Sprintf("openssl pkcs12 -export -in %s -inkey %s -name kafka -out %s -password pass:%s",
			kafkaCertPath,
			kafkaKeyPath,
			kafkaBundlePath,
			keystorePassword))

	k.c.Run(ctx, k.nodes, fmt.Sprintf("rm -f %s", keystorePath))
	k.c.Run(ctx, k.nodes, fmt.Sprintf("rm -f %s", truststorePath))

	k.c.Run(ctx, k.nodes,
		fmt.Sprintf("keytool -importkeystore -deststorepass %s -destkeystore %s -srckeystore %s -srcstoretype PKCS12 -srcstorepass %s -alias kafka",
			keystorePassword,
			keystorePath,
			kafkaBundlePath,
			keystorePassword))
	k.c.Run(ctx, k.nodes,
		fmt.Sprintf("keytool -keystore %s -alias CAroot -importcert -file %s -no-prompt -storepass %s",
			truststorePath,
			caCertPath,
			keystorePassword))
	k.c.Run(ctx, k.nodes,
		fmt.Sprintf("keytool -keystore %s -alias CAroot -importcert -file %s -no-prompt -storepass %s",
			keystorePath,
			caCertPath,
			keystorePassword))

	return testCerts
}

func (k kafkaManager) PutConfigContent(ctx context.Context, data string, path string) {
	err := k.c.PutString(ctx, data, path, 0600, k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
}

func (k kafkaManager) addSCRAMUsers(ctx context.Context) {
	k.t.Status("adding entries for SASL/SCRAM users")
	k.c.Run(ctx, k.nodes, filepath.Join(k.binDir(), "kafka-configs"),
		"--zookeeper", "localhost:2181",
		"--alter",
		"--add-config", "SCRAM-SHA-512=[password=scram512-secret]",
		"--entity-type", "users",
		"--entity-name", "scram512")

	k.c.Run(ctx, k.nodes, filepath.Join(k.binDir(), "kafka-configs"),
		"--zookeeper", "localhost:2181",
		"--alter",
		"--add-config", "SCRAM-SHA-256=[password=scram256-secret]",
		"--entity-type", "users",
		"--entity-name", "scram256")
}

func (k kafkaManager) start(ctx context.Context, services ...string) {
	// This isn't necessary for the nightly tests, but it's nice for iteration.
	k.c.Run(ctx, k.nodes, k.makeCommand("confluent", "local destroy || true"))
	k.restart(ctx, services...)
}

var kafkaServices = map[string][]string{
	"zookeeper":       {"zookeeper"},
	"kafka":           {"zookeeper", "kafka"},
	"schema-registry": {"zookeeper", "kafka", "schema-registry"},
}

func (k kafkaManager) kafkaServicesForTargets(targets []string) []string {
	var services []string
	for _, tgt := range targets {
		if s, ok := kafkaServices[tgt]; ok {
			services = append(services, s...)
		} else {
			k.t.Fatalf("unknown kafka start target %q", tgt)
		}
	}
	return services
}

func (k kafkaManager) restart(ctx context.Context, targetServices ...string) {
	var services []string
	if len(targetServices) == 0 {
		services = kafkaServices["schema-registry"]
	} else {
		services = k.kafkaServicesForTargets(targetServices)
	}

	k.c.Run(ctx, k.nodes, "touch", k.serverJAASConfig())
	for _, svcName := range services {
		// The confluent tool applies the KAFKA_OPTS to all
		// services. Also, the kafka.logs.dir is used by each
		// service, despite the name.
		opts := fmt.Sprintf("-Djava.security.auth.login.config=%s -Dkafka.logs.dir=%s",
			k.serverJAASConfig(),
			fmt.Sprintf("logs/%s", svcName),
		)
		startCmd := fmt.Sprintf(
			"CONFLUENT_CURRENT=%s CONFLUENT_HOME=%s KAFKA_OPTS='%s' %s local services %s start",
			k.basePath(),
			k.confluentHome(),
			opts,
			k.confluentBin(),
			svcName)
		k.c.Run(ctx, k.nodes, startCmd)
	}

}

func (k kafkaManager) makeCommand(exe string, args ...string) string {
	cmdPath := filepath.Join(k.binDir(), exe)
	return fmt.Sprintf("CONFLUENT_CURRENT=%s CONFLUENT_HOME=%s %s %s",
		k.basePath(),
		k.confluentHome(),
		cmdPath, strings.Join(args, " "))
}

func (k kafkaManager) stop(ctx context.Context) {
	k.c.Run(ctx, k.nodes, fmt.Sprintf("rm -f %s", k.serverJAASConfig()))
	k.c.Run(ctx, k.nodes, k.makeCommand("confluent", "local services stop"))
}

func (k kafkaManager) chaosLoop(
	ctx context.Context, period, downTime time.Duration, stopper chan struct{},
) error {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-stopper:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}

		k.stop(ctx)

		select {
		case <-stopper:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(downTime):
		}

		k.restart(ctx)
	}
}

func (k kafkaManager) sinkURL(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `kafka://` + ips[0] + `:9092`
}

func (k kafkaManager) sinkURLTLS(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `kafka://` + ips[0] + `:9093`
}

func (k kafkaManager) sinkURLSASL(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `kafka://` + ips[0] + `:9094`
}

func (k kafkaManager) consumerURL(ctx context.Context) string {
	ips, err := k.c.ExternalIP(ctx, k.t.L(), k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return ips[0] + `:9092`
}

func (k kafkaManager) schemaRegistryURL(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `http://` + ips[0] + `:8081`
}

func (k kafkaManager) createTopic(ctx context.Context, topic string) error {
	kafkaAddrs := []string{k.consumerURL(ctx)}
	config := sarama.NewConfig()
	return retry.ForDuration(kafkaCreateTopicRetryDuration, func() error {
		admin, err := sarama.NewClusterAdmin(kafkaAddrs, config)
		if err != nil {
			return errors.Wrap(err, "admin client")
		}
		return admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
		}, false)
	})
}

func (k kafkaManager) consumer(ctx context.Context, topic string) (*topicConsumer, error) {
	kafkaAddrs := []string{k.consumerURL(ctx)}
	config := sarama.NewConfig()
	// I was seeing "error processing FetchRequest: kafka: error decoding
	// packet: unknown magic byte (2)" errors which
	// https://github.com/Shopify/sarama/issues/962 identifies as the
	// consumer's fetch size being less than the "max.message.bytes" that
	// kafka is configured with. Kafka notes that this is required in
	// https://kafka.apache.org/documentation.html#upgrade_11_message_format
	config.Consumer.Fetch.Default = 1000012
	consumer, err := sarama.NewConsumer(kafkaAddrs, config)
	if err != nil {
		return nil, err
	}
	tc, err := makeTopicConsumer(consumer, topic)
	if err != nil {
		_ = consumer.Close()
		return nil, err
	}
	return tc, nil
}

type tpccWorkload struct {
	workloadNodes      option.NodeListOption
	sqlNodes           option.NodeListOption
	tpccWarehouseCount int
	tolerateErrors     bool
}

func (tw *tpccWorkload) install(ctx context.Context, c cluster.Cluster) {
	// For fixtures import, use the version built into the cockroach binary so
	// the tpcc workload-versions match on release branches.
	c.Run(ctx, tw.workloadNodes, fmt.Sprintf(
		`./cockroach workload fixtures import tpcc --warehouses=%d --checks=false {pgurl%s}`,
		tw.tpccWarehouseCount,
		tw.sqlNodes.RandNode(),
	))
}

func (tw *tpccWorkload) run(ctx context.Context, c cluster.Cluster, workloadDuration string) {
	tolerateErrors := ""
	if tw.tolerateErrors {
		tolerateErrors = "--tolerate-errors"
	}
	c.Run(ctx, tw.workloadNodes, fmt.Sprintf(
		`./workload run tpcc --warehouses=%d --duration=%s %s {pgurl%s} `,
		tw.tpccWarehouseCount, workloadDuration, tolerateErrors, tw.sqlNodes,
	))
}

type ledgerWorkload struct {
	workloadNodes option.NodeListOption
	sqlNodes      option.NodeListOption
}

func (lw *ledgerWorkload) install(ctx context.Context, c cluster.Cluster) {
	// TODO(#94136): remove --data-loader=INSERT
	c.Run(ctx, lw.workloadNodes.RandNode(), fmt.Sprintf(
		`./workload init ledger --data-loader=INSERT {pgurl%s}`,
		lw.sqlNodes.RandNode(),
	))
}

func (lw *ledgerWorkload) run(ctx context.Context, c cluster.Cluster, workloadDuration string) {
	// TODO(#94136): remove --data-loader=INSERT
	c.Run(ctx, lw.workloadNodes, fmt.Sprintf(
		`./workload run ledger --data-loader=INSERT --mix=balance=0,withdrawal=50,deposit=50,reversal=0 {pgurl%s} --duration=%s`,
		lw.sqlNodes,
		workloadDuration,
	))
}

// changefeedCreator wraps the process of creating a changefeed with
// different options and sinks
type changefeedCreator struct {
	db        *gosql.DB
	targets   string
	sinkURL   string
	options   map[string]string
	extraArgs []interface{}
}

func newChangefeedCreator(db *gosql.DB, targets, sinkURL string) *changefeedCreator {
	return &changefeedCreator{
		db:      db,
		targets: targets,
		sinkURL: sinkURL,
		options: make(map[string]string),
	}
}

// With adds options to the changefeed being created. If a non-zero
// `value` is passed in one of the options, the option will be passed
// as {option}={value}.
func (cfc *changefeedCreator) With(opts map[string]string) *changefeedCreator {
	for option, value := range opts {
		cfc.options[option] = value
	}
	return cfc
}

// Args adds extra statement arguments to the query used when creating
// the changefeed. This is useful if, for instance, we want to use
// arguments when specifying changefeed options (e.g.,
// "some_option=$2")
func (cfc *changefeedCreator) Args(args ...interface{}) *changefeedCreator {
	cfc.extraArgs = append(cfc.extraArgs, args...)
	return cfc
}

// Create builds the SQL statement that creates the changefeed job,
// and executes it. Returns the job ID corresponding to the
// changefeed, and any errors that occurred in the process
func (cfc *changefeedCreator) Create() (int, error) {
	// kv.rangefeed.enabled is required for changefeeds to run
	if _, err := cfc.db.Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
		return -1, err
	}

	stmt := fmt.Sprintf("CREATE CHANGEFEED FOR %s INTO $1", cfc.targets)

	var options []string
	for option, value := range cfc.options {
		if value != "" {
			option += fmt.Sprintf("=%s", value)
		}
		options = append(options, option)
	}

	if len(options) > 0 {
		stmt += fmt.Sprintf(" WITH %s", strings.Join(options, ", "))
	}

	var jobID int
	args := append([]interface{}{cfc.sinkURL}, cfc.extraArgs...)
	if err := cfc.db.QueryRow(stmt, args...).Scan(&jobID); err != nil {
		return -1, err
	}

	return jobID, nil
}

type changefeedInfo struct {
	status        string
	errMsg        string
	statementTime time.Time
	highwaterTime time.Time
	finishedTime  time.Time
}

func (c *changefeedInfo) GetHighWater() time.Time    { return c.highwaterTime }
func (c *changefeedInfo) GetFinishedTime() time.Time { return c.finishedTime }
func (c *changefeedInfo) GetStatus() string          { return c.status }
func (c *changefeedInfo) GetError() string           { return c.status }

var _ jobInfo = (*changefeedInfo)(nil)

func getChangefeedInfo(db *gosql.DB, jobID int) (*changefeedInfo, error) {
	var status string
	var payloadBytes []byte
	var progressBytes []byte
	if err := db.QueryRow(
		`SELECT status, payload, progress FROM system.jobs WHERE id = $1`, jobID,
	).Scan(&status, &payloadBytes, &progressBytes); err != nil {
		return nil, err
	}
	var payload jobspb.Payload
	if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, err
	}
	var progress jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
		return nil, err
	}
	var highwaterTime time.Time
	highwater := progress.GetHighWater()
	if highwater != nil {
		highwaterTime = highwater.GoTime()
	}
	return &changefeedInfo{
		status:        status,
		errMsg:        payload.Error,
		statementTime: payload.GetChangefeed().StatementTime.GoTime(),
		highwaterTime: highwaterTime,
		finishedTime:  time.UnixMicro(payload.FinishedMicros),
	}, nil
}

// stopFeeds cancels any running feeds on the cluster. Not necessary for the
// nightly, but nice for development.
func stopFeeds(db *gosql.DB) {
	_, _ = db.Exec(`CANCEL JOBS (
			SELECT job_id FROM [SHOW JOBS] WHERE status = 'running'
		)`)
}

// setupKafka installs Kafka on the cluster and configures it so that
// the test runner can connect to it. Returns a function to be called
// at the end of the test for stopping Kafka.
func setupKafka(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes option.NodeListOption,
) (kafkaManager, func()) {
	kafka := kafkaManager{
		t:     t,
		c:     c,
		nodes: nodes,
	}

	kafka.install(ctx)
	if !c.IsLocal() {
		// TODO(dan): This test currently connects to kafka from the test
		// runner, so kafka needs to advertise the external address. Better
		// would be a binary we could run on one of the roachprod machines.
		c.Run(ctx, kafka.nodes, `echo "advertised.listeners=PLAINTEXT://`+kafka.consumerURL(ctx)+`" >> `+
			filepath.Join(kafka.configDir(), "server.properties"))
	}

	kafka.start(ctx, "kafka")
	return kafka, func() { kafka.stop(ctx) }
}

type topicConsumer struct {
	sarama.Consumer

	topic              string
	partitions         []string
	partitionConsumers []sarama.PartitionConsumer
}

func makeTopicConsumer(c sarama.Consumer, topic string) (*topicConsumer, error) {
	t := &topicConsumer{Consumer: c, topic: topic}
	partitions, err := t.Partitions(t.topic)
	if err != nil {
		return nil, err
	}
	for _, partition := range partitions {
		t.partitions = append(t.partitions, strconv.Itoa(int(partition)))
		pc, err := t.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return nil, err
		}
		t.partitionConsumers = append(t.partitionConsumers, pc)
	}
	return t, nil
}

func (c *topicConsumer) tryNextMessage(ctx context.Context) *sarama.ConsumerMessage {
	for _, pc := range c.partitionConsumers {
		select {
		case <-ctx.Done():
			return nil
		case m := <-pc.Messages():
			return m
		default:
		}
	}
	return nil
}

func (c *topicConsumer) Next(ctx context.Context) *sarama.ConsumerMessage {
	m := c.tryNextMessage(ctx)
	for ; m == nil; m = c.tryNextMessage(ctx) {
		if ctx.Err() != nil {
			return nil
		}
	}
	return m
}

func (c *topicConsumer) Close() {
	for _, pc := range c.partitionConsumers {
		pc.AsyncClose()
		// Drain the messages and errors as required by AsyncClose.
		for range pc.Messages() {
		}
		for range pc.Errors() {
		}
	}
	_ = c.Consumer.Close()
}
