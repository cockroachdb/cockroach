// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	gosql "database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/config"
	msk "github.com/aws/aws-sdk-go-v2/service/kafka"
	msktypes "github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/aws/aws-sdk-go/aws"
	awslog "github.com/aws/smithy-go/logging"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	roachprodaws "github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/debug"
	"github.com/cockroachdb/errors"
	prompb "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/clientcredentials"
)

// kafkaCreateTopicRetryDuration is the retry duration we use while
// trying to create a Kafka topic after setting it up on a
// node. Without retrying, a `kafka controller not available` error is
// seen with a 1-5% probability
var kafkaCreateTopicRetryDuration = 1 * time.Minute

// hydraRetryDuration is the length of time we retry hydra oauth setup.
var hydraRetryDuration = 1 * time.Minute

type sinkType string

const (
	cloudStorageSink       sinkType = "cloudstorage"
	webhookSink            sinkType = "webhook"
	pubsubSink             sinkType = "pubsub"
	kafkaSink              sinkType = "kafka"
	azureEventHubKafkaSink sinkType = "azure-event-hub"
	mskSink                sinkType = "msk"
	nullSink               sinkType = "null"
)

var envVars = []string{
	// Setting COCKROACH_CHANGEFEED_TESTING_FAST_RETRY helps tests run quickly.
	// NB: This is crucial for chaos tests as we expect changefeeds to see
	// many retries.
	"COCKROACH_CHANGEFEED_TESTING_FAST_RETRY=true",
	"COCKROACH_CHANGEFEED_TESTING_INCLUDE_PARQUET_TEST_METADATA=true",
	"COCKROACH_CHANGEFEED_TESTING_INCLUDE_PARQUET_READER_METADATA=true",
	// Enable strict re-balancing checks to ensure that rebalancing doesn't create an
	// incorrect set of spans for the changefeed.
	"COCKROACH_CHANGEFEED_TESTING_REBALANCING_CHECKS=true",
}

type cdcTester struct {
	ctx          context.Context
	t            test.Test
	mon          cluster.Monitor
	cluster      cluster.Cluster
	crdbNodes    option.NodeListOption
	workloadNode option.NodeListOption
	sinkNodes    option.NodeListOption
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

type AuthorizationRuleKeys struct {
	KeyName    string `json:"keyName"`
	PrimaryKey string `json:"primaryKey"`
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
		Env:     envVars,
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
		ct.t.Status("webhook install")
		webhookNode := ct.sinkNodes
		rootFolder := `/home/ubuntu`
		nodeIPs, _ := ct.cluster.ExternalIP(ct.ctx, ct.logger, webhookNode)

		// We use a different port every time to support multiple webhook sinks.
		webhookPort := nextWebhookPort
		nextWebhookPort++
		serverSrcPath := filepath.Join(rootFolder, fmt.Sprintf(`webhook-server-%d.go`, webhookPort))
		err := ct.cluster.PutString(ct.ctx, webhookServerScript(webhookPort), serverSrcPath, 0700, webhookNode)
		if err != nil {
			ct.t.Fatal(err)
		}

		certs, err := makeTestCerts(nodeIPs[0])
		if err != nil {
			ct.t.Fatal(err)
		}
		err = ct.cluster.PutString(ct.ctx, certs.SinkKey, filepath.Join(rootFolder, "key.pem"), 0700, webhookNode)
		if err != nil {
			ct.t.Fatal(err)
		}
		err = ct.cluster.PutString(ct.ctx, certs.SinkCert, filepath.Join(rootFolder, "cert.pem"), 0700, webhookNode)
		if err != nil {
			ct.t.Fatal(err)
		}

		// Start the server in its own monitor to not block ct.mon.Wait()
		serverExecCmd := fmt.Sprintf(`go run webhook-server-%d.go`, webhookPort)
		m := ct.cluster.NewMonitor(ct.ctx, ct.workloadNode)
		m.Go(func(ctx context.Context) error {
			return ct.cluster.RunE(ct.ctx, option.WithNodes(webhookNode), serverExecCmd, rootFolder)
		})

		sinkDestHost, err := url.Parse(fmt.Sprintf(`https://%s:%d`, nodeIPs[0], webhookPort))
		if err != nil {
			ct.t.Fatal(err)
		}

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certs.CACertBase64())
		params.Set(changefeedbase.SinkParamTLSEnabled, "true")
		sinkDestHost.RawQuery = params.Encode()
		sinkURI = fmt.Sprintf("webhook-%s", sinkDestHost.String())
	case pubsubSink:
		sinkURI = changefeedccl.GcpScheme + `://cockroach-ephemeral` + "?AUTH=implicit&topic_name=pubsubSink-roachtest&region=us-east1"
	case kafkaSink:
		kafka, _ := setupKafka(ct.ctx, ct.t, ct.cluster, ct.sinkNodes)
		kafka.mon = ct.mon
		kafka.validateOrder = args.kafkaArgs.validateOrder

		if err := kafka.startTopicConsumers(ct.ctx, args.targets, ct.doneCh); err != nil {
			ct.t.Fatal(err)
		}

		if args.kafkaArgs.kafkaChaos {
			ct.mon.Go(func(ctx context.Context) error {
				period, downTime := 2*time.Minute, 20*time.Second
				return kafka.chaosLoop(ctx, period, downTime, ct.doneCh)
			})
		}

		sinkURI = kafka.sinkURL(ct.ctx)
	case azureEventHubKafkaSink:
		kafkaNode := ct.sinkNodes
		kafka := kafkaManager{
			t:              ct.t,
			c:              ct.cluster,
			kafkaSinkNodes: kafkaNode,
			mon:            ct.mon,
		}
		kafka.install(ct.ctx)
		kafka.start(ct.ctx, "kafka")
		if err := kafka.installAzureCli(ct.ctx); err != nil {
			kafka.t.Fatal(err)
		}
		accessKeyName, accessKey, err := kafka.getAzureEventHubAccess(ct.ctx)
		if err != nil {
			kafka.t.Fatal(err)
		}
		sinkURI = fmt.Sprintf(
			`azure-event-hub://cdc-roachtest.servicebus.windows.net:9093?shared_access_key_name=%s&shared_access_key=%s&topic_name=testing`,
			url.QueryEscape(accessKeyName), url.QueryEscape(accessKey),
		)
	case mskSink:
		// Currently, the only msk tests are manual tests. When they are run,
		// this placeholder should be replaced with the actual bootstrap server
		// for the cluster being used.
		// TODO(yang): If we want to run msk roachtests nightly, replace this
		// with a long-running MSK cluster or maybe create a fresh cluster.
		sinkURI = "kafka://placeholder"
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
	conns          int
	noWait         bool
	cdcFeatureFlags
}

func (ct *cdcTester) lockSchema(targets []string) {
	for _, target := range targets {
		if _, err := ct.DB().Exec(fmt.Sprintf("ALTER TABLE %s SET (schema_locked=true)", target)); err != nil {
			ct.t.Fatal(err)
		}
	}
}

func (ct *cdcTester) runTPCCWorkload(args tpccArgs) {
	tpcc := tpccWorkload{
		sqlNodes:           ct.crdbNodes,
		workloadNodes:      ct.workloadNode,
		tpccWarehouseCount: args.warehouses,
		conns:              args.conns,
		noWait:             args.noWait,
		// TolerateErrors if crdbChaos is true; otherwise, the workload will fail
		// if it attempts to use the node which was brought down by chaos.
		tolerateErrors: args.tolerateErrors,
	}

	if !ct.t.SkipInit() {
		ct.t.Status("installing TPCC workload")
		tpcc.install(ct.ctx, ct.cluster)
		if args.SchemaLockTables.enabled(globalEntropy) == featureEnabled {
			ct.t.Status(fmt.Sprintf("Setting schema_locked for %s", allTpccTargets))
			ct.lockSchema(allTpccTargets)
		}
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

type featureFlag struct {
	v *featureState
}

type featureState int

var (
	featureUnset    featureState = 0
	featureDisabled featureState = 1
	featureEnabled  featureState = 2
)

type entropy struct {
	*rand.Rand
}

func (r *entropy) Bool() bool {
	if r.Rand == nil {
		return rand.Int()%2 == 0
	}
	return r.Rand.Int()%2 == 0
}

func (r *entropy) Intn(n int) int {
	if r.Rand == nil {
		return rand.Intn(n)
	}
	return r.Rand.Intn(n)
}

var globalRand *rand.Rand
var globalEntropy entropy

func (f *featureFlag) enabled(r entropy) featureState {
	if f.v != nil {
		return *f.v
	}

	if r.Bool() {
		f.v = &featureEnabled
		return featureEnabled
	}
	f.v = &featureDisabled
	return featureDisabled
}

type enumFeatureFlag struct {
	state string
	v     *featureState
}

// enabled returns a valid string if the returned featureState is featureEnabled.
func (f *enumFeatureFlag) enabled(r entropy, choose func(entropy) string) (string, featureState) {
	if f.v != nil {
		return f.state, *f.v
	}

	if r.Bool() {
		f.v = &featureEnabled
		f.state = choose(r)
		return f.state, featureEnabled
	}
	f.v = &featureDisabled
	return f.state, featureDisabled
}

// cdcFeatureFlags describes various cdc feature flags.
// zero value cdcFeatureFlags uses metamorphic settings for features.
type cdcFeatureFlags struct {
	RangeFeedScheduler   featureFlag
	SchemaLockTables     featureFlag
	DistributionStrategy enumFeatureFlag
}

func makeDefaultFeatureFlags() cdcFeatureFlags {
	return cdcFeatureFlags{}
}

type feedArgs struct {
	sinkType        sinkType
	targets         []string
	opts            map[string]string
	assumeRole      string
	tolerateErrors  bool
	sinkURIOverride string
	cdcFeatureFlags
	kafkaArgs kafkaFeedArgs
}

// kafkaFeedArgs are args that are specific to kafkaSink changefeeds.
type kafkaFeedArgs struct {
	// If kafkaChaos is true, the Kafka cluster will periodically restart
	// to simulate unreliability.
	kafkaChaos bool
	// If validateOrder is set to true, order validators will be created
	// for each topic to validate the changefeed's ordering guarantees.
	validateOrder bool
}

func (ct *cdcTester) newChangefeed(args feedArgs) changefeedJob {
	ct.t.Status(fmt.Sprintf("initiating %s sink", args.sinkType))
	var sinkURI string
	if args.sinkURIOverride == "" {
		sinkURI = ct.setupSink(args)
	} else {
		sinkURI = args.sinkURIOverride
	}
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
	if args.kafkaArgs.validateOrder {
		feedOptions["updated"] = ""
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
	jobID, err := newChangefeedCreator(db, db, ct.logger, globalRand, targetsStr, sinkURI, makeDefaultFeatureFlags()).
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
		logger:         ct.logger,
	}

	ct.t.Status(fmt.Sprintf("created changefeed %s with jobID %d", cj.Label(), jobID))

	return cj
}

// verifyMetrics runs the check function on the prometheus metrics of each
// cockroach node in the cluster until it returns true, or until its retry
// period expires.
func (ct *cdcTester) verifyMetrics(
	ctx context.Context, check func(metrics map[string]*prompb.MetricFamily) (ok bool),
) {
	parser := expfmt.TextParser{}

	testutils.SucceedsSoon(ct.t, func() error {
		uiAddrs, err := ct.cluster.ExternalAdminUIAddr(ctx, ct.logger, ct.cluster.CRDBNodes())
		if err != nil {
			return err
		}
		for _, uiAddr := range uiAddrs {
			uiAddr = fmt.Sprintf("https://%s/_status/vars", uiAddr)
			out, err := exec.Command("curl", "-kf", uiAddr).Output()
			if err != nil {
				return err
			}
			res, err := parser.TextToMetricFamilies(bytes.NewReader(out))
			if err != nil {
				return err
			}
			if check(res) {
				ct.t.Status("metrics check passed")
				return nil
			}
		}
		return errors.New("metrics check failed")
	})
}

// runFeedLatencyVerifier runs a goroutine which polls the various latencies
// for a changefeed job (initial scan latency, etc) and asserts that they
// are below the specified targets.
//
// It returns a function which blocks until the job succeeds and verification
// on the succeeded job completes.
func (ct *cdcTester) runFeedLatencyVerifier(
	cj changefeedJob, targets latencyTargets,
) (waitForCompletion func()) {
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

	finished := make(chan struct{})
	ct.mon.Go(func(ctx context.Context) error {
		defer close(finished)
		err := verifier.pollLatencyUntilJobSucceeds(ctx, ct.DB(), cj.jobID, time.Second, ct.doneCh)
		if err != nil {
			return err
		}

		verifier.assertValid(ct.t)
		verifier.maybeLogLatencyHist()
		return nil
	})

	return func() {
		select {
		case <-ct.ctx.Done():
		case <-finished:
		}
	}
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

type opt func(ct *cdcTester)

// withNumSinkNodes sets the number of nodes to use for sink nodes. Only Kafka has been tested currently.
// N.B. this allocates a workload + sink only node, without it, workload and sink will be on the same node.
func withNumSinkNodes(num int) opt {
	return func(ct *cdcTester) {
		ct.crdbNodes = ct.cluster.Range(1, ct.cluster.Spec().NodeCount-num-1)
		ct.workloadNode = ct.cluster.Node(ct.cluster.Spec().NodeCount)
		ct.sinkNodes = ct.cluster.Range(ct.cluster.Spec().NodeCount-num, ct.cluster.Spec().NodeCount-1)
	}
}

// Silence staticcheck.
var _ = withNumSinkNodes

func newCDCTester(ctx context.Context, t test.Test, c cluster.Cluster, opts ...opt) cdcTester {
	// By convention the nodes are split up like [crdb..., sink..., workload].
	// N.B.:
	//    - If it's a single node cluster everything shares node 1.
	//    - If the sink is not provisioned through the withNumSinkNodes opt, it shares a node with the workload node.
	lastCrdbNode := c.Spec().NodeCount - 1
	if lastCrdbNode == 0 {
		lastCrdbNode = 1
	}

	tester := cdcTester{
		ctx:          ctx,
		t:            t,
		cluster:      c,
		crdbNodes:    c.Range(1, lastCrdbNode),
		workloadNode: c.Node(c.Spec().NodeCount),
		sinkNodes:    c.Node(c.Spec().NodeCount),
		doneCh:       make(chan struct{}),
		sinkCache:    make(map[sinkType]string),
		workloadWg:   &sync.WaitGroup{},
	}

	for _, opt := range opts {
		opt(&tester)
	}

	tester.mon = c.NewMonitor(ctx, tester.crdbNodes)

	changefeedLogger, err := t.L().ChildLogger("changefeed")
	if err != nil {
		t.Fatal(err)
	}
	tester.logger = changefeedLogger

	startOpts, settings := makeCDCBenchOptions(c)

	// With a target_duration of 10s, we won't see slow span logs from changefeeds untils we are > 100s
	// behind, which is well above the 60s targetSteadyLatency we have in some tests.
	settings.ClusterSettings["changefeed.slow_span_log_threshold"] = "30s"
	settings.ClusterSettings["server.child_metrics.enabled"] = "true"

	settings.Env = append(settings.Env, envVars...)

	c.Start(ctx, t.L(), startOpts, settings, tester.crdbNodes)
	tester.startGrafana()
	return tester
}

func (ct *cdcTester) startGrafana() {
	cfg := (&prometheus.Config{}).
		WithPrometheusNode(ct.workloadNode.InstallNodes()[0]).
		WithCluster(ct.crdbNodes.InstallNodes()).
		WithNodeExporter(ct.crdbNodes.InstallNodes()).
		WithGrafanaDashboardJSON(grafana.ChangefeedRoachtestGrafanaDashboardJSON)

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
	// Make the logs dir on every node to work around the `roachprod get logs`
	// spam.
	c.Run(ctx, option.WithNodes(c.All()), `mkdir -p logs`)

	crdbNodes, workloadNode, kafkaNode := c.CRDBNodes(), c.Node(c.Spec().NodeCount), c.Node(c.Spec().NodeCount)
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

	c.Run(ctx, option.WithNodes(workloadNode), `./cockroach workload init bank {pgurl:1}`)
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
	_, err := newChangefeedCreator(db, db, t.L(), globalRand, "bank.bank", kafka.sinkURL(ctx), makeDefaultFeatureFlags()).
		With(options).
		Create()
	if err != nil {
		t.Fatal(err)
	}

	tc, err := kafka.newConsumer(ctx, "bank", nil /* stopper */)
	if err != nil {
		t.Fatal(errors.Wrap(err, "could not create kafka consumer"))
	}
	defer tc.close()

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
		err := c.RunE(ctx, option.WithNodes(workloadNode), `./cockroach workload run bank {pgurl:1} --max-rate=10`)
		if atomic.LoadInt64(&doneAtomic) > 0 {
			return nil
		}
		return errors.Wrap(err, "workload failed")
	})
	m.Go(func(ctx context.Context) error {
		defer workloadCancel()
		defer func() { close(messageBuf) }()
		v := cdctest.NewCountValidator(cdctest.NoOpValidator)
		for {
			m, err := tc.next(ctx)
			if err != nil {
				return err
			}
			messageBuf <- m
			updated, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
			if err != nil {
				return err
			}

			partitionStr := strconv.Itoa(int(m.Partition))
			if len(m.Key) > 0 {
				if err := v.NoteRow(partitionStr, string(m.Key), string(m.Value), updated, m.Topic); err != nil {
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
		baV, err := cdctest.NewBeforeAfterValidator(db, `bank.bank`, cdctest.ChangefeedOption{
			FullTableName: false,
			KeyInValue:    false,
			Format:        "json",
		})
		if err != nil {
			return err
		}
		validators := cdctest.Validators{
			cdctest.NewOrderValidator(`bank`),
			fprintV,
			baV,
		}
		v := cdctest.NewCountValidator(validators)

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
				if err := v.NoteRow(partitionStr, string(m.Key), string(m.Value), updated, m.Topic); err != nil {
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

				l.Printf("%s was spent validating this resolved timestamp: %s", timeutil.Since(noteResolvedStartTime), resolved)
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

type cdcCheckpointType int

const (
	cdcNormalCheckpoint cdcCheckpointType = iota
	cdcShutdownCheckpoint
)

// runCDCInitialScanRollingRestart runs multiple initial-scan-only changefeeds
// on a 4-node cluster, using node 1 as the coordinator and continuously
// restarting nodes 2-4 to hopefully force the changefeed to replan and exercise
// the checkpoint restore logic.
func runCDCInitialScanRollingRestart(
	ctx context.Context, t test.Test, c cluster.Cluster, checkpointType cdcCheckpointType,
) {
	startOpts := option.DefaultStartOpts()
	ips, err := c.ExternalIP(ctx, t.L(), c.Node(1))
	sinkURL := fmt.Sprintf("https://%s:%d", ips[0], debug.WebhookServerPort)
	sink := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	if err != nil {
		t.Fatal(err)
	}

	// We configure "racks" localities to push replicas off n1 later.
	racks := install.MakeClusterSettings(install.NumRacksOption(c.Spec().NodeCount))
	racks.Env = append(racks.Env, `COCKROACH_CHANGEFEED_TESTING_FAST_RETRY=true`)
	c.Start(ctx, t.L(), option.DefaultStartOpts(), racks)
	m := c.NewMonitor(ctx, c.All())

	restart := func(n int) error {
		cmd := fmt.Sprintf("./cockroach node drain --certs-dir=%s --port={pgport:%d} --self", install.CockroachNodeCertsDir, n)
		if err := c.RunE(ctx, option.WithNodes(c.Node(n)), cmd); err != nil {
			return err
		}
		m.ExpectDeath()
		c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(n))
		opts := startOpts
		opts.RoachprodOpts.IsRestart = true
		c.Start(ctx, t.L(), opts, racks, c.Node(n))
		m.ResetDeaths()
		return nil
	}

	// Restart n1 to shed any leases it still has.
	if err := restart(1); err != nil {
		t.Fatal(err)
	}

	db := c.Conn(ctx, t.L(), 1)

	// Setup a large table with 1M rows and a small table with 5 rows.
	// Keep ranges off n1 so that our plans use 2, 3, and 4.
	const (
		largeRowCount = 1000000
		smallRowCount = 5
	)
	t.L().Printf("setting up test data...")
	setupStmts := []string{
		`ALTER RANGE default CONFIGURE ZONE USING constraints = '[-rack=0]'`,
		fmt.Sprintf(`CREATE TABLE large (id PRIMARY KEY) AS SELECT generate_series(1, %d) id`, largeRowCount),
		`ALTER TABLE large SCATTER`,
		fmt.Sprintf(`CREATE TABLE small (id PRIMARY KEY) AS SELECT generate_series(%d, %d)`, largeRowCount+1, largeRowCount+smallRowCount),
		`ALTER TABLE small SCATTER`,
	}
	switch checkpointType {
	case cdcNormalCheckpoint:
		setupStmts = append(setupStmts,
			`SET CLUSTER SETTING changefeed.frontier_checkpoint_frequency = '1s'`,
			`SET CLUSTER SETTING changefeed.shutdown_checkpoint.enabled = 'false'`,
		)
	case cdcShutdownCheckpoint:
		const largeSplitCount = 5
		setupStmts = append(setupStmts,
			`SET CLUSTER SETTING changefeed.frontier_checkpoint_frequency = '0'`,
			`SET CLUSTER SETTING changefeed.shutdown_checkpoint.enabled = 'true'`,
			// Split some bigger chunks up to scatter it a bit more.
			fmt.Sprintf(`ALTER TABLE large SPLIT AT SELECT id FROM large ORDER BY random() LIMIT %d`, largeSplitCount/4),
			`ALTER TABLE large SCATTER`,
			// Finish splitting, so that drained ranges spread out evenly.
			fmt.Sprintf(`ALTER TABLE large SPLIT AT SELECT id FROM large ORDER BY random() LIMIT %d`, largeSplitCount),
			`ALTER TABLE large SCATTER`,
		)
	}
	for _, s := range setupStmts {
		t.L().Printf(s)
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	t.L().Printf("test data is setup")

	// Run the sink server.
	m.Go(func(ctx context.Context) error {
		t.L().Printf("starting up sink server at %s...", sinkURL)
		err := c.RunE(ctx, option.WithNodes(c.Node(1)), "./cockroach workload debug webhook-server")
		if err != nil {
			return err
		}
		t.L().Printf("sink server exited")
		return nil
	})

	// Restart nodes 2, 3, and 4 in a loop.
	stopRestarts := make(chan struct{})
	m.Go(func(ctx context.Context) error {
		defer func() {
			t.L().Printf("done restarting nodes")
		}()
		t.L().Printf("starting rolling drain+restarts of 2, 3, 4...")
		timer := time.NewTimer(0 * time.Second)
		for {
			for _, n := range []int{2, 3, 4} {
				select {
				case <-stopRestarts:
					return nil
				case <-ctx.Done():
					return nil
				case <-timer.C:
				}
				if err := restart(n); err != nil {
					return err
				}
				// We wait a bit between restarts so that the change aggregators have
				// a chance to make some progress.
				timer.Reset(10 * time.Second)
			}
		}
	})

	wait := make(chan struct{})

	// Run the changefeed, then ask the sink how many rows it saw.
	var unique int
	func() {
		defer close(wait)
		defer close(stopRestarts)
		defer func() {
			_, err := sink.Get(sinkURL + "/exit")
			t.L().Printf("exiting webhook sink status: %v", err)
		}()

		const numChangefeeds = 5
		for i := 0; i < numChangefeeds; i++ {
			t.L().Printf("starting changefeed %d...", i)
			var job int
			if err := db.QueryRow(
				fmt.Sprintf("CREATE CHANGEFEED FOR TABLE large, small INTO 'webhook-%s/?insecure_tls_skip_verify=true' WITH initial_scan='only'", sinkURL),
			).Scan(&job); err != nil {
				t.Fatal(err)
			}

			t.L().Printf("waiting for changefeed %d...", job)
			if _, err := db.ExecContext(ctx, "SHOW JOB WHEN COMPLETE $1", job); err != nil {
				t.Fatal(err)
			}

			t.L().Printf("changefeed complete, checking sink...")
			get := func(p string) (int, error) {
				b, err := sink.Get(sinkURL + p)
				if err != nil {
					return 0, err
				}
				body, err := io.ReadAll(b.Body)
				if err != nil {
					return 0, err
				}
				i, err := strconv.Atoi(string(body))
				if err != nil {
					return 0, err
				}
				return i, nil
			}
			unique, err = get("/unique")
			if err != nil {
				t.Fatal(err)
			}
			dupes, err := get("/dupes")
			if err != nil {
				t.Fatal(err)
			}
			t.L().Printf("sink got %d unique, %d dupes", unique, dupes)
			expected := largeRowCount + smallRowCount
			if unique != expected {
				t.Fatalf("expected %d, got %d", expected, unique)
			}
			_, err = sink.Get(sinkURL + "/reset")
			t.L().Printf("resetting sink %v", err)
		}
	}()

	<-wait
	// TODO(#116314)
	if runtime.GOOS != "darwin" {
		m.Wait()
	}
}

// This test verifies that the changefeed avro + confluent schema registry works
// end-to-end (including the schema registry default of requiring backward
// compatibility within a topic).
func runCDCSchemaRegistry(ctx context.Context, t test.Test, c cluster.Cluster) {
	crdbNodes, kafkaNode := c.Node(1), c.Node(1)
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), crdbNodes)
	kafka := kafkaManager{
		t:              t,
		c:              c,
		kafkaSinkNodes: kafkaNode,
	}
	kafka.install(ctx)
	kafka.start(ctx, "schema-registry")
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

	_, err := newChangefeedCreator(db, db, t.L(), globalRand, "foo", kafka.sinkURL(ctx), makeDefaultFeatureFlags()).
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
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(kafkaNode),
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
	crdbNodes, kafkaNode := c.CRDBNodes(), c.Node(c.Spec().NodeCount)
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), crdbNodes)

	kafka := kafkaManager{
		t:              t,
		c:              c,
		kafkaSinkNodes: kafkaNode,
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
		{
			"create changefeed with confluent-cloud scheme",
			fmt.Sprintf("%s&api_key=plain&api_secret=plain-secret", kafka.sinkURLAsConfluentCloudUrl(ctx)),
		},
	}

	for _, f := range feeds {
		t.Status(fmt.Sprintf("running:%s, query:%s", f.desc, f.queryArg))
		_, err := newChangefeedCreator(db, db, t.L(), globalRand, "auth_test_table", f.queryArg, makeDefaultFeatureFlags()).Create()
		if err != nil {
			t.Fatalf("%s: %s", f.desc, err.Error())
		}
	}
}

// runCDCMultipleSchemaChanges is a regression test for #136847.
func runCDCMultipleSchemaChanges(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())

	db := c.Conn(ctx, t.L(), 1)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")

	tableNames := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, tableName := range tableNames {
		createStmt := fmt.Sprintf(`CREATE TABLE %s (
	id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
	col TIMESTAMP(6) NULL
)`, tableName)
		sqlDB.Exec(t, createStmt)
	}

	var jobID int
	sqlDB.QueryRow(t,
		fmt.Sprintf("CREATE CHANGEFEED FOR %s INTO 'null://'", strings.Join(tableNames, ", ")),
	).Scan(&jobID)

	alterStmts := []string{"SET sql_safe_updates = false"}
	for _, tableName := range tableNames {
		alterStmts = append(alterStmts, fmt.Sprintf(`ALTER TABLE %s DROP col`, tableName))
	}
	sqlDB.ExecMultiple(t, alterStmts...)
	timeAfterSchemaChanges := timeutil.Now()

	t.L().Printf("waiting for changefeed highwater to pass %s", timeAfterSchemaChanges)
highwaterLoop:
	for {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(30 * time.Second):
			info, err := getChangefeedInfo(db, jobID)
			require.NoError(t, err)
			status := info.GetStatus()
			if status != "running" {
				errorStr := info.GetError()
				// Wait for job error to be populated.
				if errorStr == "" {
					t.L().Printf("changefeed status is %s instead of running, no error set yet", status)
					continue
				}
				t.Fatalf("changefeed status is %s instead of running: %s", status, errorStr)
			}
			hw := info.GetHighWater()
			if hw.After(timeAfterSchemaChanges) {
				break highwaterLoop
			}
			t.L().Printf("changefeed highwater is %s <= %s", hw, timeAfterSchemaChanges)
		}
	}
}

func registerCDC(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:      "cdc/initial-scan-only",
		Owner:     registry.OwnerCDC,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode(), spec.Arch(vm.ArchAMD64)),
		// This test uses google cloudStorageSink because it is the fastest,
		// but it is not a requirement for this test. The sink could be
		// chosen on a per cloud basis if we want to run this on other clouds.
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
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
			waitForCompletion := ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
			})
			waitForCompletion()

			exportStatsFile()
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/initial-scan-rolling-restart/normal-checkpoint",
		Owner:            registry.OwnerCDC,
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Timeout:          30 * time.Minute,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCInitialScanRollingRestart(ctx, t, c, cdcNormalCheckpoint)
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/initial-scan-rolling-restart/shutdown-checkpoint",
		Owner:            registry.OwnerCDC,
		Cluster:          r.MakeClusterSpec(4),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Timeout:          30 * time.Minute,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCInitialScanRollingRestart(ctx, t, c, cdcShutdownCheckpoint)
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/tpcc-1000/sink=kafka",
		Owner:            registry.OwnerCDC,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode(), spec.CPU(16)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 1000, duration: "120m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType: kafkaSink,
				targets:  allTpccTargets,
				kafkaArgs: kafkaFeedArgs{
					validateOrder: true,
				},
				opts: map[string]string{"initial_scan": "'no'"},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 3 * time.Minute,
				steadyLatency:      10 * time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/tpcc-1000/sink=msk",
		Owner:            registry.OwnerCDC,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode(), spec.CPU(16)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.OnlyAWS,
		Suites:           registry.ManualOnly,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 1000, duration: "60m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType: mskSink,
				targets:  allTpccTargets,
				opts: map[string]string{
					"initial_scan": "'no'",
					// updated is specified so that we can compare emitted bytes with
					// cdc/tpcc-1000/sink=kafka.
					"updated": "",
				},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 3 * time.Minute,
				steadyLatency:      10 * time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/tpcc-1000/sink=cloudstorage",
		Owner:            registry.OwnerCDC,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode(), spec.CPU(16)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 1000, duration: "120m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType: cloudStorageSink,
				targets:  allTpccTargets,
				opts: map[string]string{
					"initial_scan": "'no'",
					"diff":         "",
				},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 3 * time.Minute,
				steadyLatency:      10 * time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:      "cdc/initial-scan-only/parquet",
		Owner:     registry.OwnerCDC,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode(), spec.Arch(vm.ArchAMD64)),
		// This test uses google cloudStorageSink because it is the fastest,
		// but it is not a requirement for this test. The sink could be
		// chosen on a per cloud basis if we want to run this on other clouds.
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 200})

			feed := ct.newChangefeed(feedArgs{
				sinkType: cloudStorageSink,
				targets:  allTpccTargets,
				opts:     map[string]string{"initial_scan": "'only'", "format": "'parquet'"},
			})
			waitForCompletion := ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
			})
			waitForCompletion()

		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/initial-scan-only/parquet/metamorphic",
		Skip:             "#119295",
		Owner:            registry.OwnerCDC,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode(), spec.Arch(vm.ArchAMD64)),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			// Metamorphic testing runs two changefeeds,
			// Run the workload with 1 warehouse only to speed up the test.
			ct.runTPCCWorkload(tpccArgs{warehouses: 1})

			// Randomly select one table as changefeed target and skip other tables to
			// speed up the test.
			randomlySelectedIndex := getRandomIndex(len(allTpccTargets))
			selectedTargetTable := allTpccTargets[randomlySelectedIndex]
			trimmedTargetTable := strings.TrimPrefix(selectedTargetTable, `tpcc.`)

			firstFeed := ct.newChangefeed(feedArgs{
				sinkType: cloudStorageSink,
				targets:  []string{selectedTargetTable},
				opts:     map[string]string{"initial_scan": "'only'", "format": "'parquet'"},
			})
			firstFeed.waitForCompletion()

			secFeed := ct.newChangefeed(feedArgs{
				sinkType: cloudStorageSink,
				targets:  []string{selectedTargetTable},
				opts:     map[string]string{"initial_scan": "'only'", "format": "'parquet'"},
			})
			secFeed.waitForCompletion()

			db := c.Conn(context.Background(), t.L(), 1)
			sqlRunner := sqlutils.MakeSQLRunner(db)
			checkTwoChangeFeedExportContent(ctx, t, sqlRunner, firstFeed.sinkURI, secFeed.sinkURI, trimmedTargetTable)
		},
	})
	r.Add(registry.TestSpec{
		Name:      "cdc/tpcc-1000/sink=null",
		Owner:     registry.OwnerCDC,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:    registry.MetamorphicLeases,
		// TODO(radu): fix this.
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.ManualOnly,
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
		Name:             "cdc/initial-scan",
		Owner:            registry.OwnerCDC,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
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
		Name:             "cdc/kafka-chaos",
		Owner:            `cdc`,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 100, duration: "30m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType: kafkaSink,
				targets:  allTpccTargets,
				kafkaArgs: kafkaFeedArgs{
					kafkaChaos:    true,
					validateOrder: true,
				},
				opts: map[string]string{"initial_scan": "'no'"},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 3 * time.Minute,
				steadyLatency:      5 * time.Minute,
			})
			ct.waitForWorkload()
			ct.verifyMetrics(ctx, verifyMetricsNonZero("changefeed_network_bytes_in", "changefeed_network_bytes_out"))
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/kafka-chaos-single-row",
		Owner:            `cdc`,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			// Since this test fails with the v1 kafka sink, hardcode the v2 sink.
			_, err := ct.DB().ExecContext(ctx, `SET CLUSTER SETTING changefeed.new_kafka_sink.enabled = true;`)
			if err != nil {
				t.Fatal(err)
			}

			_, err = ct.DB().ExecContext(ctx, `CREATE TABLE t (id INT PRIMARY KEY, x INT);`)
			if err != nil {
				t.Fatal("failed to create table")
			}
			_, err = ct.DB().ExecContext(ctx, `INSERT INTO t VALUES (1, -1);`)
			if err != nil {
				t.Fatal("failed to insert row into table")
			}

			feed := ct.newChangefeed(feedArgs{
				sinkType: kafkaSink,
				targets:  []string{"t"},
				kafkaArgs: kafkaFeedArgs{
					kafkaChaos:    true,
					validateOrder: true,
				},
				opts: map[string]string{
					"updated":                       "",
					"initial_scan":                  "'no'",
					"min_checkpoint_frequency":      "'3s'",
					"protect_data_from_gc_on_pause": "",
					"on_error":                      "pause",
					"kafka_sink_config":             `'{"Flush": {"MaxMessages": 100, "Frequency": "1s","Messages": 100 }, "Version": "2.7.2", "RequiredAcks": "ALL","Compression": "GZIP"}'`,
				},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				steadyLatency: 5 * time.Minute,
			})

			conn1, err := ct.DB().Conn(ctx)
			if err != nil {
				t.Fatalf("failed to create a conn: %s", err)
			}
			defer func() {
				_ = conn1.Close()
			}()
			conn2, err := ct.DB().Conn(ctx)
			if err != nil {
				t.Fatalf("failed to create a conn: %s", err)
			}
			defer func() {
				_ = conn2.Close()
			}()

			const testDuration = 30 * time.Minute
			// Repeatedly update a single row in a table in order to create a large
			// number of events with the same key that will span multiple batches.
			for start, i := timeutil.Now(), 0; i < 1000000 && timeutil.Since(start) < testDuration; i++ {
				stmt := fmt.Sprintf(`UPDATE t SET x = %d WHERE id = 1;`, i)
				if i%2 == 0 {
					_, err = conn1.ExecContext(ctx, stmt)
				} else {
					_, err = conn2.ExecContext(ctx, stmt)
				}
				if err != nil {
					t.Fatalf("failed to execute stmt %q: %s", stmt, err)
				}
			}
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/crdb-chaos",
		Owner:            `cdc`,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runTPCCWorkload(tpccArgs{warehouses: 100, duration: "30m", tolerateErrors: true})

			feed := ct.newChangefeed(feedArgs{
				sinkType: kafkaSink,
				targets:  allTpccTargets,
				kafkaArgs: kafkaFeedArgs{
					validateOrder: true,
				},
				opts:           map[string]string{"initial_scan": "'no'"},
				tolerateErrors: true,
			})

			// Restart nodes after starting the changefeed so that we avoid trying to
			// start the feed on a down node.
			ct.startCRDBChaos()

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
		Benchmark:                  true,
		Cluster:                    r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:                     registry.MetamorphicLeases,
		CompatibleClouds:           registry.AllExceptAWS,
		Suites:                     registry.Suites(registry.Nightly),
		RequiresDeprecatedWorkload: true, // uses ledger
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			ct.runLedgerWorkload(ledgerArgs{duration: "28m"})

			alterStmt := "ALTER DATABASE ledger CONFIGURE ZONE USING range_max_bytes = 805306368, range_min_bytes = 134217728"
			_, err := ct.DB().ExecContext(ctx, alterStmt)
			if err != nil {
				t.Fatalf("failed statement %q: %s", alterStmt, err.Error())
			}

			feed := ct.newChangefeed(feedArgs{
				sinkType: kafkaSink,
				targets:  allLedgerTargets,
				kafkaArgs: kafkaFeedArgs{
					validateOrder: true,
				},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 10 * time.Minute,
				steadyLatency:      time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/cloud-sink-gcs/rangefeed=true",
		Owner:            `cdc`,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
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
			ct.verifyMetrics(ctx, verifyMetricsNonZero("cloud_read_bytes", "cloud_write_bytes"))
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/pubsub-sink",
		Owner:            `cdc`,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			// The deprecated pubsub sink is unable to handle the throughput required for 100 warehouses
			if _, err := ct.DB().Exec("SET CLUSTER SETTING changefeed.new_pubsub_sink_enabled = true;"); err != nil {
				ct.t.Fatal(err)
			}
			ct.runTPCCWorkload(tpccArgs{warehouses: 100, duration: "30m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType: pubsubSink,
				targets:  allTpccTargets,
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
				steadyLatency:      time.Minute,
			})

			ct.waitForWorkload()

			ct.verifyMetrics(ctx, verifyMetricsNonZero("changefeed_network_bytes_in", "changefeed_network_bytes_out"))
		},
	})

	// In order to run this test, the service account corresponding to the
	// implicit credentials must have the Service Account Token Creator role on
	// the first account on the assume-role chain:
	// cdc-roachtest-intermediate@cockroach-ephemeral.iam.gserviceaccount.com. See
	// https://cloud.google.com/iam/docs/create-short-lived-credentials-direct.
	r.Add(registry.TestSpec{
		Name:             "cdc/pubsub-sink/assume-role",
		Owner:            `cdc`,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			// Bump memory limit (too low in 22.2)
			if _, err := ct.DB().Exec("SET CLUSTER SETTING changefeed.memory.per_changefeed_limit = '512MiB';"); err != nil {
				ct.t.Fatal(err)
			}

			ct.runTPCCWorkload(tpccArgs{warehouses: 1, duration: "5m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType:   pubsubSink,
				assumeRole: "cdc-roachtest-intermediate@cockroach-ephemeral.iam.gserviceaccount.com,cdc-roachtest@cockroach-ephemeral.iam.gserviceaccount.com",
				targets:    allTpccTargets,
				opts:       map[string]string{"initial_scan": "'no'"},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 5 * time.Minute,
				steadyLatency:      time.Minute,
			})

			ct.waitForWorkload()

			ct.verifyMetrics(ctx, verifyMetricsNonZero("changefeed_network_bytes_in", "changefeed_network_bytes_out"))
		},
	})

	// In order to run this test, the service account corresponding to the
	// implicit credentials must have the Service Account Token Creator role on
	// the first account on the assume-role chain:
	// cdc-roachtest-intermediate@cockroach-ephemeral.iam.gserviceaccount.com. See
	// https://cloud.google.com/iam/docs/create-short-lived-credentials-direct.
	r.Add(registry.TestSpec{
		Name:             "cdc/cloud-sink-gcs/assume-role",
		Owner:            `cdc`,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			// Bump memory limit (too low in 22.2)
			if _, err := ct.DB().Exec("SET CLUSTER SETTING changefeed.memory.per_changefeed_limit = '512MiB';"); err != nil {
				ct.t.Fatal(err)
			}

			ct.runTPCCWorkload(tpccArgs{warehouses: 50, duration: "5m"})

			feed := ct.newChangefeed(feedArgs{
				sinkType:   cloudStorageSink,
				assumeRole: "cdc-roachtest-intermediate@cockroach-ephemeral.iam.gserviceaccount.com,cdc-roachtest@cockroach-ephemeral.iam.gserviceaccount.com",
				targets:    allTpccTargets,
				opts:       map[string]string{"initial_scan": "'no'"},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 5 * time.Minute,
				steadyLatency:      time.Minute,
			})
			ct.waitForWorkload()
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/webhook-sink",
		Owner:            `cdc`,
		Benchmark:        true,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(16), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			// Consider an installation failure to be a flake which is out of
			// our control. This should be rare.
			err := c.Install(ctx, t.L(), ct.sinkNodes, "go")
			if err != nil {
				t.Skip(err)
			}

			ct.runTPCCWorkload(tpccArgs{warehouses: 100, duration: "30m"})

			// The deprecated webhook sink is unable to handle the throughput required for 100 warehouses
			if _, err := ct.DB().Exec("SET CLUSTER SETTING changefeed.new_webhook_sink_enabled = true;"); err != nil {
				ct.t.Fatal(err)
			}

			feed := ct.newChangefeed(feedArgs{
				sinkType: webhookSink,
				targets:  allTpccTargets,
				opts: map[string]string{
					"metrics_label":       "'webhook'",
					"webhook_sink_config": `'{"Flush": { "Messages": 100, "Frequency": "5s" } }'`,
				},
			})

			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 30 * time.Minute,
			})

			ct.waitForWorkload()

			ct.verifyMetrics(ctx, verifyMetricsNonZero("changefeed_network_bytes_in", "changefeed_network_bytes_out"))
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/kafka-auth",
		Owner:            `cdc`,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCKafkaAuth(ctx, t, c)
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/kafka-auth-msk",
		Owner:            registry.OwnerCDC,
		Cluster:          r.MakeClusterSpec(1, spec.Arch(vm.ArchAMD64)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.OnlyAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.All())
			mskMgr := newMSKManager(ctx, t)
			mskMgr.MakeCluster(ctx)
			defer mskMgr.TearDown()

			t.Status("waiting for msk cluster to be active")
			waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			defer cancel()
			brokers := mskMgr.WaitForClusterActiveAndDNSUpdated(waitCtx, c)
			t.Status("cluster is active")
			mskMgr.CreateTopic(ctx, "auth_test_table", c)

			db := c.Conn(ctx, t.L(), 1)
			defer stopFeeds(db)

			tdb := sqlutils.MakeSQLRunner(db)
			tdb.Exec(t, `CREATE TABLE auth_test_table (a INT PRIMARY KEY)`)

			t.L().Printf("creating changefeed with iam: %s", brokers.connectURI)
			_, err := newChangefeedCreator(db, db, t.L(), globalRand, "auth_test_table", brokers.connectURI, makeDefaultFeatureFlags()).Create()
			if err != nil {
				t.Fatalf("creating changefeed: %v", err)
			}
		},
	})
	r.Add(registry.TestSpec{
		Name:      "cdc/kafka-oauth",
		Owner:     `cdc`,
		Benchmark: true,
		// Only Kafka 3 supports Arm64, but the broker setup for Oauth used only works with Kafka 2
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode(), spec.Arch(vm.ArchAMD64)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Cloud() == spec.Local && runtime.GOARCH == "arm64" {
				// N.B. We have to skip locally since amd64 emulation may not be available everywhere.
				t.L().PrintfCtx(ctx, "Skipping test under ARM64")
				return
			}

			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			// Run tpcc workload for tiny bit.  Roachtest monitor does not
			// like when there are no tasks that were started with the monitor
			// (This can be removed once #108530 resolved).
			ct.runTPCCWorkload(tpccArgs{warehouses: 1, duration: "30s"})

			kafkaNode := ct.sinkNodes
			kafka := kafkaManager{
				t:              ct.t,
				c:              ct.cluster,
				kafkaSinkNodes: kafkaNode,
				mon:            ct.mon,
				useKafka2:      true, // The broker-side oauth configuration used only works with Kafka 2
			}
			kafka.install(ct.ctx)

			creds, kafkaEnv := kafka.configureOauth(ct.ctx)

			kafka.start(ctx, "kafka", kafkaEnv)

			feed := ct.newChangefeed(feedArgs{
				sinkType:        kafkaSink,
				sinkURIOverride: kafka.sinkURLOAuth(ct.ctx, creds),
				targets:         allTpccTargets,
				opts:            map[string]string{"initial_scan": "'only'"},
			})

			feed.waitForCompletion()
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/kafka-topics",
		Owner:            `cdc`,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode(), spec.Arch(vm.ArchAMD64)),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()

			// cdc/kafka-topics tests that sarama clients only fetches metadata for
			// topics that changefeeds need but not for all topics on the kafka
			// cluster. The test verifies the work by 1. creating lots of random kafka
			// topics on the kafka cluster 2. running some tpcc workload with a
			// changefeed configured to watch all tpcc tables (note that cdc creates
			// kafka topics for every target tables internally) 3. assert that
			// changefeed only fetches metadata for tpcc tables but not for other
			// random topics created in 1.

			// Run minimal level of tpcc workload and changefeed.
			ct.runTPCCWorkload(tpccArgs{warehouses: 1, duration: "30s"})

			kafka, cleanup := setupKafka(ctx, t, c, c.Node(c.Spec().NodeCount))
			defer cleanup()

			db := c.Conn(ctx, t.L(), 1)
			defer stopFeeds(db)
			const ignoreTopicPrefix = "ignore_topic_do_not_fetch"

			// Create random topics on kafka cluster.
			t.Status("creating kafka topics")
			for i := 0; i < 100; i++ {
				if err := kafka.createTopic(ctx, ignoreTopicPrefix+fmt.Sprintf("%d", i)); err != nil {
					t.Fatal(err)
				}
			}

			// Wait for workload to complete to make sure the tables are created and some writes to the table are done.
			ct.waitForWorkload()

			// Run initial_scan on all tpcc tables.
			feed := ct.newChangefeed(feedArgs{
				sinkType: kafkaSink,
				targets:  allTpccTargets,
				opts:     map[string]string{"initial_scan": "'only'"},
			})
			feed.waitForCompletion()

			testutils.SucceedsWithin(t, func() error {
				// Check logs on cockroach nodes (skip the last node running workload
				// and kafka). We are looking for logs that begin with "client/metadata
				// fetching metadata for". This is a log line outputted from
				// sarama/client.go.tryRefreshMetadata
				// https://github.com/IBM/sarama/blob/fd84c2b0f0185100dbaec28ca4074289b35cc1b1/client.go#L1023-L1027
				// which tells us what topics are being fetched for metadata. We expect
				// to see logs for tpcc tables but not for random topics.
				//
				// We now also check for similar logs from the kafka v2 sink &
				// kgo. There isn't a direct equivalent, however, so check a few
				// things.
				logSearchStr := `(client/metadata fetching metadata for|updating kafka metadata for topics|fetching metadata to learn its partitions|waiting for metadata for new topic)`
				results, checkLogsErr := ct.cluster.RunWithDetails(ct.ctx, t.L(),
					option.WithNodes(ct.cluster.Range(1, c.Spec().NodeCount-1)),
					fmt.Sprintf(`grep -E "%s" logs/cockroach.log`, logSearchStr))
				if checkLogsErr != nil {
					t.Fatal(checkLogsErr)
				}

				hasUnexpectedMetadata := func(str string) error {
					// We do not expect to see fetching metadata for any topics with
					// ignoreTopicPrefix.
					if strings.Contains(str, ignoreTopicPrefix) {
						return errors.Newf("did not expect to fetch metadata for %s", ignoreTopicPrefix)
					}

					// We do not expect to see fetching metadata for all topics.
					if strings.Contains(str, "all topics") || strings.Contains(str, "updating kafka metadata for topics: []") {
						return errors.New("did not expect to fetch metadata for all topics")
					}
					return nil
				}

				hasTpccTargets := func(str string) (bool, bool) {
					hasSomeTpccTargets := false
					hasAllTpccTargets := true
					for _, target := range allTpccTargets {
						trimmedTargetName := strings.TrimPrefix(target, `tpcc.`)
						// Make sure we are actually fetching metadata for tpcc topics.
						if strings.Contains(str, trimmedTargetName) {
							hasSomeTpccTargets = true
						} else {
							hasAllTpccTargets = false
						}
					}

					return hasSomeTpccTargets, hasAllTpccTargets
				}

				hasExpectedTargets := false
				var err error
				for i, res := range results {
					hasSomeTpccTargets, hasAllTpccTargets := hasTpccTargets(res.Stdout)
					if hasAllTpccTargets {
						hasExpectedTargets = true
					}
					if hasSomeTpccTargets && !hasAllTpccTargets {
						// We expect that nodes either fetch metadata for all tpcc tables or none.
						err = errors.CombineErrors(err,
							errors.Newf("node %d fetched metadata for some tpcc tables but not all", i+1))
					}

					if checkForAllNodes := hasUnexpectedMetadata(res.Stdout); checkForAllNodes != nil {
						// We do not expect any nodes to fetch metadata for topics with ignoreTopicPrefix.
						err = errors.CombineErrors(err, errors.Wrapf(checkForAllNodes, "node %d", i+1))
					}
				}
				if !hasExpectedTargets {
					// Note that we are not sure which node is running changefeeds, so we
					// assert that at least one node fetches metadata for all tpcc tables.
					err = errors.CombineErrors(err,
						errors.New("expected fetching metadata for tpcc tables on at least one node but did not find any"))
				}
				return err
			}, time.Minute)
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/kafka-azure",
		Owner:            `cdc`,
		CompatibleClouds: registry.OnlyAzure,
		// The Azure CLI only packages AMD64 binaries in its deb installer, so lock to AMD64.
		Cluster: r.MakeClusterSpec(2, spec.WorkloadNode(), spec.Arch(vm.ArchAMD64)),
		Leases:  registry.MetamorphicLeases,
		Suites:  registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			ct := newCDCTester(ctx, t, c)
			defer ct.Close()
			// Just use 1 warehouse and no initial scan since this would involve
			// cross-cloud traffic which is far more expensive.  The throughput also
			// can't be too high to not hit the Throughput Unit (TU) limit of 1MBps/TU
			ct.runTPCCWorkload(tpccArgs{warehouses: 1, duration: "30m"})
			feed := ct.newChangefeed(feedArgs{
				sinkType: azureEventHubKafkaSink,
				targets:  allTpccTargets,
				opts:     map[string]string{"initial_scan": "'no'"},
			})
			ct.runFeedLatencyVerifier(feed, latencyTargets{
				initialScanLatency: 3 * time.Minute,
				steadyLatency:      10 * time.Minute,
			})
			ct.waitForWorkload()
			ct.verifyMetrics(ctx, verifyMetricsNonZero("changefeed_network_bytes_in", "changefeed_network_bytes_out"))
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/bank",
		Owner:            `cdc`,
		Cluster:          r.MakeClusterSpec(4, spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Timeout:          60 * time.Minute,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCBank(ctx, t, c)
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/schemareg",
		Owner:            `cdc`,
		Cluster:          r.MakeClusterSpec(1),
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runCDCSchemaRegistry(ctx, t, c)
		},
	})
	r.Add(registry.TestSpec{
		Name:             "cdc/multiple-schema-changes",
		Owner:            registry.OwnerCDC,
		Benchmark:        false,
		Cluster:          r.MakeClusterSpec(3, spec.CPU(16)),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Nightly),
		Timeout:          1 * time.Hour,
		Run:              runCDCMultipleSchemaChanges,
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
	CACert   string
	CAKey    string
	SinkCert string
	SinkKey  string
}

func (t *testCerts) CACertBase64() string {
	return base64.StdEncoding.EncodeToString([]byte(t.CACert))
}

func makeTestCerts(sinkNodeIP string, dnsNames ...string) (*testCerts, error) {
	CAKey, err := rsa.GenerateKey(cryptorand.Reader, keyLength)
	if err != nil {
		return nil, errors.Wrap(err, "CA private key")
	}

	SinkKey, err := rsa.GenerateKey(cryptorand.Reader, keyLength)
	if err != nil {
		return nil, errors.Wrap(err, "sink private key")
	}

	CACert, CACertSpec, err := generateCACert(CAKey)
	if err != nil {
		return nil, errors.Wrap(err, "CA cert gen")
	}

	SinkCert, err := generateSinkCert(sinkNodeIP, SinkKey, CACertSpec, CAKey, dnsNames...)
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

	SinkKeyPEM, err := pemEncodePrivateKey(SinkKey)
	if err != nil {
		return nil, errors.Wrap(err, "pem encode sink key")
	}

	SinkCertPEM, err := pemEncodeCert(SinkCert)
	if err != nil {
		return nil, errors.Wrap(err, "pem encode sink cert")
	}

	return &testCerts{
		CACert:   CACertPEM,
		CAKey:    CAKeyPEM,
		SinkCert: SinkCertPEM,
		SinkKey:  SinkKeyPEM,
	}, nil
}

func generateSinkCert(
	sinkIP string,
	priv *rsa.PrivateKey,
	CACert *x509.Certificate,
	CAKey *rsa.PrivateKey,
	dnsNames ...string,
) ([]byte, error) {
	ip := net.ParseIP(sinkIP)
	if ip == nil {
		return nil, fmt.Errorf("invalid IP address: %s", sinkIP)
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
			CommonName:         "sink-node",
		},
		NotBefore:   timeutil.Now(),
		NotAfter:    timeutil.Now().Add(certLifetime),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDataEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageKeyAgreement,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    append([]string{"localhost"}, dnsNames...),
		IPAddresses: []net.IP{ip},
	}
	return x509.CreateCertificate(cryptorand.Reader, certSpec, CACert, &priv.PublicKey, CAKey)
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
	cert, err := x509.CreateCertificate(cryptorand.Reader, certSpec, certSpec, &priv.PublicKey, priv)
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
	ret, err := cryptorand.Int(cryptorand.Reader, limit)
	if err != nil {
		return nil, errors.Wrap(err, "generate random serial")
	}
	return ret, nil
}

var nextWebhookPort = 3001 // 3000 is used by grafana

var webhookServerScript = func(port int) string {
	return fmt.Sprintf(`
package main

import (
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})
	log.Fatal(http.ListenAndServeTLS(":%d",  "/home/ubuntu/cert.pem",  "/home/ubuntu/key.pem", nil))
}
`, port)
}

var hydraServerStartScript = `
export SECRETS_SYSTEM=arbitrarySystemSecret
export OAUTH2_ISSUER_URL=http://localhost:4444
export OAUTH2_CONSENT_URL=http://localhost:3000/consent
export OAUTH2_LOGIN_URL=http://localhost:3000/login
export OIDC_SUBJECT_IDENTIFIERS_SUPPORTED_TYPES=public,pairwise
export OIDC_SUBJECT_IDENTIFIERS_PAIRWISE_SALT=arbitraryPairwiseSalt
export SERVE_COOKIES_SAME_SITE_MODE=Lax
export HYDRA_ADMIN_URL=http://localhost:4445
export DSN=memory

./hydra serve all --dev
`

var installAzureCliScript = `
sudo apt-get update && \
sudo apt-get install -y ca-certificates curl apt-transport-https lsb-release gnupg && \
sudo mkdir -p /etc/apt/keyrings && \
curl -sLS https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor | sudo tee /etc/apt/keyrings/microsoft.gpg > /dev/null && \
sudo chmod go+r /etc/apt/keyrings/microsoft.gpg && \
AZ_DIST=$(lsb_release -cs) && \
echo "deb [arch=\"dpkg --print-architecture\" signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ $AZ_DIST main" | sudo tee /etc/apt/sources.list.d/azure-cli.list && \
sudo apt-get update && \
sudo apt-get install -y azure-cli
`

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

	// Requires formatting with the node's internal IP
	kafkaOauthConfigTmpl = `
sasl.enabled.mechanisms=OAUTHBEARER
sasl.mechanism.inter.broker.protocol=OAUTHBEARER
security.inter.broker.protocol=SASL_PLAINTEXT
listeners=PLAINTEXT://:9092,SASL_PLAINTEXT://:9095
advertised.listeners=PLAINTEXT://%[1]s:9092,SASL_PLAINTEXT://%[1]s:9095

listener.name.sasl_plaintext.oauthbearer.sasl.login.callback.handler.class=br.com.jairsjunior.security.oauthbearer.OauthAuthenticateLoginCallbackHandler
listener.name.sasl_plaintext.oauthbearer.sasl.server.callback.handler.class=br.com.jairsjunior.security.oauthbearer.OauthAuthenticateValidatorCallbackHandler

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

	kafkaOauthJAASConfig = `
KafkaServer {
org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
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
	t              test.Test
	c              cluster.Cluster
	kafkaSinkNodes option.NodeListOption
	mon            cluster.Monitor

	// Our method of requiring OAuth on the broker only works with Kafka 2
	useKafka2 bool

	// validateOrder specifies whether consumers created by the
	// kafkaManager should create and use order validators.
	validateOrder bool
}

func (k kafkaManager) basePath() string {
	if k.c.IsLocal() {
		return `/tmp/confluent`
	}
	return `/mnt/data1/confluent`
}

func (k kafkaManager) confluentInstallBase() string {
	if k.useKafka2 {
		return "confluent-6.1.0"
	} else {
		return "confluent-7.4.0"
	}
}

func (k kafkaManager) confluentDownloadScript() string {
	var downloadURL string
	var downloadSHA string
	if k.useKafka2 {
		downloadURL = "https://storage.googleapis.com/cockroach-test-artifacts/confluent/confluent-community-6.1.0.tar.gz"
		downloadSHA = "53b0e2f08c4cfc55087fa5c9120a614ef04d306db6ec3bcd7710f89f05355355"
	} else {
		downloadURL = "https://packages.confluent.io/archive/7.4/confluent-community-7.4.0.tar.gz"
		downloadSHA = "cc3066e9b55c211664c6fb9314c553521a0cb0d5b78d163e74480bdc60256d75"
	}

	// Confluent CLI Versions 3 and above do not support a local schema registry,
	// and while confluent-7.4.0 does include a cli with a schema-registry it
	// requires logging in to Confluent Cloud, so instead the latest 2.x cli
	// version is used.
	confluentCLIVersion := "2.38.1"
	confluentCLIDownloadURLBase := "https://s3-us-west-2.amazonaws.com/confluent.cloud/confluent-cli/archives"

	return fmt.Sprintf(`#!/usr/bin/env bash
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
    aarch64)
      echo "arm64"
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
    linux/arm64)
      CONFLUENT_CLI_URL="${CONFLUENT_CLI_URL_BASE}/${CONFLUENT_CLI_VERSION}/confluent_v${CONFLUENT_CLI_VERSION}_linux_arm64.tar.gz"
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
`, downloadURL, downloadSHA, k.confluentInstallBase(), confluentCLIVersion, confluentCLIDownloadURLBase)
}

func (k kafkaManager) confluentHome() string {
	return filepath.Join(k.basePath(), k.confluentInstallBase())
}

func (k kafkaManager) configDir() string {
	return filepath.Join(k.basePath(), k.confluentInstallBase(), "etc/kafka")
}

func (k kafkaManager) binDir() string {
	return filepath.Join(k.basePath(), k.confluentInstallBase(), "bin")
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

	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), `mkdir -p `+folder)

	downloadScriptPath := filepath.Join(folder, "install.sh")
	downloadScript := k.confluentDownloadScript()
	err := k.c.PutString(ctx, downloadScript, downloadScriptPath, 0700, k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), downloadScriptPath, folder)
	if !k.c.IsLocal() {
		k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), `mkdir -p logs`)
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
		err := k.c.RunE(ctx, option.WithNodes(k.kafkaSinkNodes), `sudo apt-get -q update 2>&1 > logs/apt-get-update.log`)
		if err != nil {
			return err
		}
		return k.c.RunE(ctx, option.WithNodes(k.kafkaSinkNodes), `sudo DEBIAN_FRONTEND=noninteractive apt-get -yq --no-install-recommends install openssl default-jre maven 2>&1 > logs/apt-get-install.log`)
	})
}

// installAzureCli installs azure cli on the kafka node which is necessary for
// retrieving endpoint connection string for the roachtest azure event hub.
func (k kafkaManager) installAzureCli(ctx context.Context) error {
	k.t.Status("installing azure cli")
	retryOpts := retry.Options{
		InitialBackoff: 1 * time.Minute,
		MaxBackoff:     5 * time.Minute,
	}
	return retry.WithMaxAttempts(ctx, retryOpts, 3, func() error {
		return k.c.RunE(ctx, option.WithNodes(k.kafkaSinkNodes), installAzureCliScript)
	})
}

// getAzureEventHubAccess retrieves the Azure Event Hub access key name and key
// for the cdc-roachtest event hub set up in the CRL Azure account for roachtest
// testing.
func (k kafkaManager) getAzureEventHubAccess(ctx context.Context) (string, string, error) {
	// The necessary credential env vars have been added to TeamCity agents by
	// dev-inf. Note that running this test on roachprod would not work due to
	// lacking the required credentials env vars set up.
	azureClientID := os.Getenv("AZURE_CLIENT_ID")
	azureClientSecret := os.Getenv("AZURE_CLIENT_SECRET")
	azureSubscriptionID := os.Getenv("AZURE_SUBSCRIPTION_ID")
	azureTenantID := os.Getenv("AZURE_TENANT_ID")

	k.t.Status("getting azure event hub connection string")
	// az login --service-principal -t <Tenant-ID> -u <Client-ID> -p=<Client-secret>
	cmdStr := fmt.Sprintf("az login --service-principal -t %s -u %s -p=%s", azureTenantID, azureClientID, azureClientSecret)
	_, err := k.c.RunWithDetailsSingleNode(ctx, k.t.L(), option.WithNodes(k.kafkaSinkNodes), cmdStr)
	if err != nil {
		return "", "", errors.Wrap(err, "error running `az login`")
	}

	cmdStr = fmt.Sprintf("az account set --subscription %s", azureSubscriptionID)
	_, err = k.c.RunWithDetailsSingleNode(ctx, k.t.L(), option.WithNodes(k.kafkaSinkNodes), cmdStr)
	if err != nil {
		return "", "", errors.Wrap(err, "error running `az account set` command")
	}

	cmdStr = "az eventhubs namespace authorization-rule keys list --name cdc-roachtest-auth-rule " +
		"--namespace-name cdc-roachtest --resource-group e2e-infra-event-hub-rg"
	results, err := k.c.RunWithDetailsSingleNode(ctx, k.t.L(), option.WithNodes(k.kafkaSinkNodes), cmdStr)
	if err != nil {
		return "", "", errors.Wrap(err, "error running `az eventhubs` command")
	}

	var keys AuthorizationRuleKeys
	err = json.Unmarshal([]byte(results.Stdout), &keys)
	if err != nil {
		return "", "", errors.Wrap(err, "error unmarshalling az eventhubs keys")
	}

	return keys.KeyName, keys.PrimaryKey, nil
}

func (k kafkaManager) runWithRetry(ctx context.Context, cmd string) {
	retryOpts := retry.Options{
		InitialBackoff: 1 * time.Minute,
		MaxBackoff:     5 * time.Minute,
	}
	err := retry.WithMaxAttempts(ctx, retryOpts, 3, func() error {
		return k.c.RunE(ctx, option.WithNodes(k.kafkaSinkNodes), cmd)
	})
	if err != nil {
		k.t.Fatal(err)
	}
}

func (k kafkaManager) configureHydraOauth(ctx context.Context) (string, string) {
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), `rm -rf /home/ubuntu/hydra`)
	k.runWithRetry(ctx, `bash <(curl https://raw.githubusercontent.com/ory/meta/master/install.sh) -d -b . hydra v2.0.3`)

	err := k.c.PutString(ctx, hydraServerStartScript, "/home/ubuntu/hydra-serve.sh", 0700, k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	mon := k.c.NewMonitor(ctx, k.kafkaSinkNodes)
	mon.Go(func(ctx context.Context) error {
		err := k.c.RunE(ctx, option.WithNodes(k.kafkaSinkNodes), `/home/ubuntu/hydra-serve.sh`)
		return errors.Wrap(err, "hydra failed")
	})

	var result install.RunResultDetails
	// The admin server may not be up immediately, so retry the create command
	// until it succeeds or times out.

	err = retry.ForDuration(hydraRetryDuration, func() error {
		result, err = k.c.RunWithDetailsSingleNode(ctx, k.t.L(), option.WithNodes(k.kafkaSinkNodes), "/home/ubuntu/hydra create oauth2-client",
			"-e", "http://localhost:4445",
			"--grant-type", "client_credentials",
			"--token-endpoint-auth-method", "client_secret_basic",
			"--name", `"Test Client"`,
		)
		return err
	})
	if err != nil {
		k.t.Fatal(err)
	}

	hydraClientRegexp, err := regexp.Compile(`CLIENT ID\t([^\s]+)\t\nCLIENT SECRET\t([^\s]+)`)
	if err != nil {
		k.t.Fatal(err)
	}
	matches := hydraClientRegexp.FindAllStringSubmatch(result.Stdout, -1)[0]
	clientID := matches[1]
	clientSecret := matches[2]

	return clientID, clientSecret
}

func (k kafkaManager) configureOauth(ctx context.Context) (clientcredentials.Config, string) {
	configDir := k.configDir()

	ips, err := k.c.InternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	nodeIP := ips[0]

	kafkaOauthConfig := fmt.Sprintf(kafkaOauthConfigTmpl, nodeIP)
	kafkaConfigPath := filepath.Join(configDir, "server.properties")
	kafkaJAASPath := filepath.Join(configDir, "server_jaas.conf")
	k.PutConfigContent(ctx, kafkaOauthConfig, kafkaConfigPath)
	k.PutConfigContent(ctx, kafkaOauthJAASConfig, kafkaJAASPath)

	// In order to run Kafka with OAuth a custom implementation of certain Java
	// classes has to be provided.
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), `rm -rf /home/ubuntu/kafka-oauth`)
	k.runWithRetry(ctx, `git clone https://github.com/jairsjunior/kafka-oauth.git /home/ubuntu/kafka-oauth`)
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), `(cd /home/ubuntu/kafka-oauth; git checkout c2b307548ef944d3fbe899b453d24e1fc8380add; mvn package)`)

	// CLASSPATH allows Kafka to load in the custom implementation
	kafkaEnv := "CLASSPATH='/home/ubuntu/kafka-oauth/target/*'"

	// Hydra is used as an open source OAuth server
	clientID, clientSecret := k.configureHydraOauth(ctx)
	tokenURL := fmt.Sprintf("http://%s:4444/oauth2/token", nodeIP)

	authHeader := `Basic ` + base64.StdEncoding.EncodeToString([]byte(
		fmt.Sprintf(`%s:%s`, clientID, clientSecret),
	))

	// Env parameters for the kafka-oauth classes
	kafkaEnv += " OAUTH_WITH_SSL=false"
	kafkaEnv += " OAUTH_LOGIN_SERVER='127.0.0.1:4444'"
	kafkaEnv += " OAUTH_LOGIN_ENDPOINT='/oauth2/token'"
	kafkaEnv += " OAUTH_LOGIN_GRANT_TYPE='client_credentials'"
	kafkaEnv += " OAUTH_LOGIN_SCOPE=''"
	kafkaEnv += fmt.Sprintf(" OAUTH_AUTHORIZATION='%s'", authHeader)
	kafkaEnv += " OAUTH_INTROSPECT_SERVER='127.0.0.1:4445'"
	kafkaEnv += " OAUTH_INTROSPECT_ENDPOINT='/admin/oauth2/introspect'"
	kafkaEnv += fmt.Sprintf(" OAUTH_INTROSPECT_AUTHORIZATION='%s'", authHeader)

	credentials := clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     tokenURL,
	}

	k.t.Status("configured oauth credentials: %v", credentials)

	return credentials, kafkaEnv
}

func (k kafkaManager) configureAuth(ctx context.Context) *testCerts {
	k.t.Status("generating TLS certificates")
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	kafkaIP := ips[0]

	details, err := k.c.RunWithDetailsSingleNode(ctx, k.t.L(), option.WithNodes(k.kafkaSinkNodes), "hostname", "-f")
	if err != nil {
		k.t.Fatal(err)
	}
	hostname := strings.TrimSpace(details.Stdout)
	k.t.L().Printf("hostname included in TLS certificates: %s", hostname)
	testCerts, err := makeTestCerts(kafkaIP, hostname)
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

	k.PutConfigContent(ctx, testCerts.SinkKey, kafkaKeyPath)
	k.PutConfigContent(ctx, testCerts.SinkCert, kafkaCertPath)
	k.PutConfigContent(ctx, testCerts.CAKey, caKeyPath)
	k.PutConfigContent(ctx, testCerts.CACert, caCertPath)
	k.PutConfigContent(ctx, kafkaConfig, kafkaConfigPath)
	k.PutConfigContent(ctx, kafkaJAASConfig, kafkaJAASPath)

	k.t.Status("constructing java keystores")
	// Convert PEM cert and key into pkcs12 bundle so that it can be imported into a java keystore.
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes),
		fmt.Sprintf("openssl pkcs12 -export -in %s -inkey %s -name kafka -out %s -password pass:%s",
			kafkaCertPath,
			kafkaKeyPath,
			kafkaBundlePath,
			keystorePassword))

	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), fmt.Sprintf("rm -f %s", keystorePath))
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), fmt.Sprintf("rm -f %s", truststorePath))

	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes),
		fmt.Sprintf("keytool -importkeystore -deststorepass %s -destkeystore %s -srckeystore %s -srcstoretype PKCS12 -srcstorepass %s -alias kafka",
			keystorePassword,
			keystorePath,
			kafkaBundlePath,
			keystorePassword))
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes),
		fmt.Sprintf("keytool -keystore %s -alias CAroot -importcert -file %s -no-prompt -storepass %s",
			truststorePath,
			caCertPath,
			keystorePassword))
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes),
		fmt.Sprintf("keytool -keystore %s -alias CAroot -importcert -file %s -no-prompt -storepass %s",
			keystorePath,
			caCertPath,
			keystorePassword))

	return testCerts
}

func (k kafkaManager) PutConfigContent(ctx context.Context, data string, path string) {
	err := k.c.PutString(ctx, data, path, 0600, k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
}

func (k kafkaManager) addSCRAMUsers(ctx context.Context) {
	k.t.Status("adding entries for SASL/SCRAM users")
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), filepath.Join(k.binDir(), "kafka-configs"),
		"--zookeeper", "localhost:2181",
		"--alter",
		"--add-config", "SCRAM-SHA-512=[password=scram512-secret]",
		"--entity-type", "users",
		"--entity-name", "scram512")

	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), filepath.Join(k.binDir(), "kafka-configs"),
		"--zookeeper", "localhost:2181",
		"--alter",
		"--add-config", "SCRAM-SHA-256=[password=scram256-secret]",
		"--entity-type", "users",
		"--entity-name", "scram256")
}

func (k kafkaManager) start(ctx context.Context, service string, envVars ...string) {
	// This isn't necessary for the nightly tests, but it's nice for iteration.
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), k.makeCommand("confluent", "local destroy || true"))
	k.restart(ctx, service, envVars...)
	// Wait for kafka to be ready. Otherwise we can sometimes try to connect to
	// it too fast which fails the test.
	k.waitForKafkaAvailable(ctx)
}

func (k kafkaManager) waitForKafkaAvailable(ctx context.Context) {
	k.t.Status("waiting for kafka to be ready")
	// Note: this command will retry the connection itself for some seconds, so
	// we don't need to explicitly retry here.
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), filepath.Join(k.binDir(), "kafka-topics"),
		"--bootstrap-server=localhost:9092", "--list")
}

var kafkaServices = map[string][]string{
	"zookeeper":       {"zookeeper"},
	"kafka":           {"zookeeper", "kafka"},
	"schema-registry": {"zookeeper", "kafka", "schema-registry"},
}

func (k kafkaManager) restart(ctx context.Context, targetService string, envVars ...string) {
	services := kafkaServices[targetService]

	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), "touch", k.serverJAASConfig())
	for _, svcName := range services {
		// The confluent tool applies the KAFKA_OPTS to all
		// services. Also, the kafka.logs.dir is used by each
		// service, despite the name.
		opts := fmt.Sprintf("-Djava.security.auth.login.config=%s -Dkafka.logs.dir=%s",
			k.serverJAASConfig(),
			fmt.Sprintf("logs/%s", svcName),
		)

		startCmd := fmt.Sprintf("CONFLUENT_CURRENT=%s CONFLUENT_HOME=%s KAFKA_OPTS='%s' %s",
			k.basePath(),
			k.confluentHome(),
			opts,
			strings.Join(envVars, " "),
		)
		startCmd += fmt.Sprintf(" %s local services %s start", k.confluentBin(), svcName)

		// Sometimes kafka wants to be difficult and not start back up first try. Give it some time.
		k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes).WithRetryOpts(retry.Options{
			InitialBackoff: 5 * time.Second,
			MaxBackoff:     30 * time.Second,
			MaxRetries:     30,
		}).WithShouldRetryFn(func(*install.RunResultDetails) bool { return true }), startCmd)
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
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), fmt.Sprintf("rm -f %s", k.serverJAASConfig()))
	k.c.Run(ctx, option.WithNodes(k.kafkaSinkNodes), k.makeCommand("confluent", "local services stop"))
}

func (k kafkaManager) chaosLoop(
	ctx context.Context, period, downTime time.Duration, stopper <-chan struct{},
) error {
	t := time.NewTicker(period)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-stopper:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}

		k.t.L().Printf("kafka chaos loop iteration %d: stopping", i)
		k.stop(ctx)

		select {
		case <-stopper:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(downTime):
		}

		k.t.L().Printf("kafka chaos loop iteration %d: restarting", i)
		k.restart(ctx, "kafka")
	}
}

func (k kafkaManager) sinkURL(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `kafka://` + ips[0] + `:9092`
}

func (k kafkaManager) sinkURLTLS(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `kafka://` + ips[0] + `:9093`
}

func (k kafkaManager) sinkURLSASL(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `kafka://` + ips[0] + `:9094`
}

// sinkURLAsConfluentCloudUrl allows the test to connect to the kafka brokers
// as if it was connecting to kafka hosted in confluent cloud.
func (k kafkaManager) sinkURLAsConfluentCloudUrl(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	// Confluent cloud does not use TLS 1.2 and instead uses PLAIN username/password
	// authentication (see https://docs.confluent.io/platform/current/security/security_tutorial.html#overview).
	// Because the kafka manager has certs configured, connecting without a ca_cert will raise an error.
	// To connect without a cert, we set insecure_tls_skip_verify=true.
	return `confluent-cloud://` + ips[0] + `:9094?insecure_tls_skip_verify=true`
}

func (k kafkaManager) sinkURLOAuth(ctx context.Context, creds clientcredentials.Config) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	kafkaURI, err := url.Parse("kafka://" + ips[0] + `:9095`)
	if err != nil {
		k.t.Fatal(err)
	}

	encodedSecret := base64.StdEncoding.EncodeToString([]byte(creds.ClientSecret))

	params := kafkaURI.Query()
	params.Set(changefeedbase.SinkParamSASLEnabled, "true")
	params.Set(changefeedbase.SinkParamSASLMechanism, "OAUTHBEARER")
	params.Set(changefeedbase.SinkParamSASLClientID, creds.ClientID)
	params.Set(changefeedbase.SinkParamSASLClientSecret, encodedSecret)
	params.Set(changefeedbase.SinkParamSASLTokenURL, creds.TokenURL)
	kafkaURI.RawQuery = params.Encode()
	return kafkaURI.String()
}

func (k kafkaManager) advertiseURLs(ctx context.Context) []string {
	ips, err := k.c.ExternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	for i := range ips {
		ips[i] += `:9092`
	}
	return ips
}

func (k kafkaManager) consumerURL(ctx context.Context) string {
	ips, err := k.c.ExternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return ips[0] + `:9092`
}

func (k kafkaManager) schemaRegistryURL(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.t.L(), k.kafkaSinkNodes)
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

func (k kafkaManager) newConsumer(
	ctx context.Context, topic string, stopper <-chan struct{},
) (*topicConsumer, error) {
	kafkaAddrs := []string{k.consumerURL(ctx)}
	config := sarama.NewConfig()
	// I was seeing "error processing FetchRequest: kafka: error decoding
	// packet: unknown magic byte (2)" errors which
	// https://github.com/IBM/sarama/issues/962 identifies as the
	// consumer's fetch size being less than the "max.message.bytes" that
	// kafka is configured with. Kafka notes that this is required in
	// https://kafka.apache.org/documentation.html#upgrade_11_message_format
	config.Consumer.Fetch.Default = 1000012
	consumer, err := sarama.NewConsumer(kafkaAddrs, config)
	if err != nil {
		return nil, err
	}
	var validator cdctest.Validator
	if k.validateOrder {
		validator = cdctest.NewOrderValidator(topic)
	}
	tc, err := newTopicConsumer(k.t, consumer, topic, validator, stopper)
	if err != nil {
		_ = consumer.Close()
		return nil, err
	}
	k.t.L().Printf("topic consumer for %s has partitions: %s", topic, tc.partitions)
	return tc, nil
}

func (k kafkaManager) startTopicConsumers(
	ctx context.Context, targets []string, stopper <-chan struct{},
) error {
	for _, target := range targets {
		// We need to strip off the database and schema name since the topic name
		// is just the table name.
		topic := target
		lastPeriodIndex := strings.LastIndex(target, ".")
		if lastPeriodIndex != -1 {
			topic = topic[lastPeriodIndex+1:]
		}

		k.t.L().Printf("starting topic consumer for topic: %s", topic)
		k.mon.Go(func(ctx context.Context) error {
			topicConsumer, err := k.newConsumer(ctx, topic, stopper)
			if err != nil {
				return err
			}
			defer topicConsumer.close()
			everyN := roachtestutil.Every(30 * time.Second)
			for {
				select {
				case <-stopper:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				// The topicConsumer has order validation built-in so this has
				// the side effect of validating the order of incoming messages.
				_, err := topicConsumer.next(ctx)
				if err != nil {
					k.t.L().Printf("topic consumer for %s encountered error: %s", topic, err)
					return err
				}
				if everyN.ShouldLog() {
					k.t.L().Printf("topic consumer for %s validated %d rows and %d resolved timestamps",
						topic, topicConsumer.validator.NumRows, topicConsumer.validator.NumResolved)
				}
			}
		})
	}

	return nil
}

type tpccWorkload struct {
	workloadNodes      option.NodeListOption
	sqlNodes           option.NodeListOption
	tpccWarehouseCount int
	tolerateErrors     bool
	conns              int
	noWait             bool
}

func (tw *tpccWorkload) install(ctx context.Context, c cluster.Cluster) {
	// For fixtures import, use the version built into the cockroach binary so
	// the tpcc workload-versions match on release branches.
	c.Run(ctx, option.WithNodes(tw.workloadNodes), fmt.Sprintf(
		`./cockroach workload fixtures import tpcc --warehouses=%d --checks=false {pgurl%s}`,
		tw.tpccWarehouseCount,
		tw.sqlNodes.RandNode(),
	))
}

func (tw *tpccWorkload) run(ctx context.Context, c cluster.Cluster, workloadDuration string) {
	var cmd bytes.Buffer
	fmt.Fprintf(&cmd, "./cockroach workload run tpcc --warehouses=%d --duration=%s ", tw.tpccWarehouseCount, workloadDuration)
	if tw.tolerateErrors {
		cmd.WriteString("--tolerate-errors ")
	}
	if tw.conns > 0 {
		fmt.Fprintf(&cmd, " --conns=%d ", tw.conns)
	}
	if tw.noWait {
		cmd.WriteString("--wait=0 ")
	}
	fmt.Fprintf(&cmd, "{pgurl%s}", tw.sqlNodes)

	c.Run(ctx, option.WithNodes(tw.workloadNodes), cmd.String())
}

type ledgerWorkload struct {
	workloadNodes option.NodeListOption
	sqlNodes      option.NodeListOption
}

func (lw *ledgerWorkload) install(ctx context.Context, c cluster.Cluster) {
	// TODO(#94136): remove --data-loader=INSERT
	c.Run(ctx, option.WithNodes(lw.workloadNodes.RandNode()), fmt.Sprintf(
		`./workload init ledger --data-loader=INSERT {pgurl%s}`,
		lw.sqlNodes.RandNode(),
	))
}

func (lw *ledgerWorkload) run(ctx context.Context, c cluster.Cluster, workloadDuration string) {
	// TODO(#94136): remove --data-loader=INSERT
	c.Run(ctx, option.WithNodes(lw.workloadNodes), fmt.Sprintf(
		`./workload run ledger --data-loader=INSERT --mix=balance=0,withdrawal=50,deposit=50,reversal=0 {pgurl%s} --duration=%s`,
		lw.sqlNodes,
		workloadDuration,
	))
}

// changefeedCreator wraps the process of creating a changefeed with
// different options and sinks
type changefeedCreator struct {
	db              *gosql.DB
	systemDB        *gosql.DB
	logger          *logger.Logger
	targets         string
	sinkURL         string
	options         map[string]string
	extraArgs       []interface{}
	flags           cdcFeatureFlags
	rng             entropy
	settingsApplied bool
}

func newChangefeedCreator(
	db, systemDB *gosql.DB,
	logger *logger.Logger,
	r *rand.Rand,
	targets, sinkURL string,
	flags cdcFeatureFlags,
) *changefeedCreator {
	return &changefeedCreator{
		db:       db,
		systemDB: systemDB,
		logger:   logger,
		targets:  targets,
		sinkURL:  sinkURL,
		options:  make(map[string]string),
		flags:    flags,
		rng:      entropy{Rand: r},
	}
}

// With adds options to the changefeed being created. If a non-zero
// `value` is passed in one of the options, the option will be passed
// as {option}={value}.
func (cfc *changefeedCreator) With(opts map[string]string) *changefeedCreator {
	for opt, value := range opts {
		cfc.options[opt] = value
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

func chooseDistributionStrategy(r entropy) string {
	vals := changefeedccl.RangeDistributionStrategy.GetAvailableValues()
	return vals[r.Intn(len(vals))]
}

// applySettings aplies various settings to the cluster -- once per the
// lifetime of changefeedCreator
func (cfc *changefeedCreator) applySettings() error {
	if cfc.settingsApplied {
		return nil
	}
	// kv.rangefeed.enabled is required for changefeeds to run
	if _, err := cfc.systemDB.Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
		return err
	}

	schedEnabled := cfc.flags.RangeFeedScheduler.enabled(cfc.rng)
	if schedEnabled != featureUnset {
		cfc.logger.Printf("Setting kv.rangefeed.scheduler.enabled to %t", schedEnabled == featureEnabled)
		if _, err := cfc.systemDB.Exec(
			"SET CLUSTER SETTING kv.rangefeed.scheduler.enabled = $1", schedEnabled == featureEnabled,
		); err != nil {
			return err
		}
	}

	rangeDistribution, rangeDistributionEnabled := cfc.flags.DistributionStrategy.enabled(cfc.rng,
		chooseDistributionStrategy)
	if rangeDistributionEnabled == featureEnabled {
		cfc.logger.Printf("Setting changefeed.default_range_distribution_strategy to %s", rangeDistribution)
		if _, err := cfc.db.Exec(fmt.Sprintf(
			"SET CLUSTER SETTING changefeed.default_range_distribution_strategy = '%s'", rangeDistribution)); err != nil {
			return err
		}
	}

	return nil
}

// Create builds the SQL statement that creates the changefeed job,
// and executes it. Returns the job ID corresponding to the
// changefeed, and any errors that occurred in the process
func (cfc *changefeedCreator) Create() (int, error) {
	if err := cfc.applySettings(); err != nil {
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
	*jobRecord

	startedTime   time.Time
	statementTime time.Time
}

var _ jobInfo = (*changefeedInfo)(nil)

func getChangefeedInfo(db *gosql.DB, jobID int) (*changefeedInfo, error) {
	jr, err := getJobRecord(db, jobID)
	if err != nil {
		return nil, err
	}
	return &changefeedInfo{
		jobRecord:     jr,
		startedTime:   time.UnixMicro(jr.payload.StartedMicros),
		statementTime: jr.payload.GetChangefeed().StatementTime.GoTime(),
	}, nil
}

// stopFeeds cancels any running feeds on the cluster. Not necessary for the
// nightly, but nice for development.
func stopFeeds(db *gosql.DB) {
	_, _ = db.Exec(`CANCEL JOBS (
			SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running'
		)`)
}

// setupKafka installs Kafka on the cluster and configures it so that
// the test runner can connect to it. Returns a function to be called
// at the end of the test for stopping Kafka.
func setupKafka(
	ctx context.Context, t test.Test, c cluster.Cluster, nodes option.NodeListOption,
) (kafkaManager, func()) {
	kafka := kafkaManager{
		t:              t,
		c:              c,
		kafkaSinkNodes: nodes,
	}

	kafka.install(ctx)
	if !c.IsLocal() {
		// TODO(dan): This test currently connects to kafka from the test
		// runner, so kafka needs to advertise the external address. Better
		// would be a binary we could run on one of the roachprod machines.
		urls := kafka.advertiseURLs(ctx)
		// Setup multiple kafkas.
		for i, url := range urls {
			c.Run(ctx, option.WithNodes([]int{[]int(kafka.kafkaSinkNodes)[i]}), `echo "advertised.listeners=PLAINTEXT://`+url+`" >> `+
				filepath.Join(kafka.configDir(), "server.properties"))
			c.Run(ctx, option.WithNodes([]int{[]int(kafka.kafkaSinkNodes)[i]}), `echo "broker.id=`+strconv.Itoa(i)+`" >> `+
				filepath.Join(kafka.configDir(), "server.properties"))
			// Default num partitions = num nodes.
			c.Run(ctx, option.WithNodes([]int{[]int(kafka.kafkaSinkNodes)[i]}), `echo "num.partitions=`+strconv.Itoa(len(urls))+`" >> `+
				filepath.Join(kafka.configDir(), "server.properties"))
			// Use the zookeeper on first kafka node, and ignore the rest of them (if any).
			if i > 0 {
				zkUrl := strings.Replace(urls[0], ":9092", ":2181", 1)
				c.Run(ctx, option.WithNodes([]int{[]int(kafka.kafkaSinkNodes)[i]}), `echo "zookeeper.connect=`+zkUrl+`" >> `+
					filepath.Join(kafka.configDir(), "server.properties"))
			}
		}
	}

	kafka.start(ctx, "kafka")
	kafka.waitForKafkaAvailable(ctx)
	return kafka, func() { kafka.stop(ctx) }
}

const mskRoleArn = "arn:aws:iam::541263489771:role/roachprod-msk-full-access" // See pkg/roachprod/vm/aws/terraform/iam.tf
const mskRegion = "us-east-2"

type mskManager struct {
	t         test.Test
	mskClient *msk.Client
	// awsOpts lets us set the region and logger for each aws-sdk request.
	awsOpts func(*msk.Options)

	clusterArn  string
	connectInfo mskIAMConnectInfo
}

func newMSKManager(ctx context.Context, t test.Test) *mskManager {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithDefaultRegion(mskRegion))
	if err != nil {
		t.Fatalf("failed to load aws config: %v", err)
	}
	mskClient := msk.NewFromConfig(awsCfg)

	return &mskManager{
		t:         t,
		mskClient: mskClient,
		awsOpts: func(o *msk.Options) {
			o.Logger = awslog.LoggerFunc(func(classification awslog.Classification, format string, v ...interface{}) {
				format = fmt.Sprintf("msk(%v): %s", classification, format)
				t.L().Printf(format, v...)
			})
			o.Region = mskRegion
		}}
}

func (m *mskManager) getAWSRegion() roachprodaws.AWSRegion {
	var region roachprodaws.AWSRegion
	for _, r := range roachprodaws.DefaultConfig.Regions {
		if r.Name == mskRegion {
			region = r
			break
		}
	}
	if region.Name == "" {
		m.t.Fatalf("failed to find region %s in roachprodaws.DefaultConfig.Regions", mskRegion)
	}
	return region
}

// MakeCluster creates a new MSK Serverless cluster.
func (m *mskManager) MakeCluster(ctx context.Context) {
	clusterName := fmt.Sprintf("roachtest-cdc-%v", timeutil.Now().Format("2006-01-02-15-04-05"))
	clusterTags := map[string]string{"roachtest": "true"}

	region := m.getAWSRegion()
	subnets := make([]string, 0, 3)
	for _, az := range region.AvailabilityZones {
		subnets = append(subnets, az.SubnetID)
	}

	req := &msk.CreateClusterV2Input{
		ClusterName: aws.String(clusterName),
		Tags:        clusterTags,
		Serverless: &msktypes.ServerlessRequest{
			VpcConfigs: []msktypes.VpcConfig{
				{
					SubnetIds:        subnets,
					SecurityGroupIds: []string{region.SecurityGroup},
				},
			},
			ClientAuthentication: &msktypes.ServerlessClientAuthentication{
				Sasl: &msktypes.ServerlessSasl{Iam: &msktypes.Iam{Enabled: aws.Bool(true)}},
			},
		},
	}
	resp, err := m.mskClient.CreateClusterV2(ctx, req, m.awsOpts)
	if err != nil {
		m.t.Fatalf("failed to create msk serverless cluster: %v", err)
	}
	m.clusterArn = *resp.ClusterArn
}

type mskIAMConnectInfo struct {
	broker     string
	connectURI string
}

// WaitForClusterActiveAndDNSUpdated waits for the MSK cluster to become active and to be available via DNS.
func (m *mskManager) WaitForClusterActiveAndDNSUpdated(
	ctx context.Context, c cluster.Cluster,
) mskIAMConnectInfo {
	for ctx.Err() == nil {
		resp, err := m.mskClient.DescribeClusterV2(ctx, &msk.DescribeClusterV2Input{ClusterArn: &m.clusterArn}, m.awsOpts)
		if err != nil {
			m.t.Fatalf("failed to describe msk cluster: %v", err)
		}
		if resp.ClusterInfo.State == msktypes.ClusterStateActive {
			break
		}
		select {
		case <-ctx.Done():
			m.t.Fatalf("timed out waiting for msk cluster to become active")
		case <-time.After(5 * time.Second):
		}
	}

	resp, err := m.mskClient.GetBootstrapBrokers(ctx, &msk.GetBootstrapBrokersInput{ClusterArn: &m.clusterArn}, m.awsOpts)
	if err != nil {
		m.t.Fatalf("failed to describe msk cluster: %v", err)
	}
	broker := *resp.BootstrapBrokerStringSaslIam
	connectInfo := mskIAMConnectInfo{
		broker:     broker,
		connectURI: fmt.Sprintf("kafka://%s?tls_enabled=true&sasl_enabled=true&sasl_mechanism=AWS_MSK_IAM&sasl_aws_region=%s&sasl_aws_iam_role_arn=%s&sasl_aws_iam_session_name=cdc-msk", broker, mskRegion, mskRoleArn),
	}

	// Wait for the broker's DNS to propagate.
	m.t.L().Printf("waiting for dns to resolve for %s", connectInfo.broker)
	host := strings.Split(connectInfo.broker, ":")[0]
	for ctx.Err() == nil {
		if err := c.RunE(ctx, option.WithNodes(c.All()), "nslookup", host); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			m.t.Fatalf("timed out waiting for msk dns to resolve")
		case <-time.After(5 * time.Second):
		}
	}

	m.connectInfo = connectInfo
	return connectInfo
}

//go:embed create-msk-topic/main.go.txt
var createMskTopicMain string

const createMSKTopicBinPath = "/tmp/create-msk-topic"

var setupMskTopicScript = fmt.Sprintf(`
#!/bin/bash
set -e -o pipefail
wget https://go.dev/dl/go1.22.8.linux-amd64.tar.gz -O /tmp/go.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf /tmp/go.tar.gz
echo export PATH=$PATH:/usr/local/go/bin >> ~/.profile
source ~/.profile

cd %s
rm -f go.mod go.sum
go mod init create-msk-topic
go mod tidy
go build .

./create-msk-topic --broker "$1" --topic "$2" --role-arn "$3"
`, createMSKTopicBinPath)

// CreateTopic creates a topic on the MSK cluster.
func (m *mskManager) CreateTopic(ctx context.Context, topic string, c cluster.Cluster) {
	createTopicNode := c.Node(1)
	withCTN := option.WithNodes(createTopicNode)

	require.NoError(m.t, c.RunE(ctx, withCTN, "mkdir", "-p", createMSKTopicBinPath))
	require.NoError(m.t, c.PutString(ctx, createMskTopicMain, path.Join(createMSKTopicBinPath, "main.go"), 0700, createTopicNode))
	require.NoError(m.t, c.PutString(ctx, setupMskTopicScript, path.Join(createMSKTopicBinPath, "run.sh"), 0700, createTopicNode))
	require.NoError(m.t, c.RunE(ctx, withCTN, path.Join(createMSKTopicBinPath, "run.sh"), m.connectInfo.broker, topic, mskRoleArn))
}

// TearDown deletes the MSK cluster.
func (m *mskManager) TearDown() {
	_, err := m.mskClient.DeleteCluster(context.Background(), &msk.DeleteClusterInput{ClusterArn: &m.clusterArn}, m.awsOpts)
	if err != nil {
		m.t.Fatalf("failed to delete msk cluster: %v", err)
	}
}

type topicConsumer struct {
	t                  test.Test
	consumer           sarama.Consumer
	topic              string
	partitions         []string
	partitionConsumers map[int32]sarama.PartitionConsumer
	validator          *cdctest.CountValidator
	stopper            <-chan struct{}
}

func newTopicConsumer(
	t test.Test,
	consumer sarama.Consumer,
	topic string,
	validator cdctest.Validator,
	stopper <-chan struct{},
) (*topicConsumer, error) {
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return nil, err
	}

	numPartitions := len(partitions)
	topicPartitions := make([]string, 0, numPartitions)
	partitionConsumers := make(map[int32]sarama.PartitionConsumer, numPartitions)
	for _, partition := range partitions {
		topicPartitions = append(topicPartitions, strconv.Itoa(int(partition)))
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return nil, err
		}
		partitionConsumers[partition] = pc
	}

	if validator == nil {
		validator = cdctest.NoOpValidator
	}
	countValidator := cdctest.NewCountValidator(validator)

	return &topicConsumer{
		t:                  t,
		consumer:           consumer,
		topic:              topic,
		partitions:         topicPartitions,
		partitionConsumers: partitionConsumers,
		validator:          countValidator,
		stopper:            stopper,
	}, nil
}

func (c *topicConsumer) tryNextMessage(ctx context.Context) (*sarama.ConsumerMessage, error) {
	for partition, pc := range c.partitionConsumers {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case m := <-pc.Messages():
			if m == nil {
				return m, nil
			}
			if err := c.validateMessage(partition, m); err != nil {
				return nil, err
			}
			return m, nil
		default:
		}
	}
	return nil, nil
}

func (c *topicConsumer) validateMessage(partition int32, m *sarama.ConsumerMessage) error {
	updated, resolved, err := cdctest.ParseJSONValueTimestamps(m.Value)
	if err != nil {
		return err
	}
	partitionStr := strconv.Itoa(int(partition))
	switch {
	case len(m.Key) == 0:
		err := c.validator.NoteResolved(partitionStr, resolved)
		if err != nil {
			return err
		}
	default:
		err := c.validator.NoteRow(partitionStr, string(m.Key), string(m.Value), updated, m.Topic)
		if err != nil {
			return err
		}
	}
	if failures := c.validator.Failures(); len(failures) > 0 {
		c.t.Fatalf("topic consumer for %s encountered validator error(s): %s", c.topic, strings.Join(failures, ","))
	}
	return nil
}

func (c *topicConsumer) next(ctx context.Context) (*sarama.ConsumerMessage, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.stopper:
			return nil, nil
		default:
		}
		m, err := c.tryNextMessage(ctx)
		if err != nil {
			return nil, err
		}
		if m != nil {
			return m, nil
		}
	}
}

func (c *topicConsumer) close() {
	for _, pc := range c.partitionConsumers {
		pc.AsyncClose()
		// Drain the messages and errors as required by AsyncClose.
		for range pc.Messages() {
		}
		for range pc.Errors() {
		}
	}
	_ = c.consumer.Close()
}

// verifyMetricsNonZero returns a check function for runMetricsVerifier that
// checks that the metrics matching the names input are > 0.
func verifyMetricsNonZero(names ...string) func(metrics map[string]*prompb.MetricFamily) (ok bool) {
	namesMap := make(map[string]struct{}, len(names))
	for _, name := range names {
		namesMap[name] = struct{}{}
	}

	return func(metrics map[string]*prompb.MetricFamily) (ok bool) {
		found := map[string]struct{}{}

		for name, fam := range metrics {
			if _, ok := namesMap[name]; !ok {
				continue
			}

			for _, m := range fam.Metric {
				if m.Counter.GetValue() > 0 {
					found[name] = struct{}{}
				}
			}

			if len(found) == len(names) {
				return true
			}
		}
		return false
	}
}
