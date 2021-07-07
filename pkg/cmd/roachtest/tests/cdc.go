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
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

type workloadType string

const (
	tpccWorkloadType   workloadType = "tpcc"
	ledgerWorkloadType workloadType = "ledger"
)

type sinkType int32

const (
	cloudStorageSink sinkType = iota + 1
	webhookSink
)

type cdcTestArgs struct {
	workloadType       workloadType
	tpccWarehouseCount int
	workloadDuration   string
	initialScan        bool
	kafkaChaos         bool
	crdbChaos          bool
	whichSink          sinkType
	sinkURI            string

	// preStartStatements are executed after the workload is initialized but before the
	// changefeed is created.
	preStartStatements []string

	targetInitialScanLatency time.Duration
	targetSteadyLatency      time.Duration
	targetTxnPerSecond       float64
}

func cdcBasicTest(ctx context.Context, t test.Test, c cluster.Cluster, args cdcTestArgs) {
	crdbNodes := c.Range(1, c.Spec().NodeCount-1)
	workloadNode := c.Node(c.Spec().NodeCount)
	kafkaNode := c.Node(c.Spec().NodeCount)
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)
	c.Start(ctx, crdbNodes)

	db := c.Conn(ctx, 1)
	defer stopFeeds(db)
	if _, err := db.Exec(`SET CLUSTER SETTING kv.rangefeed.enabled = true`); err != nil {
		t.Fatal(err)
	}
	kafka := kafkaManager{
		t:     t,
		c:     c,
		nodes: kafkaNode,
	}

	var sinkURI string
	if args.sinkURI != "" {
		sinkURI = args.sinkURI
	} else if args.whichSink == cloudStorageSink {
		ts := timeutil.Now().Format(`20060102150405`)
		// cockroach-tmp is a multi-region bucket with a TTL to clean up old
		// data.
		sinkURI = `experimental-gs://cockroach-tmp/roachtest/` + ts + "?AUTH=implicit"
	} else if args.whichSink == webhookSink {
		// setup a sample cert for use by the mock sink
		cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
		if err != nil {
			t.Fatal(err)
		}
		sinkDest, err := cdctest.StartMockWebhookSink(cert)
		if err != nil {
			t.Fatal(err)
		}

		sinkDestHost, err := url.Parse(sinkDest.URL())
		if err != nil {
			t.Fatal(err)
		}

		params := sinkDestHost.Query()
		params.Set(changefeedbase.SinkParamCACert, certEncoded)
		sinkDestHost.RawQuery = params.Encode()

		sinkURI = fmt.Sprintf("webhook-%s", sinkDestHost.String())
	} else {
		t.Status("installing kafka")
		kafka.install(ctx)
		kafka.start(ctx)
		sinkURI = kafka.sinkURL(ctx)
	}

	m := c.NewMonitor(ctx, crdbNodes)
	workloadCompleteCh := make(chan struct{}, 1)

	workloadStart := timeutil.Now()
	if args.workloadType == tpccWorkloadType {
		t.Status("installing TPCC")
		tpcc := tpccWorkload{
			sqlNodes:           crdbNodes,
			workloadNodes:      workloadNode,
			tpccWarehouseCount: args.tpccWarehouseCount,
			// TolerateErrors if crdbChaos is true; otherwise, the workload will fail
			// if it attempts to use the node which was brought down by chaos.
			tolerateErrors: args.crdbChaos,
		}

		tpcc.install(ctx, c)
		// TODO(dan,ajwerner): sleeping momentarily before running the workload
		// mitigates errors like "error in newOrder: missing stock row" from tpcc.
		time.Sleep(2 * time.Second)
		t.Status("initiating workload")
		m.Go(func(ctx context.Context) error {
			defer func() { close(workloadCompleteCh) }()
			tpcc.run(ctx, c, args.workloadDuration)
			return nil
		})
	} else {
		t.Status("installing Ledger Workload")
		lw := ledgerWorkload{
			sqlNodes:      crdbNodes,
			workloadNodes: workloadNode,
		}
		lw.install(ctx, c)

		t.Status("initiating workload")
		m.Go(func(ctx context.Context) error {
			defer func() { close(workloadCompleteCh) }()
			lw.run(ctx, c, args.workloadDuration)
			return nil
		})
	}

	changefeedLogger, err := t.L().ChildLogger("changefeed")
	if err != nil {
		t.Fatal(err)
	}
	defer changefeedLogger.Close()
	verifier := makeLatencyVerifier(
		args.targetInitialScanLatency,
		args.targetSteadyLatency,
		changefeedLogger,
		t.Status,
		args.crdbChaos,
	)
	defer verifier.maybeLogLatencyHist()

	m.Go(func(ctx context.Context) error {
		// Some of the tests have a tight enough bound on targetSteadyLatency
		// that the default for kv.closed_timestamp.target_duration means the
		// changefeed is never considered sufficiently caught up. We could
		// instead make targetSteadyLatency less aggressive, but it'd be nice to
		// keep it where it is.
		//
		// TODO(ssd): As of 797819b35f5 this is actually increasing rather than decreasing
		// the closed_timestamp.target_duration. We can probably remove this. However,
		// as of 2021-04-20, we want to understand why this test has started failing more often
		// before changing this.
		if _, err := db.Exec(
			`SET CLUSTER SETTING kv.closed_timestamp.target_duration='10s'`,
		); err != nil {
			t.Fatal(err)
		}

		// With a target_duration of 10s, we won't see slow span logs from changefeeds untils we are > 100s
		// behind, which is well above the 60s targetSteadyLatency we have in some tests.
		if _, err := db.Exec(
			`SET CLUSTER SETTING changefeed.slow_span_log_threshold='30s'`,
		); err != nil {
			// We don't hard fail here because, not all versions support this setting
			t.L().Printf("failed to set cluster setting: %s", err)
		}

		for _, stmt := range args.preStartStatements {
			_, err := db.ExecContext(ctx, stmt)
			if err != nil {
				t.Fatalf("failed pre-start statement %q: %s", stmt, err.Error())
			}
		}

		var targets string
		if args.workloadType == tpccWorkloadType {
			targets = `tpcc.warehouse, tpcc.district, tpcc.customer, tpcc.history,
			tpcc.order, tpcc.new_order, tpcc.item, tpcc.stock,
			tpcc.order_line`
		} else {
			targets = `ledger.customer, ledger.transaction, ledger.entry, ledger.session`
		}

		jobID, err := createChangefeed(db, targets, sinkURI, args)
		if err != nil {
			return err
		}

		info, err := getChangefeedInfo(db, jobID)
		if err != nil {
			return err
		}
		verifier.statementTime = info.statementTime
		changefeedLogger.Printf("started changefeed at (%d) %s\n",
			verifier.statementTime.UnixNano(), verifier.statementTime)
		t.Status("watching changefeed")
		return verifier.pollLatency(ctx, db, jobID, time.Second, workloadCompleteCh)
	})

	if args.kafkaChaos {
		m.Go(func(ctx context.Context) error {
			period, downTime := 2*time.Minute, 20*time.Second
			return kafka.chaosLoop(ctx, period, downTime, workloadCompleteCh)
		})
	}

	if args.crdbChaos {
		chaosDuration, err := time.ParseDuration(args.workloadDuration)
		if err != nil {
			t.Fatal(err)
		}
		ch := Chaos{
			Timer:   Periodic{Period: 2 * time.Minute, DownTime: 20 * time.Second},
			Target:  crdbNodes.RandNode,
			Stopper: time.After(chaosDuration),
		}
		m.Go(ch.Runner(c, t, m))
	}
	m.Wait()

	verifier.assertValid(t)
	workloadEnd := timeutil.Now()
	if args.targetTxnPerSecond > 0.0 {
		verifyTxnPerSecond(
			ctx, c, t, crdbNodes.RandNode(), workloadStart, workloadEnd, args.targetTxnPerSecond, 0.05,
		)
	}
}

func runCDCBank(ctx context.Context, t test.Test, c cluster.Cluster) {

	// Make the logs dir on every node to work around the `roachprod get logs`
	// spam.
	c.Run(ctx, c.All(), `mkdir -p logs`)

	crdbNodes, workloadNode, kafkaNode := c.Range(1, c.Spec().NodeCount-1), c.Node(c.Spec().NodeCount), c.Node(c.Spec().NodeCount)
	c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)
	c.Start(ctx, crdbNodes)
	kafka := kafkaManager{
		t:     t,
		c:     c,
		nodes: kafkaNode,
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
	defer kafka.stop(ctx)

	t.Status("creating kafka topic")
	if err := kafka.createTopic(ctx, "bank"); err != nil {
		t.Fatal(err)
	}

	c.Run(ctx, workloadNode, `./workload init bank {pgurl:1}`)
	db := c.Conn(ctx, 1)
	defer stopFeeds(db)

	if _, err := db.Exec(
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`,
	); err != nil {
		t.Fatal(err)
	}

	// NB: the WITH diff option was not supported until v20.1.
	withDiff := t.IsBuildVersion("v20.1.0")
	var opts = []string{`updated`, `resolved`}
	if withDiff {
		opts = append(opts, `diff`)
	}
	var jobID string
	if err := db.QueryRow(
		`CREATE CHANGEFEED FOR bank.bank INTO $1 WITH `+strings.Join(opts, `, `), kafka.sinkURL(ctx),
	).Scan(&jobID); err != nil {
		t.Fatal(err)
	}

	t.Status("running workload")
	workloadCtx, workloadCancel := context.WithCancel(ctx)
	m := c.NewMonitor(workloadCtx, crdbNodes)
	var doneAtomic int64
	m.Go(func(ctx context.Context) error {
		err := c.RunE(ctx, workloadNode, `./workload run bank {pgurl:1} --max-rate=10`)
		if atomic.LoadInt64(&doneAtomic) > 0 {
			return nil
		}
		return errors.Wrap(err, "workload failed")
	})
	m.Go(func(ctx context.Context) (_err error) {
		defer workloadCancel()

		defer func() {
			_err = errors.Wrap(_err, "CDC failed")
		}()

		l, err := t.L().ChildLogger(`changefeed`)
		if err != nil {
			return err
		}
		defer l.Close()

		tc, err := kafka.consumer(ctx, "bank")
		if err != nil {
			return errors.Wrap(err, "could not create kafka consumer")
		}

		defer tc.Close()

		if _, err := db.Exec(
			`CREATE TABLE fprint (id INT PRIMARY KEY, balance INT, payload STRING)`,
		); err != nil {
			return errors.Wrap(err, "CREATE TABLE failed")
		}

		const requestedResolved = 100
		fprintV, err := cdctest.NewFingerprintValidator(db, `bank.bank`, `fprint`, tc.partitions, 0)
		if err != nil {
			return errors.Wrap(err, "error creating validator")
		}
		validators := cdctest.Validators{
			cdctest.NewOrderValidator(`bank`),
			fprintV,
		}
		if withDiff {
			baV, err := cdctest.NewBeforeAfterValidator(db, `bank.bank`)
			if err != nil {
				return err
			}
			validators = append(validators, baV)
		}
		v := cdctest.MakeCountValidator(validators)

		for {
			m := tc.Next(ctx)
			if m == nil {
				return fmt.Errorf("unexpected end of changefeed")
			}
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
				l.Printf("%d of %d resolved timestamps, latest is %s behind realtime",
					v.NumResolvedWithRows, requestedResolved, timeutil.Since(resolved.GoTime()))
				if v.NumResolvedWithRows >= requestedResolved {
					atomic.StoreInt64(&doneAtomic, 1)
					break
				}
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
	c.Start(ctx, crdbNodes)
	kafka := kafkaManager{
		t:     t,
		c:     c,
		nodes: kafkaNode,
	}
	kafka.install(ctx)
	kafka.start(ctx)
	defer kafka.stop(ctx)

	db := c.Conn(ctx, 1)
	defer stopFeeds(db)

	if _, err := db.Exec(`SET CLUSTER SETTING kv.rangefeed.enabled = $1`, true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE foo (a INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	// NB: the WITH diff option was not supported until v20.1.
	withDiff := t.IsBuildVersion("v20.1.0")
	var opts = []string{`updated`, `resolved`, `format=experimental_avro`, `confluent_schema_registry=$2`}
	if withDiff {
		opts = append(opts, `diff`)
	}
	var jobID string
	if err := db.QueryRow(
		`CREATE CHANGEFEED FOR foo INTO $1 WITH `+strings.Join(opts, `, `),
		kafka.sinkURL(ctx), kafka.schemaRegistryURL(ctx),
	).Scan(&jobID); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`INSERT INTO foo VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE foo ADD COLUMN b STRING`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO foo VALUES (2, '2')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE foo ADD COLUMN c INT`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO foo VALUES (3, '3', 3)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE foo DROP COLUMN b`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO foo VALUES (4, 4)`); err != nil {
		t.Fatal(err)
	}

	output, err := c.RunWithBuffer(ctx, t.L(), kafkaNode,
		kafka.makeCommand("kafka-avro-console-consumer",
			"--from-beginning",
			"--topic=foo",
			"--max-messages=14",
			"--bootstrap-server=localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	t.L().Printf("\n%s\n", output)

	updatedRE := regexp.MustCompile(`"updated":\{"string":"[^"]+"\}`)
	updatedMap := make(map[string]struct{})
	var resolved []string
	for _, line := range strings.Split(string(output), "\n") {
		if strings.Contains(line, `"updated"`) {
			line = updatedRE.ReplaceAllString(line, `"updated":{"string":""}`)
			updatedMap[line] = struct{}{}
		} else if strings.Contains(line, `"resolved"`) {
			resolved = append(resolved, line)
		}
	}
	// There are various internal races and retries in changefeeds that can
	// produce duplicates. This test is really only to verify that the confluent
	// schema registry works end-to-end, so do the simplest thing and sort +
	// unique the output.
	updated := make([]string, 0, len(updatedMap))
	for u := range updatedMap {
		updated = append(updated, u)
	}
	sort.Strings(updated)

	var expected []string
	if withDiff {
		expected = []string{
			`{"before":null,"after":{"foo":{"a":{"long":1}}},"updated":{"string":""}}`,
			`{"before":null,"after":{"foo":{"a":{"long":2},"b":{"string":"2"}}},"updated":{"string":""}}`,
			`{"before":null,"after":{"foo":{"a":{"long":3},"b":{"string":"3"},"c":{"long":3}}},"updated":{"string":""}}`,
			`{"before":null,"after":{"foo":{"a":{"long":4},"c":{"long":4}}},"updated":{"string":""}}`,
			`{"before":{"foo_before":{"a":{"long":1},"b":null,"c":null}},"after":{"foo":{"a":{"long":1},"c":null}},"updated":{"string":""}}`,
			`{"before":{"foo_before":{"a":{"long":1},"c":null}},"after":{"foo":{"a":{"long":1},"c":null}},"updated":{"string":""}}`,
			`{"before":{"foo_before":{"a":{"long":2},"b":{"string":"2"},"c":null}},"after":{"foo":{"a":{"long":2},"c":null}},"updated":{"string":""}}`,
			`{"before":{"foo_before":{"a":{"long":2},"c":null}},"after":{"foo":{"a":{"long":2},"c":null}},"updated":{"string":""}}`,
			`{"before":{"foo_before":{"a":{"long":3},"b":{"string":"3"},"c":{"long":3}}},"after":{"foo":{"a":{"long":3},"c":{"long":3}}},"updated":{"string":""}}`,
			`{"before":{"foo_before":{"a":{"long":3},"c":{"long":3}}},"after":{"foo":{"a":{"long":3},"c":{"long":3}}},"updated":{"string":""}}`,
		}
	} else {
		expected = []string{
			`{"updated":{"string":""},"after":{"foo":{"a":{"long":1},"c":null}}}`,
			`{"updated":{"string":""},"after":{"foo":{"a":{"long":1}}}}`,
			`{"updated":{"string":""},"after":{"foo":{"a":{"long":2},"b":{"string":"2"}}}}`,
			`{"updated":{"string":""},"after":{"foo":{"a":{"long":2},"c":null}}}`,
			`{"updated":{"string":""},"after":{"foo":{"a":{"long":3},"b":{"string":"3"},"c":{"long":3}}}}`,
			`{"updated":{"string":""},"after":{"foo":{"a":{"long":3},"c":{"long":3}}}}`,
			`{"updated":{"string":""},"after":{"foo":{"a":{"long":4},"c":{"long":4}}}}`,
		}
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
	c.Start(ctx, crdbNodes)

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

	db := c.Conn(ctx, 1)
	defer stopFeeds(db)

	if _, err := db.Exec(
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE auth_test_table (a INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

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

	var jobID int
	for _, f := range feeds {
		t.Status(f.desc)
		row := db.QueryRow(`CREATE CHANGEFEED FOR auth_test_table INTO $1`, f.queryArg)
		if err := row.Scan(&jobID); err != nil {
			t.Fatalf("%s: %s", f.desc, err.Error())
		}
	}
}

func registerCDC(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "cdc/tpcc-1000",
		Owner:           registry.OwnerCDC,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType:             tpccWorkloadType,
				tpccWarehouseCount:       1000,
				workloadDuration:         "120m",
				targetInitialScanLatency: 3 * time.Minute,
				targetSteadyLatency:      10 * time.Minute,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/tpcc-1000/sink=null",
		Owner:           registry.OwnerCDC,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		Tags:            []string{"manual"},
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType:             tpccWorkloadType,
				tpccWarehouseCount:       1000,
				workloadDuration:         "120m",
				targetInitialScanLatency: 3 * time.Minute,
				targetSteadyLatency:      10 * time.Minute,
				sinkURI:                  "null://",
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/initial-scan",
		Owner:           registry.OwnerCDC,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType:             tpccWorkloadType,
				tpccWarehouseCount:       100,
				workloadDuration:         "30m",
				initialScan:              true,
				targetInitialScanLatency: 30 * time.Minute,
				targetSteadyLatency:      time.Minute,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/sink-chaos",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType:             tpccWorkloadType,
				tpccWarehouseCount:       100,
				workloadDuration:         "30m",
				kafkaChaos:               true,
				targetInitialScanLatency: 3 * time.Minute,
				targetSteadyLatency:      5 * time.Minute,
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/crdb-chaos",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType:             tpccWorkloadType,
				tpccWarehouseCount:       100,
				workloadDuration:         "30m",
				crdbChaos:                true,
				targetInitialScanLatency: 3 * time.Minute,
				// TODO(aayush): It should be okay to drop this as low as 2 to 3 minutes. See
				// #36879 for some discussion.
				targetSteadyLatency: 5 * time.Minute,
			})
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
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType: ledgerWorkloadType,
				// TODO(ssd): Range splits cause changefeed latencies to balloon
				// because of catchup-scan performance. Reducing the test time and
				// bumping the range_max_bytes avoids the split until we can improve
				// catchup scan performance.
				workloadDuration:         "28m",
				initialScan:              true,
				targetInitialScanLatency: 10 * time.Minute,
				targetSteadyLatency:      time.Minute,
				targetTxnPerSecond:       575,
				preStartStatements:       []string{"ALTER DATABASE ledger CONFIGURE ZONE USING range_max_bytes = 805306368, range_min_bytes = 134217728"},
			})
		},
	})
	r.Add(registry.TestSpec{
		Name:            "cdc/cloud-sink-gcs/rangefeed=true",
		Owner:           `cdc`,
		Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			cdcBasicTest(ctx, t, c, cdcTestArgs{
				workloadType: tpccWorkloadType,
				// Sending data to Google Cloud Storage is a bit slower than sending to
				// Kafka on an adjacent machine, so use half the data of the
				// initial-scan test. Consider adding a test that writes to nodelocal,
				// which should be much faster, with a larger warehouse count.
				tpccWarehouseCount:       50,
				workloadDuration:         "30m",
				initialScan:              true,
				whichSink:                cloudStorageSink,
				targetInitialScanLatency: 30 * time.Minute,
				targetSteadyLatency:      time.Minute,
			})
		},
	})
	// TODO(ryan min): uncomment once parallelism is implemented for webhook
	// sink, currently fails with "initial scan did not complete"
	/*
		r.Add(testSpec{
			Name:            "cdc/webhook-sink",
			Owner:           OwnerCDC,
			Cluster:         r.MakeClusterSpec(4, spec.CPU(16)),
			RequiresLicense: true,
			Run: func(ctx context.Context, t *test, c Cluster) {
				cdcBasicTest(ctx, t, c, cdcTestArgs{
					workloadType:             tpccWorkloadType,
					tpccWarehouseCount:       100,
					workloadDuration:         "30m",
					initialScan:              true,
					whichSink:                webhookSink,
					targetInitialScanLatency: 30 * time.Minute,
					targetSteadyLatency:      time.Minute,
				})
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
	// kafkaJAASConfig is a JAAS configuration file that creats a
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
		k.c.Run(ctx, k.nodes, `sudo apt-get -q update 2>&1 > logs/apt-get-update.log`)
		k.c.Run(ctx, k.nodes, `yes | sudo apt-get -q install openssl default-jre 2>&1 > logs/apt-get-install.log`)
	}
}

func (k kafkaManager) configureAuth(ctx context.Context) *testCerts {
	k.t.Status("generating TLS certificates")
	ips, err := k.c.InternalIP(ctx, k.nodes)
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

func (k kafkaManager) restart(ctx context.Context, services ...string) {
	var startArgs string
	if len(services) == 0 {
		startArgs = "schema-registry"
	} else {
		startArgs = strings.Join(services, " ")
	}

	k.c.Run(ctx, k.nodes, "touch", k.serverJAASConfig())

	startCmd := fmt.Sprintf(
		"CONFLUENT_CURRENT=%s CONFLUENT_HOME=%s KAFKA_OPTS=-Djava.security.auth.login.config=%s %s local services %s start",
		k.basePath(),
		k.confluentHome(),
		k.serverJAASConfig(),
		k.confluentBin(),
		startArgs)
	k.c.Run(ctx, k.nodes, startCmd)
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
	ips, err := k.c.InternalIP(ctx, k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `kafka://` + ips[0] + `:9092`
}

func (k kafkaManager) sinkURLTLS(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `kafka://` + ips[0] + `:9093`
}

func (k kafkaManager) sinkURLSASL(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `kafka://` + ips[0] + `:9094`
}

func (k kafkaManager) consumerURL(ctx context.Context) string {
	ips, err := k.c.ExternalIP(ctx, k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return ips[0] + `:9092`
}

func (k kafkaManager) schemaRegistryURL(ctx context.Context) string {
	ips, err := k.c.InternalIP(ctx, k.nodes)
	if err != nil {
		k.t.Fatal(err)
	}
	return `http://` + ips[0] + `:8081`
}

func (k kafkaManager) createTopic(ctx context.Context, topic string) error {
	kafkaAddrs := []string{k.consumerURL(ctx)}
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(kafkaAddrs, config)
	if err != nil {
		return errors.Wrap(err, "admin client")
	}
	return admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
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
	c.Run(ctx, lw.workloadNodes.RandNode(), fmt.Sprintf(
		`./workload init ledger {pgurl%s}`,
		lw.sqlNodes.RandNode(),
	))
}

func (lw *ledgerWorkload) run(ctx context.Context, c cluster.Cluster, workloadDuration string) {
	c.Run(ctx, lw.workloadNodes, fmt.Sprintf(
		`./workload run ledger --mix=balance=0,withdrawal=50,deposit=50,reversal=0 {pgurl%s} --duration=%s`,
		lw.sqlNodes,
		workloadDuration,
	))
}

type latencyVerifier struct {
	statementTime            time.Time
	targetSteadyLatency      time.Duration
	targetInitialScanLatency time.Duration
	tolerateErrors           bool
	logger                   *logger.Logger
	setTestStatus            func(...interface{})

	initialScanLatency   time.Duration
	maxSeenSteadyLatency time.Duration
	maxSeenSteadyEveryN  log.EveryN
	latencyBecameSteady  bool

	latencyHist *hdrhistogram.Histogram
}

func makeLatencyVerifier(
	targetInitialScanLatency time.Duration,
	targetSteadyLatency time.Duration,
	l *logger.Logger,
	setTestStatus func(...interface{}),
	tolerateErrors bool,
) *latencyVerifier {
	const sigFigs, minLatency, maxLatency = 1, 100 * time.Microsecond, 100 * time.Second
	hist := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
	return &latencyVerifier{
		targetInitialScanLatency: targetInitialScanLatency,
		targetSteadyLatency:      targetSteadyLatency,
		logger:                   l,
		setTestStatus:            setTestStatus,
		latencyHist:              hist,
		tolerateErrors:           tolerateErrors,
		maxSeenSteadyEveryN:      log.Every(10 * time.Second),
	}
}

func (lv *latencyVerifier) noteHighwater(highwaterTime time.Time) {
	if highwaterTime.Before(lv.statementTime) {
		return
	}
	if lv.initialScanLatency == 0 {
		lv.initialScanLatency = timeutil.Since(lv.statementTime)
		lv.logger.Printf("initial scan completed: latency %s\n", lv.initialScanLatency)
		return
	}

	latency := timeutil.Since(highwaterTime)
	if latency < lv.targetSteadyLatency/2 {
		lv.latencyBecameSteady = true
	}
	if !lv.latencyBecameSteady {
		// Before we have RangeFeed, the polls just get
		// progressively smaller after the initial one. Start
		// tracking the max latency once we seen a latency
		// that's less than the max allowed. Verify at the end
		// of the test that this happens at some point.
		if lv.maxSeenSteadyEveryN.ShouldLog() {
			lv.setTestStatus(fmt.Sprintf(
				"watching changefeed: end-to-end latency %s not yet below target steady latency %s",
				latency.Truncate(time.Millisecond), lv.targetSteadyLatency.Truncate(time.Millisecond)))
		}
		return
	}
	if err := lv.latencyHist.RecordValue(latency.Nanoseconds()); err != nil {
		lv.logger.Printf("could not record value %s: %s\n", latency, err)
	}
	if latency > lv.maxSeenSteadyLatency {
		lv.maxSeenSteadyLatency = latency
	}
	if lv.maxSeenSteadyEveryN.ShouldLog() {
		lv.setTestStatus(fmt.Sprintf(
			"watching changefeed: end-to-end steady latency %s; max steady latency so far %s",
			latency.Truncate(time.Millisecond), lv.maxSeenSteadyLatency.Truncate(time.Millisecond)))
	}
}

func (lv *latencyVerifier) pollLatency(
	ctx context.Context, db *gosql.DB, jobID int, interval time.Duration, stopper chan struct{},
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stopper:
			return nil
		case <-time.After(time.Second):
		}

		info, err := getChangefeedInfo(db, jobID)
		if err != nil {
			if lv.tolerateErrors {
				lv.logger.Printf("error getting changefeed info: %s", err)
				continue
			}
			return err
		}
		if info.status != `running` {
			lv.logger.Printf("unexpected status: %s, error: %s", info.status, info.errMsg)
			return errors.Errorf(`unexpected status: %s`, info.status)
		}
		lv.noteHighwater(info.highwaterTime)
	}
}

func (lv *latencyVerifier) assertValid(t test.Test) {
	if lv.initialScanLatency == 0 {
		t.Fatalf("initial scan did not complete")
	}
	if lv.initialScanLatency > lv.targetInitialScanLatency {
		t.Fatalf("initial scan latency was more than target: %s vs %s",
			lv.initialScanLatency, lv.targetInitialScanLatency)
	}
	if !lv.latencyBecameSteady {
		t.Fatalf("latency never dropped to acceptable steady level: %s", lv.targetSteadyLatency)
	}
	if lv.maxSeenSteadyLatency > lv.targetSteadyLatency {
		t.Fatalf("max latency was more than allowed: %s vs %s",
			lv.maxSeenSteadyLatency, lv.targetSteadyLatency)
	}
}

func (lv *latencyVerifier) maybeLogLatencyHist() {
	if lv.latencyHist == nil {
		return
	}
	lv.logger.Printf(
		"changefeed end-to-end __avg(ms)__p50(ms)__p75(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)\n")
	lv.logger.Printf("changefeed end-to-end  %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
		time.Duration(lv.latencyHist.Mean()).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(50)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(75)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(90)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(95)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(99)).Seconds()*1000,
		time.Duration(lv.latencyHist.ValueAtQuantile(100)).Seconds()*1000,
	)
}

func createChangefeed(db *gosql.DB, targets, sinkURL string, args cdcTestArgs) (int, error) {
	var jobID int
	createStmt := fmt.Sprintf(`CREATE CHANGEFEED FOR %s INTO $1`, targets)
	extraArgs := []interface{}{sinkURL}
	if args.whichSink == cloudStorageSink || args.whichSink == webhookSink {
		createStmt += ` WITH resolved='10s', envelope=wrapped`
	} else {
		createStmt += ` WITH resolved`
	}
	if !args.initialScan {
		createStmt += `, cursor='-1s'`
	}
	if err := db.QueryRow(createStmt, extraArgs...).Scan(&jobID); err != nil {
		return 0, err
	}
	return jobID, nil
}

type changefeedInfo struct {
	status        string
	errMsg        string
	statementTime time.Time
	highwaterTime time.Time
}

func getChangefeedInfo(db *gosql.DB, jobID int) (changefeedInfo, error) {
	var status string
	var payloadBytes []byte
	var progressBytes []byte
	if err := db.QueryRow(
		`SELECT status, payload, progress FROM system.jobs WHERE id = $1`, jobID,
	).Scan(&status, &payloadBytes, &progressBytes); err != nil {
		return changefeedInfo{}, err
	}
	var payload jobspb.Payload
	if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
		return changefeedInfo{}, err
	}
	var progress jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
		return changefeedInfo{}, err
	}
	var highwaterTime time.Time
	highwater := progress.GetHighWater()
	if highwater != nil {
		highwaterTime = highwater.GoTime()
	}
	return changefeedInfo{
		status:        status,
		errMsg:        payload.Error,
		statementTime: payload.GetChangefeed().StatementTime.GoTime(),
		highwaterTime: highwaterTime,
	}, nil
}

// stopFeeds cancels any running feeds on the cluster. Not necessary for the
// nightly, but nice for development.
func stopFeeds(db *gosql.DB) {
	_, _ = db.Exec(`CANCEL JOBS (
			SELECT job_id FROM [SHOW JOBS] WHERE status = 'running'
		)`)
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
