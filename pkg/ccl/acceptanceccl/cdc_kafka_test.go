// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acceptanceccl

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/acceptance"
	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
)

func TestCDC(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	acceptance.RunDocker(t, func(t *testing.T) {
		ctx := context.Background()
		cfg := acceptance.ReadConfigFromFlags()
		// Should we thread the old value of cfg.Nodes to the TestCluster?
		cfg.Nodes = nil
		// We're just using this DockerCluster for all its helpers.
		// CockroachDB will be run via TestCluster.
		c := acceptance.StartCluster(ctx, t, cfg).(*cluster.DockerCluster)
		log.Infof(ctx, "cluster started successfully")
		defer c.AssertAndStop(ctx, t)

		// Share kafka between all the subtests because it takes forever (~20s)
		// to start up.
		k, err := startDockerKafka(ctx, c)
		if err != nil {
			t.Fatalf(`%+v`, err)
		}
		defer k.Close(ctx)

		t.Run(`PauseUnpause`, func(t *testing.T) { testPauseUnpause(ctx, t, c, k) })
		t.Run(`Bank`, func(t *testing.T) { testBank(ctx, t, c, k) })
	})
}

func testPauseUnpause(ctx context.Context, t *testing.T, c *cluster.DockerCluster, k *dockerKafka) {
	defer func(prev time.Duration) { jobs.DefaultAdoptInterval = prev }(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond

	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

	into := `kafka://localhost:` + k.kafkaPort + `?topic_prefix=PauseUnpause_`
	var jobID int
	sqlDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO $1 WITH timestamps`, into).Scan(&jobID)

	tc, err := makeTopicsConsumer(k.consumer, `PauseUnpause_foo`)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Close()

	tc.assertPayloads(t, []string{
		`PauseUnpause_foo: [1]->{"a":1,"b":"a"}`,
		`PauseUnpause_foo: [2]->{"a":2,"b":"b"}`,
		`PauseUnpause_foo: [4]->{"a":4,"b":"c"}`,
		`PauseUnpause_foo: [7]->{"a":7,"b":"d"}`,
		`PauseUnpause_foo: [8]->{"a":8,"b":"e"}`,
	})

	// Wait for the highwater mark on the job to be updated after the initial
	// scan, to make sure we don't get the initial scan data again.
	m := tc.nextMessage(t)
	if len(m.Key) != 0 {
		t.Fatalf(`expected a resolved timestamp got %s: %s->%s`, m.Topic, m.Key, m.Value)
	}

	sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
	sqlDB.Exec(t, `RESUME JOB $1`, jobID)
	tc.assertPayloads(t, []string{
		`PauseUnpause_foo: [16]->{"a":16,"b":"f"}`,
	})
}

func testBank(ctx context.Context, t *testing.T, c *cluster.DockerCluster, k *dockerKafka) {
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "bank"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	const numRows, numRanges, payloadBytes, maxTransfer = 10, 10, 10, 999
	sqlDB.Exec(t, `CREATE DATABASE bank`)
	gen := bank.FromConfig(numRows, payloadBytes, numRanges)
	if _, err := workload.Setup(ctx, sqlDB.DB, gen, 0, 0); err != nil {
		t.Fatal(err)
	}

	into := `kafka://localhost:` + k.kafkaPort + `?topic_prefix=Bank_`
	sqlDB.Exec(t, `CREATE CHANGEFEED FOR bank INTO $1 WITH timestamps`, into)

	var done int64
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		for {
			if atomic.LoadInt64(&done) > 0 {
				return nil
			}

			// TODO(dan): This bit is copied from the bank workload. It's
			// currently much easier to do this than to use the real Ops,
			// which is silly. Fixme.
			from := rand.Intn(numRows)
			to := rand.Intn(numRows)
			for from == to {
				to = rand.Intn(numRows)
			}
			amount := rand.Intn(maxTransfer)
			sqlDB.Exec(t, `UPDATE bank.bank
				SET balance = CASE id WHEN $1 THEN balance-$3 WHEN $2 THEN balance+$3 END
				WHERE id IN ($1, $2)
			`, from, to, amount)
		}
	})

	tc, err := makeTopicsConsumer(k.consumer, `Bank_bank`)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Close()

	partitionIDs, err := k.consumer.Partitions(`Bank_bank`)
	if err != nil {
		t.Fatal(err)
	}
	if len(partitionIDs) <= 1 {
		t.Fatal("test requires at least 2 partitions to be interesting")
	}
	partitions := make([]string, len(partitionIDs))
	for i, p := range partitionIDs {
		partitions[i] = strconv.Itoa(int(p))
	}

	const requestedResolved = 100
	var numResolved, rowsSinceResolved int
	v := changefeedccl.Validators{
		changefeedccl.NewOrderValidator(`Bank_bank`),
		changefeedccl.NewFingerprintValidator(sqlDB.DB, `bank`, `fprint`, partitions),
	}
	sqlDB.Exec(t, `CREATE TABLE fprint (id INT PRIMARY KEY, balance INT, payload STRING)`)
	for {
		m := tc.nextMessage(t)
		updated, resolved, err := changefeedccl.ParseJSONValueTimestamps(m.Value)
		if err != nil {
			t.Fatal(err)
		}

		partitionStr := strconv.Itoa(int(m.Partition))
		if len(m.Key) > 0 {
			v.NoteRow(partitionStr, string(m.Key), string(m.Value), updated)
			rowsSinceResolved++
		} else {
			if err := v.NoteResolved(partitionStr, resolved); err != nil {
				t.Fatal(err)
			}
			if rowsSinceResolved > 0 {
				numResolved++
				if numResolved > requestedResolved {
					atomic.StoreInt64(&done, 1)
					break
				}
			}
			rowsSinceResolved = 0
		}
	}
	for _, f := range v.Failures() {
		t.Error(f)
	}

	if err := g.Wait(); err != nil {
		t.Errorf(`%+v`, err)
	}
}

const (
	confluentVersion = `4.0.0`
	zookeeperImage   = `docker.io/confluentinc/cp-zookeeper:` + confluentVersion
	kafkaImage       = `docker.io/confluentinc/cp-kafka:` + confluentVersion
)

type dockerKafka struct {
	serviceContainers        map[string]*cluster.Container
	zookeeperPort, kafkaPort string

	consumer sarama.Consumer
}

func getOpenPort() (string, error) {
	l, err := net.Listen(`tcp`, `:0`)
	if err != nil {
		return ``, err
	}
	err = l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), err
}

// startDockerKafka runs zookeeper and kafka in docker containers.
//
// There's enough complexity in Kafka that there's no way to have any confidence
// in end-to-end correctness testing based on a mock. We need real Kafka. Both
// kafka and its zookeeper dependence are java, and so we need to run them in
// Docker for these tests to be portable and reproducible. (I know, I know.)
//
// The major trick here is that kafka has an internal mechanism that lets you
// talk to any node and it redirects you to the one that has the data you're
// looking for. This is also used internally by the system. These are configured
// as advertised hosts.
//
// So, our CockroachDB changefeed needs to be able to reach Kafka at some
// address configured in the CREATE CHANGEFEED. Then it receives the list of
// advertised host:ports from Kafka, and it needs to be able to contact all of
// those. The same is true for the kafka nodes and the consumer that the test
// uses for to make assertions. Docker for mac really makes this difficult. The
// kafka nodes are running in docker, so they want to be able to talk to each
// other by their docker hostnames. We could also run CockroachDB inside docker,
// but the test is running outside (where the docker hosts don't resolve) and
// there's no easy way to change that.
//
// The easiest thing would be docker's `--network=host`, but alas that's not
// available with docker for mac.
//
// Kafka (theoretically) allows for multiple sets of named advertised listeners,
// differentiated by port. When you connect, it sends back the ones relevant to
// the port you connected to. This is designed for exactly this sort of
// situation. But this is tragically underdocumented and after *literally tens
// of hours* I could not get this to work.
//
// We could run some program inside docker to consume and proxy that information
// out to the test somehow (e.g. tailing `kafka-console-consumer`), but this is
// likely to introduce the same sort of bugs we'd have with the mock.
//
// We could also use `--network=container` instead of bridge networking, which
// lets us share the same network namespace between a bunch of containers. Then
// we make everything run on unique ports (and export them) and use "localhost"
// for the host everywhere. This works well and might be what we have to do if
// we need to run multi-node Kafka clusters. However, all the necessary ports
// have to be exported from the first container started, which requires some
// major surgery to DockerCluster.
//
// In the end, what we do is similar. Zookeeper and Kafka are assigned unique
// ports that are unassigned on the host. They run on that port inside docker
// and it's mapped to the same port on the host. (Zookeeper doesn't need to be
// available externally, but it was easy and sometimes it's nice for debugging
// the test.) A one node Kafka cluster can talk to itself on localhost and the
// unique port. CockroachDB also can, but only from outside docker. And... uh...
// we're done. \o/
//
// This is a monstrosity, so please fix it if you can figure out a better way.
func startDockerKafka(
	ctx context.Context, d *cluster.DockerCluster, topics ...string,
) (*dockerKafka, error) {
	k := &dockerKafka{
		serviceContainers: make(map[string]*cluster.Container),
	}
	var err error
	if k.zookeeperPort, err = getOpenPort(); err != nil {
		return nil, err
	}
	if k.kafkaPort, err = getOpenPort(); err != nil {
		return nil, err
	}

	zookeeper, err := d.SidecarContainer(ctx, container.Config{
		Hostname: `zookeeper`,
		Image:    zookeeperImage,
		ExposedPorts: map[nat.Port]struct{}{
			nat.Port(k.zookeeperPort + `/tcp`): {},
		},
		Env: []string{
			`ZOOKEEPER_CLIENT_PORT=` + k.zookeeperPort,
			`ZOOKEEPER_TICK_TIME=2000`,
		},
	}, map[string]string{k.zookeeperPort: k.zookeeperPort})
	if err != nil {
		return nil, err
	}
	kafka, err := d.SidecarContainer(ctx, container.Config{
		Hostname: `kafka`,
		Image:    kafkaImage,
		ExposedPorts: map[nat.Port]struct{}{
			nat.Port(k.kafkaPort + `/tcp`): {},
		},
		Env: []string{
			`KAFKA_ZOOKEEPER_CONNECT=` + zookeeper.Name() + `:` + k.zookeeperPort,
			`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`,
			`KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:` + k.kafkaPort,
			`KAFKA_NUM_PARTITIONS=3`,
		},
	}, map[string]string{k.kafkaPort: k.kafkaPort})
	if err != nil {
		return nil, err
	}

	k.serviceContainers = map[string]*cluster.Container{
		`zookeeper`: zookeeper,
		`kafka`:     kafka,
	}
	for _, n := range []string{`zookeeper`, `kafka`} {
		s := k.serviceContainers[n]
		if err := s.Start(ctx); err != nil {
			return nil, err
		}
		log.Infof(ctx, "%s is running: %s", s.Name(), s.ID())
	}

	// Wait for kafka to be available.
	if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
		addrs := []string{`localhost:` + k.kafkaPort}
		var err error
		k.consumer, err = sarama.NewConsumer(addrs, sarama.NewConfig())
		return err
	}); err != nil {
		return nil, err
	}

	return k, nil
}

func (k *dockerKafka) Close(ctx context.Context) {
	for _, c := range k.serviceContainers {
		if err := c.Kill(ctx); err != nil {
			log.Warningf(ctx, "could not kill container %s (%s)", c.Name(), c.ID())
		}
		if err := c.Remove(ctx); err != nil {
			log.Warningf(ctx, "could not remove container %s (%s)", c.Name(), c.ID())
		}
	}
	if err := k.consumer.Close(); err != nil {
		log.Infof(ctx, `failed to close consumer: %+v`, err)
	}
}

type topicsConsumer struct {
	sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
}

func makeTopicsConsumer(c sarama.Consumer, topics ...string) (*topicsConsumer, error) {
	t := &topicsConsumer{Consumer: c}
	for _, topic := range topics {
		partitions, err := t.Partitions(topic)
		if err != nil {
			return nil, err
		}
		for _, partition := range partitions {
			pc, err := t.ConsumePartition(topic, partition, sarama.OffsetOldest)
			if err != nil {
				return nil, err
			}
			t.partitionConsumers = append(t.partitionConsumers, pc)
		}
	}
	return t, nil
}

func (c *topicsConsumer) Close() {
	for _, pc := range c.partitionConsumers {
		pc.AsyncClose()
		// Drain the messages and errors as required by AsyncClose.
		for range pc.Messages() {
		}
		for range pc.Errors() {
		}
	}
}

func (c *topicsConsumer) tryNextMessage(t testing.TB) *sarama.ConsumerMessage {
	for _, pc := range c.partitionConsumers {
		select {
		case m := <-pc.Messages():
			return m
		default:
		}
	}
	return nil
}

func (c *topicsConsumer) nextMessage(t testing.TB) *sarama.ConsumerMessage {
	m := c.tryNextMessage(t)
	for ; m == nil; m = c.tryNextMessage(t) {
	}
	return m
}

func (c *topicsConsumer) assertPayloads(t testing.TB, expected []string) {
	var actual []string
	for len(actual) < len(expected) {
		m := c.nextMessage(t)

		// Skip resolved timestamps messages.
		if len(m.Key) == 0 {
			continue
		}

		// Strip out the updated timestamp in the value.
		var valueRaw map[string]interface{}
		if err := json.Unmarshal(m.Value, &valueRaw); err != nil {
			t.Fatal(err)
		}
		delete(valueRaw, `__crdb__`)
		value, err := json.Marshal(valueRaw)
		if err != nil {
			t.Fatal(err)
		}

		actual = append(actual, fmt.Sprintf(`%s: %s->%s`, m.Topic, m.Key, value))
	}
	// The tests that use this aren't concerned with order, just that these are
	// the next len(expected) messages.
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}
