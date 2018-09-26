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
	"io/ioutil"
	"math/rand"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/acceptance"
	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
)

func TestCDC(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	acceptance.RunDocker(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
			}
			// Additional debugging in case #28102 shows up again.
			logs, err := c.Client().ContainerLogs(
				context.Background(),
				k.serviceContainers[`kafka`].Name(),
				types.ContainerLogsOptions{ShowStdout: true, ShowStderr: true},
			)
			if err != nil {
				log.Warningf(ctx, "unable to get additional debugging: %v", err)
				return
			}
			defer logs.Close()
			logsBytes, err := ioutil.ReadAll(logs)
			if err != nil {
				log.Warningf(ctx, "unable to get additional debugging: %v", err)
				return
			}
			log.Infof(ctx, "KAFKA DOCKER CONTAINER LOGS:\n%s", string(logsBytes))
		}()

		t.Run(`Bank`, func(t *testing.T) { testBank(ctx, t, c, k) })
		t.Run(`Errors`, func(t *testing.T) { testErrors(ctx, t, k) })
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
	sqlDB.Exec(t, `CREATE CHANGEFEED FOR bank INTO $1 WITH updated, resolved`, into)

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

	// TODO(dan): This should be higher (it was 100 initially) but a change that
	// tuned the kafka producer config raised the running time of this test to
	// tens of minutes. While I'm figuring out the right tunings, lower this to
	// speed the test up.
	const requestedResolved = 5
	var numResolved, rowsSinceResolved int
	v := changefeedccl.Validators{
		changefeedccl.NewOrderValidator(`Bank_bank`),
		// TODO(mrtracy): Disabled by #30902. Re-enabling is tracked by #31110.
		// changefeedccl.NewFingerprintValidator(sqlDB.DB, `bank`, `fprint`, partitions),
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

func testErrors(ctx context.Context, t *testing.T, k *dockerKafka) {
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo INTO 'kafka://nope'`,
	); !testutils.IsError(err, `client has run out of available brokers`) {
		t.Errorf(`expected 'client has run out of available brokers' error got: %+v`, err)
	}

	into := `kafka://localhost:` + k.kafkaPort + `?kafka_topic_prefix=Errors_`
	if _, err := sqlDBRaw.Exec(
		`CREATE CHANGEFEED FOR foo INTO $1`, into,
	); !testutils.IsError(err, `unknown sink query parameter: kafka_topic_prefix`) {
		t.Errorf(`expected "unknown sink query parameter: kafka_topic_prefix" error got: %v`, err)
	}
	into = `kafka://localhost:` + k.kafkaPort + `?schema_topic=foo`
	if _, err := sqlDBRaw.Exec(
		`CREATE CHANGEFEED FOR foo INTO $1`, into,
	); !testutils.IsError(err, `schema_topic is not yet supported`) {
		t.Errorf(`expected "schema_topic is not yet supported" error got: %v`, err)
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
