// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/mocks"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/golang/mock/gomock"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ Sink = (*fakeKafkaSinkV2)(nil)
var _ SinkWithTopics = (*fakeKafkaSinkV2)(nil)

// Dial implements Sink interface. We use it to initialize the fake kafka sink,
// since the test framework doesn't use constructors. We set up our mocks to
// feed records into the channel that the wrapper can read from.
func (s *fakeKafkaSinkV2Bench) Dial() error {
	bs := s.batchingSink
	kc := bs.client.(*kafkaSinkClientV2)
	s.ctrl = gomock.NewController(s.t)
	s.client = mocks.NewMockKafkaClientV2(s.ctrl)
	s.client.EXPECT().ProduceSync(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, msgs ...*kgo.Record) kgo.ProduceResults {
		for _, m := range msgs {
			var key sarama.Encoder
			if m.Key != nil {
				key = sarama.ByteEncoder(m.Key)
			}

			var headers []sarama.RecordHeader
			for _, h := range m.Headers {
				headers = append(headers, sarama.RecordHeader{
					Key:   []byte(h.Key),
					Value: h.Value,
				})
			}

			select {
			case <-ctx.Done():
				return kgo.ProduceResults{kgo.ProduceResult{Err: ctx.Err()}}
			case s.feedCh <- &sarama.ProducerMessage{
				Topic:     m.Topic,
				Key:       key,
				Value:     sarama.ByteEncoder(m.Value),
				Partition: m.Partition,
				Headers:   headers,
			}:
			}
		}
		return nil
	}).AnyTimes()
	s.client.EXPECT().Close().AnyTimes()

	kc.client.Close()
	kc.client = s.client

	s.adminClient = mocks.NewMockKafkaAdminClientV2(s.ctrl)
	s.adminClient.EXPECT().ListTopics(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
		// Say each topic has one partition and one replica.
		td := kadm.TopicDetails{}
		for _, topic := range topics {
			td[topic] = kadm.TopicDetail{
				Topic: topic,
				Partitions: map[int32]kadm.PartitionDetail{
					0: {Topic: topic, Partition: 0, Leader: 0, Replicas: []int32{0}, ISR: []int32{0}},
				},
			}
		}
		return td, nil
	}).AnyTimes()
	kc.adminClient = s.adminClient

	return bs.Dial()
}

type fakeKafkaSinkV2Bench struct {
	*batchingSink
	// For compatibility with all the other fakeKafka test stuff, we convert kgo Records to sarama messages.
	// TODO(#126991): clean this up when we remove the v1 sink.
	feedCh      chan *sarama.ProducerMessage
	t           *testing.B
	ctrl        *gomock.Controller
	client      *mocks.MockKafkaClientV2
	adminClient *mocks.MockKafkaAdminClientV2
}

type benchmarkMetrics struct {
	mu         syncutil.Mutex
	latencies  []time.Duration
	batchSizes []int
	msgCount   int
	startTime  time.Time

	// Computed metrics
	throughput float64
	p50Latency time.Duration
	p95Latency time.Duration
	p99Latency time.Duration
}

func (m *benchmarkMetrics) updateMsgCount(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgCount += count
}

func (m *benchmarkMetrics) recordLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencies = append(m.latencies, d)
}

var _ cdctest.TestFeedFactory = (*kafkaBenchFeedFactory)(nil)

// Primary benchmark factory type
type kafkaBenchFeedFactory struct {
	enterpriseFeedFactory
	knobs *sinkKnobs
	b     *testing.B
}

var _ cdctest.TestFeed = (*kafkaFeed)(nil)

// Benchmark-specific feed implementation
type kafkaBenchFeed struct {
	*jobFeed
	seenTrackerMap
	source   chan *sarama.ProducerMessage
	tg       *teeGroup
	registry *cdctest.SchemaRegistry
	metrics  *benchmarkMetrics // Benchmark-specific
}

var _ cdctest.TestFeedFactory = (*kafkaFeedFactory)(nil)

// makeKafkaFeedFactory returns a TestFeedFactory implementation using the `kafka` uri.
func makeKafkaBenchFeedFactory(
	t *testing.B, srvOrCluster interface{}, rootDB *gosql.DB,
) cdctest.TestFeedFactory {
	return makeKafkaFeedFactoryWithConnectionCheckForBench(t, srvOrCluster, rootDB, false)
}

func makeKafkaFeedFactoryWithConnectionCheckForBench(
	t *testing.B, srvOrCluster interface{}, rootDB *gosql.DB, forceKafkaV1ConnectionCheck bool,
) cdctest.TestFeedFactory {
	s, injectables := getInjectables(srvOrCluster)
	return &kafkaBenchFeedFactory{
		knobs: &sinkKnobs{
			bypassKafkaV1ConnectionCheck: !forceKafkaV1ConnectionCheck,
		},
		enterpriseFeedFactory: enterpriseFeedFactory{
			s:      s,
			db:     rootDB,
			rootDB: rootDB,
			di:     newDepInjector(injectables...),
		},
		b: t,
	}
}

func (k *kafkaBenchFeedFactory) Feed(create string, args ...interface{}) (cdctest.TestFeed, error) {
	parsed, err := parser.ParseOne(create)
	if err != nil {
		return nil, err
	}
	createStmt := parsed.AST.(*tree.CreateChangefeed)

	// Set SinkURI if it wasn't provided.  It's okay if it is -- since we may
	// want to set some kafka specific URI parameters.
	defaultURI := fmt.Sprintf("%s://does.not.matter/", changefeedbase.SinkSchemeKafka)
	if err := setURI(createStmt, defaultURI, true, &args); err != nil {
		return nil, err
	}

	var registry *cdctest.SchemaRegistry
	for _, opt := range createStmt.Options {
		if opt.Key == changefeedbase.OptFormat {
			format, err := exprAsString(opt.Value)
			if err != nil {
				return nil, err
			}
			if format == string(changefeedbase.OptFormatAvro) {
				// Must use confluent schema registry so that we register our schema
				// in order to be able to decode kafka messages.
				registry = cdctest.StartTestSchemaRegistry()
				registryOption := tree.KVOption{
					Key:   changefeedbase.OptConfluentSchemaRegistry,
					Value: tree.NewStrVal(registry.URL()),
				}
				createStmt.Options = append(createStmt.Options, registryOption)
				break
			}
		}
	}

	tg := newTeeGroup()
	// feedCh must have some buffer to hold the messages.
	// basically, sarama is fully async, so we have to be async as well; otherwise, tests deadlock.
	// Fixed sized buffer is probably okay at this point, but we should probably
	// have  a proper fix.
	feedCh := make(chan *sarama.ProducerMessage, 1024)
	wrapSink := func(s Sink) Sink {
		if KafkaV2Enabled.Get(&k.s.ClusterSettings().SV) {
			return &fakeKafkaSinkV2Bench{
				batchingSink: s.(*batchingSink),
				feedCh:       feedCh,
				t:            k.b,
			}
		}

		return &fakeKafkaSink{
			Sink:   s,
			tg:     tg,
			feedCh: feedCh,
			knobs:  k.knobs,
		}
	}

	c := &kafkaBenchFeed{
		jobFeed:        newJobFeed(k.jobsTableConn(), wrapSink),
		seenTrackerMap: make(map[string]struct{}),
		source:         feedCh,
		tg:             tg,
		registry:       registry,
		metrics: &benchmarkMetrics{
			startTime:  timeutil.Now(),
			latencies:  make([]time.Duration, 0),
			batchSizes: make([]int, 0),
		},
	}

	if err := k.startFeedJob(c.jobFeed, tree.AsStringWithFlags(createStmt, tree.FmtShowPasswords), args...); err != nil {
		return nil, errors.CombineErrors(err, c.Close())
	}
	return c, nil
}

// Server implements TestFeedFactory
func (k *kafkaBenchFeedFactory) Server() serverutils.ApplicationLayerInterface {
	return k.s
}

// Next implements TestFeed
func (k *kafkaBenchFeed) Next() (*cdctest.TestFeedMessage, error) {
	start := timeutil.Now()
	defer func() {
		k.metrics.mu.Lock()
		k.metrics.latencies = append(k.metrics.latencies, timeutil.Since(start))
		k.metrics.msgCount++
		k.metrics.mu.Unlock()
	}()

	for {
		var msg *sarama.ProducerMessage
		if err := timeutil.RunWithTimeout(
			context.Background(), timeoutOp("kafka.Next", k.jobID), timeout(),
			func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-k.shutdown:
					return k.terminalJobError()
				case msg = <-k.source:
					return nil
				}
			},
		); err != nil {
			return nil, err
		}

		fm := &cdctest.TestFeedMessage{
			Topic:     msg.Topic,
			Partition: `kafka`, // TODO(yevgeniy): support multiple partitions.
		}

		decode := func(encoded sarama.Encoder, dest *[]byte) error {
			// It's a bit weird to use encoder to get decoded bytes.
			// But it's correct: we produce messages to sarama, and we set
			// key/value to sarama.ByteEncoder(payload) -- and sarama ByteEncoder
			// is just the type alias to []byte -- alas, we can't cast it, so just "encode"
			// it to get back our original byte array.
			decoded, err := encoded.Encode()
			if err != nil {
				return err
			}
			if k.registry == nil {
				*dest = decoded
			} else {
				// Convert avro record to json.
				jsonBytes, err := k.registry.AvroToJSON(decoded)
				if err != nil {
					return err
				}
				*dest = jsonBytes
			}
			return nil
		}

		if msg.Key == nil {
			// It's a resolved timestamp
			if err := decode(msg.Value, &fm.Resolved); err != nil {
				return nil, err
			}
			return fm, nil
		}
		// It's a regular message
		if err := decode(msg.Key, &fm.Key); err != nil {
			return nil, err
		}
		if err := decode(msg.Value, &fm.Value); err != nil {
			return nil, err
		}

		for _, h := range msg.Headers {
			fm.Headers = append(fm.Headers, cdctest.Header{K: string(h.Key), V: h.Value})
		}
		slices.SortFunc(fm.Headers, func(a, b cdctest.Header) int { return strings.Compare(a.K, b.K) })

		if isNew := k.markSeen(fm); isNew {
			return fm, nil
		}
	}
}
func (k *kafkaBenchFeed) Partitions() []string {
	// TODO(yevgeniy): Support multiple partitions.
	return []string{`kafka`}
}

// Close implements TestFeed interface.
// GetMetrics returns the current benchmark metrics
func (k *kafkaBenchFeed) GetMetrics() *benchmarkMetrics {
	return k.metrics
}

func (k *kafkaBenchFeed) Close() error {
	if k.registry != nil {
		defer k.registry.Close()
	}
	return errors.CombineErrors(k.jobFeed.Close(), k.tg.wait())
}
