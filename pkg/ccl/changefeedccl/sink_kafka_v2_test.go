// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/mocks"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl"
)

func TestKafkaSinkClientV2_Resolved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	topics := []string{"t1", "t2", "t3"}

	fx := newKafkaSinkV2Fx(t, withTargets(topics))
	defer fx.close()

	forEachTopic := func(cb func(topic string) error) error {
		for _, topic := range topics {
			if err := cb(topic); err != nil {
				return err
			}
		}
		return nil
	}
	topicDetails := kadm.TopicDetails{
		"t1": kadm.TopicDetail{
			Topic: "t1",
			Partitions: map[int32]kadm.PartitionDetail{
				0: {Topic: "t1", Partition: 0},
				1: {Topic: "t1", Partition: 1},
			},
		},
		"t2": kadm.TopicDetail{
			Topic: "t2",
			Partitions: map[int32]kadm.PartitionDetail{
				0: {Topic: "t2", Partition: 0},
				1: {Topic: "t2", Partition: 1},
			},
		},
		"t3": kadm.TopicDetail{
			Topic: "t3",
			Partitions: map[int32]kadm.PartitionDetail{
				0: {Topic: "t3", Partition: 0},
				1: {Topic: "t3", Partition: 1},
			},
		},
	}
	fx.ac.EXPECT().ListTopics(fx.ctx, "t1", "t2", "t3").Times(1).Return(topicDetails, nil)
	matchers := make([]any, 0, 6)
	for _, topic := range []string{"t1", "t2", "t3"} {
		for _, partition := range []int32{0, 1} {
			matchers = append(matchers, fnMatcher(func(arg any) bool {
				return arg.(*kgo.Record).Topic == topic && arg.(*kgo.Record).Partition == partition
			}))
		}
	}
	fx.kc.EXPECT().ProduceSync(fx.ctx, matchers...)

	require.NoError(t, fx.sink.FlushResolvedPayload(fx.ctx, []byte(`{"resolved" 42}`), forEachTopic, retry.Options{}))
}

func TestKafkaSinkClientV2_Basic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fx := newKafkaSinkV2Fx(t)
	defer fx.close()

	buf := fx.sink.MakeBatchBuffer("t")
	keys := []string{"k1", "k2", "k3"}
	for i, key := range keys {
		buf.Append(context.Background(), []byte(key), []byte(strconv.Itoa(i)), attributes{})
	}
	payload, err := buf.Close()
	require.NoError(t, err)

	fx.kc.EXPECT().ProduceSync(fx.ctx, payload.([]*kgo.Record)).Times(1).Return(nil)

	require.NoError(t, fx.sink.Flush(fx.ctx, payload))
}

func TestKafkaSinkClientV2_Resize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	setup := func(t *testing.T, canResize bool) (*kafkaSinkV2Fx, SinkPayload, []any) {
		fx := newKafkaSinkV2Fx(t, withSettings(func(settings *cluster.Settings) {
			if canResize {
				changefeedbase.BatchReductionRetryEnabled.Override(context.Background(), &settings.SV, true)
			}
		}))
		defer fx.close()

		buf := fx.sink.MakeBatchBuffer("t")
		for i := range 100 {
			buf.Append(context.Background(),
				[]byte("k1"),
				[]byte(strconv.Itoa(i)),
				attributes{
					mvcc: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
				},
			)
		}
		payload, err := buf.Close()
		require.NoError(t, err)

		payloadAnys := make([]any, 0, len(payload.([]*kgo.Record)))
		for _, r := range payload.([]*kgo.Record) {
			payloadAnys = append(payloadAnys, r)
		}

		return fx, payload, payloadAnys
	}

	t.Run("resize disabled", func(t *testing.T) {
		fx, payload, payloadAnys := setup(t, false)
		pr := kgo.ProduceResults{kgo.ProduceResult{Err: fmt.Errorf("..: %w", kerr.MessageTooLarge)}}
		fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys...).Times(1).Return(pr)
		require.Error(t, fx.sink.Flush(fx.ctx, payload))
	})

	t.Run("resize enabled and it keeps failing", func(t *testing.T) {
		fx, payload, payloadAnys := setup(t, true)

		pr := kgo.ProduceResults{kgo.ProduceResult{Err: fmt.Errorf("..: %w", kerr.MessageTooLarge)}}
		// it should keep splitting it in two until it hits size=1
		gomock.InOrder(
			fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys...).Times(1).Return(pr),
			fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys[:50]...).Times(1).Return(pr),
			fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys[:25]...).Times(1).Return(pr),
			fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys[:12]...).Times(1).Return(pr),
			fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys[:6]...).Times(1).Return(pr),
			fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys[:3]...).Times(1).Return(pr),
			fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys[:1]...).Times(1).Return(pr),
		)

		err := fx.sink.Flush(fx.ctx, payload)
		require.Error(t, err)

		// Validate that error includes key, size, and mvcc
		errStr := err.Error()
		require.Contains(t, errStr, "k1", "error should include key")
		require.Regexp(t, `size=\d+`, errStr, "error should include size")
		require.Regexp(t, `mvcc=\d+`, errStr, "error should include mvcc timestamp")
	})

	t.Run("resize enabled and it gets everything", func(t *testing.T) {
		fx, payload, payloadAnys := setup(t, true)

		prErr := kgo.ProduceResults{kgo.ProduceResult{Err: fmt.Errorf("..: %w", kerr.MessageTooLarge)}}
		prOk := kgo.ProduceResults{}
		// fails twice and succeeds at size 25
		gotRecordValues := make(map[string]struct{})
		fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys...).Times(1).Return(prErr)
		fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys[:50]...).MinTimes(1).Return(prErr)
		fx.kc.EXPECT().ProduceSync(fx.ctx, payloadAnys[50:]...).MinTimes(1).Return(prErr)

		fx.kc.EXPECT().ProduceSync(fx.ctx, gomock.Any()).Times(4).DoAndReturn(func(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults {
			require.Len(t, records, 25)
			for _, r := range records {
				gotRecordValues[string(r.Value)] = struct{}{}
			}
			return prOk
		})
		require.NoError(t, fx.sink.Flush(fx.ctx, payload))
		require.Len(t, gotRecordValues, 100)
	})
}

// These are really tests of the TopicNamer and our configuration of it.
func TestKafkaSinkClientV2_Naming(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("escaping", func(t *testing.T) {
		fx := newKafkaSinkV2Fx(t, withTargets([]string{"☃"}))
		defer fx.close()

		produced := make(chan struct{})
		fx.kc.EXPECT().ProduceSync(gomock.Any(), fnMatcher(func(arg any) bool {
			defer close(produced)
			rec := arg.(*kgo.Record)
			return rec.Topic == `_u2603_` && string(rec.Key) == `k☃` && string(rec.Value) == `v☃`
		})).Times(1).Return(nil)

		require.NoError(t, fx.bs.EmitRow(fx.ctx, topic(`☃`), []byte(`k☃`), []byte(`v☃`), nil, zeroTS, zeroTS, zeroAlloc, nil))

		testutils.SucceedsSoon(t, func() error {
			select {
			case <-produced:
				return nil
			default:
				return fmt.Errorf("not yet")
			}
		})
	})

	t.Run("override", func(t *testing.T) {
		fx := newKafkaSinkV2Fx(t, withTargets([]string{"t1", "t2"}), withTopicOverride("general"))
		defer fx.close()

		produced := make(chan struct{})
		fx.kc.EXPECT().ProduceSync(gomock.Any(), fnMatcher(func(arg any) bool {
			defer close(produced)
			rec := arg.(*kgo.Record)
			return rec.Topic == `general` && string(rec.Key) == `k☃` && string(rec.Value) == `v☃`
		})).Times(1).Return(nil)

		require.NoError(t, fx.bs.EmitRow(fx.ctx, topic(`t1`), []byte(`k☃`), []byte(`v☃`), nil, zeroTS, zeroTS, zeroAlloc, nil))

		testutils.SucceedsSoon(t, func() error {
			select {
			case <-produced:
				return nil
			default:
				return fmt.Errorf("not yet")
			}
		})
	})

	t.Run("prefix", func(t *testing.T) {
		fx := newKafkaSinkV2Fx(t, withTargets([]string{"t1", "t2"}), withTopicPrefix("prefix-"), withTopicOverride("☃"))
		defer fx.close()

		produced := make(chan struct{})
		fx.kc.EXPECT().ProduceSync(gomock.Any(), fnMatcher(func(arg any) bool {
			defer close(produced)
			rec := arg.(*kgo.Record)
			return rec.Topic == `prefix-_u2603_` && string(rec.Key) == `k☃` && string(rec.Value) == `v☃`
		})).Times(1).Return(nil)

		require.NoError(t, fx.bs.EmitRow(fx.ctx, topic(`t1`), []byte(`k☃`), []byte(`v☃`), nil, zeroTS, zeroTS, zeroAlloc, nil))

		testutils.SucceedsSoon(t, func() error {
			select {
			case <-produced:
				return nil
			default:
				return fmt.Errorf("not yet")
			}
		})
	})
}

func TestKafkaSinkClientV2_MultipleBrokers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fx := newKafkaSinkV2Fx(t, withRealClient(), withSinkURI("kafka://localhost:9092,localhost:9093"))
	defer fx.close()

	client := fx.bs.client.(*kafkaSinkClientV2).client.(*kgo.Client)
	brokers := client.OptValue("SeedBrokers").([]string)
	require.Equal(t, []string{"localhost:9092", "localhost:9093"}, brokers)

}

func TestKafkaSinkClientV2_Opts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseExpectedOpts := map[string]any{
		"ClientID":                            "CockroachDB",
		"ProducerBatchCompression":            []kgo.CompressionCodec{kgo.NoCompression()},
		"RequiredAcks":                        kgo.LeaderAck(),
		"MaxVersions":                         kversion.Stable(), // We don't set this but it's kgo's default.
		"DialTLSConfig":                       (*tls.Config)(nil),
		"SASL":                                ([]sasl.Mechanism)(nil),
		"DisableIdempotentWrite":              true,
		"MaxProduceRequestsInflightPerBroker": 5,
		"MaxBufferedRecords":                  int64(1000), // This is the default that we set.
	}
	baseBatchCfg := sinkBatchConfig{}

	cases := []struct {
		name       string
		jsonConfig map[string]any
		// Both expectedOpts and expectedBatchCfg will be merged with their respective base* before comparison.
		expectedOpts                map[string]any
		expectedBatchConfig         sinkBatchConfig
		expectedBatchingSinkMinFreq time.Duration
	}{
		{
			name: "default",
		},
		{
			name: "client id",
			jsonConfig: map[string]any{
				"ClientID": "test",
			},
			expectedOpts: map[string]any{
				"ClientID": "test",
			},
		},
		{
			name: "compression",
			jsonConfig: map[string]any{
				"Compression": "lz4",
			},
			expectedOpts: map[string]any{
				"ProducerBatchCompression": []kgo.CompressionCodec{kgo.Lz4Compression()},
			},
		},
		{
			name: "required acks",
			jsonConfig: map[string]any{
				"RequiredAcks": "0",
			},
			expectedOpts: map[string]any{
				"RequiredAcks": kgo.NoAck(),
			},
		},
		{
			name: "flush",
			jsonConfig: map[string]any{
				"Flush": map[string]any{
					"Messages":  99,
					"Frequency": "100ms", // Must set frequency to set messages, otherwise it's a validation error.
				},
			},
			expectedBatchConfig: sinkBatchConfig{
				Messages:  99,
				Frequency: jsonDuration(100 * time.Millisecond),
			},
		},
		{
			name: "lots of options",
			jsonConfig: map[string]any{
				"ClientID":     "test",
				"Compression":  "gzip",
				"RequiredAcks": "ALL",
				"Version":      "0.8.2.2",
				"Flush": map[string]any{
					"Messages":    100,
					"Bytes":       1000,
					"Frequency":   "2s",
					"MaxMessages": 2000,
				},
			},
			expectedOpts: map[string]any{
				"ClientID":                 "test",
				"ProducerBatchCompression": []kgo.CompressionCodec{kgo.GzipCompression()},
				"RequiredAcks":             kgo.AllISRAcks(),
				"MaxVersions":              kversion.V0_8_2(),
				"MaxBufferedRecords":       int64(2000),
			},
			expectedBatchConfig: sinkBatchConfig{
				Frequency: jsonDuration(2 * time.Second),
				Messages:  100,
				Bytes:     1000,
			},
			expectedBatchingSinkMinFreq: 2 * time.Second,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			jsonBs, err := json.Marshal(c.jsonConfig)
			require.NoError(t, err)

			fx := newKafkaSinkV2Fx(t, withJSONConfig(string(jsonBs)), withRealClient())
			defer fx.close()

			expectedOpts := shallowMerge(baseExpectedOpts, c.expectedOpts)
			expectedBatchCfg := mergeBatchConfig(baseBatchCfg, c.expectedBatchConfig)

			client := fx.bs.client.(*kafkaSinkClientV2).client.(*kgo.Client)
			batchCfg := fx.bs.client.(*kafkaSinkClientV2).batchCfg
			for k, v := range expectedOpts {
				val := client.OptValue(k)
				assert.Equal(t, v, val, "opt %q has value %+#v, expected %+#v", k, val, v)
			}
			assert.Equal(t, expectedBatchCfg, batchCfg)
			if c.expectedBatchingSinkMinFreq != 0 {
				assert.Equal(t, c.expectedBatchingSinkMinFreq, fx.bs.minFlushFrequency)
			}
		})
	}
}

func TestKafkaTopicNamesForTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, cleanup := makeServer(t)
	defer cleanup()

	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING, FAMILY f1 (a, b), FAMILY f2 (c))`)

	execCfg := s.Server.ExecutorConfig().(sql.ExecutorConfig)
	tableDesc := cdctest.GetHydratedTableDescriptor(t, execCfg, "d", "foo")
	makeTargets := func(targets ...changefeedbase.Target) changefeedbase.Targets {
		res := changefeedbase.Targets{}
		for _, target := range targets {
			res.Add(target)
		}
		return res
	}

	baseTarget := changefeedbase.Target{
		DescID:            tableDesc.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(tableDesc.GetName()),
	}

	for _, tc := range []struct {
		name     string
		sinkURI  string
		targets  changefeedbase.Targets
		expected []string
	}{
		{
			name:    "primary family only",
			sinkURI: "kafka://localhost:9092?topic_prefix=pre-",
			targets: makeTargets(changefeedbase.Target{
				Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
				DescID:            baseTarget.DescID,
				StatementTimeName: baseTarget.StatementTimeName,
			}),
			expected: []string{"pre-foo"},
		},
		{
			name:    "column family",
			sinkURI: "kafka://localhost:9092?topic_prefix=pre-",
			targets: makeTargets(changefeedbase.Target{
				Type:              jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
				DescID:            baseTarget.DescID,
				FamilyName:        "f2",
				StatementTimeName: baseTarget.StatementTimeName,
			}),
			expected: []string{"pre-foo.f2"},
		},
		{
			name:    "each family",
			sinkURI: "kafka://localhost:9092?topic_prefix=pre-",
			targets: makeTargets(changefeedbase.Target{
				Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
				DescID:            baseTarget.DescID,
				StatementTimeName: baseTarget.StatementTimeName,
			}),
			expected: []string{"pre-foo.f1", "pre-foo.f2"},
		},
		{
			name:    "single topic override",
			sinkURI: "kafka://localhost:9092?topic_prefix=pre-&topic_name=all",
			targets: makeTargets(changefeedbase.Target{
				Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
				DescID:            baseTarget.DescID,
				StatementTimeName: baseTarget.StatementTimeName,
			}),
			expected: []string{"pre-all"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsedURL, err := url.Parse(tc.sinkURI)
			require.NoError(t, err)

			topics, err := kafkaTopicNamesForTargets(
				ctx, &execCfg, tc.targets, &changefeedbase.SinkURL{URL: parsedURL}, s.Server.Clock().Now(),
			)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.expected, topics)
		})
	}
}

// TestMaybeCreateKafkaTopicsPreconditions exercises the early-exit and
// validation paths of maybeCreateKafkaTopics: it should no-op for non-explicit
// option values and non-Kafka sinks, and surface a clear error when the v2
// kafka sink is not enabled.
func TestMaybeCreateKafkaTopicsPreconditions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, cleanup := makeServer(t)
	defer cleanup()
	execCfg := s.Server.ExecutorConfig().(sql.ExecutorConfig)

	// failOnClient ensures we never construct a real kafka client in cases
	// where maybeCreateKafkaTopics should bail before reaching that point.
	failOnClient := kafkaSinkV2Knobs{
		OverrideClient: func(opts []kgo.Opt) (KafkaClientV2, KafkaAdminClientV2) {
			t.Fatal("unexpected kafka client creation")
			return nil, nil
		},
	}

	explicit := map[string]string{
		changefeedbase.OptCreateKafkaTopics: string(changefeedbase.CreateKafkaTopicsExplicit),
	}
	brokerAuto := map[string]string{
		changefeedbase.OptCreateKafkaTopics: string(changefeedbase.CreateKafkaTopicsBrokerAuto),
	}
	off := map[string]string{
		changefeedbase.OptCreateKafkaTopics: string(changefeedbase.CreateKafkaTopicsOff),
	}

	cases := []struct {
		name           string
		sinkURI        string
		opts           map[string]string
		kafkaV2Enabled bool
		expectedErr    string
	}{
		{
			name:           "default option is no-op",
			sinkURI:        "kafka://localhost:9092",
			kafkaV2Enabled: true,
		},
		{
			name:           "broker_auto option is no-op",
			sinkURI:        "kafka://localhost:9092",
			opts:           brokerAuto,
			kafkaV2Enabled: true,
		},
		{
			name:           "off option is no-op",
			sinkURI:        "kafka://localhost:9092",
			opts:           off,
			kafkaV2Enabled: true,
		},
		{
			name:           "non-kafka sink is no-op",
			sinkURI:        "null://",
			opts:           explicit,
			kafkaV2Enabled: true,
		},
		{
			name:           "kafka v2 disabled is an error",
			sinkURI:        "kafka://localhost:9092",
			opts:           explicit,
			kafkaV2Enabled: false,
			expectedErr:    "requires the v2 kafka sink",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			KafkaV2Enabled.Override(ctx, &execCfg.Settings.SV, tc.kafkaV2Enabled)
			err := maybeCreateKafkaTopics(ctx, &execCfg, jobspb.ChangefeedDetails{
				SinkURI: tc.sinkURI,
				Opts:    tc.opts,
			}, changefeedbase.Targets{}, hlc.Timestamp{}, failOnClient)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateKafkaTopicsOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name        string
		opts        map[string]string
		expected    changefeedbase.CreateKafkaTopics
		expectedErr string
	}{
		{
			name:     "default",
			expected: changefeedbase.CreateKafkaTopicsBrokerAuto,
		},
		{
			name:     "broker_auto",
			opts:     map[string]string{changefeedbase.OptCreateKafkaTopics: string(changefeedbase.CreateKafkaTopicsBrokerAuto)},
			expected: changefeedbase.CreateKafkaTopicsBrokerAuto,
		},
		{
			name:     "explicit",
			opts:     map[string]string{changefeedbase.OptCreateKafkaTopics: string(changefeedbase.CreateKafkaTopicsExplicit)},
			expected: changefeedbase.CreateKafkaTopicsExplicit,
		},
		{
			name:     "bare option",
			opts:     map[string]string{changefeedbase.OptCreateKafkaTopics: ""},
			expected: changefeedbase.CreateKafkaTopicsExplicit,
		},
		{
			name:     "off",
			opts:     map[string]string{changefeedbase.OptCreateKafkaTopics: string(changefeedbase.CreateKafkaTopicsOff)},
			expected: changefeedbase.CreateKafkaTopicsOff,
		},
		{
			name:        "invalid option",
			opts:        map[string]string{changefeedbase.OptCreateKafkaTopics: "yes"},
			expectedErr: "unknown create_kafka_topics: yes",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := changefeedbase.MakeStatementOptions(tc.opts)
			got, err := opts.GetCreateKafkaTopics()
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestCreateMissingKafkaTopics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	knobs := kafkaSinkV2Knobs{SkipCreateTopicVersionCheck: true}
	existingTopic := func(topic string) kadm.TopicDetail {
		return kadm.TopicDetail{
			Topic: topic,
			Partitions: map[int32]kadm.PartitionDetail{
				0: {Topic: topic, Partition: 0, Leader: 0, Replicas: []int32{0}, ISR: []int32{0}},
			},
		}
	}
	unknownTopic := func(topic string) kadm.TopicDetail {
		return kadm.TopicDetail{Topic: topic, Err: kerr.UnknownTopicOrPartition}
	}

	t.Run("all topics already exist", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		adminClient := mocks.NewMockKafkaAdminClientV2(ctrl)
		adminClient.EXPECT().ListTopics(gomock.Any(), "a", "b").Return(kadm.TopicDetails{
			"a": existingTopic("a"),
			"b": existingTopic("b"),
		}, nil)

		require.NoError(t, createMissingKafkaTopics(ctx, adminClient, []string{"a", "b"}, knobs))
	})

	t.Run("creates only missing topics", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		adminClient := mocks.NewMockKafkaAdminClientV2(ctrl)
		adminClient.EXPECT().ListTopics(gomock.Any(), "a", "b", "c").Return(kadm.TopicDetails{
			"a": existingTopic("a"),
			"b": unknownTopic("b"),
		}, nil)
		adminClient.EXPECT().ValidateCreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "b", "c").Return(kadm.CreateTopicResponses{
			"b": {Topic: "b"},
			"c": {Topic: "c"},
		}, nil)
		adminClient.EXPECT().CreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "b", "c").Return(kadm.CreateTopicResponses{
			"b": {Topic: "b", NumPartitions: 3, ReplicationFactor: 1},
			"c": {Topic: "c", NumPartitions: 3, ReplicationFactor: 1},
		}, nil)

		require.NoError(t, createMissingKafkaTopics(ctx, adminClient, []string{"a", "b", "c"}, knobs))
	})

	// Each error path simply propagates an error from the admin client. The
	// table covers both top-level errors (returned directly from the call) and
	// per-topic errors (returned in the response map).
	t.Run("error propagation", func(t *testing.T) {
		cases := []struct {
			name        string
			setupMock   func(*mocks.MockKafkaAdminClientV2)
			expectedErr string
		}{
			{
				name: "list topics top-level error",
				setupMock: func(m *mocks.MockKafkaAdminClientV2) {
					m.EXPECT().ListTopics(gomock.Any(), "a").Return(nil, errors.New("list failed"))
				},
				expectedErr: "list failed",
			},
			{
				name: "list topics per-topic error",
				setupMock: func(m *mocks.MockKafkaAdminClientV2) {
					m.EXPECT().ListTopics(gomock.Any(), "a").Return(kadm.TopicDetails{
						"a": {Topic: "a", Err: errors.New("topic list failed")},
					}, nil)
				},
				expectedErr: "topic list failed",
			},
			{
				name: "validate top-level error",
				setupMock: func(m *mocks.MockKafkaAdminClientV2) {
					m.EXPECT().ListTopics(gomock.Any(), "a").Return(kadm.TopicDetails{"a": unknownTopic("a")}, nil)
					m.EXPECT().ValidateCreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "a").Return(nil, errors.New("validate failed"))
				},
				expectedErr: "validate failed",
			},
			{
				name: "validate per-topic error",
				setupMock: func(m *mocks.MockKafkaAdminClientV2) {
					m.EXPECT().ListTopics(gomock.Any(), "a").Return(kadm.TopicDetails{"a": unknownTopic("a")}, nil)
					m.EXPECT().ValidateCreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "a").Return(kadm.CreateTopicResponses{
						"a": {Topic: "a", Err: errors.New("topic validate failed")},
					}, nil)
				},
				expectedErr: "topic validate failed",
			},
			{
				name: "create top-level error",
				setupMock: func(m *mocks.MockKafkaAdminClientV2) {
					m.EXPECT().ListTopics(gomock.Any(), "a").Return(kadm.TopicDetails{"a": unknownTopic("a")}, nil)
					m.EXPECT().ValidateCreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "a").Return(kadm.CreateTopicResponses{
						"a": {Topic: "a"},
					}, nil)
					m.EXPECT().CreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "a").Return(nil, errors.New("create failed"))
				},
				expectedErr: "create failed",
			},
			{
				name: "create per-topic error",
				setupMock: func(m *mocks.MockKafkaAdminClientV2) {
					m.EXPECT().ListTopics(gomock.Any(), "a").Return(kadm.TopicDetails{"a": unknownTopic("a")}, nil)
					m.EXPECT().ValidateCreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "a").Return(kadm.CreateTopicResponses{
						"a": {Topic: "a"},
					}, nil)
					m.EXPECT().CreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "a").Return(kadm.CreateTopicResponses{
						"a": {Topic: "a", Err: errors.New("topic create failed")},
					}, nil)
				},
				expectedErr: "topic create failed",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				adminClient := mocks.NewMockKafkaAdminClientV2(ctrl)
				tc.setupMock(adminClient)
				require.ErrorContains(t, createMissingKafkaTopics(ctx, adminClient, []string{"a"}, knobs), tc.expectedErr)
			})
		}
	})

	t.Run("topic already exists race", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		adminClient := mocks.NewMockKafkaAdminClientV2(ctrl)
		adminClient.EXPECT().ListTopics(gomock.Any(), "a").Return(kadm.TopicDetails{
			"a": unknownTopic("a"),
		}, nil)
		adminClient.EXPECT().ValidateCreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "a").Return(kadm.CreateTopicResponses{
			"a": {Topic: "a"},
		}, nil)
		adminClient.EXPECT().CreateTopics(gomock.Any(), int32(-1), int16(-1), nil, "a").Return(kadm.CreateTopicResponses{
			"a": {Topic: "a", Err: kerr.TopicAlreadyExists},
		}, nil)

		require.NoError(t, createMissingKafkaTopics(ctx, adminClient, []string{"a"}, knobs))
	})
}

func TestChangefeedCreateKafkaTopicsExplicit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	kafkaClient := mocks.NewMockKafkaClientV2(ctrl)
	adminClient := mocks.NewMockKafkaAdminClientV2(ctrl)
	kafkaClient.EXPECT().Close().AnyTimes()

	const expectedTopic = "pre-foo"
	var topicsCreated atomic.Bool
	topicCreatedCh := make(chan struct{})
	adminClient.EXPECT().ListTopics(gomock.Any(), expectedTopic).DoAndReturn(
		func(ctx context.Context, topics ...string) (kadm.TopicDetails, error) {
			require.Equal(t, []string{expectedTopic}, topics)
			detail := kadm.TopicDetail{
				Topic: expectedTopic,
				Partitions: map[int32]kadm.PartitionDetail{
					0: {Topic: expectedTopic, Partition: 0, Leader: 0, Replicas: []int32{0}, ISR: []int32{0}},
				},
			}
			if !topicsCreated.Load() {
				detail.Err = kerr.UnknownTopicOrPartition
				detail.Partitions = nil
			}
			return kadm.TopicDetails{expectedTopic: detail}, nil
		},
	).AnyTimes()
	adminClient.EXPECT().ValidateCreateTopics(gomock.Any(), int32(-1), int16(-1), nil, expectedTopic).Return(kadm.CreateTopicResponses{
		expectedTopic: {Topic: expectedTopic},
	}, nil)
	adminClient.EXPECT().CreateTopics(gomock.Any(), int32(-1), int16(-1), nil, expectedTopic).DoAndReturn(
		func(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
			require.Equal(t, []string{expectedTopic}, topics)
			if topicsCreated.CompareAndSwap(false, true) {
				close(topicCreatedCh)
			}
			return kadm.CreateTopicResponses{
				expectedTopic: {Topic: expectedTopic, NumPartitions: 1, ReplicationFactor: 1},
			}, nil
		},
	)

	s, cleanup := makeServer(t, feedTestNoTenants, withKnobsFn(func(knobs *base.TestingKnobs) {
		if knobs.DistSQL == nil {
			knobs.DistSQL = &execinfra.TestingKnobs{}
		}
		if knobs.DistSQL.(*execinfra.TestingKnobs).Changefeed == nil {
			knobs.DistSQL.(*execinfra.TestingKnobs).Changefeed = &TestingKnobs{}
		}
		cfKnobs := knobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
		cfKnobs.KafkaSinkV2Knobs = kafkaSinkV2Knobs{
			OverrideClient: func(opts []kgo.Opt) (KafkaClientV2, KafkaAdminClientV2) {
				return kafkaClient, adminClient
			},
			SkipCreateTopicVersionCheck: true,
		}
	}))
	defer cleanup()

	execCfg := s.Server.ExecutorConfig().(sql.ExecutorConfig)
	KafkaV2Enabled.Override(ctx, &execCfg.Settings.SV, true)
	sqlDB := sqlutils.MakeSQLRunner(s.DB)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)

	var jobID int
	sqlDB.QueryRow(t,
		`CREATE CHANGEFEED FOR foo INTO 'kafka://localhost:9092?topic_prefix=pre-' WITH initial_scan = 'no', create_kafka_topics = 'explicit'`,
	).Scan(&jobID)
	defer sqlDB.Exec(t, `CANCEL JOB $1`, jobID)

	select {
	case <-topicCreatedCh:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for changefeed resumer to create kafka topics")
	}
}

func shallowMerge(a, b map[string]any) map[string]any {
	res := make(map[string]any, len(a))
	for k, v := range a {
		res[k] = v
	}
	for k, v := range b {
		res[k] = v
	}
	return res
}

func mergeBatchConfig(a, b sinkBatchConfig) sinkBatchConfig {
	if b.Frequency != 0 {
		a.Frequency = b.Frequency
	}
	if b.Messages != 0 {
		a.Messages = b.Messages
	}
	if b.Bytes != 0 {
		a.Bytes = b.Bytes
	}
	return a
}

func TestKafkaSinkClientV2_CompressionOpts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cases := []struct {
		name         string
		codec, level string
		expected     kgo.CompressionCodec
		shouldErr    bool
	}{
		{
			name:     "default",
			expected: kgo.NoCompression(),
		},
		{
			name:     "gzip no level",
			codec:    "GZIP",
			expected: kgo.GzipCompression(),
		},
		{
			name:     "gzip level zero (not the default)",
			codec:    "GZIP",
			level:    "0",
			expected: kgo.GzipCompression().WithLevel(0),
		},
		{
			name:     "gzip level 9",
			codec:    "GZIP",
			level:    "9",
			expected: kgo.GzipCompression().WithLevel(9),
		},
		{
			name:     "gzip level -1",
			codec:    "GZIP",
			level:    "-1",
			expected: kgo.GzipCompression().WithLevel(-1),
		},
		{
			name:     "gzip level -2",
			codec:    "GZIP",
			level:    "-2",
			expected: kgo.GzipCompression().WithLevel(-2),
		},
		{
			name:     "snappy no level",
			codec:    "SNAPPY",
			expected: kgo.SnappyCompression(),
		},
		{
			name:     "lz4 no level",
			codec:    "LZ4",
			expected: kgo.Lz4Compression(),
		},
		{
			name:     "lz4 level 1024",
			codec:    "LZ4",
			level:    "1024",
			expected: kgo.Lz4Compression().WithLevel(1024),
		},
		{
			name:     "zstd no level",
			codec:    "ZSTD",
			expected: kgo.ZstdCompression(),
		},
		{
			name:     "zstd level 4",
			codec:    "ZSTD",
			level:    "4",
			expected: kgo.ZstdCompression().WithLevel(4),
		},
		{
			name:      "invalid gzip level",
			codec:     "GZIP",
			level:     "100",
			shouldErr: true,
		},
		{
			name:      "invalid gzip level '-3'",
			codec:     "GZIP",
			level:     "-3",
			shouldErr: true,
		},
		{
			name:      "invalid snappy level",
			codec:     "SNAPPY",
			level:     "1", // Snappy doesn't have levels.
			shouldErr: true,
		},
		{
			name:      "invalid zstd level",
			codec:     "ZSTD",
			level:     "10", // ZSTD has a valid range of [1, 4].
			shouldErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opt := map[string]any{}
			if c.codec != "" {
				opt["Compression"] = c.codec
			}
			if c.level != "" {
				i, err := strconv.Atoi(c.level)
				require.NoError(t, err)
				opt["CompressionLevel"] = i
			}
			jbs, err := json.Marshal(opt)
			require.NoError(t, err)

			var createErr error
			fx := newKafkaSinkV2Fx(t, withJSONConfig(string(jbs)), withRealClient(), withCreateClientErrorCb(func(err error) { createErr = err }))
			defer fx.close()

			if c.shouldErr {
				require.Error(t, createErr)
				return
			} else {
				require.NoError(t, createErr)
			}

			client := fx.bs.client.(*kafkaSinkClientV2).client.(*kgo.Client)
			val := client.OptValue("ProducerBatchCompression").([]kgo.CompressionCodec)
			assert.Equal(t, []kgo.CompressionCodec{c.expected}, val)
		})
	}

}

func TestKafkaSinkClientV2_ErrorsEventually(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This will make a real kafka client but with a bogus address, so it should
	// fail to produce.
	fx := newKafkaSinkV2Fx(t, withRealClient(), withKOptsClient([]kgo.Opt{kgo.RecordDeliveryTimeout(1 * time.Second)}))
	defer fx.close()

	buf := fx.sink.MakeBatchBuffer("t")
	buf.Append(context.Background(), []byte("k1"), []byte("v1"), attributes{})
	payload, err := buf.Close()
	require.NoError(t, err)

	require.Error(t, fx.sink.Flush(fx.ctx, payload))
}

func TestKafkaSinkClientV2_PartitionsSameAsV1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	kp := newKgoChangefeedPartitioner("").ForTopic("t")
	sp := newChangefeedPartitioner("t")

	rng, _ := randutil.NewTestRand()
	for numParts := 1; numParts <= 100; numParts++ {
		t.Run(strconv.Itoa(numParts), func(t *testing.T) {
			for i := 0; i < 10_000; i++ {
				// Test that the partitioners give the same results for arbitrary keys.
				key := []byte(strconv.Itoa(rng.Int()))
				kgoPart := int32(kp.Partition(&kgo.Record{Key: key}, numParts))
				saramaPart, err := sp.Partition(&sarama.ProducerMessage{Key: sarama.ByteEncoder(key)}, int32(numParts))
				require.NoError(t, err)
				assert.Equal(t, saramaPart, kgoPart, "key %s with %d partitions", key, numParts)
			}

			// ...And for nil keys and hardcoded partitions.
			hardcodedPart := int32(rng.Int() % numParts)
			kgoPart := int32(kp.Partition(&kgo.Record{Key: nil, Partition: hardcodedPart}, numParts))
			assert.Equal(t, kgoPart, hardcodedPart, "nil key with %d partitions and hardcoded partition %d", numParts, hardcodedPart)
			// However, note that the v1 kafka sink, actually *does not*
			// send nil-keyed (resolved) messages to the partition it asked
			// for. See https://github.com/cockroachdb/cockroach/issues/122666. This is a bug and we are fixing it here in the new version.

		})
	}
}

// TestPartitionAlg tests that different hash methods map to the
// expected partitions for the same keys. This could fail if the kgo
// default hash algorithm changes.
func TestPartitionAlg(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defaultPartitioner := newKgoChangefeedPartitioner("").ForTopic("test-topic")
	fnvPartitioner := newKgoChangefeedPartitioner("fnv-1a").ForTopic("test-topic")
	murmur2Partitioner := newKgoChangefeedPartitioner("murmur2").ForTopic("test-topic")

	// Test 2 keys for better odds of catching a change in behavior.
	key1 := []byte(strconv.Itoa(42))
	key2 := []byte(strconv.Itoa(43))
	numPartitions := 100

	fnvPart1 := fnvPartitioner.Partition(&kgo.Record{Key: key1}, numPartitions)
	fnvPart2 := fnvPartitioner.Partition(&kgo.Record{Key: key2}, numPartitions)
	require.Equal(t, 85, fnvPart1)
	require.Equal(t, 4, fnvPart2)

	// Default is fnv-1a.
	defaultPart1 := defaultPartitioner.Partition(&kgo.Record{Key: key1}, numPartitions)
	defaultPart2 := defaultPartitioner.Partition(&kgo.Record{Key: key2}, numPartitions)
	require.Equal(t, 85, defaultPart1)
	require.Equal(t, 4, defaultPart2)

	murmur2Part1 := murmur2Partitioner.Partition(&kgo.Record{Key: key1}, numPartitions)
	murmur2Part2 := murmur2Partitioner.Partition(&kgo.Record{Key: key2}, numPartitions)
	require.Equal(t, 72, murmur2Part1)
	require.Equal(t, 94, murmur2Part2)
}

// kafkaSinkV2Fx is a test fixture for testing the v2 kafka sink. It supports a
// variety of options via `fxOpt`s passed into its constructor.
type kafkaSinkV2Fx struct {
	t        *testing.T
	settings *cluster.Settings
	ctx      context.Context
	kc       *mocks.MockKafkaClientV2
	ac       *mocks.MockKafkaAdminClientV2
	mockCtrl *gomock.Controller

	// set with fxOpts to modify the created sinks
	targetNames         []string
	topicOverride       string
	topicPrefix         string
	sinkJSONConfig      changefeedbase.SinkSpecificJSONConfig
	batchConfig         sinkBatchConfig
	realClient          bool
	additionalKOpts     []kgo.Opt
	createTopics        changefeedbase.CreateKafkaTopics
	createClientErrorCb func(error)
	uri                 string

	sink *kafkaSinkClientV2
	bs   *batchingSink
}

type fxOpt func(*kafkaSinkV2Fx)

func withSettings(cb func(*cluster.Settings)) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		cb(fx.settings)
	}
}

func withTargets(ts []string) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.targetNames = ts
	}
}

func withTopicOverride(override string) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.topicOverride = override
	}
}

func withTopicPrefix(prefix string) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.topicPrefix = prefix
	}
}

func withJSONConfig(cfg string) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.sinkJSONConfig = changefeedbase.SinkSpecificJSONConfig(cfg)
	}
}

func withRealClient() fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.realClient = true
	}
}

func withSinkURI(uri string) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.uri = uri
	}
}

func withKOptsClient(kOpts []kgo.Opt) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.additionalKOpts = kOpts
	}
}

func withCreateTopics(createTopics changefeedbase.CreateKafkaTopics) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.createTopics = createTopics
	}
}

func withCreateClientErrorCb(cb func(error)) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.createClientErrorCb = cb
	}
}

func newKafkaSinkV2Fx(t *testing.T, opts ...fxOpt) *kafkaSinkV2Fx {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	kc := mocks.NewMockKafkaClientV2(ctrl)
	ac := mocks.NewMockKafkaAdminClientV2(ctrl)

	settings := cluster.MakeTestingClusterSettings()

	fx := &kafkaSinkV2Fx{
		t:            t,
		settings:     settings,
		ctx:          ctx,
		kc:           kc,
		ac:           ac,
		mockCtrl:     ctrl,
		targetNames:  []string{"t"},
		createTopics: changefeedbase.CreateKafkaTopicsBrokerAuto,
	}

	for _, opt := range opts {
		opt(fx)
	}

	var knobs kafkaSinkV2Knobs

	if !fx.realClient {
		knobs.OverrideClient = func(opts []kgo.Opt) (KafkaClientV2, KafkaAdminClientV2) {
			return kc, ac
		}
	}

	uri := "kafka://localhost:9092"
	if fx.uri != "" {
		uri = fx.uri
	}

	var err error
	fx.sink, err = newKafkaSinkClientV2(ctx, fx.additionalKOpts,
		fx.batchConfig, uri, settings, knobs, nilMetricsRecorderBuilder,
		nil, nil, "" /* partitionAlg */, fx.createTopics)
	if err != nil && fx.createClientErrorCb != nil {
		fx.createClientErrorCb(err)
		return fx
	}
	require.NoError(t, err)

	targets := makeChangefeedTargets(fx.targetNames...)

	u, err := url.Parse(uri)
	require.NoError(t, err)

	q := u.Query()
	if fx.topicOverride != "" {
		q.Set("topic_name", fx.topicOverride)
	}
	if fx.topicPrefix != "" {
		q.Set("topic_prefix", fx.topicPrefix)
	}
	u.RawQuery = q.Encode()

	bs, err := makeKafkaSinkV2(ctx, &changefeedbase.SinkURL{URL: u}, targets, changefeedbase.KafkaSinkOptions{
		JSONConfig: fx.sinkJSONConfig,
	}, 1, nilPacerFactory, timeutil.DefaultTimeSource{}, settings, nilMetricsRecorderBuilder, knobs, fx.createTopics)
	if err != nil && fx.createClientErrorCb != nil {
		fx.createClientErrorCb(err)
		return fx
	}
	require.NoError(t, err)
	fx.bs = bs.(*batchingSink)

	return fx
}

func (fx *kafkaSinkV2Fx) close() {
	if fx.sink != nil {
		if _, ok := fx.sink.client.(*mocks.MockKafkaClientV2); ok {
			fx.kc.EXPECT().Close().AnyTimes()
		}
		require.NoError(fx.t, fx.sink.Close())
	}
	if fx.bs != nil {
		require.NoError(fx.t, fx.bs.Close())
	}
}

type fnMatcher func(arg any) bool

func (f fnMatcher) Matches(x any) bool {
	return f(x)
}

func (f fnMatcher) String() string {
	return "matching function"
}

var _ gomock.Matcher = fnMatcher(nil)

func TestMaxRequestSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name           string
		clusterSetting int64
		// perChangefeed is the value set in the config option on the changefeed
		// as MaxBytes in the json.
		perChangefeed int

		// wantValue is the expected value of the kgo "ProducerBatchMaxBytes"
		// option. If we expect the setting to be valid we assert that the
		// client gets configured with the expected value.
		wantValue int32
		// wantErrContains is a substring that we expect to be contained in
		// the error message if we expect client creation to fail.
		wantErrContains string
	}{
		{
			name:      "default",
			wantValue: int32(changefeedbase.KafkaMaxRequestSizeLimit),
		},
		{
			name:           "cluster_setting",
			clusterSetting: 1 << 20,
			wantValue:      int32(1 << 20),
		},
		{
			name:          "per_changefeed_override",
			perChangefeed: 2 << 20,
			wantValue:     int32(2 << 20),
		},
		{
			name:           "per_changefeed_overrides_cluster_setting",
			clusterSetting: 1 << 20,
			perChangefeed:  4 << 20,
			wantValue:      int32(4 << 20),
		},
		{
			name:           "falls_back_to_cluster_setting",
			clusterSetting: 3 << 20,
			wantValue:      int32(3 << 20),
		},
		{
			name:          "boundary_minimum_accepted",
			perChangefeed: changefeedbase.KafkaMaxRequestSizeMin,
			wantValue:     int32(changefeedbase.KafkaMaxRequestSizeMin),
		},
		{
			name:          "boundary_maximum_accepted",
			perChangefeed: changefeedbase.KafkaMaxRequestSizeLimit,
			wantValue:     int32(changefeedbase.KafkaMaxRequestSizeLimit),
		},
		{
			name:            "validation_too_small",
			perChangefeed:   100,
			wantErrContains: "at least 512",
		},
		{
			name:            "validation_too_large",
			perChangefeed:   256<<20 + 1,
			wantErrContains: fmt.Sprintf("at most %d", changefeedbase.KafkaMaxRequestSizeLimit),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var opts []fxOpt
			opts = append(opts, withRealClient())

			if tc.clusterSetting != 0 {
				v := tc.clusterSetting
				opts = append(opts, withSettings(func(s *cluster.Settings) {
					changefeedbase.KafkaMaxRequestSize.Override(
						context.Background(), &s.SV, v,
					)
				}))
			}
			if tc.perChangefeed != 0 {
				jsonBs, err := json.Marshal(map[string]any{
					"Flush": map[string]any{
						"MaxBytes": tc.perChangefeed,
					},
				})
				require.NoError(t, err)
				opts = append(opts, withJSONConfig(string(jsonBs)))
			}

			var createErr error
			opts = append(opts, withCreateClientErrorCb(func(err error) {
				createErr = err
			}))

			fx := newKafkaSinkV2Fx(t, opts...)
			defer fx.close()

			if tc.wantErrContains != "" {
				require.Error(t, createErr)
				require.Contains(t, createErr.Error(), tc.wantErrContains)
				return
			}
			require.NoError(t, createErr)

			client := fx.bs.client.(*kafkaSinkClientV2).client.(*kgo.Client)
			actual := client.OptValue("ProducerBatchMaxBytes").(int32)
			require.Equal(t, tc.wantValue, actual)
		})
	}
}
