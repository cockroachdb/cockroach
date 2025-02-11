// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/mocks"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
		buf.Append([]byte(key), []byte(strconv.Itoa(i)), attributes{})
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
			buf.Append([]byte("k1"), []byte(strconv.Itoa(i)), attributes{})
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
		require.Error(t, fx.sink.Flush(fx.ctx, payload))
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

		require.NoError(t, fx.bs.EmitRow(fx.ctx, topic(`☃`), []byte(`k☃`), []byte(`v☃`), zeroTS, zeroTS, zeroAlloc))

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

		require.NoError(t, fx.bs.EmitRow(fx.ctx, topic(`t1`), []byte(`k☃`), []byte(`v☃`), zeroTS, zeroTS, zeroAlloc))

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

		require.NoError(t, fx.bs.EmitRow(fx.ctx, topic(`t1`), []byte(`k☃`), []byte(`v☃`), zeroTS, zeroTS, zeroAlloc))

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
		"ClientID":                 "CockroachDB",
		"ProducerBatchCompression": []kgo.CompressionCodec{kgo.NoCompression()},
		"RequiredAcks":             kgo.LeaderAck(),
		"MaxVersions":              kversion.Stable(), // We don't set this but it's kgo's default.
		"DialTLSConfig":            (*tls.Config)(nil),
		"SASL":                     ([]sasl.Mechanism)(nil),
		"DisableIdempotentWrite":   true,
		"MaxBufferedRecords":       int64(1000), // This is the default that we set.
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
	buf.Append([]byte("k1"), []byte("v1"), attributes{})
	payload, err := buf.Close()
	require.NoError(t, err)

	require.Error(t, fx.sink.Flush(fx.ctx, payload))
}

func TestKafkaSinkClientV2_PartitionsSameAsV1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	kp := newKgoChangefeedPartitioner().ForTopic("t")
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
		t:           t,
		settings:    settings,
		ctx:         ctx,
		kc:          kc,
		ac:          ac,
		mockCtrl:    ctrl,
		targetNames: []string{"t"},
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
	fx.sink, err = newKafkaSinkClientV2(ctx, fx.additionalKOpts, fx.batchConfig, uri, settings, knobs, nilMetricsRecorderBuilder, nil)
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

	bs, err := makeKafkaSinkV2(ctx, &changefeedbase.SinkURL{URL: u}, targets, fx.sinkJSONConfig, 1, nilPacerFactory, timeutil.DefaultTimeSource{}, settings, nilMetricsRecorderBuilder, knobs)
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
