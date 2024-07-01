// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"crypto/tls"
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

func TestKafkaSinkClientV2_Opts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type check struct {
		opt      string
		expected any
	}

	t.Run("default", func(t *testing.T) {
		fx := newKafkaSinkV2Fx(t, withJSONConfig("{}"), withRealClient())
		defer fx.close()

		opts := []check{
			{"ClientID", "CockroachDB"},
			{"ProducerBatchCompression", []kgo.CompressionCodec{kgo.NoCompression()}},
			{"RequiredAcks", kgo.LeaderAck()},
			{"MaxVersions", kversion.Stable()}, // We don't set this but it's kgo's default.
			{"DialTLSConfig", (*tls.Config)(nil)},
			{"SASL", ([]sasl.Mechanism)(nil)},
		}

		client := fx.bs.client.(*kafkaSinkClientV2).client.(*kgo.Client)
		for _, o := range opts {
			val := client.OptValue(o.opt)
			assert.Equal(t, o.expected, val, "opt %q has value %+#v, expected %+#v", o.opt, val, o.expected)
		}
		assert.Equal(t, 1000, fx.bs.client.(*kafkaSinkClientV2).batchCfg.Messages)
		assert.Equal(t, 0, fx.bs.client.(*kafkaSinkClientV2).batchCfg.Bytes)
		assert.Equal(t, jsonDuration(1*time.Millisecond), fx.bs.client.(*kafkaSinkClientV2).batchCfg.Frequency)
	})

	t.Run("custom", func(t *testing.T) {
		j := `
	{
		"ClientID": "test",
		"Compression": "gzip",
		"RequiredAcks": "ALL",
		"Version": "0.8.2.2",
		"Flush": {
			"Messages": 100,
			"Bytes": 1000,
			"Frequency": "1s",
			"MaxMessages": 1000
		}
	}
	`
		fx := newKafkaSinkV2Fx(t, withJSONConfig(j), withRealClient())
		defer fx.close()

		opts := []check{
			{"ClientID", "test"},
			{"ProducerBatchCompression", []kgo.CompressionCodec{kgo.GzipCompression()}},
			{"RequiredAcks", kgo.AllISRAcks()},
			{"MaxVersions", kversion.V0_8_2()},
		}

		client := fx.bs.client.(*kafkaSinkClientV2).client.(*kgo.Client)
		for _, o := range opts {
			val := client.OptValue(o.opt)
			assert.Equal(t, o.expected, val, "opt %q has value %+#v, expected %+#v", o.opt, val, o.expected)
		}
		assert.Equal(t, 100, fx.bs.client.(*kafkaSinkClientV2).batchCfg.Messages) // Takes the minimum of the two, for backwards compatibility.
		assert.Equal(t, 1000, fx.bs.client.(*kafkaSinkClientV2).batchCfg.Bytes)
		assert.Equal(t, jsonDuration(1*time.Second), fx.bs.client.(*kafkaSinkClientV2).batchCfg.Frequency)
	})
}

func TestKafkaSinkClientV2_ErrorsEventually(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This will make a real kafka client but with a bogus address, so it should
	// fail to produce. There might be cases where we do retry forever, however,
	// like if some records have already been produced and there's no way to
	// safely give up without causing ordering issues. If we don't want this to
	// happen, we would need to disable idempotency and set max inflight to 1.
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

	for numParts := 1; numParts <= 100; numParts++ {
		t.Run(strconv.Itoa(numParts), func(t *testing.T) {
			for i := 0; i < 100; i++ {
				// Test that the partitioners give the same results for arbitrary keys.
				key := []byte(strconv.Itoa(i))
				kgoPart := int32(kp.Partition(&kgo.Record{Key: key}, numParts))
				saramaPart, err := sp.Partition(&sarama.ProducerMessage{Key: sarama.ByteEncoder(key)}, int32(numParts))
				require.NoError(t, err)
				assert.Equal(t, saramaPart, kgoPart, "key %s with %d partitions", key, numParts)

				// ...And for nil keys and hardcoded partitions.
				hardcodedPart := int32(i % numParts)
				kgoPart = int32(kp.Partition(&kgo.Record{Key: nil, Partition: hardcodedPart}, numParts))
				assert.Equal(t, kgoPart, hardcodedPart, "nil key with %d partitions and hardcoded partition %d", numParts, hardcodedPart)
				// However, note that the v1 kafka sink, actually *does not*
				// send nil-keyed (resolved) messages to the partition it asked
				// for. See https://github.com/cockroachdb/cockroach/issues/122666. This is a bug and we are fixing it here in the new version.
			}
		})
	}
}

type kafkaSinkV2Fx struct {
	t        *testing.T
	settings *cluster.Settings
	ctx      context.Context
	kc       *mocks.MockKafkaClientV2
	ac       *mocks.MockKafkaAdminClientV2
	mockCtrl *gomock.Controller

	// set with fxOpts to modify the created sinks
	targetNames     []string
	topicOverride   string
	topicPrefix     string
	sinkJSONConfig  changefeedbase.SinkSpecificJSONConfig
	batchConfig     sinkBatchConfig
	realClient      bool
	additionalKOpts []kgo.Opt

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

func withKOptsClient(kOpts []kgo.Opt) fxOpt {
	return func(fx *kafkaSinkV2Fx) {
		fx.additionalKOpts = kOpts
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

	knobs := kafkaSinkV2Knobs{
		OverrideClient: func(opts []kgo.Opt) (KafkaClientV2, KafkaAdminClientV2) {
			return kc, ac
		},
	}

	if fx.realClient {
		knobs = kafkaSinkV2Knobs{}
	}

	var err error
	fx.sink, err = newKafkaSinkClientV2(ctx, fx.additionalKOpts, fx.batchConfig, "no addrs", settings, knobs, nilMetricsRecorderBuilder)
	require.NoError(t, err)

	targets := makeChangefeedTargets(fx.targetNames...)

	u, err := url.Parse("kafka://localhost:9092")
	require.NoError(t, err)

	q := u.Query()
	if fx.topicOverride != "" {
		q.Set("topic_name", fx.topicOverride)
	}
	if fx.topicPrefix != "" {
		q.Set("topic_prefix", fx.topicPrefix)
	}
	u.RawQuery = q.Encode()

	bs, err := makeKafkaSinkV2(ctx, sinkURL{URL: u}, targets, fx.sinkJSONConfig, 1, nilPacerFactory, timeutil.DefaultTimeSource{}, settings, nilMetricsRecorderBuilder, knobs)
	require.NoError(t, err)
	fx.bs = bs.(*batchingSink)

	return fx
}

func (fx *kafkaSinkV2Fx) close() {
	if _, ok := fx.sink.client.(*mocks.MockKafkaClientV2); ok {
		fx.kc.EXPECT().Close().AnyTimes()
	}
	require.NoError(fx.t, fx.sink.Close())
	require.NoError(fx.t, fx.bs.Close())
}

type fnMatcher func(arg any) bool

func (f fnMatcher) Matches(x any) bool {
	return f(x)
}

func (f fnMatcher) String() string {
	return "matching function"
}

var _ gomock.Matcher = fnMatcher(nil)
