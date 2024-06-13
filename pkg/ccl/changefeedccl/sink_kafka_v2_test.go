package changefeedccl

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/mocks"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaSinkClientV2_Resolved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fx := newKafkaSinkV2Fx(t)
	defer fx.close()

	forEachTopic := func(cb func(topic string) error) error {
		topics := []string{"t1", "t2", "t3"}
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

// TODO: test resizing

func TestKafkaSinkClientV2_Basic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fx := newKafkaSinkV2Fx(t)
	defer fx.close()

	// construct a batch of messages

	buf := fx.sink.MakeBatchBuffer("t")
	keys := []string{"k1", "k2", "k3"}
	for i, key := range keys {
		// TODO: do we do anything with those attributes in v1? tablename
		buf.Append([]byte(key), []byte(strconv.Itoa(i)), attributes{})
	}
	payload, err := buf.Close()
	require.NoError(t, err)

	fx.kc.EXPECT().ProduceSync(fx.ctx, payload.([]*kgo.Record)).Times(1).Return(nil)

	require.NoError(t, fx.sink.Flush(fx.ctx, payload))
}

func TestKafkaBuffer(t *testing.T) {
	t.Skip("TODO: test flushing under various configurations")
}

type kafkaSinkV2Fx struct {
	t        *testing.T
	ctx      context.Context
	kc       *mocks.MockKafkaClientV2
	ac       *mocks.MockKafkaAdminClientV2
	mockCtrl *gomock.Controller

	sink *kafkaSinkClient
}

func newKafkaSinkV2Fx(t *testing.T, clientOpts ...kgo.Opt) *kafkaSinkV2Fx {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	kc := mocks.NewMockKafkaClientV2(ctrl)
	ac := mocks.NewMockKafkaAdminClientV2(ctrl)

	targets := makeChangefeedTargets("t")
	topics, err := MakeTopicNamer(targets, WithPrefix(noTopicPrefix), WithSingleName(defaultTopicName), WithSanitizeFn(SQLNameToKafkaName))
	require.NoError(t, err)

	settings := cluster.MakeTestingClusterSettings()

	sink, err := newKafkaSinkClient(ctx, clientOpts, sinkBatchConfig{}, "no addrs", topics, settings, kafkaSinkV2Knobs{
		OverrideClient: func(opts []kgo.Opt) (KafkaClientV2, KafkaAdminClientV2) {
			return kc, ac
		},
	})
	require.NoError(t, err)
	return &kafkaSinkV2Fx{
		t:        t,
		ctx:      ctx,
		kc:       kc,
		ac:       ac,
		mockCtrl: ctrl,
		sink:     sink,
	}
}

func (fx *kafkaSinkV2Fx) close() {
	fx.kc.EXPECT().Close()
	require.NoError(fx.t, fx.sink.Close())
}

type fnMatcher func(arg any) bool

func (f fnMatcher) Matches(x any) bool {
	return f(x)
}

func (f fnMatcher) String() string {
	return "matching function"
}

var _ gomock.Matcher = fnMatcher(nil)
