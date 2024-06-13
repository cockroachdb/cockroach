package changefeedccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/mocks"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
	require.NoError(fx.t, fx.sink.Close())
}

// this tests the inner sink client v2, not including the batching sink wrapper
// copied mostly from TestKafkaSink
func TestKafkaSinkClientV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fx := newKafkaSinkV2Fx(t)
	defer fx.close()
}
