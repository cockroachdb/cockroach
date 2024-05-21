package changefeedccl

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestKafkaSinkV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	p := newAsyncProducerMock(1)
	sink, cleanup := makeTestKafkaSink(
		t, noTopicPrefix, defaultTopicName, p, "t")
	defer cleanup()

	// No inflight
	require.NoError(t, sink.Flush(ctx))

	// Timeout
	require.NoError(t,
		sink.EmitRow(ctx, topic(`t`), []byte(`1`), nil, zeroTS, zeroTS, zeroAlloc))

	m1 := <-p.inputCh
	for i := 0; i < 2; i++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		require.True(t, errors.Is(context.DeadlineExceeded, sink.Flush(timeoutCtx)))
	}
	go func() { p.successesCh <- m1 }()
	require.NoError(t, sink.Flush(ctx))

	// Check no inflight again now that we've sent something
	require.NoError(t, sink.Flush(ctx))

	// Mixed success and error.
	var pool testAllocPool
	require.NoError(t, sink.EmitRow(ctx,
		topic(`t`), []byte(`2`), nil, zeroTS, zeroTS, pool.alloc()))
	m2 := <-p.inputCh
	require.NoError(t, sink.EmitRow(
		ctx, topic(`t`), []byte(`3`), nil, zeroTS, zeroTS, pool.alloc()))

	m3 := <-p.inputCh
	require.NoError(t, sink.EmitRow(
		ctx, topic(`t`), []byte(`4`), nil, zeroTS, zeroTS, pool.alloc()))

	m4 := <-p.inputCh
	go func() { p.successesCh <- m2 }()
	go func() {
		p.errorsCh <- &sarama.ProducerError{
			Msg: m3,
			Err: errors.New("m3"),
		}
	}()
	go func() { p.successesCh <- m4 }()
	require.Regexp(t, "m3", sink.Flush(ctx))

	// Check simple success again after error
	require.NoError(t, sink.EmitRow(
		ctx, topic(`t`), []byte(`5`), nil, zeroTS, zeroTS, pool.alloc()))

	m5 := <-p.inputCh
	go func() { p.successesCh <- m5 }()
	require.NoError(t, sink.Flush(ctx))
	// At the end, all of the resources has been released
	require.EqualValues(t, 0, pool.used())
}

func makeTestKafkaSinkV2(
	t testing.TB,
	topicPrefix string,
	topicNameOverride string,
	p sarama.AsyncProducer,
	targetNames ...string,
) (s *kafkaSinkClient, cleanup func()) {
	targets := makeChangefeedTargets(targetNames...)
	topics, err := MakeTopicNamer(targets,
		WithPrefix(topicPrefix), WithSingleName(topicNameOverride), WithSanitizeFn(SQLNameToKafkaName))
	require.NoError(t, err)

	kcfg := &sarama.Config{}
	bcfg := sinkBatchConfig{}
	s, err = newKafkaSinkClient(kcfg, bcfg, "no addrs", topics, nil, kafkaSinkKnobs{
		OverrideAsyncProducerFromClient: func(client kafkaClient) (sarama.AsyncProducer, error) {
			return p, nil
		},
		OverrideClientInit: func(config *sarama.Config) (kafkaClient, error) {
			client := &fakeKafkaClient{config}
			return client, nil
		},
	})
	require.NoError(t, err)

	return s, func() {
		require.NoError(t, s.Close())
	}
}
