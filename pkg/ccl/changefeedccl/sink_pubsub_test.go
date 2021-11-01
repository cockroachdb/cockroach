package changefeedccl

import (
	"context"
	"encoding/json"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"gocloud.dev/pubsub"
	"net/url"
	"testing"
	"time"
)

const testMemPubsubURI = "mem://topicTest"

func getGenericPubsubSinkOptions() map[string]string {
	opts := make(map[string]string)
	opts[changefeedbase.OptFormat] = string(changefeedbase.OptFormatJSON)
	opts[changefeedbase.OptKeyInValue] = ``
	opts[changefeedbase.OptEnvelope] = string(changefeedbase.OptEnvelopeWrapped)
	return opts
}

type emitRowInput struct {
	key map[string]string
	val map[string]string
}


type testSubscriber struct {
	sub *pubsub.Subscription
	url *url.URL
	ctx context.Context
	results chan []byte
	errChan chan error
	timer timeutil.TimerI
}

func makeTestMemPubsub(uri string) (testSubscriber, error){
	ctx := context.TODO()
	var ts timeutil.DefaultTimeSource
	sinkUrl, err := url.Parse(uri)
	if err != nil {
		return testSubscriber{}, err
	}
	t := testSubscriber{ctx: ctx, timer: ts.NewTimer(), results: make(chan []byte), errChan: make(chan error), url: sinkUrl}
	return t, nil

}

func (s *testSubscriber) setTimer(limit time.Duration){
	s.timer.Reset(limit)
}

func (s *testSubscriber) Dial() error{
	subscription, err := pubsub.OpenSubscription(s.ctx, s.url.String())
	if err != nil {
		return err
	}
	s.sub = subscription
	return nil
}

func (s *testSubscriber) Close(){
	close(s.results)
	close(s.errChan)
	if s.sub != nil {
		s.sub.Shutdown(s.ctx)
	}
}

func (s *testSubscriber)recieveRows(){
	for{
		msg, err := s.sub.Receive(s.ctx)
		if err != nil {
			break
		}
		select {
		case <-s.ctx.Done():
			break
		case <-s.timer.Ch():
			break
		case s.results <- msg.Body:
		}
	}
}

func TestRecieveRows(t *testing.T) {

	sinkCtx := context.TODO()
	u, err := url.Parse(testMemPubsubURI)
	p, err := MakePubsubSink(sinkCtx, u, getGenericPubsubSinkOptions())
	require.NoError(t, err)
	err = p.Dial()
	require.NoError(t, err)

	m, err := makeTestMemPubsub(testMemPubsubURI)
	require.NoError(t, err)
	limit, err := time.ParseDuration("30s")
	require.NoError(t, err)
	m.setTimer(limit)
	err = m.Dial()
	require.NoError(t, err)
	defer m.Close()

	go m.recieveRows()

	input := []emitRowInput{
		{
			key: map[string]string{
				"testkey": "key",
			},
			val: map[string]string{
				"testval": "val",
			},
		},
	}

	for _ , i := range input {

		var pool testAllocPool
		k, err := json.Marshal(i.key)
		require.NoError(t, err)
		v, err := json.Marshal(i.val)
		require.NoError(t, err)
		err = p.EmitRow(sinkCtx, nil, k , v , hlc.Timestamp{}, pool.alloc())
		require.NoError(t, err)
		message := <- m.results

		var recieved payload
		err = json.Unmarshal(message, &recieved)
		require.NoError(t, err)
		require.Equal(t, payload{
			Key: k,
			Value: v,
		}, recieved)

	}
}

func TestReceiveRowsAndFlush(t *testing.T) {
	sinkCtx := context.TODO()
	u, err := url.Parse(testMemPubsubURI)
	p, err := MakePubsubSink(sinkCtx, u, getGenericPubsubSinkOptions())
	require.NoError(t, err)
	err = p.Dial()
	require.NoError(t, err)

	m, err := makeTestMemPubsub(testMemPubsubURI)
	require.NoError(t, err)
	limit, err := time.ParseDuration("30s")
	require.NoError(t, err)
	m.setTimer(limit)
	err = m.Dial()
	require.NoError(t, err)
	defer m.Close()

	go m.recieveRows()


	err = p.Flush(sinkCtx)
	require.NoError(t, err)

	input := []emitRowInput{
		{
			key: map[string]string{
				"testkey": "key",
			},
			val: map[string]string{
				"testval": "val",
			},
		},
		{
			key: map[string]string{
				"testkey2": "key2",
			},
			val: map[string]string{
				"testval2": "val2",
			},
		},
	}

	for _ , i := range input {

		var pool testAllocPool
		k, err := json.Marshal(i.key)
		require.NoError(t, err)
		v, err := json.Marshal(i.val)
		require.NoError(t, err)
		err = p.EmitRow(sinkCtx, nil, k , v , hlc.Timestamp{}, pool.alloc())
		require.NoError(t, err)
		message := <- m.results

		var recieved payload
		err = json.Unmarshal(message, &recieved)
		require.NoError(t, err)
		require.Equal(t, payload{
			Key: k,
			Value: v,
		}, recieved)
	}

	err = p.Flush(sinkCtx)
	require.NoError(t, err)

	err = p.Flush(sinkCtx)
	require.NoError(t, err)
}
