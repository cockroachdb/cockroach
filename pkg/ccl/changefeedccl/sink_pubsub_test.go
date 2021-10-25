package changefeedccl

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"gocloud.dev/pubsub"
	"net/url"
	"testing"
	"time"
)

const testMemPubsubURI = "mem://topicTest"


func createSubscriber(ctx context.Context) (*pubsub.Subscription, error) {
	subscription, err := pubsub.OpenSubscription(ctx, testMemPubsubURI)
	if err != nil {
		return nil, err
	}
	return subscription, nil
}

func recieveRows(sub *pubsub.Subscription, results chan string, ctx context.Context, timer timeutil.TimerI){
	for{
		msg, err := sub.Receive(ctx)
		if err != nil {
			break
		}
		select {
		case <-ctx.Done():
			break
		case <-timer.Ch():
			break
		case results <- string(msg.Body):
		}
	}
}

func TestRecieveRows(t *testing.T) {
	ctx := context.TODO()
	sub, err := createSubscriber(ctx)
	defer sub.Shutdown(ctx)
	require.NoError(t, err)
	var ts timeutil.TimeSource
	timer := ts.NewTimer()
	limit, err := time.ParseDuration("30s")
	results := make(chan string)
	require.NoError(t, err)
	timer.Reset(limit)

	go recieveRows(sub, results, ctx, timer)

	u, err := url.Parse(testMemPubsubURI)
	require.NoError(t, err)

	p, err := MakePubsubSink(ctx, u, nil)
	require.NoError(t, err)
	err = p.Dial()
	require.NoError(t, err)

	err = p.EmitRow(ctx, nil, []byte("test key"), []byte("test value"), hlc.Timestamp{}, kvevent.Alloc{})
	require.NoError(t, err)

}
