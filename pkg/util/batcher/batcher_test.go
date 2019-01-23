package batcher

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

type batchResp struct {
	br *roachpb.BatchResponse
	pe *roachpb.Error
}

type batchSend struct {
	ba       roachpb.BatchRequest
	respChan chan<- batchResp
}

type chanSender chan batchSend

func (c chanSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	respChan := make(chan batchResp)
	select {
	case c <- batchSend{ba: ba, respChan: respChan}:
	case <-ctx.Done():
		return nil, roachpb.NewError(ctx.Err())
	}
	resp := <-respChan
	return resp.br, resp.pe
}

func TestBatcherSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	sc := make(chanSender)
	b := New(Config{
		MaxIdle:         10 * time.Millisecond,
		MaxWait:         10 * time.Millisecond,
		MaxMsgsPerBatch: 3,
		Sender:          sc,
		Stopper:         stopper,
	})
	var g errgroup.Group
	sendRequest := func(rangeID roachpb.RangeID, request roachpb.Request) {
		g.Go(func() error {
			_, _, err := b.Send(context.Background(), rangeID, request)
			return err
		})
	}
	// Send 3 requests to range 2 and 2 to range 1.
	// The 3rd range 2 request will trigger immediate sending due to the
	// MaxMsgsPerBatch configuration. The range 1 batch will be sent after the
	// MaxWait timeout expires.
	sendRequest(1, &roachpb.GetRequest{})
	sendRequest(2, &roachpb.GetRequest{})
	sendRequest(1, &roachpb.GetRequest{})
	sendRequest(2, &roachpb.GetRequest{})
	sendRequest(2, &roachpb.GetRequest{})
	// Wait for the range 2 request and ensure it contains 3 requests.
	s := <-sc
	assert.Len(t, s.ba.Requests, 3)
	s.respChan <- batchResp{}
	// Wait for the range 1 request and ensure it contains 2 requests.
	s = <-sc
	assert.Len(t, s.ba.Requests, 2)
	s.respChan <- batchResp{}
	// Make sure everything gets a response.
	if err := g.Wait(); err != nil {
		t.Fatalf("expected no errors, got %v", err)
	}
}

func TestSendAfterStopped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	sc := make(chanSender)
	b := New(Config{
		Sender:  sc,
		Stopper: stopper,
	})
	stopper.Stop(context.Background())
	_, _, err := b.Send(context.Background(), 1, &roachpb.GetRequest{})
	assert.Equal(t, err, stop.ErrUnavailable)
}

func TestSendAfterCanceled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := make(chanSender)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	b := New(Config{
		Sender:  sc,
		Stopper: stopper,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := b.Send(ctx, 1, &roachpb.GetRequest{})
	assert.Equal(t, err, ctx.Err())
}

func TestStopDuringSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	sc := make(chanSender)
	b := New(Config{
		Sender:  sc,
		Stopper: stopper,
		MaxWait: 10 * time.Millisecond,
		MaxIdle: 10 * time.Millisecond,
	})
	errChan := make(chan error)
	go func() {
		_, _, err := b.Send(context.Background(), 1, &roachpb.GetRequest{})
		errChan <- err
	}()
	time.Sleep(100 * time.Microsecond) // encourage the goroutine to send
	stopper.Stop(context.Background())
	assert.Equal(t, <-errChan, stop.ErrUnavailable)
}
