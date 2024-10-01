// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package requestbatcher

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type batchResp struct {
	// TODO(ajwerner): we never actually test that this result is what we expect
	// it to be. We should add a test that does so.
	br *kvpb.BatchResponse
	pe *kvpb.Error
}

type batchSend struct {
	ctx      context.Context
	ba       *kvpb.BatchRequest
	respChan chan<- batchResp
}

type chanSender chan batchSend

func (c chanSender) Send(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	respChan := make(chan batchResp, 1)
	select {
	case c <- batchSend{ctx: ctx, ba: ba, respChan: respChan}:
	case <-ctx.Done():
		return nil, kvpb.NewError(ctx.Err())
	}
	select {
	case resp := <-respChan:
		return resp.br, resp.pe
	case <-ctx.Done():
		return nil, kvpb.NewError(ctx.Err())
	}
}

type senderGroup struct {
	b *RequestBatcher
	g errgroup.Group
}

func (g *senderGroup) Send(rangeID roachpb.RangeID, request kvpb.Request) {
	g.g.Go(func() error {
		_, err := g.b.Send(context.Background(), rangeID, request)
		return err
	})
}

func (g *senderGroup) Wait() error {
	return g.g.Wait()
}

func TestBatcherSendOnSizeWithReset(t *testing.T) {
	// Note: the timing-dependency and possible flakiness can be addressed by
	// using a manual time(r) source, see TestBatcherSend for an example.

	// This test ensures that when a single batch ends up sending due to size
	// constrains its timer is successfully canceled and does not lead to a
	// nil panic due to an attempt to send a batch due to the old timer.
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	sc := make(chanSender)
	// The challenge with populating this timeout is that if we set it too short
	// then there's a chance that the batcher will send based on time and not
	// size which somewhat defeats the purpose of the test in the first place.
	// If we set the timeout too long then the test will take a long time for no
	// good reason. Instead of erring on the side of being conservative with the
	// timeout we instead allow the test to pass successfully even if it doesn't
	// exercise the path we intended. This is better than having the test block
	// forever or fail. We don't expect that it will take 5ms in the common case
	// to send two messages on a channel and if it does, oh well, the logic below
	// deals with that too and at least the test doesn't fail or hang forever.
	const wait = 5 * time.Millisecond
	b := New(Config{
		MaxIdle:         wait,
		MaxWait:         wait,
		MaxMsgsPerBatch: 2,
		Sender:          sc,
		Stopper:         stopper,
	})
	g := senderGroup{b: b}
	g.Send(1, &kvpb.GetRequest{})
	g.Send(1, &kvpb.GetRequest{})
	s := <-sc
	s.respChan <- batchResp{}
	// See the comment above wait. In rare cases the batch will be sent before the
	// second request can be added. In this case we need to expect that another
	// request will be sent and handle it so that the test does not block forever.
	if len(s.ba.Requests) == 1 {
		t.Logf("batch was sent due to time rather than size constraints, passing anyway")
		s := <-sc
		s.respChan <- batchResp{}
	} else {
		time.Sleep(wait)
	}
	if err := g.Wait(); err != nil {
		t.Fatalf("Failed to send: %v", err)
	}
}

// TestBatchesAtTheSameTime attempts to test that batches which seem to occur at
// exactly the same moment are eventually sent. Sometimes it may be the case
// that this test fails to exercise that path if the channel send to the
// goroutine happens to take more than 10ms but in that case both batches will
// definitely get sent and the test will pass. This test was added to account
// for a bug where the internal timer would not get set if two batches had the
// same deadline. This test failed regularly before that bug was fixed.
func TestBatchesAtTheSameTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	sc := make(chanSender)
	start := timeutil.Now()
	then := start.Add(10 * time.Millisecond)
	mt := timeutil.NewManualTime(then)
	b := New(Config{
		MaxIdle:    20 * time.Millisecond,
		Sender:     sc,
		Stopper:    stopper,
		manualTime: mt,
	})
	const N = 20
	sendChan := make(chan Response, N)
	for i := 0; i < N; i++ {
		assert.Nil(t, b.SendWithChan(
			context.Background(), sendChan, roachpb.RangeID(i), &kvpb.GetRequest{}, kvpb.AdmissionHeader{}))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func(ctx context.Context) {
		// At this point, all the requests should've made it into the
		// batcher and have been timestamped. We want to be a real clock
		// so that the timers fire on their own accord.
		for {
			select {
			case <-time.After(5 * time.Millisecond):
				mt.Advance(5 * time.Millisecond)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
	for i := 0; i < N; i++ {
		bs := <-sc
		bs.respChan <- batchResp{}
	}
}

func TestBackpressure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	// Note: the timing-dependency and possible flakiness can be addressed by
	// using a manual time(r) source, see TestBatcherSend for an example.

	sc := make(chanSender)
	backpressureLimit := 3
	b := New(Config{
		MaxIdle:                   50 * time.Millisecond,
		MaxWait:                   50 * time.Millisecond,
		MaxMsgsPerBatch:           1,
		Sender:                    sc,
		Stopper:                   stopper,
		InFlightBackpressureLimit: func() int { return backpressureLimit },
	})

	// These 3 should all send without blocking but should put the batcher into
	// back pressure.
	sendChan := make(chan Response, 6)
	assert.Nil(t, b.SendWithChan(
		context.Background(), sendChan, 1, &kvpb.GetRequest{}, kvpb.AdmissionHeader{}))
	assert.Nil(t, b.SendWithChan(
		context.Background(), sendChan, 2, &kvpb.GetRequest{}, kvpb.AdmissionHeader{}))
	assert.Nil(t, b.SendWithChan(
		context.Background(), sendChan, 3, &kvpb.GetRequest{}, kvpb.AdmissionHeader{}))
	var sent int64
	send := func() {
		assert.Nil(t, b.SendWithChan(
			context.Background(), sendChan, 4, &kvpb.GetRequest{}, kvpb.AdmissionHeader{}))
		atomic.AddInt64(&sent, 1)
	}
	go send()
	go send()
	canReply := make(chan struct{})
	reply := func(bs batchSend) {
		<-canReply
		bs.respChan <- batchResp{}
	}
	for i := 0; i < 3; i++ {
		bs := <-sc
		go reply(bs)
		// We don't expect any of the calls to send to have finished yet.
		assert.Equal(t, int64(0), atomic.LoadInt64(&sent))
	}
	// Allow one reply to fly which should not unblock the requests since the
	// threshold to stop backpressuring is < 2, and there are still 2 in-flight
	// requests.
	canReply <- struct{}{}
	runtime.Gosched() // tickle the runtime in case there might be a timing bug
	assert.Equal(t, int64(0), atomic.LoadInt64(&sent))
	canReply <- struct{}{} // now the two requests should send
	defer func() {
		if t.Failed() {
			close(canReply)
		}
	}()
	testutils.SucceedsSoon(t, func() error {
		if numSent := atomic.LoadInt64(&sent); numSent != 2 {
			return fmt.Errorf("expected %d to have been sent, so far %d", 2, numSent)
		}
		return nil
	})
	go reply(<-sc)
	go reply(<-sc)
	// Now we have 3 outstanding reply() calls that we need to unblock.
	canReply <- struct{}{}
	canReply <- struct{}{}
	canReply <- struct{}{}
	// Now consume all the responses on sendChan.
	for i := 0; i < 5; i++ {
		<-sendChan
	}

	// Lower backpressureLimit to 1.
	backpressureLimit = 1
	atomic.StoreInt64(&sent, 0)
	send()
	// This should block.
	go send()
	// Try to reply to first, but reply will not happen yet.
	go reply(<-sc)
	runtime.Gosched() // tickle the runtime in case there might be a timing bug
	assert.Equal(t, int64(1), atomic.LoadInt64(&sent))
	// Allow one reply, which will unblock the request.
	canReply <- struct{}{}
	runtime.Gosched() // tickle the runtime in case there might be a timing bug
	testutils.SucceedsSoon(t, func() error {
		if numSent := atomic.LoadInt64(&sent); numSent != 2 {
			return fmt.Errorf("expected %d to have been sent, so far %d", 2, numSent)
		}
		return nil
	})
	// Allow second reply too.
	close(canReply)
	reply(<-sc)
	// Now consume all the responses on sendChan.
	<-sendChan
	<-sendChan
}

func TestBatcherSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	sc := make(chanSender)
	mt := timeutil.NewManualTime(time.Time{})
	peekCh := make(chan *RequestBatcher) // must be unbuffered
	b := New(Config{
		// We're using a manual timer here, so these fire when we advance `mt`
		// accordingly.
		MaxIdle:         50 * time.Millisecond,
		MaxWait:         50 * time.Millisecond,
		MaxMsgsPerBatch: 3,
		Sender:          sc,
		Stopper:         stopper,
		manualTime:      mt,
		testingPeekCh:   peekCh,
	})

	// Send 3 requests to range 2 and 2 to range 1.
	// The 3rd range 2 request will trigger immediate sending due to the
	// MaxMsgsPerBatch configuration. The range 1 batch will be sent after the
	// MaxWait timeout expires (manually via `mt`).
	g := senderGroup{b: b}
	g.Send(1, &kvpb.GetRequest{})
	g.Send(2, &kvpb.GetRequest{})
	g.Send(1, &kvpb.GetRequest{})
	g.Send(2, &kvpb.GetRequest{})
	g.Send(2, &kvpb.GetRequest{})

	// We should ~immediately see the requests to r2 show up in a single
	// batch because no timers are firing but three is the limit for when
	// a batch is full. We should not see anything to r1 yet because this
	// is waiting for us to fire a timer.
	// NB: we don't actually verify that they're for r2. This could be added
	// (noting that ba.RangeID is zero at this level of the stack, so that won't
	// do it). But - we check that the requests to r1 are still in the batcher
	// later in the test.
	select {
	case s := <-sc:
		require.Len(t, s.ba.Requests, 3)
		s.respChan <- batchResp{}
	case <-time.After(5 * time.Second):
		t.Fatalf("requests to r2 did not show up")
	}

	// Check that r1 is queued up in entirety. This is
	// a nice check and also assures that once we fire
	// the timer, we can expect to see everything at once.
	testutils.SucceedsSoon(t, func() error {
		b := <-peekCh
		defer func() {
			peekCh <- b
		}()
		var r1waiting int
		if r1b, ok := b.batches.get(1); ok {
			r1waiting = len(r1b.reqs)
		}
		if r1waiting != 2 {
			return errors.Errorf("expect two requests waiting on r1, not %d", r1waiting)
		}
		return nil
	})

	// There should be a timer at this point since we know we have the requests
	// to r1 waiting.
	require.Len(t, mt.Timers(), 1)
	// Time passes and the timer is triggered.
	mt.AdvanceTo(mt.Timers()[0])

	// Wait for the range 1 request and ensure it contains 2 requests.
	s := <-sc
	require.Len(t, s.ba.Requests, 2)
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
	_, err := b.Send(context.Background(), 1, &kvpb.GetRequest{})
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
	_, err := b.Send(ctx, 1, &kvpb.GetRequest{})
	assert.Equal(t, err, ctx.Err())
}

// TestStopDuringSend ensures that in-flight requests are canceled when the
// RequestBatcher's stopper indicates that it should quiesce.
func TestStopDuringSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	sc := make(chanSender, 1)
	b := New(Config{
		Sender:          sc,
		Stopper:         stopper,
		MaxMsgsPerBatch: 1,
	})
	errChan := make(chan error)
	go func() {
		_, err := b.Send(context.Background(), 1, &kvpb.GetRequest{})
		errChan <- err
	}()
	// Wait for the request to get sent.
	<-sc
	stopper.Stop(context.Background())
	// Depending on the ordering of when channels close the sender might
	// get one of two errors.
	assert.True(t, testutils.IsError(<-errChan,
		stop.ErrUnavailable.Error()+"|"+context.Canceled.Error()))
}

func TestPanicWithNilStopper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to panic with a nil Stopper")
		}
	}()
	New(Config{Sender: make(chanSender)})
}

// TestBatchTimeout verifies the RequestBatcher uses the context with the
// deadline from the latest call and max timeout to send.
func TestBatchTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const timeout = 5 * time.Millisecond
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	// Note: the timing-dependency and possible flakiness can be addressed by
	// using a manual time(r) source, see TestBatcherSend for an example.

	testCases := []struct {
		requestTimeout  time.Duration
		maxTimeout      time.Duration
		expectedTimeout time.Duration
	}{
		{
			requestTimeout:  timeout,
			maxTimeout:      0,
			expectedTimeout: timeout,
		},
		{
			requestTimeout:  0,
			maxTimeout:      timeout,
			expectedTimeout: timeout,
		},
		{
			requestTimeout:  7 * time.Millisecond,
			maxTimeout:      timeout,
			expectedTimeout: timeout,
		},
		{
			requestTimeout:  timeout,
			maxTimeout:      7 * time.Millisecond,
			expectedTimeout: timeout,
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("With%sRequestTimeout%sMaxTimeout", tc.requestTimeout, tc.maxTimeout),
			func(t *testing.T) {
				sc := make(chanSender)
				b := New(Config{
					// MaxMsgsPerBatch of 1 is chosen so that the first call to Send will
					// immediately lead to a batch being sent.
					MaxMsgsPerBatch: 1,
					Sender:          sc,
					Stopper:         stopper,
					MaxTimeout:      tc.maxTimeout,
				})
				// This test attempts to verify that a batch with a request with a
				// timeout will be sent with that timeout. The test faces challenges of
				// timing. There are several different phases at which the timeout may
				// fire; the request may time out before it has been sent to the
				// batcher, it may timeout while it is being sent or it may not time
				// out until after it has been sent. Each of these cases are handled
				// and verified to ensure that the request was indeed sent with a
				// timeout.
				ctx, cancel := context.WithTimeout(context.Background(), tc.requestTimeout)
				defer cancel()
				respChan := make(chan Response, 1)
				if err := b.SendWithChan(
					ctx, respChan, 1, &kvpb.GetRequest{}, kvpb.AdmissionHeader{}); err != nil {
					testutils.IsError(err, context.DeadlineExceeded.Error())
					return
				}
				select {
				case s := <-sc:
					deadline, hasDeadline := s.ctx.Deadline()
					assert.True(t, hasDeadline)
					assert.True(t, timeutil.Until(deadline) < tc.expectedTimeout)
					s.respChan <- batchResp{}
				case resp := <-respChan:
					assert.Nil(t, resp.Resp)
					testutils.IsError(resp.Err, context.DeadlineExceeded.Error())
				}
			},
		)
	}
	t.Run("WithTimeoutAndPagination", func(t *testing.T) {
		maxTimeout := 45 * time.Second
		firstAndSecondSendTimeDiff := 20 * time.Millisecond
		sc := make(chanSender)
		b := New(Config{
			// MaxMsgsPerBatch of 1 is chosen so that the first call to Send will
			// immediately lead to a batch being sent.
			MaxMsgsPerBatch: 1,
			Sender:          sc,
			Stopper:         stopper,
			MaxTimeout:      maxTimeout,
		})
		// This test will simulate multiple calls to Send (due to pagination) and
		// test that the MaxTimeout set is a timeout per BatchRequest rather than
		// a timeout per batch. MaxTimeout is set to a large value, so the timeout
		// should never actually be hit. This means that the subtest does not face
		// the same timing issues that the subtests above do.
		ctx := context.Background()
		respChan := make(chan Response, 1)
		minStartTimeForFirstBatchRequest := timeutil.Now()
		err := b.SendWithChan(ctx, respChan, 1, &kvpb.GetRequest{}, kvpb.AdmissionHeader{})
		require.NoError(t, err)
		// First call to Send.
		s := <-sc
		deadline, hasDeadline := s.ctx.Deadline()
		assert.True(t, hasDeadline)
		assert.GreaterOrEqual(t, deadline.Sub(minStartTimeForFirstBatchRequest), maxTimeout)
		time.Sleep(firstAndSecondSendTimeDiff)
		minStartTimeForSecondBatchRequest := timeutil.Now()
		s.respChan <- batchResp{
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					{
						Value: &kvpb.ResponseUnion_Get{
							Get: &kvpb.GetResponse{
								ResponseHeader: kvpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{},
								},
							},
						},
					},
				},
			},
		}
		// Second call to Send occurs at least firstAndSecondSendTimeDiff time
		// after first call to Send.
		s = <-sc
		deadline, hasDeadline = s.ctx.Deadline()
		assert.True(t, hasDeadline)
		assert.GreaterOrEqual(t, deadline.Sub(minStartTimeForSecondBatchRequest), maxTimeout)
		s.respChan <- batchResp{}
	})
	t.Run("NoTimeout", func(t *testing.T) {
		sc := make(chanSender)
		b := New(Config{
			// MaxMsgsPerBatch of 2 is chosen so that the second call to Send will
			// immediately lead to a batch being sent.
			MaxMsgsPerBatch: 2,
			Sender:          sc,
			Stopper:         stopper,
		})
		// This test attempts to verify that a batch with two requests where one
		// carries a timeout leads to the batch being sent without a timeout.
		// There is a hazard that the goroutine which is being canceled is not
		// able to send its request to the batcher before its deadline expires
		// in which case the batch is never sent due to size constraints.
		// The test will pass in this scenario with after logging and cleaning up.
		ctx1, cancel1 := context.WithTimeout(context.Background(), timeout)
		defer cancel1()
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()
		var wg sync.WaitGroup
		wg.Add(2)
		var err1, err2 error
		err1Chan := make(chan error, 1)
		go func() {
			_, err1 = b.Send(ctx1, 1, &kvpb.GetRequest{})
			err1Chan <- err1
			wg.Done()
		}()
		go func() { _, err2 = b.Send(ctx2, 1, &kvpb.GetRequest{}); wg.Done() }()
		select {
		case s := <-sc:
			assert.Len(t, s.ba.Requests, 2)
			s.respChan <- batchResp{}
		case <-err1Chan:
			// This case implies that the test did not exercise what was intended
			// but that's okay, clean up the other request and return.
			assert.Equal(t, context.DeadlineExceeded, err1)
			t.Logf("canceled goroutine failed to send within %v, passing", timeout)
			cancel2()
			wg.Wait()
			return
		}
		wg.Wait()
		testutils.IsError(err1, context.DeadlineExceeded.Error())
		assert.Nil(t, err2)
	})
}

// TestIdleAndMaxTimeoutDisabled exercises the RequestBatcher when it is
// configured to send only based on batch size policies.
func TestIdleAndMaxTimeoutDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	sc := make(chanSender)
	b := New(Config{
		MaxMsgsPerBatch: 2,
		Sender:          sc,
		Stopper:         stopper,
	})
	// Send 2 requests to range 1. Even with an arbitrarily large delay between
	// the requests, they should only be sent when the MaxMsgsPerBatch limit is
	// reached, because no MaxWait timeout is configured.
	g := senderGroup{b: b}
	g.Send(1, &kvpb.GetRequest{})
	select {
	case <-sc:
		t.Fatalf("RequestBatcher should not sent based on time")
	case <-time.After(10 * time.Millisecond):
	}
	g.Send(1, &kvpb.GetRequest{})
	s := <-sc
	assert.Len(t, s.ba.Requests, 2)
	s.respChan <- batchResp{}
	// Make sure everything gets a response.
	if err := g.Wait(); err != nil {
		t.Fatalf("expected no errors, got %v", err)
	}
}

// TestMaxKeysPerBatchReq exercises the RequestBatcher when it is configured to
// assign each request a MaxSpanRequestKeys limit. When such a limit is used,
// the RequestBatcher may receive partial responses to the requests that it
// issues, so it needs to be prepared to paginate requests and combine partial
// responses.
func TestMaxKeysPerBatchReq(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	sc := make(chanSender)
	b := New(Config{
		MaxMsgsPerBatch:    3,
		MaxKeysPerBatchReq: 5,
		Sender:             sc,
		Stopper:            stopper,
	})
	// Send 3 ResolveIntentRange requests. The requests are limited so
	// pagination will be required. The following sequence of partial
	// results will be returned:
	//  send([{d-g}, {a-d}, {b, m}]) ->
	//    scans from [a, c) before hitting limit
	//    returns [{d-g}, {c-d}, {c-m}]
	//  send([{d-g}, {c-d}, {c-m}]) ->
	//    scans from [c, e) before hitting limit
	//    returns [{e-g}, {}, {e-m}]
	//  send([{e-g}, {e-m}]) ->
	//    scans from [e, h) before hitting limit
	//    returns [{}, {h-m}]
	//  send([{h-m}]) ->
	//    scans from [h, m) without hitting limit
	//    returns [{}]
	type span [2]string // [key, endKey]
	type spanMap map[span]span
	var nilResumeSpan span
	makeReq := func(sp span) *kvpb.ResolveIntentRangeRequest {
		var req kvpb.ResolveIntentRangeRequest
		req.Key = roachpb.Key(sp[0])
		req.EndKey = roachpb.Key(sp[1])
		return &req
	}
	makeResp := func(ba *kvpb.BatchRequest, resumeSpans spanMap) *kvpb.BatchResponse {
		br := ba.CreateReply()
		for i, ru := range ba.Requests {
			req := ru.GetResolveIntentRange()
			reqSp := span{string(req.Key), string(req.EndKey)}
			resumeSp, ok := resumeSpans[reqSp]
			if !ok {
				t.Fatalf("unexpected request: %+v", req)
			}
			if resumeSp == nilResumeSpan {
				continue
			}
			resp := br.Responses[i].GetResolveIntentRange()
			resp.ResumeSpan = &roachpb.Span{
				Key: roachpb.Key(resumeSp[0]), EndKey: roachpb.Key(resumeSp[1]),
			}
			resp.ResumeReason = kvpb.RESUME_KEY_LIMIT
		}
		return br
	}
	g := senderGroup{b: b}
	g.Send(1, makeReq(span{"d", "g"}))
	g.Send(1, makeReq(span{"a", "d"}))
	g.Send(1, makeReq(span{"b", "m"}))
	//  send([{d-g}, {a-d}, {b, m}]) ->
	//    scans from [a, c) before hitting limit
	//    returns [{d-g}, {c-d}, {c-m}]
	s := <-sc
	assert.Equal(t, int64(5), s.ba.MaxSpanRequestKeys)
	assert.Len(t, s.ba.Requests, 3)
	br := makeResp(s.ba, spanMap{
		{"d", "g"}: {"d", "g"},
		{"a", "d"}: {"c", "d"},
		{"b", "m"}: {"c", "m"},
	})
	s.respChan <- batchResp{br: br}
	//  send([{d-g}, {c-d}, {c-m}]) ->
	//    scans from [c, e) before hitting limit
	//    returns [{e-g}, {}, {e-m}]
	s = <-sc
	assert.Equal(t, int64(5), s.ba.MaxSpanRequestKeys)
	assert.Len(t, s.ba.Requests, 3)
	br = makeResp(s.ba, spanMap{
		{"d", "g"}: {"e", "g"},
		{"c", "d"}: nilResumeSpan,
		{"c", "m"}: {"e", "m"},
	})
	s.respChan <- batchResp{br: br}
	//  send([{e-g}, {e-m}]) ->
	//    scans from [e, h) before hitting limit
	//    returns [{}, {h-m}]
	s = <-sc
	assert.Equal(t, int64(5), s.ba.MaxSpanRequestKeys)
	assert.Len(t, s.ba.Requests, 2)
	br = makeResp(s.ba, spanMap{
		{"e", "g"}: nilResumeSpan,
		{"e", "m"}: {"h", "m"},
	})
	s.respChan <- batchResp{br: br}
	//  send([{h-m}]) ->
	//    scans from [h, m) without hitting limit
	//    returns [{}]
	s = <-sc
	assert.Equal(t, int64(5), s.ba.MaxSpanRequestKeys)
	assert.Len(t, s.ba.Requests, 1)
	br = makeResp(s.ba, spanMap{
		{"h", "m"}: nilResumeSpan,
	})
	s.respChan <- batchResp{br: br}
	// Make sure everything gets a response.
	if err := g.Wait(); err != nil {
		t.Fatalf("expected no errors, got %v", err)
	}
}

// TestTargetBytesPerBatchReq checks that the correct TargetBytes limit is set
// according to the TargetBytesPerBatchReqFn passed in via the config.
func TestTargetBytesPerBatchReq(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	sc := make(chanSender)
	b := New(Config{
		// MaxMsgsPerBatch of 1 is chosen so that the first call to Send will
		// immediately lead to a batch being sent.
		MaxMsgsPerBatch:        1,
		Sender:                 sc,
		Stopper:                stopper,
		TargetBytesPerBatchReq: 4 << 20,
	})
	respChan := make(chan Response, 1)

	err := b.SendWithChan(
		context.Background(), respChan, 1, &kvpb.GetRequest{}, kvpb.AdmissionHeader{})
	require.NoError(t, err)
	s := <-sc
	assert.Equal(t, int64(4<<20), s.ba.TargetBytes)
	s.respChan <- batchResp{}
}

func TestPanicWithNilSender(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to panic with a nil Sender")
		}
	}()
	New(Config{Stopper: stopper})
}
