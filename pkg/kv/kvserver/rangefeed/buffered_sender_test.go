// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestBufferedSenderWithSendBufferedError tests that BufferedSender can handle stream
// disconnects properly including context canceled, metrics updates, rangefeed
// cleanup.
func TestBufferedSenderDisconnectStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	smMetrics := NewStreamManagerMetrics()
	bs := NewBufferedSender(testServerStream, NewBufferedSenderMetrics())
	sm := NewStreamManager(bs, smMetrics)
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	const streamID = 0
	err := kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER))
	errEvent := makeMuxRangefeedErrorEvent(int64(streamID), 1, err)

	t.Run("basic operation", func(t *testing.T) {
		var num atomic.Int32
		sm.AddStream(int64(streamID), &cancelCtxDisconnector{
			cancel: func() {
				num.Add(1)
				require.NoError(t, sm.sender.sendBuffered(errEvent, nil))
			},
		})
		require.Equal(t, int64(1), smMetrics.ActiveMuxRangeFeed.Value())
		require.Equal(t, 0, bs.len())
		sm.DisconnectStream(int64(streamID), err)
		testServerStream.waitForEvent(t, errEvent)
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, 1, testServerStream.totalEventsSent())
		waitForRangefeedCount(t, smMetrics, 0)
		testServerStream.reset()
	})
	t.Run("disconnect stream on the same stream is idempotent", func(t *testing.T) {
		sm.AddStream(int64(streamID), &cancelCtxDisconnector{
			cancel: func() {
				require.NoError(t, sm.sender.sendBuffered(errEvent, nil))
			},
		})
		require.Equal(t, int64(1), smMetrics.ActiveMuxRangeFeed.Value())
		sm.DisconnectStream(int64(streamID), err)
		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		sm.DisconnectStream(int64(streamID), err)
		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		require.Equalf(t, 1, testServerStream.totalEventsSent(),
			"expected only 1 error event in %s", testServerStream.String())
		waitForRangefeedCount(t, smMetrics, 0)
	})
}

func TestBufferedSenderChaosWithStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()

	smMetrics := NewStreamManagerMetrics()
	bs := NewBufferedSender(testServerStream, NewBufferedSenderMetrics())
	sm := NewStreamManager(bs, smMetrics)
	require.NoError(t, sm.Start(ctx, stopper))

	rng, _ := randutil.NewTestRand()

	// [activeStreamStart,activeStreamEnd) are in the active streams.
	// activeStreamStart <= activeStreamEnd. If activeStreamStart ==
	// activeStreamEnd, no streams are active yet. [0, activeStreamStart) are
	// disconnected.
	var actualSum atomic.Int32
	activeStreamStart := int64(0)
	activeStreamEnd := int64(0)

	t.Run("mixed operations of add and disconnect stream", func(t *testing.T) {
		const ops = 1000
		var wg sync.WaitGroup
		for i := 0; i < ops; i++ {
			addStream := rng.Intn(2) == 0
			require.LessOrEqualf(t, activeStreamStart, activeStreamEnd, "test programming error")
			if addStream || activeStreamStart == activeStreamEnd {
				streamID := activeStreamEnd
				sm.AddStream(streamID, &cancelCtxDisconnector{
					cancel: func() {
						actualSum.Add(1)
						_ = sm.sender.sendBuffered(
							makeMuxRangefeedErrorEvent(streamID, 1, newErrBufferCapacityExceeded()), nil)
					},
				})
				activeStreamEnd++
			} else {
				wg.Add(1)
				go func(id int64) {
					defer wg.Done()
					sm.DisconnectStream(id, newErrBufferCapacityExceeded())
				}(activeStreamStart)
				activeStreamStart++
			}
		}

		wg.Wait()
		require.Equal(t, int32(activeStreamStart), actualSum.Load())

		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		// We stop the sender as a way to syncronize the send
		// loop. While we've waiting for the buffer to be
		// empty, we also need to know that the sender is done
		// handling the last message that it processed before
		// we observe any of the counters on the test stream.
		stopper.Stop(ctx)
		require.Equal(t, activeStreamStart, int64(testServerStream.totalEventsSent()))
		expectedActiveStreams := activeStreamEnd - activeStreamStart
		require.Equal(t, int(expectedActiveStreams), sm.activeStreamCount())
		waitForRangefeedCount(t, smMetrics, int(expectedActiveStreams))
	})

	t.Run("stream manager on stop", func(t *testing.T) {
		sm.Stop(ctx)
		require.Equal(t, int64(0), smMetrics.ActiveMuxRangeFeed.Value())
		require.Equal(t, 0, sm.activeStreamCount())
		// Cleanup functions should be called for all active streams.
		require.Equal(t, int32(activeStreamEnd), actualSum.Load())
		// No error events should be sent during Stop().
		require.Equal(t, activeStreamStart, int64(testServerStream.totalEventsSent()))

	})

	t.Run("stream manager after stop", func(t *testing.T) {
		// No events should be buffered after stopped.
		val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
		ev1 := new(kvpb.RangeFeedEvent)
		ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
		muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: 1}
		require.Equal(t, bs.sendBuffered(muxEv, nil).Error(), errors.New("stream sender is stopped").Error())
		require.Equal(t, 0, bs.len())
	})
}

// We create a new stream manager for each MuxRangeFeed call (to handle the gRPC stream).
// Since we care about buffered sender, this doesn't matter to us.
// Just wanted to note that a processor could potentially have different registrations
// registered on it that correspond to different senders (stream managers).
// But since we're stress testing, we can assume that every registration is for the same
// underlying stream to maximize the load on a single sender.

// A: From here on we focus on 1 MuxRangeFeed invocation, in which we
//  1. Create a stream manager to manage a buffered sender for the locked gRPC stream
// *2. Start the stream manager, which runs the sender's `run` method in a goroutine
//      ↳ this is done with `stopper` to get a handle on it
//  3. Start listening for requests to create a new rangefeed

// -- we've got a stream manager and are running the sender's polling method

// B: When we receive a RangeFeedRequest, we
//  1. Ask our stream manager for a new input stream called a `streamSink`
//  2. Register it with the rangefeed processor on the replica of note
//      ↳ this requires searching for that replica, hence: stores -> store -> replica
//      ↳ it's of note that if no processor exists yet, we create it here
//  3. Once it's registered, we save our stream's disconnector in the stream manager

// -- we've got an input pipe into our sender

// C: Continuing from step 2 above, to register the stream with the rangefeed processor, we
//  1. Create an unbuffered registration that pipes into the stream
//  2. Add it to the registry
// *3. Start a goroutine for the registration's `runOutputLoop`
//      ↳ for unbuffered registration, this is a quick job for catchup scans exclusively
//      ↳ for buffered registration (deprecated) it's a polling loop

// -- now the processor knows where to forward it's logical operations to
// -- all the infra is setup

//	We only have K=(32|64|128) concurrent scheduler workers
//
// ==> we can only send K events concurrently at any instant
const (
	// the degree of concurrency is limited by the number of processor workers
	workers = 8

	// this value represents the number of registrations that overlap each logical op event
	// each event will be sent this number of times to our buffered sender (via imaginary registrations)
	registrations = 100

	// workload parameters
	testDuration = 1 * time.Second
	tickInterval = 1 * time.Millisecond
	initialDelay = tickInterval / workers / 4
)

func TestMichael(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// this mock stream gives us observability into the # of events that were actually sent
	testServerStream := newTestServerStream()
	bs := NewBufferedSender(testServerStream, NewBufferedSenderMetrics())

	// initialize the stream manager like normal
	smMetrics := NewStreamManagerMetrics()
	sm := NewStreamManager(bs, smMetrics)

	// start the sender's `run` loop in a goroutine
	require.NoError(t, sm.Start(ctx, stopper))

	// this test is time-based, so we need to count the # of events enqueued by each worker
	eventsEnqueuedByWorkers := make(chan int, workers)

	var wg sync.WaitGroup
	SpawnProcessorWorker := func(id int) {
		wg.Add(1)

		taskCtx, task := trace.NewTask(ctx, fmt.Sprintf("Handler-%d", id))
		defer task.End()

		// spawn a worker in a goroutine
		require.NoError(t, stopper.RunAsyncTask(taskCtx, fmt.Sprintf("Worker %v", id),
			func(ctx context.Context) {
				// offset it's start
				time.Sleep(time.Duration(id) * initialDelay)

				stream, _ := sm.NewStream(int64(id), 42).(BufferedStream)

				ticker := time.NewTicker(tickInterval)
				timer := time.NewTimer(testDuration)

				var numEvents int
				for {
					select {
					case <-ticker.C:
						// at each tick, we consume a logical op, and create an event from it
						val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
						event := new(kvpb.RangeFeedEvent) // note: this only allocates one event per tick
						event.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val, PrevValue: val})

						// the event gets published to each overlapping registration (which we assume to be all of them)
						// we don't need a registration because all unbuffered registration does is call stream.SendBuffered
						trace.WithRegion(ctx, fmt.Sprintf("Sending %v messages", registrations), func() {
							for range registrations {
								trace.WithRegion(ctx, "SendBuffered", func() {
									if err := stream.SendBufferedWithCtx(event, nil, ctx); err != nil {
										panic("😰") // require.NoError needs synchronization; this doesn't
									}
									numEvents++
								})
							}
						})
					case <-timer.C:
						eventsEnqueuedByWorkers <- numEvents
						wg.Done()
						return
					}
				}
			}))
	}

	// let the boys run
	for i := range workers {
		SpawnProcessorWorker(i)
	}
	wg.Wait()

	// the number of events we actually sent out
	totalEventsSentBySender := testServerStream.totalEventsSent()

	// the number of events we enqueued
	close(eventsEnqueuedByWorkers)
	var totalEventsSentByWorkers int
	for n := range eventsEnqueuedByWorkers {
		totalEventsSentByWorkers += n
	}

	// if our buffered sender's been able to keep up, then we shouldn't have a backlog at the end of the test
	require.Greater(t, float64(totalEventsSentBySender)/float64(totalEventsSentByWorkers)*100, 90., "throughput needs to be >90%")
}
