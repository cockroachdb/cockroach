// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"runtime/trace"
	"sort"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
func latencyPercentile(latencies []time.Duration, p float64) time.Duration {
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	index := int(float64(len(latencies)) * p)
	if index >= len(latencies) {
		index = len(latencies) - 1
	}
	if index < 0 {
		index = 0
	}
	return latencies[index]
}

func TestThroughput(t *testing.T) {
	const (
		workers         = 32
		eventsPerWorker = eventQueueChunkSize
		// we assume that the each event overlaps with all registrations
		// so the total # of events = workers * eventsPerWorker * registrationsPerProcessor
		registrationsPerProcessor = 10
	)

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

	var wg sync.WaitGroup
	SpawnProcessorWorker := func(id int) {
		wg.Add(1)

		taskCtx, task := trace.NewTask(ctx, fmt.Sprintf("Handler-%d", id))
		defer task.End()

		// spawn a worker in a goroutine
		require.NoError(t, stopper.RunAsyncTask(taskCtx, fmt.Sprintf("Worker %v", id),
			func(ctx context.Context) {
				stream, _ := sm.NewStream(int64(id), 42).(BufferedStream)

				// We offset the start times to make it a little more realistic
				time.Sleep(time.Duration(id) * time.Millisecond)

				for range eventsPerWorker {
					// at each tick, we consume a logical op, and create an event from it
					val := roachpb.Value{
						RawBytes: []byte("val"),
						Timestamp: hlc.Timestamp{
							WallTime: timeutil.Now().UnixNano(),
						},
					}
					event := new(kvpb.RangeFeedEvent) // note: this only allocates one event per tick
					event.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val, PrevValue: val})

					// the event gets published to each overlapping registration (which we assume to be all of them)
					// we don't need a real registration because all unbuffered registration does is call stream.SendBuffered
					j := 0
					for j < registrationsPerProcessor {
						if err := stream.SendBuffered(event, nil); err != nil {
							// NOTE: this isn't realistic
							// In reality, if we hit the buffer limit, we'd drop the registration and start a catchup scan,
							// so we wouldn't be placing continuous pressure on the mutex like we're doing here.
							// However, the overhead of a catchup scan is highly variable so it's hard to place a
							// time.Sleep() here that would accurately capture the impact that running a catchup
							// scan would have on the overall throughput.
							continue
						}
						j++
					}
				}
				wg.Done()
			}))
	}

	// let the boys run
	start := time.Now()
	for i := range workers {
		SpawnProcessorWorker(i)
	}
	wg.Wait()
	testServerStream.waitForEventCount(t, eventsPerWorker*workers*registrationsPerProcessor)

	dur := time.Since(start)

	totalEventsSentBySender := testServerStream.totalEventsSent()
	throughput := float64(totalEventsSentBySender) / dur.Seconds()

	for streamID, latencies := range testServerStream.latencies {
		p50 := latencyPercentile(latencies, 0.5)
		p90 := latencyPercentile(latencies, 0.9)
		max := latencyPercentile(latencies, 1)
		fmt.Printf("StreamID:%v, p50: %v, p90: %v, max: %v\n", streamID, p50, p90, max)
	}

	fmt.Printf("totalEventsSentBySender: %v in %v\n", totalEventsSentBySender, dur)
	fmt.Printf("throughput: %v\n", throughput)
}
