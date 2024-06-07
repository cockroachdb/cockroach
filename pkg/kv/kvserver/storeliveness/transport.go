// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storeliveness

import (
	"context"
	"net"
	"runtime/pprof"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

const (
	// Outgoing messages are queued per-node on a channel of this size.
	sendBufferSize = 128

	// When no message has been queued for this duration, the corresponding
	// instance of processQueue will shut down.
	idleTimeout = time.Minute

	// connClass is the rpc ConnectionClass used by store liveness traffic.
	connClass = rpc.SystemClass
)

// MessageHandler is the interface that must be implemented by
// arguments to Transport.ListenIncomingHeartbeats.
type MessageHandler interface {
	// HandleMessage is called for each incoming Message.
	HandleMessage(ctx context.Context, msg slpb.Message)
}

// Transport handles the rpc messages for Store liveness.
//
// The transport is asynchronous with respect to the caller, and internally
// multiplexes outbound messages. Internally, each message is queued on a
// per-destination queue before being asynchronously delivered.
type Transport struct {
	log.AmbientContext
	st      *cluster.Settings
	tracer  *tracing.Tracer
	clock   *hlc.Clock
	stopper *stop.Stopper

	// Queues maintains a map[roachpb.NodeID]*sendQueue.
	queues syncutil.IntMap

	dialer   *nodedialer.Dialer
	handlers syncutil.IntMap // map[roachpb.StoreID]*MessageHandler
}

// sendQueue is a queue of outgoing Messages.
type sendQueue struct {
	msgs chan slpb.Message
	// The number of bytes in flight. Must be updated *atomically* on sending and
	// receiving from the reqs channel.
	bytes atomic.Int64
}

// NewDummyTransport returns a dummy raft transport for use in tests which need
// a non-nil raft transport that need not function.
func NewDummyTransport(st *cluster.Settings, tracer *tracing.Tracer, clock *hlc.Clock) *Transport {
	resolver := func(roachpb.NodeID) (net.Addr, error) {
		return nil, errors.New("dummy resolver")
	}
	return NewTransport(log.MakeTestingAmbientContext(tracer), st, tracer, clock,
		nodedialer.New(nil, resolver), nil, nil,
	)
}

// NewTransport creates a new store liveness Transport.
func NewTransport(
	ambient log.AmbientContext,
	st *cluster.Settings,
	tracer *tracing.Tracer,
	clock *hlc.Clock,
	dialer *nodedialer.Dialer,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
) *Transport {
	t := &Transport{
		AmbientContext: ambient,
		st:             st,
		tracer:         tracer,
		clock:          clock,
		stopper:        stopper,
		dialer:         dialer,
	}
	if grpcServer != nil {
		slpb.RegisterStoreLivenessServer(grpcServer, t)
	}
	return t
}

// visitQueues calls the visit callback on each outgoing messages sub-queue.
func (t *Transport) visitQueues(visit func(*sendQueue)) {
	t.queues.Range(func(k int64, v unsafe.Pointer) bool {
		visit((*sendQueue)(v))
		return true
	})
}

// queueMessageCount returns the total number of outgoing messages in the queue.
func (t *Transport) queueMessageCount() int64 {
	var count int64
	t.visitQueues(func(q *sendQueue) { count += int64(len(q.msgs)) })
	return count
}

// queueByteSize returns the total bytes size of outgoing messages in the queue.
func (t *Transport) queueByteSize() int64 {
	var size int64
	t.visitQueues(func(q *sendQueue) { size += q.bytes.Load() })
	return size
}

// getMessageHandler returns the registered MessageHandler for the given
// StoreID. If no handlers are registered for the StoreID, it returns (nil,
// false).
func (t *Transport) getMessageHandler(storeID roachpb.StoreID) (MessageHandler, bool) {
	if value, ok := t.handlers.Load(int64(storeID)); ok {
		return *(*MessageHandler)(value), true
	}
	return nil, false
}

// handleMessage proxies a request to the listening server interface.
func (t *Transport) handleMessage(ctx context.Context, msg slpb.Message) error {
	h, ok := t.getMessageHandler(msg.To.StoreID)
	if !ok {
		log.Warningf(ctx, "unable to accept message %+v from %+v: no handler registered for %+v",
			msg, msg.From, msg.To)
		return kvpb.NewStoreNotFoundError(msg.To.StoreID)
	}

	h.HandleMessage(ctx, msg)
	return nil
}

// Stream proxies the incoming requests to the listening server interface.
func (t *Transport) Stream(stream slpb.StoreLiveness_StreamServer) error {
	errCh := make(chan error, 1)

	// Node stopping error is caught below in the select.
	taskCtx, cancel := t.stopper.WithCancelOnQuiesce(stream.Context())
	taskCtx = t.AnnotateCtx(taskCtx)
	defer cancel()

	if err := t.stopper.RunAsyncTaskEx(
		taskCtx,
		stop.TaskOpts{
			TaskName: "storeliveness.Transport: processing batch",
			SpanOpt:  stop.ChildSpan,
		}, func(ctx context.Context) {
			errCh <- func() error {
				for {
					batch, err := stream.Recv()
					if err != nil {
						return err
					}
					if !batch.Now.IsEmpty() {
						t.clock.Update(batch.Now)
					}
					for _, msg := range batch.Messages {
						if pErr := t.handleMessage(ctx, msg); pErr != nil {
							return err
						}
					}
				}
			}()
		}); err != nil {
		return err
	}

	select {
	case err := <-errCh:
		return err
	case <-t.stopper.ShouldQuiesce():
		return nil
	}
}

// ListenMessages registers a MessageHandler to receive proxied messages.
func (t *Transport) ListenMessages(storeID roachpb.StoreID, handler MessageHandler) {
	t.handlers.Store(int64(storeID), unsafe.Pointer(&handler))
}

// processQueue opens a Store Liveness client stream and sends messages from the
// designated queue (ch) via that stream, exiting when an error is received or
// when it idles out. All messages remaining in the queue at that point are
// lost and a new instance of processQueue will be started by the next message
// to be sent.
func (t *Transport) processQueue(q *sendQueue, stream slpb.StoreLiveness_StreamClient) (err error) {
	defer func() {
		_, closeErr := stream.CloseAndRecv()
		err = errors.Join(err, closeErr)
	}()

	var it timeutil.Timer
	defer it.Stop()
	batch := &slpb.MessageBatch{}
	for {
		it.Reset(idleTimeout)
		select {
		case <-t.stopper.ShouldQuiesce():
			return nil

		case <-it.C:
			it.Read = true
			return nil

		case msg := <-q.msgs:
			size := int64(msg.Size())
			q.bytes.Add(-size)
			batch.Messages = append(batch.Messages, msg)

			// Pull off as many queued requests as possible, within reason.
			select {
			case msg = <-q.msgs:
				size := int64(msg.Size())
				q.bytes.Add(-size)
				batch.Messages = append(batch.Messages, msg)
			default:
			}

			batch.Now = t.clock.NowAsClockTimestamp()
			if err := stream.Send(batch); err != nil {
				return err
			}

			// Reuse the Messages slice, but zero out the contents to avoid delaying
			// GC of memory referenced from within.
			for i := range batch.Messages {
				batch.Messages[i] = slpb.Message{}
			}
			batch.Messages = batch.Messages[:0]
			batch.Now = hlc.ClockTimestamp{}
		}
	}
}

// getQueue returns the queue for the specified node ID and a boolean
// indicating whether the queue already exists (true) or was created (false).
func (t *Transport) getQueue(nodeID roachpb.NodeID) (*sendQueue, bool) {
	value, ok := t.queues.Load(int64(nodeID))
	if !ok {
		q := sendQueue{
			msgs: make(chan slpb.Message, sendBufferSize),
		}
		value, ok = t.queues.LoadOrStore(int64(nodeID), unsafe.Pointer(&q))
	}
	return (*sendQueue)(value), ok
}

// SendAsync sends a message to the recipient specified in the request. It
// returns false if the outgoing queue is full. The returned bool may be a false
// positive but will never be a false negative; if sent is true the message may
// or may not actually be sent but if it's false the message definitely was not
// sent. It is not safe to continue using the reference to the provided request.
func (t *Transport) SendAsync(msg slpb.Message) (sent bool) {
	toNodeID := msg.To.NodeID
	//defer func() {
	//	if !sent {
	//		t.metrics.MessagesDropped.Inc(1)
	//	}
	//}()

	if b, ok := t.dialer.GetCircuitBreaker(toNodeID, connClass); ok && b.Signal().Err() != nil {
		return false
	}

	q, existingQueue := t.getQueue(toNodeID)
	if !existingQueue {
		// Note that startProcessNewQueue is in charge of deleting the queue.
		ctx := t.AnnotateCtx(context.Background())
		if !t.startProcessNewQueue(ctx, toNodeID) {
			return false
		}
	}

	// Note: computing the size of the request *before* sending it to the queue,
	// because the receiver takes ownership of, and can modify it.
	size := int64(msg.Size())
	select {
	case q.msgs <- msg:
		q.bytes.Add(size)
		return true
	default:
		//if logSendQueueFullEvery.ShouldLog() {
		log.Warningf(t.AnnotateCtx(context.Background()), "store liveness send queue to n%d is full", toNodeID)
		//}
		return false
	}
}

// startProcessNewQueue connects to the node and launches a worker goroutine
// that processes the queue for the given nodeID (which must exist) until
// the underlying connection is closed or an error occurs. This method
// takes on the responsibility of deleting the queue when the worker shuts down.
// The class parameter dictates the ConnectionClass which should be used to dial
// the remote node. Traffic for system ranges and heartbeats will receive a
// different class than that of user data ranges.
//
// Returns whether the worker was started (the queue is deleted either way).
func (t *Transport) startProcessNewQueue(
	ctx context.Context, toNodeID roachpb.NodeID,
) (started bool) {
	cleanup := func(q *sendQueue) {
		// Account for the remainder of `ch` which was never sent.
		// NB: we deleted the queue above, so within a short amount
		// of time nobody should be writing into the channel any
		// more. We might miss a message or two here, but that's
		// OK (there's nobody who can safely close the channel the
		// way the code is written).
		for {
			select {
			case msg := <-q.msgs:
				q.bytes.Add(-int64(msg.Size()))
				//t.metrics.MessagesDropped.Inc(1)
			default:
				return
			}
		}
	}
	worker := func(ctx context.Context) {
		q, existingQueue := t.getQueue(toNodeID)
		if !existingQueue {
			log.Fatalf(ctx, "queue for n%d does not exist", toNodeID)
		}
		defer cleanup(q)
		defer func() {
			t.queues.Delete(int64(toNodeID))
		}()
		conn, err := t.dialer.Dial(ctx, toNodeID, connClass)
		if err != nil {
			// DialNode already logs sufficiently, so just return.
			return
		}

		client := slpb.NewStoreLivenessClient(conn)
		streamCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		stream, err := client.Stream(streamCtx) // closed via cancellation
		if err != nil {
			log.Warningf(ctx, "creating stream client for node %d failed: %+v", toNodeID, err)
			return
		}

		if err := t.processQueue(q, stream); err != nil {
			log.Warningf(ctx, "while processing outgoing queue to node %d: %s:", toNodeID, err)
		}
	}
	err := t.stopper.RunAsyncTask(ctx, "storeliveness.Transport: sending messages",
		func(ctx context.Context) {
			pprof.Do(ctx, pprof.Labels("remote_node_id", toNodeID.String()), worker)
		})
	if err != nil {
		t.queues.Delete(int64(toNodeID))
		return false
	}
	return true
}
