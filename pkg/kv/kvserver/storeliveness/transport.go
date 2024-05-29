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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"net"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// Outgoing messages are queued per-node on a channel of this size.
	//
	// This buffer was sized many moons ago and is very large. If the
	// buffer fills up, we drop raft messages, so we'd be in trouble.
	// But as is, the buffer can hold to a lot of memory, especially
	// during RESTORE/IMPORT where we're routinely sending out SSTs,
	// which weigh in at a few mbs each; an individual raft instance
	// will limit how many it has in-flight per-follower, but groups
	// don't compete among each other for budget.
	sendBufferSize = 10000

	// When no message has been queued for this duration, the corresponding
	// instance of processQueue will shut down.
	//
	// TODO(tamird): make culling of outbound streams more evented, so that we
	// need not rely on this timeout to shut things down.
	idleTimeout = time.Minute
)

// targetOutgoingBatchSize wraps "kv.storeliveness.target_batch_size".
var targetOutgoingBatchSize = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.storeliveness.target_batch_size",
	"size of a batch of store liveness batch after which it will be sent without further batching",
	64<<20, // 64 MB
	settings.PositiveInt,
)

// HeartbeatResponseStream is the subset of the StoreLiveness_HeartbeatServer
// interface that is needed for sending responses.
type HeartbeatResponseStream interface {
	Send(response *storelivenesspb.HeartbeatResponse) error
}

// lockedMessageResponseStream is an implementation of
// HeartbeatResponseStream which provides support for concurrent calls to
// Send. Note that the default implementation of grpc.Stream for server
// responses (grpc.serverStream) is not safe for concurrent calls to Send.
type lockedHeartbeatResponseStream struct {
	wrapped storelivenesspb.StoreLiveness_HeartbeatServer
	sendMu  syncutil.Mutex
}

func (s *lockedHeartbeatResponseStream) Send(resp *storelivenesspb.HeartbeatResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.wrapped.Send(resp)
}

func (s *lockedHeartbeatResponseStream) Recv() (*storelivenesspb.HeartbeatRequestBatch, error) {
	// No need for lock. gRPC.Stream.RecvMsg is safe for concurrent use.
	return s.wrapped.Recv()
}

// HeartbeatHandler is the interface that must be implemented by
// arguments to Transport.ListenIncomingHeartbeats.
type HeartbeatHandler interface {
	// HandleHeartbeatRequest is called for each incoming Heartbeat. The request is
	// always processed asynchronously and the response is sent over respStream.
	// If an error is encountered during asynchronous processing, it will be
	// streamed back to the sender of the message as a HeartbeatResponse.
	HandleHeartbeatRequest(ctx context.Context, req *storelivenesspb.HeartbeatRequest,
		respStream HeartbeatResponseStream) *kvpb.Error

	// HandleHeartbeatResponse is called for each HeartbeatResponse. Note that
	// not all messages receive a response. An error is returned if and only if
	// the underlying store liveness connection should be closed.
	HandleHeartbeatResponse(context.Context, *storelivenesspb.HeartbeatResponse) error
}

// SLTransport handles the rpc messages for Store liveness.
//
// The transport is asynchronous with respect to the caller, and
// internally multiplexes outbound messages. Internally, each message is
// queued on a per-destination queue before being asynchronously delivered.
//
// Callers are required to construct a HeartbeatSender before being able to
// dispatch messages, and must provide an error handler which will be invoked
// asynchronously in the event that the recipient of any message closes its
// inbound RPC stream. This callback is asynchronous with respect to the
// outbound message which caused the remote to hang up; all that is known is
// which remote hung up.
type SLTransport struct {
	log.AmbientContext
	st      *cluster.Settings
	tracer  *tracing.Tracer
	clock   *hlc.Clock
	stopper *stop.Stopper
	//metrics *TransportMetrics

	// Queues maintains a map[roachpb.NodeID]*sendQueue on a per rpc-class
	// level.
	queues [rpc.NumConnectionClasses]syncutil.IntMap

	dialer            *nodedialer.Dialer
	heartbeatHandlers syncutil.IntMap // map[roachpb.StoreID]*HeartbeatHandler
}

// sendQueue is a queue of outgoing HeartbeatRequests.
type sendQueue struct {
	reqs chan *storelivenesspb.HeartbeatRequest
	// The number of bytes in flight. Must be updated *atomically* on sending and
	// receiving from the reqs channel.
	bytes atomic.Int64
}

// NewDummyTransport returns a dummy raft transport for use in tests which
// need a non-nil raft transport that need not function.
func NewDummyTransport(
	st *cluster.Settings, tracer *tracing.Tracer, clock *hlc.Clock,
) *SLTransport {
	resolver := func(roachpb.NodeID) (net.Addr, error) {
		return nil, errors.New("dummy resolver")
	}
	return NewSLTransport(log.MakeTestingAmbientContext(tracer), st, tracer, clock,
		nodedialer.New(nil, resolver), nil, nil,
	)
}

// NewSLTransport creates a new store liveness Transport.
func NewSLTransport(
	ambient log.AmbientContext,
	st *cluster.Settings,
	tracer *tracing.Tracer,
	clock *hlc.Clock,
	dialer *nodedialer.Dialer,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
) *SLTransport {
	t := &SLTransport{
		AmbientContext: ambient,
		st:             st,
		tracer:         tracer,
		clock:          clock,
		stopper:        stopper,
		dialer:         dialer,
	}
	if grpcServer != nil {
		storelivenesspb.RegisterStoreLivenessServer(grpcServer, t)
	}
	return t
}

// visitQueues calls the visit callback on each outgoing messages sub-queue.
func (t *SLTransport) visitQueues(visit func(*sendQueue)) {
	for class := range t.queues {
		t.queues[class].Range(func(k int64, v unsafe.Pointer) bool {
			visit((*sendQueue)(v))
			return true
		})
	}
}

// queueMessageCount returns the total number of outgoing heeartbeats in the queue.
func (t *SLTransport) queueHeartbeatCount() int64 {
	var count int64
	t.visitQueues(func(q *sendQueue) { count += int64(len(q.reqs)) })
	return count
}

// queueByteSize returns the total bytes size of outgoing messages in the queue.
func (t *SLTransport) queueByteSize() int64 {
	var size int64
	t.visitQueues(func(q *sendQueue) { size += q.bytes.Load() })
	return size
}

// getIncomingHeartbeatHandler returns the registered
// IncomingHeartbeatHandler for the given StoreID. If no handlers are
// registered for the StoreID, it returns (nil, false).
func (t *SLTransport) getIncomingHeartbeatHandler(
	storeID roachpb.StoreID,
) (HeartbeatHandler, bool) {
	if value, ok := t.heartbeatHandlers.Load(int64(storeID)); ok {
		return *(*HeartbeatHandler)(value), true
	}
	return nil, false
}

// handleHeartbeatRequest proxies a request to the listening server interface.
func (t *SLTransport) handleHeartbeatRequest(
	ctx context.Context, req *storelivenesspb.HeartbeatRequest, respStream HeartbeatResponseStream,
) *kvpb.Error {
	incomingHeartbeatHandler, ok := t.getIncomingHeartbeatHandler(req.Header.To.StoreID)
	if !ok {
		log.Warningf(ctx, "unable to accept Heartbeat message from %+v: no handler registered for %+v",
			req.Header.From, req.Header.To)
		return kvpb.NewError(kvpb.NewStoreNotFoundError(req.Header.To.StoreID))
	}

	return incomingHeartbeatHandler.HandleHeartbeatRequest(ctx, req, respStream)
}

// newHeartbeatResponse constructs a HeartbeatResponse from the
// given request and error.
func newHeartbeatResponse(
	req *storelivenesspb.HeartbeatRequest, pErr *kvpb.Error,
) *storelivenesspb.HeartbeatResponse {
	resp := &storelivenesspb.HeartbeatResponse{
		Header: storelivenesspb.Header{
			From: req.Header.To,
			To:   req.Header.From,
		},
		Heartbeat: req.Heartbeat,
	}
	if pErr != nil {
		resp.Union.SetValue(pErr)
	}
	return resp
}

// Heartbeat proxies the incoming requests to the listening server interface.
func (t *SLTransport) Heartbeat(stream storelivenesspb.StoreLiveness_HeartbeatServer) (lastErr error) {
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
				stream := &lockedHeartbeatResponseStream{wrapped: stream}
				for {
					batch, err := stream.Recv()
					if err != nil {
						return err
					}
					if !batch.Now.IsEmpty() {
						t.clock.Update(batch.Now)
					}
					if len(batch.Requests) == 0 {
						continue
					}
					for i := range batch.Requests {
						req := &batch.Requests[i]
						//t.metrics.HeartbeatsRcvd.Inc(1)
						if pErr := t.handleHeartbeatRequest(ctx, req, stream); pErr != nil {
							if err := stream.Send(newHeartbeatResponse(req, pErr)); err != nil {
								return err
							}
							//t.metrics.ReverseSent.Inc(1)
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

// ListenHeartbeatMessages registers a HeartbeatHandler to receive proxied messages.
func (t *SLTransport) ListenHeartbeatMessages(
	storeID roachpb.StoreID, handler HeartbeatHandler,
) {
	t.heartbeatHandlers.Store(int64(storeID), unsafe.Pointer(&handler))
}

// StopHeartbeats unregisters a IncomingHeartbeatHandler.
func (t *SLTransport) StopHeartbeats(storeID roachpb.StoreID) {
	t.heartbeatHandlers.Delete(int64(storeID))
}

// processQueue opens a Store liveness client stream and sends messages from the
// designated queue (ch) via that stream, exiting when an error is received or
// when it idles out. All messages remaining in the queue at that point are
// lost and a new instance of processQueue will be started by the next message
// to be sent.
func (t *SLTransport) processQueue(
	q *sendQueue, stream storelivenesspb.StoreLiveness_HeartbeatClient, class rpc.ConnectionClass,
) error {
	errCh := make(chan error, 1)

	ctx := stream.Context()

	if err := t.stopper.RunAsyncTask(
		ctx, "storeliveness.Transport: processing queue",
		func(ctx context.Context) {
			errCh <- func() error {
				for {
					resp, err := stream.Recv()
					if err != nil {
						return err
					}
					//t.metrics.ReverseRcvd.Inc(1)
					incomingMessageHandler, ok := t.getIncomingHeartbeatHandler(resp.Header.To.StoreID)
					if !ok {
						log.Warningf(ctx, "no handler found for store %s in response %s",
							resp.Header.To.StoreID, resp)
						continue
					}
					if err := incomingMessageHandler.HandleHeartbeatResponse(ctx, resp); err != nil {
						return err
					}
				}
			}()
		}); err != nil {
		return err
	}

	annotateWithClockTimestamp := func(batch *storelivenesspb.HeartbeatRequestBatch) {
		batch.Now = t.clock.NowAsClockTimestamp()
	}

	clearRequestBatch := func(batch *storelivenesspb.HeartbeatRequestBatch) {
		// Reuse the Requests slice, but zero out the contents to avoid delaying
		// GC of memory referenced from within.
		for i := range batch.Requests {
			batch.Requests[i] = storelivenesspb.HeartbeatRequest{}
		}
		batch.Requests = batch.Requests[:0]
		batch.StoreIDs = nil
		batch.Now = hlc.ClockTimestamp{}
	}

	var it timeutil.Timer
	defer it.Stop()

	batch := &storelivenesspb.HeartbeatRequestBatch{}
	for {
		it.Reset(idleTimeout)
		select {
		case <-t.stopper.ShouldQuiesce():
			return nil

		case <-it.C:
			it.Read = true
			return nil

		case err := <-errCh:
			return err

		case req := <-q.reqs:
			size := int64(req.Size())
			q.bytes.Add(-size)
			budget := targetOutgoingBatchSize.Get(&t.st.SV) - size

			batch.Requests = append(batch.Requests, *req)
			releaseHeartbeatRequest(req)

			// Pull off as many queued requests as possible, within reason.
			for budget > 0 {
				select {
				case req = <-q.reqs:
					size := int64(req.Size())
					q.bytes.Add(-size)
					budget -= size
					batch.Requests = append(batch.Requests, *req)
					releaseHeartbeatRequest(req)
				default:
					budget = -1
				}
			}

			annotateWithClockTimestamp(batch)
			if err := stream.Send(batch); err != nil {
				return err
			}
			//t.metrics.MessagesSent.Inc(int64(len(batch.Requests)))
			clearRequestBatch(batch)
		}
	}
}

var heartbeatRequestPool = sync.Pool{
	New: func() interface{} {
		return &storelivenesspb.HeartbeatRequest{}
	},
}

func newHeartbeatRequest() *storelivenesspb.HeartbeatRequest {
	return heartbeatRequestPool.Get().(*storelivenesspb.HeartbeatRequest)
}

func releaseHeartbeatRequest(m *storelivenesspb.HeartbeatRequest) {
	*m = storelivenesspb.HeartbeatRequest{}
	heartbeatRequestPool.Put(m)
}

// getQueue returns the queue for the specified node ID and a boolean
// indicating whether the queue already exists (true) or was created (false).
func (t *SLTransport) getQueue(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (*sendQueue, bool) {
	queuesMap := &t.queues[class]
	value, ok := queuesMap.Load(int64(nodeID))
	if !ok {
		q := sendQueue{
			reqs: make(chan *storelivenesspb.HeartbeatRequest, sendBufferSize),
		}
		value, ok = queuesMap.LoadOrStore(int64(nodeID), unsafe.Pointer(&q))
	}
	return (*sendQueue)(value), ok
}

// SendAsync sends a message to the recipient specified in the request. It
// returns false if the outgoing queue is full. The returned bool may be a false
// positive but will never be a false negative; if sent is true the message may
// or may not actually be sent but if it's false the message definitely was not
// sent. It is not safe to continue using the reference to the provided request.
func (t *SLTransport) SendAsync(
	req *storelivenesspb.HeartbeatRequest, class rpc.ConnectionClass,
) (sent bool) {
	toNodeID := req.Header.To.NodeID
	defer func() {
		if !sent {
			//t.metrics.MessagesDropped.Inc(1)
			releaseHeartbeatRequest(req)
		}
	}()

	if b, ok := t.dialer.GetCircuitBreaker(toNodeID, class); ok && b.Signal().Err() != nil {
		return false
	}

	q, existingQueue := t.getQueue(toNodeID, class)
	if !existingQueue {
		// Note that startProcessNewQueue is in charge of deleting the queue.
		ctx := t.AnnotateCtx(context.Background())
		if !t.startProcessNewQueue(ctx, toNodeID, class) {
			return false
		}
	}

	// Note: computing the size of the request *before* sending it to the queue,
	// because the receiver takes ownership of, and can modify it.
	size := int64(req.Size())
	select {
	case q.reqs <- req:
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
func (t *SLTransport) startProcessNewQueue(
	ctx context.Context, toNodeID roachpb.NodeID, class rpc.ConnectionClass,
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
			case req := <-q.reqs:
				q.bytes.Add(-int64(req.Size()))
				//t.metrics.MessagesDropped.Inc(1)
				releaseHeartbeatRequest(req)
			default:
				return
			}
		}
	}
	worker := func(ctx context.Context) {
		q, existingQueue := t.getQueue(toNodeID, class)
		if !existingQueue {
			log.Fatalf(ctx, "queue for n%d does not exist", toNodeID)
		}
		defer cleanup(q)
		defer func() {
			t.queues[class].Delete(int64(toNodeID))
		}()
		conn, err := t.dialer.Dial(ctx, toNodeID, class)
		if err != nil {
			// DialNode already logs sufficiently, so just return.
			return
		}

		client := storelivenesspb.NewStoreLivenessClient(conn)
		batchCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		stream, err := client.Heartbeat(batchCtx) // closed via cancellation
		if err != nil {
			log.Warningf(ctx, "creating batch client for node %d failed: %+v", toNodeID, err)
			return
		}

		if err := t.processQueue(q, stream, class); err != nil {
			log.Warningf(ctx, "while processing outgoing queue to node %d: %s:", toNodeID, err)
		}
	}
	err := t.stopper.RunAsyncTask(ctx, "storeliveness.Transport: sending/receiving messages",
		func(ctx context.Context) {
			pprof.Do(ctx, pprof.Labels("remote_node_id", toNodeID.String()), worker)
		})
	if err != nil {
		t.queues[class].Delete(int64(toNodeID))
		return false
	}
	return true
}
