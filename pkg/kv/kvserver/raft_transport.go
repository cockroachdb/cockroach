// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

const (
	// Outgoing messages are queued per-node on a channel of this size.
	//
	// TODO(peter): The normal send buffer size is larger than we would like. It
	// is a temporary patch for the issue discussed in #8630 where
	// Store.HandleRaftRequest can block applying a preemptive snapshot for a
	// long enough period of time that grpc flow control kicks in and messages
	// are dropped on the sending side.
	raftSendBufferSize = 10000

	// When no message has been queued for this duration, the corresponding
	// instance of processQueue will shut down.
	//
	// TODO(tamird): make culling of outbound streams more evented, so that we
	// need not rely on this timeout to shut things down.
	raftIdleTimeout = time.Minute
)

// RaftMessageResponseStream is the subset of the
// MultiRaft_RaftMessageServer interface that is needed for sending responses.
type RaftMessageResponseStream interface {
	Context() context.Context
	Send(*RaftMessageResponse) error
}

// lockedRaftMessageResponseStream is an implementation of
// RaftMessageResponseStream which provides support for concurrent calls to
// Send. Note that the default implementation of grpc.Stream for server
// responses (grpc.serverStream) is not safe for concurrent calls to Send.
type lockedRaftMessageResponseStream struct {
	wrapped MultiRaft_RaftMessageBatchServer
	sendMu  syncutil.Mutex
}

func (s *lockedRaftMessageResponseStream) Context() context.Context {
	return s.wrapped.Context()
}

func (s *lockedRaftMessageResponseStream) Send(resp *RaftMessageResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.wrapped.Send(resp)
}

func (s *lockedRaftMessageResponseStream) Recv() (*RaftMessageRequestBatch, error) {
	// No need for lock. gRPC.Stream.RecvMsg is safe for concurrent use.
	return s.wrapped.Recv()
}

// SnapshotResponseStream is the subset of the
// MultiRaft_RaftSnapshotServer interface that is needed for sending responses.
type SnapshotResponseStream interface {
	Context() context.Context
	Send(*SnapshotResponse) error
	Recv() (*SnapshotRequest, error)
}

// RaftMessageHandler is the interface that must be implemented by
// arguments to RaftTransport.Listen.
type RaftMessageHandler interface {
	// HandleRaftRequest is called for each incoming Raft message. The request is
	// always processed asynchronously and the response is sent over respStream.
	// If an error is encountered during asynchronous processing, it will be
	// streamed back to the sender of the message as a RaftMessageResponse.
	HandleRaftRequest(ctx context.Context, req *RaftMessageRequest,
		respStream RaftMessageResponseStream) *roachpb.Error

	// HandleRaftResponse is called for each raft response. Note that
	// not all messages receive a response. An error is returned if and only if
	// the underlying Raft connection should be closed.
	HandleRaftResponse(context.Context, *RaftMessageResponse) error

	// HandleSnapshot is called for each new incoming snapshot stream, after
	// parsing the initial SnapshotRequest_Header on the stream.
	HandleSnapshot(header *SnapshotRequest_Header, respStream SnapshotResponseStream) error
}

type raftTransportStats struct {
	nodeID        roachpb.NodeID
	queue         int
	queueMax      int32
	clientSent    int64
	clientRecv    int64
	clientDropped int64
	serverSent    int64
	serverRecv    int64
}

type raftTransportStatsSlice []*raftTransportStats

func (s raftTransportStatsSlice) Len() int           { return len(s) }
func (s raftTransportStatsSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s raftTransportStatsSlice) Less(i, j int) bool { return s[i].nodeID < s[j].nodeID }

// RaftTransport handles the rpc messages for raft.
//
// The raft transport is asynchronous with respect to the caller, and
// internally multiplexes outbound messages. Internally, each message is
// queued on a per-destination queue before being asynchronously delivered.
//
// Callers are required to construct a RaftSender before being able to
// dispatch messages, and must provide an error handler which will be invoked
// asynchronously in the event that the recipient of any message closes its
// inbound RPC stream. This callback is asynchronous with respect to the
// outbound message which caused the remote to hang up; all that is known is
// which remote hung up.
type RaftTransport struct {
	log.AmbientContext
	st *cluster.Settings

	stopper *stop.Stopper

	queues   [rpc.NumConnectionClasses]syncutil.IntMap // map[roachpb.NodeID]*chan *RaftMessageRequest
	stats    [rpc.NumConnectionClasses]syncutil.IntMap // map[roachpb.NodeID]*raftTransportStats
	dialer   *nodedialer.Dialer
	handlers syncutil.IntMap // map[roachpb.StoreID]*RaftMessageHandler
}

// NewDummyRaftTransport returns a dummy raft transport for use in tests which
// need a non-nil raft transport that need not function.
func NewDummyRaftTransport(st *cluster.Settings) *RaftTransport {
	resolver := func(roachpb.NodeID) (net.Addr, error) {
		return nil, errors.New("dummy resolver")
	}
	return NewRaftTransport(log.AmbientContext{Tracer: st.Tracer}, st,
		nodedialer.New(nil, resolver), nil, nil)
}

// NewRaftTransport creates a new RaftTransport.
func NewRaftTransport(
	ambient log.AmbientContext,
	st *cluster.Settings,
	dialer *nodedialer.Dialer,
	grpcServer *grpc.Server,
	stopper *stop.Stopper,
) *RaftTransport {
	t := &RaftTransport{
		AmbientContext: ambient,
		st:             st,

		stopper: stopper,
		dialer:  dialer,
	}

	if grpcServer != nil {
		RegisterMultiRaftServer(grpcServer, t)
	}
	// statsMap is used to associate a queue with its raftTransportStats.
	statsMap := make(map[roachpb.NodeID]*raftTransportStats)
	clearStatsMap := func() {
		for k := range statsMap {
			delete(statsMap, k)
		}
	}
	if t.stopper != nil && log.V(1) {
		ctx := t.AnnotateCtx(context.Background())
		_ = t.stopper.RunAsyncTask(ctx, "raft-transport", func(ctx context.Context) {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			lastStats := make(map[roachpb.NodeID]raftTransportStats)
			lastTime := timeutil.Now()
			var stats raftTransportStatsSlice
			for {
				select {
				case <-ticker.C:
					stats = stats[:0]
					getStats := func(k int64, v unsafe.Pointer) bool {
						s := (*raftTransportStats)(v)
						// Clear the queue length stat. Note that this field is only
						// mutated by this goroutine.
						s.queue = 0
						stats = append(stats, s)
						statsMap[roachpb.NodeID(k)] = s
						return true
					}
					setQueueLength := func(k int64, v unsafe.Pointer) bool {
						ch := *(*chan *RaftMessageRequest)(v)
						if s, ok := statsMap[roachpb.NodeID(k)]; ok {
							s.queue += len(ch)
						}
						return true
					}
					for c := range t.stats {
						clearStatsMap()
						t.stats[c].Range(getStats)
						t.queues[c].Range(setQueueLength)
					}
					clearStatsMap() // no need to hold on to references to stats

					now := timeutil.Now()
					elapsed := now.Sub(lastTime).Seconds()
					sort.Sort(stats)

					var buf bytes.Buffer
					// NB: The header is 80 characters which should display in a single
					// line on most terminals.
					fmt.Fprintf(&buf,
						"         qlen   qmax   qdropped client-sent client-recv server-sent server-recv\n")
					for _, s := range stats {
						last := lastStats[s.nodeID]
						cur := raftTransportStats{
							nodeID:        s.nodeID,
							queue:         s.queue,
							queueMax:      atomic.LoadInt32(&s.queueMax),
							clientDropped: atomic.LoadInt64(&s.clientDropped),
							clientSent:    atomic.LoadInt64(&s.clientSent),
							clientRecv:    atomic.LoadInt64(&s.clientRecv),
							serverSent:    atomic.LoadInt64(&s.serverSent),
							serverRecv:    atomic.LoadInt64(&s.serverRecv),
						}
						fmt.Fprintf(&buf, "  %3d: %6d %6d %10d %11.1f %11.1f %11.1f %11.1f\n",
							cur.nodeID, cur.queue, cur.queueMax, cur.clientDropped,
							float64(cur.clientSent-last.clientSent)/elapsed,
							float64(cur.clientRecv-last.clientRecv)/elapsed,
							float64(cur.serverSent-last.serverSent)/elapsed,
							float64(cur.serverRecv-last.serverRecv)/elapsed)
						lastStats[s.nodeID] = cur
					}
					lastTime = now
					log.Infof(ctx, "stats:\n%s", buf.String())
				case <-t.stopper.ShouldQuiesce():
					return
				}
			}
		})
	}

	return t
}

func (t *RaftTransport) queuedMessageCount() int64 {
	var n int64
	addLength := func(k int64, v unsafe.Pointer) bool {
		ch := *(*chan *RaftMessageRequest)(v)
		n += int64(len(ch))
		return true
	}
	for class := range t.queues {
		t.queues[class].Range(addLength)
	}
	return n
}

func (t *RaftTransport) getHandler(storeID roachpb.StoreID) (RaftMessageHandler, bool) {
	if value, ok := t.handlers.Load(int64(storeID)); ok {
		return *(*RaftMessageHandler)(value), true
	}
	return nil, false
}

// handleRaftRequest proxies a request to the listening server interface.
func (t *RaftTransport) handleRaftRequest(
	ctx context.Context, req *RaftMessageRequest, respStream RaftMessageResponseStream,
) *roachpb.Error {
	handler, ok := t.getHandler(req.ToReplica.StoreID)
	if !ok {
		log.Warningf(ctx, "unable to accept Raft message from %+v: no handler registered for %+v",
			req.FromReplica, req.ToReplica)
		return roachpb.NewError(roachpb.NewStoreNotFoundError(req.ToReplica.StoreID))
	}

	return handler.HandleRaftRequest(ctx, req, respStream)
}

// newRaftMessageResponse constructs a RaftMessageResponse from the
// given request and error.
func newRaftMessageResponse(req *RaftMessageRequest, pErr *roachpb.Error) *RaftMessageResponse {
	resp := &RaftMessageResponse{
		RangeID: req.RangeID,
		// From and To are reversed in the response.
		ToReplica:   req.FromReplica,
		FromReplica: req.ToReplica,
	}
	if pErr != nil {
		resp.Union.SetValue(pErr)
	}
	return resp
}

func (t *RaftTransport) getStats(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) *raftTransportStats {
	statsMap := &t.stats[class]
	value, ok := statsMap.Load(int64(nodeID))
	if !ok {
		stats := &raftTransportStats{nodeID: nodeID}
		value, _ = statsMap.LoadOrStore(int64(nodeID), unsafe.Pointer(stats))
	}
	return (*raftTransportStats)(value)
}

// RaftMessageBatch proxies the incoming requests to the listening server interface.
func (t *RaftTransport) RaftMessageBatch(stream MultiRaft_RaftMessageBatchServer) error {
	errCh := make(chan error, 1)

	// Node stopping error is caught below in the select.
	if err := t.stopper.RunAsyncTask(
		stream.Context(), "storage.RaftTransport: processing batch",
		func(ctx context.Context) {
			errCh <- func() error {
				var stats *raftTransportStats
				stream := &lockedRaftMessageResponseStream{wrapped: stream}
				for {
					batch, err := stream.Recv()
					if err != nil {
						return err
					}
					if len(batch.Requests) == 0 {
						continue
					}

					// This code always uses the DefaultClass. Class is primarily a
					// client construct and the server has no way to determine which
					// class an inbound connection holds on the client side. Because of
					// this we associate all server receives and sends with the
					// DefaultClass. This data is exclusively used to print a debug
					// log message periodically. Using this policy may lead to a
					// DefaultClass log line showing a high rate of server recv but
					// a low rate of client sends if most of the traffic is due to
					// system ranges.
					//
					// TODO(ajwerner): consider providing transport metadata to inform
					// the server of the connection class or keep shared stats for all
					// connection with a host.
					if stats == nil {
						stats = t.getStats(batch.Requests[0].FromReplica.NodeID, rpc.DefaultClass)
					}

					for i := range batch.Requests {
						req := &batch.Requests[i]
						atomic.AddInt64(&stats.serverRecv, 1)
						if pErr := t.handleRaftRequest(ctx, req, stream); pErr != nil {
							atomic.AddInt64(&stats.serverSent, 1)
							if err := stream.Send(newRaftMessageResponse(req, pErr)); err != nil {
								return err
							}
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

// RaftSnapshot handles incoming streaming snapshot requests.
func (t *RaftTransport) RaftSnapshot(stream MultiRaft_RaftSnapshotServer) error {
	errCh := make(chan error, 1)
	if err := t.stopper.RunAsyncTask(
		stream.Context(), "storage.RaftTransport: processing snapshot",
		func(ctx context.Context) {
			errCh <- func() error {
				req, err := stream.Recv()
				if err != nil {
					return err
				}
				if req.Header == nil {
					return stream.Send(&SnapshotResponse{
						Status:  SnapshotResponse_ERROR,
						Message: "client error: no header in first snapshot request message"})
				}
				rmr := req.Header.RaftMessageRequest
				handler, ok := t.getHandler(rmr.ToReplica.StoreID)
				if !ok {
					log.Warningf(ctx, "unable to accept Raft message from %+v: no handler registered for %+v",
						rmr.FromReplica, rmr.ToReplica)
					return roachpb.NewStoreNotFoundError(rmr.ToReplica.StoreID)
				}
				return handler.HandleSnapshot(req.Header, stream)
			}()
		}); err != nil {
		return err
	}
	select {
	case <-t.stopper.ShouldQuiesce():
		return nil
	case err := <-errCh:
		return err
	}
}

// Listen registers a raftMessageHandler to receive proxied messages.
func (t *RaftTransport) Listen(storeID roachpb.StoreID, handler RaftMessageHandler) {
	t.handlers.Store(int64(storeID), unsafe.Pointer(&handler))
}

// Stop unregisters a raftMessageHandler.
func (t *RaftTransport) Stop(storeID roachpb.StoreID) {
	t.handlers.Delete(int64(storeID))
}

// processQueue opens a Raft client stream and sends messages from the
// designated queue (ch) via that stream, exiting when an error is received or
// when it idles out. All messages remaining in the queue at that point are
// lost and a new instance of processQueue will be started by the next message
// to be sent.
func (t *RaftTransport) processQueue(
	nodeID roachpb.NodeID,
	ch chan *RaftMessageRequest,
	stats *raftTransportStats,
	stream MultiRaft_RaftMessageBatchClient,
	class rpc.ConnectionClass,
) error {
	errCh := make(chan error, 1)

	ctx := stream.Context()

	if err := t.stopper.RunAsyncTask(
		ctx, "storage.RaftTransport: processing queue",
		func(ctx context.Context) {
			errCh <- func() error {
				for {
					resp, err := stream.Recv()
					if err != nil {
						return err
					}
					atomic.AddInt64(&stats.clientRecv, 1)
					handler, ok := t.getHandler(resp.ToReplica.StoreID)
					if !ok {
						log.Warningf(ctx, "no handler found for store %s in response %s",
							resp.ToReplica.StoreID, resp)
						continue
					}
					if err := handler.HandleRaftResponse(ctx, resp); err != nil {
						return err
					}
				}
			}()
		}); err != nil {
		return err
	}

	var raftIdleTimer timeutil.Timer
	defer raftIdleTimer.Stop()
	batch := &RaftMessageRequestBatch{}
	for {
		raftIdleTimer.Reset(raftIdleTimeout)
		select {
		case <-t.stopper.ShouldQuiesce():
			return nil
		case <-raftIdleTimer.C:
			raftIdleTimer.Read = true
			return nil
		case err := <-errCh:
			return err
		case req := <-ch:
			batch.Requests = append(batch.Requests, *req)
			req.release()
			// Pull off as many queued requests as possible.
			//
			// TODO(peter): Think about limiting the size of the batch we send.
			for done := false; !done; {
				select {
				case req = <-ch:
					batch.Requests = append(batch.Requests, *req)
					req.release()
				default:
					done = true
				}
			}

			err := stream.Send(batch)
			batch.Requests = batch.Requests[:0]

			atomic.AddInt64(&stats.clientSent, 1)
			if err != nil {
				return err
			}
		}
	}
}

// getQueue returns the queue for the specified node ID and a boolean
// indicating whether the queue already exists (true) or was created (false).
func (t *RaftTransport) getQueue(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (chan *RaftMessageRequest, bool) {
	queuesMap := &t.queues[class]
	value, ok := queuesMap.Load(int64(nodeID))
	if !ok {
		ch := make(chan *RaftMessageRequest, raftSendBufferSize)
		value, ok = queuesMap.LoadOrStore(int64(nodeID), unsafe.Pointer(&ch))
	}
	return *(*chan *RaftMessageRequest)(value), ok
}

// SendAsync sends a message to the recipient specified in the request. It
// returns false if the outgoing queue is full. The returned bool may be a false
// positive but will never be a false negative; if sent is true the message may
// or may not actually be sent but if it's false the message definitely was not
// sent. It is not safe to continue using the reference to the provided request.
func (t *RaftTransport) SendAsync(req *RaftMessageRequest, class rpc.ConnectionClass) (sent bool) {
	toNodeID := req.ToReplica.NodeID
	stats := t.getStats(toNodeID, class)
	defer func() {
		if !sent {
			atomic.AddInt64(&stats.clientDropped, 1)
		}
	}()

	if req.RangeID == 0 && len(req.Heartbeats) == 0 && len(req.HeartbeatResps) == 0 {
		// Coalesced heartbeats are addressed to range 0; everything else
		// needs an explicit range ID.
		panic("only messages with coalesced heartbeats or heartbeat responses may be sent to range ID 0")
	}
	if req.Message.Type == raftpb.MsgSnap {
		panic("snapshots must be sent using SendSnapshot")
	}

	if !t.dialer.GetCircuitBreaker(toNodeID, class).Ready() {
		return false
	}

	ch, existingQueue := t.getQueue(toNodeID, class)
	if !existingQueue {
		// Note that startProcessNewQueue is in charge of deleting the queue.
		ctx := t.AnnotateCtx(context.Background())
		if !t.startProcessNewQueue(ctx, toNodeID, class, stats) {
			return false
		}
	}

	select {
	case ch <- req:
		l := int32(len(ch))
		if v := atomic.LoadInt32(&stats.queueMax); v < l {
			atomic.CompareAndSwapInt32(&stats.queueMax, v, l)
		}
		return true
	default:
		req.release()
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
func (t *RaftTransport) startProcessNewQueue(
	ctx context.Context,
	toNodeID roachpb.NodeID,
	class rpc.ConnectionClass,
	stats *raftTransportStats,
) (started bool) {
	cleanup := func(ch chan *RaftMessageRequest) {
		// Account for the remainder of `ch` which was never sent.
		// NB: we deleted the queue above, so within a short amount
		// of time nobody should be writing into the channel any
		// more. We might miss a message or two here, but that's
		// OK (there's nobody who can safely close the channel the
		// way the code is written).
		for {
			select {
			case <-ch:
				atomic.AddInt64(&stats.clientDropped, 1)
			default:
				return
			}
		}
	}
	worker := func(ctx context.Context) {
		ch, existingQueue := t.getQueue(toNodeID, class)
		if !existingQueue {
			log.Fatalf(ctx, "queue for n%d does not exist", toNodeID)
		}
		defer cleanup(ch)
		defer t.queues[class].Delete(int64(toNodeID))
		conn, err := t.dialer.Dial(ctx, toNodeID, class)
		if err != nil {
			// DialNode already logs sufficiently, so just return.
			return
		}
		client := NewMultiRaftClient(conn)
		batchCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		stream, err := client.RaftMessageBatch(batchCtx) // closed via cancellation
		if err != nil {
			log.Warningf(ctx, "creating batch client for node %d failed: %+v", toNodeID, err)
			return
		}

		if err := t.processQueue(toNodeID, ch, stats, stream, class); err != nil {
			log.Warningf(ctx, "while processing outgoing Raft queue to node %d: %s:", toNodeID, err)
		}
	}
	err := t.stopper.RunAsyncTask(ctx, "storage.RaftTransport: sending messages", worker)
	if err != nil {
		t.queues[class].Delete(int64(toNodeID))
		return false
	}
	return true
}

// SendSnapshot streams the given outgoing snapshot. The caller is responsible
// for closing the OutgoingSnapshot.
func (t *RaftTransport) SendSnapshot(
	ctx context.Context,
	storePool *StorePool,
	header SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newBatch func() storage.Batch,
	sent func(),
) error {
	nodeID := header.RaftMessageRequest.ToReplica.NodeID

	conn, err := t.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
	if err != nil {
		return err
	}
	client := NewMultiRaftClient(conn)
	stream, err := client.RaftSnapshot(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err := stream.CloseSend(); err != nil {
			log.Warningf(ctx, "failed to close snapshot stream: %+v", err)
		}
	}()
	return sendSnapshot(
		ctx, t.st, stream, storePool, header, snap, newBatch, sent,
	)
}
