// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"math"
	"net"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowdispatch"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/node_rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
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
	raftSendBufferSize = 10000

	// When no message has been queued for this duration, the corresponding
	// instance of processQueue will shut down.
	//
	// TODO(tamird): make culling of outbound streams more evented, so that we
	// need not rely on this timeout to shut things down.
	raftIdleTimeout = time.Minute
)

// targetRaftOutgoingBatchSize wraps "kv.raft.command.target_batch_size".
var targetRaftOutgoingBatchSize = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.raft.command.target_batch_size",
	"size of a batch of raft commands after which it will be sent without further batching",
	64<<20, // 64 MB
	settings.PositiveInt,
)

// RaftMessageResponseStream is the subset of the
// MultiRaft_RaftMessageServer interface that is needed for sending responses.
type RaftMessageResponseStream interface {
	Send(*kvserverpb.RaftMessageResponse) error
}

// lockedRaftMessageResponseStream is an implementation of
// RaftMessageResponseStream which provides support for concurrent calls to
// Send. Note that the default implementation of grpc.Stream for server
// responses (grpc.serverStream) is not safe for concurrent calls to Send.
type lockedRaftMessageResponseStream struct {
	wrapped MultiRaft_RaftMessageBatchServer
	sendMu  syncutil.Mutex
}

func (s *lockedRaftMessageResponseStream) Send(resp *kvserverpb.RaftMessageResponse) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.wrapped.Send(resp)
}

func (s *lockedRaftMessageResponseStream) Recv() (*kvserverpb.RaftMessageRequestBatch, error) {
	// No need for lock. gRPC.Stream.RecvMsg is safe for concurrent use.
	return s.wrapped.Recv()
}

// SnapshotResponseStream is the subset of the
// MultiRaft_RaftSnapshotServer interface that is needed for sending responses.
type SnapshotResponseStream interface {
	Send(*kvserverpb.SnapshotResponse) error
	Recv() (*kvserverpb.SnapshotRequest, error)
}

// IncomingRaftMessageHandler is the interface that must be implemented by
// arguments to RaftTransport.ListenIncomingRaftMessages.
type IncomingRaftMessageHandler interface {
	// HandleRaftRequest is called for each incoming Raft message. The request is
	// always processed asynchronously and the response is sent over respStream.
	// If an error is encountered during asynchronous processing, it will be
	// streamed back to the sender of the message as a RaftMessageResponse.
	HandleRaftRequest(ctx context.Context, req *kvserverpb.RaftMessageRequest,
		respStream RaftMessageResponseStream) *kvpb.Error

	// HandleRaftResponse is called for each raft response. Note that
	// not all messages receive a response. An error is returned if and only if
	// the underlying Raft connection should be closed.
	HandleRaftResponse(context.Context, *kvserverpb.RaftMessageResponse) error

	// HandleSnapshot is called for each new incoming snapshot stream, after
	// parsing the initial SnapshotRequest_Header on the stream.
	HandleSnapshot(
		ctx context.Context,
		header *kvserverpb.SnapshotRequest_Header,
		respStream SnapshotResponseStream,
	) error

	// HandleDelegatedSnapshot is called for each incoming delegated snapshot
	// request.
	HandleDelegatedSnapshot(
		ctx context.Context,
		req *kvserverpb.DelegateSendSnapshotRequest,
	) *kvserverpb.DelegateSnapshotResponse
}

// OutgoingRaftMessageHandler is the interface that must be implemented by
// arguments to RaftTransport.ListenOutgoingMessage.
type OutgoingRaftMessageHandler interface {
	// HandleRaftRequestSent is called synchronously for every Raft messages right
	// before it is sent to raftSendQueue in RaftTransport.SendAsync(). Note that
	// the message may not be successfully queued if it gets dropped by SendAsync
	// due to a full outgoing queue.
	//
	// As of now, the only use case of this function is for metrics update on
	// messages sent which is why it only takes specific properties of the request
	// as arguments. But it can be easily extended to take the complete request if
	// needed.
	HandleRaftRequestSent(ctx context.Context,
		fromNodeID roachpb.NodeID, toNodeID roachpb.NodeID, msgSize int64)
}

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
	st      *cluster.Settings
	stopper *stop.Stopper
	clock   *hlc.Clock
	metrics *RaftTransportMetrics

	// Queues maintains a map[roachpb.NodeID]*raftSendQueue on a per rpc-class
	// level. When new raftSendQueues are instantiated, or old ones deleted, we
	// also maintain kvflowControl.mu.connectedTracker. So writing to this field
	// is done while holding kvflowControl.mu.
	//
	// TODO(pav-kv): only SystemClass and "default" raft class slots are used.
	// Find an efficient way to have only the necessary number of slots.
	queues [rpc.NumConnectionClasses]syncutil.Map[roachpb.NodeID, raftSendQueue]

	dialer                  *nodedialer.Dialer
	incomingMessageHandlers syncutil.Map[roachpb.StoreID, IncomingRaftMessageHandler]
	outgoingMessageHandlers syncutil.Map[roachpb.StoreID, OutgoingRaftMessageHandler]

	// kvflowControl is used for replication admission control v1.
	kvflowControl struct {
		// Everything nested under this struct is used to return flow tokens
		// from the receiver (where work was admitted) up to the sender (where
		// work originated and tokens were deducted). There are few parts to the
		// protocol, and can be experimented with using TestFlowControlRaftTransport
		// and various TestFlowControl* tests in this package. We briefly sketch
		// how the various pieces fit below, repeating some of what is described
		// in kvflowcontrol/doc.go (which details about how/why we integrate
		// with the RaftTransport so intimately).
		//
		// Background: Each CRDB node acts as both the "server" and the "client"
		// of bidirectional RaftTransport streams. That is, raft messages from
		// n1->n2 are sent from the client code on n1 to server code on n2.
		// Responses to those raft messages are sent from the client code on n2
		// to server code in n1.
		//
		// - When flow tokens are deducted where the MsgApps originate, and
		//   after they're admitted below-raft, the remote nodes send back
		//   kvflowcontrolpb.AdmittedRaftLogEntries through the RaftMessageBatch
		//   stream. This happens on the client side, and we read from
		//   dispatchReader and either attach the protos to already outbound
		//   messages, or fire off one-off messages periodically if there's
		//   little raft activity but still tokens to be returned.
		//
		// - On the server side, we intercept every message and look for these
		//   protos, and inform the storesForFlowControl integration interface
		//   of this fact.
		//
		// - The server side maintains the set of client-side stores it's
		//   currently connected to, from the POV of these server side streams.
		//   Since tokens are returned to the server along these streams, when
		//   they disconnect, it's possible for us to be leaking tokens in
		//   transit. So it uses information about the set of client-side stores
		//   it's no longer connected to, to simply free up all held tokens. See
		//   uses of connectionTracker below and how the storesForFlowControl
		//   interface is informed about stores we're no longer connected to.
		//
		//   - There's some complexity in how this set of connected stores is
		//     tracked. For one it's a "connected" set, not a "disconnected"
		//     one. It's hard to do the latter. Ignoring memory maintenance
		//     issues that come from nodes that are no longer part of the
		//     cluster, we have multiple RPC classes from the client side and
		//     clients are also free to establish/disconnect many streams
		//     concurrently without synchronization on their end. The server is
		//     not guaranteed to learn about these streams connecting or
		//     disconnecting in any particular order, so it's difficult to track
		//     directly by looking at connection/disconnection events alone
		//     whether we're connected somehow to a given client. Some form of
		//     heartbeat scheme comes to mind, which fits more naturally with
		//     tracking the set of "connected" stores. We can still react to
		//     streams disconnecting by clearing the relevant stores from the
		//     tracked set, relying on a subsequent heartbeat (from, say, a
		//     different stream/RPC class) to re-track that client+its store as
		//     connected.
		//
		// - How does the server learn about what stores are present on each
		//   client? When establishing the stream, the client simply sends this
		//   information over. If the client learns of newly added stores
		//   (possible during early startup), it sends it again. See uses of
		//   localStoreIDs and setAdditionalStoreIDs below.
		//
		// - Idle streams are periodically culled from the client side. It's
		//   possible that we cull a stream without having delivered all flow
		//   tokens to the sender (for example, if below-raft admission happens
		//   after the stream is culled). The server side detected these
		//   disconnected streams and releases tokens, but on the client side we
		//   don't want to accumulate to-be-delivered flow tokens unboundedly.
		//   So we track the set of servers we're connected to, across all RPC
		//   classes, and periodically just clear our outbox if there are
		//   dispatches bound for nodes we're simply not connected to, across
		//   all RPC classes. See uses of connectedNodes below.
		mu struct {
			syncutil.RWMutex
			localStoreIDs     []roachpb.StoreID // sent to servers to track client-side stores
			connectionTracker *connectionTrackerForFlowControl
		}
		setAdditionalStoreIDs atomic.Bool
		dispatchReader        kvflowcontrol.DispatchReader
		handles               kvflowcontrol.Handles
		disconnectListener    RaftTransportDisconnectListener
	}
	// kvflowcontrol2 is used for replication admission control v2.
	kvflowcontrol2 struct {
		piggybackReader              node_rac2.PiggybackMsgReader
		piggybackedResponseScheduler PiggybackedAdmittedResponseScheduler
	}

	knobs *RaftTransportTestingKnobs
}

// raftSendQueue is a queue of outgoing RaftMessageRequest messages.
type raftSendQueue struct {
	reqs chan *kvserverpb.RaftMessageRequest
	// The specific node this queue is sending RaftMessageRequests to.
	nodeID roachpb.NodeID
	// The number of bytes in flight. Must be updated *atomically* on sending and
	// receiving from the reqs channel.
	bytes atomic.Int64
}

// NewDummyRaftTransport returns a dummy raft transport for use in tests which
// need a non-nil raft transport that need not function.
func NewDummyRaftTransport(
	ambient log.AmbientContext, st *cluster.Settings, clock *hlc.Clock,
) *RaftTransport {
	resolver := func(roachpb.NodeID) (net.Addr, roachpb.Locality, error) {
		return nil, roachpb.Locality{}, errors.New("dummy resolver")
	}
	return NewRaftTransport(ambient, st, nil, clock, nodedialer.New(nil, resolver), nil,
		kvflowdispatch.NewDummyDispatch(), NoopStoresFlowControlIntegration{},
		NoopRaftTransportDisconnectListener{}, nil, nil, nil,
	)
}

// NewRaftTransport creates a new RaftTransport.
func NewRaftTransport(
	ambient log.AmbientContext,
	st *cluster.Settings,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	dialer *nodedialer.Dialer,
	grpcServer *grpc.Server,
	kvflowTokenDispatch kvflowcontrol.DispatchReader,
	kvflowHandles kvflowcontrol.Handles,
	disconnectListener RaftTransportDisconnectListener,
	piggybackReader node_rac2.PiggybackMsgReader,
	piggybackedResponseScheduler PiggybackedAdmittedResponseScheduler,
	knobs *RaftTransportTestingKnobs,
) *RaftTransport {
	if knobs == nil {
		knobs = &RaftTransportTestingKnobs{}
	}
	t := &RaftTransport{
		AmbientContext: ambient,
		st:             st,
		stopper:        stopper,
		clock:          clock,
		dialer:         dialer,
		knobs:          knobs,
	}
	t.kvflowControl.dispatchReader = kvflowTokenDispatch
	t.kvflowControl.handles = kvflowHandles
	t.kvflowControl.disconnectListener = disconnectListener
	t.kvflowControl.mu.connectionTracker = newConnectionTrackerForFlowControl()
	t.kvflowcontrol2.piggybackReader = piggybackReader
	t.kvflowcontrol2.piggybackedResponseScheduler = piggybackedResponseScheduler

	t.initMetrics()
	if grpcServer != nil {
		RegisterMultiRaftServer(grpcServer, t)
	}
	return t
}

// Start various internal goroutines needed for the RaftTransport's internal
// functioning.
func (t *RaftTransport) Start(ctx context.Context) error {
	if err := t.startDroppingFlowTokensForDisconnectedNodes(ctx); err != nil {
		return errors.Wrapf(err, "failed to run flow token dispatch loop")
	}
	return nil
}

// SetInitialStoreIDs informs the RaftTransport of the initial set of store
// IDs the local node starts off with. If it's a restarting node, restarted with
// no additional stores, this is just all the local store IDs. For nodes newly
// added to the cluster, it's just the first initialized store. If there are
// additional stores post-restart, or stores other than the first for a newly
// added node, they're provided using (*RaftTransport).SetAdditionalStoreIDs.
func (t *RaftTransport) SetInitialStoreIDs(storeIDs []roachpb.StoreID) {
	t.kvflowControl.mu.Lock()
	defer t.kvflowControl.mu.Unlock()
	t.kvflowControl.mu.localStoreIDs = storeIDs
}

// SetAdditionalStoreIDs informs the RaftTransport of any additional stores the
// local node starts off with. See commentary on SetInitialStoreIDs for more
// details.
func (t *RaftTransport) SetAdditionalStoreIDs(storeIDs []roachpb.StoreID) {
	if len(storeIDs) == 0 {
		return // nothing to do
	}
	t.kvflowControl.mu.Lock()
	defer t.kvflowControl.mu.Unlock()
	t.kvflowControl.mu.localStoreIDs = append(t.kvflowControl.mu.localStoreIDs, storeIDs...)
	t.kvflowControl.setAdditionalStoreIDs.Store(true)
}

// Metrics returns metrics tracking this transport.
func (t *RaftTransport) Metrics() *RaftTransportMetrics {
	return t.metrics
}

// visitQueues calls the visit callback on each outgoing messages sub-queue.
func (t *RaftTransport) visitQueues(visit func(*raftSendQueue)) {
	for class := range t.queues {
		t.queues[class].Range(func(_ roachpb.NodeID, v *raftSendQueue) bool {
			visit(v)
			return true
		})
	}
}

// queueMessageCount returns the total number of outgoing messages in the queue.
func (t *RaftTransport) queueMessageCount() int64 {
	var count int64
	t.visitQueues(func(q *raftSendQueue) { count += int64(len(q.reqs)) })
	return count
}

// queueByteSize returns the total bytes size of outgoing messages in the queue.
func (t *RaftTransport) queueByteSize() int64 {
	var size int64
	t.visitQueues(func(q *raftSendQueue) { size += q.bytes.Load() })
	return size
}

// getIncomingRaftMessageHandler returns the registered
// IncomingRaftMessageHandler for the given StoreID. If no handlers are
// registered for the StoreID, it returns (nil, false).
func (t *RaftTransport) getIncomingRaftMessageHandler(
	storeID roachpb.StoreID,
) (IncomingRaftMessageHandler, bool) {
	if value, ok := t.incomingMessageHandlers.Load(storeID); ok {
		return *value, true
	}
	return nil, false
}

// getOutgoingMessageHandler returns the registered OutgoingRaftMessageHandler
// for the given StoreID. If no handlers are registered for the StoreID, it
// returns (nil, false).
func (t *RaftTransport) getOutgoingMessageHandler(
	storeID roachpb.StoreID,
) (OutgoingRaftMessageHandler, bool) {
	if value, ok := t.outgoingMessageHandlers.Load(storeID); ok {
		return *value, true
	}
	return nil, false
}

// handleRaftRequest proxies a request to the listening server interface.
func (t *RaftTransport) handleRaftRequest(
	ctx context.Context, req *kvserverpb.RaftMessageRequest, respStream RaftMessageResponseStream,
) *kvpb.Error {
	for i := range req.AdmittedRaftLogEntries {
		// Process any flow tokens that were returned over the RaftTransport. Do
		// this first thing, before these requests enter the receive queues
		// which could drop them if full and cause token leaks, or bail after
		// processing the raft heartbeats. See I8 from kvflowcontrol/doc.go.
		admittedEntries := req.AdmittedRaftLogEntries[i]
		handle, found := t.kvflowControl.handles.Lookup(admittedEntries.RangeID)
		if found {
			handle.ReturnTokensUpto(
				ctx,
				admissionpb.WorkPriority(admittedEntries.AdmissionPriority),
				admittedEntries.UpToRaftLogPosition,
				kvflowcontrol.Stream{StoreID: admittedEntries.StoreID},
			)
		}

		if log.V(1) {
			log.Infof(ctx, "informed of below-raft %s", admittedEntries)
		}
	}
	if req.ToReplica.StoreID == roachpb.StoreID(0) && len(req.AdmittedRaftLogEntries) > 0 {
		// The fallback token dispatch mechanism does not specify a destination
		// replica, and as such, there's no handler for it. We don't want to
		// return StoreNotFoundErrors in such cases.
		return nil
	}

	incomingMessageHandler, ok := t.getIncomingRaftMessageHandler(req.ToReplica.StoreID)
	if !ok {
		log.Warningf(ctx, "unable to accept Raft message from %+v: no handler registered for %+v",
			req.FromReplica, req.ToReplica)
		return kvpb.NewError(kvpb.NewStoreNotFoundError(req.ToReplica.StoreID))
	}

	return incomingMessageHandler.HandleRaftRequest(ctx, req, respStream)
}

// newRaftMessageResponse constructs a RaftMessageResponse from the
// given request and error.
func newRaftMessageResponse(
	req *kvserverpb.RaftMessageRequest, pErr *kvpb.Error,
) *kvserverpb.RaftMessageResponse {
	resp := &kvserverpb.RaftMessageResponse{
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

// RaftMessageBatch proxies the incoming requests to the listening server interface.
func (t *RaftTransport) RaftMessageBatch(stream MultiRaft_RaftMessageBatchServer) (lastErr error) {
	errCh := make(chan error, 1)

	// Node stopping error is caught below in the select.
	taskCtx, cancel := t.stopper.WithCancelOnQuiesce(stream.Context())
	taskCtx = t.AnnotateCtx(taskCtx)
	defer cancel()

	if err := t.stopper.RunAsyncTaskEx(
		taskCtx,
		stop.TaskOpts{
			TaskName: "storage.RaftTransport: processing batch",
			SpanOpt:  stop.ChildSpan,
		}, func(ctx context.Context) {
			errCh <- func() error {
				var storeIDs []roachpb.StoreID
				defer func() {
					ctx := t.AnnotateCtx(context.Background())
					t.kvflowControl.mu.Lock()
					t.kvflowControl.mu.connectionTracker.markStoresDisconnected(storeIDs)
					t.kvflowControl.mu.Unlock()
					t.kvflowControl.disconnectListener.OnRaftTransportDisconnected(ctx, storeIDs...)
					if fn := t.knobs.OnServerStreamDisconnected; fn != nil {
						fn()
					}
				}()

				stream := &lockedRaftMessageResponseStream{wrapped: stream}
				for {
					batch, err := stream.Recv()
					if err != nil {
						return err
					}
					if !batch.Now.IsEmpty() {
						t.clock.Update(batch.Now)
					}
					if len(batch.StoreIDs) > 0 {
						// Collect the set of store IDs from the client side to
						// later free up relevant flow tokens once the gRPC
						// stream breaks/disconnects.
						storeIDs = batch.StoreIDs
					}
					t.kvflowControl.mu.Lock()
					t.kvflowControl.mu.connectionTracker.markStoresConnected(storeIDs)
					t.kvflowControl.mu.Unlock()
					if len(batch.AdmittedStates) != 0 {
						// Dispatch the admitted vectors to RACv2.
						// NB: we do this via this special path instead of using the
						// handleRaftRequest path since we don't have a full-fledged
						// RaftMessageRequest for each range (each of these responses could
						// be for a different range), and because what we need to do w.r.t.
						// queueing is much simpler (we don't need to worry about queue size
						// since we only keep the highest admitted marks from each replica).
						t.kvflowcontrol2.piggybackedResponseScheduler.
							ScheduleAdmittedResponseForRangeRACv2(ctx, batch.AdmittedStates)
					}
					if len(batch.Requests) == 0 {
						continue
					}
					for i := range batch.Requests {
						req := &batch.Requests[i]
						t.metrics.MessagesRcvd.Inc(1)
						if pErr := t.handleRaftRequest(ctx, req, stream); pErr != nil {
							if err := stream.Send(newRaftMessageResponse(req, pErr)); err != nil {
								return err
							}
							t.metrics.ReverseSent.Inc(1)
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

// DelegateRaftSnapshot handles incoming delegated snapshot requests and passes
// the request to pass off to the sender store. Errors during the snapshots
// process are sent back as a response.
func (t *RaftTransport) DelegateRaftSnapshot(stream MultiRaft_DelegateRaftSnapshotServer) error {
	ctx, cancel := t.stopper.WithCancelOnQuiesce(stream.Context())
	defer cancel()
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	resp := t.InternalDelegateRaftSnapshot(ctx, req.GetSend())
	err = stream.Send(resp)
	if err != nil {
		return err
	}
	return nil
}

// InternalDelegateRaftSnapshot processes requests in a request/response fashion for normal DelegateSnapshotRequests
func (t *RaftTransport) InternalDelegateRaftSnapshot(
	ctx context.Context, req *kvserverpb.DelegateSendSnapshotRequest,
) *kvserverpb.DelegateSnapshotResponse {
	if req == nil {
		err := errors.New("client error: no message in first delegated snapshot request")
		return &kvserverpb.DelegateSnapshotResponse{
			Status:       kvserverpb.DelegateSnapshotResponse_ERROR,
			EncodedError: errors.EncodeError(context.Background(), err),
		}
	}
	// Get the handler of the sender store.
	incomingMessageHandler, ok := t.getIncomingRaftMessageHandler(req.DelegatedSender.StoreID)
	if !ok {
		log.Warningf(
			ctx,
			"unable to accept Raft message: %+v: no handler registered for"+
				" the sender store"+" %+v",
			req.CoordinatorReplica.StoreID,
			req.DelegatedSender.StoreID,
		)
		err := errors.New("unable to accept Raft message: no handler registered for the sender store")
		return &kvserverpb.DelegateSnapshotResponse{
			Status:       kvserverpb.DelegateSnapshotResponse_ERROR,
			EncodedError: errors.EncodeError(context.Background(), err),
		}
	}

	// Pass off the snapshot request to the sender store.
	return incomingMessageHandler.HandleDelegatedSnapshot(ctx, req)
}

// RaftSnapshot handles incoming streaming snapshot requests.
func (t *RaftTransport) RaftSnapshot(stream MultiRaft_RaftSnapshotServer) error {
	ctx, cancel := t.stopper.WithCancelOnQuiesce(stream.Context())
	defer cancel()
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if req.Header == nil {
		err := errors.New("client error: no header in first snapshot request message")
		return stream.Send(snapRespErr(err))
	}
	rmr := req.Header.RaftMessageRequest
	incomingMessageHandler, ok := t.getIncomingRaftMessageHandler(rmr.ToReplica.StoreID)
	if !ok {
		log.Warningf(ctx, "unable to accept Raft message from %+v: no handler registered for %+v",
			rmr.FromReplica, rmr.ToReplica)
		return kvpb.NewStoreNotFoundError(rmr.ToReplica.StoreID)
	}
	return incomingMessageHandler.HandleSnapshot(ctx, req.Header, stream)
}

// ListenIncomingRaftMessages registers a IncomingRaftMessageHandler to receive proxied messages.
func (t *RaftTransport) ListenIncomingRaftMessages(
	storeID roachpb.StoreID, handler IncomingRaftMessageHandler,
) {
	t.incomingMessageHandlers.Store(storeID, &handler)
}

// StopIncomingRaftMessages unregisters a IncomingRaftMessageHandler.
func (t *RaftTransport) StopIncomingRaftMessages(storeID roachpb.StoreID) {
	t.incomingMessageHandlers.Delete(storeID)
}

// ListenOutgoingMessage registers an OutgoingRaftMessageHandler to capture
// messages right before they are sent through the raftSendQueue.
func (t *RaftTransport) ListenOutgoingMessage(
	storeID roachpb.StoreID, handler OutgoingRaftMessageHandler,
) {
	t.outgoingMessageHandlers.Store(storeID, &handler)
}

// StopOutgoingMessage unregisters an OutgoingRaftMessageHandler.
func (t *RaftTransport) StopOutgoingMessage(storeID roachpb.StoreID) {
	t.outgoingMessageHandlers.Delete(storeID)
}

// processQueue opens a Raft client stream and sends messages from the
// designated queue (ch) via that stream, exiting when an error is received or
// when it idles out. All messages remaining in the queue at that point are
// lost and a new instance of processQueue will be started by the next message
// to be sent.
func (t *RaftTransport) processQueue(
	q *raftSendQueue, stream MultiRaft_RaftMessageBatchClient, class rpc.ConnectionClass,
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
					t.metrics.ReverseRcvd.Inc(1)
					incomingMessageHandler, ok := t.getIncomingRaftMessageHandler(resp.ToReplica.StoreID)
					if !ok {
						log.Warningf(ctx, "no handler found for store %s in response %s",
							resp.ToReplica.StoreID, resp)
						continue
					}
					if err := incomingMessageHandler.HandleRaftResponse(ctx, resp); err != nil {
						return err
					}
				}
			}()
		}); err != nil {
		return err
	}

	// For replication admission control v1.
	maybeAnnotateWithAdmittedRaftLogEntries := func(
		req *kvserverpb.RaftMessageRequest,
		admitted []kvflowcontrolpb.AdmittedRaftLogEntries,
	) {
		if len(admitted) == 0 {
			return // nothing to do
		}
		req.AdmittedRaftLogEntries = append(req.AdmittedRaftLogEntries, admitted...)
		flowTokenDispatchCount := len(req.AdmittedRaftLogEntries)
		if log.V(2) && flowTokenDispatchCount > 0 {
			for i, admittedEntries := range req.AdmittedRaftLogEntries {
				log.Infof(ctx, "informing n%s of below-raft %s: %d out of %d dispatches",
					q.nodeID, admittedEntries,
					i+1, flowTokenDispatchCount,
				)
			}
		}
	}

	// For replication admission control v1.
	var sentInitialStoreIDs, sentAdditionalStoreIDs bool
	maybeAnnotateWithStoreIDs := func(batch *kvserverpb.RaftMessageRequestBatch) {
		shouldSendAdditionalStoreIDs := t.kvflowControl.setAdditionalStoreIDs.Load() && !sentAdditionalStoreIDs
		if !sentInitialStoreIDs || shouldSendAdditionalStoreIDs {
			t.kvflowControl.mu.RLock()
			batch.StoreIDs = nil
			batch.StoreIDs = append(batch.StoreIDs, t.kvflowControl.mu.localStoreIDs...)
			t.kvflowControl.mu.RUnlock()
			// Unconditionally set sentInitialStoreIDs, since we always have
			// the initial store IDs before the additional ones.
			sentInitialStoreIDs = true
			// Mark that we've sent the additional store IDs, to not need to
			// re-send it again.
			sentAdditionalStoreIDs = shouldSendAdditionalStoreIDs
			log.VInfof(ctx, 1, "informing n%d of %d local store ID(s) (%s) over the raft transport[%s]",
				q.nodeID, len(batch.StoreIDs), roachpb.StoreIDSlice(batch.StoreIDs), class)
		}
	}

	// For replication admission control v2.
	maybeAnnotateWithAdmittedStates := func(
		batch *kvserverpb.RaftMessageRequestBatch, admitted []kvflowcontrolpb.PiggybackedAdmittedState,
	) {
		batch.AdmittedStates = append(batch.AdmittedStates, admitted...)
	}

	annotateWithClockTimestamp := func(batch *kvserverpb.RaftMessageRequestBatch) {
		batch.Now = t.clock.NowAsClockTimestamp()
	}

	clearRequestBatch := func(batch *kvserverpb.RaftMessageRequestBatch) {
		// Reuse the Requests slice, but zero out the contents to avoid delaying
		// GC of memory referenced from within.
		for i := range batch.Requests {
			batch.Requests[i] = kvserverpb.RaftMessageRequest{}
		}
		batch.Requests = batch.Requests[:0]
		batch.StoreIDs = nil
		batch.Now = hlc.ClockTimestamp{}
		for i := range batch.AdmittedStates {
			batch.AdmittedStates[i] = kvflowcontrolpb.PiggybackedAdmittedState{}
		}
		batch.AdmittedStates = batch.AdmittedStates[:0]
	}

	var raftIdleTimer timeutil.Timer
	defer raftIdleTimer.Stop()
	idleTimeout := raftIdleTimeout
	if overrideFn := t.knobs.OverrideIdleTimeout; overrideFn != nil {
		idleTimeout = overrideFn()
	}

	var dispatchPendingFlowTokensTimer timeutil.Timer
	defer dispatchPendingFlowTokensTimer.Stop()
	dispatchPendingFlowTokensTimer.Reset(kvadmission.FlowTokenDispatchInterval.Get(&t.st.SV))

	dispatchPendingFlowTokensCh := dispatchPendingFlowTokensTimer.C
	if t.knobs.TriggerFallbackDispatchCh != nil {
		dispatchPendingFlowTokensCh = t.knobs.TriggerFallbackDispatchCh
	}

	batch := &kvserverpb.RaftMessageRequestBatch{}
	for {
		raftIdleTimer.Reset(idleTimeout)

		select {
		case <-t.stopper.ShouldQuiesce():
			return nil

		case <-raftIdleTimer.C:
			raftIdleTimer.Read = true
			return nil

		case err := <-errCh:
			return err

		case req := <-q.reqs:
			size := int64(req.Size())
			q.bytes.Add(-size)
			budget := targetRaftOutgoingBatchSize.Get(&t.st.SV) - size

			var pendingDispatches []kvflowcontrolpb.AdmittedRaftLogEntries
			var admittedStates []kvflowcontrolpb.PiggybackedAdmittedState
			if disableFn := t.knobs.DisablePiggyBackedFlowTokenDispatch; disableFn == nil || !disableFn() {
				// RACv1.
				//
				// Piggyback any pending flow token dispatches on raft transport
				// messages already bound for the remote node. If the stream
				// over which we're returning these flow tokens breaks, this is
				// detected by the remote node, where tokens were originally
				// deducted, who then frees up all held tokens (see I1 from
				// kvflowcontrol/doc.go). If the stream is culled because it's
				// idle, that's deducted remotely using the same stream-break
				// mechanism. If there are no open streams to a given node and
				// there's still pending flow tokens, we'll drop those tokens to
				// reclaim memory in dropFlowTokensForDisconnectedNodes. For
				// idle-but-not-culled connections, we have a fallback timer to
				// periodically transmit one-off RaftMessageRequests for timely
				// token returns.
				pendingDispatches, _ = t.kvflowControl.dispatchReader.PendingDispatchFor(
					q.nodeID,
					kvadmission.FlowTokenDispatchMaxBytes.Get(&t.st.SV),
				)
				maybeAnnotateWithAdmittedRaftLogEntries(req, pendingDispatches)

				// RACv2.
				admittedStates, _ = t.kvflowcontrol2.piggybackReader.PopMsgsForNode(
					timeutil.Now(), q.nodeID, kvadmission.FlowTokenDispatchMaxBytes.Get(&t.st.SV))
				maybeAnnotateWithAdmittedStates(batch, admittedStates)
			}

			batch.Requests = append(batch.Requests, *req)
			releaseRaftMessageRequest(req)

			// Pull off as many queued requests as possible, within reason.
			for budget > 0 {
				select {
				case req = <-q.reqs:
					size := int64(req.Size())
					q.bytes.Add(-size)
					budget -= size
					batch.Requests = append(batch.Requests, *req)
					releaseRaftMessageRequest(req)
				default:
					budget = -1
				}
			}

			maybeAnnotateWithStoreIDs(batch)
			annotateWithClockTimestamp(batch)

			if err := stream.Send(batch); err != nil {
				t.metrics.FlowTokenDispatchesDropped.Inc(int64(
					len(pendingDispatches) + len(admittedStates)))
				return err
			}
			t.metrics.MessagesSent.Inc(int64(len(batch.Requests)))
			clearRequestBatch(batch)

		case <-dispatchPendingFlowTokensCh:
			dispatchPendingFlowTokensTimer.Read = true
			dispatchPendingFlowTokensTimer.Reset(kvadmission.FlowTokenDispatchInterval.Get(&t.st.SV))

			if disableFn := t.knobs.DisableFallbackFlowTokenDispatch; disableFn != nil && disableFn() {
				continue // nothing to do
			}

			// RACv1.
			pendingDispatches, remainingDispatches := t.kvflowControl.dispatchReader.PendingDispatchFor(
				q.nodeID,
				kvadmission.FlowTokenDispatchMaxBytes.Get(&t.st.SV),
			)
			// RACv2.
			admittedStates, remainingAdmittedResponses := t.kvflowcontrol2.piggybackReader.PopMsgsForNode(
				timeutil.Now(), q.nodeID, kvadmission.FlowTokenDispatchMaxBytes.Get(&t.st.SV))
			if len(pendingDispatches) == 0 && len(admittedStates) == 0 {
				continue // nothing to do
			}
			// If there are remaining dispatches/responses, schedule them
			// immediately in the following raft message.
			if remainingDispatches > 0 || remainingAdmittedResponses > 0 {
				dispatchPendingFlowTokensTimer.Reset(0)
			}

			if len(pendingDispatches) != 0 {
				req := newRaftMessageRequest()
				maybeAnnotateWithAdmittedRaftLogEntries(req, pendingDispatches)
				batch.Requests = append(batch.Requests, *req)
				releaseRaftMessageRequest(req)
			}

			maybeAnnotateWithStoreIDs(batch)
			annotateWithClockTimestamp(batch)
			maybeAnnotateWithAdmittedStates(batch, admittedStates)

			if err := stream.Send(batch); err != nil {
				t.metrics.FlowTokenDispatchesDropped.Inc(int64(
					len(pendingDispatches) + len(admittedStates)))
				return err
			}
			t.metrics.MessagesSent.Inc(int64(len(batch.Requests)))
			clearRequestBatch(batch)

			if fn := t.knobs.OnFallbackDispatch; fn != nil {
				fn()
			}

		case gotNodeID := <-t.knobs.MarkSendQueueAsIdleCh:
			if q.nodeID == gotNodeID {
				return nil
			}
			select {
			// Echo the node ID back into MarkSendQueueAsIdleCh until it reaches
			// a raftSendQueue for this ID. Only used in tests.
			case t.knobs.MarkSendQueueAsIdleCh <- gotNodeID:
			default:
			}
		}
	}
}

// getQueue returns the queue for the specified node ID and a boolean
// indicating whether the queue already exists (true) or was created (false).
func (t *RaftTransport) getQueue(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (*raftSendQueue, bool) {
	queuesMap := &t.queues[class]
	value, ok := queuesMap.Load(nodeID)
	if !ok {
		t.kvflowControl.mu.Lock()
		q := &raftSendQueue{
			reqs:   make(chan *kvserverpb.RaftMessageRequest, raftSendBufferSize),
			nodeID: nodeID,
		}
		value, ok = queuesMap.LoadOrStore(nodeID, q)
		t.kvflowControl.mu.connectionTracker.markNodeConnected(nodeID, class)
		t.kvflowControl.mu.Unlock()
	}
	return value, ok
}

// SendAsync sends a message to the recipient specified in the request. It
// returns false if the outgoing queue is full. The returned bool may be a false
// positive but will never be a false negative; if sent is true the message may
// or may not actually be sent but if it's false the message definitely was not
// sent. It is not safe to continue using the reference to the provided request.
func (t *RaftTransport) SendAsync(
	req *kvserverpb.RaftMessageRequest, class rpc.ConnectionClass,
) (sent bool) {
	toNodeID := req.ToReplica.NodeID
	defer func() {
		if !sent {
			t.metrics.MessagesDropped.Inc(1)
			releaseRaftMessageRequest(req)
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
	if outgoingMessageHandler, ok := t.getOutgoingMessageHandler(req.FromReplica.StoreID); ok {
		// Capture outgoing Raft messages only when the sender's store has an
		// OutgoingRaftMessageHandler registered.
		outgoingMessageHandler.HandleRaftRequestSent(context.Background(), req.FromReplica.NodeID, req.ToReplica.NodeID, size)
	}
	select {
	case q.reqs <- req:
		q.bytes.Add(size)
		return true
	default:
		if logRaftSendQueueFullEvery.ShouldLog() {
			log.Warningf(t.AnnotateCtx(context.Background()), "raft send queue to n%d is full", toNodeID)
		}
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
	ctx context.Context, toNodeID roachpb.NodeID, class rpc.ConnectionClass,
) (started bool) {
	cleanup := func(q *raftSendQueue) {
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
				t.metrics.MessagesDropped.Inc(1)
				releaseRaftMessageRequest(req)
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
		defer func() {
			if fn := t.knobs.OnWorkerTeardown; fn != nil {
				fn(toNodeID)
			}
		}()
		defer cleanup(q)
		defer func() {
			t.kvflowControl.mu.Lock()
			t.queues[class].Delete(toNodeID)
			t.kvflowControl.mu.connectionTracker.markNodeDisconnected(toNodeID, class)
			t.kvflowControl.mu.Unlock()
		}()
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

		if err := t.processQueue(q, stream, class); err != nil {
			log.Warningf(ctx, "while processing outgoing Raft queue to node %d: %s:", toNodeID, err)
		}
	}
	err := t.stopper.RunAsyncTask(ctx, "storage.RaftTransport: sending/receiving messages",
		func(ctx context.Context) {
			pprof.Do(ctx, pprof.Labels("remote_node_id", toNodeID.String()), worker)
		})
	if err != nil {
		t.kvflowControl.mu.Lock()
		t.queues[class].Delete(toNodeID)
		t.kvflowControl.mu.connectionTracker.markNodeDisconnected(toNodeID, class)
		t.kvflowControl.mu.Unlock()
		return false
	}
	return true
}

// startDroppingFlowTokensForDisconnectedNodes kicks of an asynchronous worker
// that periodically scans for nodes we're no longer connected to, and if there
// are any pending flow tokens bound for that node, it simply drops them. This
// "connected nodes" is a client-side view of the world. On the server-side when
// gRPC streams disconnect, we release all held tokens. So simply dropping these
// pending dispatches on the client side does not cause token leaks, and exists
// to prevent an unbounded accumulation of memory.
func (t *RaftTransport) startDroppingFlowTokensForDisconnectedNodes(ctx context.Context) error {
	return t.stopper.RunAsyncTask(
		ctx,
		"kvserver.RaftTransport: drop flow tokens for disconnected nodes",
		func(ctx context.Context) {
			settingChangeCh := make(chan struct{}, 1)
			kvadmission.FlowTokenDropInterval.SetOnChange(
				&t.st.SV, func(ctx context.Context) {
					select {
					case settingChangeCh <- struct{}{}:
					default:
					}
				})

			var timer timeutil.Timer
			defer timer.Stop()

			for {
				interval := kvadmission.FlowTokenDropInterval.Get(&t.st.SV)
				if interval > 0 {
					timer.Reset(interval)
				} else {
					// Disable the mechanism.
					timer.Stop()
				}
				select {
				case <-timer.C:
					timer.Read = true
					t.dropFlowTokensForDisconnectedNodes()
					continue

				case <-settingChangeCh:
					// Loop around to use the updated timer.
					continue

				case <-ctx.Done():
					return

				case <-t.stopper.ShouldQuiesce():
					return
				}
			}
		},
	)
}

// TestingDropFlowTokensForDisconnectedNodes exports
// dropFlowTokensForDisconnectedNodes for testing purposes.
func (t *RaftTransport) TestingDropFlowTokensForDisconnectedNodes() {
	t.dropFlowTokensForDisconnectedNodes()
}

// TestingPrintFlowControlConnectionTracker renders the state of the underlying
// connection tracker.
func (t *RaftTransport) TestingPrintFlowControlConnectionTracker() string {
	t.kvflowControl.mu.RLock()
	defer t.kvflowControl.mu.RUnlock()
	return t.kvflowControl.mu.connectionTracker.testingPrint()
}

func (t *RaftTransport) dropFlowTokensForDisconnectedNodes() {
	t.kvflowControl.mu.RLock()
	defer t.kvflowControl.mu.RUnlock()
	for _, nodeID := range t.kvflowControl.dispatchReader.PendingDispatch() {
		if t.kvflowControl.mu.connectionTracker.isNodeConnected(nodeID) {
			// If there's already a queue active, there's nothing to do. We rely
			// on timely piggybacking of flow tokens on existing raft transport
			// messages. It's only when all queues are idle/culled that we use
			// this worker to drop any held tokens. The expectation is that on
			// the server-side of the stream, we'll have returned all tokens
			// when streams disconnect.
			continue
		}
		// Drop any held tokens for that node. Pass maxBytes = MaxInt64 to clear the
		// outbox.
		pendingDispatches, _ := t.kvflowControl.dispatchReader.PendingDispatchFor(
			nodeID,
			math.MaxInt64,
		)
		t.metrics.FlowTokenDispatchesDropped.Inc(int64(len(pendingDispatches)))
	}
	now := timeutil.Now()
	for _, nodeID := range t.kvflowcontrol2.piggybackReader.NodesWithMsgs(now) {
		if t.kvflowControl.mu.connectionTracker.isNodeConnected(nodeID) {
			continue
		}
		msgs, remainingMsgs :=
			t.kvflowcontrol2.piggybackReader.PopMsgsForNode(now, nodeID, math.MaxInt64)
		t.metrics.FlowTokenDispatchesDropped.Inc(int64(len(msgs)))
		if remainingMsgs > 0 {
			panic(errors.AssertionFailedf("expected zero remaining msgs, and found %d", remainingMsgs))
		}
	}
}

// SendSnapshot streams the given outgoing snapshot. The caller is responsible
// for closing the OutgoingSnapshot.
//
// The optional (but usually present) returned message is an MsgAppResp that
// results from the follower applying the snapshot, acking the log at the index
// of the snapshot.
func (t *RaftTransport) SendSnapshot(
	ctx context.Context,
	clusterID uuid.UUID,
	storePool *storepool.StorePool,
	header kvserverpb.SnapshotRequest_Header,
	snap *OutgoingSnapshot,
	newWriteBatch func() storage.WriteBatch,
	sent func(),
	recordBytesSent snapshotRecordMetrics,
) (*kvserverpb.SnapshotResponse, error) {
	nodeID := header.RaftMessageRequest.ToReplica.NodeID

	conn, err := t.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
	if err != nil {
		return nil, err
	}
	client := NewMultiRaftClient(conn)
	stream, err := client.RaftSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := stream.CloseSend(); err != nil {
			log.Warningf(ctx, "failed to close snapshot stream: %+v", err)
		}
	}()
	return sendSnapshot(ctx, clusterID, t.st, t.Tracer, stream, storePool, header, snap, newWriteBatch, sent, recordBytesSent)
}

// DelegateSnapshot sends a DelegateSnapshotRequest to a remote store
// and determines if it encountered any errors when sending the snapshot.
func (t *RaftTransport) DelegateSnapshot(
	ctx context.Context, req *kvserverpb.DelegateSendSnapshotRequest,
) (*kvserverpb.DelegateSnapshotResponse, error) {
	nodeID := req.DelegatedSender.NodeID
	conn, err := t.dialer.Dial(ctx, nodeID, rpc.DefaultClass)
	if err != nil {
		return nil, errors.Mark(err, errMarkSnapshotError)
	}
	client := NewMultiRaftClient(conn)

	// Creates a rpc stream between the leaseholder and sender.
	stream, err := client.DelegateRaftSnapshot(ctx)
	if err != nil {
		return nil, errors.Mark(err, errMarkSnapshotError)
	}
	defer func() {
		if err := stream.CloseSend(); err != nil {
			log.Warningf(ctx, "failed to close delegate snapshot stream: %+v", err)
		}
	}()

	// Send the request.
	wrappedRequest := &kvserverpb.DelegateSnapshotRequest{Value: &kvserverpb.DelegateSnapshotRequest_Send{Send: req}}
	if err := stream.Send(wrappedRequest); err != nil {
		return nil, errors.Mark(err, errMarkSnapshotError)
	}
	// Wait for response to see if the receiver successfully applied the snapshot.
	resp, err := stream.Recv()
	if err != nil {
		return nil, errors.Mark(
			errors.Wrapf(err, "%v: remote failed to send snapshot", req), errMarkSnapshotError,
		)
	}

	if len(resp.CollectedSpans) != 0 {
		span := tracing.SpanFromContext(ctx)
		if span == nil {
			log.Warningf(ctx, "trying to ingest remote spans but there is no recording span set up")
		} else {
			span.ImportRemoteRecording(resp.CollectedSpans)
		}
	}

	switch resp.Status {
	case kvserverpb.DelegateSnapshotResponse_ERROR:
		return nil, errors.Mark(
			errors.Wrapf(resp.Error(), "error sending couldn't accept %v", req), errMarkSnapshotError)
	case kvserverpb.DelegateSnapshotResponse_APPLIED:
		// This is the response we're expecting. Snapshot successfully applied.
		log.VEventf(ctx, 3, "%s: delegated snapshot was successfully applied", resp)
		return resp, nil
	default:
		return nil, err
	}
}

// RaftTransportTestingKnobs provide fine-grained control over the RaftTransport
// for tests.
type RaftTransportTestingKnobs struct {
	// MarkSendQueueAsIdleCh is used to selectively mark a raft send queue as
	// idle, identified by the remote node ID. The remote node ID must be
	// connected to by this transport.
	MarkSendQueueAsIdleCh chan roachpb.NodeID
	// OnWorkerTeardown, if set, is invoked when a worker thread for a given
	// send queue is tore down.
	OnWorkerTeardown func(roachpb.NodeID)
	// OnServerStreamDisconnected is invoked whenever the RaftMessageBatch
	// stream is disconnected on the server side.
	OnServerStreamDisconnected func()
	// TriggerFallbackDispatchCh is used to manually trigger the fallback
	// dispatch in tests.
	TriggerFallbackDispatchCh chan time.Time
	// OnFallbackDispatch is invoked whenever the fallback token dispatch
	// mechanism is used.
	OnFallbackDispatch func()
	// OverrideIdleTimeout overrides the raftIdleTimeout, which controls how
	// long until an instance of processQueue winds down after not observing any
	// messages.
	OverrideIdleTimeout func() time.Duration
	// DisableFallbackFlowTokenDispatch disables the fallback mechanism when
	// dispatching flow tokens.
	DisableFallbackFlowTokenDispatch func() bool
	// DisablePiggyBackedFlowTokenDispatch disables the piggybacked mechanism
	// when dispatching flow tokens.
	DisablePiggyBackedFlowTokenDispatch func() bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *RaftTransportTestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*RaftTransportTestingKnobs)(nil)
