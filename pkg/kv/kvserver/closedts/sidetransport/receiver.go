// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sidetransport

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Receiver is the gRPC server for the closed timestamp side-transport,
// receiving updates from remote nodes. It maintains the set of current
// streaming connections.
//
// The state of the Receiver is exposed on each node at /debug/closedts-receiver.
type Receiver struct {
	log.AmbientContext
	stop         *stop.Stopper
	stores       Stores
	testingKnobs receiverTestingKnobs

	mu struct {
		syncutil.RWMutex
		// conns maintains the list of currently-open connections.
		conns map[roachpb.NodeID]*incomingStream
	}

	historyMu struct {
		syncutil.Mutex
		lastClosed map[roachpb.NodeID]streamCloseInfo
	}
}

type streamCloseInfo struct {
	nodeID    roachpb.NodeID
	closeErr  error
	closeTime time.Time
}

// receiverTestingKnobs contains knobs for incomingStreams connected to a
// Receiver. The map is indexed by the sender NodeID.
type receiverTestingKnobs map[roachpb.NodeID]incomingStreamTestingKnobs

var _ ctpb.SideTransportServer = &Receiver{}

// NewReceiver creates a Receiver, to be used as a gRPC server with
// ctpb.RegisterClosedTimestampSideTransportServer.
func NewReceiver(
	nodeID *base.NodeIDContainer,
	stop *stop.Stopper,
	stores Stores,
	testingKnobs receiverTestingKnobs,
) *Receiver {
	r := &Receiver{
		stop:         stop,
		stores:       stores,
		testingKnobs: testingKnobs,
	}
	r.AmbientContext.AddLogTag("n", nodeID)
	r.mu.conns = make(map[roachpb.NodeID]*incomingStream)
	r.historyMu.lastClosed = make(map[roachpb.NodeID]streamCloseInfo)
	return r
}

// PushUpdates is the streaming RPC handler.
func (s *Receiver) PushUpdates(stream ctpb.SideTransport_PushUpdatesServer) error {
	// Create a steam to service this connection. The stream will call back into
	// the Receiver through onFirstMsg to register itself once it finds out the
	// sender's node id.
	ctx := s.AnnotateCtx(stream.Context())
	return newIncomingStream(s, s.stores).Run(ctx, s.stop, stream)
}

// GetClosedTimestamp returns the latest closed timestamp that the receiver
// knows for a particular range, together with the LAI needed to have applied in
// order to use this closed timestamp.
//
// leaseholderNode is the last known leaseholder for the range. For efficiency
// reasons, only the closed timestamp info received from that node is checked
// for closed timestamp info about this range.
func (s *Receiver) GetClosedTimestamp(
	ctx context.Context, rangeID roachpb.RangeID, leaseholderNode roachpb.NodeID,
) (hlc.Timestamp, ctpb.LAI) {
	s.mu.RLock()
	conn, ok := s.mu.conns[leaseholderNode]
	s.mu.RUnlock()
	if !ok {
		return hlc.Timestamp{}, 0
	}
	return conn.GetClosedTimestamp(ctx, rangeID)
}

// onFirstMsg is called when the first message on a stream is received. This is
// the point where the stream finds out what node it's receiving data from.
func (s *Receiver) onFirstMsg(ctx context.Context, r *incomingStream, nodeID roachpb.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.VEventf(ctx, 2, "n%d opened a closed timestamps side-transport connection", nodeID)
	// If we already have a connection from nodeID, we don't accept this one. The
	// other one has to be zombie going away soon. The client is expected to retry
	// to establish the new connection.
	//
	// We could figure out a way to signal the existing connection to terminate,
	// but it doesn't seem worth it.
	if _, ok := s.mu.conns[nodeID]; ok {
		return errors.Errorf("connection from n%d already exists", nodeID)
	}
	s.mu.conns[nodeID] = r
	r.testingKnobs = s.testingKnobs[nodeID]
	return nil
}

// onRecvErr is called when one of the inbound streams errors out. The stream is
// removed from the Receiver's collection.
func (s *Receiver) onRecvErr(ctx context.Context, nodeID roachpb.NodeID, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err != io.EOF {
		log.Warningf(ctx, "closed timestamps side-transport connection dropped from node: %d", nodeID)
	} else {
		log.VEventf(ctx, 2, "closed timestamps side-transport connection dropped from node: %d (%s)", nodeID, err)
	}
	if nodeID != 0 {
		// Remove the connection from the map, awaiting a new conn to be opened by
		// the remote node. Note that, in doing so, we lose all information about
		// the ranges tracked by this connection. We could go through all of them
		// and move their data to the replica, but that might be expensive.
		// Alternatively, we could also do something else to not destroy the state
		// of this connection. Note, though, that if any of these closed timestamps
		// have been actually used to serve a read already, the info has been copied
		// to the respective replica.
		delete(s.mu.conns, nodeID)
		s.historyMu.Lock()
		s.historyMu.lastClosed[nodeID] = streamCloseInfo{
			nodeID:    nodeID,
			closeErr:  err,
			closeTime: timeutil.Now(),
		}
		s.historyMu.Unlock()
	}
}

// incomingStream represents an inbound connection to a node publishing closed
// timestamp information. It maintains the latest closed timestamps communicated
// by the sender node.
type incomingStream struct {
	// The server that created this stream.
	server       *Receiver
	stores       Stores
	testingKnobs incomingStreamTestingKnobs
	connectedAt  time.Time
	// The node that's sending info on this stream.
	nodeID roachpb.NodeID

	mu struct {
		syncutil.RWMutex
		streamState
		lastReceived time.Time
	}
}

type incomingStreamTestingKnobs struct {
	onFirstMsg chan struct{}
	onRecvErr  func(sender roachpb.NodeID, err error)
	onMsg      chan *ctpb.Update
}

// Stores is the interface of *Stores needed by incomingStream.
type Stores interface {
	// ForwardSideTransportClosedTimestampForRange forwards the side-transport
	// closed timestamp for the local replica(s) of the given range.
	ForwardSideTransportClosedTimestampForRange(
		ctx context.Context, rangeID roachpb.RangeID, closedTS hlc.Timestamp, lai ctpb.LAI)
}

func newIncomingStream(s *Receiver, stores Stores) *incomingStream {
	r := &incomingStream{
		server:      s,
		stores:      stores,
		connectedAt: timeutil.Now(),
	}
	return r
}

// GetClosedTimestamp returns the latest closed timestamp that the receiver
// knows for a particular range, together with the LAI needed to have applied in
// order to use this closed timestamp. Returns an empty timestamp if the stream
// does not have state for the range.
func (r *incomingStream) GetClosedTimestamp(
	ctx context.Context, rangeID roachpb.RangeID,
) (hlc.Timestamp, ctpb.LAI) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.mu.tracked[rangeID]
	if !ok {
		return hlc.Timestamp{}, 0
	}
	return r.mu.lastClosed[info.policy], info.lai
}

// processUpdate processes one update received on the stream, updating the local
// state.
func (r *incomingStream) processUpdate(ctx context.Context, msg *ctpb.Update) {
	log.VEventf(ctx, 4, "received side-transport update: %v", msg)

	if msg.NodeID == 0 {
		log.Fatalf(ctx, "missing NodeID in message: %s", msg)
	}

	if msg.NodeID != r.nodeID {
		log.Fatalf(ctx, "wrong NodeID; expected %d, got %d", r.nodeID, msg.NodeID)
	}

	// Handle the removed ranges. In order to not lose closed ts info, before we
	// can remove a range from our tracking, we copy the info about its closed
	// timestamp to the local replica(s). Note that it's important to do this
	// before updating lastClosed below since, by definition, the closed
	// timestamps in this message don't apply to the Removed ranges.
	if len(msg.Removed) != 0 {
		// Note that we call r.stores.ForwardSideTransportClosedTimestampForRange while holding
		// our read lock, not write lock. ForwardSideTransportClosedTimestampForRange will call
		// into each Replica, telling it to hold on locally to the the info we're about to
		// remove from the stream. We can't do this with the mutex write-locked
		// because replicas call GetClosedTimestamp() independently, with r.mu held
		// (=> deadlock).
		r.mu.RLock()
		for _, rangeID := range msg.Removed {
			info, ok := r.mu.tracked[rangeID]
			if !ok {
				log.Fatalf(ctx, "attempting to unregister a missing range: r%d", rangeID)
			}
			r.stores.ForwardSideTransportClosedTimestampForRange(
				ctx, rangeID, r.mu.lastClosed[info.policy], info.lai)
		}
		r.mu.RUnlock()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.lastReceived = timeutil.Now()

	// Reset all the state on snapshots.
	if msg.Snapshot {
		for i := range r.mu.lastClosed {
			r.mu.lastClosed[i] = hlc.Timestamp{}
		}
		r.mu.tracked = make(map[roachpb.RangeID]trackedRange, len(r.mu.tracked))
	} else if msg.SeqNum != r.mu.lastSeqNum+1 {
		log.Fatalf(ctx, "expected closed timestamp side-transport message with sequence number "+
			"%d, got %d", r.mu.lastSeqNum+1, msg.SeqNum)
	}
	r.mu.lastSeqNum = msg.SeqNum

	for _, rng := range msg.AddedOrUpdated {
		r.mu.tracked[rng.RangeID] = trackedRange{
			lai:    rng.LAI,
			policy: rng.Policy,
		}
	}
	for _, rangeID := range msg.Removed {
		delete(r.mu.tracked, rangeID)
	}
	for _, update := range msg.ClosedTimestamps {
		r.mu.lastClosed[update.Policy] = update.ClosedTimestamp
	}
}

// Run handles an incoming stream of closed timestamps.
func (r *incomingStream) Run(
	ctx context.Context,
	stopper *stop.Stopper,
	// The gRPC stream with incoming messages.
	stream ctpb.SideTransport_PushUpdatesServer,
) error {
	// We have to do the stream processing on a separate goroutine because Recv()
	// is blocking, with no way to interrupt it other than returning from the RPC
	// handler (i.e. this Run function).
	// The main goroutine remains in charge of listening for stopper quiescence.
	streamDone := make(chan struct{})
	if err := stopper.RunAsyncTask(ctx, "closedts side-transport server conn", func(ctx context.Context) {
		// On exit, signal the other goroutine to terminate.
		defer close(streamDone)
		for {
			msg, err := stream.Recv()
			if err != nil {
				if fn := r.testingKnobs.onRecvErr; fn != nil {
					fn(r.nodeID, err)
				}

				r.server.onRecvErr(ctx, r.nodeID, err)
				return
			}

			if r.nodeID == 0 {
				r.nodeID = msg.NodeID

				if err := r.server.onFirstMsg(ctx, r, r.nodeID); err != nil {
					log.Warningf(ctx, "%s", err.Error())
					return
				} else if ch := r.testingKnobs.onFirstMsg; ch != nil {
					ch <- struct{}{}
				}
				if !msg.Snapshot {
					log.Fatal(ctx, "expected the first message to be a snapshot")
				}
			}

			r.processUpdate(ctx, msg)
			if ch := r.testingKnobs.onMsg; ch != nil {
				select {
				case ch <- msg:
				default:
				}
			}
		}
	}); err != nil {
		return err
	}

	// Block until the client terminates (or there's another stream error) or
	// the stopper signals us to bail.
	select {
	case <-streamDone:
	case <-stopper.ShouldQuiesce():
	}
	// Returning causes a blocked stream.Recv() (if there still is one) to return.
	return nil
}
