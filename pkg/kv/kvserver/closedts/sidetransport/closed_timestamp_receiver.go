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
	"fmt"
	"io"
	"strings"

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

type closedTimestampStreamReceiver struct {
	// The server that created this stream.
	server *ClosedTimestampReceiver
	stores StoresInterface
	// The node that's sending info on this stream.
	nodeID roachpb.NodeID

	mu struct {
		syncutil.Mutex
		lastTimestamps [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp
		ranges         map[roachpb.RangeID]rangeInfo
		lastSeq        ctpb.SeqNum
	}
}

type rangeInfo struct {
	lai    ctpb.LAI
	policy roachpb.RangeClosedTimestampPolicy
}

// StoresInterface is the Stores interface needed by the
// closedTimestampStreamReceiver.
type StoresInterface interface {
	// ForwardSideTransportClosedTimestampForRange forwards the side-transport
	// closed timestamp for the local replicas of the given range.
	ForwardSideTransportClosedTimestampForRange(
		ctx context.Context, rangeID roachpb.RangeID, closedTS hlc.Timestamp, lai ctpb.LAI)
}

func newClosedTimestampStreamReceiver(
	s *ClosedTimestampReceiver, stores StoresInterface,
) *closedTimestampStreamReceiver {
	r := &closedTimestampStreamReceiver{
		server: s,
		stores: stores,
	}
	return r
}

func (r *closedTimestampStreamReceiver) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var s strings.Builder
	s.WriteString(fmt.Sprintf("n%d closed timestamps: ", r.nodeID))
	now := timeutil.Now()
	rangesByPoicy := make(map[roachpb.RangeClosedTimestampPolicy]*strings.Builder)
	for pol, ts := range r.mu.lastTimestamps {
		policy := roachpb.RangeClosedTimestampPolicy(pol)
		s.WriteString(fmt.Sprintf("%s: %s (lead/lag: %s), ", policy, ts, now.Sub(ts.GoTime())))
		rangesByPoicy[policy] = &strings.Builder{}
	}
	s.WriteRune('\n')
	for rid, info := range r.mu.ranges {
		rangesByPoicy[info.policy].WriteString(fmt.Sprintf("%d, ", rid))
	}
	first := true
	for policy, sb := range rangesByPoicy {
		if !first {
			s.WriteRune('\n')
		} else {
			first = false
		}
		s.WriteString(fmt.Sprintf("%s ranges: %s", policy, sb.String()))
	}
	return s.String()
}

// GetClosedTimestamp returns the latest closed timestamp that the receiver
// knows for a particular range, together with the LAI needed to have applied in
// order to use this closed timestamp.
func (r *closedTimestampStreamReceiver) GetClosedTimestamp(
	ctx context.Context, rangeID roachpb.RangeID,
) (hlc.Timestamp, ctpb.LAI) {
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.mu.ranges[rangeID]
	if !ok {
		return hlc.Timestamp{}, 0
	}
	return r.mu.lastTimestamps[info.policy], info.lai
}

// processUpdate processes one update on the stream, updating the local state.
func (r *closedTimestampStreamReceiver) processUpdate(ctx context.Context, msg *ctpb.Update) {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.VEventf(ctx, 3, "received side-transport update: %v", msg)

	if msg.NodeID == 0 {
		log.Fatalf(ctx, "missing NodeID in message: %s", msg)
	}

	if msg.NodeID != r.nodeID {
		log.Fatalf(ctx, "wrong NodeID; expected %d, got %d", r.nodeID, msg.NodeID)
	}

	// Reset all the state on snapshots.
	if msg.Snapshot {
		for i := range r.mu.lastTimestamps {
			r.mu.lastTimestamps[i] = hlc.Timestamp{}
		}
		r.mu.ranges = make(map[roachpb.RangeID]rangeInfo, len(r.mu.ranges))
	} else if msg.SeqNum != r.mu.lastSeq+1 {
		log.Fatalf(ctx, "expected closed timestamp side-transport message with sequence number "+
			"%d, got %d", r.mu.lastSeq+1, msg.SeqNum)
	}

	// Handle the removed ranges. In order to not lose closed ts info, before we
	// can remove a range from our tracking, we copy the info about its closed
	// timestamp to the local replica(s).
	for _, rangeID := range msg.Removed {
		info, ok := r.mu.ranges[rangeID]
		if !ok {
			log.Fatalf(ctx, "attempting to unregister a missing range: r%d", rangeID)
		}
		r.stores.ForwardSideTransportClosedTimestampForRange(
			ctx, rangeID, r.mu.lastTimestamps[info.policy], info.lai)
		delete(r.mu.ranges, rangeID)
	}

	for _, rng := range msg.AddedOrUpdated {
		r.mu.ranges[rng.RangeID] = rangeInfo{
			lai:    rng.LAI,
			policy: rng.Policy,
		}
	}
	for _, update := range msg.ClosedTimestamps {
		r.mu.lastTimestamps[update.Policy] = update.ClosedTimestamp
	}
	r.mu.lastSeq = msg.SeqNum
}

// Run handles an incoming stream of closed timestamps.
func (r *closedTimestampStreamReceiver) Run(
	ctx context.Context,
	stopper *stop.Stopper,
	// The gRPC stream with incoming messages.
	stream ctpb.SideTransport_PushUpdatesServer,
) error {
	// We have to do the stream processing on a separate goroutine because Recv()
	// is blocking, with no wait to interrupt it other than returning from the RPC
	// handler (i.e. this Run function).
	// The main goroutine remains in charge of listening for stopper quiescence.
	streamDone := make(chan struct{})
	if err := stopper.RunAsyncTask(ctx, "closedts side-transport server conn", func(ctx context.Context) {
		// On exit, signal the other goroutine to terminate.
		defer close(streamDone)
		for {
			msg, err := stream.Recv()
			if err != nil {
				r.server.onRecvErr(ctx, r.nodeID, err)
				return
			}

			if r.nodeID == 0 {
				r.nodeID = msg.NodeID
				if err := r.server.onFirstMsg(r, r.nodeID); err != nil {
					return
				}
				if !msg.Snapshot {
					log.Fatal(ctx, "expected the first message to be a snapshot")
				}
			}

			r.processUpdate(ctx, msg)
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

// ClosedTimestampReceiver is the gRPC server for the closed timestamp
// side-transport, receiving updates from remote nodes. It maintains the set of
// current streaming connections.
type ClosedTimestampReceiver struct {
	log.AmbientContext
	stop   *stop.Stopper
	stores StoresInterface

	mu struct {
		syncutil.Mutex
		connections map[roachpb.NodeID]*closedTimestampStreamReceiver
	}
}

var _ ctpb.SideTransportServer = &ClosedTimestampReceiver{}

// NewSideTransportReceiver creates a ClosedTimestampReceiver, to be used as a
// gRPC server with ctpb.RegisterClosedTimestampSideTransportServer.
func NewSideTransportReceiver(
	nodeID *base.NodeIDContainer, stop *stop.Stopper, stores StoresInterface,
) *ClosedTimestampReceiver {
	r := &ClosedTimestampReceiver{
		stop:   stop,
		stores: stores,
	}
	r.AmbientContext.AddLogTag("n", nodeID)
	r.mu.connections = make(map[roachpb.NodeID]*closedTimestampStreamReceiver)
	return r
}

// PushUpdates is the streaming RPC handler.
func (s *ClosedTimestampReceiver) PushUpdates(stream ctpb.SideTransport_PushUpdatesServer) error {
	// Create a steam to service this connection. The stream will call back into the server
	// through onFirstMsg to register itself once it finds out its node id.
	ctx := s.AnnotateCtx(context.Background())
	return newClosedTimestampStreamReceiver(s, s.stores).Run(ctx, s.stop, stream)
}

// GetClosedTimestamp returns the latest closed timestamp that the receiver
// knows for a particular range, together with the LAI needed to have applied in
// order to use this closed timestamp.
//
// leaseholderNode is the last known leaseholder for the range.
func (s *ClosedTimestampReceiver) GetClosedTimestamp(
	ctx context.Context, rangeID roachpb.RangeID, leaseholderNode roachpb.NodeID,
) (hlc.Timestamp, ctpb.LAI) {
	s.mu.Lock()
	conn, ok := s.mu.connections[leaseholderNode]
	s.mu.Unlock()
	if !ok {
		return hlc.Timestamp{}, 0
	}

	return conn.GetClosedTimestamp(ctx, rangeID)
}

func (s *ClosedTimestampReceiver) onFirstMsg(
	r *closedTimestampStreamReceiver, nodeID roachpb.NodeID,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If we already have a connection from nodeID, we don't accept this one. The
	// other one has to be zombie going away soon. The client is expected to retry
	// to establish the new connection.
	//
	// We could figure out a way to signal the existing connection to terminate,
	// but it doesn't seem worth it.
	if _, ok := s.mu.connections[nodeID]; ok {
		return errors.Errorf("connection from n%d already exists", nodeID)
	}
	s.mu.connections[nodeID] = r
	return nil
}

func (s *ClosedTimestampReceiver) onRecvErr(ctx context.Context, nodeID roachpb.NodeID, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err != io.EOF {
		log.Warningf(ctx, "closed timestamps side-transport connection dropped from node: %d", nodeID)
	}
	if nodeID != 0 {
		delete(s.mu.connections, nodeID)
	}
}
