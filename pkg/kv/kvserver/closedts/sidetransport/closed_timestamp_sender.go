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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// closingPeriod dictates how often the ClosedTimestampSender will close
// timestamps.
const closingPeriod = 200 * time.Millisecond

// ClosedTimestampSender represents the sending-side of the closed timestamps
// "side-transport". Its role is to periodically advance the closed timestamps
// of all the ranges with leases on the current node and to communicate these
// closed timestamps to all other nodes that have replicas for any of these
// ranges.
//
// This side-transport is particularly important for range that are not seeing
// frequent writes; in the absence of Raft proposals, this is the only way for
// the closed timestamps to advance.
//
// The ClosedTimestampSender is notified when leases are acquired or lost by the
// current node. The sender periodically loops through all the ranges with local
// leases, tries to advance the closed timestamp of each range according to its
// policy, and then publishes a message with the update to all other nodes with
// non-leaseholder replicas. Every node receives the same messages; for
// efficiency the sender does not keep track of which follower node is
// interested in which ranges. On the receiver side the closed timestamp updates
// are processed lazily, so it doesn't particularly matter that each receiver is
// told about ranges that it doesn't care about.
type ClosedTimestampSender struct {
	stopper *stop.Stopper
	dialer  *nodedialer.Dialer
	st      *cluster.Settings
	clock   *hlc.Clock
	nodeID  roachpb.NodeID

	trackedMu struct {
		syncutil.Mutex
		// tracked maintains the information that was communicated to connections in
		// the last sent message (implicitly or explicitly). A range enters this
		// structure as soon as it's included in a message, and exits it when it's
		// removed through ClosedTimestampUpdate.Removed.
		tracked        map[roachpb.RangeID]trackedRange
		lastTimestamps map[roachpb.RangeClosedTimestampPolicy]hlc.Timestamp
		// lastSeqNum is the sequence number of the last message published.
		lastSeqNum ctpb.UpdateSequenceNumber
	}

	leaseholdersMu struct {
		syncutil.Mutex
		leaseholders map[roachpb.RangeID]leaseholder
	}

	// buf contains recent messages published to connections. Adding a message to
	// this buffer signals the connections to send it on their streams.
	buf *updatesBuf

	// connections contains connections to all nodes with follower replicas.
	// connections are added as nodes get replicas for ranges with local leases
	// and removed when the respective node no longer has any replicas with
	// local leases.
	connections map[roachpb.NodeID]*connection
}

// trackedRange contains the information that the side-transport last published
// about a particular range.
type trackedRange struct {
	lai    ctpb.LAI
	policy roachpb.RangeClosedTimestampPolicy
}

type leaseholder struct {
	r        *kvserver.Replica
	storeID  roachpb.StoreID
	leaseSeq roachpb.LeaseSequence
}

// NewSideTransportSender creates a ClosedTimestampSender. Run must be called on
// it afterwards to get it to start publishing closed timestamps.
func NewSideTransportSender(
	stopper *stop.Stopper, dialer *nodedialer.Dialer, clock *hlc.Clock, st *cluster.Settings,
) *ClosedTimestampSender {
	bufSize := 3 * time.Second.Nanoseconds() / closingPeriod.Nanoseconds()
	if bufSize < 2 {
		bufSize = 2
	}
	s := &ClosedTimestampSender{
		stopper: stopper,
		dialer:  dialer,
		buf:     newUpdatesBuf(int(bufSize)),
		clock:   clock,
		st:      st,
	}
	s.trackedMu.tracked = make(map[roachpb.RangeID]trackedRange)
	s.trackedMu.lastTimestamps = make(map[roachpb.RangeClosedTimestampPolicy]hlc.Timestamp)
	s.leaseholdersMu.leaseholders = make(map[roachpb.RangeID]leaseholder)
	s.connections = make(map[roachpb.NodeID]*connection)
	return s
}

// Run starts a goroutine that periodically closes new timestamps for all the
// ranges where the leaseholder is on this node.
//
// nodeID is the id of the local node. Used to avoid connecting to ourselves.
// This is not know at construction time.
func (s *ClosedTimestampSender) Run(ctx context.Context, nodeID roachpb.NodeID) {
	s.nodeID = nodeID

	_ /* err */ = s.stopper.RunAsyncTask(ctx, "closedts side-transport publisher",
		func(ctx context.Context) {
			defer func() {
				// Cleanup all connections.
				s.trackedMu.Lock()
				defer s.trackedMu.Unlock()
				for _, r := range s.connections {
					r.close()
				}
			}()

			var timer timeutil.Timer
			defer timer.Stop()
			for {
				timer.Reset(closingPeriod)
				select {
				case <-timer.C:
					timer.Read = true
					if !s.st.Version.IsActive(ctx, clusterversion.ClosedTimestampsRaftTransport) {
						continue
					}
					s.publish(ctx)
				case <-s.stopper.ShouldQuiesce():
					return
				}
			}
		})
}

// RegisterLeaseholder adds a range to the leaseholders collection. From now on,
// the side-transport will try to advance this range's closed timestamp.
func (s *ClosedTimestampSender) RegisterLeaseholder(
	ctx context.Context, r *kvserver.Replica, storeID roachpb.StoreID, leaseSeq roachpb.LeaseSequence,
) {
	s.leaseholdersMu.Lock()
	defer s.leaseholdersMu.Unlock()

	if lh, ok := s.leaseholdersMu.leaseholders[r.RangeID]; ok {
		// The leaseholder is already registered. If we're already aware of a newer lease,
		// there's nothing to do.
		if lh.leaseSeq >= leaseSeq {
			return
		}
	}
	s.leaseholdersMu.leaseholders[r.RangeID] = leaseholder{
		r:        r,
		storeID:  storeID,
		leaseSeq: leaseSeq,
	}
}

// UnregisterLeaseholder removes a range from the leaseholders collection.
func (s *ClosedTimestampSender) UnregisterLeaseholder(
	ctx context.Context, rangeID roachpb.RangeID, storeID roachpb.StoreID,
) {
	s.leaseholdersMu.Lock()
	defer s.leaseholdersMu.Unlock()

	if lh, ok := s.leaseholdersMu.leaseholders[rangeID]; ok && lh.storeID == storeID {
		delete(s.leaseholdersMu.leaseholders, rangeID)
	}
}

func (s *ClosedTimestampSender) publish(ctx context.Context) {
	s.trackedMu.Lock()
	defer s.trackedMu.Unlock()

	// We'll accumulate all the nodes we need to connect to in order to check if
	// we need to open new connections or close existing ones.
	nodesWithFollowers := util.MakeFastIntSet()

	// Fix the closed timestamps that will be communicated to by this message.
	// These timestamps (one per range policy) will apply to all the ranges
	// included in message.
	var targetByPolicy [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp
	now := s.clock.NowAsClockTimestamp()
	lagTargetDuration := closedts.TargetDuration.Get(&s.st.SV)
	targetByPolicy[roachpb.LAG_BY_CLUSTER_SETTING] =
		kvserver.ClosedTimestampTargetByPolicy(now, roachpb.LAG_BY_CLUSTER_SETTING, lagTargetDuration)
	targetByPolicy[roachpb.LEAD_FOR_GLOBAL_READS] =
		kvserver.ClosedTimestampTargetByPolicy(now, roachpb.LEAD_FOR_GLOBAL_READS, lagTargetDuration)

	s.trackedMu.lastSeqNum++
	msg := &ctpb.ClosedTimestampUpdate{
		NodeID: s.nodeID,
		SeqNum: s.trackedMu.lastSeqNum,
		// The first message produced is essentially a snapshot, since it has no
		// previous state to reference.
		Snapshot: s.trackedMu.lastSeqNum == 1,
	}

	// Make a copy of the leaseholders map, in order to release its mutex quickly. We can't hold
	// this mutex while calling into any replicas (and locking the replica) because replicas call
	// into the ClosedTimestampSender and take leaseholdersMu through Register/UnregisterLeaseholder.
	s.leaseholdersMu.Lock()
	leaseholders := make(map[roachpb.RangeID]leaseholder, len(s.leaseholdersMu.leaseholders))
	for k, v := range s.leaseholdersMu.leaseholders {
		leaseholders[k] = v
	}
	s.leaseholdersMu.Unlock()

	for _, lh := range leaseholders {
		r := lh.r
		// Make sure that we're communicating with all of the range's followers.
		// Note that we're including this range's followers before deciding below if
		// this message will include this range. This is because we don't want
		// dynamic conditions about the activity of this range to dictate the
		// opening and closing of connections to the other nodes.
		for _, repl := range r.Desc().Replicas().VoterFullAndNonVoterDescriptors() {
			nodesWithFollowers.Add(int(repl.NodeID))
		}

		// Check whether the desired timestamp can be closed on this range.
		canClose, lai, policy := r.BumpSideTransportClosed(ctx, now, targetByPolicy)
		lastMsg, tracked := s.trackedMu.tracked[r.RangeID]
		if !canClose {
			// We can't close the desired timestamp. If this range was tracked, we
			// need to un-track it.
			if tracked {
				msg.Removed = append(msg.Removed, r.RangeID)
				delete(s.trackedMu.tracked, r.RangeID)
			}
			continue
		}

		// Check whether the range needs to be explicitly updated through the
		// current message, or if its update can be implicit.
		needExplicit := false
		if !tracked {
			// If the range was not included in the last message, we need to include
			// it now to start "tracking" it in the side-transport.
			needExplicit = true
		} else if lastMsg.lai < lai {
			// If the range's LAI has changed, we need to explicitly publish the new
			// LAI.
			needExplicit = true
		} else if lastMsg.policy != policy {
			// If the policy changed, we need to explicitly publish that; the
			// receiver will updates its bookkeeping to indicate that this range is
			// updated through implicit updates for the new policy.
			needExplicit = true
		}
		if needExplicit {
			msg.AddedOrUpdated = append(msg.AddedOrUpdated, ctpb.RangeUpdate{RangeID: r.RangeID, LAI: lai, Policy: policy})
			s.trackedMu.tracked[r.RangeID] = trackedRange{lai: lai, policy: policy}
		}

	}

	// If there's any tracked ranges for which we're not the leaseholder any more,
	// we need to untrack them and tell the connections about it.
	for rid := range s.trackedMu.tracked {
		if _, ok := leaseholders[rid]; !ok {
			msg.Removed = append(msg.Removed, rid)
			delete(s.trackedMu.tracked, rid)
		}
	}

	// Close connections to the nodes that no longer need any info from us
	// (because they don't have replicas for any of the ranges with leases on this
	// node).
	for nodeID, conn := range s.connections {
		if !nodesWithFollowers.Contains(int(nodeID)) {
			delete(s.connections, nodeID)
			conn.close()
		}
	}

	// Open connections to any node that needs info from us and is missing a conn.
	nodesWithFollowers.ForEach(func(nid int) {
		nodeID := roachpb.NodeID(nid)
		// Note that we don't open a connection to ourselves. The timestamps that
		// we'we closing are written directly to the sideTransportClosedTimestamp
		// fields of the local replicas.
		if _, ok := s.connections[nodeID]; !ok && nodeID != s.nodeID {
			c := newConnection(s, nodeID, s.dialer, s.buf)
			s.connections[nodeID] = c
			c.run(ctx, s.stopper)
		}
	})

	// Publish the new message to all connections.
	s.buf.Push(ctx, msg)
}

// GetSnapshot generates an update that contains all the sender's state (as
// opposed to being an incremental delta since a previous message). The returned
// msg will have the `snapshot` field set, and a sequence number indicating
// where to resume sending incremental updates.
func (s *ClosedTimestampSender) GetSnapshot() *ctpb.ClosedTimestampUpdate {
	s.trackedMu.Lock()
	defer s.trackedMu.Unlock()

	msg := &ctpb.ClosedTimestampUpdate{
		Snapshot: true,
		// Assigning this SeqNum means that the next incremental sent needs to be
		// lastSeqNum+1. Notice that GetSnapshot synchronizes with the publishing of
		// of incremental messages.
		SeqNum: s.trackedMu.lastSeqNum,
	}
	for rid, r := range s.trackedMu.tracked {
		msg.AddedOrUpdated = append(msg.AddedOrUpdated, ctpb.RangeUpdate{
			RangeID: rid,
			LAI:     r.lai,
			Policy:  r.policy,
		})
	}
	for policy, ts := range s.trackedMu.lastTimestamps {
		msg.ClosedTimestamp = append(msg.ClosedTimestamp, ctpb.ClosedTimestampUpdateGroupUpdate{
			Policy:          policy,
			ClosedTimestamp: ts,
		})
	}
	return msg
}

// updatesBuf is a circular buffer of ClosedTimestampUpdates. It's created with
// a given capacity and, once it fills up, new items overwrite the oldest ones.
// It lets consumers query for the update with a particular sequence number and
// it lets queries block until the next update is produced.
type updatesBuf struct {
	mu struct {
		syncutil.Mutex
		data []*ctpb.ClosedTimestampUpdate
		// head points to the earliest update in the buffer. If the buffer is empty,
		// head is 0 and the respective slot is nil.
		//
		// tail points to the next slot to be written to. When the buffer is full,
		// tail == head meaning that the head will be overwritten by the next
		// insertion.
		head, tail int
		// updated is signaled when a new item is inserted.
		updated *sync.Cond
	}
}

func newUpdatesBuf(size int) *updatesBuf {
	buf := &updatesBuf{}
	buf.mu.data = make([]*ctpb.ClosedTimestampUpdate, size)
	buf.mu.updated = sync.NewCond(&buf.mu)
	return buf
}

func (b *updatesBuf) Push(ctx context.Context, update *ctpb.ClosedTimestampUpdate) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// If the buffer is not empty, sanity check the seq num.
	if b.sizeLocked() != 0 {
		lastIdx := (b.mu.tail - 1)
		if lastIdx < 0 {
			lastIdx += len(b.mu.data)
		}
		if prevSeq := b.mu.data[lastIdx].SeqNum; prevSeq != update.SeqNum-1 {
			log.Fatalf(ctx, "bad sequence number; expected %d, got %d", prevSeq+1, update.SeqNum)
		}
	}

	b.mu.data[b.mu.tail] = update
	overwrite := b.fullLocked()
	b.mu.tail = (b.mu.tail + 1) % len(b.mu.data)
	// If the tail just overwrote the head, move the head.
	if overwrite {
		b.mu.head = (b.mu.head + 1) % len(b.mu.data)
	}

	// Notify everybody who might have been waiting for this message - we expect
	// all the connections to be blocked waiting.
	b.mu.updated.Broadcast()
}

// GetBySeq looks through the buffer and returns the update with the requested
// sequence number. It's OK to request a seqNum one higher than the highest
// produced; the call will block until the message is produced.
//
// If the requested message is too old and is no longer in the buffer, returns nil.
func (b *updatesBuf) GetBySeq(
	ctx context.Context, seqNum ctpb.UpdateSequenceNumber,
) *ctpb.ClosedTimestampUpdate {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Loop until the requested seqNum is added to the buffer.
	for {
		var firstSeq, lastSeq ctpb.UpdateSequenceNumber
		if b.sizeLocked() == 0 {
			firstSeq, lastSeq = 0, 0
		} else {
			firstSeq, lastSeq = b.mu.data[b.mu.head].SeqNum, b.mu.data[b.mu.tail].SeqNum
		}
		if seqNum < firstSeq {
			// Requesting a message that's not in the buffer any more.
			return nil
		}
		// If the requested msg has not been produced yet, block.
		if seqNum == lastSeq+1 {
			b.mu.updated.Wait()
			continue
		}
		if seqNum > lastSeq+1 {
			log.Fatalf(ctx, "skipping sequence numbers; requested: %d, last: %d", seqNum, lastSeq)
		}
		idx := (b.mu.head + (int)(seqNum-firstSeq)) % len(b.mu.data)
		return b.mu.data[idx]
	}
}

func (b *updatesBuf) sizeLocked() int {
	if b.mu.head < b.mu.tail {
		return b.mu.tail - b.mu.head
	} else if b.mu.head == b.mu.tail {
		// The buffer is either empty or full.
		if b.mu.head == 0 && b.mu.data[0] == nil {
			return 0
		}
		return len(b.mu.data)
	} else {
		return len(b.mu.data) + b.mu.tail - b.mu.head
	}
}

func (b *updatesBuf) fullLocked() bool {
	return b.sizeLocked() == len(b.mu.data)
}

// connection represents a connection to one particular node. The connection
// watches an updatesBuf and streams all the messages to the respective node.
type connection struct {
	log.AmbientContext
	producer *ClosedTimestampSender
	nodeID   roachpb.NodeID
	dialer   *nodedialer.Dialer
	// buf accumulates messages to be sent to the connection. If the buffer
	// overflows (because this stream is disconnected for long enough), we'll have
	// to send a snapshot before we can resume sending regular messages.
	buf    *updatesBuf
	stream ctpb.ClosedTimestampSideTransport_PushUpdatesClient
	closed int32 // atomic
}

func newConnection(
	p *ClosedTimestampSender, nodeID roachpb.NodeID, dialer *nodedialer.Dialer, buf *updatesBuf,
) *connection {
	r := &connection{
		producer: p,
		nodeID:   nodeID,
		dialer:   dialer,
		buf:      buf,
	}
	r.AddLogTag("ctstream", nodeID)
	return r
}

// close makes the connection stop sending messages. The run() goroutine will
// exit asynchronously. The parent ClosedTimestampSender is expected to remove
// this connection from its list.
func (r *connection) close() {
	atomic.StoreInt32(&r.closed, 1)
}

func (r *connection) sendMsg(ctx context.Context, msg *ctpb.ClosedTimestampUpdate) error {
	if r.stream == nil {
		conn, err := r.dialer.Dial(ctx, r.nodeID, rpc.SystemClass)
		if err != nil {
			return err
		}
		r.stream, err = ctpb.NewClosedTimestampSideTransportClient(conn).PushUpdates(ctx)
		if err != nil {
			return err
		}
	}
	return r.stream.Send(msg)
}

func (r *connection) run(ctx context.Context, stopper *stop.Stopper) {
	_ /* err */ = stopper.RunAsyncTask(ctx, fmt.Sprintf("closedts publisher for n%d", r.nodeID),
		func(ctx context.Context) {
			ctx, cancel := stopper.WithCancelOnQuiesce(r.AnnotateCtx(ctx))
			defer cancel()
			everyN := log.Every(10 * time.Second)

			var lastSent ctpb.UpdateSequenceNumber
			for {
				var msg *ctpb.ClosedTimestampUpdate

				if r.stream == nil {
					// The stream is not established; the first message needs to be a
					// snapshot.
					msg = r.producer.GetSnapshot()
				} else {
					msg = r.buf.GetBySeq(ctx, lastSent+1)
					if msg == nil {
						// The sequence number we've requested is no longer in the buffer.
						// We need to generate a snapshot in order to re-initialize the
						// stream.
						msg = r.producer.GetSnapshot()
					}
				}

				// See if we've been signaled to stop.
				closed := atomic.LoadInt32(&r.closed) > 0
				if closed || ctx.Err() != nil {
					if r.stream != nil {
						_ /* err */ = r.stream.CloseSend()
					}
					return
				}

				lastSent = msg.SeqNum
				if err := r.sendMsg(ctx, msg); err != nil {
					if everyN.ShouldLog() {
						log.Warningf(ctx, "failed to send closed timestamp message %d to n%d: %s",
							lastSent, r.nodeID, err)
					}
					// Keep track of the fact that we need a new connection.
					//
					// TODO(andrei): Instead of simply trying to establish a connection
					// again when the next message needs to be sent and get rejected by
					// the circuit breaker if the remote node is still unreachable, we
					// should have a blocking version of Dial() that we just leave hanging
					// and get a notification when it succeeds.
					r.stream = nil
				}
			}
		})
}
