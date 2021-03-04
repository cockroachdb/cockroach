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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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

// Sender represents the sending-side of the closed timestamps "side-transport".
// Its role is to periodically advance the closed timestamps of all the ranges
// with leases on the current node and to communicate these closed timestamps to
// all other nodes that have replicas for any of these ranges.
//
// This side-transport is particularly important for range that are not seeing
// frequent writes; in the absence of Raft proposals, this is the only way for
// the closed timestamps to advance.
//
// The Sender is notified when leases are acquired or lost by the current node.
// The sender periodically loops through all the ranges with local leases, tries
// to advance the closed timestamp of each range according to its policy, and
// then publishes a message with the update to all other nodes with
// non-leaseholder replicas. Every node receives the same messages; for
// efficiency the sender does not keep track of which follower node is
// interested in which ranges. On the receiver side the closed timestamp updates
// are processed lazily, so it doesn't particularly matter that each receiver is
// told about ranges that it doesn't care about.
type Sender struct {
	stopper *stop.Stopper
	st      *cluster.Settings
	clock   *hlc.Clock
	nodeID  roachpb.NodeID

	trackedMu struct {
		syncutil.Mutex
		// lastSeqNum is the sequence number of the last message published.
		lastSeqNum ctpb.SeqNum
		// lastClosed is the closed timestamp published for each policy in the
		// last message.
		lastClosed [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp
		// tracked maintains the information that was communicated to connections in
		// the last sent message (implicitly or explicitly). A range enters this
		// structure as soon as it's included in a message, and exits it when it's
		// removed through Update.Removed.
		tracked map[roachpb.RangeID]trackedRange
	}

	leaseholdersMu struct {
		syncutil.Mutex
		leaseholders map[roachpb.RangeID]leaseholder
	}

	// buf contains recent messages published to connections. Adding a message
	// to this buffer signals the connections to send it on their streams.
	buf *updatesBuf

	// connFactory is used to establish new connections.
	connFactory connFactory
	// conns contains connections to all nodes with follower replicas of any of
	// the registered leaseholder. connections are added as nodes get replicas
	// for ranges with local leases and removed when the respective node no
	// longer has any replicas with local leases.
	conns map[roachpb.NodeID]conn
}

// trackedRange contains the information that the side-transport last published
// about a particular range.
type trackedRange struct {
	lai    ctpb.LAI
	policy roachpb.RangeClosedTimestampPolicy
}

// leaseholder represents a leaseholder replicas that has been registered with
// the sender and can send closed timestamp updates through the side transport.
type leaseholder struct {
	Replica
	leaseSeq roachpb.LeaseSequence
}

// Replica represents a *Replica object, but with only the capabilities needed
// by the closed timestamp side transport to accomplish its job.
type Replica interface {
	// Accessors.
	StoreID() roachpb.StoreID
	GetRangeID() roachpb.RangeID
	Desc() *roachpb.RangeDescriptor

	// BumpSideTransportClosed advances the range's closed timestamp if it can.
	// If the closed timestamp is advanced, the function synchronizes with
	// incoming requests, making sure that future requests are not allowed to
	// write below the new closed timestamp.
	//
	// Returns false is the desired timestamp could not be closed. This can
	// happen if the lease is no longer valid, if the range has proposals
	// in-flight, if there are requests evaluating above the desired closed
	// timestamp, or if the range has already closed a higher timestamp.
	//
	// If the closed timestamp was advanced, the function returns a LAI to be
	// attached to the newly closed timestamp.
	BumpSideTransportClosed(
		ctx context.Context,
		now hlc.ClockTimestamp,
		targetByPolicy [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp,
	) (bool, ctpb.LAI, roachpb.RangeClosedTimestampPolicy)
}

// NewSender creates a Sender. Run must be called on it afterwards to get it to
// start publishing closed timestamps.
func NewSender(
	stopper *stop.Stopper, st *cluster.Settings, clock *hlc.Clock, dialer *nodedialer.Dialer,
) *Sender {
	return newSenderWithConnFactory(stopper, st, clock, newRPCConnFactory(dialer))
}

func newSenderWithConnFactory(
	stopper *stop.Stopper, st *cluster.Settings, clock *hlc.Clock, connFactory connFactory,
) *Sender {
	s := &Sender{
		stopper:     stopper,
		st:          st,
		clock:       clock,
		buf:         newUpdatesBuf(),
		connFactory: connFactory,
	}
	s.trackedMu.tracked = make(map[roachpb.RangeID]trackedRange)
	s.leaseholdersMu.leaseholders = make(map[roachpb.RangeID]leaseholder)
	s.conns = make(map[roachpb.NodeID]conn)
	return s
}

// Run starts a goroutine that periodically closes new timestamps for all the
// ranges where the leaseholder is on this node.
//
// nodeID is the id of the local node. Used to avoid connecting to ourselves.
// This is not know at construction time.
func (s *Sender) Run(ctx context.Context, nodeID roachpb.NodeID) {
	s.nodeID = nodeID

	confCh := make(chan struct{}, 1)
	confChanged := func() {
		select {
		case confCh <- struct{}{}:
		default:
		}
	}
	closedts.SideTransportCloseInterval.SetOnChange(&s.st.SV, confChanged)

	_ /* err */ = s.stopper.RunAsyncTask(ctx, "closedts side-transport publisher",
		func(ctx context.Context) {
			defer func() {
				// Closing the buffer signals all connections to quit.
				s.buf.Close()
			}()

			timer := timeutil.NewTimer()
			defer timer.Stop()
			for {
				interval := closedts.SideTransportCloseInterval.Get(&s.st.SV)
				if interval > 0 {
					timer.Reset(interval)
				} else {
					// Disable the side-transport.
					timer.Stop()
					timer = timeutil.NewTimer()
				}
				select {
				case <-timer.C:
					timer.Read = true
					if !s.st.Version.IsActive(ctx, clusterversion.ClosedTimestampsRaftTransport) {
						continue
					}
					s.publish(ctx)
				case <-confCh:
					// Loop around to use the updated timer.
					continue
				case <-s.stopper.ShouldQuiesce():
					return
				}
			}
		})
}

// RegisterLeaseholder adds a replica to the leaseholders collection. From now
// on, until the replica is unregistered, the side-transport will try to advance
// this replica's closed timestamp.
func (s *Sender) RegisterLeaseholder(
	ctx context.Context, r Replica, leaseSeq roachpb.LeaseSequence,
) {
	s.leaseholdersMu.Lock()
	defer s.leaseholdersMu.Unlock()

	if lh, ok := s.leaseholdersMu.leaseholders[r.GetRangeID()]; ok {
		// The leaseholder is already registered. If we're already aware of this
		// or a newer lease, there's nothing to do.
		if lh.leaseSeq >= leaseSeq {
			return
		}
		// Otherwise, update the leaseholder, which may be different object if
		// the lease moved between replicas for the same range on the same node
		// but on different stores.
	}
	s.leaseholdersMu.leaseholders[r.GetRangeID()] = leaseholder{
		Replica:  r,
		leaseSeq: leaseSeq,
	}
}

// UnregisterLeaseholder removes a replica from the leaseholders collection, if
// the replica is currently tracked.
func (s *Sender) UnregisterLeaseholder(
	ctx context.Context, storeID roachpb.StoreID, rangeID roachpb.RangeID,
) {
	s.leaseholdersMu.Lock()
	defer s.leaseholdersMu.Unlock()

	if lh, ok := s.leaseholdersMu.leaseholders[rangeID]; ok && lh.StoreID() == storeID {
		delete(s.leaseholdersMu.leaseholders, rangeID)
	}
}

func (s *Sender) publish(ctx context.Context) hlc.ClockTimestamp {
	s.trackedMu.Lock()
	defer s.trackedMu.Unlock()

	msg := &ctpb.Update{
		NodeID:           s.nodeID,
		ClosedTimestamps: make([]ctpb.Update_GroupUpdate, len(s.trackedMu.lastClosed)),
	}

	// Determine the message's sequence number.
	s.trackedMu.lastSeqNum++
	msg.SeqNum = s.trackedMu.lastSeqNum
	// The first message produced is essentially a snapshot, since it has no
	// previous state to reference.
	msg.Snapshot = msg.SeqNum == 1

	// Fix the closed timestamps that will be communicated to by this message.
	// These timestamps (one per range policy) will apply to all the ranges
	// included in message.
	now := s.clock.NowAsClockTimestamp()
	maxClockOffset := s.clock.MaxOffset()
	lagTargetDuration := closedts.TargetDuration.Get(&s.st.SV)
	for i := range s.trackedMu.lastClosed {
		pol := roachpb.RangeClosedTimestampPolicy(i)
		target := closedts.TargetForPolicy(now, maxClockOffset, lagTargetDuration, pol)
		s.trackedMu.lastClosed[pol] = target
		msg.ClosedTimestamps[pol] = ctpb.Update_GroupUpdate{
			Policy:          pol,
			ClosedTimestamp: target,
		}
	}

	// Make a copy of the leaseholders map, in order to release its mutex
	// quickly. We can't hold this mutex while calling into any replicas (and
	// locking the replica) because replicas call into the Sender and take
	// leaseholdersMu through Register/UnregisterLeaseholder.
	s.leaseholdersMu.Lock()
	leaseholders := make(map[roachpb.RangeID]leaseholder, len(s.leaseholdersMu.leaseholders))
	for k, v := range s.leaseholdersMu.leaseholders {
		leaseholders[k] = v
	}
	s.leaseholdersMu.Unlock()

	// We'll accumulate all the nodes we need to connect to in order to check if
	// we need to open new connections or close existing ones.
	nodesWithFollowers := util.MakeFastIntSet()

	// If there's any tracked ranges for which we're not the leaseholder any more,
	// we need to untrack them and tell the connections about it.
	for rid := range s.trackedMu.tracked {
		if _, ok := leaseholders[rid]; !ok {
			msg.Removed = append(msg.Removed, rid)
			delete(s.trackedMu.tracked, rid)
		}
	}

	// Iterate through each leaseholder and determine whether it can be part of
	// this update or not.
	for _, lh := range leaseholders {
		lhRangeID := lh.GetRangeID()
		lastMsg, tracked := s.trackedMu.tracked[lhRangeID]

		// Make sure that we're communicating with all of the range's followers.
		// Note that we're including this range's followers before deciding below if
		// this message will include this range. This is because we don't want
		// dynamic conditions about the activity of this range to dictate the
		// opening and closing of connections to the other nodes.
		for _, repl := range lh.Desc().Replicas().VoterFullAndNonVoterDescriptors() {
			nodesWithFollowers.Add(int(repl.NodeID))
		}

		// Check whether the desired timestamp can be closed on this range.
		canClose, lai, policy := lh.BumpSideTransportClosed(ctx, now, s.trackedMu.lastClosed)
		if !canClose {
			// We can't close the desired timestamp. If this range was tracked, we
			// need to un-track it.
			if tracked {
				msg.Removed = append(msg.Removed, lhRangeID)
				delete(s.trackedMu.tracked, lhRangeID)
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
			msg.AddedOrUpdated = append(msg.AddedOrUpdated, ctpb.Update_RangeUpdate{
				RangeID: lhRangeID,
				LAI:     lai,
				Policy:  policy,
			})
			s.trackedMu.tracked[lhRangeID] = trackedRange{lai: lai, policy: policy}
		}
	}

	// Close connections to the nodes that no longer need any info from us
	// (because they don't have replicas for any of the ranges with leases on this
	// node).
	for nodeID, c := range s.conns {
		if !nodesWithFollowers.Contains(int(nodeID)) {
			delete(s.conns, nodeID)
			c.close()
		}
	}

	// Open connections to any node that needs info from us and is missing a conn.
	nodesWithFollowers.ForEach(func(nid int) {
		nodeID := roachpb.NodeID(nid)
		// Note that we don't open a connection to ourselves. The timestamps that
		// we're closing are written directly to the sideTransportClosedTimestamp
		// fields of the local replicas in BumpSideTransportClosed.
		if _, ok := s.conns[nodeID]; !ok && nodeID != s.nodeID {
			c := s.connFactory.new(s, nodeID)
			c.run(ctx, s.stopper)
			s.conns[nodeID] = c
		}
	})

	// Publish the new message to all connections.
	s.buf.Push(ctx, msg)

	// Return the publication time, for tests.
	return now
}

// GetSnapshot generates an update that contains all the sender's state (as
// opposed to being an incremental delta since a previous message). The returned
// msg will have the `snapshot` field set, and a sequence number indicating
// where to resume sending incremental updates.
func (s *Sender) GetSnapshot() *ctpb.Update {
	s.trackedMu.Lock()
	defer s.trackedMu.Unlock()

	msg := &ctpb.Update{
		NodeID: s.nodeID,
		// Assigning this SeqNum means that the next incremental sent needs to be
		// lastSeqNum+1. Notice that GetSnapshot synchronizes with the publishing of
		// of incremental messages.
		SeqNum:           s.trackedMu.lastSeqNum,
		Snapshot:         true,
		ClosedTimestamps: make([]ctpb.Update_GroupUpdate, len(s.trackedMu.lastClosed)),
		AddedOrUpdated:   make([]ctpb.Update_RangeUpdate, 0, len(s.trackedMu.tracked)),
	}
	for pol, ts := range s.trackedMu.lastClosed {
		msg.ClosedTimestamps[pol] = ctpb.Update_GroupUpdate{
			Policy:          roachpb.RangeClosedTimestampPolicy(pol),
			ClosedTimestamp: ts,
		}
	}
	for rid, r := range s.trackedMu.tracked {
		msg.AddedOrUpdated = append(msg.AddedOrUpdated, ctpb.Update_RangeUpdate{
			RangeID: rid,
			LAI:     r.lai,
			Policy:  r.policy,
		})
	}
	return msg
}

// updatesBuf is a circular buffer of Updates. It's created with a given
// capacity and, once it fills up, new items overwrite the oldest ones. It lets
// consumers query for the update with a particular sequence number and it lets
// queries block until the next update is produced.
type updatesBuf struct {
	mu struct {
		syncutil.Mutex
		// updated is signaled when a new item is inserted.
		updated sync.Cond
		// data contains pointers to the Updates.
		data []*ctpb.Update
		// head points to the earliest update in the buffer. If the buffer is empty,
		// head is 0 and the respective slot is nil.
		//
		// tail points to the next slot to be written to. When the buffer is full,
		// tail == head meaning that the head will be overwritten by the next
		// insertion.
		head, tail int
		// closed is set by the producer to signal the consumers to exit.
		closed bool
	}
}

// Size the buffer such that a stream sender goroutine can be blocked for a
// little while and not have to send a snapshot when it resumes.
const updatesBufSize = 50

func newUpdatesBuf() *updatesBuf {
	buf := &updatesBuf{}
	buf.mu.updated.L = &buf.mu
	buf.mu.data = make([]*ctpb.Update, updatesBufSize)
	return buf
}

// Push adds a new update to the back of the buffer.
func (b *updatesBuf) Push(ctx context.Context, update *ctpb.Update) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// If the buffer is not empty, sanity check the seq num.
	if b.sizeLocked() != 0 {
		lastIdx := b.lastIdxLocked()
		if prevSeq := b.mu.data[lastIdx].SeqNum; prevSeq != update.SeqNum-1 {
			log.Fatalf(ctx, "bad sequence number; expected %d, got %d", prevSeq+1, update.SeqNum)
		}
	}

	overwrite := b.fullLocked()
	b.mu.data[b.mu.tail] = update
	b.mu.tail = (b.mu.tail + 1) % len(b.mu.data)
	// If the tail just overwrote the head, move the head.
	if overwrite {
		b.mu.head = (b.mu.head + 1) % len(b.mu.data)
	}

	// Notify everybody who might have been waiting for this message - we expect
	// all the connections to be blocked waiting.
	b.mu.updated.Broadcast()
}

func (b *updatesBuf) lastIdxLocked() int {
	lastIdx := b.mu.tail - 1
	if lastIdx < 0 {
		lastIdx += len(b.mu.data)
	}
	return lastIdx
}

// GetBySeq looks through the buffer and returns the update with the requested
// sequence number. It's OK to request a seqNum one higher than the highest
// produced; the call will block until the message is produced.
//
// If the requested message is too old and is no longer in the buffer, returns nil.
//
// The bool retval is set to false if the producer has closed the buffer. In
// that case, the consumers should quit.
func (b *updatesBuf) GetBySeq(ctx context.Context, seqNum ctpb.SeqNum) (*ctpb.Update, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Loop until the requested seqNum is added to the buffer.
	for {
		if b.mu.closed {
			return nil, false
		}

		var firstSeq, lastSeq ctpb.SeqNum
		if b.sizeLocked() == 0 {
			firstSeq, lastSeq = 0, 0
		} else {
			firstSeq, lastSeq = b.mu.data[b.mu.head].SeqNum, b.mu.data[b.lastIdxLocked()].SeqNum
		}
		if seqNum < firstSeq {
			// Requesting a message that's not in the buffer any more.
			return nil, true
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
		return b.mu.data[idx], true
	}
}

func (b *updatesBuf) sizeLocked() int {
	if b.mu.head < b.mu.tail {
		return b.mu.tail - b.mu.head
	} else if b.mu.head == b.mu.tail {
		// The buffer is either empty or full.
		// Since there's no popping from the buffer, it can only be empty if nothing
		// was ever pushed to it.
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

// Close unblocks all the consumers and signals them to exit.
func (b *updatesBuf) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.closed = true
	b.mu.updated.Broadcast()
}

// connFactory is capable of creating new connections to specific nodes.
type connFactory interface {
	new(*Sender, roachpb.NodeID) conn
}

// conn is a side-transport connection to a node. A conn watches an updatesBuf
// and streams all the messages to the respective node.
type conn interface {
	run(context.Context, *stop.Stopper)
	close()
}

// rpcConnFactory is an implementation of connFactory that establishes
// connections to other nodes using gRPC.
type rpcConnFactory struct {
	dialer *nodedialer.Dialer
}

func newRPCConnFactory(dialer *nodedialer.Dialer) connFactory {
	return &rpcConnFactory{
		dialer: dialer,
	}
}

// new implements the connFactory interface.
func (f *rpcConnFactory) new(s *Sender, nodeID roachpb.NodeID) conn {
	return newRPCConn(f.dialer, s, nodeID)
}

// rpcConn is an implementation of conn that is implemented using a gRPC stream.
//
// The connection will read messages from producer.buf. If the buffer overflows
// (because this stream is disconnected for long enough), we'll have to send a
// snapshot before we can resume sending regular messages.
type rpcConn struct {
	log.AmbientContext
	dialer   *nodedialer.Dialer
	producer *Sender
	nodeID   roachpb.NodeID
	stream   ctpb.SideTransport_PushUpdatesClient
	// cancelStreamCtx cleans up the resources (goroutine) associated with stream.
	// It needs to be called whenever stream is discarded.
	cancelStreamCtx context.CancelFunc
	closed          int32 // atomic
}

func newRPCConn(dialer *nodedialer.Dialer, producer *Sender, nodeID roachpb.NodeID) conn {
	r := &rpcConn{
		dialer:   dialer,
		producer: producer,
		nodeID:   nodeID,
	}
	r.AddLogTag("ctstream", nodeID)
	return r
}

// cleanupStream releases the resources associated with r.stream and marks the conn
// as needing a new stream.
func (r *rpcConn) cleanupStream() {
	if r.stream == nil {
		return
	}
	_ /* err */ = r.stream.CloseSend()
	r.stream = nil
	r.cancelStreamCtx()
	r.cancelStreamCtx = nil
}

// close makes the connection stop sending messages. The run() goroutine will
// exit asynchronously. The parent Sender is expected to remove this connection
// from its list.
func (r *rpcConn) close() {
	atomic.StoreInt32(&r.closed, 1)
}

func (r *rpcConn) sendMsg(ctx context.Context, msg *ctpb.Update) error {
	if r.stream == nil {
		conn, err := r.dialer.Dial(ctx, r.nodeID, rpc.SystemClass)
		if err != nil {
			return err
		}
		streamCtx, cancel := context.WithCancel(ctx)
		stream, err := ctpb.NewSideTransportClient(conn).PushUpdates(streamCtx)
		if err != nil {
			cancel()
			return err
		}
		r.stream = stream
		// This will need to be called when we're done with the stream.
		r.cancelStreamCtx = cancel
	}
	return r.stream.Send(msg)
}

// run implements the conn interface.
func (r *rpcConn) run(ctx context.Context, stopper *stop.Stopper) {
	_ /* err */ = stopper.RunAsyncTask(ctx, fmt.Sprintf("closedts publisher for n%d", r.nodeID),
		func(ctx context.Context) {
			ctx = r.AnnotateCtx(ctx)

			defer r.cleanupStream()
			everyN := log.Every(10 * time.Second)

			var lastSent ctpb.SeqNum
			for {
				var msg *ctpb.Update

				// If we've been disconnected, reset the message sequence. We'll ask the
				// buffer for the very first message ever, which was a snapshot.
				// Generally, the buffer is not going to have that message any more and
				// so we'll generate a snapshot below. Except soon after startup when
				// streams are initially established, when the initial message should
				// still be in the buffer.
				if r.stream == nil {
					lastSent = 0
				}

				var ok bool
				msg, ok = r.producer.buf.GetBySeq(ctx, lastSent+1)
				// We can be signaled to stop in two ways: the buffer can be closed (in
				// which case all connections must exit), or this connection was closed
				// via close(). In either case, we quit.
				if !ok {
					return
				}
				closed := atomic.LoadInt32(&r.closed) > 0
				if closed {
					return
				}

				if msg == nil {
					// The sequence number we've requested is no longer in the buffer.
					// We need to generate a snapshot in order to re-initialize the
					// stream.
					msg = r.producer.GetSnapshot()
				}

				lastSent = msg.SeqNum
				if err := r.sendMsg(ctx, msg); err != nil {
					if err != io.EOF && everyN.ShouldLog() {
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
					r.cleanupStream()
				}
			}
		})
}
