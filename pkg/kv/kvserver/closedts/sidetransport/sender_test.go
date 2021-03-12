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
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// mockReplica is a mock implementation of the Replica interface.
type mockReplica struct {
	storeID roachpb.StoreID
	rangeID roachpb.RangeID
	mu      struct {
		syncutil.Mutex
		desc roachpb.RangeDescriptor
	}

	canBump        bool
	cantBumpReason CantCloseReason
	lai            ctpb.LAI
	policy         roachpb.RangeClosedTimestampPolicy
}

var _ Replica = &mockReplica{}

func (m *mockReplica) StoreID() roachpb.StoreID    { return m.storeID }
func (m *mockReplica) GetRangeID() roachpb.RangeID { return m.rangeID }
func (m *mockReplica) BumpSideTransportClosed(
	_ context.Context, _ hlc.ClockTimestamp, _ [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp,
) BumpSideTransportClosedResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	reason := ReasonUnknown
	if !m.canBump {
		reason = m.cantBumpReason
	}
	return BumpSideTransportClosedResult{
		OK:         m.canBump,
		FailReason: reason,
		Desc:       &m.mu.desc,
		LAI:        m.lai,
		Policy:     m.policy,
	}
}

func (m *mockReplica) removeReplica(nid roachpb.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	replicas := m.mu.desc.Replicas()
	for _, rd := range replicas.Descriptors() {
		if rd.NodeID == nid {
			replicas.RemoveReplica(rd.NodeID, rd.StoreID)
			m.mu.desc.SetReplicas(replicas)
			return
		}
	}
	panic(fmt.Sprintf("replica not found for n%d", nid))
}

// mockConnFactory is a mock implementation of the connFactory interface.
type mockConnFactory struct{}

func (f *mockConnFactory) new(_ *Sender, nodeID roachpb.NodeID) conn {
	return &mockConn{nodeID: nodeID}
}

// mockConn is a mock implementation of the conn interface.
type mockConn struct {
	nodeID  roachpb.NodeID
	running bool
	closed  bool
}

func (c *mockConn) run(context.Context, *stop.Stopper) { c.running = true }
func (c *mockConn) close()                             { c.closed = true }
func (c *mockConn) getState() connState                { return connState{} }

func newMockSender(connFactory connFactory) (*Sender, *stop.Stopper) {
	stopper := stop.NewStopper()
	st := cluster.MakeTestingClusterSettings()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	s := newSenderWithConnFactory(stopper, st, clock, connFactory)
	s.nodeID = 1 // usually set in (*Sender).Run
	return s, stopper
}

func newMockReplica(id roachpb.RangeID, nodes ...roachpb.NodeID) *mockReplica {
	var desc roachpb.RangeDescriptor
	desc.RangeID = id
	for _, nodeID := range nodes {
		desc.AddReplica(nodeID, roachpb.StoreID(nodeID), roachpb.VOTER_FULL)
	}
	r := &mockReplica{
		storeID: 1,
		rangeID: id,
		canBump: true,
		lai:     5,
		policy:  roachpb.LAG_BY_CLUSTER_SETTING,
	}
	r.mu.desc = desc
	return r
}

func expGroupUpdates(s *Sender, now hlc.ClockTimestamp) []ctpb.Update_GroupUpdate {
	targetForPolicy := func(pol roachpb.RangeClosedTimestampPolicy) hlc.Timestamp {
		return closedts.TargetForPolicy(
			now,
			s.clock.MaxOffset(),
			closedts.TargetDuration.Get(&s.st.SV),
			closedts.LeadForGlobalReadsOverride.Get(&s.st.SV),
			closedts.SideTransportCloseInterval.Get(&s.st.SV),
			pol,
		)
	}
	return []ctpb.Update_GroupUpdate{
		{Policy: roachpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: targetForPolicy(roachpb.LAG_BY_CLUSTER_SETTING)},
		{Policy: roachpb.LEAD_FOR_GLOBAL_READS, ClosedTimestamp: targetForPolicy(roachpb.LEAD_FOR_GLOBAL_READS)},
	}
}

func TestSenderBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	connFactory := &mockConnFactory{}
	s, stopper := newMockSender(connFactory)
	defer stopper.Stop(ctx)

	// No leaseholders.
	now := s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.leaseholdersMu.leaseholders, 0)
	require.Len(t, s.connsMu.conns, 0)

	require.Equal(t, ctpb.SeqNum(1), s.trackedMu.lastSeqNum)
	up, ok := s.buf.GetBySeq(ctx, 1)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(1), up.SeqNum)
	require.True(t, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Nil(t, up.Removed)
	require.Nil(t, up.AddedOrUpdated)

	// Add a leaseholder that can close.
	r1 := newMockReplica(15, 1, 2, 3)
	s.RegisterLeaseholder(ctx, r1, 1)
	now = s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 1)
	require.Equal(t, map[roachpb.RangeID]trackedRange{
		15: {lai: 5, policy: roachpb.LAG_BY_CLUSTER_SETTING},
	}, s.trackedMu.tracked)
	require.Len(t, s.leaseholdersMu.leaseholders, 1)
	require.Len(t, s.connsMu.conns, 2)

	require.Equal(t, ctpb.SeqNum(2), s.trackedMu.lastSeqNum)
	up, ok = s.buf.GetBySeq(ctx, 2)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(2), up.SeqNum)
	require.Equal(t, false, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Nil(t, up.Removed)
	require.Equal(t, []ctpb.Update_RangeUpdate{
		{RangeID: 15, LAI: 5, Policy: roachpb.LAG_BY_CLUSTER_SETTING},
	}, up.AddedOrUpdated)

	c2, ok := s.connsMu.conns[2]
	require.True(t, ok)
	require.Equal(t, &mockConn{nodeID: 2, running: true, closed: false}, c2.(*mockConn))
	c3, ok := s.connsMu.conns[3]
	require.True(t, ok)
	require.Equal(t, &mockConn{nodeID: 3, running: true, closed: false}, c3.(*mockConn))

	// The leaseholder can not close the next timestamp.
	r1.canBump = false
	r1.cantBumpReason = ProposalsInFlight
	now = s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.leaseholdersMu.leaseholders, 1)
	require.Len(t, s.connsMu.conns, 2)
	require.Equal(t, 1, s.trackedMu.closingFailures[r1.cantBumpReason])

	require.Equal(t, ctpb.SeqNum(3), s.trackedMu.lastSeqNum)
	up, ok = s.buf.GetBySeq(ctx, 3)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(3), up.SeqNum)
	require.Equal(t, false, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Equal(t, []roachpb.RangeID{15}, up.Removed)
	require.Nil(t, up.AddedOrUpdated)

	// The leaseholder loses its lease.
	s.UnregisterLeaseholder(ctx, 1, 15)
	now = s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.leaseholdersMu.leaseholders, 0)
	require.Len(t, s.connsMu.conns, 0)

	require.Equal(t, ctpb.SeqNum(4), s.trackedMu.lastSeqNum)
	up, ok = s.buf.GetBySeq(ctx, 4)
	require.True(t, ok)
	require.Equal(t, roachpb.NodeID(1), up.NodeID)
	require.Equal(t, ctpb.SeqNum(4), up.SeqNum)
	require.Equal(t, false, up.Snapshot)
	require.Equal(t, expGroupUpdates(s, now), up.ClosedTimestamps)
	require.Nil(t, up.Removed)
	require.Nil(t, up.AddedOrUpdated)

	require.True(t, c2.(*mockConn).closed)
	require.True(t, c3.(*mockConn).closed)
}

func TestSenderConnectionChanges(t *testing.T) {
	// TODO: Two ranges.
	// Add follower for range 1: 2, 3.
	// - check conns to 2 and 3.
	// Add follower for range 2: 3, 4.
	// - check conns to 2, 3, 4.
	// Remove followers for range 2, 3.
	// - check conns to 3, 4.
	// Remove followers for range 3.
	// - check conns to 4.
}

func TestSenderSameRangeDifferentStores(t *testing.T) {
	// TODO: Two replicas, different stores, same replica.
}

// TODO(andrei): add test for updatesBuf.

// mockReceiver is a SideTransportServer.
type mockReceiver struct {
	stop chan struct{}
	mu   struct {
		syncutil.Mutex
		called bool
	}
}

var _ ctpb.SideTransportServer = &mockReceiver{}

// PushUpdates is the streaming RPC handler.
func (s *mockReceiver) PushUpdates(stream ctpb.SideTransport_PushUpdatesServer) error {
	s.mu.Lock()
	s.mu.called = true
	s.mu.Unlock()
	// Block the RPC until close() is called.
	<-s.stop
	return nil
}

func newMockReceiver() *mockReceiver {
	return &mockReceiver{
		stop: make(chan struct{}),
	}
}

func (s *mockReceiver) getCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.called
}

// sideTransportGRPCServer wraps a Receiver (a real one of a mock) in a gRPC
// server listening on a network interface.
type sideTransportGRPCServer struct {
	lis      net.Listener
	srv      *grpc.Server
	receiver ctpb.SideTransportServer
}

func (s *sideTransportGRPCServer) Close() {
	s.srv.Stop()
	_ /* err */ = s.lis.Close()
}

func (s *sideTransportGRPCServer) addr() net.Addr {
	return s.lis.Addr()
}

func newMockSideTransportGRPCServer(stopper *stop.Stopper) (*sideTransportGRPCServer, error) {
	receiver := newMockReceiver()
	stopper.AddCloser(receiver)
	server, err := newMockSideTransportGRPCServerWithOpts(stopper, receiver)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func newMockSideTransportGRPCServerWithOpts(
	stopper *stop.Stopper, receiver ctpb.SideTransportServer,
) (*sideTransportGRPCServer, error) {
	lis, err := net.Listen("tcp", "localhost:")
	if err != nil {
		return nil, err
	}

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	grpcServer := rpc.NewServer(rpc.NewInsecureTestingContext(clock, stopper))
	ctpb.RegisterSideTransportServer(grpcServer, receiver)
	go func() {
		_ /* err */ = grpcServer.Serve(lis)
	}()
	server := &sideTransportGRPCServer{
		lis:      lis,
		srv:      grpcServer,
		receiver: receiver,
	}
	stopper.AddCloser(server)
	return server, nil
}

func (s *sideTransportGRPCServer) mockReceiver() *mockReceiver {
	return s.receiver.(*mockReceiver)
}

func (s *mockReceiver) Close() {
	close(s.stop)
}

type mockDialer struct {
	mu struct {
		syncutil.Mutex
		addrs map[roachpb.NodeID]string
		conns []*grpc.ClientConn
	}
}

var _ nodeDialer = &mockDialer{}

type nodeAddr struct {
	nid  roachpb.NodeID
	addr string
}

func newMockDialer(addrs ...nodeAddr) *mockDialer {
	d := &mockDialer{}
	d.mu.addrs = make(map[roachpb.NodeID]string)
	for _, addr := range addrs {
		d.mu.addrs[addr.nid] = addr.addr
	}
	return d
}

func (m *mockDialer) addOrUpdateNode(nid roachpb.NodeID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.addrs[nid] = addr
}

func (m *mockDialer) Dial(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (_ *grpc.ClientConn, _ error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	addr, ok := m.mu.addrs[nodeID]
	if !ok {
		return nil, errors.Errorf("node not configured in mockDialer: n%d", nodeID)
	}

	c, err := grpc.Dial(addr, grpc.WithInsecure())
	if err == nil {
		m.mu.conns = append(m.mu.conns, c)
	}
	return c, err
}

func (m *mockDialer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.mu.conns {
		_ /* err */ = c.Close() // nolint:grpcconnclose
	}
}

// Test that the stopper quiescence interrupts a stream.Send.
func TestRPCConnUnblocksOnStopper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	srv, err := newMockSideTransportGRPCServer(stopper)
	require.NoError(t, err)
	dialer := newMockDialer(nodeAddr{
		nid:  2,
		addr: srv.addr().String(),
	})
	defer dialer.Close()

	ch := make(chan struct{})
	s, stopper := newMockSender(newRPCConnFactory(dialer,
		connTestingKnobs{beforeSend: func(_ roachpb.NodeID, msg *ctpb.Update) {
			// Try to send an update to ch, if anyone is still listening.
			ch <- struct{}{}
		}}))
	defer stopper.Stop(ctx)

	// Add leaseholders that can close, in order to establish a connection to n2.
	// We add many of them, and we'll increment their LAIs periodically such that
	// all messages need to explicitly mention all of them, in order to get large
	// messages. This speeds up the test, since the large messages make the sender
	// block quicker.
	const numReplicas = 10000
	replicas := make([]*mockReplica, numReplicas)
	for i := 0; i < numReplicas; i++ {
		replicas[i] = newMockReplica(roachpb.RangeID(i+1), 1, 2)
		s.RegisterLeaseholder(ctx, replicas[i], 1 /* leaseSeq */)
	}

	incrementLAIs := func() {
		for _, r := range replicas {
			r.lai++
		}
	}

	s.publish(ctx)
	require.Len(t, s.connsMu.conns, 1)

	// Now get the rpcConn to keep sending messages by calling s.publish()
	// repeatedly. We'll detect when the rpcConn is blocked (because the Receiver
	// is not reading any of the messages).
	senderBlocked := make(chan struct{})
	go func() {
		// Publish enough messages to fill up the network buffers and cause the
		// stream.Send() to block.
		for {
			select {
			case <-ch:
				// As soon as the conn send a message, publish another update to cause
				// the conn to send another message.
				incrementLAIs()
				s.publish(ctx)
			case <-time.After(100 * time.Millisecond):
				// The conn hasn't sent anything in a while. It must be blocked on Send.
				close(senderBlocked)
				return
			}
		}
	}()

	// Wait for the sender to appear blocked.
	<-senderBlocked

	// Stop the stopper. If this doesn't timeout, then the rpcConn's task must
	// have been unblocked.
	stopper.Stop(ctx)

	require.True(t, srv.mockReceiver().getCalled())
}

// Test a Sender and Receiver talking gRPC to each other.
func TestSenderReceiverIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// We're going to create Receivers, corresponding to 3 nodes. Node 1 will also
	// be the Sender, so we won't expect a connection to it (the Sender doesn't
	// connect to itself).
	const numNodes = 3
	receivers := make([]*Receiver, numNodes)
	dialer := newMockDialer(nodeAddr{})
	defer dialer.Close()
	incomingStreamOnN2FromN1Terminated := make(chan error)
	for i := 0; i < numNodes; i++ {
		receiverStop := stop.NewStopper()
		defer func(i int) {
			receiverStop.Stop(ctx)
		}(i)
		nid := &base.NodeIDContainer{}
		nid.Set(ctx, roachpb.NodeID(i+1))
		stores := &mockStores{}
		knobs := receiverTestingKnobs{
			roachpb.NodeID(1): {
				onFirstMsg: make(chan struct{}),
				onMsg:      make(chan *ctpb.Update),
			},
		}
		incomingFromN1Knobs := knobs[1]
		switch nid.Get() {
		case 1:
			// n1 doesn't expect any streams, since the only active sender will be on
			// n1 and it's not supposed to connect to the local receiver.
			incomingFromN1Knobs.onRecvErr = func(_ roachpb.NodeID, _ error) {
				t.Errorf("unexpected receive error on node n%d", nid)
			}
		case 2:
			// n2 gets a special handler.
			incomingFromN1Knobs.onRecvErr = func(_ roachpb.NodeID, err error) {
				incomingStreamOnN2FromN1Terminated <- err
			}
		}
		knobs[1] = incomingFromN1Knobs
		receivers[i] = NewReceiver(nid, receiverStop, stores, knobs)
		srv, err := newMockSideTransportGRPCServerWithOpts(receiverStop, receivers[i])
		dialer.addOrUpdateNode(nid.Get(), srv.addr().String())
		require.NoError(t, err)
	}

	s, senderStopper := newMockSender(newRPCConnFactory(dialer, connTestingKnobs{}))
	defer senderStopper.Stop(ctx)
	s.Run(ctx, roachpb.NodeID(1))

	// Add a replica with replicas on n2 and n3.
	r1 := newMockReplica(15, 1, 2, 3)
	s.RegisterLeaseholder(ctx, r1, 1 /* leaseSeq */)
	// Check that connections to n2,3 are established.
	<-receivers[1].testingKnobs[1].onFirstMsg
	<-receivers[2].testingKnobs[1].onFirstMsg
	// Remove one of the replicas and check that the connection to the respective
	// Receiver drops (since there's no other ranges with replicas on n2).
	r1.removeReplica(roachpb.NodeID(2))
	<-incomingStreamOnN2FromN1Terminated
	// Check that the other Receiver is still receiving updates.
	<-receivers[2].testingKnobs[1].onMsg
}
