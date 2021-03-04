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
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// mockReplica is a mock implementation of the Replica interface.
type mockReplica struct {
	storeID roachpb.StoreID
	rangeID roachpb.RangeID
	desc    roachpb.RangeDescriptor

	canBump bool
	lai     ctpb.LAI
	policy  roachpb.RangeClosedTimestampPolicy
}

func (m *mockReplica) StoreID() roachpb.StoreID       { return m.storeID }
func (m *mockReplica) GetRangeID() roachpb.RangeID    { return m.rangeID }
func (m *mockReplica) Desc() *roachpb.RangeDescriptor { return &m.desc }
func (m *mockReplica) BumpSideTransportClosed(
	_ context.Context, _ hlc.ClockTimestamp, _ [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp,
) (bool, ctpb.LAI, roachpb.RangeClosedTimestampPolicy) {
	return m.canBump, m.lai, m.policy
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
	return &mockReplica{
		storeID: 1,
		rangeID: id,
		desc:    desc,
		canBump: true,
		lai:     5,
		policy:  roachpb.LAG_BY_CLUSTER_SETTING,
	}
}

func expGroupUpdates(s *Sender, now hlc.ClockTimestamp) []ctpb.Update_GroupUpdate {
	maxClockOffset := s.clock.MaxOffset()
	lagTargetDuration := closedts.TargetDuration.Get(&s.st.SV)
	targetForPolicy := func(pol roachpb.RangeClosedTimestampPolicy) hlc.Timestamp {
		return closedts.TargetForPolicy(now, maxClockOffset, lagTargetDuration, pol)
	}
	return []ctpb.Update_GroupUpdate{
		{Policy: roachpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: targetForPolicy(roachpb.LAG_BY_CLUSTER_SETTING)},
		{Policy: roachpb.LEAD_FOR_GLOBAL_READS, ClosedTimestamp: targetForPolicy(roachpb.LEAD_FOR_GLOBAL_READS)},
	}
}

func TestSenderBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	connFactory := &mockConnFactory{}
	s, stopper := newMockSender(connFactory)
	defer stopper.Stop(ctx)

	// No leaseholders.
	now := s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.leaseholdersMu.leaseholders, 0)
	require.Len(t, s.conns, 0)

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
	require.Len(t, s.conns, 2)

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

	c2, ok := s.conns[2]
	require.True(t, ok)
	require.Equal(t, &mockConn{nodeID: 2, running: true, closed: false}, c2.(*mockConn))
	c3, ok := s.conns[3]
	require.True(t, ok)
	require.Equal(t, &mockConn{nodeID: 3, running: true, closed: false}, c3.(*mockConn))

	// The leaseholder can not close the next timestamp.
	r1.canBump = false
	now = s.publish(ctx)
	require.Len(t, s.trackedMu.tracked, 0)
	require.Len(t, s.leaseholdersMu.leaseholders, 1)
	require.Len(t, s.conns, 2)

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
	require.Len(t, s.conns, 0)

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

// mockSideTransportGRPCServer wraps a mockReceiver in a gRPC server listening
// on a network interface.
type mockSideTransportGRPCServer struct {
	lis      net.Listener
	srv      *grpc.Server
	receiver *mockReceiver
	stopper  *stop.Stopper
}

func (s *mockSideTransportGRPCServer) close() {
	s.receiver.close()
	s.srv.Stop()
	_ /* err */ = s.lis.Close()
	s.stopper.Stop(context.Background())
}

func (s *mockSideTransportGRPCServer) addr() net.Addr {
	return s.lis.Addr()
}

func newMockSideTransportGRPCServer() (*mockSideTransportGRPCServer, error) {
	lis, err := net.Listen("tcp", "localhost:")
	if err != nil {
		return nil, err
	}

	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	grpcServer := rpc.NewServer(rpc.NewInsecureTestingContext(clock, stopper))

	receiver := newMockReceiver()
	ctpb.RegisterSideTransportServer(grpcServer, receiver)
	go func() {
		_ /* err */ = grpcServer.Serve(lis)
	}()
	return &mockSideTransportGRPCServer{
		lis:      lis,
		srv:      grpcServer,
		receiver: receiver,
		stopper:  stopper,
	}, nil
}

func (s *mockReceiver) close() {
	close(s.stop)
}

type mockDialer struct {
	serverAddr string
	mu         struct {
		syncutil.Mutex
		conns []*grpc.ClientConn
	}
}

var _ nodeDialer = &mockDialer{}

func (m *mockDialer) Dial(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (_ *grpc.ClientConn, _ error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, err := grpc.Dial(m.serverAddr, grpc.WithInsecure())
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
	ctx := context.Background()

	srv, err := newMockSideTransportGRPCServer()
	require.NoError(t, err)
	defer srv.close()
	dialer := &mockDialer{serverAddr: srv.addr().String()}
	defer dialer.Close()

	ch := make(chan struct{})
	s, stopper := newMockSender(newRPCConnFactory(dialer,
		connTestingKnobs{beforeSend: func(_ roachpb.NodeID, msg *ctpb.Update) {
			// Try to send an update to ch, if anyone is still listening.
			ch <- struct{}{}
		}}))
	defer stopper.Stop(ctx)

	// Add a leaseholder that can close, in order to establish a connection to n2.
	r1 := newMockReplica(15, 1, 2)
	s.RegisterLeaseholder(ctx, r1, 1)
	s.publish(ctx)
	require.Len(t, s.conns, 1)

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

	require.True(t, srv.receiver.getCalled())
}
