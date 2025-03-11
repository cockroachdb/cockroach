// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sidetransport

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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
	lai            kvpb.LeaseAppliedIndex
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
	clock := hlc.NewClockForTesting(nil)
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

func newMockReplicaEx(id roachpb.RangeID, replicas ...roachpb.ReplicationTarget) *mockReplica {
	var desc roachpb.RangeDescriptor
	desc.RangeID = id
	for _, r := range replicas {
		desc.AddReplica(r.NodeID, r.StoreID, roachpb.VOTER_FULL)
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

// mockReceiver is a SideTransportServer.
type mockReceiver struct {
	stop     chan struct{}
	called   atomic.Bool
	calledCh chan struct{}
}

var _ ctpb.SideTransportServer = &mockReceiver{}

// PushUpdates is the streaming RPC handler.
func (s *mockReceiver) PushUpdates(stream ctpb.SideTransport_PushUpdatesServer) error {
	if s.called.CompareAndSwap(false, true) {
		close(s.calledCh)
	}
	// Block the RPC until close() is called.
	<-s.stop
	return nil
}

func newMockReceiver() *mockReceiver {
	return &mockReceiver{
		stop:     make(chan struct{}),
		calledCh: make(chan struct{}),
	}
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

func newMockSideTransportGRPCServer(
	ctx context.Context, stopper *stop.Stopper,
) (*sideTransportGRPCServer, error) {
	receiver := newMockReceiver()
	if err := stopper.RunAsyncTask(ctx, "stopper-watcher", func(ctx context.Context) {
		// We can't use a Closer since the receiver will be blocking inside of a task.
		<-stopper.ShouldQuiesce()
		receiver.Close()
	}); err != nil {
		return nil, err
	}
	server, err := newMockSideTransportGRPCServerWithOpts(ctx, stopper, receiver)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func newMockSideTransportGRPCServerWithOpts(
	ctx context.Context, stopper *stop.Stopper, receiver ctpb.SideTransportServer,
) (*sideTransportGRPCServer, error) {
	lis, err := net.Listen("tcp", "localhost:")
	if err != nil {
		return nil, err
	}

	clock := hlc.NewClockForTesting(nil)
	grpcServer, err := rpc.NewServer(ctx, rpc.NewInsecureTestingContext(ctx, clock, stopper))
	if err != nil {
		return nil, err
	}
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
	//lint:ignore SA1019 grpc.WithInsecure is deprecated
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

type failingDialer struct {
	dialCount int32
}

var _ nodeDialer = &failingDialer{}

func (f *failingDialer) Dial(
	ctx context.Context, nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (_ *grpc.ClientConn, err error) {
	atomic.AddInt32(&f.dialCount, 1)
	return nil, errors.New("failingDialer")
}

func (f *failingDialer) callCount() int32 {
	return atomic.LoadInt32(&f.dialCount)
}
