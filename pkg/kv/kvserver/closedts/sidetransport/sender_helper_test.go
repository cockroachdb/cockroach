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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/multiregion"
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
	locality        roachpb.Locality
	getNodeLocality func(nodeID roachpb.NodeID) roachpb.Locality
	storeID         roachpb.StoreID
	rangeID         roachpb.RangeID
	mu              struct {
		syncutil.Mutex
		desc roachpb.RangeDescriptor
	}

	canBump        bool
	cantBumpReason CantCloseReason
	lai            kvpb.LeaseAppliedIndex
	policy         roachpb.RangeClosedTimestampPolicy
}

func (m *mockReplica) GetLocalityProximity() roachpb.LocalityComparisonType {
	result := roachpb.LocalityComparisonType_UNDEFINED
	for _, peer := range m.mu.desc.InternalReplicas {
		peerLocality := m.getNodeLocality(peer.NodeID)
		distance, _, _ := m.locality.CompareWithLocality(peerLocality)
		if distance == roachpb.LocalityComparisonType_UNDEFINED {
			continue
		}
		// Return immediately if we find cross-region, since it has the highest precedence.
		if distance == roachpb.LocalityComparisonType_CROSS_REGION {
			return distance
		}
		// Update result if we find a higher precedence (lower value), but skip UNDEFINED.
		if result == roachpb.LocalityComparisonType_UNDEFINED || distance < result {
			result = distance
		}
	}
	return result
}

var _ Replica = &mockReplica{}

func (m *mockReplica) StoreID() roachpb.StoreID    { return m.storeID }
func (m *mockReplica) GetRangeID() roachpb.RangeID { return m.rangeID }
func (m *mockReplica) BumpSideTransportClosed(
	_ context.Context,
	_ hlc.ClockTimestamp,
	targetByPolicy map[ctpb.RangeClosedTimestampByPolicyLocality]hlc.Timestamp,
) BumpSideTransportClosedResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	reason := ReasonUnknown
	if !m.canBump {
		reason = m.cantBumpReason
	}
	_, policyLocality := GetTargetAndPolicyLocality(
		m.policy, m.GetLocalityProximity(), targetByPolicy)
	return BumpSideTransportClosedResult{
		OK:         m.canBump,
		FailReason: reason,
		Desc:       &m.mu.desc,
		LAI:        m.lai,
		Policy:     policyLocality,
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

func newMockSender(
	connFactory connFactory, latencyTracker *multiregion.LatencyTracker,
) (*Sender, *stop.Stopper) {
	stopper := stop.NewStopper()
	st := cluster.MakeTestingClusterSettings()
	clock := hlc.NewClockForTesting(nil)
	s := newSenderWithConnFactory(stopper, st, clock, connFactory, latencyTracker)
	s.nodeID = 1 // usually set in (*Sender).Run
	return s, stopper
}

var sameZoneLocality = roachpb.Locality{
	Tiers: []roachpb.Tier{
		{Key: "region", Value: "us-east"},
		{Key: "zone", Value: "us-east-1a"},
	},
}
var sameRegionDiffZoneLocality = roachpb.Locality{
	Tiers: []roachpb.Tier{
		{Key: "region", Value: "us-east"},
		{Key: "zone", Value: "us-east-1b"},
	},
}
var crossRegionLocality = roachpb.Locality{
	Tiers: []roachpb.Tier{
		{Key: "region", Value: "us-west"},
		{Key: "zone", Value: "us-west-1a"},
	},
}

var nodeDescs = map[roachpb.NodeID]roachpb.Locality{
	roachpb.NodeID(1): sameZoneLocality,
	roachpb.NodeID(2): sameRegionDiffZoneLocality,
	roachpb.NodeID(3): crossRegionLocality,
}

var getNodeDesc = func(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	return &roachpb.NodeDescriptor{
		NodeID:   nodeID,
		Locality: nodeDescs[nodeID],
	}, nil
}

var getNodeLocality = func(nodeID roachpb.NodeID) roachpb.Locality { return nodeDescs[nodeID] }

func newMockReplicaWithLocality(id roachpb.RangeID, nodes ...roachpb.NodeID) *mockReplica {
	var desc roachpb.RangeDescriptor
	desc.RangeID = id
	for _, nodeID := range nodes {
		desc.AddReplica(nodeID, roachpb.StoreID(nodeID), roachpb.VOTER_FULL)
	}
	r := &mockReplica{
		locality:        sameZoneLocality,
		getNodeLocality: getNodeLocality,
		storeID:         1,
		rangeID:         id,
		canBump:         true,
		lai:             5,
		policy:          roachpb.LEAD_FOR_GLOBAL_READS,
	}
	r.mu.desc = desc
	return r
}

func newMockReplica(id roachpb.RangeID, nodes ...roachpb.NodeID) *mockReplica {
	var desc roachpb.RangeDescriptor
	desc.RangeID = id
	for _, nodeID := range nodes {
		desc.AddReplica(nodeID, roachpb.StoreID(nodeID), roachpb.VOTER_FULL)
	}
	r := &mockReplica{
		locality: roachpb.Locality{},
		getNodeLocality: func(nodeID roachpb.NodeID) roachpb.Locality {
			return roachpb.Locality{}
		},
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
		locality: roachpb.Locality{},
		getNodeLocality: func(nodeID roachpb.NodeID) roachpb.Locality {
			return roachpb.Locality{}
		},
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
	targetForPolicy := func(pol ctpb.RangeClosedTimestampByPolicyLocality) hlc.Timestamp {
		return closedts.TargetForPolicy(
			now,
			s.clock.MaxOffset(),
			closedts.TargetDuration.Get(&s.st.SV),
			closedts.LeadForGlobalReadsOverride.Get(&s.st.SV),
			closedts.SideTransportCloseInterval.Get(&s.st.SV),
			s.getNetworkRTTByPolicyLocality(pol),
			closedTimestampPolicy(pol),
		)
	}
	if s.latencyTracker.Enabled() {
		return []ctpb.Update_GroupUpdate{
			{Policy: ctpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: targetForPolicy(ctpb.LAG_BY_CLUSTER_SETTING)},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY)},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_REGION)},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_ZONE, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_WITH_CROSS_ZONE)},
			{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_WITH_SAME_ZONE)},
		}
	}
	return []ctpb.Update_GroupUpdate{
		{Policy: ctpb.LAG_BY_CLUSTER_SETTING, ClosedTimestamp: targetForPolicy(ctpb.LAG_BY_CLUSTER_SETTING)},
		{Policy: ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY, ClosedTimestamp: targetForPolicy(ctpb.LEAD_FOR_GLOBAL_READS_WITH_NO_LOCALITY)},
	}
}

func noopLatencyTracker() *multiregion.LatencyTracker {
	getLatency := func(roachpb.NodeID) (time.Duration, bool) {
		return 0, false
	}
	return multiregion.NewLatencyTracker(roachpb.Locality{}, getLatency, getNodeDesc)
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
