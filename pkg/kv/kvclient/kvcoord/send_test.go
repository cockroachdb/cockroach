// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type Node time.Duration

func (n Node) Batch(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if n > 0 {
		time.Sleep(time.Duration(n))
	}
	return &roachpb.BatchResponse{}, nil
}

func (n Node) RangeFeed(_ *roachpb.RangeFeedRequest, _ roachpb.Internal_RangeFeedServer) error {
	panic("unimplemented")
}

// TestSendToOneClient verifies that Send correctly sends a request
// to one server using the heartbeat RPC.
func TestSendToOneClient(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	// This test uses the testing function sendBatch() which does not
	// support setting the node ID on GRPCDialNode(). Disable Node ID
	// checks to avoid log.Fatal.
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	s := rpc.NewServer(rpcContext)
	roachpb.RegisterInternalServer(s, Node(0))
	ln, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}
	nodeDialer := nodedialer.New(rpcContext, func(roachpb.NodeID) (net.Addr, error) {
		return ln.Addr(), nil
	})

	reply, err := sendBatch(context.Background(), nil, []net.Addr{ln.Addr()}, rpcContext, nodeDialer)
	if err != nil {
		t.Fatal(err)
	}
	if reply == nil {
		t.Errorf("expected reply")
	}
}

// firstNErrorTransport is a mock transport that sends an error on
// requests to the first N addresses, then succeeds.
type firstNErrorTransport struct {
	replicas  ReplicaSlice
	numErrors int
	numSent   int
}

func (f *firstNErrorTransport) IsExhausted() bool {
	return f.numSent >= len(f.replicas)
}

func (f *firstNErrorTransport) SendNext(
	_ context.Context, _ roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	var err error
	if f.numSent < f.numErrors {
		err = roachpb.NewSendError("test")
	}
	f.numSent++
	return &roachpb.BatchResponse{}, err
}

func (f *firstNErrorTransport) NextInternalClient(
	ctx context.Context,
) (context.Context, roachpb.InternalClient, error) {
	panic("unimplemented")
}

func (f *firstNErrorTransport) NextReplica() roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{}
}

func (*firstNErrorTransport) MoveToFront(roachpb.ReplicaDescriptor) {
}

// TestComplexScenarios verifies various complex success/failure scenarios by
// mocking sendOne.
func TestComplexScenarios(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	// We're going to serve multiple node IDs with that one
	// context. Disable node ID checks.
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true
	nodeDialer := nodedialer.New(rpcContext, nil)

	// TODO(bdarnell): the retryable flag is no longer used for RPC errors.
	// Rework this test to incorporate application-level errors carried in
	// the BatchResponse.
	testCases := []struct {
		numServers int
		numErrors  int
		success    bool
	}{
		// --- Success scenarios ---
		{1, 0, true},
		{5, 0, true},
		// There are some errors, but enough RPCs succeed.
		{5, 1, true},
		{5, 4, true},
		{5, 2, true},

		// --- Failure scenarios ---
		// All RPCs fail.
		{5, 5, false},
	}
	for i, test := range testCases {
		var serverAddrs []net.Addr
		for j := 0; j < test.numServers; j++ {
			serverAddrs = append(serverAddrs, util.NewUnresolvedAddr("dummy",
				strconv.Itoa(j)))
		}

		reply, err := sendBatch(
			context.Background(),
			func(
				_ SendOptions,
				_ *nodedialer.Dialer,
				replicas ReplicaSlice,
			) (Transport, error) {
				return &firstNErrorTransport{
					replicas:  replicas,
					numErrors: test.numErrors,
				}, nil
			},
			serverAddrs,
			rpcContext,
			nodeDialer,
		)
		if test.success {
			if err != nil {
				t.Errorf("%d: unexpected error: %s", i, err)
			}
			if reply == nil {
				t.Errorf("%d: expected reply", i)
			}
		} else {
			if err == nil {
				t.Errorf("%d: unexpected success", i)
			}
		}
	}
}

// TestSplitHealthy tests that the splitHealthy helper function sorts healthy
// nodes before unhealthy nodes.
func TestSplitHealthy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		in       []batchClient
		out      []batchClient
		nHealthy int
	}{
		{nil, nil, 0},
		{
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: false},
			},
			1,
		},
		{
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: false},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: false},
			},
			2,
		},
		{
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			[]batchClient{
				{replica: roachpb.ReplicaDescriptor{NodeID: 1}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 2}, healthy: true},
				{replica: roachpb.ReplicaDescriptor{NodeID: 3}, healthy: true},
			},
			3,
		},
	}

	for i, td := range testData {
		nHealthy := splitHealthy(td.in)
		if nHealthy != td.nHealthy {
			t.Errorf("%d. splitHealthy(%+v) = %d; not %d", i, td.in, nHealthy, td.nHealthy)
		}
		if !reflect.DeepEqual(td.in, td.out) {
			t.Errorf("%d. splitHealthy(...)\n  = %+v;\nnot %+v", i, td.in, td.out)
		}
	}
}

func makeReplicas(addrs ...net.Addr) ReplicaSlice {
	replicas := make(ReplicaSlice, len(addrs))
	for i, addr := range addrs {
		replicas[i].NodeDesc = &roachpb.NodeDescriptor{
			Address: util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		}
	}
	return replicas
}

// sendBatch sends Batch requests to specified addresses using send.
func sendBatch(
	ctx context.Context,
	transportFactory TransportFactory,
	addrs []net.Addr,
	rpcContext *rpc.Context,
	nodeDialer *nodedialer.Dialer,
) (*roachpb.BatchResponse, error) {
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		RPCContext: rpcContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: transportFactory,
		},
		Settings: cluster.MakeTestingClusterSettings(),
	}, nil)
	return ds.sendToReplicas(
		ctx,
		roachpb.BatchRequest{},
		SendOptions{metrics: &ds.metrics},
		0, /* rangeID */
		makeReplicas(addrs...),
		nodeDialer,
		roachpb.ReplicaDescriptor{},
		false, /* withCommit */
	)
}
