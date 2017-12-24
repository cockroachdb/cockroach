// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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

// TestSendToOneClient verifies that Send correctly sends a request
// to one server using the heartbeat RPC.
func TestSendToOneClient(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		testutils.NewNodeTestBaseContext(),
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
		&cluster.MakeTestingClusterSettings().Version,
	)
	s := rpc.NewServer(rpcContext)
	roachpb.RegisterInternalServer(s, Node(0))
	ln, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	reply, err := sendBatch(context.Background(), nil, []net.Addr{ln.Addr()}, rpcContext)
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
	args      roachpb.BatchRequest
	numErrors int
	numSent   int
}

func (f *firstNErrorTransport) IsExhausted() bool {
	return f.numSent >= len(f.replicas)
}

func (f *firstNErrorTransport) GetPending() []roachpb.ReplicaDescriptor {
	return nil
}

func (f *firstNErrorTransport) SendNext(_ context.Context, done chan<- BatchCall) {
	call := BatchCall{
		Reply: &roachpb.BatchResponse{},
	}
	if f.numSent < f.numErrors {
		call.Err = roachpb.NewSendError("test")
	}
	f.numSent++
	done <- call
}

func (f *firstNErrorTransport) NextReplica() roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{}
}

func (*firstNErrorTransport) MoveToFront(roachpb.ReplicaDescriptor) {
}

func (*firstNErrorTransport) Close() {
}

// TestComplexScenarios verifies various complex success/failure scenarios by
// mocking sendOne.
func TestComplexScenarios(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	nodeContext := rpc.NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		testutils.NewNodeTestBaseContext(),
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
		&cluster.MakeTestingClusterSettings().Version,
	)

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
				_ *rpc.Context,
				replicas ReplicaSlice,
				args roachpb.BatchRequest,
			) (Transport, error) {
				return &firstNErrorTransport{
					replicas:  replicas,
					args:      args,
					numErrors: test.numErrors,
				}, nil
			},
			serverAddrs,
			nodeContext,
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
				{remoteAddr: "1", healthy: false},
				{remoteAddr: "2", healthy: false},
				{remoteAddr: "3", healthy: true},
			},
			[]batchClient{
				{remoteAddr: "3", healthy: true},
				{remoteAddr: "1", healthy: false},
				{remoteAddr: "2", healthy: false},
			},
			1,
		},
		{
			[]batchClient{
				{remoteAddr: "1", healthy: true},
				{remoteAddr: "2", healthy: false},
				{remoteAddr: "3", healthy: true},
			},
			[]batchClient{
				{remoteAddr: "1", healthy: true},
				{remoteAddr: "3", healthy: true},
				{remoteAddr: "2", healthy: false},
			},
			2,
		},
		{
			[]batchClient{
				{remoteAddr: "1", healthy: true},
				{remoteAddr: "2", healthy: true},
				{remoteAddr: "3", healthy: true},
			},
			[]batchClient{
				{remoteAddr: "1", healthy: true},
				{remoteAddr: "2", healthy: true},
				{remoteAddr: "3", healthy: true},
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
	ctx context.Context, transportFactory TransportFactory, addrs []net.Addr, rpcContext *rpc.Context,
) (*roachpb.BatchResponse, error) {
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: transportFactory,
		},
	}, nil)
	return ds.sendToReplicas(ctx, SendOptions{metrics: &ds.metrics}, 0, makeReplicas(addrs...), roachpb.BatchRequest{}, rpcContext)
}
