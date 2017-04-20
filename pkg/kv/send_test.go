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
//
// Author: joezxy (joe.zxy@foxmail.com)

package kv

import (
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func newTestServer(t *testing.T, ctx *rpc.Context) (*grpc.Server, net.Listener) {
	s := rpc.NewServer(ctx)

	ln, err := netutil.ListenAndServeGRPC(ctx.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	return s, ln
}

type Node time.Duration

func (n Node) Batch(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if n > 0 {
		time.Sleep(time.Duration(n))
	}
	return &roachpb.BatchResponse{}, nil
}

func TestInvalidAddrLength(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The provided replicas is nil, so its length will be always less than the
	// specified response number
	ds := &DistSender{}
	ret, err := ds.sendToReplicas(context.Background(), SendOptions{}, 0, nil, roachpb.BatchRequest{}, nil)

	// the expected return is nil and SendError
	if _, ok := err.(*roachpb.SendError); !ok || ret != nil {
		t.Fatalf("Shorter replicas should return nil and SendError.")
	}
}

// TestSendToOneClient verifies that Send correctly sends a request
// to one server using the heartbeat RPC.
func TestSendToOneClient(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	rpcContext := rpc.NewContext(
		log.AmbientContext{},
		testutils.NewNodeTestBaseContext(),
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
	)
	s, ln := newTestServer(t, rpcContext)
	roachpb.RegisterInternalServer(s, Node(0))

	reply, err := sendBatch(context.Background(), SendOptions{}, []net.Addr{ln.Addr()}, rpcContext)
	if err != nil {
		t.Fatal(err)
	}
	if reply == nil {
		t.Errorf("expected reply")
	}
}

// channelSaveTransport captures the 'done' channels of every RPC it
// "sends".
type channelSaveTransport struct {
	ch        chan chan<- BatchCall
	remaining int
}

func (c *channelSaveTransport) IsExhausted() bool {
	return c.remaining <= 0
}

func (c *channelSaveTransport) SendNextTimeout(defaultTimeout time.Duration) (time.Duration, bool) {
	if c.IsExhausted() {
		return 0, false
	}
	return defaultTimeout, true
}

func (c *channelSaveTransport) SendNext(_ context.Context, done chan<- BatchCall) {
	c.remaining--
	c.ch <- done
}

func (*channelSaveTransport) MoveToFront(roachpb.ReplicaDescriptor) {
}

func (*channelSaveTransport) Close() {
}

// setupSendNextTest sets up a situation in which SendNextTimeout has
// caused RPCs to be sent to all three replicas simultaneously. The
// caller may then cause those RPCs to finish by writing to one of the
// 'done' channels in the first return value; the second returned
// channel will contain the final result of the send() call.
//
// TODO(bdarnell): all the 'done' channels are currently the same.
// Either give each call its own channel, return a list of (replica
// descriptor, channel) pair, or decide we don't care about
// distinguishing them and just send a single channel.
func setupSendNextTest(t *testing.T) ([]chan<- BatchCall, chan BatchCall, *stop.Stopper) {
	stopper := stop.NewStopper()
	nodeContext := rpc.NewContext(
		log.AmbientContext{},
		testutils.NewNodeTestBaseContext(),
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
	)

	addrs := []net.Addr{
		util.NewUnresolvedAddr("dummy", "1"),
		util.NewUnresolvedAddr("dummy", "2"),
		util.NewUnresolvedAddr("dummy", "3"),
	}

	doneChanChan := make(chan chan<- BatchCall, len(addrs))

	opts := SendOptions{
		SendNextTimeout: 1 * time.Millisecond,
		transportFactory: func(_ SendOptions,
			_ *rpc.Context,
			replicas ReplicaSlice,
			_ roachpb.BatchRequest,
		) (Transport, error) {
			return &channelSaveTransport{
				ch:        doneChanChan,
				remaining: len(replicas),
			}, nil
		},
	}

	sendChan := make(chan BatchCall, 1)
	go func() {
		// Send the batch. This will block until we signal one of the done
		// channels.
		br, err := sendBatch(context.Background(), opts, addrs, nodeContext)
		sendChan <- BatchCall{br, err}
	}()

	doneChans := make([]chan<- BatchCall, len(addrs))
	for i := range doneChans {
		// Note that this blocks until the replica has been contacted.
		doneChans[i] = <-doneChanChan
	}
	return doneChans, sendChan, stopper
}

// Test the behavior of SendNextTimeout when all servers are slow to
// respond (but successful).
func TestSendNext_AllSlow(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop(context.TODO())

	// Now that all replicas have been contacted, let one finish.
	doneChans[1] <- BatchCall{
		Reply: &roachpb.BatchResponse{
			BatchResponse_Header: roachpb.BatchResponse_Header{
				Now: hlc.Timestamp{Logical: 42},
			},
		},
		Err: nil,
	}

	// The RPC now completes successfully.
	bc := <-sendChan
	if bc.Err != nil {
		t.Fatal(bc.Err)
	}
	// Make sure the response we sent in is the one we get back.
	if bc.Reply.Now.Logical != 42 {
		t.Errorf("got unexpected response: %s", bc.Reply)
	}
}

// Test the behavior of SendNextTimeout when some servers return
// RPC errors but one succeeds.
func TestSendNext_RPCErrorThenSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop(context.TODO())

	// Now that all replicas have been contacted, let two finish with
	// retryable errors.
	for i := 1; i <= 2; i++ {
		doneChans[i] <- BatchCall{
			Reply: nil,
			Err:   roachpb.NewSendError("boom"),
		}
	}

	// The client is still waiting for the third slow RPC to complete.
	select {
	case bc := <-sendChan:
		t.Fatalf("got unexpected response %v", bc)
	default:
	}

	// Now let the final server complete the RPC successfully.
	doneChans[0] <- BatchCall{
		Reply: &roachpb.BatchResponse{},
		Err:   nil,
	}

	// The client side now completes successfully.
	bc := <-sendChan
	if bc.Err != nil {
		t.Fatal(bc.Err)
	}
}

// Test the behavior of SendNextTimeout when all servers return
// RPC errors (this is effectively the same whether
// SendNextTimeout is used or not).
func TestSendNext_AllRPCErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop(context.TODO())

	// All replicas finish with RPC errors.
	for i := 0; i <= 2; i++ {
		doneChans[i] <- BatchCall{
			Reply: nil,
			Err:   errors.New("boom"),
		}
	}

	// The client side completes with a retryable send error.
	bc := <-sendChan
	if _, ok := bc.Err.(*roachpb.SendError); !ok {
		t.Errorf("did not get expected SendError; got %T instead", bc.Err)
	}
}

func TestSendNext_RetryableApplicationErrorThenSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop(context.TODO())

	// One replica finishes with a retryable error.
	doneChans[1] <- BatchCall{
		Reply: &roachpb.BatchResponse{
			BatchResponse_Header: roachpb.BatchResponse_Header{
				Error: roachpb.NewError(roachpb.NewRangeNotFoundError(1)),
			},
		},
	}

	// A second replica finishes successfully.
	doneChans[2] <- BatchCall{
		Reply: &roachpb.BatchResponse{},
	}

	// The client send finishes with the second response.
	bc := <-sendChan
	if bc.Err != nil {
		t.Fatalf("unexpected RPC error: %s", bc.Err)
	}
	if bc.Reply.Error != nil {
		t.Errorf("expected successful reply, got %s", bc.Reply.Error)
	}
}

func TestSendNext_AllRetryableApplicationErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop(context.TODO())

	// All replicas finish with a retryable error.
	for _, ch := range doneChans {
		ch <- BatchCall{
			Reply: &roachpb.BatchResponse{
				BatchResponse_Header: roachpb.BatchResponse_Header{
					Error: roachpb.NewError(roachpb.NewRangeNotFoundError(1)),
				},
			},
		}
	}

	// The client send finishes with one of the errors, wrapped in a SendError.
	bc := <-sendChan
	if bc.Err == nil {
		t.Fatalf("expected SendError, got err=nil and reply=%s", bc.Reply)
	} else if _, ok := bc.Err.(*roachpb.SendError); !ok {
		t.Fatalf("expected SendError, got err=%s", bc.Err)
	} else if exp := "range 1 was not found"; !testutils.IsError(bc.Err, exp) {
		t.Errorf("expected SendError to contain %q, but got %v", exp, bc.Err)
	}
}

func TestSendNext_NonRetryableApplicationError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	doneChans, sendChan, stopper := setupSendNextTest(t)
	defer stopper.Stop(context.TODO())

	// One replica finishes with a non-retryable error.
	doneChans[1] <- BatchCall{
		Reply: &roachpb.BatchResponse{
			BatchResponse_Header: roachpb.BatchResponse_Header{
				Error: roachpb.NewError(roachpb.NewTransactionReplayError()),
			},
		},
	}

	// The client completes with that error, without waiting for the
	// others to finish.
	bc := <-sendChan
	if bc.Err != nil {
		t.Fatalf("expected error in payload, not rpc error %s", bc.Err)
	}
	if _, ok := bc.Reply.Error.GetDetail().(*roachpb.TransactionReplayError); !ok {
		t.Errorf("expected TransactionReplayError, got %v", bc.Reply.Error)
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

func (f *firstNErrorTransport) SendNextTimeout(defaultTimeout time.Duration) (time.Duration, bool) {
	if f.IsExhausted() {
		return 0, false
	}
	return defaultTimeout, true
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
		log.AmbientContext{},
		testutils.NewNodeTestBaseContext(),
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
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

		opts := SendOptions{
			transportFactory: func(
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
		}

		reply, err := sendBatch(context.Background(), opts, serverAddrs, nodeContext)
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
	ctx context.Context, opts SendOptions, addrs []net.Addr, rpcContext *rpc.Context,
) (*roachpb.BatchResponse, error) {
	ds := NewDistSender(DistSenderConfig{}, nil)
	opts.metrics = &ds.metrics
	return ds.sendToReplicas(ctx, opts, 0, makeReplicas(addrs...), roachpb.BatchRequest{}, rpcContext)
}
