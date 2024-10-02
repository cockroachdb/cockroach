// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// maxDelay simulates goroutine scheduling delays.
var maxDelay = 10 * time.Millisecond

// testMessageHandler stores all received messages in a channel.
type testMessageHandler struct {
	messages chan *slpb.Message
}

func newMessageHandler(size int) testMessageHandler {
	return testMessageHandler{
		messages: make(chan *slpb.Message, size),
	}
}

func (tmh *testMessageHandler) HandleMessage(msg *slpb.Message) error {
	// Simulate a message handling delay.
	time.Sleep(time.Duration(rand.Int63n(int64(maxDelay))))
	select {
	case tmh.messages <- msg:
		return nil
	default:
		return receiveQueueSizeLimitReachedErr
	}
}

var _ MessageHandler = (*testMessageHandler)(nil)

// clockWithManualSource is a pair of clocks: a manual clock and a clock that
// uses the manual clock as a source.
type clockWithManualSource struct {
	manual *hlc.HybridManualClock
	clock  *hlc.Clock
}

// transportTester contains objects needed to test the Store Liveness Transport.
// Typical usage will add multiple nodes with AddNode, add multiple stores with
// AddStore, and send messages with SendAsync.
type transportTester struct {
	t              testing.TB
	st             *cluster.Settings
	stopper        *stop.Stopper
	gossip         *gossip.Gossip
	nodeRPCContext *rpc.Context
	clocks         map[roachpb.NodeID]clockWithManualSource
	transports     map[roachpb.NodeID]*Transport
	maxHandlerSize int
}

func newTransportTester(t testing.TB, st *cluster.Settings) *transportTester {
	ctx := context.Background()
	tt := &transportTester{
		t:              t,
		st:             st,
		stopper:        stop.NewStopper(),
		clocks:         map[roachpb.NodeID]clockWithManualSource{},
		transports:     map[roachpb.NodeID]*Transport{},
		maxHandlerSize: maxReceiveQueueSize,
	}

	opts := rpc.DefaultContextOptions()
	opts.Stopper = tt.stopper
	opts.Settings = st
	opts.Insecure = true
	tt.nodeRPCContext = rpc.NewContext(ctx, opts)

	// We are sharing the same RPC context for all simulated nodes, so
	// we can't enforce some RPC check validation.
	tt.nodeRPCContext.TestingAllowNamedRPCToAnonymousServer = true

	tt.gossip = gossip.NewTest(1, tt.stopper, metric.NewRegistry())

	return tt
}

func (tt *transportTester) Stop() {
	tt.stopper.Stop(context.Background())
}

// AddNodeWithoutGossip creates new Transport for the node but doesn't gossip
// the node's address. Instead, it returns the node's address, which can be
// gossiped later.
func (tt *transportTester) AddNodeWithoutGossip(
	nodeID roachpb.NodeID, stopper *stop.Stopper,
) net.Addr {
	manual := hlc.NewHybridManualClock()
	clock := hlc.NewClockForTesting(manual)
	tt.clocks[nodeID] = clockWithManualSource{manual: manual, clock: clock}
	grpcServer, err := rpc.NewServer(context.Background(), tt.nodeRPCContext)
	require.NoError(tt.t, err)
	transport := NewTransport(
		log.MakeTestingAmbientCtxWithNewTracer(),
		tt.stopper,
		clock,
		nodedialer.New(tt.nodeRPCContext, gossip.AddressResolver(tt.gossip)),
		grpcServer,
		nil, /* knobs */
	)
	tt.transports[nodeID] = transport

	listener, err := netutil.ListenAndServeGRPC(stopper, grpcServer, util.TestAddr)
	require.NoError(tt.t, err)

	return listener.Addr()
}

// AddNode registers a node with the cluster. The node is gossiped immediately.
func (tt *transportTester) AddNode(nodeID roachpb.NodeID) {
	address := tt.AddNodeWithoutGossip(nodeID, tt.stopper)
	tt.UpdateGossip(nodeID, address)
}

func (tt *transportTester) UpdateGossip(nodeID roachpb.NodeID, address net.Addr) {
	if err := tt.gossip.AddInfoProto(
		gossip.MakeNodeIDKey(nodeID),
		&roachpb.NodeDescriptor{
			NodeID:  nodeID,
			Address: util.MakeUnresolvedAddr(address.Network(), address.String()),
		},
		time.Hour,
	); err != nil {
		tt.t.Fatal(err)
	}
}

// AddStore registers a store on a node and returns a message handler for
// messages sent to that store.
func (tt *transportTester) AddStore(id slpb.StoreIdent) testMessageHandler {
	handler := newMessageHandler(tt.maxHandlerSize)
	tt.transports[id.NodeID].ListenMessages(id.StoreID, &handler)
	return handler
}

// TestTransportSendAndReceive tests the basic send-and-receive functionality of
// Transport. The test sets up two nodes with two stores each, sends messages
// between all pairs of stores, and ensures the messages are received.
func TestTransportSendAndReceive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tt := newTransportTester(t, cluster.MakeTestingClusterSettings())
	defer tt.Stop()

	// Node 1: stores 1, 2
	// Node 2: stores 3, 4
	node1, node2 := roachpb.NodeID(1), roachpb.NodeID(2)
	store1 := slpb.StoreIdent{NodeID: node1, StoreID: roachpb.StoreID(1)}
	store2 := slpb.StoreIdent{NodeID: node1, StoreID: roachpb.StoreID(2)}
	store3 := slpb.StoreIdent{NodeID: node2, StoreID: roachpb.StoreID(3)}
	store4 := slpb.StoreIdent{NodeID: node2, StoreID: roachpb.StoreID(4)}
	stores := []slpb.StoreIdent{store1, store2, store3, store4}
	handlers := make(map[slpb.StoreIdent]testMessageHandler)

	tt.AddNode(node1)
	tt.AddNode(node2)
	for _, store := range stores {
		handlers[store] = tt.AddStore(store)
	}

	makeMsg := func(from slpb.StoreIdent, to slpb.StoreIdent) slpb.Message {
		return slpb.Message{Type: slpb.MsgHeartbeat, From: from, To: to}
	}

	// Send messages between each pair of stores.
	for _, from := range stores {
		for _, to := range stores {
			tt.transports[from.NodeID].SendAsync(ctx, makeMsg(from, to))
		}
	}

	// Assert that each store received messages from all other stores.
	for recipient, handler := range handlers {
		var senders []slpb.StoreIdent
		for len(senders) < len(stores) {
			testutils.SucceedsSoon(
				t, func() error {
					select {
					case msg := <-handler.messages:
						senders = append(senders, msg.From)
						require.Equal(t, recipient, msg.To)
						return nil
					default:
					}
					return errors.New("still waiting to receive messages")
				},
			)
		}
		require.ElementsMatch(t, stores, senders)
	}
	// There are two stores per node, so we expect the number of messages sent and
	// received by each node to be equal to twice the number of stores.
	require.Equal(t, 2*int64(len(stores)), tt.transports[node1].metrics.MessagesSent.Count())
	require.Equal(t, 2*int64(len(stores)), tt.transports[node2].metrics.MessagesReceived.Count())
}

// TestTransportRestartedNode simulates a node restart by stopping a node's
// Transport and replacing it with a new one. The test sends messages between a
// single sender and a single receiver, and includes 4 parts:
//
//  1. The receiver's address hasn't been gossiped yet.
//  2. The receiver successfully gets the message after its address is gossiped.
//  3. The receiver is crashed, and the sender eventually detects that via the
//     node dialer circuit breaker.
//  4. The receiver is replaced with a new Transport, and messages are delivered successfully.
func TestTransportRestartedNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tt := newTransportTester(t, cluster.MakeClusterSettings())
	defer tt.Stop()

	sender := slpb.StoreIdent{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)}
	tt.AddNode(sender.NodeID)
	tt.AddStore(sender)

	receiver := slpb.StoreIdent{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)}
	// Use a separate stopper for the receiver so that we can fully stop it
	// (releasing its bound network address) independently of the sender and the
	// entire test.
	receiverStopper := stop.NewStopper()
	addr := tt.AddNodeWithoutGossip(receiver.NodeID, receiverStopper)
	handler := tt.AddStore(receiver)

	msg := slpb.Message{Type: slpb.MsgHeartbeat, From: sender, To: receiver}

	checkEnqueued := func(expectedEnqueued bool) {
		testutils.SucceedsSoon(
			t, func() error {
				enqueued := tt.transports[sender.NodeID].SendAsync(ctx, msg)
				if enqueued != expectedEnqueued {
					return errors.Newf("enqueue success is still %v", enqueued)
				}
				return nil
			},
		)
	}

	checkSent := func() {
		initialSent := tt.transports[sender.NodeID].metrics.MessagesSent.Count()
		testutils.SucceedsSoon(
			t, func() error {
				tt.transports[sender.NodeID].SendAsync(ctx, msg)
				sent := tt.transports[sender.NodeID].metrics.MessagesSent.Count()
				if initialSent >= sent {
					return errors.Newf("message not sent yet; initial %d, current %d", initialSent, sent)
				}
				return nil
			},
		)
	}

	checkDropped := func() {
		initialDropped := tt.transports[sender.NodeID].metrics.MessagesSendDropped.Count()
		testutils.SucceedsSoon(
			t, func() error {
				tt.transports[sender.NodeID].SendAsync(ctx, msg)
				dropped := tt.transports[sender.NodeID].metrics.MessagesSendDropped.Count()
				if initialDropped >= dropped {
					return errors.Newf(
						"message not dropped yet; initial %d, current %d", initialDropped, dropped,
					)
				}
				return nil
			},
		)
	}

	checkReceived := func() {
		testutils.SucceedsSoon(
			t, func() error {
				select {
				case received := <-handler.messages:
					require.Equal(t, msg, *received)
					return nil
				default:
					// To ensure messages start getting delivered, keep sending messages
					// out. Even after SendAsync returns true, messages may still not be
					// delivered (e.g. if the receiver node is not up yet).
					tt.transports[sender.NodeID].SendAsync(ctx, msg)
				}
				return errors.New("still waiting to receive message")
			},
		)
	}

	// Part 1: send a message to the receiver whose address hasn't been gossiped yet.
	// The message is sent out successfully.
	checkEnqueued(true /* expectedEnqueued */)
	// The message sent as part of checkSend above will likely be dropped it's
	// also possible that the SendAsync races with the deletion of the send queue
	// (due to the failed node dial), in which case a dropped message will not be
	// recorded.
	checkDropped()

	// Part 2: send messages to the receiver, whose address is now gossiped, and
	// assert the messages are received.
	tt.UpdateGossip(receiver.NodeID, addr)
	checkEnqueued(true /* expectedEnqueued */)
	checkSent()
	checkReceived()

	// Part 3: send messages to the crashed receiver and ensure the message send
	// fails after the circuit breaker kicks in.
	receiverStopper.Stop(context.Background())
	checkEnqueued(false /* expectedEnqueued */)
	// Subsequent calls to SendAsync are expected to result in messages being
	// dropped due to the tripped circuit breaker.
	checkDropped()

	// Part 4: send messages to the restarted/replaced receiver; ensure the
	// message send succeeds (after the circuit breaker un-trips) and the messages
	// are received.
	tt.AddNode(receiver.NodeID)
	tt.AddStore(receiver)
	checkEnqueued(true /* expectedEnqueued */)
	checkSent()
	checkReceived()
}

// TestTransportSendToMissingStore verifies that sending a message to a store
// that doesn't exist on a given node doesn't affect the ability of other stores
// on the node to receive messages.
func TestTransportSendToMissingStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tt := newTransportTester(t, cluster.MakeClusterSettings())
	defer tt.Stop()

	sender := slpb.StoreIdent{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)}
	tt.AddNode(sender.NodeID)
	tt.AddStore(sender)

	tt.AddNode(roachpb.NodeID(2))
	// Store 2 registers a handler with the Transport on node 2.
	existingRcv := slpb.StoreIdent{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)}
	handler := tt.AddStore(existingRcv)

	// Store 3 does not register a handler with the Transport on node 2.
	// To the Transport on node 2, store 3 does not exist.
	missingRcv := slpb.StoreIdent{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(3)}

	missingMsg := slpb.Message{Type: slpb.MsgHeartbeat, From: sender, To: missingRcv}
	existingMsg := slpb.Message{Type: slpb.MsgHeartbeat, From: sender, To: existingRcv}

	// Send the message to the missing store first to ensure it doesn't affect the
	// receipt of the message to the existing store.
	require.True(t, tt.transports[sender.NodeID].SendAsync(ctx, missingMsg))
	require.True(t, tt.transports[sender.NodeID].SendAsync(ctx, existingMsg))

	// Wait for the message to the existing store to be received.
	testutils.SucceedsSoon(
		t, func() error {
			select {
			case received := <-handler.messages:
				require.Equal(t, existingMsg, *received)
				require.Equal(
					t, int64(1), tt.transports[existingRcv.NodeID].metrics.MessagesReceived.Count(),
				)
				return nil
			default:
			}
			return errors.New("still waiting to receive message")
		},
	)
	require.Equal(t, int64(2), tt.transports[sender.NodeID].metrics.MessagesSent.Count())
}

// TestTransportClockPropagation verifies that the HLC clock timestamps are
// propagated and updated via Transport messages. The test sends a message
// between a single sender and a single receiver, and ensures the receiver
// forwards its clock to the sender's clock.
func TestTransportClockPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tt := newTransportTester(t, cluster.MakeTestingClusterSettings())
	defer tt.Stop()

	sender := slpb.StoreIdent{NodeID: roachpb.NodeID(1), StoreID: roachpb.StoreID(1)}
	tt.AddNode(sender.NodeID)
	tt.AddStore(sender)

	receiver := slpb.StoreIdent{NodeID: roachpb.NodeID(2), StoreID: roachpb.StoreID(2)}
	tt.AddNode(receiver.NodeID)
	handler := tt.AddStore(receiver)

	senderClock := tt.clocks[sender.NodeID]
	receiverClock := tt.clocks[receiver.NodeID]

	// Pause both clocks.
	senderClock.manual.Pause()
	receiverClock.manual.Pause()

	// Advance the sender's clock beyond the receiver's clock.
	receiverTime := receiverClock.clock.Now()
	var senderTime hlc.Timestamp
	for senderTime.LessEq(receiverTime) {
		senderClock.manual.Increment(1000000)
		senderTime = senderClock.clock.Now()
	}
	require.NotEqual(t, senderClock.clock.Now(), receiverClock.clock.Now())

	// Send a message from the sender to the receiver.
	msg := slpb.Message{Type: slpb.MsgHeartbeat, From: sender, To: receiver}
	require.True(t, tt.transports[sender.NodeID].SendAsync(ctx, msg))

	// Wait for the message to be received.
	testutils.SucceedsSoon(
		t, func() error {
			select {
			case received := <-handler.messages:
				require.Equal(t, msg, *received)
				return nil
			default:
			}
			return errors.New("still waiting to receive message")
		},
	)

	// Check that the receiver's clock is equal to the sender's clock.
	require.Equal(t, senderClock.clock.Now(), receiverClock.clock.Now())
}

// TestTransportShortCircuit tests that a message from one local store to
// another local store short-circuits the dialer and is handled directly by the
// recipient's message handler.
func TestTransportShortCircuit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tt := newTransportTester(t, cluster.MakeTestingClusterSettings())
	defer tt.Stop()

	// Node 1: stores 1, 2
	// Node 2: store 3
	node1, node2 := roachpb.NodeID(1), roachpb.NodeID(2)
	store1 := slpb.StoreIdent{NodeID: node1, StoreID: roachpb.StoreID(1)}
	store2 := slpb.StoreIdent{NodeID: node1, StoreID: roachpb.StoreID(2)}
	store3 := slpb.StoreIdent{NodeID: node2, StoreID: roachpb.StoreID(3)}

	tt.AddNode(node1)
	tt.AddNode(node2)
	tt.AddStore(store1)
	handler := tt.AddStore(store2)
	tt.AddStore(store3)

	// Reach in and set node 1's dialer to nil. If SendAsync attempts to dial a
	// node, it will panic.
	tt.transports[node1].dialer = nil

	// Send messages between two stores on the same node.
	tt.transports[store1.NodeID].SendAsync(
		ctx, slpb.Message{Type: slpb.MsgHeartbeat, From: store1, To: store2},
	)
	// The message is received.
	testutils.SucceedsSoon(
		t, func() error {
			select {
			case msg := <-handler.messages:
				require.Equal(t, store1, msg.From)
				require.Equal(t, store2, msg.To)
				return nil
			default:
			}
			return errors.New("still waiting to receive message")
		},
	)

	// Send messages between two stores on different nodes. With a nil dialer,
	// we expect a panic.
	require.Panics(
		t, func() {
			tt.transports[store1.NodeID].SendAsync(
				ctx, slpb.Message{Type: slpb.MsgHeartbeat, From: store1, To: store3},
			)
		}, "sending message to a remote store with a nil dialer",
	)
}

// TestTransportIdleSendQueue tests that the send queue idles out.
func TestTransportIdleSendQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tt := newTransportTester(t, cluster.MakeTestingClusterSettings())
	defer tt.Stop()

	node1, node2 := roachpb.NodeID(1), roachpb.NodeID(2)
	sender := slpb.StoreIdent{NodeID: node1, StoreID: roachpb.StoreID(1)}
	receiver := slpb.StoreIdent{NodeID: node2, StoreID: roachpb.StoreID(2)}
	msg := slpb.Message{Type: slpb.MsgHeartbeat, From: sender, To: receiver}

	tt.AddNode(node1)
	tt.AddNode(node2)
	tt.AddStore(sender)
	handler := tt.AddStore(receiver)

	tt.transports[sender.NodeID].knobs.OverrideIdleTimeout = func() time.Duration {
		return time.Millisecond
	}

	// Send and receive a message.
	require.True(t, tt.transports[sender.NodeID].SendAsync(ctx, msg))
	testutils.SucceedsSoon(
		t, func() error {
			select {
			case received := <-handler.messages:
				require.Equal(t, msg, *received)
				return nil
			default:
			}
			return errors.New("still waiting to receive message")
		},
	)

	testutils.SucceedsSoon(
		t, func() error {
			if tt.transports[sender.NodeID].metrics.SendQueueIdle.Count() != int64(1) {
				return errors.New("idle queue metrics not incremented yet")
			}
			return nil
		},
	)
}

// TestTransportFullReceiveQueue tests that messages are dropped when the
// receive queue reaches its max size.
func TestTransportFullReceiveQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tt := newTransportTester(t, cluster.MakeTestingClusterSettings())
	tt.maxHandlerSize = 100
	defer tt.Stop()

	node1, node2 := roachpb.NodeID(1), roachpb.NodeID(2)
	sender := slpb.StoreIdent{NodeID: node1, StoreID: roachpb.StoreID(1)}
	receiver := slpb.StoreIdent{NodeID: node2, StoreID: roachpb.StoreID(2)}
	msg := slpb.Message{Type: slpb.MsgHeartbeat, From: sender, To: receiver}

	tt.AddNode(node1)
	tt.AddNode(node2)
	tt.AddStore(sender)
	tt.AddStore(receiver)

	// Fill up the receive queue of the receiver. Nothing is consuming from it.
	sendDropped := 0
	for i := 0; i < tt.maxHandlerSize; i++ {
		testutils.SucceedsSoon(
			t, func() error {
				// The message enqueue can fail temporarily if the sender queue fills up.
				if !tt.transports[sender.NodeID].SendAsync(ctx, msg) {
					sendDropped++
					return errors.New("still waiting to enqueue message")
				}
				return nil
			},
		)
	}

	require.Equal(
		t, int64(sendDropped), tt.transports[sender.NodeID].metrics.MessagesSendDropped.Count(),
	)
	testutils.SucceedsSoon(
		t, func() error {
			if tt.transports[sender.NodeID].metrics.MessagesSent.Count() != int64(tt.maxHandlerSize) {
				return errors.New("not all messages are sent yet")
			}
			return nil
		},
	)
	testutils.SucceedsSoon(
		t, func() error {
			if tt.transports[receiver.NodeID].metrics.MessagesReceived.Count() != int64(tt.maxHandlerSize) {
				return errors.New("not all messages are received yet")
			}
			return nil
		},
	)
	// The receiver queue is full but the enqueue to the sender queue succeeds.
	require.True(t, tt.transports[sender.NodeID].SendAsync(ctx, msg))
	testutils.SucceedsSoon(
		t, func() error {
			if tt.transports[receiver.NodeID].metrics.MessagesReceiveDropped.Count() != int64(1) {
				return errors.New("message not dropped yet")
			}
			return nil
		},
	)
}
