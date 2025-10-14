// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"sync"
	"testing"
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// mockBatchTransport captures batches sent via SendBatchDirect for testing.
type mockBatchTransport struct {
	mu           sync.Mutex
	batches      [][]slpb.Message
	sentMessages []slpb.Message
}

func (m *mockBatchTransport) SendAsync(ctx context.Context, msg slpb.Message) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMessages = append(m.sentMessages, msg)
	return true
}

func (m *mockBatchTransport) SendBatchDirect(
	ctx context.Context, nodeID roachpb.NodeID, messages []slpb.Message,
) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	batch := make([]slpb.Message, len(messages))
	copy(batch, messages)
	m.batches = append(m.batches, batch)
	return true
}

func (m *mockBatchTransport) getBatches() [][]slpb.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([][]slpb.Message, len(m.batches))
	copy(result, m.batches)
	return result
}

func (m *mockBatchTransport) getAsyncMessages() []slpb.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]slpb.Message, len(m.sentMessages))
	copy(result, m.sentMessages)
	return result
}

func (m *mockBatchTransport) clearAsyncMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentMessages = m.sentMessages[:0]
}

func (m *mockBatchTransport) getBatchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}

func (m *mockBatchTransport) getTotalMessagesSent() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, batch := range m.batches {
		total += len(batch)
	}
	return total
}

var _ BatchMessageSender = (*mockBatchTransport)(nil)

// TestHeartbeatCoordinatorResponsesBypassBatching verifies that heartbeat
// responses are sent immediately and bypass the batching mechanism.
func TestHeartbeatCoordinatorResponsesBypassBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	response := slpb.Message{
		Type:       slpb.MsgHeartbeatResp,
		From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
		To:         slpb.StoreIdent{NodeID: 2, StoreID: 2},
		Epoch:      1,
		Expiration: hlc.Timestamp{WallTime: 100},
	}

	sent := coordinator.SendAsync(ctx, response)
	require.True(t, sent)

	// Response should be sent immediately via SendAsync, not batched.
	asyncMessages := transport.getAsyncMessages()
	require.Len(t, asyncMessages, 1)
	require.Equal(t, slpb.MsgHeartbeatResp, asyncMessages[0].Type)

	// No batches should be created.
	require.Equal(t, 0, transport.getBatchCount())

	require.Equal(t, int64(1), coordinator.metrics.MessagesSentImmediate.Count())
	require.Equal(t, int64(0), coordinator.metrics.MessagesEnqueued.Count())
}

// TestHeartbeatCoordinatorRequestsAreBatched verifies that heartbeat requests
// are enqueued and sent as batches after the batching duration.
func TestHeartbeatCoordinatorRequestsAreBatched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	// Set short durations for the test.
	HeartbeatBatchingDuration.Override(ctx, &settings.SV, 20*time.Millisecond)

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	// Enqueue multiple requests to the same node.
	requests := []slpb.Message{
		{
			Type:       slpb.MsgHeartbeat,
			From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:         slpb.StoreIdent{NodeID: 2, StoreID: 2},
			Epoch:      1,
			Expiration: hlc.Timestamp{WallTime: 100},
		},
		{
			Type:       slpb.MsgHeartbeat,
			From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:         slpb.StoreIdent{NodeID: 2, StoreID: 2},
			Epoch:      2,
			Expiration: hlc.Timestamp{WallTime: 200},
		},
	}

	successes := coordinator.Enqueue(ctx, requests)
	require.Equal(t, 2, successes)

	coordinator.SignalToSend()

	time.Sleep(60 * time.Millisecond)

	batches := transport.getBatches()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)

	require.Equal(t, int64(2), coordinator.metrics.MessagesEnqueued.Count())
	require.Equal(t, int64(2), coordinator.metrics.MessagesSent.Count())
	require.Equal(t, int64(1), coordinator.metrics.SignalsAccepted.Count())
}

// TestHeartbeatCoordinatorMultipleNodes verifies that messages to different
// nodes are sent in separate batches with smearing.
func TestHeartbeatCoordinatorMultipleNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	HeartbeatBatchingDuration.Override(ctx, &settings.SV, 20*time.Millisecond)
	HeartbeatSmearDuration.Override(ctx, &settings.SV, 20*time.Millisecond)

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	requests := []slpb.Message{
		{
			Type:       slpb.MsgHeartbeat,
			From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:         slpb.StoreIdent{NodeID: 2, StoreID: 2},
			Epoch:      1,
			Expiration: hlc.Timestamp{WallTime: 100},
		},
		{
			Type:       slpb.MsgHeartbeat,
			From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:         slpb.StoreIdent{NodeID: 3, StoreID: 3},
			Epoch:      1,
			Expiration: hlc.Timestamp{WallTime: 100},
		},
	}

	coordinator.Enqueue(ctx, requests)
	coordinator.SignalToSend()

	time.Sleep(60 * time.Millisecond)

	batches := transport.getBatches()
	require.Len(t, batches, 2)
	require.Len(t, batches[0], 1)
	require.Len(t, batches[1], 1)

	require.Equal(t, int64(2), coordinator.metrics.ActiveQueues.Value())
}

// TestHeartbeatCoordinatorClearQueues verifies that ClearQueues removes all
// pending messages from the queues.
func TestHeartbeatCoordinatorClearQueues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	requests := []slpb.Message{
		{
			Type:       slpb.MsgHeartbeat,
			From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:         slpb.StoreIdent{NodeID: 2, StoreID: 2},
			Epoch:      1,
			Expiration: hlc.Timestamp{WallTime: 100},
		},
		{
			Type:       slpb.MsgHeartbeat,
			From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:         slpb.StoreIdent{NodeID: 3, StoreID: 3},
			Epoch:      1,
			Expiration: hlc.Timestamp{WallTime: 100},
		},
	}

	coordinator.Enqueue(ctx, requests)

	require.Equal(t, int64(2), coordinator.metrics.TotalMessagesInQueues.Value())

	coordinator.ClearQueues()

	require.Equal(t, int64(0), coordinator.metrics.TotalMessagesInQueues.Value())

	coordinator.SignalToSend()
	time.Sleep(60 * time.Millisecond)

	require.Equal(t, 0, transport.getBatchCount())
}

// TestHeartbeatCoordinatorBatchingWindow verifies that additional signals
// during the batching window don't interrupt the batching process.
func TestHeartbeatCoordinatorBatchingWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	HeartbeatBatchingDuration.Override(ctx, &settings.SV, 50*time.Millisecond)
	HeartbeatSmearDuration.Override(ctx, &settings.SV, 20*time.Millisecond)

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	coordinator.Enqueue(ctx, []slpb.Message{
		{
			Type:       slpb.MsgHeartbeat,
			From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:         slpb.StoreIdent{NodeID: 2, StoreID: 2},
			Epoch:      1,
			Expiration: hlc.Timestamp{WallTime: 100},
		},
	})
	coordinator.SignalToSend()

	time.Sleep(10 * time.Millisecond)
	coordinator.Enqueue(ctx, []slpb.Message{
		{
			Type:       slpb.MsgHeartbeat,
			From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:         slpb.StoreIdent{NodeID: 2, StoreID: 2},
			Epoch:      2,
			Expiration: hlc.Timestamp{WallTime: 200},
		},
	})
	coordinator.SignalToSend() // This signal should be drained during collection window.

	time.Sleep(100 * time.Millisecond)

	batches := transport.getBatches()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)
}

// TestHeartbeatCoordinatorSmearing verifies that messages are sent with
// smearing over the configured duration.
func TestHeartbeatCoordinatorSmearing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	HeartbeatBatchingDuration.Override(ctx, &settings.SV, 10*time.Millisecond)
	HeartbeatSmearDuration.Override(ctx, &settings.SV, 50*time.Millisecond)

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	var requests []slpb.Message
	for i := 2; i <= 5; i++ {
		requests = append(requests, slpb.Message{
			Type: slpb.MsgHeartbeat,
			From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To: slpb.StoreIdent{NodeID: roachpb.NodeID(i),
				StoreID: roachpb.StoreID(i)},
			Epoch:      1,
			Expiration: hlc.Timestamp{WallTime: 100},
		})
	}

	coordinator.Enqueue(ctx, requests)
	coordinator.SignalToSend()

	time.Sleep(80 * time.Millisecond)

	batches := transport.getBatches()
	require.Len(t, batches, 4)

	for _, batch := range batches {
		require.Len(t, batch, 1)
	}

	require.Equal(t, int64(4), coordinator.metrics.MessagesSent.Count())
	require.Equal(t, int64(4), coordinator.metrics.ActiveQueues.Value())
}

// TestHeartbeatCoordinator_BasicEnqueue tests basic message enqueueing.
func TestHeartbeatCoordinator_BasicEnqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	msg1 := slpb.Message{
		Type: slpb.MsgHeartbeat,
		From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
		To:   slpb.StoreIdent{NodeID: 2, StoreID: 1},
	}
	msg2 := slpb.Message{
		Type: slpb.MsgHeartbeat,
		From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
		To:   slpb.StoreIdent{NodeID: 2, StoreID: 2},
	}

	coordinator.Enqueue(ctx, []slpb.Message{msg1, msg2})

	queue, ok := coordinator.nodeQueues.Load(roachpb.NodeID(2))
	require.True(t, ok)

	require.Equal(t, 2, len((*queue).messages),
		"Both messages should be in the same queue")
}

// TestHeartbeatCoordinator_RequestEnqueue tests that requests are enqueued for
// smearing.
func TestHeartbeatCoordinator_RequestEnqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	request := slpb.Message{
		Type: slpb.MsgHeartbeat,
		From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
		To:   slpb.StoreIdent{NodeID: 2, StoreID: 1},
	}

	coordinator.Enqueue(ctx, []slpb.Message{request})

	queue, ok := coordinator.nodeQueues.Load(roachpb.NodeID(2))
	require.True(t, ok)
	require.Equal(t, 1, len((*queue).messages))
}

// TestHeartbeatCoordinator_SignalTriggersAsyncBatchSend tests that SignalToSend
// drains queues and triggers async batch sending via Transport.SendBatchDirect.
func TestHeartbeatCoordinator_SignalTriggersAsyncBatchSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	HeartbeatBatchingDuration.Override(ctx, &settings.SV, 10*time.Millisecond)
	HeartbeatSmearDuration.Override(ctx, &settings.SV, 10*time.Millisecond)

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	var messages []slpb.Message
	for i := 0; i < 5; i++ {
		msg := slpb.Message{
			Type: slpb.MsgHeartbeat,
			From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:   slpb.StoreIdent{NodeID: 2, StoreID: roachpb.StoreID(i + 1)},
		}
		messages = append(messages, msg)
	}

	coordinator.Enqueue(ctx, messages)

	queue, ok := coordinator.nodeQueues.Load(roachpb.NodeID(2))
	require.True(t, ok)
	require.Equal(t, 5, len((*queue).messages))

	coordinator.SignalToSend()

	time.Sleep(50 * time.Millisecond)

	require.Equal(t, 0, len((*queue).messages),
		"Queue should be empty after SignalToSend")

	require.Equal(t, 1, transport.getBatchCount(), "Should have sent 1 batch")
	batches := transport.getBatches()
	require.Len(t, batches[0], 5, "Batch should contain 5 messages")
}

// TestHeartbeatCoordinator_SignalCoalescing tests signal coalescing behaviour.
func TestHeartbeatCoordinator_SignalCoalescing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	HeartbeatBatchingDuration.Override(ctx, &settings.SV, 10*time.Millisecond)
	HeartbeatSmearDuration.Override(ctx, &settings.SV, 10*time.Millisecond)

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	msg1 := slpb.Message{
		Type: slpb.MsgHeartbeat,
		From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
		To:   slpb.StoreIdent{NodeID: 2, StoreID: 1},
	}
	msg2 := slpb.Message{
		Type: slpb.MsgHeartbeat,
		From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
		To:   slpb.StoreIdent{NodeID: 2, StoreID: 2},
	}

	coordinator.Enqueue(ctx, []slpb.Message{msg1, msg2})

	coordinator.SignalToSend()

	coordinator.SignalToSend()
	coordinator.SignalToSend()

	time.Sleep(50 * time.Millisecond)

	queue, ok := coordinator.nodeQueues.Load(roachpb.NodeID(2))
	require.True(t, ok)
	require.Equal(t, 0, len((*queue).messages), "Queue should be empty after processing")

	// Verify at least one signal was accepted (the first one).
	require.GreaterOrEqual(t, coordinator.metrics.SignalsAccepted.Count(), int64(1),
		"Should have accepted at least 1 signal")
}

// TestHeartbeatCoordinator_SmearingTimingBehaviour verifies that smearing occurs
// within the expected time window.
func TestHeartbeatCoordinator_SmearingTimingBkehaviour(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	HeartbeatBatchingDuration.Override(ctx, &settings.SV, 10*time.Millisecond)
	HeartbeatSmearDuration.Override(ctx, &settings.SV, 10*time.Millisecond)

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	for nodeID := roachpb.NodeID(2); nodeID <= 10; nodeID++ {
		msg := slpb.Message{
			Type: slpb.MsgHeartbeat,
			From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:   slpb.StoreIdent{NodeID: nodeID, StoreID: 1},
		}
		coordinator.Enqueue(ctx, []slpb.Message{msg})
	}

	start := time.Now()
	coordinator.SignalToSend()

	time.Sleep(50 * time.Millisecond)
	duration := time.Since(start)

	require.Less(t, duration, 100*time.Millisecond,
		"Processing should complete within 100ms")

	require.Equal(t, 9, transport.getTotalMessagesSent(),
		"Should have sent 9 messages")

	t.Logf("Processed 9 nodes in %v (smear duration: %v)",
		duration, HeartbeatSmearDuration.Get(&settings.SV))
}

// TestHeartbeatCoordinator_ProductionScaleSimulation tests heartbeat smearing
// at production scale:
// 24 nodes with 3 stores per node (72 total stores), simulating realistic
// cluster behaviour.
func TestHeartbeatCoordinator_ProductionScaleSimulation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	HeartbeatBatchingDuration.Override(ctx, &settings.SV, 10*time.Millisecond)
	HeartbeatSmearDuration.Override(ctx, &settings.SV, 10*time.Millisecond)

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	const numNodes = 24
	const storesPerNode = 3
	const totalStores = numNodes * storesPerNode

	t.Logf("Starting production-scale simulation: %d nodes, %d stores per node,"+
		" %d total stores", numNodes, storesPerNode, totalStores)

	var allMessages []slpb.Message
	messageCount := 0

	for fromNodeID := roachpb.NodeID(1); fromNodeID <= numNodes; fromNodeID++ {
		for fromStoreID := roachpb.StoreID(1); fromStoreID <= storesPerNode; fromStoreID++ {
			for toNodeID := roachpb.NodeID(1); toNodeID <= numNodes; toNodeID++ {
				if toNodeID == fromNodeID {
					continue
				}
				for toStoreID := roachpb.StoreID(1); toStoreID <= storesPerNode; toStoreID++ {
					msg := slpb.Message{
						Type: slpb.MsgHeartbeat,
						From: slpb.StoreIdent{NodeID: fromNodeID, StoreID: fromStoreID},
						To:   slpb.StoreIdent{NodeID: toNodeID, StoreID: toStoreID},
					}
					allMessages = append(allMessages, msg)
					messageCount++
				}
			}
		}
	}

	t.Logf("Generated %d heartbeat messages for production-scale simulation",
		messageCount)

	startTime := time.Now()
	coordinator.Enqueue(ctx, allMessages)
	enqueueDuration := time.Since(startTime)

	t.Logf("Enqueued %d messages in %v", messageCount, enqueueDuration)

	nodeQueueCounts := make(map[roachpb.NodeID]int)
	for nodeID := roachpb.NodeID(1); nodeID <= numNodes; nodeID++ {
		queue, ok := coordinator.nodeQueues.Load(nodeID)
		if ok {
			count := len((*queue).messages)
			nodeQueueCounts[nodeID] = count
			t.Logf("  Node %d: %d messages", nodeID, count)
		}
	}

	smearStartTime := time.Now()
	coordinator.SignalToSend()

	time.Sleep(50 * time.Millisecond)
	smearDuration := time.Since(smearStartTime)

	t.Logf("Heartbeat smearing completed in %v (target: ~20ms)", smearDuration)

	for nodeID := roachpb.NodeID(1); nodeID <= numNodes; nodeID++ {
		queue, ok := coordinator.nodeQueues.Load(nodeID)
		if ok {
			require.Equal(t, 0, len((*queue).messages),
				"Node %d queue should be empty after SignalToSend", nodeID)
		}
	}

	t.Logf("Total messages processed: %d", messageCount)
	t.Logf("Enqueue duration: %v", enqueueDuration)
	t.Logf("Smear duration: %v", smearDuration)
	t.Logf("Messages per millisecond: %.2f",
		float64(messageCount)/float64(smearDuration.Milliseconds()))
	t.Logf("Average messages per node: %.2f",
		float64(messageCount)/float64(numNodes))

	smearDurationMs := smearDuration.Milliseconds()
	if smearDurationMs > 0 {
		t.Logf("Smearing efficiency: %.2f messages/ms",
			float64(messageCount)/float64(smearDurationMs))
	}

	t.Logf("Production-scale heartbeat smearing simulation completed successfully")
}
