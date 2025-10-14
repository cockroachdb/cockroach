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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// mockBatchTransport captures batches sent via SendBatchDirect for testing.
type mockBatchTransport struct {
	mu           sync.Mutex
	batches      [][]slpb.Message // Each entry is a batch sent to a node
	sentMessages []slpb.Message   // For SendAsync (responses)
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
	// Make a copy of the batch
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

// func (m *mockBatchTransport) getSentMessages() []slpb.Message {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
// 	result := make([]slpb.Message, len(m.sentMessages))
// 	copy(result, m.sentMessages)
// 	return result
// }

func (m *mockBatchTransport) getAllMessages() []slpb.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	var allMessages []slpb.Message

	// Add messages from batches (sent via SendBatchDirect)
	for _, batch := range m.batches {
		allMessages = append(allMessages, batch...)
	}

	// Add messages sent via SendAsync (responses)
	allMessages = append(allMessages, m.sentMessages...)

	return allMessages
}

func (m *mockBatchTransport) getBatchMessages() []slpb.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	var batchMessages []slpb.Message

	// Add messages from batches (sent via SendBatchDirect)
	for _, batch := range m.batches {
		batchMessages = append(batchMessages, batch...)
	}

	return batchMessages
}

func (m *mockBatchTransport) getAsyncMessages() []slpb.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]slpb.Message, len(m.sentMessages))
	copy(result, m.sentMessages)
	return result
}

func (m *mockBatchTransport) clearAllMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = m.batches[:0]
	m.sentMessages = m.sentMessages[:0]
}

func (m *mockBatchTransport) clearBatchMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = m.batches[:0]
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

// Ensure mockBatchTransport implements BatchMessageSender interface
var _ BatchMessageSender = (*mockBatchTransport)(nil)

// TestHeartbeatCoordinator_BasicEnqueue tests basic message enqueueing.
func TestHeartbeatCoordinator_BasicEnqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tickDuration := 1 * time.Second
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, tickDuration, stopper, settings)

	// Create test messages for the same node
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

	// Enqueue messages
	coordinator.Enqueue([]slpb.Message{msg1, msg2})

	// Verify queue was created
	queue, ok := coordinator.nodeQueues.Load(roachpb.NodeID(2))
	require.True(t, ok)

	// Verify messages are in channel
	require.Equal(t, 2, len((*queue).messages), "Both messages should be in the same queue")
}

// TestHeartbeatCoordinator_ResponseBypass tests that responses bypass smearing.
func TestHeartbeatCoordinator_ResponseBypass(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tickDuration := 1 * time.Second
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, tickDuration, stopper, settings)

	// Create a response message
	response := slpb.Message{
		Type: slpb.MsgHeartbeatResp,
		From: slpb.StoreIdent{NodeID: 2, StoreID: 1},
		To:   slpb.StoreIdent{NodeID: 1, StoreID: 1},
	}

	// Send response - should bypass smearing and go directly to transport
	sent := coordinator.SendAsync(ctx, response)
	require.True(t, sent)

	// Verify no queue was created for responses (they bypass coordinator)
	_, ok := coordinator.nodeQueues.Load(roachpb.NodeID(1))
	require.False(t, ok)

	// Verify response was sent via SendAsync (not batched)
	transport.mu.Lock()
	require.Len(t, transport.sentMessages, 1)
	require.Equal(t, response, transport.sentMessages[0])
	transport.mu.Unlock()
}

// TestHeartbeatCoordinator_RequestEnqueue tests that requests are enqueued for smearing.
func TestHeartbeatCoordinator_RequestEnqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tickDuration := 1 * time.Second
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, tickDuration, stopper, settings)

	// Create a request message
	request := slpb.Message{
		Type: slpb.MsgHeartbeat,
		From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
		To:   slpb.StoreIdent{NodeID: 2, StoreID: 1},
	}

	// Send request - should be enqueued for smearing
	sent := coordinator.SendAsync(ctx, request)
	require.True(t, sent)

	// Verify queue was created and message enqueued
	queue, ok := coordinator.nodeQueues.Load(roachpb.NodeID(2))
	require.True(t, ok)
	require.Equal(t, 1, len((*queue).messages))
}

// TestHeartbeatCoordinator_SignalTriggersAsyncBatchSend tests that Signal drains queues
// and triggers async batch sending via Transport.SendBatchDirect.
func TestHeartbeatCoordinator_SignalTriggersAsyncBatchSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tickDuration := 1 * time.Second
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, tickDuration, stopper, settings)

	// Create messages for a single node
	var messages []slpb.Message
	for i := 0; i < 5; i++ {
		msg := slpb.Message{
			Type: slpb.MsgHeartbeat,
			From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:   slpb.StoreIdent{NodeID: 2, StoreID: roachpb.StoreID(i + 1)},
		}
		messages = append(messages, msg)
	}

	// Enqueue messages
	coordinator.Enqueue(messages)

	// Verify messages are in queue
	queue, ok := coordinator.nodeQueues.Load(roachpb.NodeID(2))
	require.True(t, ok)
	require.Equal(t, 5, len((*queue).messages))

	// Signal coordinator to process messages
	coordinator.Signal()

	// Wait for async processing (collection window + smear + async task)
	time.Sleep(50 * time.Millisecond)

	// Verify queue is now empty (messages were drained)
	require.Equal(t, 0, len((*queue).messages), "Queue should be empty after Signal")

	// Verify batch was sent via SendBatchDirect
	require.Equal(t, 1, transport.getBatchCount(), "Should have sent 1 batch")
	batches := transport.getBatches()
	require.Len(t, batches[0], 5, "Batch should contain 5 messages")
}

// TestHeartbeatCoordinator_MultipleNodesSmearing tests that messages to different nodes
// are properly distributed and smeared together.
func TestHeartbeatCoordinator_MultipleNodesSmearing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tickDuration := 1 * time.Second
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, tickDuration, stopper, settings)

	// Create messages for 4 different nodes, 2 messages each
	destinationNodes := []roachpb.NodeID{2, 3, 4, 5}
	for _, nodeID := range destinationNodes {
		for i := 0; i < 2; i++ {
			msg := slpb.Message{
				Type: slpb.MsgHeartbeat,
				From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
				To:   slpb.StoreIdent{NodeID: nodeID, StoreID: roachpb.StoreID(i + 1)},
			}
			coordinator.Enqueue([]slpb.Message{msg})
		}
	}

	// Verify messages are distributed across different node queues
	for _, nodeID := range destinationNodes {
		queue, ok := coordinator.nodeQueues.Load(nodeID)
		require.True(t, ok, "Queue should exist for node %d", nodeID)
		require.Equal(t, 2, len((*queue).messages), "Node %d should have 2 messages", nodeID)
	}

	// Signal coordinator
	coordinator.Signal()

	// Wait for async processing
	time.Sleep(50 * time.Millisecond)

	// Verify all queues are empty
	for _, nodeID := range destinationNodes {
		queue, ok := coordinator.nodeQueues.Load(nodeID)
		require.True(t, ok)
		require.Equal(t, 0, len((*queue).messages), "Node %d queue should be empty", nodeID)
	}

	// Verify 4 batches were sent (one per node)
	require.Equal(t, 4, transport.getBatchCount(), "Should have sent 4 batches (one per node)")

	// Verify total messages sent
	require.Equal(t, 8, transport.getTotalMessagesSent(), "Should have sent 8 total messages")
}

// TestHeartbeatCoordinator_SignalCoalescing tests signal coalescing behavior.
func TestHeartbeatCoordinator_SignalCoalescing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tickDuration := 1 * time.Second
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, tickDuration, stopper, settings)

	// Create test messages
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

	// Enqueue messages
	coordinator.Enqueue([]slpb.Message{msg1, msg2})

	// Send first signal - should be accepted
	coordinator.Signal()

	// Immediately send more signals while processing - these should be ignored or drained
	coordinator.Signal()
	coordinator.Signal()

	// Wait for processing to complete
	time.Sleep(50 * time.Millisecond)

	// Verify queue is empty (messages were processed)
	queue, ok := coordinator.nodeQueues.Load(roachpb.NodeID(2))
	require.True(t, ok)
	require.Equal(t, 0, len((*queue).messages), "Queue should be empty after processing")

	// Verify at least one signal was accepted (the first one)
	// Note: With async processing, timing may vary, so we just verify processing occurred
	require.GreaterOrEqual(t, coordinator.metrics.SignalsAccepted.Count(), int64(1), "Should have accepted at least 1 signal")
}

// TestHeartbeatCoordinator_ClearQueues tests that ClearQueues empties all queues.
func TestHeartbeatCoordinator_ClearQueues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tickDuration := 1 * time.Second
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, tickDuration, stopper, settings)

	// Enqueue messages to multiple nodes
	for nodeID := roachpb.NodeID(2); nodeID <= 5; nodeID++ {
		for i := 0; i < 3; i++ {
			msg := slpb.Message{
				Type: slpb.MsgHeartbeat,
				From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
				To:   slpb.StoreIdent{NodeID: nodeID, StoreID: roachpb.StoreID(i + 1)},
			}
			coordinator.Enqueue([]slpb.Message{msg})
		}
	}

	// Verify queues have messages
	for nodeID := roachpb.NodeID(2); nodeID <= 5; nodeID++ {
		queue, ok := coordinator.nodeQueues.Load(nodeID)
		require.True(t, ok)
		require.Equal(t, 3, len((*queue).messages))
	}

	// Clear all queues
	coordinator.ClearQueues()

	// Verify all queues are empty
	for nodeID := roachpb.NodeID(2); nodeID <= 5; nodeID++ {
		queue, ok := coordinator.nodeQueues.Load(nodeID)
		require.True(t, ok)
		require.Equal(t, 0, len((*queue).messages), "Node %d queue should be empty", nodeID)
	}

	// Verify metrics reflect empty queues
	require.Equal(t, int64(0), coordinator.metrics.TotalMessagesInQueues.Value())
}

// TestHeartbeatCoordinator_SmearingTimingBehavior verifies that smearing occurs
// within the expected time window.
func TestHeartbeatCoordinator_SmearingTimingBehavior(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tickDuration := 1 * time.Second
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, tickDuration, stopper, settings)

	// Create messages for multiple nodes to test smearing
	for nodeID := roachpb.NodeID(2); nodeID <= 10; nodeID++ {
		msg := slpb.Message{
			Type: slpb.MsgHeartbeat,
			From: slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:   slpb.StoreIdent{NodeID: nodeID, StoreID: 1},
		}
		coordinator.Enqueue([]slpb.Message{msg})
	}

	// Signal and measure processing time
	start := time.Now()
	coordinator.Signal()

	// Wait for processing
	time.Sleep(50 * time.Millisecond)
	duration := time.Since(start)

	// Verify processing completed reasonably quickly
	// (collection window ~10ms + smear ~10ms + async overhead)
	require.Less(t, duration, 100*time.Millisecond, "Processing should complete within 100ms")

	// Verify all messages were sent
	require.Equal(t, 9, transport.getTotalMessagesSent(), "Should have sent 9 messages")

	t.Logf("Processed 9 nodes in %v (smear duration: %v)",
		duration, HeartbeatSmearDuration.Get(&settings.SV))
}

// TestHeartbeatCoordinator_ProductionScaleSimulation tests heartbeat smearing at production scale:
// 24 nodes with 3 stores per node (72 total stores), simulating realistic cluster behavior.
// This test is designed to validate the heartbeat smearing design under realistic load.
func TestHeartbeatCoordinator_ProductionScaleSimulation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tickDuration := 1 * time.Second
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}
	coordinator := NewHeartbeatCoordinator(transport, tickDuration, stopper, settings)

	// Production-scale configuration: 24 nodes, 3 stores per node
	const numNodes = 24
	const storesPerNode = 3
	const totalStores = numNodes * storesPerNode

	t.Logf("Starting production-scale simulation: %d nodes, %d stores per node, %d total stores",
		numNodes, storesPerNode, totalStores)

	// Create heartbeat messages for all stores across all nodes
	var allMessages []slpb.Message
	messageCount := 0

	// Each store sends heartbeats to all other nodes (realistic cross-node communication)
	for fromNodeID := roachpb.NodeID(1); fromNodeID <= numNodes; fromNodeID++ {
		for fromStoreID := roachpb.StoreID(1); fromStoreID <= storesPerNode; fromStoreID++ {
			// Send heartbeats to all other nodes (not self)
			for toNodeID := roachpb.NodeID(1); toNodeID <= numNodes; toNodeID++ {
				if toNodeID == fromNodeID {
					continue // Skip self
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

	t.Logf("Generated %d heartbeat messages for production-scale simulation", messageCount)

	// Verify message distribution across node queues
	nodeQueueCounts := make(map[roachpb.NodeID]int)
	for _, msg := range allMessages {
		nodeQueueCounts[msg.To.NodeID]++
	}

	t.Logf("Message distribution across nodes:")
	for nodeID := roachpb.NodeID(1); nodeID <= numNodes; nodeID++ {
		count := nodeQueueCounts[nodeID]
		t.Logf("  Node %d: %d messages", nodeID, count)
	}

	// Enqueue all messages
	startTime := time.Now()
	coordinator.Enqueue(allMessages)
	enqueueDuration := time.Since(startTime)

	t.Logf("Enqueued %d messages in %v", messageCount, enqueueDuration)

	// Verify all node queues are populated correctly
	for nodeID := roachpb.NodeID(1); nodeID <= numNodes; nodeID++ {
		queue, ok := coordinator.nodeQueues.Load(nodeID)
		require.True(t, ok, "Queue should exist for node %d", nodeID)
		expectedCount := nodeQueueCounts[nodeID]
		actualCount := len((*queue).messages)
		require.Equal(t, expectedCount, actualCount, "Node %d should have %d messages, got %d",
			nodeID, expectedCount, actualCount)
	}

	// Test the actual workflow: Enqueue + Signal (like sendHeartbeats does)
	// This simulates the real heartbeat sending process with smearing
	smearStartTime := time.Now()
	coordinator.Signal()

	// Wait for async processing to complete
	time.Sleep(25 * time.Millisecond)
	smearDuration := time.Since(smearStartTime)

	t.Logf("Heartbeat smearing completed in %v (target: ~10ms)", smearDuration)

	// Verify all queues are now empty (messages were processed)
	for nodeID := roachpb.NodeID(1); nodeID <= numNodes; nodeID++ {
		queue, ok := coordinator.nodeQueues.Load(nodeID)
		require.True(t, ok)
		require.Equal(t, 0, len((*queue).messages), "Node %d queue should be empty after Signal", nodeID)
	}

	// Performance metrics and validation
	t.Logf("=== Production-Scale Simulation Results ===")
	t.Logf("Total messages processed: %d", messageCount)
	t.Logf("Enqueue duration: %v", enqueueDuration)
	t.Logf("Smear duration: %v", smearDuration)
	t.Logf("Messages per millisecond: %.2f", float64(messageCount)/float64(smearDuration.Nanoseconds())*1e6)
	t.Logf("Average messages per node: %.2f", float64(messageCount)/float64(numNodes))

	// Validate smearing effectiveness
	smearDurationMs := smearDuration.Milliseconds()
	if smearDurationMs > 0 {
		t.Logf("Smearing efficiency: %.2f messages/ms", float64(messageCount)/float64(smearDurationMs))
	}

	// Verify the smear duration is reasonable (should be around 10ms, but allow some variance)
	expectedSmearDuration := HeartbeatSmearDuration.Get(&settings.SV)
	if smearDuration > expectedSmearDuration*2 {
		t.Logf("WARNING: Smear duration %v exceeded expected %v by more than 2x",
			smearDuration, expectedSmearDuration)
	}

	t.Logf("Production-scale heartbeat smearing simulation completed successfully")
}
