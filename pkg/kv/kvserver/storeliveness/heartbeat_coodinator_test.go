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

	// Set a short batching duration for the test.
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

	successes := coordinator.Enqueue(requests)
	require.Equal(t, 2, successes)

	coordinator.SignalToSend()

	// Wait for batching duration plus some buffer.
	time.Sleep(50 * time.Millisecond)

	batches := transport.getBatches()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)

	require.Equal(t, int64(2), coordinator.metrics.MessagesEnqueued.Count())
	require.Equal(t, int64(2), coordinator.metrics.MessagesSent.Count())
	require.Equal(t, int64(1), coordinator.metrics.SignalsAccepted.Count())
}

// TestHeartbeatCoordinatorMultipleNodes verifies that messages to different
// nodes are sent in separate batches.
func TestHeartbeatCoordinatorMultipleNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	settings := cluster.MakeTestingClusterSettings()
	transport := &mockBatchTransport{}

	HeartbeatBatchingDuration.Override(ctx, &settings.SV, 20*time.Millisecond)

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

	coordinator.Enqueue(requests)
	coordinator.SignalToSend()

	time.Sleep(50 * time.Millisecond)

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

	coordinator.Enqueue(requests)

	require.Equal(t, int64(2), coordinator.metrics.TotalMessagesInQueues.Value())

	coordinator.ClearQueues()

	require.Equal(t, int64(0), coordinator.metrics.TotalMessagesInQueues.Value())

	coordinator.SignalToSend()
	time.Sleep(50 * time.Millisecond)

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

	coordinator := NewHeartbeatCoordinator(transport, stopper, settings)

	coordinator.Enqueue([]slpb.Message{
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
	coordinator.Enqueue([]slpb.Message{
		{
			Type:       slpb.MsgHeartbeat,
			From:       slpb.StoreIdent{NodeID: 1, StoreID: 1},
			To:         slpb.StoreIdent{NodeID: 2, StoreID: 2},
			Epoch:      2,
			Expiration: hlc.Timestamp{WallTime: 200},
		},
	})
	coordinator.SignalToSend() // This signal should be drained during collection window.

	time.Sleep(80 * time.Millisecond)

	batches := transport.getBatches()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)
}
