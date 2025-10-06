// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"
	"sort"
	"time"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/taskpacer"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// heartbeatPacerConfig implements taskpacer.Config for heartbeat message pacing.
type heartbeatPacerConfig struct {
	refresh time.Duration // Interval between tasks (smear duration)
	smear   time.Duration // Interval between batches within a task
}

func (c *heartbeatPacerConfig) GetRefresh() time.Duration {
	return c.refresh
}

func (c *heartbeatPacerConfig) GetSmear() time.Duration {
	return c.smear
}

type HeartbeatCoordinator struct {
	transport    MessageSender
	tickDuration time.Duration
	stopper      *stop.Stopper
	settings     *cluster.Settings

	nodeQueues syncutil.Map[roachpb.NodeID, *DestinationQueue]

	metrics *HeartbeatCoordinatorMetrics
}

type DestinationQueue struct {
	mu       syncutil.Mutex
	messages []slpb.Message
}

// Ensure HeartbeatCoordinator implements MessageSender interface
var _ MessageSender = (*HeartbeatCoordinator)(nil)

// NewHeartbeatCoordinator creates a new HeartbeatCoordinator.
func NewHeartbeatCoordinator(
	transport MessageSender,
	tickDuration time.Duration,
	stopper *stop.Stopper,
	settings *cluster.Settings,
) *HeartbeatCoordinator {
	metrics := newHeartbeatCoordinatorMetrics()

	// Initialize gauge metrics
	metrics.TickDuration.Update(tickDuration.Milliseconds())
	smearDuration := HeartbeatSmearDuration.Get(&settings.SV)
	metrics.SmearDuration.Update(smearDuration.Milliseconds())

	log.KvExec.Infof(context.Background(), "HeartbeatCoordinator created with tick_duration=%v, smear_duration=%v",
		tickDuration, smearDuration)

	return &HeartbeatCoordinator{
		transport:    transport,
		tickDuration: tickDuration,
		stopper:      stopper,
		settings:     settings,
		metrics:      metrics,
	}
}

// SendAsync implements the MessageSender interface.
// Heartbeat responses are sent immediately to bypass smearing.
// Heartbeat requests are enqueued for smeared delivery.
func (c *HeartbeatCoordinator) SendAsync(ctx context.Context, msg slpb.Message) bool {
	// Bypass smearing for responses - send immediately
	if msg.Type == slpb.MsgHeartbeatResp {
		c.metrics.MessagesSentImmediate.Inc(1)
		log.KvExec.VInfof(ctx, 2, "HeartbeatCoordinator sending immediate response to node %d", msg.To.NodeID)

		success := c.transport.SendAsync(ctx, msg)
		if !success {
			c.metrics.SendErrors.Inc(1)
			log.KvExec.Warningf(ctx, "Failed to send immediate heartbeat response to node %d", msg.To.NodeID)
		}
		return success
	}

	// Enqueue requests for smeared delivery
	c.metrics.MessagesEnqueued.Inc(1)
	log.KvExec.VInfof(ctx, 2, "HeartbeatCoordinator enqueuing request to node %d", msg.To.NodeID)

	c.Enqueue([]slpb.Message{msg})
	return true
}

// Enqueue adds messages to the appropriate destination queue.
func (c *HeartbeatCoordinator) Enqueue(messages []slpb.Message) int {
	log.KvExec.VInfof(context.Background(), 2, "HeartbeatCoordinator enqueuing %d messages", len(messages))
	successes := 0
	for _, msg := range messages {
		nodeID := msg.To.NodeID

		// Get or create queue for this destination
		queue, isNew := c.getOrCreateQueue(nodeID)
		if isNew {
			c.metrics.ActiveQueues.Inc(1)
			log.KvExec.VInfof(context.Background(), 1, "HeartbeatCoordinator created new queue for node %d", nodeID)
		}

		// Enqueue the message
		c.enqueueToQueue(queue, msg)

		successes++
	}

	// Update total messages in queues metric
	c.updateTotalMessagesMetric()

	return successes
}

// updateTotalMessagesMetric updates the total messages in queues metric.
func (c *HeartbeatCoordinator) updateTotalMessagesMetric() {
	totalMessages := 0
	c.nodeQueues.Range(func(nodeID roachpb.NodeID, queue **DestinationQueue) bool {
		(*queue).mu.Lock()
		defer (*queue).mu.Unlock()
		totalMessages += len((*queue).messages)
		return true
	})
	c.metrics.TotalMessagesInQueues.Update(int64(totalMessages))
}

// enqueueToQueue safely appends a message to a queue with proper locking.
func (c *HeartbeatCoordinator) enqueueToQueue(queue *DestinationQueue, msg slpb.Message) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.messages = append(queue.messages, msg)
}

// getOrCreateQueue returns the queue for the given nodeID, creating it if necessary.
// Returns (queue, isNew) where isNew indicates if the queue was just created.
func (c *HeartbeatCoordinator) getOrCreateQueue(nodeID roachpb.NodeID) (*DestinationQueue, bool) {
	// Try to get existing queue
	if queue, ok := c.nodeQueues.Load(nodeID); ok {
		return *queue, false
	}

	// Create new queue
	queue := &DestinationQueue{
		messages: make([]slpb.Message, 0),
	}

	// Store the new queue (or get existing if another goroutine created it)
	stored, loaded := c.nodeQueues.LoadOrStore(nodeID, &queue)
	return *stored, !loaded
}

// sendMessagesWithPacing sends messages using the Pacer to control the rate.
func (c *HeartbeatCoordinator) sendMessagesWithPacing() {
	startTime := timeutil.Now()
	log.KvExec.VInfof(context.Background(), 1, "HeartbeatCoordinator starting paced send from nodeQueues")

	// Collect all nodeIDs that have messages - this is now local to this call
	var nodeIDs []roachpb.NodeID
	c.nodeQueues.Range(func(nodeID roachpb.NodeID, queue **DestinationQueue) bool {
		(*queue).mu.Lock()
		defer (*queue).mu.Unlock()
		hasMessages := len((*queue).messages) > 0
		if hasMessages {
			nodeIDs = append(nodeIDs, nodeID)
		}
		return true
	})

	if len(nodeIDs) == 0 {
		log.KvExec.VInfof(context.Background(), 2, "HeartbeatCoordinator no messages to send")
		return
	}

	// Sort nodeIDs for deterministic ordering
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})

	smearDuration := HeartbeatSmearDuration.Get(&c.settings.SV)
	log.KvExec.VInfof(context.Background(), 2, "HeartbeatCoordinator using smear duration: %v", smearDuration)

	// Create a new pacer config with the smear duration
	pacerConfig := &heartbeatPacerConfig{
		refresh: smearDuration,
		smear:   smearDuration / 10, // Divide into ~10 batches
	}

	now := timeutil.Now()

	// Create a temporary pacer for this batch
	tempPacer := taskpacer.New(pacerConfig)
	tempPacer.StartTask(now)

	// Use pacer to schedule when each node should be signaled
	workLeft := len(nodeIDs)
	nodeIndex := 0

	for workLeft > 0 {
		// Get pacing information from pacer
		todo, by := tempPacer.Pace(timeutil.Now(), workLeft)

		if todo <= 0 {
			log.KvExec.Warningf(context.Background(), "HeartbeatCoordinator pacer returned todo=%d, breaking", todo)
			c.metrics.PacerErrors.Inc(1)
			break
		}

		log.KvExec.VInfof(context.Background(), 3, "HeartbeatCoordinator signaling batch of %d nodes", todo)

		// Process the next batch of nodes synchronously
		for j := 0; j < todo; j++ {
			if nodeIndex < len(nodeIDs) {
				nodeID := nodeIDs[nodeIndex]
				c.processNodeQueue(nodeID)
				nodeIndex++
			}
		}

		workLeft -= todo

		// Sleep until next batch if needed
		if workLeft > 0 && by.After(timeutil.Now()) {
			sleepDuration := by.Sub(timeutil.Now())
			if sleepDuration > 0 {
				log.KvExec.VInfof(context.Background(), 3, "HeartbeatCoordinator sleeping for %v before next batch", sleepDuration)
				time.Sleep(sleepDuration)
			}
		}
	}

	totalDuration := timeutil.Since(startTime)
	log.KvExec.Infof(context.Background(), "HeartbeatCoordinator completed paced send: signaled %d nodes over %v",
		len(nodeIDs), totalDuration)
}

// processScheduledNodes function removed - processing is now synchronous

// processNodeQueue processes all messages in a specific node's queue.
func (c *HeartbeatCoordinator) processNodeQueue(nodeID roachpb.NodeID) {
	queue, ok := c.nodeQueues.Load(nodeID)
	if !ok {
		log.KvExec.Warningf(context.Background(), "HeartbeatCoordinator no queue found for node %d", nodeID)
		return
	}

	(*queue).mu.Lock()
	defer (*queue).mu.Unlock()
	nodeMessages := (*queue).messages
	(*queue).messages = nil // Clear the queue

	if len(nodeMessages) == 0 {
		log.KvExec.VInfof(context.Background(), 3, "HeartbeatCoordinator no messages for node %d", nodeID)
		return
	}

	// Send messages for this node
	messagesSent := 0
	for _, msg := range nodeMessages {
		success := c.transport.SendAsync(context.Background(), msg)
		if success {
			c.metrics.MessagesSent.Inc(1)
			messagesSent++
		} else {
			c.metrics.SendErrors.Inc(1)
			log.KvExec.Warningf(context.Background(), "HeartbeatCoordinator failed to send message to node %d", nodeID)
		}
	}

	log.KvExec.VInfof(context.Background(), 3, "HeartbeatCoordinator sent %d messages to node %d", messagesSent, nodeID)
}
